import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

/**
 * run-full-sync - ORCHESTRATORE LEGGERO
 * 
 * Non esegue direttamente la pipeline, ma coordina l'esecuzione di step separati.
 * Ogni step è eseguito da una edge function dedicata o dal sync-step-runner.
 * 
 * Sequenza step:
 * 1. import_ftp - Download file da FTP (edge function esistente)
 * 2. parse_merge - Parse e merge file
 * 3. ean_mapping - Mapping EAN
 * 4. pricing - Calcolo prezzi
 * 5. override - Skip (non implementato server-side)
 * 6. export_ean - Generazione catalogo EAN
 * 7. export_mediaworld - Export Mediaworld
 * 8. export_eprice - Export ePrice  
 * 9. upload_sftp - Upload su SFTP (edge function esistente)
 * 
 * Stato sync_runs:
 * - running: in esecuzione
 * - success: completata con successo
 * - failed: fallita
 */

interface PipelineMetrics {
  products_total: number;
  products_processed: number;
  products_ean_mapped: number;
  products_ean_missing: number;
  products_ean_invalid: number;
  products_after_override: number;
  mediaworld_export_rows: number;
  mediaworld_export_skipped: number;
  eprice_export_rows: number;
  eprice_export_skipped: number;
  exported_files_count: number;
  sftp_uploaded_files: number;
  warnings: string[];
}

function initMetrics(): PipelineMetrics {
  return {
    products_total: 0, products_processed: 0, products_ean_mapped: 0,
    products_ean_missing: 0, products_ean_invalid: 0, products_after_override: 0,
    mediaworld_export_rows: 0, mediaworld_export_skipped: 0,
    eprice_export_rows: 0, eprice_export_skipped: 0,
    exported_files_count: 0, sftp_uploaded_files: 0, warnings: []
  };
}

async function callStep(supabaseUrl: string, serviceKey: string, functionName: string, body: any): Promise<{ success: boolean; error?: string; data?: any }> {
  try {
    console.log(`[orchestrator] Calling ${functionName}...`);
    const resp = await fetch(`${supabaseUrl}/functions/v1/${functionName}`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${serviceKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    const data = await resp.json();
    if (data.status === 'error') {
      console.log(`[orchestrator] ${functionName} failed: ${data.message || data.error}`);
      return { success: false, error: data.message || data.error, data };
    }
    console.log(`[orchestrator] ${functionName} completed`);
    return { success: true, data };
  } catch (e: any) {
    console.error(`[orchestrator] Error calling ${functionName}:`, e);
    return { success: false, error: e.message };
  }
}

async function isCancelRequested(supabase: any, runId: string): Promise<boolean> {
  const { data } = await supabase.from('sync_runs').select('cancel_requested').eq('id', runId).single();
  return data?.cancel_requested === true;
}

async function finalizeRun(supabase: any, runId: string, status: string, startTime: number, errorMessage?: string, errorDetails?: any): Promise<void> {
  const { data: run } = await supabase.from('sync_runs').select('steps, metrics').eq('id', runId).single();
  await supabase.from('sync_runs').update({
    status, 
    finished_at: new Date().toISOString(), 
    runtime_ms: Date.now() - startTime,
    steps: run?.steps || {},
    metrics: run?.metrics || initMetrics(),
    error_message: errorMessage || null, 
    error_details: errorDetails || null
  }).eq('id', runId);
  console.log(`[orchestrator] Run ${runId} finalized with status: ${status}`);
}

async function updateCurrentStep(supabase: any, runId: string, stepName: string): Promise<void> {
  const { data: run } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
  const steps = { ...(run?.steps || {}), current_step: stepName };
  await supabase.from('sync_runs').update({ steps }).eq('id', runId);
}

serve(async (req) => {
  if (req.method === 'OPTIONS') return new Response(null, { headers: corsHeaders });
  if (req.method !== 'POST') {
    return new Response(JSON.stringify({ status: 'error', message: 'Method not allowed' }), 
      { status: 405, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
  }

  const supabaseUrl = Deno.env.get('SUPABASE_URL')!;
  const supabaseServiceKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
  const supabaseAnonKey = Deno.env.get('SUPABASE_ANON_KEY')!;
  
  let runId: string | null = null;
  let startTime = Date.now();
  let supabase: any = null;

  try {
    const body = await req.json().catch(() => ({}));
    const trigger = body.trigger as string;
    
    if (!trigger || !['cron', 'manual'].includes(trigger)) {
      return new Response(JSON.stringify({ status: 'error', message: 'trigger deve essere cron o manual' }), 
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    console.log(`[orchestrator] Starting pipeline, trigger: ${trigger}`);

    // Auth for manual triggers
    if (trigger === 'manual') {
      const authHeader = req.headers.get('Authorization');
      if (!authHeader?.startsWith('Bearer ')) {
        return new Response(JSON.stringify({ status: 'error', message: 'Auth required' }), 
          { status: 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      const jwt = authHeader.replace('Bearer ', '');
      const userClient = createClient(supabaseUrl, supabaseAnonKey, { global: { headers: { Authorization: `Bearer ${jwt}` } } });
      const { data: userData, error: userError } = await userClient.auth.getUser();
      if (userError || !userData?.user) {
        return new Response(JSON.stringify({ status: 'error', message: 'Invalid token' }), 
          { status: 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      console.log(`[orchestrator] Manual trigger by user: ${userData.user.id}`);
    }

    supabase = createClient(supabaseUrl, supabaseServiceKey);

    // Check running jobs
    const { data: runningJobs } = await supabase.from('sync_runs').select('id').eq('status', 'running').limit(1);
    if (runningJobs?.length) {
      return new Response(JSON.stringify({ status: 'error', message: 'Sync già in corso' }), 
        { status: 409, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    // For cron, check if enabled
    if (trigger === 'cron') {
      const { data: config } = await supabase.from('sync_config').select('enabled, frequency_minutes').eq('id', 1).single();
      if (!config?.enabled) {
        console.log('[orchestrator] Sync disabled');
        return new Response(JSON.stringify({ status: 'skipped', message: 'Disabled' }), 
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      const { data: lastRun } = await supabase.from('sync_runs').select('started_at').eq('trigger_type', 'cron').eq('attempt', 1).order('started_at', { ascending: false }).limit(1);
      if (lastRun?.length) {
        const elapsed = Date.now() - new Date(lastRun[0].started_at).getTime();
        if (elapsed < config.frequency_minutes * 60 * 1000) {
          console.log('[orchestrator] Frequency not elapsed');
          return new Response(JSON.stringify({ status: 'skipped', message: 'Frequency not elapsed' }), 
            { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }
      }
    }

    // Get fee config
    const { data: feeData } = await supabase.from('fee_config').select('*').limit(1).single();
    const feeConfig = {
      feeDrev: feeData?.fee_drev ?? 1.05,
      feeMkt: feeData?.fee_mkt ?? 1.08,
      shippingCost: feeData?.shipping_cost ?? 6.00,
      mediaworldPrepDays: feeData?.mediaworld_preparation_days ?? 3,
      epricePrepDays: feeData?.eprice_preparation_days ?? 1
    };

    // Create run record
    runId = crypto.randomUUID();
    startTime = Date.now();
    await supabase.from('sync_runs').insert({ 
      id: runId, started_at: new Date().toISOString(), status: 'running', 
      trigger_type: trigger, attempt: 1, steps: { current_step: 'import_ftp' }, metrics: initMetrics() 
    });
    console.log(`[orchestrator] Run created: ${runId}`);

    // ========== STEP 1: FTP Import ==========
    await updateCurrentStep(supabase, runId, 'import_ftp');
    console.log('[orchestrator] === STEP 1: FTP Import ===');
    
    const ftpStepStart = Date.now();
    for (const fileType of ['material', 'stock', 'price']) {
      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', startTime, 'Interrotta dall\'utente', { step: 'import_ftp' });
        return new Response(JSON.stringify({ status: 'cancelled' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      const result = await callStep(supabaseUrl, supabaseServiceKey, 'import-catalog-ftp', { fileType });
      if (!result.success) {
        await finalizeRun(supabase, runId, 'failed', startTime, `FTP ${fileType}: ${result.error}`, { step: 'import_ftp', fileType });
        return new Response(JSON.stringify({ status: 'failed', error: result.error }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
    }
    
    // Update step result
    const { data: currentRun } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
    await supabase.from('sync_runs').update({ 
      steps: { ...(currentRun?.steps || {}), import_ftp: { status: 'success', duration_ms: Date.now() - ftpStepStart } } 
    }).eq('id', runId);

    // ========== STEP 2-5: Parse, EAN, Pricing via step-runner ==========
    const stepRunnerSteps = ['parse_merge', 'ean_mapping', 'pricing'];
    
    for (const step of stepRunnerSteps) {
      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', startTime, 'Interrotta dall\'utente', { step });
        return new Response(JSON.stringify({ status: 'cancelled' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      await updateCurrentStep(supabase, runId, step);
      console.log(`[orchestrator] === STEP: ${step} ===`);
      
      const result = await callStep(supabaseUrl, supabaseServiceKey, 'sync-step-runner', { 
        run_id: runId, step, fee_config: feeConfig 
      });
      
      if (!result.success) {
        await finalizeRun(supabase, runId, 'failed', startTime, `${step}: ${result.error}`, { step });
        return new Response(JSON.stringify({ status: 'failed', error: result.error }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
    }

    // ========== STEP 5: Override (skipped) ==========
    await updateCurrentStep(supabase, runId, 'override');
    const { data: runForOverride } = await supabase.from('sync_runs').select('steps, metrics').eq('id', runId).single();
    await supabase.from('sync_runs').update({ 
      steps: { ...(runForOverride?.steps || {}), override: { status: 'skipped', duration_ms: 0, reason: 'Non implementato server-side' } },
      metrics: { ...(runForOverride?.metrics || {}), products_after_override: runForOverride?.metrics?.products_processed || 0 }
    }).eq('id', runId);

    // ========== STEP 6-8: Export steps ==========
    const exportSteps = ['export_ean', 'export_mediaworld', 'export_eprice'];
    
    for (const step of exportSteps) {
      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', startTime, 'Interrotta dall\'utente', { step });
        return new Response(JSON.stringify({ status: 'cancelled' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      await updateCurrentStep(supabase, runId, step);
      console.log(`[orchestrator] === STEP: ${step} ===`);
      
      const result = await callStep(supabaseUrl, supabaseServiceKey, 'sync-step-runner', { 
        run_id: runId, step, fee_config: feeConfig 
      });
      
      if (!result.success) {
        await finalizeRun(supabase, runId, 'failed', startTime, `${step}: ${result.error}`, { step });
        return new Response(JSON.stringify({ status: 'failed', error: result.error }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
    }

    // Update exported files count
    const { data: runForExport } = await supabase.from('sync_runs').select('metrics').eq('id', runId).single();
    await supabase.from('sync_runs').update({ 
      metrics: { ...(runForExport?.metrics || {}), exported_files_count: 3 }
    }).eq('id', runId);

    // ========== STEP 9: SFTP Upload ==========
    if (await isCancelRequested(supabase, runId)) {
      await finalizeRun(supabase, runId, 'failed', startTime, 'Interrotta dall\'utente', { step: 'upload_sftp' });
      return new Response(JSON.stringify({ status: 'cancelled' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }
    
    await updateCurrentStep(supabase, runId, 'upload_sftp');
    console.log('[orchestrator] === STEP 9: SFTP Upload ===');
    
    const sftpStepStart = Date.now();
    const sftpResult = await callStep(supabaseUrl, supabaseServiceKey, 'upload-exports-to-sftp', {
      files: [
        { bucket: 'exports', path: 'Catalogo EAN.csv', filename: 'Catalogo EAN.csv' },
        { bucket: 'exports', path: 'Export ePrice.csv', filename: 'Export ePrice.csv' },
        { bucket: 'exports', path: 'Export Mediaworld.csv', filename: 'Export Mediaworld.csv' }
      ]
    });
    
    if (!sftpResult.success) {
      await finalizeRun(supabase, runId, 'failed', startTime, `SFTP: ${sftpResult.error}`, { step: 'upload_sftp' });
      return new Response(JSON.stringify({ status: 'failed', error: sftpResult.error }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }
    
    const uploaded = sftpResult.data?.results?.filter((r: any) => r.uploaded).length || 0;
    const { data: runForSftp } = await supabase.from('sync_runs').select('steps, metrics').eq('id', runId).single();
    await supabase.from('sync_runs').update({ 
      steps: { ...(runForSftp?.steps || {}), upload_sftp: { status: 'success', duration_ms: Date.now() - sftpStepStart, uploaded } },
      metrics: { ...(runForSftp?.metrics || {}), sftp_uploaded_files: uploaded }
    }).eq('id', runId);

    // ========== SUCCESS ==========
    await finalizeRun(supabase, runId, 'success', startTime);
    
    const { data: finalRun } = await supabase.from('sync_runs').select('metrics').eq('id', runId).single();
    console.log(`[orchestrator] Pipeline completed successfully: ${runId}`);
    
    return new Response(JSON.stringify({ status: 'success', run_id: runId, metrics: finalRun?.metrics }), 
      { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

  } catch (err: any) {
    console.error('[orchestrator] Fatal error:', err);
    
    // Always try to finalize the run
    if (runId && supabase) {
      try {
        await finalizeRun(supabase, runId, 'failed', startTime, err.message, { fatal: true, error: err.message });
      } catch (finalizeErr) {
        console.error('[orchestrator] Failed to finalize run:', finalizeErr);
      }
    }
    
    return new Response(JSON.stringify({ status: 'error', message: err.message }), 
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
  }
});
