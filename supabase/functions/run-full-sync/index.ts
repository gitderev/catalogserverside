import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

/**
 * run-full-sync - ORCHESTRATORE LEGGERO
 * 
 * Non esegue logica di business, solo orchestrazione.
 * Chiama step separati tramite sync-step-runner per evitare WORKER_LIMIT.
 * 
 * Sequenza step:
 * 1. import_ftp - Download file da FTP (import-catalog-ftp)
 * 2. parse_merge - Parse e merge file (chunked)
 * 3. ean_mapping - Mapping EAN
 * 4. pricing - Calcolo prezzi (chunked)
 * 5. export_ean - Generazione catalogo EAN
 * 6. export_mediaworld - Export Mediaworld
 * 7. export_eprice - Export ePrice
 * 8. upload_sftp - Upload su SFTP (upload-exports-to-sftp)
 */

async function callStep(supabaseUrl: string, serviceKey: string, functionName: string, body: any): Promise<{ success: boolean; error?: string; data?: any }> {
  try {
    console.log(`[orchestrator] Calling ${functionName}...`);
    const resp = await fetch(`${supabaseUrl}/functions/v1/${functionName}`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${serviceKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    
    // Check HTTP status first
    if (!resp.ok) {
      const errorText = await resp.text().catch(() => 'Unknown error');
      console.log(`[orchestrator] ${functionName} HTTP error ${resp.status}: ${errorText}`);
      return { success: false, error: `HTTP ${resp.status}: ${errorText}` };
    }
    
    const data = await resp.json().catch(() => ({ status: 'error', message: 'Invalid JSON response' }));
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

// Verify step completion by checking the database
async function verifyStepCompleted(supabase: any, runId: string, stepName: string): Promise<{ success: boolean; error?: string }> {
  const { data: run } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
  const stepResult = run?.steps?.[stepName];
  
  if (!stepResult) {
    console.log(`[orchestrator] Step ${stepName} has no result in database - likely crashed`);
    return { success: false, error: `Step ${stepName} non ha prodotto risultati (possibile crash per memoria)` };
  }
  
  if (stepResult.status === 'failed') {
    console.log(`[orchestrator] Step ${stepName} failed according to database: ${stepResult.error}`);
    return { success: false, error: stepResult.error };
  }
  
  if (stepResult.status !== 'success') {
    console.log(`[orchestrator] Step ${stepName} has unexpected status: ${stepResult.status}`);
    return { success: false, error: `Step ${stepName} stato imprevisto: ${stepResult.status}` };
  }
  
  console.log(`[orchestrator] Step ${stepName} verified as successful in database`);
  return { success: true };
}

async function isCancelRequested(supabase: any, runId: string): Promise<boolean> {
  const { data } = await supabase.from('sync_runs').select('cancel_requested').eq('id', runId).single();
  return data?.cancel_requested === true;
}

async function updateRun(supabase: any, runId: string, updates: any): Promise<void> {
  await supabase.from('sync_runs').update(updates).eq('id', runId);
}

async function finalizeRun(supabase: any, runId: string, status: string, startTime: number, errorMessage?: string): Promise<void> {
  await supabase.from('sync_runs').update({
    status, 
    finished_at: new Date().toISOString(), 
    runtime_ms: Date.now() - startTime,
    error_message: errorMessage || null
  }).eq('id', runId);
  console.log(`[orchestrator] Run ${runId} finalized: ${status}`);
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
      trigger_type: trigger, attempt: 1, steps: { current_step: 'import_ftp' }, metrics: {} 
    });
    console.log(`[orchestrator] Run created: ${runId}`);

    // ========== STEP 1: FTP Import ==========
    await updateRun(supabase, runId, { steps: { current_step: 'import_ftp' } });
    console.log('[orchestrator] === STEP 1: FTP Import ===');
    
    for (const fileType of ['material', 'stock', 'price']) {
      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', startTime, 'Interrotta dall\'utente');
        return new Response(JSON.stringify({ status: 'cancelled' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      const result = await callStep(supabaseUrl, supabaseServiceKey, 'import-catalog-ftp', { fileType });
      if (!result.success) {
        await finalizeRun(supabase, runId, 'failed', startTime, `FTP ${fileType}: ${result.error}`);
        return new Response(JSON.stringify({ status: 'failed', error: result.error }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
    }

    // ========== STEPS 2-7: Processing via sync-step-runner (chunked) ==========
    const processingSteps = ['parse_merge', 'ean_mapping', 'pricing', 'export_ean', 'export_mediaworld', 'export_eprice'];
    
    for (const step of processingSteps) {
      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', startTime, 'Interrotta dall\'utente');
        return new Response(JSON.stringify({ status: 'cancelled' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      await updateRun(supabase, runId, { steps: { current_step: step } });
      console.log(`[orchestrator] === STEP: ${step} ===`);
      
      const result = await callStep(supabaseUrl, supabaseServiceKey, 'sync-step-runner', { 
        run_id: runId, step, fee_config: feeConfig 
      });
      
      // Even if HTTP call succeeded, verify the step actually completed in the database
      // This catches cases where the edge function crashed (e.g., memory limit)
      const verification = await verifyStepCompleted(supabase, runId, step);
      
      if (!result.success || !verification.success) {
        const error = result.error || verification.error || `Step ${step} fallito`;
        await finalizeRun(supabase, runId, 'failed', startTime, error);
        return new Response(JSON.stringify({ status: 'failed', error }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
    }

    // ========== STEP 8: SFTP Upload ==========
    if (await isCancelRequested(supabase, runId)) {
      await finalizeRun(supabase, runId, 'failed', startTime, 'Interrotta dall\'utente');
      return new Response(JSON.stringify({ status: 'cancelled' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }
    
    await updateRun(supabase, runId, { steps: { current_step: 'upload_sftp' } });
    console.log('[orchestrator] === STEP 8: SFTP Upload ===');
    
    const sftpResult = await callStep(supabaseUrl, supabaseServiceKey, 'upload-exports-to-sftp', {
      files: [
        { bucket: 'exports', path: 'Catalogo EAN.csv', filename: 'Catalogo EAN.csv' },
        { bucket: 'exports', path: 'Export ePrice.csv', filename: 'Export ePrice.csv' },
        { bucket: 'exports', path: 'Export Mediaworld.csv', filename: 'Export Mediaworld.csv' }
      ]
    });
    
    if (!sftpResult.success) {
      await finalizeRun(supabase, runId, 'failed', startTime, `SFTP: ${sftpResult.error}`);
      return new Response(JSON.stringify({ status: 'failed', error: sftpResult.error }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    // ========== SUCCESS ==========
    await finalizeRun(supabase, runId, 'success', startTime);
    console.log(`[orchestrator] Pipeline completed: ${runId}`);
    
    return new Response(JSON.stringify({ status: 'success', run_id: runId }), 
      { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

  } catch (err: any) {
    console.error('[orchestrator] Fatal error:', err);
    
    if (runId && supabase) {
      try {
        await finalizeRun(supabase, runId, 'failed', startTime, err.message);
      } catch (e) {
        console.error('[orchestrator] Failed to finalize:', e);
      }
    }
    
    return new Response(JSON.stringify({ status: 'error', message: err.message }), 
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
  }
});
