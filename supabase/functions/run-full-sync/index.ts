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
 * IMPORTANTE: parse_merge è CHUNKED - può richiedere più invocazioni.
 * L'orchestratore chiama parse_merge in loop finché non è completed.
 * 
 * Sequenza step:
 * 1. import_ftp - Download file da FTP (import-catalog-ftp)
 * 2. parse_merge - Parse e merge file (CHUNKED - loop finché completed)
 * 3. ean_mapping - Mapping EAN
 * 4. pricing - Calcolo prezzi
 * 5. export_ean - Generazione catalogo EAN
 * 6. export_mediaworld - Export Mediaworld
 * 7. export_eprice - Export ePrice
 * 8. upload_sftp - Upload su SFTP (upload-exports-to-sftp)
 */

const MAX_PARSE_MERGE_CHUNKS = 100; // Safety limit: max chunks per parse_merge

// All pipeline steps in order
const ALL_STEPS = ['import_ftp', 'parse_merge', 'ean_mapping', 'pricing', 'export_ean', 'export_mediaworld', 'export_eprice', 'upload_sftp'];

async function callStep(supabaseUrl: string, serviceKey: string, functionName: string, body: any): Promise<{ success: boolean; error?: string; data?: any }> {
  try {
    console.log(`[orchestrator] Calling ${functionName}...`);
    const resp = await fetch(`${supabaseUrl}/functions/v1/${functionName}`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${serviceKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    
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
    console.log(`[orchestrator] ${functionName} completed, step_status=${data.step_status || 'N/A'}`);
    return { success: true, data };
  } catch (e: any) {
    console.error(`[orchestrator] Error calling ${functionName}:`, e);
    return { success: false, error: e.message };
  }
}

async function verifyStepCompleted(supabase: any, runId: string, stepName: string): Promise<{ success: boolean; status?: string; error?: string }> {
  const { data: run } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
  const stepResult = run?.steps?.[stepName];
  
  if (!stepResult) {
    console.log(`[orchestrator] Step ${stepName} has no result in database - likely crashed`);
    return { success: false, error: `Step ${stepName} non ha prodotto risultati (possibile crash per memoria)` };
  }
  
  if (stepResult.status === 'failed') {
    console.log(`[orchestrator] Step ${stepName} failed: ${stepResult.error}`);
    return { success: false, status: 'failed', error: stepResult.error };
  }
  
  // For parse_merge: handle intermediate phases as "still in progress"
  const intermediatePhases = ['building_stock_index', 'building_price_index', 'preparing_material', 'in_progress'];
  if (intermediatePhases.includes(stepResult.status)) {
    console.log(`[orchestrator] Step ${stepName} is ${stepResult.status} (needs more invocations)`);
    return { success: true, status: 'in_progress' };
  }
  
  if (stepResult.status === 'completed' || stepResult.status === 'success') {
    console.log(`[orchestrator] Step ${stepName} verified as completed`);
    return { success: true, status: 'completed' };
  }
  
  // 'pending' at this point means step didn't start processing yet - should continue
  if (stepResult.status === 'pending') {
    console.log(`[orchestrator] Step ${stepName} is pending (first invocation needed)`);
    return { success: true, status: 'in_progress' };
  }
  
  console.log(`[orchestrator] Step ${stepName} has unexpected status: ${stepResult.status}`);
  return { success: false, error: `Step ${stepName} stato imprevisto: ${stepResult.status}` };
}

async function isCancelRequested(supabase: any, runId: string): Promise<boolean> {
  const { data } = await supabase.from('sync_runs').select('cancel_requested').eq('id', runId).single();
  return data?.cancel_requested === true;
}

/**
 * CRITICAL: updateRunSteps - Merges step updates without overwriting existing steps
 * This prevents losing step results when updating current_step
 */
async function updateRunSteps(supabase: any, runId: string, partialSteps: Record<string, any>): Promise<void> {
  const { data: run } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
  const existingSteps = run?.steps || {};
  const merged = { ...existingSteps, ...partialSteps };
  await supabase.from('sync_runs').update({ steps: merged }).eq('id', runId);
  console.log(`[orchestrator] Steps updated (merged): current_step=${partialSteps.current_step || 'unchanged'}`);
}

async function updateRun(supabase: any, runId: string, updates: any): Promise<void> {
  await supabase.from('sync_runs').update(updates).eq('id', runId);
}

async function finalizeRun(supabase: any, runId: string, status: string, startTime: number, errorMessage?: string, errorDetails?: any): Promise<void> {
  const updates: any = {
    status, 
    finished_at: new Date().toISOString(), 
    runtime_ms: Date.now() - startTime,
    error_message: errorMessage || null
  };
  
  if (errorDetails) {
    updates.error_details = errorDetails;
  }
  
  await supabase.from('sync_runs').update(updates).eq('id', runId);
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

    const { data: runningJobs } = await supabase.from('sync_runs').select('id').eq('status', 'running').limit(1);
    if (runningJobs?.length) {
      return new Response(JSON.stringify({ status: 'error', message: 'Sync già in corso' }), 
        { status: 409, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

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

    // ========== FAIL-FAST VALIDATION: fee_config fields ==========
    const { data: feeData, error: feeError } = await supabase.from('fee_config').select('*').limit(1).single();
    
    if (feeError || !feeData) {
      console.error('[orchestrator] FAIL-FAST: fee_config not found or error:', feeError);
      return new Response(JSON.stringify({ 
        status: 'error', 
        message: 'Configurazione fee_config mancante. Impossibile procedere.' 
      }), { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }
    
    // FAIL-FAST: Validate required IT/EU fields for Mediaworld
    const mediaworldValidation = {
      includeEu: feeData.mediaworld_include_eu,
      itDays: feeData.mediaworld_it_preparation_days,
      euDays: feeData.mediaworld_eu_preparation_days
    };
    
    if (typeof mediaworldValidation.includeEu !== 'boolean') {
      console.error('[orchestrator] FAIL-FAST: mediaworld_include_eu non definito o non boolean');
      return new Response(JSON.stringify({ 
        status: 'error', 
        message: 'FAIL-FAST: mediaworld_include_eu deve essere definito (true/false) in fee_config.' 
      }), { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }
    
    if (typeof mediaworldValidation.itDays !== 'number' || !Number.isFinite(mediaworldValidation.itDays)) {
      console.error('[orchestrator] FAIL-FAST: mediaworld_it_preparation_days non definito o non numerico');
      return new Response(JSON.stringify({ 
        status: 'error', 
        message: 'FAIL-FAST: mediaworld_it_preparation_days deve essere un numero in fee_config.' 
      }), { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }
    
    if (typeof mediaworldValidation.euDays !== 'number' || !Number.isFinite(mediaworldValidation.euDays)) {
      console.error('[orchestrator] FAIL-FAST: mediaworld_eu_preparation_days non definito o non numerico');
      return new Response(JSON.stringify({ 
        status: 'error', 
        message: 'FAIL-FAST: mediaworld_eu_preparation_days deve essere un numero in fee_config.' 
      }), { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }
    
    // FAIL-FAST: Validate required IT/EU fields for ePrice
    const epriceValidation = {
      includeEu: feeData.eprice_include_eu,
      itDays: feeData.eprice_it_preparation_days,
      euDays: feeData.eprice_eu_preparation_days
    };
    
    if (typeof epriceValidation.includeEu !== 'boolean') {
      console.error('[orchestrator] FAIL-FAST: eprice_include_eu non definito o non boolean');
      return new Response(JSON.stringify({ 
        status: 'error', 
        message: 'FAIL-FAST: eprice_include_eu deve essere definito (true/false) in fee_config.' 
      }), { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }
    
    if (typeof epriceValidation.itDays !== 'number' || !Number.isFinite(epriceValidation.itDays)) {
      console.error('[orchestrator] FAIL-FAST: eprice_it_preparation_days non definito o non numerico');
      return new Response(JSON.stringify({ 
        status: 'error', 
        message: 'FAIL-FAST: eprice_it_preparation_days deve essere un numero in fee_config.' 
      }), { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }
    
    if (typeof epriceValidation.euDays !== 'number' || !Number.isFinite(epriceValidation.euDays)) {
      console.error('[orchestrator] FAIL-FAST: eprice_eu_preparation_days non definito o non numerico');
      return new Response(JSON.stringify({ 
        status: 'error', 
        message: 'FAIL-FAST: eprice_eu_preparation_days deve essere un numero in fee_config.' 
      }), { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }
    
    console.log('[orchestrator] FAIL-FAST validation passed:', {
      mediaworld: mediaworldValidation,
      eprice: epriceValidation
    });
    
    // Build fee config from validated data (NO defaults for IT/EU fields)
    const feeConfig = {
      feeDrev: feeData.fee_drev ?? 1.05,
      feeMkt: feeData.fee_mkt ?? 1.08,
      shippingCost: feeData.shipping_cost ?? 6.00,
      mediaworldPrepDays: feeData.mediaworld_preparation_days ?? 3,
      epricePrepDays: feeData.eprice_preparation_days ?? 1,
      // IT/EU stock config - NO DEFAULTS (fail-fast validated above)
      mediaworldIncludeEu: feeData.mediaworld_include_eu,
      mediaworldItPrepDays: feeData.mediaworld_it_preparation_days,
      mediaworldEuPrepDays: feeData.mediaworld_eu_preparation_days,
      epriceIncludeEu: feeData.eprice_include_eu,
      epriceItPrepDays: feeData.eprice_it_preparation_days,
      epriceEuPrepDays: feeData.eprice_eu_preparation_days
    };

    runId = crypto.randomUUID();
    startTime = Date.now();
    
    // Initialize all steps as pending for UI visibility
    const initialSteps: Record<string, any> = { current_step: 'import_ftp' };
    for (const step of ALL_STEPS) {
      initialSteps[step] = { status: 'pending' };
    }
    
    await supabase.from('sync_runs').insert({ 
      id: runId, started_at: new Date().toISOString(), status: 'running', 
      trigger_type: trigger, attempt: 1, 
      steps: initialSteps, 
      metrics: {},
      location_warnings: {},
      error_details: null
    });
    console.log(`[orchestrator] Run created: ${runId}`);

    // Return immediately with run_id so UI can start polling
    const responseJson = JSON.stringify({ 
      run_id: runId, 
      queued: false, 
      status: 'running', 
      message: 'Pipeline avviata' 
    });
    const immediateResponse = new Response(responseJson, { 
      status: 200, 
      headers: { ...corsHeaders, 'Content-Type': 'application/json' } 
    });

    // Continue processing in background using waitUntil
    const processPipeline = async () => {
      try {
        // ========== STEP 1: FTP Import (including stock location) ==========
        await updateRunSteps(supabase, runId!, { current_step: 'import_ftp' });
        console.log('[orchestrator] === STEP 1: FTP Import ===');
        
        for (const fileType of ['material', 'stock', 'price', 'stockLocation']) {
          if (await isCancelRequested(supabase, runId!)) {
            await finalizeRun(supabase, runId!, 'failed', startTime, 'Interrotta dall\'utente');
            return;
          }
          
          const result = await callStep(supabaseUrl, supabaseServiceKey, 'import-catalog-ftp', { 
            fileType,
            run_id: runId
          });
          
          if (!result.success && fileType !== 'stockLocation') {
            const errorDetails = { step: 'import_ftp', fileType, error: result.error };
            await updateRunSteps(supabase, runId!, { import_ftp: { status: 'failed', error: result.error, details: errorDetails } });
            await finalizeRun(supabase, runId!, 'failed', startTime, `FTP ${fileType}: ${result.error}`, errorDetails);
            return;
          }
          
          if (!result.success && fileType === 'stockLocation') {
            console.log(`[orchestrator] Stock location import failed (non-blocking): ${result.error}`);
            const { data: run } = await supabase.from('sync_runs').select('location_warnings').eq('id', runId).single();
            const warnings = run?.location_warnings || {};
            warnings.missing_location_file = 1;
            await supabase.from('sync_runs').update({ location_warnings: warnings }).eq('id', runId);
          }
        }
        
        // Mark import_ftp as completed
        await updateRunSteps(supabase, runId!, { import_ftp: { status: 'completed' } });

        // ========== STEP 2: PARSE_MERGE (CHUNKED) ==========
        await updateRunSteps(supabase, runId!, { current_step: 'parse_merge' });
        console.log('[orchestrator] === STEP 2: parse_merge (CHUNKED) ===');
        
        let parseMergeComplete = false;
        let chunkCount = 0;
        
        while (!parseMergeComplete && chunkCount < MAX_PARSE_MERGE_CHUNKS) {
          if (await isCancelRequested(supabase, runId!)) {
            await finalizeRun(supabase, runId!, 'failed', startTime, 'Interrotta dall\'utente');
            return;
          }
          
          chunkCount++;
          console.log(`[orchestrator] parse_merge chunk ${chunkCount}/${MAX_PARSE_MERGE_CHUNKS}...`);
          
          const result = await callStep(supabaseUrl, supabaseServiceKey, 'sync-step-runner', { 
            run_id: runId, step: 'parse_merge', fee_config: feeConfig 
          });
          
          if (!result.success) {
            const errorDetails = { step: 'parse_merge', chunk: chunkCount, error: result.error };
            await finalizeRun(supabase, runId!, 'failed', startTime, `parse_merge chunk ${chunkCount}: ${result.error}`, errorDetails);
            return;
          }
          
          const verification = await verifyStepCompleted(supabase, runId!, 'parse_merge');
          
          if (!verification.success) {
            const errorDetails = { step: 'parse_merge', chunk: chunkCount, verification_error: verification.error };
            await finalizeRun(supabase, runId!, 'failed', startTime, verification.error || 'parse_merge verification failed', errorDetails);
            return;
          }
          
          if (verification.status === 'completed') {
            parseMergeComplete = true;
            console.log(`[orchestrator] parse_merge completed after ${chunkCount} chunks`);
          } else if (verification.status === 'in_progress') {
            console.log(`[orchestrator] parse_merge in_progress, continuing to next chunk...`);
          } else {
            const errorDetails = { step: 'parse_merge', unexpected_status: verification.status };
            await finalizeRun(supabase, runId!, 'failed', startTime, `parse_merge unexpected status: ${verification.status}`, errorDetails);
            return;
          }
        }
        
        if (!parseMergeComplete) {
          const errorDetails = { step: 'parse_merge', chunks: chunkCount, limit: MAX_PARSE_MERGE_CHUNKS };
          await finalizeRun(supabase, runId!, 'failed', startTime, `parse_merge exceeded ${MAX_PARSE_MERGE_CHUNKS} chunks limit`, errorDetails);
          return;
        }

        // ========== STEPS 3-7: Processing via sync-step-runner ==========
        const remainingSteps = ['ean_mapping', 'pricing', 'export_ean', 'export_mediaworld', 'export_eprice'];
        
        for (const step of remainingSteps) {
          if (await isCancelRequested(supabase, runId!)) {
            await finalizeRun(supabase, runId!, 'failed', startTime, 'Interrotta dall\'utente');
            return;
          }
          
          await updateRunSteps(supabase, runId!, { current_step: step });
          console.log(`[orchestrator] === STEP: ${step} ===`);
          
          const result = await callStep(supabaseUrl, supabaseServiceKey, 'sync-step-runner', { 
            run_id: runId, step, fee_config: feeConfig 
          });
          
          const verification = await verifyStepCompleted(supabase, runId!, step);
          
          if (!result.success || !verification.success) {
            const error = result.error || verification.error || `Step ${step} fallito`;
            const errorDetails = { step, call_error: result.error, verification_error: verification.error };
            await finalizeRun(supabase, runId!, 'failed', startTime, error, errorDetails);
            return;
          }
        }

        // ========== STEP 8: SFTP Upload ==========
        if (await isCancelRequested(supabase, runId!)) {
          await finalizeRun(supabase, runId!, 'failed', startTime, 'Interrotta dall\'utente');
          return;
        }
        
        await updateRunSteps(supabase, runId!, { current_step: 'upload_sftp' });
        console.log('[orchestrator] === STEP 8: SFTP Upload ===');
        
        // Read actual export file paths from sync_runs.steps.exports.files
        const { data: runData } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
        const exportFiles = runData?.steps?.exports?.files || {};
        
        // Build files array from actual paths, with fallback to legacy paths
        const files = [
          { 
            bucket: 'exports', 
            path: (exportFiles.ean || 'Catalogo EAN.xlsx').replace(/^exports\//, ''), 
            filename: 'Catalogo EAN.xlsx' 
          },
          { 
            bucket: 'exports', 
            path: (exportFiles.eprice || 'Export ePrice.xlsx').replace(/^exports\//, ''), 
            filename: 'Export ePrice.xlsx' 
          },
          { 
            bucket: 'exports', 
            path: (exportFiles.mediaworld || 'Export Mediaworld.xlsx').replace(/^exports\//, ''), 
            filename: 'Export Mediaworld.xlsx' 
          }
        ];
        
        console.log(`[orchestrator] SFTP files from steps.exports.files:`, files.map(f => f.path));
        
        const sftpResult = await callStep(supabaseUrl, supabaseServiceKey, 'upload-exports-to-sftp', { files });
        
        if (!sftpResult.success) {
          const errorDetails = { step: 'upload_sftp', files, error: sftpResult.error };
          await updateRunSteps(supabase, runId!, { upload_sftp: { status: 'failed', error: sftpResult.error, details: errorDetails } });
          await finalizeRun(supabase, runId!, 'failed', startTime, `SFTP: ${sftpResult.error}`, errorDetails);
          return;
        }
        
        // Mark upload_sftp as completed
        await updateRunSteps(supabase, runId!, { upload_sftp: { status: 'completed' } });

        // ========== SUCCESS ==========
        await finalizeRun(supabase, runId!, 'success', startTime);
        console.log(`[orchestrator] Pipeline completed: ${runId}`);
        
      } catch (err: any) {
        console.error('[orchestrator] Pipeline error:', err);
        if (runId) {
          try {
            const errorDetails = { step: 'unknown', exception: err.message, stack: err.stack };
            await finalizeRun(supabase, runId, 'failed', startTime, err.message, errorDetails);
          } catch (e) {
            console.error('[orchestrator] Failed to finalize:', e);
          }
        }
      }
    };

    // Use EdgeRuntime.waitUntil if available, otherwise run synchronously
    // @ts-ignore - EdgeRuntime may not be typed
    if (typeof EdgeRuntime !== 'undefined' && EdgeRuntime.waitUntil) {
      // @ts-ignore
      EdgeRuntime.waitUntil(processPipeline());
    } else {
      // Fallback: fire-and-forget (don't await)
      processPipeline().catch(err => console.error('[orchestrator] Background error:', err));
    }

    return immediateResponse;

  } catch (err: any) {
    console.error('[orchestrator] Fatal error:', err);
    
    if (runId && supabase) {
      try {
        const errorDetails = { fatal: true, exception: err.message, stack: err.stack };
        await finalizeRun(supabase, runId, 'failed', startTime, err.message, errorDetails);
      } catch (e) {
        console.error('[orchestrator] Failed to finalize:', e);
      }
    }
    
    return new Response(JSON.stringify({ 
      error: err.message, 
      details: 'Errore durante l\'avvio della pipeline',
      run_id: runId || null
    }), { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
  }
});
