import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";

type SupabaseClient = ReturnType<typeof createClient>;

function errMsg(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

/**
 * run-full-sync - ORCHESTRATORE LEGGERO CON RESUME via cron-tick
 * 
 * Non esegue logica di business, solo orchestrazione.
 * Chiama step separati tramite sync-step-runner per evitare WORKER_LIMIT.
 * 
 * RESUME: Se parse_merge (o altri step chunked) richiedono più di
 * ORCHESTRATOR_BUDGET_MS, l'orchestratore YIELD, RILASCIA IL LOCK,
 * e restituisce { status: 'yielded', run_id, needs_resume: true }.
 * cron-tick (ogni 5 min) rileva la run in stato running e la riprende.
 * 
 * LOCK POLICY: acquire a inizio invocazione, release SEMPRE in finally
 * (anche su yield e error). Nessun lock mantenuto tra invocazioni.
 * 
 * SFTP upload: SOLO per trigger === 'cron'.
 * File SFTP: Export Mediaworld.csv, Export ePrice.csv, catalogo_ean.xlsx,
 *            amazon_listing_loader.xlsm, amazon_price_inventory.txt
 * 
 * Storage versioning: latest/ (overwrite) + versions/<timestamp>/ (storico)
 * Retention: per ciascuno dei 5 file, max 3 versioni; elimina >3 solo se >7 giorni.
 * 
 * success_with_warning: basato SOLO su warning_count > 0.
 */

// Time budget for this Edge Function invocation. Must be well under
// the Supabase Edge Function wall-clock limit (~60s).
// After this, we yield and return needs_resume: true.
const ORCHESTRATOR_BUDGET_MS = 25_000; // 25s

const MAX_PARSE_MERGE_CHUNKS = 100;

// The 5 canonical export files
const EXPORT_FILES = [
  'Export Mediaworld.csv',
  'Export ePrice.csv',
  'catalogo_ean.xlsx',
  'amazon_listing_loader.xlsm',
  'amazon_price_inventory.txt'
];

async function callStep(supabaseUrl: string, serviceKey: string, functionName: string, body: Record<string, unknown>): Promise<{ success: boolean; error?: string; data?: Record<string, unknown>; httpStatus?: number }> {
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
      
      if (resp.status === 546 || errorText.includes('WORKER_LIMIT')) {
        const supabase = createClient(supabaseUrl, serviceKey);
        const runId = body.run_id as string;
        if (runId) {
          try {
            await supabase.rpc('log_sync_event', {
              p_run_id: runId,
              p_level: 'ERROR',
              p_message: `WORKER_LIMIT in ${functionName}: HTTP ${resp.status}`,
              p_details: { step: body.step || functionName, chunk_index: body.chunk_index, http_status: resp.status, suggestion: 'ridurre CHUNK_LINES o ottimizzare chunking' }
            });
          } catch (logErr) {
            console.warn(`[orchestrator] Non-blocking log_sync_event error:`, logErr);
          }
        }
      }
      
      return { success: false, error: `HTTP ${resp.status}: ${errorText}`, httpStatus: resp.status };
    }
    
    const data = await resp.json().catch(() => ({ status: 'error', message: 'Invalid JSON response' }));
    if (data.status === 'error') {
      console.log(`[orchestrator] ${functionName} failed: ${data.message || data.error}`);
      return { success: false, error: data.message || data.error, data, httpStatus: resp.status };
    }
    console.log(`[orchestrator] ${functionName} completed, step_status=${data.step_status || 'N/A'}`);
    return { success: true, data, httpStatus: resp.status };
  } catch (e: unknown) {
    console.error(`[orchestrator] Error calling ${functionName}:`, e);
    return { success: false, error: errMsg(e) };
  }
}

async function verifyStepCompleted(supabase: SupabaseClient, runId: string, stepName: string): Promise<{ success: boolean; status?: string; error?: string }> {
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
  
  const intermediatePhases = ['building_stock_index', 'building_price_index', 'preparing_material', 'in_progress', 'finalizing'];
  if (intermediatePhases.includes(stepResult.status)) {
    console.log(`[orchestrator] Step ${stepName} is ${stepResult.status} (needs more invocations)`);
    return { success: true, status: 'in_progress' };
  }
  
  if (stepResult.status === 'completed' || stepResult.status === 'success') {
    console.log(`[orchestrator] Step ${stepName} verified as completed`);
    return { success: true, status: 'completed' };
  }
  
  if (stepResult.status === 'pending') {
    console.log(`[orchestrator] Step ${stepName} is pending (first invocation needed)`);
    return { success: true, status: 'in_progress' };
  }
  
  console.log(`[orchestrator] Step ${stepName} has unexpected status: ${stepResult.status}`);
  return { success: false, error: `Step ${stepName} stato imprevisto: ${stepResult.status}` };
}

async function isCancelRequested(supabase: SupabaseClient, runId: string): Promise<boolean> {
  const { data } = await supabase.from('sync_runs').select('cancel_requested').eq('id', runId).single();
  return data?.cancel_requested === true;
}

async function updateRun(supabase: SupabaseClient, runId: string, updates: Record<string, unknown>): Promise<void> {
  await supabase.from('sync_runs').update(updates).eq('id', runId);
}

/** Update only current_step inside steps JSON without wiping persisted step state */
async function updateCurrentStep(supabase: SupabaseClient, runId: string, stepName: string): Promise<void> {
  const { data: run } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
  const currentSteps = run?.steps || {};
  const updatedSteps = { ...currentSteps, current_step: stepName };
  await supabase.from('sync_runs').update({ steps: updatedSteps }).eq('id', runId);
}

async function finalizeRun(supabase: SupabaseClient, runId: string, status: string, startTime: number, errorMessage?: string): Promise<void> {
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
  let supabase: SupabaseClient | null = null;
  const orchestratorStart = Date.now();

  try {
    const body = await req.json().catch(() => ({}));
    const trigger = body.trigger as string;
    const attemptNumber = (body.attempt as number) || 1;
    const resumeRunId = body.resume_run_id as string | undefined;
    
    if (!trigger || !['cron', 'manual'].includes(trigger)) {
      return new Response(JSON.stringify({ status: 'error', message: 'trigger deve essere cron o manual' }), 
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    supabase = createClient(supabaseUrl, supabaseServiceKey);

    // Load sync_config for run_timeout_minutes
    const { data: syncConfig } = await supabase.from('sync_config').select('run_timeout_minutes').eq('id', 1).single();
    const runTimeoutMinutes = syncConfig?.run_timeout_minutes || 60;
    // Lock TTL: just enough for this invocation (60s), NOT the full run timeout.
    // Lock is released in finally and re-acquired at next invocation.
    const lockTtlSeconds = 120; // 2min safety margin over 60s edge function limit

    // ========== RESUME MODE ==========
    if (resumeRunId) {
      console.log(`[orchestrator] RESUME mode for run ${resumeRunId}, trigger: ${trigger}`);
      runId = resumeRunId;
      
      // Verify the run exists and is still running
      const { data: existingRun, error: runErr } = await supabase
        .from('sync_runs')
        .select('status, steps, started_at, trigger_type')
        .eq('id', runId)
        .single();
      
      if (runErr || !existingRun) {
        return new Response(JSON.stringify({ status: 'error', message: `Run ${runId} non trovata` }), 
          { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      if (existingRun.status !== 'running') {
        console.log(`[orchestrator] Run ${runId} is ${existingRun.status}, not running. Cannot resume.`);
        return new Response(JSON.stringify({ status: 'already_finished', run_status: existingRun.status }), 
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      // Acquire lock (fresh, not renew - lock was released after last yield)
      const { data: lockResult } = await supabase.rpc('try_acquire_sync_lock', {
        p_lock_name: 'global_sync',
        p_run_id: runId,
        p_ttl_seconds: lockTtlSeconds
      });
      
      if (!lockResult) {
        // Lock held by someone else - check who
        const { data: existingLock } = await supabase
          .from('sync_locks')
          .select('run_id, lease_until')
          .eq('lock_name', 'global_sync')
          .maybeSingle();
        
        console.log(`[orchestrator] Cannot acquire lock for resume: held by ${existingLock?.run_id}`);
        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId,
            p_level: 'INFO',
            p_message: 'locked',
            p_details: { 
              step: 'orchestrator_resume', 
              lock_name: 'global_sync',
              owner_run_id: existingLock?.run_id, 
              lease_until: existingLock?.lease_until,
              origin: trigger,
              reason: 'lock_held_by_other'
            }
          });
        } catch (_) { /* non-blocking */ }
        return new Response(JSON.stringify({ 
          status: 'locked', message: 'resume_locked_skip', 
          run_id: runId,
          owner_run_id: existingLock?.run_id, 
          lease_until: existingLock?.lease_until 
        }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      startTime = new Date(existingRun.started_at).getTime();
      const currentStep = existingRun.steps?.current_step || 'parse_merge';
      console.log(`[orchestrator] Resuming run ${runId} at step: ${currentStep}, lock acquired`);
      
      // Log resume event
      try {
        const stepState = existingRun.steps?.[currentStep] || {};
        await supabase.rpc('log_sync_event', {
          p_run_id: runId,
          p_level: 'INFO',
          p_message: 'resume_triggered',
          p_details: { 
            step: currentStep, 
            origin: trigger, 
            elapsed_since_start_ms: Date.now() - startTime,
            cursor_pos: stepState.cursor_pos,
            chunk_index: stepState.chunk_index,
            file_total_size: stepState.materialBytes
          }
        });
      } catch (_) { /* non-blocking */ }
      
      // Load fee_config for steps that need it
      const FEE_CONFIG_SINGLETON_ID = '00000000-0000-0000-0000-000000000001';
      const { data: feeData } = await supabase.from('fee_config').select('*').eq('id', FEE_CONFIG_SINGLETON_ID).maybeSingle();
      const feeConfig = buildFeeConfig(feeData);
      
      // Resume the pipeline from the current step
      return await runPipeline(supabase, supabaseUrl, supabaseServiceKey, runId, existingRun.trigger_type || trigger, feeConfig, startTime, currentStep, orchestratorStart);
    }

    // ========== NEW RUN MODE ==========
    console.log(`[orchestrator] Starting NEW pipeline, trigger: ${trigger}`);

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
      
      // Check if there's already a running run - resume it instead of creating new
      const { data: existingRunning } = await supabase
        .from('sync_runs')
        .select('id, steps, started_at, trigger_type')
        .eq('status', 'running')
        .limit(1);
      
      if (existingRunning?.length) {
        const existing = existingRunning[0];
        console.log(`[orchestrator] Found existing running run ${existing.id}, resuming instead of creating new`);
        runId = existing.id;
        
        // Try to acquire lock
        const { data: lockOk } = await supabase.rpc('try_acquire_sync_lock', {
          p_lock_name: 'global_sync',
          p_run_id: runId,
          p_ttl_seconds: lockTtlSeconds
        });
        
        if (!lockOk) {
          // Lock held - check if by same run (renew) or different
          const { data: lockUpdate } = await supabase
            .from('sync_locks')
            .update({ lease_until: new Date(Date.now() + lockTtlSeconds * 1000).toISOString(), updated_at: new Date().toISOString() })
            .eq('lock_name', 'global_sync')
            .eq('run_id', runId)
            .select('lock_name')
            .maybeSingle();
          
          if (!lockUpdate) {
            try {
              await supabase.rpc('log_sync_event', {
                p_run_id: runId,
                p_level: 'INFO',
                p_message: 'start_resuming_existing_run: lock occupato, resume delegato a cron-tick',
                p_details: { origin: 'manual' }
              });
            } catch (_) {}
            return new Response(JSON.stringify({ status: 'locked', message: 'run in corso, resume delegato a cron-tick', run_id: runId }), 
              { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
          }
        }
        
        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId,
            p_level: 'INFO',
            p_message: 'start_resuming_existing_run',
            p_details: { origin: 'manual', step: existing.steps?.current_step }
          });
        } catch (_) {}
        
        startTime = new Date(existing.started_at).getTime();
        const currentStep = existing.steps?.current_step || 'parse_merge';
        
        const FEE_CONFIG_SINGLETON_ID = '00000000-0000-0000-0000-000000000001';
        const { data: feeData } = await supabase.from('fee_config').select('*').eq('id', FEE_CONFIG_SINGLETON_ID).maybeSingle();
        const feeConfig = buildFeeConfig(feeData);
        
        return await runPipeline(supabase, supabaseUrl, supabaseServiceKey, runId, existing.trigger_type || trigger, feeConfig, startTime, currentStep, orchestratorStart);
      }
    }

    runId = crypto.randomUUID();
    
    const { data: lockResult } = await supabase.rpc('try_acquire_sync_lock', {
      p_lock_name: 'global_sync',
      p_run_id: runId,
      p_ttl_seconds: lockTtlSeconds
    });
    
    if (!lockResult) {
      console.log('[orchestrator] INFO: Lock not acquired, sync already in progress');
      runId = null;
      return new Response(JSON.stringify({ status: 'locked', message: 'not_started', run_id: null }), 
        { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    console.log(`[orchestrator] Lock acquired for run ${runId}`);

    const FEE_CONFIG_SINGLETON_ID = '00000000-0000-0000-0000-000000000001';
    const { data: feeData } = await supabase.from('fee_config').select('*').eq('id', FEE_CONFIG_SINGLETON_ID).maybeSingle();
    const feeConfig = buildFeeConfig(feeData);

    startTime = Date.now();
    await supabase.from('sync_runs').insert({ 
      id: runId, started_at: new Date().toISOString(), status: 'running', 
      trigger_type: trigger, attempt: attemptNumber, steps: { current_step: 'import_ftp' }, metrics: {},
      location_warnings: {}, warning_count: 0, file_manifest: {}
    });
    console.log(`[orchestrator] Run created: ${runId}`);
    
    try {
      await supabase.rpc('log_sync_event', {
        p_run_id: runId,
        p_level: 'INFO',
        p_message: 'start_created_run',
        p_details: { origin: trigger, attempt: attemptNumber }
      });
    } catch (_) {}

    return await runPipeline(supabase, supabaseUrl, supabaseServiceKey, runId, trigger, feeConfig, startTime, 'import_ftp', orchestratorStart);

  } catch (err: unknown) {
    console.error('[orchestrator] Fatal error:', err);
    
    if (runId && supabase) {
      try {
        await finalizeRun(supabase, runId, 'failed', startTime, errMsg(err));
        await fetch(`${supabaseUrl}/functions/v1/send-sync-notification`, {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${supabaseServiceKey}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ run_id: runId, status: 'failed' })
        }).catch(() => {});
      } catch (e: unknown) {
        console.error('[orchestrator] Failed to finalize:', e);
      }
    }
    
    return new Response(JSON.stringify({ status: 'error', message: errMsg(err) }), 
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
  } finally {
    // ALWAYS release lock - even on yield, error, success
    if (runId && supabase) {
      try {
        const { data: released } = await supabase.rpc('release_sync_lock', {
          p_lock_name: 'global_sync',
          p_run_id: runId
        });
        if (released) {
          console.log(`[orchestrator] Lock released for run ${runId}`);
        } else {
          console.log(`[orchestrator] Lock release: no lock found for run ${runId} (already released or expired)`);
        }
      } catch (e: unknown) {
        console.warn(`[orchestrator] WARN: Failed to release lock: ${errMsg(e)}`);
      }
    }
  }
});

// ============================================================
// PIPELINE EXECUTION (shared between new run and resume)
// ============================================================

function buildFeeConfig(feeData: Record<string, unknown> | null): Record<string, unknown> {
  return {
    feeDrev: feeData?.fee_drev ?? 1.05,
    feeMkt: feeData?.fee_mkt ?? 1.08,
    shippingCost: feeData?.shipping_cost ?? 6.00,
    mediaworldPrepDays: feeData?.mediaworld_preparation_days ?? 3,
    epricePrepDays: feeData?.eprice_preparation_days ?? 1,
    mediaworldIncludeEu: feeData?.mediaworld_include_eu ?? false,
    mediaworldItPrepDays: feeData?.mediaworld_it_preparation_days ?? 3,
    mediaworldEuPrepDays: feeData?.mediaworld_eu_preparation_days ?? 5,
    epriceIncludeEu: feeData?.eprice_include_eu ?? false,
    epriceItPrepDays: feeData?.eprice_it_preparation_days ?? 1,
    epriceEuPrepDays: feeData?.eprice_eu_preparation_days ?? 3,
    eanFeeDrev: feeData?.ean_fee_drev ?? null,
    eanFeeMkt: feeData?.ean_fee_mkt ?? null,
    eanShippingCost: feeData?.ean_shipping_cost ?? null,
    mediaworldFeeDrev: feeData?.mediaworld_fee_drev ?? null,
    mediaworldFeeMkt: feeData?.mediaworld_fee_mkt ?? null,
    mediaworldShippingCost: feeData?.mediaworld_shipping_cost ?? null,
    epriceFeeDrev: feeData?.eprice_fee_drev ?? null,
    epriceFeeMkt: feeData?.eprice_fee_mkt ?? null,
    epriceShippingCost: feeData?.eprice_shipping_cost ?? null
  };
}

// Steps in pipeline order. parse_merge is special (chunked).
const ALL_STEPS_BEFORE_PARSE = ['import_ftp'];
const ALL_STEPS_AFTER_PARSE = ['ean_mapping', 'pricing', 'export_ean', 'export_mediaworld', 'export_eprice'];
const CRON_EXTRA_STEPS = ['export_ean_xlsx', 'export_amazon'];

async function runPipeline(
  supabase: SupabaseClient,
  supabaseUrl: string,
  supabaseServiceKey: string,
  runId: string,
  trigger: string,
  feeConfig: Record<string, unknown>,
  runStartTime: number,
  resumeFromStep: string,
  orchestratorStart: number,
): Promise<Response> {
  
  const budgetExceeded = () => (Date.now() - orchestratorStart) > ORCHESTRATOR_BUDGET_MS;
  
  // Helper to yield - lock will be released in finally block
  const yieldResponse = async (currentStep: string, reason: string): Promise<Response> => {
    console.log(`[orchestrator] YIELD at step ${currentStep}: ${reason}`);
    
    // Read current step state for logging
    let stepState: Record<string, unknown> = {};
    try {
      const { data: run } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
      stepState = run?.steps?.[currentStep] || {};
    } catch (_) {}
    
    try {
      await supabase.rpc('log_sync_event', {
        p_run_id: runId,
        p_level: 'INFO',
        p_message: 'orchestrator_yield_scheduled',
        p_details: { 
          step: currentStep, 
          orchestrator_elapsed_ms: Date.now() - orchestratorStart,
          budget_ms: ORCHESTRATOR_BUDGET_MS,
          reason,
          cursor_pos_end: stepState.cursor_pos,
          file_total_size: stepState.materialBytes,
          chunk_index: stepState.chunk_index,
          resume_via: 'cron-tick (every 5min)'
        }
      });
    } catch (_) { /* non-blocking */ }
    
    // Lock is released in finally block - cron-tick will re-acquire
    return new Response(JSON.stringify({ 
      status: 'yielded', 
      run_id: runId, 
      current_step: currentStep,
      needs_resume: true 
    }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
  };

  // ========== STEP 1: FTP Import ==========
  if (resumeFromStep === 'import_ftp') {
    await updateCurrentStep(supabase, runId, 'import_ftp');
    console.log('[orchestrator] === STEP 1: FTP Import ===');
    
    for (const fileType of ['material', 'stock', 'price', 'stockLocation']) {
      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', runStartTime, 'Interrotta dall\'utente');
        return new Response(JSON.stringify({ status: 'cancelled' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      if (budgetExceeded()) {
        return await yieldResponse('import_ftp', `budget exceeded during FTP import (${fileType})`);
      }
      
      const result = await callStep(supabaseUrl, supabaseServiceKey, 'import-catalog-ftp', { 
        fileType, run_id: runId
      });
      
      if (!result.success && fileType !== 'stockLocation') {
        await finalizeRun(supabase, runId, 'failed', runStartTime, `FTP ${fileType}: ${result.error}`);
        return new Response(JSON.stringify({ status: 'failed', error: result.error }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      if (!result.success && fileType === 'stockLocation') {
        console.log(`[orchestrator] Stock location import failed (non-blocking): ${result.error}`);
        const { data: run } = await supabase.from('sync_runs').select('location_warnings').eq('id', runId).single();
        const warnings = run?.location_warnings || {};
        warnings.missing_location_file = 1;
        await supabase.from('sync_runs').update({ location_warnings: warnings }).eq('id', runId);
        
        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId,
            p_level: 'WARN',
            p_message: 'File stock location non trovato o non importabile',
            p_details: { step: 'import_ftp', location_warning: 'missing_location_file' }
          });
        } catch (logErr) {
          console.warn('[orchestrator] log_sync_event failed:', logErr);
        }
      }
    }
    // FTP done, fall through to parse_merge
    resumeFromStep = 'parse_merge';
  }

  // ========== STEP 2: PARSE_MERGE (CHUNKED with time budget) ==========
  if (resumeFromStep === 'parse_merge') {
    await updateCurrentStep(supabase, runId, 'parse_merge');
    console.log('[orchestrator] === STEP 2: parse_merge (CHUNKED) ===');
    
    let parseMergeComplete = false;
    let chunkCount = 0;
    
    while (!parseMergeComplete && chunkCount < MAX_PARSE_MERGE_CHUNKS) {
      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', runStartTime, 'Interrotta dall\'utente');
        return new Response(JSON.stringify({ status: 'cancelled' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      // Check orchestrator time budget BEFORE calling the step
      if (budgetExceeded()) {
        return await yieldResponse('parse_merge', `orchestrator budget exceeded after ${chunkCount} chunks this invocation`);
      }
      
      chunkCount++;
      console.log(`[orchestrator] parse_merge chunk ${chunkCount} (this invocation)...`);
      
      const result = await callStep(supabaseUrl, supabaseServiceKey, 'sync-step-runner', { 
        run_id: runId, step: 'parse_merge', fee_config: feeConfig 
      });
      
      if (!result.success) {
        await finalizeRun(supabase, runId, 'failed', runStartTime, `parse_merge: ${result.error}`);
        return new Response(JSON.stringify({ status: 'failed', error: result.error }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      const verification = await verifyStepCompleted(supabase, runId, 'parse_merge');
      
      if (!verification.success) {
        await finalizeRun(supabase, runId, 'failed', runStartTime, verification.error || 'parse_merge verification failed');
        return new Response(JSON.stringify({ status: 'failed', error: verification.error }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      if (verification.status === 'completed') {
        parseMergeComplete = true;
        console.log(`[orchestrator] parse_merge completed after ${chunkCount} chunks this invocation`);
      } else if (verification.status === 'in_progress') {
        console.log(`[orchestrator] parse_merge in_progress, continuing...`);
      } else {
        await finalizeRun(supabase, runId, 'failed', runStartTime, `parse_merge unexpected status: ${verification.status}`);
        return new Response(JSON.stringify({ status: 'failed', error: `Unexpected status: ${verification.status}` }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
    }
    
    if (!parseMergeComplete) {
      // Either hit MAX_PARSE_MERGE_CHUNKS or time budget. If still in_progress, yield.
      const verification = await verifyStepCompleted(supabase, runId, 'parse_merge');
      if (verification.status === 'in_progress') {
        return await yieldResponse('parse_merge', `chunk limit ${MAX_PARSE_MERGE_CHUNKS} reached this invocation`);
      }
      await finalizeRun(supabase, runId, 'failed', runStartTime, `parse_merge exceeded ${MAX_PARSE_MERGE_CHUNKS} chunks limit`);
      return new Response(JSON.stringify({ status: 'failed', error: 'parse_merge chunk limit exceeded' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }
    // parse_merge done, fall through to remaining steps
    resumeFromStep = 'ean_mapping';
  }

  // ========== STEPS 3+: Remaining processing steps ==========
  const allRemainingSteps = [...ALL_STEPS_AFTER_PARSE];
  if (trigger === 'cron') {
    allRemainingSteps.push(...CRON_EXTRA_STEPS);
  }
  
  // Find where to resume from in the remaining steps
  let startIdx = 0;
  if (resumeFromStep !== 'ean_mapping') {
    const idx = allRemainingSteps.indexOf(resumeFromStep);
    if (idx >= 0) startIdx = idx;
  }
  
  // Backoff sequence for export_ean_xlsx 546 retries (seconds)
  const XLSX_RETRY_BACKOFF = [60, 120, 240, 300, 300, 300, 300, 300];
  const XLSX_MAX_RETRIES = 8;

  for (let i = startIdx; i < allRemainingSteps.length; i++) {
    const step = allRemainingSteps[i];
    
    if (await isCancelRequested(supabase, runId)) {
      await finalizeRun(supabase, runId, 'failed', runStartTime, 'Interrotta dall\'utente');
      return new Response(JSON.stringify({ status: 'cancelled' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }
    
    if (budgetExceeded()) {
      return await yieldResponse(step, 'orchestrator budget exceeded before step');
    }

    // ---- export_ean_xlsx: idempotency + retry_delay check ----
    if (step === 'export_ean_xlsx') {
      const { data: runData } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
      const xlsxStepState = runData?.steps?.export_ean_xlsx || {};
      const xlsxRetry = xlsxStepState.retry || {};

      // Already completed: skip (validate rows_written == total_products)
      if (xlsxStepState.status === 'completed' || xlsxStepState.status === 'success') {
        const rw = xlsxStepState.rows_written ?? xlsxStepState.rows ?? 0;
        const tp = xlsxStepState.total_products ?? rw;
        const validComplete = !tp || rw === tp;
        if (validComplete) {
          console.log(`[orchestrator] export_ean_xlsx already completed (rows_written=${rw}), skipping`);
          console.log(JSON.stringify({ diag_tag: 'xlsx_export_retry_decision', run_id: runId, step: 'export_ean_xlsx', decision: 'completed', rows_written: rw, total_products: tp, elapsed_ms: Date.now() - runStartTime }));
          continue;
        }
      }

      // In retry_delay: check if wait_seconds elapsed
      if (xlsxRetry.status === 'retry_delay' && xlsxRetry.next_retry_at) {
        const nextRetryAt = new Date(xlsxRetry.next_retry_at).getTime();
        const now = Date.now();
        if (now < nextRetryAt) {
          const waitSeconds = Math.max(1, Math.ceil((nextRetryAt - now) / 1000));
          console.log(`[orchestrator] export_ean_xlsx in retry_delay, ${waitSeconds}s remaining`);
          console.log(JSON.stringify({ diag_tag: 'xlsx_export_retry_decision', run_id: runId, step: 'export_ean_xlsx', decision: 'retry_delay', retry_attempt: xlsxRetry.retry_attempt, wait_seconds: waitSeconds, reason: 'waiting_backoff', http_status: xlsxRetry.last_http_status }));
          return new Response(JSON.stringify({
            status: 'skipped', reason: 'retry_delay', wait_seconds: waitSeconds,
            run_id: runId, current_step: 'export_ean_xlsx', needs_resume: true
          }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }
        console.log(`[orchestrator] export_ean_xlsx retry_delay expired, retrying (attempt ${(xlsxRetry.retry_attempt || 0) + 1})`);
      }
    }
    
    await updateCurrentStep(supabase, runId, step);
    console.log(`[orchestrator] === STEP: ${step} ===`);
    
    const result = await callStep(supabaseUrl, supabaseServiceKey, 'sync-step-runner', { 
      run_id: runId, step, fee_config: feeConfig 
    });
    
    // ---- export_ean_xlsx: handle 546 WORKER_LIMIT as retryable ----
    if (step === 'export_ean_xlsx' && !result.success && (result.httpStatus === 546 || (result.error && result.error.includes('WORKER_LIMIT')))) {
      const httpStatus = result.httpStatus || 546;
      const { data: runData } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
      const currentSteps = runData?.steps || {};
      const xlsxStepState = currentSteps.export_ean_xlsx || {};
      const prevRetry = xlsxStepState.retry || {};
      const retryAttempt = (prevRetry.retry_attempt || 0) + 1;

      if (retryAttempt > XLSX_MAX_RETRIES) {
        const failMsg = `export_ean_xlsx: exceeded max retries (${XLSX_MAX_RETRIES}), last HTTP ${httpStatus}`;
        console.log(JSON.stringify({ diag_tag: 'xlsx_export_retry_decision', run_id: runId, step: 'export_ean_xlsx', decision: 'fail', retry_attempt: retryAttempt, http_status: httpStatus, reason: `exceeded_max_retries_${XLSX_MAX_RETRIES}` }));
        await finalizeRun(supabase, runId, 'failed', runStartTime, failMsg);
        return new Response(JSON.stringify({ status: 'failed', error: failMsg }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }

      const backoffSeconds = XLSX_RETRY_BACKOFF[Math.min(retryAttempt - 1, XLSX_RETRY_BACKOFF.length - 1)];
      const nextRetryAt = new Date(Date.now() + backoffSeconds * 1000).toISOString();
      
      // Store retry state INSIDE steps.export_ean_xlsx.retry (single source of truth)
      const updatedXlsxState = {
        ...xlsxStepState,
        retry: {
          retry_attempt: retryAttempt,
          next_retry_at: nextRetryAt,
          last_http_status: httpStatus,
          last_error: `WORKER_LIMIT (HTTP ${httpStatus})`,
          status: 'retry_delay'
        }
      };
      const updatedSteps = { ...currentSteps, export_ean_xlsx: updatedXlsxState, current_step: 'export_ean_xlsx' };
      await supabase.from('sync_runs').update({ steps: updatedSteps }).eq('id', runId);

      console.log(JSON.stringify({ diag_tag: 'xlsx_export_retry_decision', run_id: runId, step: 'export_ean_xlsx', decision: 'retry_delay', retry_attempt: retryAttempt, backoff_seconds: backoffSeconds, wait_seconds: backoffSeconds, http_status: httpStatus, reason: 'worker_limit_546' }));

      // Return skipped/retry_delay — NOT failed, NOT drain_complete
      return new Response(JSON.stringify({
        status: 'skipped', reason: 'retry_delay', wait_seconds: backoffSeconds,
        run_id: runId, current_step: 'export_ean_xlsx', needs_resume: true
      }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    // For non-export_ean_xlsx steps, or non-546 errors: standard verification
    const verification = await verifyStepCompleted(supabase, runId, step);
    
    if (!result.success || !verification.success) {
      const error = result.error || verification.error || `Step ${step} fallito`;
      await finalizeRun(supabase, runId, 'failed', runStartTime, error);
      return new Response(JSON.stringify({ status: 'failed', error }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    // ---- export_ean_xlsx: mark completed with rows validation ----
    if (step === 'export_ean_xlsx' && result.success) {
      const { data: runData } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
      const currentSteps = runData?.steps || {};
      const xlsxStepState = currentSteps.export_ean_xlsx || {};
      const rw = xlsxStepState.rows_written ?? xlsxStepState.rows ?? xlsxStepState.metrics?.ean_xlsx_rows ?? 0;
      const tp = xlsxStepState.total_products ?? rw;

      // Validate rows_written == total_products
      if (tp > 0 && rw !== tp) {
        const failMsg = `export_ean_xlsx validation failed: rows_written(${rw}) != total_products(${tp})`;
        console.error(JSON.stringify({ diag_tag: 'xlsx_export_retry_decision', run_id: runId, step: 'export_ean_xlsx', decision: 'fail', rows_written: rw, total_products: tp, reason: 'rows_mismatch' }));
        await finalizeRun(supabase, runId, 'failed', runStartTime, failMsg);
        return new Response(JSON.stringify({ status: 'failed', error: failMsg }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }

      // Clear retry sub-state, keep completed
      const updatedXlsxState = {
        ...xlsxStepState,
        retry: { status: 'completed', retry_attempt: xlsxStepState.retry?.retry_attempt || 0 }
      };
      const updatedSteps = { ...currentSteps, export_ean_xlsx: updatedXlsxState };
      await supabase.from('sync_runs').update({ steps: updatedSteps }).eq('id', runId);

      console.log(JSON.stringify({ diag_tag: 'xlsx_export_retry_decision', run_id: runId, step: 'export_ean_xlsx', decision: 'completed', rows_written: rw, total_products: tp, elapsed_ms: Date.now() - runStartTime }));
    }
  }

  // ========== SFTP Upload (ONLY for cron trigger) ==========
  if (trigger === 'cron') {
    if (await isCancelRequested(supabase, runId)) {
      await finalizeRun(supabase, runId, 'failed', runStartTime, 'Interrotta dall\'utente');
      return new Response(JSON.stringify({ status: 'cancelled' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }
    
    if (budgetExceeded()) {
      return await yieldResponse('upload_sftp', 'orchestrator budget exceeded before SFTP');
    }
    
    await updateRun(supabase, runId, { steps: { current_step: 'upload_sftp' } });
    console.log('[orchestrator] === SFTP Upload (cron only) ===');
    
    const sftpFiles = EXPORT_FILES.map(f => ({
      bucket: 'exports',
      path: f,
      filename: f
    }));
    
    const sftpResult = await callStep(supabaseUrl, supabaseServiceKey, 'upload-exports-to-sftp', {
      files: sftpFiles
    });
    
    if (!sftpResult.success) {
      await finalizeRun(supabase, runId, 'failed', runStartTime, `SFTP: ${sftpResult.error}`);
      return new Response(JSON.stringify({ status: 'failed', error: sftpResult.error }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }
  } else {
    console.log('[orchestrator] Skipping SFTP upload (manual trigger)');
  }

  // ========== STORAGE VERSIONING ==========
  if (budgetExceeded()) {
    return await yieldResponse('versioning', 'orchestrator budget exceeded before versioning');
  }
  
  console.log('[orchestrator] === Storage versioning ===');
  const versionTimestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const fileManifest: Record<string, string> = {};
  
  for (const fileName of EXPORT_FILES) {
    try {
      const { data: fileBlob } = await supabase.storage.from('exports').download(fileName);
      if (fileBlob) {
        await supabase.storage.from('exports').upload(`latest/${fileName}`, fileBlob, { upsert: true });
        await supabase.storage.from('exports').upload(`versions/${versionTimestamp}/${fileName}`, fileBlob, { upsert: true });
        fileManifest[fileName] = versionTimestamp;
      }
    } catch (e: unknown) {
      console.warn(`[orchestrator] Versioning failed for ${fileName}: ${errMsg(e)}`);
    }
  }
  
  await supabase.from('sync_runs').update({ file_manifest: fileManifest }).eq('id', runId);
  await cleanupVersions(supabase);

  // ========== DETERMINE FINAL STATUS ==========
  const { data: finalRun } = await supabase.from('sync_runs').select('warning_count').eq('id', runId).single();
  const finalStatus = (finalRun?.warning_count || 0) > 0 ? 'success_with_warning' : 'success';
  
  await finalizeRun(supabase, runId, finalStatus, runStartTime);
  console.log(`[orchestrator] Pipeline completed: ${runId}, status: ${finalStatus}`);

  // Post-run notification
  try {
    await fetch(`${supabaseUrl}/functions/v1/send-sync-notification`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${supabaseServiceKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ run_id: runId, status: finalStatus })
    });
    console.log(`[orchestrator] Notification sent for run ${runId}`);
  } catch (e: unknown) {
    console.warn(`[orchestrator] Notification failed (non-blocking): ${errMsg(e)}`);
  }
  
  return new Response(JSON.stringify({ status: finalStatus, run_id: runId }), 
    { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
}

// ============================================================
// VERSION CLEANUP
// ============================================================

async function cleanupVersions(supabase: SupabaseClient): Promise<void> {
  try {
    const { data: versionFolders } = await supabase.storage.from('exports').list('versions', {
      sortBy: { column: 'name', order: 'desc' },
      limit: 200
    });

    if (!versionFolders || versionFolders.length === 0) {
      console.log('[orchestrator] No version folders found, skipping cleanup');
      return;
    }

    const sevenDaysAgo = Date.now() - 7 * 24 * 60 * 60 * 1000;
    const fileVersions: Record<string, Array<{ folder: string; timestamp: number }>> = {};
    for (const f of EXPORT_FILES) {
      fileVersions[f] = [];
    }

    for (const folder of versionFolders) {
      const folderName = folder.name;
      let folderTs: number;
      try {
        const isoStr = folderName.replace(/-(\d{2})-(\d{2})-(\d{3})Z$/, ':$1:$2.$3Z').replace(/T(\d{2})-/, 'T$1:');
        folderTs = new Date(isoStr).getTime();
        if (isNaN(folderTs)) {
          folderTs = folder.created_at ? new Date(folder.created_at).getTime() : 0;
        }
      } catch {
        folderTs = folder.created_at ? new Date(folder.created_at).getTime() : 0;
      }

      if (folderTs === 0) {
        console.warn(`[orchestrator] WARN: Cannot determine timestamp for version folder: ${folderName}, skipping`);
        continue;
      }

      const { data: filesInFolder } = await supabase.storage.from('exports').list(`versions/${folderName}`);
      if (!filesInFolder) continue;

      for (const file of filesInFolder) {
        if (EXPORT_FILES.includes(file.name)) {
          fileVersions[file.name].push({ folder: folderName, timestamp: folderTs });
        }
      }
    }

    const toDelete: string[] = [];
    for (const [fileName, versions] of Object.entries(fileVersions)) {
      versions.sort((a, b) => b.timestamp - a.timestamp);
      if (versions.length <= 3) continue;
      const excess = versions.slice(3);
      for (const v of excess) {
        if (v.timestamp < sevenDaysAgo) {
          toDelete.push(`versions/${v.folder}/${fileName}`);
        }
      }
    }

    if (toDelete.length > 0) {
      console.log(`[orchestrator] Deleting ${toDelete.length} old version files`);
      for (let i = 0; i < toDelete.length; i += 20) {
        const batch = toDelete.slice(i, i + 20);
        await supabase.storage.from('exports').remove(batch);
      }
      console.log(`[orchestrator] Retention cleanup completed`);
    } else {
      console.log('[orchestrator] No version files to clean up');
    }

    try {
      for (const folder of versionFolders) {
        const { data: remaining } = await supabase.storage.from('exports').list(`versions/${folder.name}`);
        if (!remaining || remaining.length === 0) {
          // Empty folders are cleaned up automatically
        }
      }
    } catch (e: unknown) {
      console.warn(`[orchestrator] Folder cleanup warning: ${errMsg(e)}`);
    }
  } catch (e: unknown) {
    console.warn(`[orchestrator] Version cleanup failed (non-blocking): ${errMsg(e)}`);
  }
}
