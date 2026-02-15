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
 * SFTP upload: eseguito per TUTTI i trigger (manual + cron).
 * Se SFTP non configurato: run failed "SFTP misconfigured".
 * 
 * Storage versioning: latest/ (overwrite) + versions/<timestamp>/ (storico)
 * Retention: per ciascuno dei 5 file, max 3 versioni; elimina >3 solo se >7 giorni.
 * 
 * Orchestrator-internal steps (versioning, notification, upload_sftp) scrivono
 * stato in steps JSONB tramite merge.
 * 
 * EXPECTED_STEPS (unified, manual + cron):
 *   import_ftp, parse_merge, ean_mapping, pricing, override_products,
 *   export_ean, export_ean_xlsx, export_amazon, export_mediaworld, export_eprice,
 *   upload_sftp, versioning, notification
 * 
 * Diagnostics mode: POST { trigger: "manual", mode: "diagnostics", run_id: "..." }
 *   Requires service role auth. Returns read-only run analysis.
 */

const ORCHESTRATOR_BUDGET_MS = 25_000;
const MAX_PARSE_MERGE_CHUNKS = 100;

const EXPORT_FILES = [
  'Export Mediaworld.csv',
  'Export ePrice.csv',
  'catalogo_ean.xlsx',
  'amazon_listing_loader.xlsm',
  'amazon_price_inventory.txt'
];

// Steps handled by sync-step-runner (verified to exist in runner switch)
const ALL_STEPS_AFTER_PARSE = [
  'ean_mapping', 'pricing', 'override_products',
  'export_ean', 'export_ean_xlsx', 'export_amazon',
  'export_mediaworld', 'export_eprice'
];

// Unified expected steps for ALL triggers (manual + cron).
// No distinction between cron and manual: all steps must complete.
const EXPECTED_STEPS = [
  'import_ftp', 'parse_merge',
  ...ALL_STEPS_AFTER_PARSE,
  'upload_sftp', 'versioning', 'notification'
];

function getExpectedSteps(_trigger: string): string[] {
  return [...EXPECTED_STEPS];
}

/**
 * callStep - NON-THROWING step caller. Always returns { ok, http_status, body }.
 */
async function callStep(supabaseUrl: string, serviceKey: string, functionName: string, reqBody: Record<string, unknown>): Promise<{ ok: boolean; http_status: number; body: unknown }> {
  try {
    console.log(`[orchestrator] Calling ${functionName}...`);
    const resp = await fetch(`${supabaseUrl}/functions/v1/${functionName}`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${serviceKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(reqBody)
    });

    const rawText = await resp.text().catch(() => '');
    let parsedBody: unknown;
    try { parsedBody = JSON.parse(rawText); } catch { parsedBody = rawText; }

    if (!resp.ok) {
      console.log(`[orchestrator] ${functionName} HTTP error ${resp.status}: ${typeof parsedBody === 'string' ? parsedBody.substring(0, 200) : JSON.stringify(parsedBody).substring(0, 200)}`);
      return { ok: false, http_status: resp.status, body: parsedBody };
    }

    const bodyObj = parsedBody as Record<string, unknown> | null;
    if (bodyObj && typeof bodyObj === 'object' && bodyObj.status === 'error') {
      console.log(`[orchestrator] ${functionName} app-level error: ${bodyObj.message || bodyObj.error}`);
      return { ok: false, http_status: resp.status, body: parsedBody };
    }

    console.log(`[orchestrator] ${functionName} completed, step_status=${(bodyObj as Record<string,unknown>)?.step_status || 'N/A'}`);
    return { ok: true, http_status: resp.status, body: parsedBody };
  } catch (e: unknown) {
    console.error(`[orchestrator] Error calling ${functionName}:`, e);
    return { ok: false, http_status: 0, body: `fetch_error: ${errMsg(e)}` };
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

/** Update only current_step inside steps JSON without wiping persisted step state */
async function updateCurrentStep(supabase: SupabaseClient, runId: string, stepName: string): Promise<void> {
  const { data: run } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
  const currentSteps = run?.steps || {};
  const updatedSteps = { ...currentSteps, current_step: stepName };
  await supabase.from('sync_runs').update({ steps: updatedSteps }).eq('id', runId);
}

/** Merge a step state into steps JSONB without overwriting other steps.
 *  Deep merges the step object: preserves existing fields (e.g. retry, cursor_pos)
 *  while overlaying the new patch. */
async function mergeStepState(supabase: SupabaseClient, runId: string, stepName: string, state: Record<string, unknown>): Promise<void> {
  const { data: run } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
  const currentSteps = run?.steps || {};
  const existingStepState = currentSteps[stepName];
  const mergedStepState = (existingStepState && typeof existingStepState === 'object')
    ? { ...existingStepState, ...state }
    : state;
  const updatedSteps = { ...currentSteps, [stepName]: mergedStepState };
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

// ============================================================
// DIAGNOSTICS MODE
// ============================================================
async function handleDiagnostics(supabase: SupabaseClient, runId: string): Promise<Response> {
  const { data: run, error } = await supabase.from('sync_runs').select('*').eq('id', runId).single();
  if (error || !run) {
    return new Response(JSON.stringify({ status: 'error', message: `Run ${runId} not found` }),
      { status: 404, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
  }

  const expectedSteps = getExpectedSteps(run.trigger_type);
  const steps = run.steps || {};
  const stepDetails: Record<string, unknown> = {};
  const missingSteps: string[] = [];

  for (const s of expectedSteps) {
    const st = steps[s];
    if (!st || (typeof st === 'object' && !st.status)) {
      missingSteps.push(s);
      stepDetails[s] = { status: 'missing' };
    } else {
      stepDetails[s] = st;
    }
  }

  const { data: events } = await supabase
    .from('sync_events')
    .select('level, message, step, created_at, details')
    .eq('run_id', runId)
    .order('created_at', { ascending: false })
    .limit(50);

  return new Response(JSON.stringify({
    status: 'ok',
    run: {
      status: run.status,
      started_at: run.started_at,
      finished_at: run.finished_at,
      runtime_ms: run.runtime_ms,
      trigger_type: run.trigger_type,
      warning_count: run.warning_count,
      error_message: run.error_message,
    },
    expected_steps: expectedSteps,
    step_details: stepDetails,
    missing_steps: missingSteps,
    events: (events || []).map(e => ({
      level: e.level, message: e.message, step: e.step,
      time: e.created_at, details: e.details
    }))
  }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
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
    const mode = body.mode as string | undefined;
    
    if (!trigger || !['cron', 'manual'].includes(trigger)) {
      return new Response(JSON.stringify({ status: 'error', message: 'trigger deve essere cron o manual' }), 
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    supabase = createClient(supabaseUrl, supabaseServiceKey);

    // ========== DIAGNOSTICS MODE ==========
    if (mode === 'diagnostics' && body.run_id) {
      // Service role only (already using service key client)
      return await handleDiagnostics(supabase, body.run_id as string);
    }

    const { data: syncConfig } = await supabase.from('sync_config').select('run_timeout_minutes').eq('id', 1).single();
    const lockTtlSeconds = 120;

    // ========== RESUME MODE ==========
    if (resumeRunId) {
      console.log(`[orchestrator] RESUME mode for run ${resumeRunId}, trigger: ${trigger}`);
      runId = resumeRunId;
      
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
      
      const { data: lockResult } = await supabase.rpc('try_acquire_sync_lock', {
        p_lock_name: 'global_sync',
        p_run_id: runId,
        p_ttl_seconds: lockTtlSeconds
      });
      
      if (!lockResult) {
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
        } catch (_) {}
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
      } catch (_) {}
      
      const FEE_CONFIG_SINGLETON_ID = '00000000-0000-0000-0000-000000000001';
      const { data: feeData } = await supabase.from('fee_config').select('*').eq('id', FEE_CONFIG_SINGLETON_ID).maybeSingle();
      const feeConfig = buildFeeConfig(feeData);
      
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
      
      const { data: existingRunning } = await supabase
        .from('sync_runs')
        .select('id, steps, started_at, trigger_type')
        .eq('status', 'running')
        .limit(1);
      
      if (existingRunning?.length) {
        const existing = existingRunning[0];
        console.log(`[orchestrator] Found existing running run ${existing.id}, resuming instead of creating new`);
        runId = existing.id;
        
        const { data: lockOk } = await supabase.rpc('try_acquire_sync_lock', {
          p_lock_name: 'global_sync',
          p_run_id: runId,
          p_ttl_seconds: lockTtlSeconds
        });
        
        if (!lockOk) {
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

    // override_products is now implemented in sync-step-runner

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
  
  const yieldResponse = async (currentStep: string, reason: string): Promise<Response> => {
    console.log(`[orchestrator] YIELD at step ${currentStep}: ${reason}`);
    
    // CRITICAL FIX: persist current_step in DB BEFORE returning yielded response
    // so that resume picks up the correct step instead of the previous one.
    await updateCurrentStep(supabase, runId, currentStep);
    
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
    } catch (_) {}
    
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
    await mergeStepState(supabase, runId, 'import_ftp', { status: 'in_progress', started_at: new Date().toISOString() });
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
      
      if (!result.ok && fileType !== 'stockLocation') {
        const errText = typeof result.body === 'string' ? result.body : JSON.stringify(result.body);
        await mergeStepState(supabase, runId, 'import_ftp', { status: 'failed', error: `FTP ${fileType}: ${errText.substring(0, 200)}` });
        await finalizeRun(supabase, runId, 'failed', runStartTime, `FTP ${fileType}: HTTP ${result.http_status} ${errText.substring(0, 200)}`);
        return new Response(JSON.stringify({ status: 'failed', error: errText.substring(0, 200) }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      if (!result.ok && fileType === 'stockLocation') {
        console.log(`[orchestrator] Stock location import failed (non-blocking): HTTP ${result.http_status}`);
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
    await mergeStepState(supabase, runId, 'import_ftp', { status: 'completed', finished_at: new Date().toISOString() });
    try {
      await supabase.rpc('log_sync_event', {
        p_run_id: runId, p_level: 'INFO', p_message: 'step_completed',
        p_details: { step: 'import_ftp' }
      });
    } catch (_) {}
    resumeFromStep = 'parse_merge';
  }

  // ========== STEP 2: PARSE_MERGE (CHUNKED) ==========
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
      
      if (budgetExceeded()) {
        return await yieldResponse('parse_merge', `orchestrator budget exceeded after ${chunkCount} chunks this invocation`);
      }
      
      chunkCount++;
      console.log(`[orchestrator] parse_merge chunk ${chunkCount} (this invocation)...`);
      
      const result = await callStep(supabaseUrl, supabaseServiceKey, 'sync-step-runner', { 
        run_id: runId, step: 'parse_merge', fee_config: feeConfig 
      });
      
      if (!result.ok) {
        const errText = typeof result.body === 'string' ? result.body : JSON.stringify(result.body);
        await finalizeRun(supabase, runId, 'failed', runStartTime, `parse_merge: ${errText.substring(0, 200)}`);
        return new Response(JSON.stringify({ status: 'failed', error: errText.substring(0, 200) }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
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
      const verification = await verifyStepCompleted(supabase, runId, 'parse_merge');
      if (verification.status === 'in_progress') {
        return await yieldResponse('parse_merge', `chunk limit ${MAX_PARSE_MERGE_CHUNKS} reached this invocation`);
      }
      await finalizeRun(supabase, runId, 'failed', runStartTime, `parse_merge exceeded ${MAX_PARSE_MERGE_CHUNKS} chunks limit`);
      return new Response(JSON.stringify({ status: 'failed', error: 'parse_merge chunk limit exceeded' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }
    resumeFromStep = 'ean_mapping';
  }

  // ========== STEPS 3+: Remaining processing steps ==========
  const allRemainingSteps = [...ALL_STEPS_AFTER_PARSE];
  
  let startIdx = 0;
  if (resumeFromStep !== 'ean_mapping') {
    const idx = allRemainingSteps.indexOf(resumeFromStep);
    if (idx >= 0) startIdx = idx;
  }

  // ---- Retry constants for export_* 546 WORKER_LIMIT ----
  const EXPORT_MAX_RETRIES = 8;
  function exportBackoffSeconds(attempt: number): number {
    return Math.min(60 * Math.pow(2, attempt - 1), 600);
  }

  function isWorkerLimit(body: unknown): boolean {
    if (typeof body === 'string') return body.includes('WORKER_LIMIT');
    if (!body || typeof body !== 'object') return false;
    const b = body as Record<string, unknown>;
    if (b.code === 'WORKER_LIMIT' || b.error_code === 'WORKER_LIMIT') return true;
    const err = b.error as Record<string, unknown> | undefined;
    if (err && typeof err === 'object' && err.code === 'WORKER_LIMIT') return true;
    if (typeof b.message === 'string' && b.message.includes('WORKER_LIMIT')) return true;
    return false;
  }

  // ---- Legacy cleanup: remove steps.export_ean_xlsx_retry if present ----
  {
    const { data: runData } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
    const currentSteps = runData?.steps || {};
    if ('export_ean_xlsx_retry' in currentSteps) {
      delete (currentSteps as Record<string, unknown>).export_ean_xlsx_retry;
      await supabase.from('sync_runs').update({ steps: currentSteps }).eq('id', runId);
      console.log(JSON.stringify({ diag_tag: 'legacy_retry_namespace_cleaned', run_id: runId, action: 'deleted' }));
      try {
        await supabase.rpc('log_sync_event', {
          p_run_id: runId, p_level: 'INFO', p_message: 'legacy_retry_namespace_cleaned',
          p_details: { action: 'deleted', namespace: 'export_ean_xlsx_retry' }
        });
      } catch (_) {}
    } else {
      console.log(JSON.stringify({ diag_tag: 'legacy_retry_namespace_cleaned', run_id: runId, action: 'absent' }));
    }
  }

  for (let i = startIdx; i < allRemainingSteps.length; i++) {
    const step = allRemainingSteps[i];
    const isExportStep = step.startsWith('export_');
    
    if (await isCancelRequested(supabase, runId)) {
      await finalizeRun(supabase, runId, 'failed', runStartTime, 'Interrotta dall\'utente');
      return new Response(JSON.stringify({ status: 'cancelled' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }
    
    if (budgetExceeded()) {
      return await yieldResponse(step, 'orchestrator budget exceeded before step');
    }

    // ---- Idempotency: skip already completed steps ----
    {
      const { data: runData } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
      const stepState = runData?.steps?.[step] || {};

      if (stepState.status === 'completed' || stepState.status === 'success') {
        console.log(`[orchestrator] ${step} already completed, skipping`);
        if (isExportStep) {
          console.log(JSON.stringify({ diag_tag: 'xlsx_export_retry_decision', run_id: runId, step, decision: 'completed', elapsed_ms: Date.now() - runStartTime }));
        }
        continue;
      }
    }

    // ---- retry_delay gate (all steps, generalized 546 WORKER_LIMIT) ----
    {
      const { data: runData } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
      const stepState = runData?.steps?.[step] || {};
      const stepRetry = stepState.retry || {};

      if (stepRetry.status === 'retry_delay' && stepRetry.next_retry_at) {
        const nextRetryAt = new Date(stepRetry.next_retry_at).getTime();
        const now = Date.now();
        if (now < nextRetryAt) {
          const waitSeconds = Math.max(1, Math.ceil((nextRetryAt - now) / 1000));
          console.log(`[orchestrator] ${step} in retry_delay, ${waitSeconds}s remaining`);
          try {
            await supabase.rpc('log_sync_event', {
              p_run_id: runId, p_level: 'INFO', p_message: 'retry_not_ready',
              p_details: { step, decision: 'retry_not_ready', wait_seconds: waitSeconds, next_retry_at: stepRetry.next_retry_at, retry_attempt: stepRetry.retry_attempt }
            });
          } catch (_) {}
          return new Response(JSON.stringify({
            status: 'skipped', reason: 'retry_not_ready', wait_seconds: waitSeconds,
            run_id: runId, current_step: step, needs_resume: true
          }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }
        console.log(`[orchestrator] ${step} retry_delay expired, retrying (attempt ${stepRetry.retry_attempt || 0})`);
      }
    }
    
    await updateCurrentStep(supabase, runId, step);
    // CRITICAL: Write in_progress BEFORE callStep to guarantee steps[step] always exists
    // when current_step points to it. This eliminates the "current_step set but step absent" bug.
    await mergeStepState(supabase, runId, step, { status: 'in_progress', started_at: new Date().toISOString() });
    console.log(`[orchestrator] === STEP: ${step} ===`);
    try {
      await supabase.rpc('log_sync_event', {
        p_run_id: runId, p_level: 'INFO', p_message: 'step_started',
        p_details: { step }
      });
    } catch (_) {}
    
    const result = await callStep(supabaseUrl, supabaseServiceKey, 'sync-step-runner', { 
      run_id: runId, step, fee_config: feeConfig 
    });
    
    // ---- export_*: handle 546 WORKER_LIMIT as retryable ----
    if (!result.ok && result.http_status === 546 && isWorkerLimit(result.body)) {
      const { data: runData } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
      const currentSteps = runData?.steps || {};
      const stepState = currentSteps[step] || {};
      const prevRetry = stepState.retry || {};
      const nextAttempt = (prevRetry.retry_attempt || 0) + 1;

      if (nextAttempt > EXPORT_MAX_RETRIES) {
        const failMsg = `Step ${step} failed: WORKER_LIMIT persistent after ${EXPORT_MAX_RETRIES} retries (HTTP 546)`;
        console.log(JSON.stringify({ diag_tag: 'xlsx_export_retry_decision', run_id: runId, step, decision: 'fail', retry_attempt: EXPORT_MAX_RETRIES, http_status: 546, reason: 'max_retries_exceeded' }));
        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'ERROR', p_message: 'export_retry_max_exceeded',
            p_details: { diag_tag: 'xlsx_export_retry_decision', step, decision: 'fail', retry_attempt: EXPORT_MAX_RETRIES, http_status: 546, reason: 'max_retries_exceeded' }
          });
        } catch (_) {}
        await finalizeRun(supabase, runId, 'failed', runStartTime, failMsg);
        return new Response(JSON.stringify({ status: 'failed', error: failMsg }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }

      const backoffSec = exportBackoffSeconds(nextAttempt);
      const nextRetryAt = new Date(Date.now() + backoffSec * 1000).toISOString();
      
      const updatedStepState = {
        ...stepState,
        retry: {
          retry_attempt: nextAttempt,
          next_retry_at: nextRetryAt,
          last_http_status: 546,
          last_error: 'worker_limit_546',
          status: 'retry_delay'
        }
      };
      const updatedSteps = { ...currentSteps, [step]: updatedStepState, current_step: step };
      await supabase.from('sync_runs').update({ steps: updatedSteps }).eq('id', runId);

      console.log(JSON.stringify({ diag_tag: 'xlsx_export_retry_decision', run_id: runId, step, decision: 'retry_delay', retry_attempt: nextAttempt, backoff_seconds: backoffSec, wait_seconds: backoffSec, http_status: 546, reason: 'worker_limit_546' }));
      try {
        await supabase.rpc('log_sync_event', {
          p_run_id: runId, p_level: 'WARN', p_message: 'export_retry_delay',
          p_details: { diag_tag: 'xlsx_export_retry_decision', step, decision: 'retry_delay', retry_attempt: nextAttempt, backoff_seconds: backoffSec, wait_seconds: backoffSec, http_status: 546, reason: 'worker_limit_546' }
        });
      } catch (_) {}

      return new Response(JSON.stringify({
        status: 'skipped', reason: 'retry_delay', wait_seconds: backoffSec,
        run_id: runId, current_step: step, needs_resume: true
      }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    // ---- Standard verification for all steps ----
    const verification = await verifyStepCompleted(supabase, runId, step);
    
    if (!result.ok || !verification.success) {
      const errText = !result.ok 
        ? (typeof result.body === 'string' ? result.body : JSON.stringify(result.body)).substring(0, 200) 
        : (verification.error || `Step ${step} fallito`);
      await mergeStepState(supabase, runId, step, { status: 'failed', error: errText });
      try {
        await supabase.rpc('log_sync_event', {
          p_run_id: runId, p_level: 'ERROR', p_message: 'step_failed',
          p_details: { step, error: errText }
        });
      } catch (_) {}
      await finalizeRun(supabase, runId, 'failed', runStartTime, errText);
      return new Response(JSON.stringify({ status: 'failed', error: errText }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    // ---- Yield on in_progress: step needs more invocations ----
    if (verification.status === 'in_progress') {
      console.log(`[orchestrator] ${step} still in_progress after callStep, yielding for resume`);
      return await yieldResponse(step, `step ${step} still in_progress, needs resume`);
    }

    // ---- On success: clear retry state and log step_completed ----
    {
      const { data: runData } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
      const currentSteps = runData?.steps || {};
      const stepState = currentSteps[step] || {};
      const hadRetry = stepState.retry?.retry_attempt > 0;

      if (stepState.retry) {
        const cleanedState = { ...stepState };
        delete cleanedState.retry;
        const updatedSteps = { ...currentSteps, [step]: cleanedState };
        await supabase.from('sync_runs').update({ steps: updatedSteps }).eq('id', runId);
      }

      try {
        await supabase.rpc('log_sync_event', {
          p_run_id: runId, p_level: 'INFO', p_message: 'step_completed',
          p_details: { step, had_retry: hadRetry, elapsed_ms: Date.now() - runStartTime }
        });
      } catch (_) {}
    }
  }

  // ========== SFTP Upload (all triggers) ==========
  {
    // Check if already completed
    const { data: sftpCheck } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
    const sftpState = sftpCheck?.steps?.upload_sftp;
    if (sftpState?.status === 'completed' || sftpState?.status === 'success') {
      console.log('[orchestrator] upload_sftp already completed, skipping');
    } else {
      const sftpHost = Deno.env.get('SFTP_HOST');
      const sftpUser = Deno.env.get('SFTP_USER');
      const sftpPassword = Deno.env.get('SFTP_PASSWORD');
      const sftpBaseDir = Deno.env.get('SFTP_BASE_DIR');
      const sftpConfigured = !!(sftpHost && sftpUser && sftpPassword && sftpBaseDir);

      if (!sftpConfigured) {
        const missingEnv: string[] = [];
        if (!sftpHost) missingEnv.push('SFTP_HOST');
        if (!sftpUser) missingEnv.push('SFTP_USER');
        if (!sftpPassword) missingEnv.push('SFTP_PASSWORD');
        if (!sftpBaseDir) missingEnv.push('SFTP_BASE_DIR');
        console.log(`[orchestrator] SFTP not configured - failing run (missing: ${missingEnv.join(', ')})`);
        await updateCurrentStep(supabase, runId, 'upload_sftp');
        await mergeStepState(supabase, runId, 'upload_sftp', { status: 'failed', error: 'SFTP misconfigured', missing_env: missingEnv });
        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'ERROR', p_message: 'sftp_failed',
            p_details: { step: 'upload_sftp', reason: 'SFTP misconfigured', missing_env: missingEnv }
          });
        } catch (_) {}
        await finalizeRun(supabase, runId, 'failed', runStartTime, 'SFTP misconfigured');
        return new Response(JSON.stringify({ status: 'failed', error: 'SFTP misconfigured' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }

      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', runStartTime, 'Interrotta dall\'utente');
        return new Response(JSON.stringify({ status: 'cancelled' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      if (budgetExceeded()) {
        return await yieldResponse('upload_sftp', 'orchestrator budget exceeded before SFTP');
      }
      
      await updateCurrentStep(supabase, runId, 'upload_sftp');
      console.log('[orchestrator] === SFTP Upload ===');
      
      const sftpFiles = EXPORT_FILES.map(f => ({
        bucket: 'exports',
        path: f,
        filename: f
      }));
      
      const sftpResult = await callStep(supabaseUrl, supabaseServiceKey, 'upload-exports-to-sftp', {
        files: sftpFiles
      });
      
      if (!sftpResult.ok) {
        const sftpErr = typeof sftpResult.body === 'string' ? sftpResult.body : JSON.stringify(sftpResult.body);
        await mergeStepState(supabase, runId, 'upload_sftp', { status: 'failed', error: sftpErr.substring(0, 200) });
        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'ERROR', p_message: 'sftp_failed',
            p_details: { step: 'upload_sftp', error: sftpErr.substring(0, 200) }
          });
        } catch (_) {}
        await finalizeRun(supabase, runId, 'failed', runStartTime, `SFTP: ${sftpErr.substring(0, 200)}`);
        return new Response(JSON.stringify({ status: 'failed', error: sftpErr.substring(0, 200) }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      await mergeStepState(supabase, runId, 'upload_sftp', { status: 'completed' });
      try {
        await supabase.rpc('log_sync_event', {
          p_run_id: runId, p_level: 'INFO', p_message: 'sftp_completed',
          p_details: { step: 'upload_sftp', files_count: EXPORT_FILES.length }
        });
      } catch (_) {}
    }
  }

  // ========== STORAGE VERSIONING ==========
  {
    const { data: verCheck } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
    const verState = verCheck?.steps?.versioning;
    if (verState?.status === 'completed' || verState?.status === 'success') {
      console.log('[orchestrator] versioning already completed, skipping');
    } else {
      if (budgetExceeded()) {
        return await yieldResponse('versioning', 'orchestrator budget exceeded before versioning');
      }
      
      await updateCurrentStep(supabase, runId, 'versioning');
      await mergeStepState(supabase, runId, 'versioning', { status: 'in_progress' });
      console.log('[orchestrator] === Storage versioning ===');
      
      try {
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
        
        await mergeStepState(supabase, runId, 'versioning', { status: 'completed', files: Object.keys(fileManifest).length });
      } catch (e: unknown) {
        console.error(`[orchestrator] Versioning error: ${errMsg(e)}`);
        await mergeStepState(supabase, runId, 'versioning', { status: 'failed', error: errMsg(e).substring(0, 200) });
      }
    }
  }

  // ========== NOTIFICATION (runs before final completeness check) ==========
  await updateCurrentStep(supabase, runId, 'notification');
  await mergeStepState(supabase, runId, 'notification', { status: 'in_progress' });
  try {
    // Determine preliminary status for notification content
    const { data: preNotifRun } = await supabase.from('sync_runs').select('steps, warning_count').eq('id', runId).single();
    const preNotifSteps = preNotifRun?.steps || {};
    const preExpected = getExpectedSteps(trigger);
    let prelimStatus = 'success';
    for (const s of preExpected) {
      if (s === 'notification') continue;
      const st = preNotifSteps[s];
      if (!st || (typeof st === 'object' && (!st.status || (st.status !== 'completed' && st.status !== 'success')))) {
        prelimStatus = 'failed';
        break;
      }
    }
    if (prelimStatus === 'success' && (preNotifRun?.warning_count || 0) > 0) {
      prelimStatus = 'success_with_warning';
    }

    const notifResult = await callStep(supabaseUrl, supabaseServiceKey, 'send-sync-notification', {
      run_id: runId, status: prelimStatus
    });
    
    // send-sync-notification returns HTTP 200 with { status: 'completed' | 'failed' }
    const notifBody = notifResult.body as Record<string, unknown> | null;
    const notifBodyStatus = notifBody?.status;
    const isNotifOk = notifResult.ok && notifBodyStatus !== 'failed';
    
    if (isNotifOk) {
      await mergeStepState(supabase, runId, 'notification', { status: 'completed' });
      try {
        await supabase.rpc('log_sync_event', {
          p_run_id: runId, p_level: 'INFO', p_message: 'notification_completed',
          p_details: { step: 'notification' }
        });
      } catch (_) {}
      console.log(`[orchestrator] Notification sent for run ${runId}`);
    } else {
      const notifErr = typeof notifResult.body === 'string' ? notifResult.body : JSON.stringify(notifResult.body);
      await mergeStepState(supabase, runId, 'notification', { 
        status: 'failed', error: notifErr.substring(0, 200),
        missing_env: notifBody?.missing_env || null
      });
      try {
        await supabase.rpc('log_sync_event', {
          p_run_id: runId, p_level: 'ERROR', p_message: 'notification_failed',
          p_details: { step: 'notification', error: notifErr.substring(0, 200), missing_env: notifBody?.missing_env }
        });
      } catch (_) {}
      console.warn(`[orchestrator] Notification failed: ${notifErr.substring(0, 200)}`);
    }
  } catch (e: unknown) {
    await mergeStepState(supabase, runId, 'notification', { status: 'failed', error: errMsg(e).substring(0, 200) });
    try {
      await supabase.rpc('log_sync_event', {
        p_run_id: runId, p_level: 'ERROR', p_message: 'notification_failed',
        p_details: { step: 'notification', error: errMsg(e).substring(0, 200) }
      });
    } catch (_) {}
    console.warn(`[orchestrator] Notification failed: ${errMsg(e)}`);
  }

  // ========== COMPLETENESS CHECK & FINAL STATUS ==========
  // All steps including notification must be completed/success. No skipped allowed.
  const { data: finalRun } = await supabase.from('sync_runs').select('steps, warning_count').eq('id', runId).single();
  const finalSteps = finalRun?.steps || {};
  const expectedSteps = getExpectedSteps(trigger);

  const missingSteps: string[] = [];
  const failedSteps: string[] = [];
  for (const s of expectedSteps) {
    const st = finalSteps[s];
    if (!st || (typeof st === 'object' && !st.status)) {
      missingSteps.push(s);
    } else if (typeof st === 'object' && st.status !== 'completed' && st.status !== 'success') {
      failedSteps.push(s);
    }
  }

  let finalStatus: string;
  if (missingSteps.length > 0 || failedSteps.length > 0) {
    const errorMsg = `Pipeline incomplete: missing=[${missingSteps.join(',')}] failed=[${failedSteps.join(',')}]`;
    console.warn(`[orchestrator] ${errorMsg}`);
    try {
      await supabase.rpc('log_sync_event', {
        p_run_id: runId, p_level: 'ERROR', p_message: 'pipeline_incomplete',
        p_details: { missing_steps: missingSteps, failed_steps: failedSteps, expected: expectedSteps.length }
      });
    } catch (_) {}
    finalStatus = 'failed';
    await finalizeRun(supabase, runId, 'failed', runStartTime, errorMsg);
  } else {
    finalStatus = (finalRun?.warning_count || 0) > 0 ? 'success_with_warning' : 'success';
    await finalizeRun(supabase, runId, finalStatus, runStartTime);
  }
  
  console.log(`[orchestrator] Pipeline completed: ${runId}, status: ${finalStatus}`);
  
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
