import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";
import { acquireOrRenewGlobalLock, assertLockOwned, renewLockLease, generateInvocationId, LOCK_TTL_SECONDS } from "../_shared/lock.ts";
import { getExpectedSteps } from "../_shared/expectedSteps.ts";

type SupabaseClient = ReturnType<typeof createClient>;

// Per-invocation nonce: generated once per edge function call.
// Used to guarantee non-reentrant locking even for the same run_id.
const INVOCATION_ID = generateInvocationId();

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
 * PIPELINE CANONICA (13 step, identica per manual e cron):
 *   import_ftp, parse_merge, ean_mapping, pricing, override_products,
 *   export_ean, export_ean_xlsx, export_amazon, export_mediaworld, export_eprice,
 *   upload_sftp, versioning, notification
 * 
 * INVARIANTI:
 *   1. Se current_step è valorizzato, steps[current_step] DEVE esistere.
 *   2. Prima di ogni step: atomic setStepInProgress (current_step + steps[step].status=in_progress in una singola write).
 *   3. 546 WORKER_LIMIT è retryable per TUTTI gli step (max 8 tentativi, backoff geometrico).
 *   4. La run non può essere completed se anche uno solo dei 13 step non è completed.
 *   5. notification è blocca-run: se fallisce, run = failed.
 *   6. yielded/retry_delay NON sono failure (toggle auto-sync resta attivo). Lock → yielded con reason="locked".
 *
 * SFTP upload: eseguito per TUTTI i trigger (manual + cron).
 * Se SFTP non configurato: run failed "SFTP misconfigured".
 */

const ORCHESTRATOR_BUDGET_MS = 25_000;
const PARSE_MERGE_BUDGET_MS = 50_000;
const MAX_PARSE_MERGE_CHUNKS = 100;

const EXPORT_FILES = [
  'Catalogo EAN.xlsx',
  'Export ePrice.xlsx',
  'Export Mediaworld.xlsx',
  'amazon_listing_loader.xlsm',
  'amazon_price_inventory.txt'
];

// Steps handled by sync-step-runner
const ALL_STEPS_AFTER_PARSE = [
  'ean_mapping', 'pricing', 'override_products',
  'export_ean', 'export_ean_xlsx', 'export_amazon',
  'export_mediaworld', 'export_eprice'
];

// ============================================================
// DEEP MERGE (recursive, plain objects only)
// Rules: object+object = recursive merge; array = overwrite; primitive/null = overwrite
// ============================================================
function deepMerge(target: Record<string, unknown>, source: Record<string, unknown>): Record<string, unknown> {
  const result: Record<string, unknown> = { ...target };
  for (const key of Object.keys(source)) {
    const srcVal = source[key];
    const tgtVal = target[key];
    if (
      srcVal !== null && typeof srcVal === 'object' && !Array.isArray(srcVal) &&
      tgtVal !== null && typeof tgtVal === 'object' && !Array.isArray(tgtVal)
    ) {
      result[key] = deepMerge(tgtVal as Record<string, unknown>, srcVal as Record<string, unknown>);
    } else {
      result[key] = srcVal;
    }
  }
  return result;
}

// Self-check for deep merge (logged once at startup, no framework needed)
{
  const existing = { status: "retry_delay", retry: { attempt: 2, retry_after: "2026-02-15T10:00:00Z" }, meta: { a: 1 } };
  const patch = { status: "in_progress", retry: { attempt: 3 } };
  const result = deepMerge(existing, patch);
  const ok = result.status === "in_progress" &&
    (result.retry as Record<string, unknown>).attempt === 3 &&
    (result.retry as Record<string, unknown>).retry_after === "2026-02-15T10:00:00Z" &&
    (result.meta as Record<string, unknown>).a === 1;
  console.log(`[orchestrator] deepMerge self-check: ${ok ? 'PASS' : 'FAIL'}`);
  if (!ok) console.error(`[orchestrator] deepMerge self-check FAILED! result=${JSON.stringify(result)}`);
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

/**
 * ATOMIC setStepInProgress: single DB write that sets current_step AND
 * creates/merges steps[stepName] with at least {status:'in_progress'}.
 * Eliminates the bug where current_step is set but steps[step] is absent.
 * After write, asserts invariant: steps[current_step] exists.
 */
async function setStepInProgress(supabase: SupabaseClient, runId: string, stepName: string): Promise<void> {
  // Lock guard: assert ownership with invocation_id and renew lease before writing
  const lockCheck = await assertLockOwned(supabase, runId, INVOCATION_ID);
  if (!lockCheck.owned) {
    console.error(`[orchestrator] LOCK NOT OWNED in setStepInProgress: step=${stepName}, holder=${lockCheck.holder_run_id}, holder_inv=${lockCheck.holder_invocation_id}`);
    try {
      await supabase.rpc('log_sync_event', {
        p_run_id: runId, p_level: 'WARN', p_message: 'lock_ownership_lost',
        p_details: { step: stepName, context: 'setStepInProgress', holder_run_id: lockCheck.holder_run_id, holder_invocation_id: lockCheck.holder_invocation_id, our_invocation_id: INVOCATION_ID }
      });
    } catch (_) {}
    throw new Error(`lock_ownership_lost: cannot write step ${stepName}, lock held by ${lockCheck.holder_run_id} inv=${lockCheck.holder_invocation_id}`);
  }
  await renewLockLease(supabase, runId, LOCK_TTL_SECONDS, INVOCATION_ID);

  // Use atomic DB RPC instead of read-modify-write
  await supabase.rpc('set_step_in_progress', {
    p_run_id: runId,
    p_step_name: stepName
  });

  // Invariant assert: steps[current_step] must exist
  const { data: verify } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
  if (!verify?.steps?.[stepName]) {
    console.error(`[orchestrator] INVARIANT BROKEN: current_step=${stepName} but steps[${stepName}] is absent after write`);
    throw new Error(`invariant_broken: steps[${stepName}] absent after setStepInProgress`);
  }
}

/** Atomic deep-merge a step state into steps JSONB via DB RPC, without overwriting other steps. */
async function mergeStepState(supabase: SupabaseClient, runId: string, stepName: string, state: Record<string, unknown>): Promise<void> {
  // Lock guard: assert ownership with invocation_id and renew lease before writing
  const lockCheck = await assertLockOwned(supabase, runId, INVOCATION_ID);
  if (!lockCheck.owned) {
    console.error(`[orchestrator] LOCK NOT OWNED in mergeStepState: step=${stepName}, holder=${lockCheck.holder_run_id}, holder_inv=${lockCheck.holder_invocation_id}`);
    try {
      await supabase.rpc('log_sync_event', {
        p_run_id: runId, p_level: 'WARN', p_message: 'lock_ownership_lost',
        p_details: { step: stepName, context: 'mergeStepState', holder_run_id: lockCheck.holder_run_id, holder_invocation_id: lockCheck.holder_invocation_id, our_invocation_id: INVOCATION_ID }
      });
    } catch (_) {}
    throw new Error(`lock_ownership_lost: cannot merge step ${stepName}, lock held by ${lockCheck.holder_run_id}`);
  }
  await renewLockLease(supabase, runId, LOCK_TTL_SECONDS, INVOCATION_ID);

  // Use atomic DB RPC for merge (no read-modify-write race)
  await supabase.rpc('merge_sync_run_step', {
    p_run_id: runId,
    p_step_name: stepName,
    p_patch: state
  });
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

// ============================================================
// 546 WORKER_LIMIT DETECTION AND RETRY
// ============================================================
const STEP_MAX_RETRIES = 8;
const BACKOFF_SECONDS = [60, 120, 240, 480, 600, 600, 600, 600];

function stepBackoffSeconds(attempt: number): number {
  const base = BACKOFF_SECONDS[Math.min(attempt - 1, BACKOFF_SECONDS.length - 1)];
  // Add ±10% jitter to avoid synchronized retries across concurrent invocations
  const jitter = Math.round(base * 0.1 * (Math.random() * 2 - 1));
  return base + jitter;
}

function isWorkerLimit(httpStatus: number, body: unknown): boolean {
  if (httpStatus === 546) return true;
  if (typeof body === 'string') return body.includes('WORKER_LIMIT');
  if (!body || typeof body !== 'object') return false;
  const b = body as Record<string, unknown>;
  if (b.code === 'WORKER_LIMIT' || b.error_code === 'WORKER_LIMIT') return true;
  const err = b.error as Record<string, unknown> | undefined;
  if (err && typeof err === 'object' && err.code === 'WORKER_LIMIT') return true;
  if (typeof b.message === 'string' && b.message.includes('WORKER_LIMIT')) return true;
  return false;
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
      return await handleDiagnostics(supabase, body.run_id as string);
    }

    const { data: syncConfig } = await supabase.from('sync_config').select('run_timeout_minutes').eq('id', 1).single();
    const lockTtlSeconds = LOCK_TTL_SECONDS;

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
      
      const lockAcquired = await acquireOrRenewGlobalLock(supabase, runId, lockTtlSeconds, INVOCATION_ID);
      
      if (!lockAcquired) {
        const { data: existingLock } = await supabase
          .from('sync_locks')
          .select('run_id, invocation_id, lease_until')
          .eq('lock_name', 'global_sync')
          .maybeSingle();
        
        console.log(`[orchestrator] Cannot acquire lock for resume: held by run=${existingLock?.run_id} inv=${existingLock?.invocation_id}`);
        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId,
            p_level: 'INFO',
            p_message: 'yielded_locked',
            p_details: { 
              step: 'orchestrator_resume', 
              lock_name: 'global_sync',
              owner_run_id: existingLock?.run_id,
              owner_invocation_id: existingLock?.invocation_id,
              our_invocation_id: INVOCATION_ID,
              lease_until: existingLock?.lease_until,
              origin: trigger,
              reason: 'lock_held_by_other_invocation'
            }
          });
        } catch (_) {}
        return new Response(JSON.stringify({ 
          status: 'yielded', reason: 'locked', message: 'resume_locked_skip', 
          run_id: runId,
          owner_run_id: existingLock?.run_id, 
          lease_until: existingLock?.lease_until,
          needs_resume: true
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
      
      // DETERMINISTIC selection: order by started_at DESC, id DESC
      const { data: existingRunning } = await supabase
        .from('sync_runs')
        .select('id, steps, started_at, trigger_type')
        .eq('status', 'running')
        .order('started_at', { ascending: false })
        .order('id', { ascending: false })
        .limit(1);
      
      if (existingRunning?.length) {
        const existing = existingRunning[0];
        console.log(`[orchestrator] Found existing running run ${existing.id}, resuming instead of creating new`);
        runId = existing.id;
        
        const lockOk = await acquireOrRenewGlobalLock(supabase, runId, lockTtlSeconds, INVOCATION_ID);
        
        if (!lockOk) {
          try {
            await supabase.rpc('log_sync_event', {
              p_run_id: runId,
              p_level: 'INFO',
              p_message: 'start_resuming_existing_run: lock occupato, resume delegato a cron-tick',
              p_details: { origin: 'manual' }
            });
          } catch (_) {}
          return new Response(JSON.stringify({ status: 'yielded', reason: 'locked', message: 'run in corso, resume delegato a cron-tick', run_id: runId, needs_resume: true }), 
            { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
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
    
    const lockAcquired = await acquireOrRenewGlobalLock(supabase, runId, lockTtlSeconds, INVOCATION_ID);
    
    if (!lockAcquired) {
      console.log('[orchestrator] INFO: Lock not acquired, sync already in progress');
      runId = null;
      return new Response(JSON.stringify({ status: 'yielded', reason: 'locked', message: 'not_started', run_id: null, needs_resume: true }), 
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
    // CRITICAL: null/undefined → true (backward compat, matches client)
    mediaworldIncludeEu: feeData?.mediaworld_include_eu == null ? true : !!feeData.mediaworld_include_eu,
    mediaworldItPrepDays: feeData?.mediaworld_it_preparation_days ?? 3,
    mediaworldEuPrepDays: feeData?.mediaworld_eu_preparation_days ?? 5,
    epriceIncludeEu: feeData?.eprice_include_eu == null ? true : !!feeData.eprice_include_eu,
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
    epriceShippingCost: feeData?.eprice_shipping_cost ?? null,
    // Amazon: null/undefined → true (backward compat)
    amazonIncludeEu: feeData?.amazon_include_eu == null ? true : !!feeData.amazon_include_eu,
    amazonItPrepDays: feeData?.amazon_it_preparation_days ?? 3,
    amazonEuPrepDays: feeData?.amazon_eu_preparation_days ?? 5,
    amazonFeeDrev: feeData?.amazon_fee_drev ?? null,
    amazonFeeMkt: feeData?.amazon_fee_mkt ?? null,
    amazonShippingCost: feeData?.amazon_shipping_cost ?? null
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
  
  const budgetExceeded = (stepName?: string) => {
    const limit = stepName === 'parse_merge' ? PARSE_MERGE_BUDGET_MS : ORCHESTRATOR_BUDGET_MS;
    return (Date.now() - orchestratorStart) > limit;
  };
  
  // Track if a step failed before notification - we still want to run notification
  let pipelineFailedBeforeNotification = false;
  let pipelineFailError = '';
  
  const makeResponse = (status: string, extra: Record<string, unknown> = {}): Response => {
    return new Response(JSON.stringify({ status, run_id: runId, ...extra }), 
      { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
  };
  
  const yieldResponse = async (currentStep: string, reason: string): Promise<Response> => {
    console.log(`[orchestrator] YIELD at step ${currentStep}: ${reason}`);
    
    // Atomic: persist current_step AND ensure steps[currentStep] exists
    await setStepInProgress(supabase, runId, currentStep);
    
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
          budget_ms: currentStep === 'parse_merge' ? PARSE_MERGE_BUDGET_MS : ORCHESTRATOR_BUDGET_MS,
          reason,
          cursor_pos_end: stepState.cursor_pos,
          file_total_size: stepState.materialBytes,
          chunk_index: stepState.chunk_index,
          resume_via: 'cron-tick'
        }
      });
    } catch (_) {}
    
    return makeResponse('yielded', { current_step: currentStep, needs_resume: true });
  };

  // ========== STEP 1: FTP Import ==========
  if (resumeFromStep === 'import_ftp') {
    await setStepInProgress(supabase, runId, 'import_ftp');
    console.log('[orchestrator] === STEP 1: FTP Import ===');
    try {
      await supabase.rpc('log_sync_event', {
        p_run_id: runId, p_level: 'INFO', p_message: 'step_started',
        p_details: { step: 'import_ftp' }
      });
    } catch (_) {}
    
    for (const fileType of ['material', 'stock', 'price', 'stockLocation']) {
      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', runStartTime, 'Interrotta dall\'utente');
        return makeResponse('failed_definitive', { error: 'Cancelled' });
      }
      
      if (budgetExceeded()) {
        return await yieldResponse('import_ftp', `budget exceeded during FTP import (${fileType})`);
      }
      
      const result = await callStep(supabaseUrl, supabaseServiceKey, 'import-catalog-ftp', { 
        fileType, run_id: runId
      });
      
      // 546 WORKER_LIMIT on import_ftp
      if (!result.ok && isWorkerLimit(result.http_status, result.body)) {
        const retryResult = await handleWorkerLimitRetry(supabase, runId, 'import_ftp', result, runStartTime);
        if (retryResult) return retryResult;
        // If null, retry limit exceeded - handled inside, but fallthrough to error
      }
      
      if (!result.ok && fileType !== 'stockLocation') {
        const errText = typeof result.body === 'string' ? result.body : JSON.stringify(result.body);
        await mergeStepState(supabase, runId, 'import_ftp', { status: 'failed', error: `FTP ${fileType}: ${errText.substring(0, 200)}` });
        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'ERROR', p_message: 'step_failed',
            p_details: { step: 'import_ftp', file_type: fileType, error: errText.substring(0, 200) }
          });
        } catch (_) {}
        pipelineFailedBeforeNotification = true;
        pipelineFailError = `FTP ${fileType}: HTTP ${result.http_status} ${errText.substring(0, 200)}`;
        // Jump to notification
        break;
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
        } catch (_) {}
      }
    }
    if (!pipelineFailedBeforeNotification) {
      await mergeStepState(supabase, runId, 'import_ftp', { status: 'completed', finished_at: new Date().toISOString() });
      try {
        await supabase.rpc('log_sync_event', {
          p_run_id: runId, p_level: 'INFO', p_message: 'step_completed',
          p_details: { step: 'import_ftp' }
        });
      } catch (_) {}
      resumeFromStep = 'parse_merge';
    }
  }

  // ========== STEP 2: PARSE_MERGE (CHUNKED) ==========
  if (!pipelineFailedBeforeNotification && resumeFromStep === 'parse_merge') {
    await setStepInProgress(supabase, runId, 'parse_merge');
    console.log('[orchestrator] === STEP 2: parse_merge (CHUNKED) ===');
    try {
      await supabase.rpc('log_sync_event', {
        p_run_id: runId, p_level: 'INFO', p_message: 'step_started',
        p_details: { step: 'parse_merge' }
      });
    } catch (_) {}
    
    let parseMergeComplete = false;
    let chunkCount = 0;
    
    while (!parseMergeComplete && chunkCount < MAX_PARSE_MERGE_CHUNKS) {
      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', runStartTime, 'Interrotta dall\'utente');
        return makeResponse('failed_definitive', { error: 'Cancelled' });
      }
      
      if (budgetExceeded('parse_merge')) {
        return await yieldResponse('parse_merge', `orchestrator budget exceeded after ${chunkCount} chunks this invocation`);
      }
      
      chunkCount++;
      console.log(`[orchestrator] parse_merge chunk ${chunkCount} (this invocation)...`);
      
      const result = await callStep(supabaseUrl, supabaseServiceKey, 'sync-step-runner', { 
        run_id: runId, step: 'parse_merge', fee_config: feeConfig, lock_invocation_id: INVOCATION_ID 
      });
      
      // 546 on parse_merge
      if (!result.ok && isWorkerLimit(result.http_status, result.body)) {
        const retryResult = await handleWorkerLimitRetry(supabase, runId, 'parse_merge', result, runStartTime);
        if (retryResult) return retryResult;
      }
      
      if (!result.ok) {
        const errText = typeof result.body === 'string' ? result.body : JSON.stringify(result.body);
        pipelineFailedBeforeNotification = true;
        pipelineFailError = `parse_merge: ${errText.substring(0, 200)}`;
        await mergeStepState(supabase, runId, 'parse_merge', { status: 'failed', error: pipelineFailError });
        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'ERROR', p_message: 'step_failed',
            p_details: { step: 'parse_merge', error: pipelineFailError }
          });
        } catch (_) {}
        break;
      }
      
      const verification = await verifyStepCompleted(supabase, runId, 'parse_merge');
      
      if (!verification.success) {
        pipelineFailedBeforeNotification = true;
        pipelineFailError = verification.error || 'parse_merge verification failed';
        await mergeStepState(supabase, runId, 'parse_merge', { status: 'failed', error: pipelineFailError });
        break;
      }
      
      if (verification.status === 'completed') {
        parseMergeComplete = true;
        console.log(`[orchestrator] parse_merge completed after ${chunkCount} chunks this invocation`);
      } else if (verification.status === 'in_progress') {
        console.log(`[orchestrator] parse_merge in_progress, continuing...`);
      } else {
        pipelineFailedBeforeNotification = true;
        pipelineFailError = `parse_merge unexpected status: ${verification.status}`;
        break;
      }
    }
    
    if (!pipelineFailedBeforeNotification && !parseMergeComplete) {
      const verification = await verifyStepCompleted(supabase, runId, 'parse_merge');
      if (verification.status === 'in_progress') {
        return await yieldResponse('parse_merge', `chunk limit ${MAX_PARSE_MERGE_CHUNKS} reached this invocation`);
      }
      pipelineFailedBeforeNotification = true;
      pipelineFailError = `parse_merge exceeded ${MAX_PARSE_MERGE_CHUNKS} chunks limit`;
    }
    if (!pipelineFailedBeforeNotification) {
      resumeFromStep = 'ean_mapping';
    }
  }

  // ========== STEPS 3+: Remaining processing steps ==========
  if (!pipelineFailedBeforeNotification) {
    const allRemainingSteps = [...ALL_STEPS_AFTER_PARSE];
    
    let startIdx = 0;
    if (resumeFromStep !== 'ean_mapping') {
      const idx = allRemainingSteps.indexOf(resumeFromStep);
      if (idx >= 0) startIdx = idx;
    }

    // ---- Legacy cleanup: remove steps.export_ean_xlsx_retry if present ----
    // Use atomic merge to null-out the legacy key without read-modify-write
    {
      const { data: runData } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
      if (runData?.steps && 'export_ean_xlsx_retry' in (runData.steps as Record<string, unknown>)) {
        await supabase.rpc('merge_sync_run_step', {
          p_run_id: runId,
          p_step_name: 'export_ean_xlsx_retry',
          p_patch: { _deleted: true }
        });
      }
    }

    for (let i = startIdx; i < allRemainingSteps.length; i++) {
      const step = allRemainingSteps[i];
      
      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', runStartTime, 'Interrotta dall\'utente');
        return makeResponse('failed_definitive', { error: 'Cancelled' });
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
          continue;
        }
      }

      // ---- retry_delay gate: check both stepState.status and stepState.retry.status ----
      {
        const { data: runData } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
        const stepState = runData?.steps?.[step] || {};
        const stepRetry = stepState.retry || {};

        // Check if step or its retry sub-object indicates retry_delay
        const isRetryDelay = stepState.status === 'retry_delay' || stepRetry.status === 'retry_delay';
        const nextRetryAt = stepRetry.next_retry_at || stepState.next_retry_at;
        
        if (isRetryDelay && nextRetryAt) {
          const nextRetryTime = new Date(nextRetryAt).getTime();
          const now = Date.now();
          if (now < nextRetryTime) {
            const waitSeconds = Math.max(1, Math.ceil((nextRetryTime - now) / 1000));
            console.log(`[orchestrator] ${step} in retry_delay, ${waitSeconds}s remaining`);
            try {
              await supabase.rpc('log_sync_event', {
                p_run_id: runId, p_level: 'INFO', p_message: 'step_yielded_retry_delay',
                p_details: { step, wait_seconds: waitSeconds, next_retry_at: nextRetryAt, attempt: stepRetry.retry_attempt }
              });
            } catch (_) {}
            return makeResponse('retry_delay', { 
              current_step: step, wait_seconds: waitSeconds, needs_resume: true, next_retry_at: nextRetryAt 
            });
          }
          console.log(`[orchestrator] ${step} retry_delay expired, retrying (attempt ${stepRetry.retry_attempt || 0})`);
        }
      }
      
      // ATOMIC: set current_step AND steps[step].status=in_progress in single write
      await setStepInProgress(supabase, runId, step);
      console.log(`[orchestrator] === STEP: ${step} ===`);
      try {
        await supabase.rpc('log_sync_event', {
          p_run_id: runId, p_level: 'INFO', p_message: 'step_started',
          p_details: { step }
        });
      } catch (_) {}
      
      const result = await callStep(supabaseUrl, supabaseServiceKey, 'sync-step-runner', { 
        run_id: runId, step, fee_config: feeConfig, lock_invocation_id: INVOCATION_ID 
      });
      
      // ---- 546 WORKER_LIMIT: generalized for ALL steps ----
      if (!result.ok && isWorkerLimit(result.http_status, result.body)) {
        const retryResult = await handleWorkerLimitRetry(supabase, runId, step, result, runStartTime);
        if (retryResult) return retryResult;
        // null means max retries exceeded - fall through to failure handling
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
        pipelineFailedBeforeNotification = true;
        pipelineFailError = errText;
        break; // Jump to notification
      }

      // ---- Yield on in_progress: step needs more invocations ----
      if (verification.status === 'in_progress') {
        console.log(`[orchestrator] ${step} still in_progress after callStep, yielding for resume`);
        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'INFO', p_message: 'step_yielded_in_progress',
            p_details: { step }
          });
        } catch (_) {}
        return await yieldResponse(step, `step ${step} still in_progress, needs resume`);
      }

      // ---- On success: clear retry state and log step_completed ----
      {
        const { data: runData } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
        const currentSteps = runData?.steps || {};
        const stepState = currentSteps[step] || {};
        const hadRetry = stepState.retry?.retry_attempt > 0;

        if (stepState.retry) {
          // Use atomic RPC to clear retry sub-key: merge with retry set to null
          await supabase.rpc('merge_sync_run_step', {
            p_run_id: runId,
            p_step_name: step,
            p_patch: { retry: null }
          });
        }

        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'INFO', p_message: 'step_completed',
            p_details: { step, had_retry: hadRetry, elapsed_ms: Date.now() - runStartTime }
          });
        } catch (_) {}
      }
    }
  }

  // ========== SFTP Upload (all triggers) ==========
  if (!pipelineFailedBeforeNotification) {
    const { data: sftpCheck } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
    const sftpState = sftpCheck?.steps?.upload_sftp;
    if (sftpState?.status === 'completed' || sftpState?.status === 'success') {
      console.log('[orchestrator] upload_sftp already completed, skipping');
    } else {
      // Validate SFTP env BEFORE marking in_progress (missing_env = immediate fail)
      const sftpHost = Deno.env.get('SFTP_HOST');
      const sftpUser = Deno.env.get('SFTP_USER');
      const sftpPassword = Deno.env.get('SFTP_PASSWORD');
      const sftpBaseDir = Deno.env.get('SFTP_BASE_DIR');

      if (!sftpHost || !sftpUser || !sftpPassword || !sftpBaseDir) {
        const missingEnv: string[] = [];
        if (!sftpHost) missingEnv.push('SFTP_HOST');
        if (!sftpUser) missingEnv.push('SFTP_USER');
        if (!sftpPassword) missingEnv.push('SFTP_PASSWORD');
        if (!sftpBaseDir) missingEnv.push('SFTP_BASE_DIR');
        console.log(`[orchestrator] SFTP not configured - failing run (missing: ${missingEnv.join(', ')})`);
        await setStepInProgress(supabase, runId, 'upload_sftp');
        await mergeStepState(supabase, runId, 'upload_sftp', { status: 'failed', code: 'missing_env', error: 'SFTP misconfigured', missing_env: missingEnv });
        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'ERROR', p_message: 'step_failed',
            p_details: { step: 'upload_sftp', code: 'missing_env', missing_env: missingEnv }
          });
        } catch (_) {}
        pipelineFailedBeforeNotification = true;
        pipelineFailError = 'SFTP misconfigured';
      } else {
        if (await isCancelRequested(supabase, runId)) {
          await finalizeRun(supabase, runId, 'failed', runStartTime, 'Interrotta dall\'utente');
          return makeResponse('failed_definitive', { error: 'Cancelled' });
        }
        
        if (budgetExceeded()) {
          return await yieldResponse('upload_sftp', 'orchestrator budget exceeded before SFTP');
        }
        
        await setStepInProgress(supabase, runId, 'upload_sftp');
        console.log('[orchestrator] === SFTP Upload ===');
        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'INFO', p_message: 'step_started',
            p_details: { step: 'upload_sftp' }
          });
        } catch (_) {}
        
        // === PRE-SFTP VALIDATION (fail fast) ===
        const REQUIRED_MARKETPLACE_XLSX = ['Catalogo EAN.xlsx', 'Export ePrice.xlsx', 'Export Mediaworld.xlsx'];
        const BLOCKED_CSV_NAMES = ['Export Mediaworld.csv', 'Export ePrice.csv', 'Catalogo EAN.csv'];
        const SFTP_WHITELIST = new Set(EXPORT_FILES);
        {
          // A) Whitelist check: exactly 5 files
          if (EXPORT_FILES.length !== 5) {
            const reason = `Pre-SFTP: EXPORT_FILES count ${EXPORT_FILES.length} !== 5`;
            console.error(`[orchestrator] ${reason}`);
            await mergeStepState(supabase, runId, 'upload_sftp', { status: 'failed', error: reason, validation: 'whitelist_count' });
            pipelineFailedBeforeNotification = true; pipelineFailError = reason;
          }
          
          // B) XLSX presenti nel bucket
          const { data: bucketContents } = await supabase.storage.from('exports').list('', { limit: 200 });
          const bucketNames = new Set((bucketContents || []).map((f: { name: string }) => f.name));
          const missingXlsx = REQUIRED_MARKETPLACE_XLSX.filter(f => !bucketNames.has(f));
          
          // C) Nessun CSV nella selezione SFTP
          const csvInSelection = EXPORT_FILES.filter(f => f.endsWith('.csv'));
          const blockedCsvInBucket = BLOCKED_CSV_NAMES.filter(f => bucketNames.has(f));
          
          // C-bis) Nessun CSV nel bucket (blocco extra)
          const allCsvInBucket = (bucketContents || [])
            .filter((f: { name: string }) => f.name.endsWith('.csv'))
            .map((f: { name: string }) => f.name);
          
          if (!pipelineFailedBeforeNotification && (missingXlsx.length > 0 || csvInSelection.length > 0)) {
            const reason = missingXlsx.length > 0
              ? `Pre-SFTP: file XLSX mancanti nel bucket: ${missingXlsx.join(', ')}`
              : `Pre-SFTP: file CSV non ammessi nella selezione SFTP: ${csvInSelection.join(', ')}`;
            console.error(`[orchestrator] ${reason}`);
            await mergeStepState(supabase, runId, 'upload_sftp', { status: 'failed', error: reason, validation: 'pre_sftp_check' });
            try { await supabase.rpc('log_sync_event', { p_run_id: runId, p_level: 'ERROR', p_message: 'sftp_pre_validation_failed', p_details: { step: 'upload_sftp', missing: missingXlsx, csv_blocked: csvInSelection, csv_in_bucket: blockedCsvInBucket } }); } catch (_) {}
            pipelineFailedBeforeNotification = true;
            pipelineFailError = reason;
          }
          
          // D) Check for extra files in SFTP selection (beyond whitelist)
          if (!pipelineFailedBeforeNotification) {
            const extraFiles = EXPORT_FILES.filter(f => !SFTP_WHITELIST.has(f));
            if (extraFiles.length > 0) {
              const reason = `Pre-SFTP: file extra nella selezione (non in whitelist): ${extraFiles.join(', ')}`;
              console.error(`[orchestrator] ${reason}`);
              await mergeStepState(supabase, runId, 'upload_sftp', { status: 'failed', error: reason, validation: 'extra_files' });
              pipelineFailedBeforeNotification = true; pipelineFailError = reason;
            }
          }
          
          // E) Check export step validation flags + verification_not_supported
          if (!pipelineFailedBeforeNotification) {
            const { data: runData } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
            const steps = runData?.steps || {};
            const exportStepsToCheck = ['export_ean_xlsx', 'export_mediaworld', 'export_eprice'];
            for (const es of exportStepsToCheck) {
              const stepState = steps[es];
              if (!stepState || (stepState.status !== 'success' && stepState.status !== 'completed')) {
                const reason = `Pre-SFTP: step ${es} non completato (status=${stepState?.status || 'missing'})`;
                console.error(`[orchestrator] ${reason}`);
                await mergeStepState(supabase, runId, 'upload_sftp', { status: 'failed', error: reason, validation: 'step_validation' });
                pipelineFailedBeforeNotification = true; pipelineFailError = reason;
                break;
              }
              if (stepState.validation_passed === false) {
                const reason = `Pre-SFTP: step ${es} template validation failed`;
                console.error(`[orchestrator] ${reason}`);
                await mergeStepState(supabase, runId, 'upload_sftp', { status: 'failed', error: reason, validation: 'template_identity' });
                try { await supabase.rpc('log_sync_event', { p_run_id: runId, p_level: 'ERROR', p_message: 'sftp_pre_validation_failed', p_details: { step: 'upload_sftp', failed_export: es, reason: 'template_identity_check_failed' } }); } catch (_) {}
                pipelineFailedBeforeNotification = true; pipelineFailError = reason;
                break;
              }
              // Check validation_passed must be explicitly true (not just absent)
              if (stepState.validation_passed !== true) {
                const reason = `Pre-SFTP: step ${es} missing validation_passed=true (got ${stepState.validation_passed})`;
                console.error(`[orchestrator] ${reason}`);
                await mergeStepState(supabase, runId, 'upload_sftp', { status: 'failed', error: reason, validation: 'missing_validation' });
                pipelineFailedBeforeNotification = true; pipelineFailError = reason;
                break;
              }
              // Check for verification_not_supported in error field (hard block)
              if (stepState.error && typeof stepState.error === 'string' && stepState.error.includes('verification_not_supported')) {
                const reason = `Pre-SFTP: step ${es} has verification_not_supported error: ${stepState.error}`;
                console.error(`[orchestrator] ${reason}`);
                await mergeStepState(supabase, runId, 'upload_sftp', { status: 'failed', error: reason, validation: 'verification_not_supported' });
                try { await supabase.rpc('log_sync_event', { p_run_id: runId, p_level: 'ERROR', p_message: 'sftp_pre_validation_failed', p_details: { step: 'upload_sftp', failed_export: es, reason: 'verification_not_supported' } }); } catch (_) {}
                pipelineFailedBeforeNotification = true; pipelineFailError = reason;
                break;
              }
              // Check for any validation warnings (hard block — no warnings allowed pre-SFTP)
              const stepWarnings = stepState.validation_warnings;
              if (Array.isArray(stepWarnings) && stepWarnings.length > 0) {
                const reason = `Pre-SFTP: step ${es} has ${stepWarnings.length} validation warning(s): ${stepWarnings.slice(0, 3).join('; ')}`;
                console.error(`[orchestrator] ${reason}`);
                await mergeStepState(supabase, runId, 'upload_sftp', { status: 'failed', error: reason, validation: 'validation_warnings_present' });
                try { await supabase.rpc('log_sync_event', { p_run_id: runId, p_level: 'ERROR', p_message: 'sftp_pre_validation_failed', p_details: { step: 'upload_sftp', failed_export: es, reason: 'validation_warnings', warnings: stepWarnings } }); } catch (_) {}
                pipelineFailedBeforeNotification = true; pipelineFailError = reason;
                break;
              }
            }
            
            // Log Amazon non-regression check + CSV audit
            if (!pipelineFailedBeforeNotification) {
              const amazonFiles = ['amazon_listing_loader.xlsm', 'amazon_price_inventory.txt'];
              const amazonPresent = amazonFiles.filter(f => bucketNames.has(f));
              const amazonMissing = amazonFiles.filter(f => !bucketNames.has(f));
              console.log(`[orchestrator] Amazon non-regression: present=${amazonPresent.join(',')}, missing=${amazonMissing.join(',')}`);
              if (allCsvInBucket.length > 0) {
                console.warn(`[orchestrator] CSV files in bucket (not in SFTP selection but present): ${allCsvInBucket.join(', ')}`);
              }
              try {
                await supabase.rpc('log_sync_event', { p_run_id: runId, p_level: 'INFO', p_message: 'sftp_pre_validation_passed', p_details: { step: 'upload_sftp', whitelist: EXPORT_FILES, amazon_present: amazonPresent, amazon_missing: amazonMissing, csv_in_bucket: allCsvInBucket } });
              } catch (logErr: unknown) {
                console.warn(`[orchestrator] Failed to log sftp_pre_validation_passed: ${logErr instanceof Error ? logErr.message : String(logErr)}`);
              }
            }
          }
        }
        
        if (!pipelineFailedBeforeNotification) {
        const sftpFiles = EXPORT_FILES.map(f => ({
          bucket: 'exports',
          path: f,
          filename: f
        }));
        
        const sftpResult = await callStep(supabaseUrl, supabaseServiceKey, 'upload-exports-to-sftp', {
          files: sftpFiles
        });
        
        // 546 on SFTP
        if (!sftpResult.ok && isWorkerLimit(sftpResult.http_status, sftpResult.body)) {
          const retryResult = await handleWorkerLimitRetry(supabase, runId, 'upload_sftp', sftpResult, runStartTime);
          if (retryResult) return retryResult;
        }
        
        if (!sftpResult.ok) {
          const sftpErr = typeof sftpResult.body === 'string' ? sftpResult.body : JSON.stringify(sftpResult.body);
          await mergeStepState(supabase, runId, 'upload_sftp', { status: 'failed', error: sftpErr.substring(0, 200) });
          try {
            await supabase.rpc('log_sync_event', {
              p_run_id: runId, p_level: 'ERROR', p_message: 'step_failed',
              p_details: { step: 'upload_sftp', error: sftpErr.substring(0, 200) }
            });
          } catch (_) {}
          pipelineFailedBeforeNotification = true;
          pipelineFailError = `SFTP: ${sftpErr.substring(0, 200)}`;
        } else {
          await mergeStepState(supabase, runId, 'upload_sftp', { status: 'completed', finished_at: new Date().toISOString() });
          try {
            await supabase.rpc('log_sync_event', {
              p_run_id: runId, p_level: 'INFO', p_message: 'step_completed',
              p_details: { step: 'upload_sftp', files_count: EXPORT_FILES.length }
            });
          } catch (_) {}
        }
        } // end if !pipelineFailedBeforeNotification (validation passed)
      }
    }
  }

  // ========== STORAGE VERSIONING ==========
  if (!pipelineFailedBeforeNotification) {
    const { data: verCheck } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
    const verState = verCheck?.steps?.versioning;
    if (verState?.status === 'completed' || verState?.status === 'success') {
      console.log('[orchestrator] versioning already completed, skipping');
    } else {
      // NOTE: no budgetExceeded() gate here — versioning is lightweight and must
      // not yield before starting, which caused unresumable stalls.
      // It runs inline regardless of remaining budget.
      
      await setStepInProgress(supabase, runId, 'versioning');
      console.log('[orchestrator] === Storage versioning ===');
      try {
        await supabase.rpc('log_sync_event', {
          p_run_id: runId, p_level: 'INFO', p_message: 'step_started',
          p_details: { step: 'versioning' }
        });
      } catch (_) {}
      
      const versioningStart = Date.now();
      try {
        // Stage: before_versioning_start
        try {
          const heapMb = Math.round((Deno as unknown as Record<string,unknown>).memoryUsage
            ? ((Deno as unknown as { memoryUsage: () => { heapUsed: number } }).memoryUsage().heapUsed / 1048576)
            : 0);
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'INFO', p_message: 'versioning_stage',
            p_details: { stage: 'before_versioning_start', heap_mb: heapMb, files_expected: EXPORT_FILES.length }
          });
        } catch (_) {}

        const versionTimestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const fileManifest: Record<string, string> = {};
        let filesVersioned = 0;
        
        for (const fileName of EXPORT_FILES) {
          try {
            const { data: fileBlob } = await supabase.storage.from('exports').download(fileName);
            if (fileBlob) {
              await supabase.storage.from('exports').upload(`latest/${fileName}`, fileBlob, { upsert: true });
              await supabase.storage.from('exports').upload(`versions/${versionTimestamp}/${fileName}`, fileBlob, { upsert: true });
              fileManifest[fileName] = versionTimestamp;
              filesVersioned++;
            }
          } catch (e: unknown) {
            console.warn(`[orchestrator] Versioning failed for ${fileName}: ${errMsg(e)}`);
          }
        }

        // Stage: after_inputs_loaded (files downloaded and re-uploaded)
        const afterInputsMs = Date.now() - versioningStart;
        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'INFO', p_message: 'versioning_stage',
            p_details: { stage: 'after_inputs_loaded', duration_ms: afterInputsMs, items_versioned: filesVersioned }
          });
        } catch (_) {}
        
        // Idempotent upsert: merge file_manifest into existing (doesn't clobber other fields)
        const { data: existingRun } = await supabase.from('sync_runs').select('file_manifest').eq('id', runId).single();
        const mergedManifest = { ...(existingRun?.file_manifest || {}), ...fileManifest };
        await supabase.from('sync_runs').update({ file_manifest: mergedManifest }).eq('id', runId);

        await cleanupVersions(supabase);

        // Stage: after_write_or_upsert
        const afterWriteMs = Date.now() - versioningStart;
        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'INFO', p_message: 'versioning_stage',
            p_details: { stage: 'after_write_or_upsert', duration_ms: afterWriteMs, rows_written: filesVersioned }
          });
        } catch (_) {}
        
        await mergeStepState(supabase, runId, 'versioning', { status: 'completed', files: filesVersioned, finished_at: new Date().toISOString() });

        // Stage: completed
        const completedMs = Date.now() - versioningStart;
        try {
          const heapMb2 = Math.round((Deno as unknown as Record<string,unknown>).memoryUsage
            ? ((Deno as unknown as { memoryUsage: () => { heapUsed: number } }).memoryUsage().heapUsed / 1048576)
            : 0);
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'INFO', p_message: 'versioning_stage',
            p_details: { stage: 'completed', duration_ms: completedMs, heap_mb: heapMb2, items_versioned: filesVersioned }
          });
        } catch (_) {}

        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'INFO', p_message: 'step_completed',
            p_details: { step: 'versioning', duration_ms: completedMs }
          });
        } catch (_) {}
      } catch (e: unknown) {
        console.error(`[orchestrator] Versioning error: ${errMsg(e)}`);
        await mergeStepState(supabase, runId, 'versioning', { status: 'failed', error: errMsg(e).substring(0, 200) });
        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'ERROR', p_message: 'step_failed',
            p_details: { step: 'versioning', reason: errMsg(e).substring(0, 200), duration_ms: Date.now() - versioningStart }
          });
        } catch (_) {}
        pipelineFailedBeforeNotification = true;
        pipelineFailError = `versioning: ${errMsg(e).substring(0, 200)}`;
      }
    }
  }

  // ========== NOTIFICATION (ALWAYS runs, even on failure - blocca-run) ==========
  {
    const { data: notifCheck } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
    const notifState = notifCheck?.steps?.notification;
    if (notifState?.status === 'completed' || notifState?.status === 'success') {
      console.log('[orchestrator] notification already completed, skipping');
    } else {
      if (budgetExceeded()) {
        return await yieldResponse('notification', 'orchestrator budget exceeded before notification');
      }

      await setStepInProgress(supabase, runId, 'notification');
      try {
        await supabase.rpc('log_sync_event', {
          p_run_id: runId, p_level: 'INFO', p_message: 'step_started',
          p_details: { step: 'notification' }
        });
      } catch (_) {}
      
      try {
        // Determine preliminary status for notification content
        const { data: preNotifRun } = await supabase.from('sync_runs').select('steps, warning_count').eq('id', runId).single();
        const preNotifSteps = preNotifRun?.steps || {};
        const preExpected = getExpectedSteps(trigger);
        let prelimStatus = pipelineFailedBeforeNotification ? 'failed' : 'success';
        if (!pipelineFailedBeforeNotification) {
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
        }

        const notifResult = await callStep(supabaseUrl, supabaseServiceKey, 'send-sync-notification', {
          run_id: runId, status: prelimStatus
        });
        
        const notifBody = notifResult.body as Record<string, unknown> | null;
        const notifBodyStatus = notifBody?.status;
        const isNotifOk = notifResult.ok && notifBodyStatus !== 'failed';
        
        if (isNotifOk) {
          await mergeStepState(supabase, runId, 'notification', { status: 'completed', finished_at: new Date().toISOString() });
          try {
            await supabase.rpc('log_sync_event', {
              p_run_id: runId, p_level: 'INFO', p_message: 'step_completed',
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
              p_run_id: runId, p_level: 'ERROR', p_message: 'step_failed',
              p_details: { step: 'notification', error: notifErr.substring(0, 200), missing_env: notifBody?.missing_env }
            });
          } catch (_) {}
          console.warn(`[orchestrator] Notification failed: ${notifErr.substring(0, 200)}`);
        }
      } catch (e: unknown) {
        await mergeStepState(supabase, runId, 'notification', { status: 'failed', error: errMsg(e).substring(0, 200) });
        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'ERROR', p_message: 'step_failed',
            p_details: { step: 'notification', error: errMsg(e).substring(0, 200) }
          });
        } catch (_) {}
        console.warn(`[orchestrator] Notification failed: ${errMsg(e)}`);
      }
    }
  }

  // ========== COMPLETENESS CHECK & FINAL STATUS ==========
  // All 13 steps including notification must be completed/success. No skipped allowed.
  const { data: finalRun } = await supabase.from('sync_runs').select('steps, warning_count').eq('id', runId).single();
  const finalSteps = finalRun?.steps || {};
  const expectedSteps = getExpectedSteps(trigger);

  const missingSteps: string[] = [];
  const incompleteSteps: string[] = [];
  for (const s of expectedSteps) {
    const st = finalSteps[s];
    if (!st || (typeof st === 'object' && !st.status)) {
      missingSteps.push(s);
    } else if (typeof st === 'object' && st.status !== 'completed' && st.status !== 'success') {
      incompleteSteps.push(s);
    }
  }

  const allStepsSuccess = missingSteps.length === 0 && incompleteSteps.length === 0;
  let finalStatus: string;
  let realWarningCount = finalRun?.warning_count || 0;

  if (!allStepsSuccess) {
    const errorMsg = pipelineFailError || `Pipeline incomplete: missing=[${missingSteps.join(',')}] incomplete=[${incompleteSteps.join(',')}]`;
    console.warn(`[orchestrator] ${errorMsg}`);
    try {
      await supabase.rpc('log_sync_event', {
        p_run_id: runId, p_level: 'ERROR', p_message: 'pipeline_incomplete',
        p_details: { missing_steps: missingSteps, incomplete_steps: incompleteSteps, expected: expectedSteps.length }
      });
    } catch (_) {}
    finalStatus = 'failed';
    await finalizeRun(supabase, runId, 'failed', runStartTime, errorMsg);
  } else {
    // Recalculate real warnings: exclude operational/diagnostic WARN events
    // that are normal during yield/resume cycles and don't represent actual data issues
    const OPERATIONAL_WARN_MESSAGES = [
      'orchestrator_yield_scheduled', 'drain_loop_incomplete', 'step_retry_scheduled',
      'resume_failed_http', 'lock_ownership_lost', 'yielded_locked',
      'multiple_running_detected', 'cron_auth_failed'
    ];

    try {
      const { data: allWarns } = await supabase
        .from('sync_events')
        .select('message')
        .eq('run_id', runId)
        .eq('level', 'WARN')
        .limit(500);
      realWarningCount = (allWarns || []).filter(
        (e: { message: string }) => !OPERATIONAL_WARN_MESSAGES.includes(e.message)
      ).length;
    } catch (_) {
      // Fallback: keep existing warning_count
      realWarningCount = finalRun?.warning_count || 0;
    }

    finalStatus = realWarningCount > 0 ? 'success_with_warning' : 'success';

    // Direct update with corrected warning_count (bypass finalizeRun to include warning_count)
    await supabase.from('sync_runs').update({
      status: finalStatus,
      finished_at: new Date().toISOString(),
      runtime_ms: Date.now() - runStartTime,
      error_message: null,
      warning_count: realWarningCount
    }).eq('id', runId);
    console.log(`[orchestrator] Run ${runId} finalized: ${finalStatus} (real_warnings=${realWarningCount})`);
  }

  // Emit run_finalized event with diagnostic counters
  try {
    let yieldCount = 0;
    let drainCapCount = 0;
    try {
      const { data: yieldEvts } = await supabase.from('sync_events').select('id').eq('run_id', runId).eq('message', 'orchestrator_yield_scheduled').limit(100);
      const { data: drainEvts } = await supabase.from('sync_events').select('id').eq('run_id', runId).eq('message', 'drain_loop_incomplete').limit(100);
      yieldCount = yieldEvts?.length || 0;
      drainCapCount = drainEvts?.length || 0;
    } catch (_) {}

    await supabase.rpc('log_sync_event', {
      p_run_id: runId,
      p_level: 'INFO',
      p_message: 'run_finalized',
      p_details: {
        status: finalStatus,
        real_warning_count: realWarningCount,
        yield_count: yieldCount,
        drain_cap_count: drainCapCount,
        finished_at: new Date().toISOString(),
        source: 'orchestrator'
      }
    });
  } catch (_) {}

  console.log(`[orchestrator] Pipeline completed: ${runId}, status: ${finalStatus}`);
  
  // Use standardized response status
  const responseStatus = finalStatus === 'failed' ? 'failed_definitive' : 'completed';
  return makeResponse(responseStatus, { final_status: finalStatus });
}

// ============================================================
// 546 WORKER_LIMIT RETRY HANDLER (generalized for all steps)
// Returns a Response to yield/fail, or null if max retries exceeded (caller handles)
// ============================================================
async function handleWorkerLimitRetry(
  supabase: SupabaseClient,
  runId: string,
  step: string,
  result: { ok: boolean; http_status: number; body: unknown },
  _runStartTime: number
): Promise<Response | null> {
  const { data: runData } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
  const currentSteps = runData?.steps || {};
  const stepState = (currentSteps[step] || {}) as Record<string, unknown>;
  const prevRetry = (stepState.retry || {}) as Record<string, unknown>;
  const nextAttempt = ((prevRetry.retry_attempt as number) || 0) + 1;

  if (nextAttempt > STEP_MAX_RETRIES) {
    const failMsg = `Step ${step} failed: WORKER_LIMIT persistent after ${STEP_MAX_RETRIES} retries (HTTP 546)`;
    console.log(`[orchestrator] ${failMsg}`);
    await mergeStepState(supabase, runId, step, { 
      status: 'failed', 
      code: 'worker_limit_exhausted',
      error: failMsg,
      retry: { retry_attempt: nextAttempt - 1, status: 'exhausted' }
    });
    try {
      await supabase.rpc('log_sync_event', {
        p_run_id: runId, p_level: 'ERROR', p_message: 'step_failed',
        p_details: { step, code: 'worker_limit_exhausted', retry_attempt: nextAttempt - 1, http_status: 546 }
      });
    } catch (_) {}
    return null; // Caller handles pipeline failure
  }

  const backoffSec = stepBackoffSeconds(nextAttempt);
  const nextRetryAt = new Date(Date.now() + backoffSec * 1000).toISOString();
  
  // Deep merge retry state into step
  await mergeStepState(supabase, runId, step, {
    status: 'retry_delay',
    retry: {
      retry_attempt: nextAttempt,
      next_retry_at: nextRetryAt,
      last_http_status: 546,
      last_error: 'worker_limit_546',
      status: 'retry_delay'
    }
  });

  // Extract body snippet for observability (max 1000 chars, no secrets)
  let bodySnippet = '';
  try {
    const raw = typeof result.body === 'string' ? result.body : JSON.stringify(result.body);
    bodySnippet = raw.substring(0, 1000);
  } catch { bodySnippet = '[unparseable]'; }

  console.log(`[orchestrator] ${step} WORKER_LIMIT retry ${nextAttempt}/${STEP_MAX_RETRIES}, backoff ${backoffSec}s`);
  try {
    await supabase.rpc('log_sync_event', {
      p_run_id: runId, p_level: 'WARN', p_message: 'step_retry_scheduled',
      p_details: { 
        step, code: 'WORKER_LIMIT', retry_attempt: nextAttempt, 
        backoff_seconds: backoffSec, next_retry_at: nextRetryAt,
        http_status: result.http_status, body_snippet: bodySnippet
      }
    });
  } catch (_) {}

  return new Response(JSON.stringify({
    status: 'retry_delay', run_id: runId, current_step: step,
    wait_seconds: backoffSec, needs_resume: true
  }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
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
