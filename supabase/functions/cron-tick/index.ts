import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";
import { getExpectedSteps } from "../_shared/expectedSteps.ts";

/**
 * cron-tick - Called every 1 minute by GitHub Actions
 * 
 * Responsibilities:
 * 1. Authenticate ONLY via x-cron-secret header
 * 2. Deterministically select the most recent running run (by started_at DESC, id DESC)
 * 3. Resume yielded/stalled runs based on thresholds:
 *    - active_window_sec=60: skip resume if last event < 60s ago
 *    - stall_threshold_sec=180: force resume if last event > 180s ago
 *    - idle_timeout_minutes=30: mark timeout if no progress for 30min
 *    - hard_timeout_minutes=2*run_timeout_minutes: absolute timeout cap
 * 4. Handle multiple running runs (timeout old idle ones)
 * 5. Check if sync is due (schedule_type, frequency, daily_time)
 * 6. Manage retry logic and auto-disable after max_attempts failures
 * 7. All decisions produce persistent events in sync_events
 */

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type, x-cron-secret',
};

// Thresholds (seconds)
const ACTIVE_WINDOW_SEC = 60;
const STALL_THRESHOLD_SEC = 180;
const IDLE_TIMEOUT_MINUTES = 30;
const FINGERPRINT_SALT = "cron-fp-v1";
let _tickStartMs = 0; // set per request for jsonResp duration calc

function errMsg(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

/** Compute SHA-256 fingerprint (first 12 hex chars) of input string. */
async function sha256fp12(input: string): Promise<string> {
  const data = new TextEncoder().encode(input);
  const hash = await crypto.subtle.digest('SHA-256', data);
  const hex = Array.from(new Uint8Array(hash)).map(b => b.toString(16).padStart(2, '0')).join('');
  return hex.substring(0, 12);
}

function toRomeTime(date: Date): { hours: number; minutes: number; dateStr: string } {
  const rome = date.toLocaleString('en-GB', { timeZone: 'Europe/Rome', hour12: false });
  const parts = rome.split(', ');
  const timeParts = parts[1].split(':');
  return {
    hours: parseInt(timeParts[0]),
    minutes: parseInt(timeParts[1]),
    dateStr: parts[0]
  };
}

type SClient = ReturnType<typeof createClient>;

/** Log a persistent decision event. Non-blocking: never throws. */
async function logDecision(
  supabase: SClient,
  runId: string,
  level: 'INFO' | 'WARN' | 'ERROR',
  message: string,
  details: Record<string, unknown>
): Promise<void> {
  try {
    await supabase.rpc('log_sync_event', {
      p_run_id: runId,
      p_level: level,
      p_message: message,
      p_details: details
    });
  } catch (e) {
    console.warn(`[cron-tick] log_sync_event failed for ${message}:`, errMsg(e));
  }
}

serve(async (req) => {
  if (req.method === 'OPTIONS') return new Response(null, { headers: corsHeaders });
  _tickStartMs = Date.now();

  try {
    // 1. AUTHENTICATE with diagnostics
    const cronSecret = Deno.env.get('CRON_SECRET');
    const providedSecret = req.headers.get('x-cron-secret');
    const hasSecretHeader = providedSecret !== null;
    const providedLen = providedSecret?.length ?? 0;
    const expectedLen = cronSecret?.length ?? 0;
    const providedFp12 = hasSecretHeader && providedSecret ? await sha256fp12(providedSecret + FINGERPRINT_SALT) : '';
    const expectedFp12 = cronSecret ? await sha256fp12(cronSecret + FINGERPRINT_SALT) : '';
    const mismatch = !cronSecret || !providedSecret || providedFp12 !== expectedFp12;
    const userAgent = req.headers.get('user-agent') || '';

    console.log(JSON.stringify({
      diag_tag: 'cron_auth_diag',
      has_secret_header: hasSecretHeader,
      provided_secret_len: providedLen,
      expected_secret_len: expectedLen,
      provided_secret_fp12: providedFp12,
      expected_secret_fp12: expectedFp12,
      mismatch,
      user_agent: userAgent
    }));

    if (!cronSecret || !providedSecret || providedSecret !== cronSecret) {
      console.log('[cron-tick] Auth failed: 401');

      // Persist WARN on most recent running run if exists
      try {
        const supabaseUrl = Deno.env.get('SUPABASE_URL')!;
        const supabaseServiceKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
        const sb = createClient(supabaseUrl, supabaseServiceKey);
        const { data: runningRuns } = await sb
          .from('sync_runs')
          .select('id')
          .eq('status', 'running')
          .order('started_at', { ascending: false })
          .limit(1);
        if (runningRuns?.[0]) {
          await logDecision(sb, runningRuns[0].id, 'WARN', 'cron_auth_failed', {
            has_secret_header: hasSecretHeader,
            provided_secret_len: providedLen,
            expected_secret_len: expectedLen,
            provided_secret_fp12: providedFp12,
            expected_secret_fp12: expectedFp12,
            mismatch,
            user_agent: userAgent
          });
        }
      } catch (_) { /* non-blocking */ }

      return jsonResp({ status: 'error', message: 'Unauthorized' }, 401);
    }

    const supabaseUrl = Deno.env.get('SUPABASE_URL')!;
    const supabaseServiceKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
    const supabase = createClient(supabaseUrl, supabaseServiceKey);

    const now = new Date();
    console.log(JSON.stringify({
      diag_tag: 'tick_started',
      ts: now.toISOString()
    }));

    // 2. LOAD CONFIG
    const { data: config, error: configError } = await supabase
      .from('sync_config')
      .select('*')
      .eq('id', 1)
      .single();

    if (configError || !config) {
      console.error('[cron-tick] Failed to load config:', configError?.message);
      return jsonResp({ status: 'error', message: 'Config not found' }, 500);
    }

    const timeoutMinutes = config.run_timeout_minutes || 60;
    const hardTimeoutMinutes = 2 * timeoutMinutes;
    // Clamp max_attempts to 5 (DB trigger enforces this on write, but clamp on read for safety)
    const MAX_ATTEMPTS_CAP = 5;
    const rawMaxAttempts = config.max_attempts || 3;
    const maxAttempts = Math.min(rawMaxAttempts, MAX_ATTEMPTS_CAP);
    if (rawMaxAttempts > MAX_ATTEMPTS_CAP) {
      console.warn(`[cron-tick] WARN: max_attempts=${rawMaxAttempts} exceeds cap=${MAX_ATTEMPTS_CAP}, clamping to ${MAX_ATTEMPTS_CAP}`);
    }
    const retryDelay = (config.retry_delay_minutes || 5) * 60 * 1000;

    // 3. DETERMINISTIC RUN SELECTION: most recent running run
    const { data: activeRuns } = await supabase
      .from('sync_runs')
      .select('id, started_at, attempt, trigger_type, steps')
      .eq('status', 'running')
      .order('started_at', { ascending: false })
      .limit(11);

    const activeRun = activeRuns?.[0] || null;
    const olderRuns = (activeRuns || []).slice(1);

    // 3b. Handle older running runs (multi-running policy)
    if (olderRuns.length > 0 && activeRun) {
      await logDecision(supabase, activeRun.id, 'WARN', 'multiple_running_detected', {
        active_run_id: activeRun.id,
        older_run_ids: olderRuns.map((r: { id: string }) => r.id),
        count: olderRuns.length + 1
      });

      for (const oldRun of olderRuns) {
        const oldAge = (now.getTime() - new Date(oldRun.started_at).getTime()) / 60000;
        if (oldAge > IDLE_TIMEOUT_MINUTES) {
          // Check last event age for this old run
          const { data: oldEvents } = await supabase
            .from('sync_events')
            .select('created_at')
            .eq('run_id', oldRun.id)
            .order('created_at', { ascending: false })
            .limit(1);
          const oldLastEventAge = oldEvents?.[0]
            ? (now.getTime() - new Date(oldEvents[0].created_at).getTime()) / 1000
            : oldAge * 60;

          if (oldLastEventAge > IDLE_TIMEOUT_MINUTES * 60) {
            console.log(`[cron-tick] Marking old run ${oldRun.id} as timeout (idle ${Math.round(oldAge)}min)`);
            await supabase.from('sync_runs').update({
              status: 'timeout',
              finished_at: now.toISOString(),
              runtime_ms: Math.round(oldAge * 60000),
              error_message: `Timeout: run secondaria idle per ${Math.round(oldAge)} minuti`
            }).eq('id', oldRun.id);

            await logDecision(supabase, oldRun.id, 'ERROR', 'older_running_timeout_marked', {
              run_age_minutes: Math.round(oldAge),
              last_event_age_s: Math.round(oldLastEventAge),
              idle_timeout_minutes: IDLE_TIMEOUT_MINUTES
            });

            // Release lock if held by this old run
            try {
              await supabase.rpc('release_sync_lock', { p_lock_name: 'global_sync', p_run_id: oldRun.id });
            } catch (_) {}
          }
        }
      }
    }

    // 4. PROCESS ACTIVE RUN
    if (activeRun) {
      const runAgeMs = now.getTime() - new Date(activeRun.started_at).getTime();
      const runAgeMin = runAgeMs / 60000;
      const currentStep = (activeRun.steps as Record<string, unknown>)?.current_step as string | undefined;
      const parseMergeState = (activeRun.steps as Record<string, unknown>)?.parse_merge as Record<string, unknown> | undefined;
      const cursorPos = parseMergeState?.cursor_pos ?? null;
      const chunkIndex = parseMergeState?.chunk_index ?? null;
      const parseMergeStatus = parseMergeState?.status as string | undefined;

      // Get last PROGRESS event (exclude skip/diagnostic events for accurate age calculation)
      const { data: lastProgressEvents } = await supabase
        .from('sync_events')
        .select('created_at, message, details, level')
        .eq('run_id', activeRun.id)
        .not('message', 'like', 'resume_skipped_%')
        .not('message', 'like', 'cron_auth_%')
        .not('message', 'like', 'drain_%')
        .order('created_at', { ascending: false })
        .limit(1);

      const lastEvent = lastProgressEvents?.[0];
      const lastEventAgeS = lastEvent
        ? (now.getTime() - new Date(lastEvent.created_at).getTime()) / 1000
        : runAgeMs / 1000;
      const lastMsg = (lastEvent?.message || '').toLowerCase();
      const isYieldEvent = lastMsg.includes('yield') || lastMsg.includes('orchestrator_yield');

      const baseDetails = {
        run_id: activeRun.id,
        trigger_type: activeRun.trigger_type,
        run_age_s: Math.round(runAgeMs / 1000),
        last_event_age_s: Math.round(lastEventAgeS),
        current_step: currentStep,
        parse_merge_status: parseMergeStatus,
        cursor_pos: cursorPos,
        chunk_index: chunkIndex
      };

      // HARD TIMEOUT: absolute cap
      if (runAgeMin > hardTimeoutMinutes) {
        console.log(`[cron-tick] Run ${activeRun.id} HARD timeout (${Math.round(runAgeMin)}min > ${hardTimeoutMinutes}min)`);
        await supabase.from('sync_runs').update({
          status: 'timeout',
          finished_at: now.toISOString(),
          runtime_ms: Math.round(runAgeMs),
          error_message: `Timeout: superati ${hardTimeoutMinutes} minuti (hard cap)`
        }).eq('id', activeRun.id);

        await logDecision(supabase, activeRun.id, 'ERROR', 'timeout_marked', {
          ...baseDetails,
          reason: 'hard_timeout',
          hard_timeout_minutes: hardTimeoutMinutes,
          idle_timeout_minutes: IDLE_TIMEOUT_MINUTES
        });

        try {
          await supabase.rpc('release_sync_lock', { p_lock_name: 'global_sync', p_run_id: activeRun.id });
        } catch (_) {}

        // Reload cron runs after marking timeout, then check max attempts
        const { data: refreshedRuns1 } = await supabase
          .from('sync_runs')
          .select('id, status, attempt, finished_at, started_at, trigger_type, error_message, cancelled_by_user')
          .eq('trigger_type', 'cron')
          .order('started_at', { ascending: false })
          .limit(50);
        await checkAndHandleMaxAttempts(supabase, supabaseUrl, supabaseServiceKey, maxAttempts, refreshedRuns1 || [], config.updated_at as string);
        await callNotification(supabaseUrl, supabaseServiceKey, activeRun.id, 'timeout');

        return jsonResp({ status: 'timeout_marked', run_id: activeRun.id, reason: 'hard_timeout' });
      }

      // IDLE TIMEOUT: no progress for 30min
      if (runAgeMin > IDLE_TIMEOUT_MINUTES && lastEventAgeS > IDLE_TIMEOUT_MINUTES * 60) {
        // Check for recent progress events in last 30min (parse_merge specific)
        let hasRecentProgress = false;
        if (currentStep === 'parse_merge') {
          const { data: progressEvents, error: _pe } = await supabase
            .from('sync_events')
            .select('id')
            .eq('run_id', activeRun.id)
            .gte('created_at', new Date(now.getTime() - IDLE_TIMEOUT_MINUTES * 60 * 1000).toISOString())
            .limit(1);
          hasRecentProgress = (progressEvents?.length || 0) > 0;
        }

        if (!hasRecentProgress) {
          console.log(`[cron-tick] Run ${activeRun.id} IDLE timeout (no progress for ${IDLE_TIMEOUT_MINUTES}min)`);
          await supabase.from('sync_runs').update({
            status: 'timeout',
            finished_at: now.toISOString(),
            runtime_ms: Math.round(runAgeMs),
            error_message: `Timeout: nessun progresso per ${IDLE_TIMEOUT_MINUTES} minuti`
          }).eq('id', activeRun.id);

          await logDecision(supabase, activeRun.id, 'ERROR', 'timeout_marked', {
            ...baseDetails,
            reason: 'idle_timeout',
            idle_timeout_minutes: IDLE_TIMEOUT_MINUTES,
            hard_timeout_minutes: hardTimeoutMinutes
          });

          try {
            await supabase.rpc('release_sync_lock', { p_lock_name: 'global_sync', p_run_id: activeRun.id });
          } catch (_) {}

          // Reload cron runs after marking timeout, then check max attempts
          const { data: refreshedRuns2 } = await supabase
            .from('sync_runs')
            .select('id, status, attempt, finished_at, started_at, trigger_type, error_message, cancelled_by_user')
            .eq('trigger_type', 'cron')
            .order('started_at', { ascending: false })
            .limit(50);
          await checkAndHandleMaxAttempts(supabase, supabaseUrl, supabaseServiceKey, maxAttempts, refreshedRuns2 || [], config.updated_at as string);
          await callNotification(supabaseUrl, supabaseServiceKey, activeRun.id, 'timeout');

          return jsonResp({ status: 'timeout_marked', run_id: activeRun.id, reason: 'idle_timeout' });
        }
      }

      // YIELD FAST-PATH: if last progress event is a yield and > 5s old, bypass ALL gating
      // This ensures yields (especially "budget exceeded before step") are resumed immediately
      const YIELD_DEBOUNCE_SEC = 5;
      if (isYieldEvent && lastEventAgeS > YIELD_DEBOUNCE_SEC) {
        console.log(`[cron-tick] Yield fast-path: bypassing gating (event=${lastEvent?.message}, age=${Math.round(lastEventAgeS)}s)`);
      } else {
        // ACTIVE WINDOW: last progress event < 60s ago → skip resume
        if (lastEventAgeS <= ACTIVE_WINDOW_SEC) {
          console.log(`[cron-tick] Run ${activeRun.id} active (last progress event ${Math.round(lastEventAgeS)}s ago), skipping resume`);
          console.log(JSON.stringify({
            diag_tag: 'cron_tick_decision',
            run_id: activeRun.id,
            decision: 'skip',
            reason: 'active_window',
            last_event_age_s: Math.round(lastEventAgeS),
            progress_source: 'last_progress_event',
            current_step: currentStep,
            chunk_index: chunkIndex
          }));
          await logDecision(supabase, activeRun.id, 'INFO', 'resume_skipped_active_window', {
            ...baseDetails,
            active_window_sec: ACTIVE_WINDOW_SEC,
            reason: 'last_progress_event_within_active_window'
          });
          return jsonResp({ status: 'resume_skipped_active_window', run_id: activeRun.id, last_event_age_s: Math.round(lastEventAgeS) });
        }

        // STALL WINDOW (60-180s): don't resume yet unless yielded (handled above)
        if (lastEventAgeS <= STALL_THRESHOLD_SEC) {
          console.log(`[cron-tick] Run ${activeRun.id} within stall window (${Math.round(lastEventAgeS)}s), skipping resume`);
          console.log(JSON.stringify({
            diag_tag: 'cron_tick_decision',
            run_id: activeRun.id,
            decision: 'skip',
            reason: 'stall_window',
            last_event_age_s: Math.round(lastEventAgeS),
            progress_source: 'last_progress_event',
            current_step: currentStep,
            chunk_index: chunkIndex
          }));
          await logDecision(supabase, activeRun.id, 'INFO', 'resume_skipped_within_stall_window', {
            ...baseDetails,
            stall_threshold_sec: STALL_THRESHOLD_SEC,
            reason: 'within_stall_window'
          });
          return jsonResp({ status: 'resume_skipped_within_stall_window', run_id: activeRun.id, last_event_age_s: Math.round(lastEventAgeS) });
        }
      }

      // RESUME: either yield fast-path or stalled (> 180s)
      const isStalled = lastEventAgeS > STALL_THRESHOLD_SEC;
      const resumeReason = isYieldEvent ? 'yield_observed' : 'stall_threshold';

      // Structured diagnostic log (Modifica B)
      console.log(JSON.stringify({
        diag_tag: 'cron_tick_decision',
        run_id: activeRun.id,
        decision: 'force_resume',
        reason: resumeReason,
        last_event_age_s: Math.round(lastEventAgeS),
        progress_source: isYieldEvent ? 'last_progress_event_yield' : 'last_progress_event_stalled',
        current_step: currentStep,
        chunk_index: chunkIndex
      }));

      // PRE-RESUME GUARD: check retry_delay from DB state BEFORE calling orchestrator
      // This prevents unnecessary orchestrator invocations every minute when step is not due
      {
        const currentStepState = (activeRun.steps as Record<string, unknown>)?.[currentStep as string] as Record<string, unknown> | undefined;
        const stepRetry = currentStepState?.retry as Record<string, unknown> | undefined;
        const isStepRetryDelay = currentStepState?.status === 'retry_delay' || stepRetry?.status === 'retry_delay';
        const nextRetryAt = (stepRetry?.next_retry_at || currentStepState?.next_retry_at) as string | undefined;

        if (isStepRetryDelay && nextRetryAt) {
          const nextRetryTime = new Date(nextRetryAt).getTime();
          if (now.getTime() < nextRetryTime) {
            const waitSeconds = Math.ceil((nextRetryTime - now.getTime()) / 1000);
            console.log(`[cron-tick] Pre-resume guard: ${currentStep} retry_delay not due (${waitSeconds}s remaining)`);
            await logDecision(supabase, activeRun.id, 'INFO', 'resume_skipped_not_due', {
              ...baseDetails,
              origin: 'cron',
              now_iso: now.toISOString(),
              next_retry_at: nextRetryAt,
              seconds_remaining: waitSeconds,
              source: 'pre_resume_db_check'
            });
            return jsonResp({
              status: 'retry_delay',
              run_id: activeRun.id,
              wait_seconds: waitSeconds,
              current_step: currentStep,
              next_retry_at: nextRetryAt,
              needs_resume: true
            });
          }
        }
      }

      // LOGICALLY COMPLETED GUARDRAIL: if all steps are done, finalize idempotently without re-executing
      {
        const { data: freshRun } = await supabase.from('sync_runs').select('steps, started_at, warning_count').eq('id', activeRun.id).single();
        const runSteps = (freshRun?.steps || {}) as Record<string, unknown>;
        const expectedSteps = getExpectedSteps(activeRun.trigger_type);
        const allComplete = expectedSteps.every(s => {
          const st = runSteps[s] as Record<string, unknown> | undefined;
          return st && (st.status === 'completed' || st.status === 'success');
        });

        if (allComplete) {
          console.log(`[cron-tick] Run ${activeRun.id} logically completed (all ${expectedSteps.length} steps done), finalizing idempotently`);

          // Recalculate real warnings (exclude operational diagnostic events)
          const OPERATIONAL_WARN_MESSAGES = [
            'orchestrator_yield_scheduled', 'drain_loop_incomplete', 'step_retry_scheduled',
            'resume_failed_http', 'lock_ownership_lost', 'yielded_locked',
            'multiple_running_detected', 'cron_auth_failed'
          ];
          let realWarningCount = freshRun?.warning_count || 0;
          try {
            const { data: allWarns } = await supabase
              .from('sync_events')
              .select('message')
              .eq('run_id', activeRun.id)
              .eq('level', 'WARN')
              .limit(500);
            realWarningCount = (allWarns || []).filter(
              (e: { message: string }) => !OPERATIONAL_WARN_MESSAGES.includes(e.message)
            ).length;
          } catch (_) { /* fallback to existing count */ }

          const finalStatus = realWarningCount > 0 ? 'success_with_warning' : 'success';
          const runtimeMs = Math.round(now.getTime() - new Date(freshRun?.started_at || activeRun.started_at).getTime());

          // Idempotent: only update if still running
          await supabase.from('sync_runs').update({
            status: finalStatus,
            finished_at: now.toISOString(),
            runtime_ms: runtimeMs,
            warning_count: realWarningCount,
            error_message: null
          }).eq('id', activeRun.id).eq('status', 'running');

          // Release lock
          try {
            await supabase.rpc('release_sync_lock', { p_lock_name: 'global_sync', p_run_id: activeRun.id });
          } catch (_) {}

          // Count yield and drain-cap events for diagnostics
          let yieldCount = 0;
          let drainCapCount = 0;
          try {
            const { data: yieldEvts } = await supabase.from('sync_events').select('id').eq('run_id', activeRun.id).eq('message', 'orchestrator_yield_scheduled').limit(100);
            const { data: drainEvts } = await supabase.from('sync_events').select('id').eq('run_id', activeRun.id).eq('message', 'drain_loop_incomplete').limit(100);
            yieldCount = yieldEvts?.length || 0;
            drainCapCount = drainEvts?.length || 0;
          } catch (_) {}

          await logDecision(supabase, activeRun.id, 'INFO', 'run_finalized', {
            status: finalStatus,
            real_warning_count: realWarningCount,
            yield_count: yieldCount,
            drain_cap_count: drainCapCount,
            runtime_ms: runtimeMs,
            finished_at: now.toISOString(),
            source: 'cron_tick_guardrail'
          });

          return jsonResp({ status: 'finalized', run_id: activeRun.id, final_status: finalStatus });
        }
      }

      await logDecision(supabase, activeRun.id, 'INFO', 'resume_triggered', {
        ...baseDetails,
        is_stalled: isStalled,
        is_yielded: isYieldEvent,
        stall_threshold_sec: STALL_THRESHOLD_SEC,
        resume_reason: resumeReason
      });

      // SINGLE RESUME PER TICK: call orchestrator once, then return immediately.
      // No drain loop — the next tick (1 min later) will handle further progress.
      // This eliminates concurrent resume calls within the same tick.
      const resumeResult = await triggerSyncResume(supabaseUrl, supabaseServiceKey, activeRun.id);

      if (resumeResult.error) {
        await logDecision(supabase, activeRun.id, 'WARN', 'resume_failed_http', {
          ...baseDetails,
          error: resumeResult.error
        });
      }

      // STOP CONDITIONS: locked, yielded, retry_delay, error — all terminate the tick immediately.
      // The next cron tick will pick up from where we left off.
      const tickTerminateReason =
        resumeResult.status === 'yielded' ? 'tick_terminated_on_yield' :
        resumeResult.status === 'retry_delay' ? 'tick_terminated_on_retry_delay' :
        resumeResult.error ? 'tick_terminated_on_error' :
        'tick_completed_single_resume';

      await logDecision(supabase, activeRun.id, 'INFO', tickTerminateReason, {
        run_id: activeRun.id,
        resume_status: resumeResult.status || 'unknown',
        resume_error: resumeResult.error || null,
        current_step: resumeResult.current_step || currentStep,
        wait_seconds: resumeResult.wait_seconds || null,
        next_retry_at: resumeResult.next_retry_at || null
      });

      return jsonResp({
        status: resumeResult.status || 'resumed',
        run_id: activeRun.id,
        resume_status: resumeResult.status,
        current_step: resumeResult.current_step || currentStep,
        wait_seconds: resumeResult.wait_seconds,
        needs_resume: resumeResult.status === 'yielded' || resumeResult.status === 'retry_delay'
      });
    }

    // No running run — check scheduling
    if (!config.enabled) {
      console.log('[cron-tick] Scheduling disabled, no running runs');
      return jsonResp({ status: 'skipped', reason: 'scheduler_disabled' });
    }

    // 5. LOAD RECENT CRON RUNS (shared by retry, max-attempts, and scheduling logic)
    const { data: lastCronRuns } = await supabase
      .from('sync_runs')
      .select('id, status, attempt, finished_at, started_at, trigger_type, error_message, cancelled_by_user')
      .eq('trigger_type', 'cron')
      .order('started_at', { ascending: false })
      .limit(50);

    const lastCronRun = lastCronRuns?.[0];

    // 5a. CHECK FOR FAILED/TIMEOUT RUNS NEEDING RETRY
    if (lastCronRun && ['failed', 'timeout'].includes(lastCronRun.status) && lastCronRun.attempt < maxAttempts) {
      const finishedAt = lastCronRun.finished_at ? new Date(lastCronRun.finished_at).getTime() : 0;
      const sinceFinished = now.getTime() - finishedAt;

      if (sinceFinished >= retryDelay) {
        console.log(`[cron-tick] Retrying: attempt ${lastCronRun.attempt + 1}/${maxAttempts}`);
        const result = await triggerSync(supabaseUrl, supabaseServiceKey, 'cron', lastCronRun.attempt + 1);
        const respStatus = result.error ? 'error' : 'retry_started';
        return jsonResp({ status: respStatus, attempt: lastCronRun.attempt + 1, ...result });
      } else {
        const waitSec = Math.round((retryDelay - sinceFinished) / 1000);
        console.log(`[cron-tick] Retry delay not elapsed, wait ${waitSec}s more`);
        return jsonResp({ status: 'skipped', reason: 'retry_delay', wait_seconds: waitSec });
      }
    }

    // 5b. CHECK MAX ATTEMPTS (only when last run is terminal failure at max attempt)
    // Uses consecutive-failure-since-last-success algorithm (see checkAndHandleMaxAttempts).
    if (lastCronRun && ['failed', 'timeout'].includes(lastCronRun.status) && lastCronRun.attempt >= maxAttempts) {
      const shouldDisable = await checkAndHandleMaxAttempts(supabase, supabaseUrl, supabaseServiceKey, maxAttempts, lastCronRuns || [], config.updated_at as string);
      if (shouldDisable) {
        return jsonResp({ status: 'max_attempts_exceeded', disabled: true });
      }
    }

    // 6. CHECK IF SYNC IS DUE
    const isDue = checkIfSyncIsDue(config, lastCronRuns || [], now);

    if (!isDue.due) {
      console.log(`[cron-tick] Not due: ${isDue.reason}`);
      return jsonResp({ status: 'skipped', reason: isDue.reason });
    }

    console.log(`[cron-tick] Sync is due: ${isDue.reason}`);

    // 7. TRIGGER SYNC
    const result = await triggerSync(supabaseUrl, supabaseServiceKey, 'cron', 1);
    const respStatus = result.error ? 'error' : 'sync_started';
    return jsonResp({ status: respStatus, ...result });

  } catch (error: unknown) {
    console.error('[cron-tick] Unexpected error:', errMsg(error));
    return jsonResp({ status: 'error', message: errMsg(error) }, 500);
  }
});

// Wrapper: wrap the serve handler to add tick_started/tick_completed logging
// (implemented inline below via helper in jsonResp path)

// ============================================================
// HELPERS
// ============================================================

function jsonResp(body: Record<string, unknown>, status = 200): Response {
  // Structured tick_completed log for every tick outcome
  console.log(JSON.stringify({
    diag_tag: 'tick_completed',
    ts: new Date().toISOString(),
    duration_ms: _tickStartMs > 0 ? Date.now() - _tickStartMs : undefined,
    outcome_status: body.status,
    run_id: body.run_id || null,
    current_step: body.current_step || null,
    reason: body.reason || null,
    resume_status: body.resume_status || null,
    http_status: status
  }));
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, 'Content-Type': 'application/json' }
  });
}

interface DueCheck { due: boolean; reason: string; }

function checkIfSyncIsDue(
  config: Record<string, unknown>,
  lastRuns: Array<Record<string, unknown>>,
  now: Date
): DueCheck {
  const scheduleType = (config.schedule_type as string) || 'hours';

  // Find last terminal (non-retry) cron run
  const lastTerminal = lastRuns.find(r =>
    r.trigger_type === 'cron' &&
    r.attempt === 1 &&
    ['success', 'success_with_warning', 'failed', 'timeout'].includes(r.status as string)
  );

  if (scheduleType === 'hours') {
    const frequencyMin = (config.frequency_minutes as number) || 60;
    const frequencyMs = frequencyMin * 60 * 1000;

    if (!lastTerminal) return { due: true, reason: 'no_previous_run' };

    const startedAt = lastTerminal.started_at as string | undefined;
    if (!startedAt) return { due: true, reason: 'no_started_at_on_last_terminal' };

    const lastStarted = new Date(startedAt).getTime();
    if (isNaN(lastStarted)) return { due: true, reason: 'invalid_started_at' };

    const elapsed = now.getTime() - lastStarted;
    if (elapsed >= frequencyMs) {
      return { due: true, reason: `frequency_elapsed (${Math.round(elapsed / 60000)}min >= ${frequencyMin}min)` };
    }
    return { due: false, reason: `frequency_not_elapsed (${Math.round(elapsed / 60000)}min < ${frequencyMin}min)` };
  }

  if (scheduleType === 'daily') {
    const dailyTime = (config.daily_time as string) || '03:00';
    const [targetHours, targetMinutes] = dailyTime.split(':').map(Number);
    const rome = toRomeTime(now);

    const nowMinutes = rome.hours * 60 + rome.minutes;
    const targetMinutesSinceMidnight = targetHours * 60 + targetMinutes;

    if (nowMinutes < targetMinutesSinceMidnight) {
      return { due: false, reason: `daily_time_not_reached (${rome.hours}:${String(rome.minutes).padStart(2, '0')} < ${dailyTime})` };
    }

    const todayRomeStr = rome.dateStr;
    if (lastTerminal) {
      const startedAt = lastTerminal.started_at as string | undefined;
      if (startedAt) {
        const lastStartedDate = new Date(startedAt);
        const lastRome = toRomeTime(lastStartedDate);
        if (lastRome.dateStr === todayRomeStr) {
          return { due: false, reason: 'already_executed_today' };
        }
      }
    }

    return { due: true, reason: `daily_time_reached (${dailyTime} Europe/Rome)` };
  }

  return { due: false, reason: `unknown_schedule_type: ${scheduleType}` };
}

async function triggerSync(
  supabaseUrl: string,
  serviceKey: string,
  trigger: string,
  attempt: number
): Promise<{ run_id?: string; error?: string; http_status?: number }> {
  try {
    const resp = await fetch(`${supabaseUrl}/functions/v1/run-full-sync`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${serviceKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ trigger, attempt })
    });

    const rawText = await resp.text().catch(() => '');
    let data: Record<string, unknown>;
    try {
      data = JSON.parse(rawText);
    } catch {
      console.error(`[cron-tick] run-full-sync returned non-JSON (HTTP ${resp.status}): ${rawText.substring(0, 1000)}`);
      return { error: 'Invalid response from orchestrator', http_status: resp.status };
    }

    if (!resp.ok) {
      console.error(`[cron-tick] run-full-sync HTTP error ${resp.status}: ${rawText.substring(0, 1000)}`);
      return { error: (data.message as string) || `HTTP ${resp.status}`, http_status: resp.status };
    }

    console.log(`[cron-tick] run-full-sync response:`, data.status, data.run_id || data.message || '');
    return { run_id: data.run_id as string | undefined, error: data.status === 'error' ? (data.message as string) : undefined, http_status: resp.status };
  } catch (e: unknown) {
    console.error('[cron-tick] Failed to trigger sync:', errMsg(e));
    return { error: errMsg(e) };
  }
}

async function triggerSyncResume(
  supabaseUrl: string,
  serviceKey: string,
  runId: string
): Promise<{ status?: string; error?: string; wait_seconds?: number; current_step?: string; next_retry_at?: string; http_status?: number }> {
  try {
    const resp = await fetch(`${supabaseUrl}/functions/v1/run-full-sync`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${serviceKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ trigger: 'cron', resume_run_id: runId })
    });

    const rawText = await resp.text().catch(() => '');
    let data: Record<string, unknown>;
    try {
      data = JSON.parse(rawText);
    } catch {
      console.error(`[cron-tick] resume returned non-JSON (HTTP ${resp.status}): ${rawText.substring(0, 1000)}`);
      return { status: 'error', error: 'Invalid response from orchestrator', http_status: resp.status };
    }

    if (!resp.ok) {
      console.error(`[cron-tick] resume HTTP error ${resp.status}: ${rawText.substring(0, 1000)}`);
      return { status: 'error', error: (data.message as string) || `HTTP ${resp.status}`, http_status: resp.status };
    }

    console.log(`[cron-tick] resume response for ${runId}:`, data.status);
    return {
      status: data.status as string,
      error: data.status === 'error' ? (data.message as string) : undefined,
      wait_seconds: data.wait_seconds as number | undefined,
      current_step: data.current_step as string | undefined,
      next_retry_at: data.next_retry_at as string | undefined,
      http_status: resp.status
    };
  } catch (e: unknown) {
    console.error('[cron-tick] Failed to resume sync:', errMsg(e));
    return { status: 'error', error: errMsg(e) };
  }
}

/** Fire-and-forget resume: triggers run-full-sync but aborts waiting after timeoutMs.
 *  The edge function continues processing server-side even after abort.
 *  Returns explicit status about what happened. */
async function triggerSyncResumeQuick(
  supabaseUrl: string,
  serviceKey: string,
  runId: string,
  timeoutMs: number
): Promise<{ resume_request: string }> {
  const controller = new AbortController();
  const tid = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(`${supabaseUrl}/functions/v1/run-full-sync`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${serviceKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ trigger: 'cron', resume_run_id: runId }),
      signal: controller.signal
    });
    clearTimeout(tid);
    if (!resp.ok) {
      console.warn(`[cron-tick] resumeQuick HTTP error ${resp.status} for run ${runId}`);
      return { resume_request: 'error' };
    }
    return { resume_request: 'confirmed' };
  } catch (e: unknown) {
    clearTimeout(tid);
    const isAbort = e instanceof DOMException && e.name === 'AbortError';
    if (isAbort) {
      // Timeout — function keeps running server-side
      return { resume_request: 'sent_without_confirmation' };
    }
    console.warn(`[cron-tick] resumeQuick network error for run ${runId}: ${errMsg(e)}`);
    return { resume_request: 'error' };
  }
}

async function callNotification(
  supabaseUrl: string,
  serviceKey: string,
  runId: string,
  status: string
): Promise<void> {
  try {
    await fetch(`${supabaseUrl}/functions/v1/send-sync-notification`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${serviceKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ run_id: runId, status })
    });
  } catch (e: unknown) {
    console.warn('[cron-tick] Notification call failed (non-blocking):', errMsg(e));
  }
}

/**
 * Determines whether the scheduler should be auto-disabled due to consecutive cron failures.
 *
 * Algorithm (deterministic):
 *   1. Only consider runs started AFTER the last config update (prevents re-disable loop when user re-enables).
 *   2. Filter runs to attempt=1 only (primary runs, not retries) for chain counting.
 *   3. Walk from most recent to oldest.
 *   4. Skip TRANSIENT failures: timeout, WORKER_LIMIT/546, cancelled, manual stop, network errors (429/503/502).
 *      These don't count and don't break the chain.
 *   5. Count consecutive PERMANENT failures (misconfig, schema errors, invariant broken, unknown errors).
 *   6. If consecutive permanent failures >= maxAttempts AND no cron run is currently running → disable.
 *   7. If a success exists more recent than the failure chain → do NOT disable (chain is reset).
 *
 * Rationale: transient errors (infrastructure limits, timeouts, network) should cause yield+backoff,
 * not auto-disable. Only deterministic permanent errors (misconfigured secrets, missing schema) should
 * trigger auto-disable, as they won't resolve without human intervention.
 */
async function checkAndHandleMaxAttempts(
  supabase: SClient,
  supabaseUrl: string,
  serviceKey: string,
  maxAttempts: number,
  allCronRuns: Array<Record<string, unknown>>,
  configUpdatedAt: string
): Promise<boolean> {
  // Guard: if any cron run is currently running, don't disable
  const hasRunning = allCronRuns.some((r) => r.status === 'running');
  if (hasRunning) {
    console.log('[cron-tick] checkMaxAttempts: skipped, a cron run is still running');
    return false;
  }

  // Only consider runs started after the last config change (prevents re-disable loop
  // when user re-enables after an auto-disable: old failures are ignored)
  const configUpdatedMs = configUpdatedAt ? new Date(configUpdatedAt).getTime() : 0;
  const recentRuns = allCronRuns.filter((r) => {
    const startedMs = new Date(r.started_at as string).getTime();
    return !configUpdatedAt || startedMs > configUpdatedMs;
  });

  // Filter to primary runs only (attempt=1) for chain determination
  const primaryRuns = recentRuns.filter((r) => r.attempt === 1);

  let consecutiveFailures = 0;
  let resetPoint: { id: string; started_at: string } | null = null;
  const failedRunIds: string[] = [];

  // Transient error patterns: these failures should NOT count toward auto-disable
  const TRANSIENT_PATTERNS = [
    'worker_limit', '546', 'timeout', 'interrotta', 'cancelled',
    'econnreset', 'econnrefused', 'etimedout', '429', '503', '502'
  ];

  for (const run of primaryRuns) {
    const status = run.status as string;

    // Success/success_with_warning resets the chain
    if (['success', 'success_with_warning'].includes(status)) {
      resetPoint = { id: run.id as string, started_at: run.started_at as string };
      break;
    }

    // Skip cancelled runs entirely (don't count, don't break chain)
    if (run.cancelled_by_user === true) continue;

    // Skip timeout status entirely (transient by nature)
    if (status === 'timeout') {
      console.log(`[cron-tick] checkMaxAttempts: skipping timeout run ${run.id} (transient)`);
      continue;
    }

    if (status === 'failed') {
      const errMsg = ((run.error_message as string) || '').toLowerCase();
      const isTransient = TRANSIENT_PATTERNS.some(p => errMsg.includes(p));

      if (isTransient) {
        console.log(`[cron-tick] checkMaxAttempts: skipping transient failure ${run.id}: ${errMsg.substring(0, 100)}`);
        continue; // don't count, don't break chain
      }

      // PERMANENT failure: counts toward auto-disable
      consecutiveFailures++;
      failedRunIds.push(run.id as string);
    }
    // skip other statuses (cancelled, running, etc.) — don't count, don't break
  }

  if (consecutiveFailures < maxAttempts) {
    // Not enough permanent failures to disable.
    if (consecutiveFailures > 0) {
      console.log(`[cron-tick] checkMaxAttempts: ${consecutiveFailures} permanent failures (< ${maxAttempts} threshold)${resetPoint ? `, reset by success ${resetPoint.id}` : ''}`);
    }
    return false;
  }

  // Disable scheduler
  console.log(`[cron-tick] ${consecutiveFailures} consecutive permanent cron failures (>= ${maxAttempts}), disabling`);

  await supabase.from('sync_config').update({
    enabled: false,
    last_disabled_reason: `Auto-disabilitato: ${consecutiveFailures} fallimenti permanenti consecutivi cron (esclusi transitori: WORKER_LIMIT, timeout, cancellati)`
  }).eq('id', 1);

  // Log persistent decision event
  if (failedRunIds[0]) {
    await logDecision(supabase, failedRunIds[0], 'ERROR', 'scheduler_auto_disabled_max_attempts', {
      max_attempts: maxAttempts,
      consecutive_failures_count: consecutiveFailures,
      reset_point: resetPoint,
      sample_run_ids: failedRunIds.slice(0, 5),
      filter: 'permanent_only',
      config_updated_at: configUpdatedAt
    });
  }

  // Non-blocking notification
  const lastFailedId = failedRunIds[0];
  if (lastFailedId) {
    const lastRun = allCronRuns.find((r) => r.id === lastFailedId);
    await callNotification(supabaseUrl, serviceKey, lastFailedId, (lastRun?.status as string) || 'failed');
  }

  return true;
}
