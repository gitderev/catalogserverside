import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";

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
    console.log('[cron-tick] Tick received');

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
    const maxAttempts = config.max_attempts || 3;
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
        // Force-clean lock if not owned
        await supabase.from('sync_locks').delete().eq('lock_name', 'global_sync');

        // Reload cron runs after marking timeout, then check max attempts
        const { data: refreshedRuns1 } = await supabase
          .from('sync_runs')
          .select('id, status, attempt, finished_at, started_at, trigger_type')
          .eq('trigger_type', 'cron')
          .order('started_at', { ascending: false })
          .limit(50);
        await checkAndHandleMaxAttempts(supabase, supabaseUrl, supabaseServiceKey, maxAttempts, refreshedRuns1 || []);
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
          await supabase.from('sync_locks').delete().eq('lock_name', 'global_sync');

          // Reload cron runs after marking timeout, then check max attempts
          const { data: refreshedRuns2 } = await supabase
            .from('sync_runs')
            .select('id, status, attempt, finished_at, started_at, trigger_type')
            .eq('trigger_type', 'cron')
            .order('started_at', { ascending: false })
            .limit(50);
          await checkAndHandleMaxAttempts(supabase, supabaseUrl, supabaseServiceKey, maxAttempts, refreshedRuns2 || []);
          await callNotification(supabaseUrl, supabaseServiceKey, activeRun.id, 'timeout');

          return jsonResp({ status: 'timeout_marked', run_id: activeRun.id, reason: 'idle_timeout' });
        }
      }

      // YIELD FAST-PATH: if last progress event is a yield and > 5s old, bypass gating
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

      await logDecision(supabase, activeRun.id, 'INFO', 'resume_triggered', {
        ...baseDetails,
        is_stalled: isStalled,
        is_yielded: isYieldEvent,
        stall_threshold_sec: STALL_THRESHOLD_SEC,
        resume_reason: resumeReason
      });

      let resumeResult = await triggerSyncResume(supabaseUrl, supabaseServiceKey, activeRun.id);

      if (resumeResult.error) {
        await logDecision(supabase, activeRun.id, 'WARN', 'resume_failed_http', {
          ...baseDetails,
          error: resumeResult.error
        });
      }

      // GUARD: if orchestrator returned retry_delay, the step is not due — skip drain loop entirely
      if (resumeResult.status === 'retry_delay') {
        const retryWait = resumeResult.wait_seconds || 0;
        console.log(`[cron-tick] resume response: retry_delay (${retryWait}s remaining), skipping drain loop`);
        await logDecision(supabase, activeRun.id, 'INFO', 'resume_skipped_not_due', {
          ...baseDetails,
          origin: 'cron',
          now_iso: new Date().toISOString(),
          next_retry_at: resumeResult.next_retry_at || 'unknown',
          seconds_remaining: retryWait,
          source: 'initial_resume_response'
        });
        return jsonResp({
          status: 'retry_delay',
          run_id: activeRun.id,
          wait_seconds: retryWait,
          needs_resume: true
        });
      }

      // DRAIN LOOP: drive the run to completion using response-JSON-based yield detection.
      // When run-full-sync responds with status "yielded", we re-trigger immediately
      // (5s debounce) instead of waiting for the next event-based check.
      // Budget is conservative to fit within edge function timeout.
      const DRAIN_MAX_ITER = 4;
      const DRAIN_BUDGET_MS = 120_000; // 2 minutes
      const DRAIN_SLEEP_MS = 25_000;   // 25s between poll iterations
      const FORCE_RESUME_DEBOUNCE_MS = 5_000; // 5s debounce for yield force-resume
      const drainStart = Date.now();
      let drainIter = 0;
      let lastForceResumeAt = 0; // in-memory debounce timestamp

      // Check if initial resume indicated yield → force immediate re-trigger
      if (resumeResult.status === 'yielded' && !resumeResult.error) {
        lastForceResumeAt = Date.now();
        console.log(JSON.stringify({
          diag_tag: 'cron_tick_decision',
          run_id: activeRun.id,
          decision: 'force_resume',
          reason: 'yield_observed',
          progress_source: 'response_json_yield',
          trigger: 'initial_resume_response'
        }));
        // Brief debounce then re-trigger
        await new Promise(resolve => setTimeout(resolve, FORCE_RESUME_DEBOUNCE_MS));
        resumeResult = await triggerSyncResume(supabaseUrl, supabaseServiceKey, activeRun.id);
        // If re-trigger returned retry_delay, skip drain entirely
        if (resumeResult.status === 'retry_delay') {
          console.log(`[cron-tick] Initial yield re-trigger returned retry_delay, stopping`);
          await logDecision(supabase, activeRun.id, 'INFO', 'resume_skipped_not_due', {
            ...baseDetails, origin: 'yield_retrigger', now_iso: new Date().toISOString(),
            next_retry_at: resumeResult.next_retry_at || 'unknown', seconds_remaining: resumeResult.wait_seconds || 0
          });
          return jsonResp({ status: 'retry_delay', run_id: activeRun.id, wait_seconds: resumeResult.wait_seconds || 0, needs_resume: true });
        }
      }

      while (drainIter < DRAIN_MAX_ITER && (Date.now() - drainStart) < DRAIN_BUDGET_MS) {
        drainIter++;

        // If last resume returned "yielded", skip the long sleep and re-trigger quickly
        if (resumeResult.status === 'yielded' && !resumeResult.error) {
          const sinceLast = Date.now() - lastForceResumeAt;
          if (sinceLast >= FORCE_RESUME_DEBOUNCE_MS) {
            lastForceResumeAt = Date.now();
            console.log(JSON.stringify({
              diag_tag: 'cron_tick_decision',
              run_id: activeRun.id,
              decision: 'force_resume',
              reason: 'yield_observed',
              progress_source: 'response_json_yield',
              drain_iteration: drainIter
            }));
            await new Promise(resolve => setTimeout(resolve, FORCE_RESUME_DEBOUNCE_MS));
            resumeResult = await triggerSyncResume(supabaseUrl, supabaseServiceKey, activeRun.id);
            // Guard: retry_delay → stop drain
            if (resumeResult.status === 'retry_delay') {
              console.log(`[cron-tick] Drain iter ${drainIter}: yield re-trigger returned retry_delay, stopping`);
              await logDecision(supabase, activeRun.id, 'INFO', 'resume_skipped_not_due', {
                run_id: activeRun.id, step: resumeResult.current_step || 'unknown', origin: 'drain_yield_retrigger',
                now_iso: new Date().toISOString(), next_retry_at: resumeResult.next_retry_at || 'unknown',
                seconds_remaining: resumeResult.wait_seconds || 0, drain_iteration: drainIter
              });
              return jsonResp({ status: 'retry_delay', run_id: activeRun.id, wait_seconds: resumeResult.wait_seconds || 0, needs_resume: true });
            }
            continue; // skip the long sleep, re-check immediately
          }
        }

        // Normal drain sleep
        await new Promise(resolve => setTimeout(resolve, DRAIN_SLEEP_MS));

        // Re-read run status
        const { data: runCheck } = await supabase
          .from('sync_runs')
          .select('id, status, steps')
          .eq('id', activeRun.id)
          .single();

        if (!runCheck || runCheck.status !== 'running') {
          const finalStatus = runCheck?.status || 'unknown';
          console.log(`[cron-tick] Drain complete: run ${activeRun.id} status=${finalStatus} after ${drainIter} drain iterations`);
          await logDecision(supabase, activeRun.id, 'INFO', 'drain_loop_complete', {
            final_status: finalStatus,
            drain_iterations: drainIter,
            drain_elapsed_ms: Date.now() - drainStart
          });
          return jsonResp({
            status: 'drain_complete',
            run_id: activeRun.id,
            final_status: finalStatus,
            drain_iterations: drainIter
          });
        }

        // Resume using full triggerSyncResume to read response JSON
        console.log(`[cron-tick] Drain iter ${drainIter}: triggering resume`);
        await logDecision(supabase, activeRun.id, 'INFO', 'drain_resume_triggered', {
          drain_iteration: drainIter,
          drain_elapsed_ms: Date.now() - drainStart
        });
        resumeResult = await triggerSyncResume(supabaseUrl, supabaseServiceKey, activeRun.id);

        // GUARD: if orchestrator returned retry_delay, stop drain loop — step is not due
        if (resumeResult.status === 'retry_delay') {
          const retryWait = resumeResult.wait_seconds || 0;
          console.log(`[cron-tick] Drain iter ${drainIter}: retry_delay (${retryWait}s), stopping drain loop`);
          await logDecision(supabase, activeRun.id, 'INFO', 'resume_skipped_not_due', {
            run_id: activeRun.id,
            step: resumeResult.current_step || 'unknown',
            origin: 'drain_loop',
            now_iso: new Date().toISOString(),
            next_retry_at: resumeResult.next_retry_at || 'unknown',
            seconds_remaining: retryWait,
            drain_iteration: drainIter
          });
          return jsonResp({
            status: 'retry_delay',
            run_id: activeRun.id,
            wait_seconds: retryWait,
            needs_resume: true
          });
        }
      }

      // Drain ended without run completing — safe stop, no destructive status change
      console.log(`[cron-tick] Drain incomplete: ${drainIter} iterations, run ${activeRun.id} still running`);
      await logDecision(supabase, activeRun.id, 'INFO', 'drain_loop_incomplete', {
        drain_iterations: drainIter,
        drain_elapsed_ms: Date.now() - drainStart,
        reason: drainIter >= DRAIN_MAX_ITER ? 'max_iterations' : 'max_time'
      });

      return jsonResp({
        status: 'drain_incomplete',
        run_id: activeRun.id,
        drain_iterations: drainIter
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
      .select('id, status, attempt, finished_at, started_at, trigger_type')
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
        return jsonResp({ status: 'retry_started', attempt: lastCronRun.attempt + 1, ...result });
      } else {
        const waitSec = Math.round((retryDelay - sinceFinished) / 1000);
        console.log(`[cron-tick] Retry delay not elapsed, wait ${waitSec}s more`);
        return jsonResp({ status: 'skipped', reason: 'retry_delay', wait_seconds: waitSec });
      }
    }

    // 5b. CHECK MAX ATTEMPTS (only when last run is terminal failure at max attempt)
    // Uses consecutive-failure-since-last-success algorithm (see checkAndHandleMaxAttempts).
    if (lastCronRun && ['failed', 'timeout'].includes(lastCronRun.status) && lastCronRun.attempt >= maxAttempts) {
      const shouldDisable = await checkAndHandleMaxAttempts(supabase, supabaseUrl, supabaseServiceKey, maxAttempts, lastCronRuns || []);
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
    return jsonResp({ status: 'sync_started', ...result });

  } catch (error: unknown) {
    console.error('[cron-tick] Unexpected error:', errMsg(error));
    return jsonResp({ status: 'error', message: errMsg(error) }, 500);
  }
});

// ============================================================
// HELPERS
// ============================================================

function jsonResp(body: Record<string, unknown>, status = 200): Response {
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
): Promise<{ run_id?: string; error?: string }> {
  try {
    const resp = await fetch(`${supabaseUrl}/functions/v1/run-full-sync`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${serviceKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ trigger, attempt })
    });

    const data = await resp.json().catch(() => ({ status: 'error', message: 'Invalid response' }));
    console.log(`[cron-tick] run-full-sync response:`, data.status, data.run_id || data.message || '');
    return { run_id: data.run_id, error: data.status === 'error' ? data.message : undefined };
  } catch (e: unknown) {
    console.error('[cron-tick] Failed to trigger sync:', errMsg(e));
    return { error: errMsg(e) };
  }
}

async function triggerSyncResume(
  supabaseUrl: string,
  serviceKey: string,
  runId: string
): Promise<{ status?: string; error?: string; wait_seconds?: number; current_step?: string; next_retry_at?: string }> {
  try {
    const resp = await fetch(`${supabaseUrl}/functions/v1/run-full-sync`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${serviceKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ trigger: 'cron', resume_run_id: runId })
    });

    const data = await resp.json().catch(() => ({ status: 'error', message: 'Invalid response' }));
    console.log(`[cron-tick] resume response for ${runId}:`, data.status);
    return {
      status: data.status,
      error: data.status === 'error' ? data.message : undefined,
      wait_seconds: data.wait_seconds,
      current_step: data.current_step,
      next_retry_at: data.next_retry_at
    };
  } catch (e: unknown) {
    console.error('[cron-tick] Failed to resume sync:', errMsg(e));
    return { error: errMsg(e) };
  }
}

/** Fire-and-forget resume: triggers run-full-sync but aborts waiting after timeoutMs.
 *  The edge function continues processing server-side even after abort. */
async function triggerSyncResumeQuick(
  supabaseUrl: string,
  serviceKey: string,
  runId: string,
  timeoutMs: number
): Promise<void> {
  const controller = new AbortController();
  const tid = setTimeout(() => controller.abort(), timeoutMs);
  try {
    await fetch(`${supabaseUrl}/functions/v1/run-full-sync`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${serviceKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ trigger: 'cron', resume_run_id: runId }),
      signal: controller.signal
    });
  } catch { /* timeout or network error — function keeps running server-side */ }
  clearTimeout(tid);
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
 *   1. Filter runs to attempt=1 only (primary runs, not retries) for chain counting.
 *   2. Walk from most recent to oldest.
 *   3. Count consecutive failed/timeout runs until a success/success_with_warning is found (reset point).
 *   4. If consecutive failures >= maxAttempts AND no cron run is currently running → disable.
 *   5. If a success exists more recent than the failure chain → do NOT disable (chain is reset).
 *
 * Example sequences (max_attempts=3):
 *   [timeout, timeout, timeout]                         → 3 consecutive → DISABLE
 *   [timeout, timeout, success, timeout, timeout]       → 2 consecutive → NO disable (reset by success)
 *   [timeout, timeout, timeout, success_with_warning]   → 3 consecutive → DISABLE (success is older)
 *   [success, timeout, timeout, timeout]                → 0 consecutive → NO disable (success is most recent)
 *   [running, timeout, timeout, timeout]                → skip (running exists)
 */
async function checkAndHandleMaxAttempts(
  supabase: SClient,
  supabaseUrl: string,
  serviceKey: string,
  maxAttempts: number,
  allCronRuns: Array<Record<string, unknown>>
): Promise<boolean> {
  // Guard: if any cron run is currently running, don't disable
  const hasRunning = allCronRuns.some((r) => r.status === 'running');
  if (hasRunning) {
    console.log('[cron-tick] checkMaxAttempts: skipped, a cron run is still running');
    return false;
  }

  // Filter to primary runs only (attempt=1) for chain determination
  const primaryRuns = allCronRuns.filter((r) => r.attempt === 1);

  let consecutiveFailures = 0;
  let resetPoint: { id: string; started_at: string } | null = null;
  const failedRunIds: string[] = [];

  for (const run of primaryRuns) {
    const status = run.status as string;
    if (['success', 'success_with_warning'].includes(status)) {
      resetPoint = { id: run.id as string, started_at: run.started_at as string };
      break; // chain is reset
    }
    if (['failed', 'timeout'].includes(status)) {
      consecutiveFailures++;
      failedRunIds.push(run.id as string);
    }
    // skip other statuses (cancelled, etc.) — don't count, don't break
  }

  if (consecutiveFailures < maxAttempts) {
    // Not enough failures to disable. Log only if we were close (avoid spam).
    if (consecutiveFailures >= maxAttempts - 1 && resetPoint) {
      console.log(`[cron-tick] checkMaxAttempts: ${consecutiveFailures} failures but reset by success ${resetPoint.id}`);
      await logDecision(supabase, failedRunIds[0] || 'unknown', 'INFO', 'max_attempts_reset_by_success', {
        max_attempts: maxAttempts,
        consecutive_failures_count: consecutiveFailures,
        reset_point: resetPoint,
        sample_run_ids: failedRunIds.slice(0, 5)
      });
    }
    return false;
  }

  // Disable scheduler
  console.log(`[cron-tick] ${consecutiveFailures} consecutive primary cron failures (>= ${maxAttempts}), disabling`);

  await supabase.from('sync_config').update({
    enabled: false,
    last_disabled_reason: `Auto-disabilitato: ${consecutiveFailures} fallimenti consecutivi cron primari (failed/timeout)`
  }).eq('id', 1);

  // Log persistent decision event
  if (failedRunIds[0]) {
    await logDecision(supabase, failedRunIds[0], 'ERROR', 'scheduler_auto_disabled_max_attempts', {
      max_attempts: maxAttempts,
      consecutive_failures_count: consecutiveFailures,
      reset_point: resetPoint,
      sample_run_ids: failedRunIds.slice(0, 5)
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
