import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";

/**
 * cron-tick - Called every 5 minutes by GitHub Actions
 * 
 * Responsibilities:
 * 1. Authenticate ONLY via x-cron-secret header (no anon/service role bypass)
 * 2. Check if a sync is due (based on schedule_type, frequency, daily_time)
 * 3. Detect and handle timed-out runs (>run_timeout_minutes)
 * 4. Manage retry logic (max_attempts with retry_delay_minutes)
 * 5. Prevent concurrent runs
 * 6. Auto-disable scheduling after max_attempts consecutive failures (failed OR timeout)
 */

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type, x-cron-secret',
};

function errMsg(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

// Convert a Date to Europe/Rome local time components
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

serve(async (req) => {
  if (req.method === 'OPTIONS') return new Response(null, { headers: corsHeaders });

  try {
    // 1. AUTHENTICATE: ONLY x-cron-secret header
    const cronSecret = Deno.env.get('CRON_SECRET');
    const providedSecret = req.headers.get('x-cron-secret');

    if (!cronSecret || !providedSecret || providedSecret !== cronSecret) {
      console.log('[cron-tick] Invalid or missing x-cron-secret');
      return new Response(
        JSON.stringify({ status: 'error', message: 'Unauthorized' }),
        { status: 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    const supabaseUrl = Deno.env.get('SUPABASE_URL')!;
    const supabaseServiceKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
    const supabase = createClient(supabaseUrl, supabaseServiceKey);

    console.log('[cron-tick] Tick received');

    // 2. LOAD CONFIG
    const { data: config, error: configError } = await supabase
      .from('sync_config')
      .select('*')
      .eq('id', 1)
      .single();

    if (configError || !config) {
      console.error('[cron-tick] Failed to load config:', configError?.message);
      return new Response(
        JSON.stringify({ status: 'error', message: 'Config not found' }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    if (!config.enabled) {
      console.log('[cron-tick] Scheduling disabled');
      return new Response(
        JSON.stringify({ status: 'skipped', reason: 'disabled' }),
        { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    const now = new Date();
    const timeoutMinutes = config.run_timeout_minutes || 60;
    const maxAttempts = config.max_attempts || 3;
    const retryDelay = (config.retry_delay_minutes || 5) * 60 * 1000;

    // 3. CHECK FOR RUNNING RUNS
    const { data: runningRuns } = await supabase
      .from('sync_runs')
      .select('id, started_at, attempt, trigger_type')
      .eq('status', 'running')
      .limit(1);

    if (runningRuns?.length) {
      const run = runningRuns[0];
      const startedAt = new Date(run.started_at).getTime();
      const elapsed = now.getTime() - startedAt;
      const timeoutMs = timeoutMinutes * 60 * 1000;

      if (elapsed > timeoutMs) {
        // TIMEOUT: mark as timeout
        console.log(`[cron-tick] Run ${run.id} timed out (${Math.round(elapsed / 60000)}min > ${timeoutMinutes}min)`);

        await supabase.from('sync_runs').update({
          status: 'timeout',
          finished_at: now.toISOString(),
          runtime_ms: elapsed,
          error_message: `Timeout: superati ${timeoutMinutes} minuti`
        }).eq('id', run.id);

        await supabase.from('sync_events').insert({
          run_id: run.id,
          level: 'ERROR',
          step: 'timeout',
          message: `Run interrotta per timeout dopo ${Math.round(elapsed / 60000)} minuti`
        });

        // Release lock
        await supabase.from('sync_locks').delete().eq('lock_key', 'sync_pipeline');

        // Check if max attempts reached (considering this timeout as a terminal failure)
        const shouldDisable = await checkAndHandleMaxAttempts(supabase, supabaseUrl, supabaseServiceKey, maxAttempts);
        
        if (shouldDisable) {
          return new Response(
            JSON.stringify({ status: 'max_attempts_exceeded', disabled: true }),
            { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
          );
        }

        // Send notification for timeout
        await callNotification(supabaseUrl, supabaseServiceKey, run.id, 'timeout');

        return new Response(
          JSON.stringify({ status: 'timeout_detected', run_id: run.id, will_retry: true }),
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      // Run is still within timeout window
      console.log(`[cron-tick] Run ${run.id} still running (${Math.round(elapsed / 60000)}min), skipping`);
      return new Response(
        JSON.stringify({ status: 'skipped', reason: 'run_in_progress', run_id: run.id }),
        { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // 4. CHECK FOR FAILED/TIMEOUT RUNS NEEDING RETRY
    const { data: lastCronRuns } = await supabase
      .from('sync_runs')
      .select('id, status, attempt, finished_at, trigger_type')
      .eq('trigger_type', 'cron')
      .order('started_at', { ascending: false })
      .limit(5);

    const lastCronRun = lastCronRuns?.[0];

    if (lastCronRun && ['failed', 'timeout'].includes(lastCronRun.status) && lastCronRun.attempt < maxAttempts) {
      // Check retry delay
      const finishedAt = lastCronRun.finished_at ? new Date(lastCronRun.finished_at).getTime() : 0;
      const sinceFinished = now.getTime() - finishedAt;

      if (sinceFinished >= retryDelay) {
        console.log(`[cron-tick] Retrying: attempt ${lastCronRun.attempt + 1}/${maxAttempts}`);

        const result = await triggerSync(supabaseUrl, supabaseServiceKey, 'cron', lastCronRun.attempt + 1);

        return new Response(
          JSON.stringify({ status: 'retry_started', attempt: lastCronRun.attempt + 1, ...result }),
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      } else {
        const waitSec = Math.round((retryDelay - sinceFinished) / 1000);
        console.log(`[cron-tick] Retry delay not elapsed, wait ${waitSec}s more`);
        return new Response(
          JSON.stringify({ status: 'skipped', reason: 'retry_delay', wait_seconds: waitSec }),
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
    }

    // 4b. CHECK FOR MAX ATTEMPTS REACHED (last run was final attempt and failed)
    if (lastCronRun && ['failed', 'timeout'].includes(lastCronRun.status) && lastCronRun.attempt >= maxAttempts) {
      const shouldDisable = await checkAndHandleMaxAttempts(supabase, supabaseUrl, supabaseServiceKey, maxAttempts);
      if (shouldDisable) {
        return new Response(
          JSON.stringify({ status: 'max_attempts_exceeded', disabled: true }),
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
    }

    // 5. CHECK IF SYNC IS DUE
    const isDue = checkIfSyncIsDue(config, lastCronRuns || [], now);
    
    if (!isDue.due) {
      console.log(`[cron-tick] Not due: ${isDue.reason}`);
      return new Response(
        JSON.stringify({ status: 'skipped', reason: isDue.reason }),
        { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    console.log(`[cron-tick] Sync is due: ${isDue.reason}`);

    // 6. TRIGGER SYNC
    const result = await triggerSync(supabaseUrl, supabaseServiceKey, 'cron', 1);

    return new Response(
      JSON.stringify({ status: 'sync_started', ...result }),
      { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );

  } catch (error: unknown) {
    console.error('[cron-tick] Unexpected error:', errMsg(error));
    return new Response(
      JSON.stringify({ status: 'error', message: errMsg(error) }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});

// ============================================================
// HELPERS
// ============================================================

interface DueCheck {
  due: boolean;
  reason: string;
}

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
    const frequencyMs = ((config.frequency_minutes as number) || 60) * 60 * 1000;

    if (!lastTerminal) return { due: true, reason: 'no_previous_run' };

    const lastStarted = new Date(lastTerminal.started_at as string).getTime();
    const elapsed = now.getTime() - lastStarted;

    if (elapsed >= frequencyMs) {
      return { due: true, reason: `frequency_elapsed (${Math.round(elapsed / 60000)}min >= ${(config.frequency_minutes as number)}min)` };
    }
    return { due: false, reason: `frequency_not_elapsed (${Math.round(elapsed / 60000)}min < ${(config.frequency_minutes as number)}min)` };
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
      const lastStartedDate = new Date(lastTerminal.started_at as string);
      const lastRome = toRomeTime(lastStartedDate);
      
      if (lastRome.dateStr === todayRomeStr) {
        return { due: false, reason: 'already_executed_today' };
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
      headers: {
        'Authorization': `Bearer ${serviceKey}`,
        'Content-Type': 'application/json'
      },
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

async function callNotification(
  supabaseUrl: string,
  serviceKey: string,
  runId: string,
  status: string
): Promise<void> {
  try {
    await fetch(`${supabaseUrl}/functions/v1/send-sync-notification`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${serviceKey}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ run_id: runId, status })
    });
  } catch (e: unknown) {
    console.warn('[cron-tick] Notification call failed (non-blocking):', errMsg(e));
  }
}

/**
 * Check if max consecutive failures reached. If so, disable scheduling and send notification.
 * Returns true if scheduling was disabled.
 */
async function checkAndHandleMaxAttempts(
  supabase: ReturnType<typeof createClient>,
  supabaseUrl: string,
  serviceKey: string,
  maxAttempts: number
): Promise<boolean> {
  // Get last N cron runs to check for consecutive failures
  const { data: recentRuns } = await supabase
    .from('sync_runs')
    .select('id, status, attempt, trigger_type')
    .eq('trigger_type', 'cron')
    .order('started_at', { ascending: false })
    .limit(maxAttempts);

  if (!recentRuns || recentRuns.length < maxAttempts) return false;

  // Check if all last maxAttempts runs are terminal failures
  const allFailed = recentRuns.slice(0, maxAttempts).every(
    r => ['failed', 'timeout'].includes(r.status)
  );

  if (!allFailed) return false;

  console.log(`[cron-tick] ${maxAttempts} consecutive failures detected, disabling scheduling`);
  
  await supabase.from('sync_config').update({
    enabled: false,
    last_disabled_reason: `Auto-disabilitato: ${maxAttempts} fallimenti consecutivi (failed/timeout)`
  }).eq('id', 1);

  // Send notification for the last failed run
  const lastFailedRun = recentRuns[0];
  await callNotification(supabaseUrl, serviceKey, lastFailedRun.id, lastFailedRun.status);

  return true;
}
