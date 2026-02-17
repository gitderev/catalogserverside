/**
 * Pure, deterministic derivation functions for sync UI state.
 * No side effects, no DB calls. Testable.
 */

export interface SyncRunRecord {
  id: string;
  started_at: string;
  finished_at: string | null;
  status: string;
  trigger_type: string;
  attempt: number;
  error_message: string | null;
  error_details: unknown;
  cancel_requested: boolean;
  cancelled_by_user: boolean;
  warning_count: number;
  steps: Record<string, unknown>;
  metrics: Record<string, unknown>;
  runtime_ms: number | null;
  location_warnings: Record<string, unknown>;
}

export interface RunClassification {
  displayStatus: string;
  displayReason: string;
  isCancelled: boolean;
  isRetryDelay: boolean;
  isRunning: boolean;
  /** Counts as a cron failure for auto-disable streak (same rule as backend) */
  isCountedAsCronFailure: boolean;
}

/**
 * Sort runs deterministically:
 * 1. finished_at DESC (null = still running = most recent)
 * 2. started_at DESC
 * 3. id DESC
 */
export function sortRunsDeterministically<T extends { finished_at: string | null; started_at: string; id: string }>(
  runs: T[]
): T[] {
  return [...runs].sort((a, b) => {
    // Null finished_at (running) comes first
    if (!a.finished_at && b.finished_at) return -1;
    if (a.finished_at && !b.finished_at) return 1;
    if (a.finished_at && b.finished_at) {
      const cmp = new Date(b.finished_at).getTime() - new Date(a.finished_at).getTime();
      if (cmp !== 0) return cmp;
    }
    const cmpStart = new Date(b.started_at).getTime() - new Date(a.started_at).getTime();
    if (cmpStart !== 0) return cmpStart;
    return b.id.localeCompare(a.id);
  });
}

/** Get last run (any trigger) after deterministic sort. */
export function getLastRunAll<T extends { finished_at: string | null; started_at: string; id: string }>(
  runs: T[]
): T | null {
  if (runs.length === 0) return null;
  return sortRunsDeterministically(runs)[0];
}

/** Get last run by trigger type after deterministic sort. */
export function getLastRunByTrigger<T extends { finished_at: string | null; started_at: string; id: string; trigger_type: string }>(
  runs: T[],
  trigger: string
): T | null {
  const filtered = runs.filter((r) => r.trigger_type === trigger);
  return getLastRunAll(filtered);
}

/**
 * Classify a run for display purposes.
 * 
 * IMPORTANT: "Cancellata" is shown ONLY if cancelled_by_user === true.
 * Never assume cancellation from error_message text.
 */
export function classifyRun(run: SyncRunRecord): RunClassification {
  const isCancelled = run.cancelled_by_user === true;
  const isRunning = run.status === 'running';

  // Check if any step is in retry_delay
  const isRetryDelay = Object.values(run.steps || {}).some((s) => {
    if (typeof s === 'object' && s !== null) {
      return (s as Record<string, unknown>).status === 'retry_delay';
    }
    return false;
  });

  let displayStatus: string;
  let displayReason: string;

  if (isCancelled) {
    displayStatus = 'Cancellata';
    displayReason = 'Sincronizzazione interrotta manualmente dall\'utente';
  } else if (run.status === 'failed') {
    displayStatus = 'Fallita';
    displayReason = run.error_message || 'Errore sconosciuto';
  } else if (run.status === 'timeout') {
    displayStatus = 'Timeout';
    displayReason = run.error_message || 'Superato il tempo massimo';
  } else if (run.status === 'success') {
    displayStatus = 'Successo';
    displayReason = '';
  } else if (run.status === 'success_with_warning') {
    displayStatus = 'Successo con avvisi';
    displayReason = `${run.warning_count} avvisi`;
  } else if (isRunning) {
    displayStatus = 'In esecuzione';
    displayReason = '';
  } else {
    displayStatus = run.status;
    displayReason = '';
  }

  // Backend rule: only primary runs (attempt=1), status failed/timeout count.
  // Cancelled runs (cancelled_by_user) are skipped (don't count, don't break streak).
  const isCountedAsCronFailure =
    run.trigger_type === 'cron' &&
    run.attempt === 1 &&
    ['failed', 'timeout'].includes(run.status) &&
    !isCancelled;

  return { displayStatus, displayReason, isCancelled, isRetryDelay, isRunning, isCountedAsCronFailure };
}

/**
 * Compute consecutive cron failure streak, using the SAME algorithm as the backend
 * (checkAndHandleMaxAttempts in cron-tick).
 * 
 * Rules:
 * - Only primary runs (attempt=1) are considered for the chain.
 * - Walk from most recent to oldest.
 * - Count consecutive failed/timeout (non-cancelled) runs.
 * - Stop at first success/success_with_warning (reset point).
 * - Other statuses (running, skipped, cancelled) are skipped (don't count, don't break).
 */
export function computeCronFailureStreak(
  cronRuns: SyncRunRecord[]
): { streak: number; streakRunIds: string[]; resetRunId: string | null } {
  // Sort deterministically then filter to primary only
  const sorted = sortRunsDeterministically(cronRuns);
  const primary = sorted.filter((r) => r.attempt === 1);

  let streak = 0;
  const streakRunIds: string[] = [];
  let resetRunId: string | null = null;

  for (const run of primary) {
    if (['success', 'success_with_warning'].includes(run.status)) {
      resetRunId = run.id;
      break;
    }
    const isCancelled = run.cancelled_by_user === true;
    if (['failed', 'timeout'].includes(run.status) && !isCancelled) {
      streak++;
      streakRunIds.push(run.id);
    }
    // Other statuses: skip (don't count, don't break chain)
  }

  return { streak, streakRunIds, resetRunId };
}

/**
 * Determine if auto-disable banner should show and which run to link to.
 */
export function deriveAutoDisableInfo(
  config: { enabled: boolean; max_attempts: number; last_disabled_reason: string | null },
  cronRuns: SyncRunRecord[]
): {
  shouldShowBanner: boolean;
  streak: number;
  maxAttempts: number;
  targetRunId: string | null;
  reason: string | null;
} {
  const maxAttempts = Math.min(config.max_attempts || 3, 5);

  // Banner only if disabled AND reason indicates auto-disable (not manual)
  const isAutoDisabled =
    !config.enabled &&
    config.last_disabled_reason != null &&
    config.last_disabled_reason.toLowerCase().includes('auto-disabilitato');

  if (!isAutoDisabled) {
    return { shouldShowBanner: false, streak: 0, maxAttempts, targetRunId: null, reason: null };
  }

  const { streak, streakRunIds } = computeCronFailureStreak(cronRuns);

  // The target run is the most recent one in the streak
  const targetRunId = streakRunIds.length > 0 ? streakRunIds[0] : null;

  return {
    shouldShowBanner: streak >= maxAttempts,
    streak,
    maxAttempts,
    targetRunId,
    reason: config.last_disabled_reason,
  };
}
