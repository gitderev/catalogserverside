/**
 * _shared/lock.ts - Lock ownership helpers for global_sync lock.
 *
 * Provides ownership-safe acquire/renew and assert functions.
 * These replace any brute-force delete on sync_locks.
 *
 * TTL_SECONDS must match the value used in run-full-sync (120s).
 *
 * INVOCATION GUARD (Codex fix):
 * Each orchestrator invocation generates a unique invocation_id (UUID).
 * The lock row stores this invocation_id. Renew and assert operations
 * verify BOTH run_id AND invocation_id match, preventing concurrent
 * invocations for the same run_id from both holding the lock.
 */

type SupabaseClient = {
  from: (table: string) => any;
  rpc: (fn: string, params: Record<string, unknown>) => Promise<{ data: any; error: any }>;
};

export const LOCK_NAME = 'global_sync';
export const LOCK_TTL_SECONDS = 120;

/**
 * Generate a unique invocation ID for this orchestrator call.
 */
export function generateInvocationId(): string {
  return crypto.randomUUID();
}

/**
 * Acquire or renew the global_sync lock for a given runId + invocationId.
 *
 * 1. Try ownership-safe UPDATE (renew lease if we already own it with matching invocation_id).
 * 2. If no rows updated, try RPC try_acquire_sync_lock (new acquisition).
 * 3. Returns true if lock is held by this invocation, false otherwise.
 */
export async function acquireOrRenewGlobalLock(
  supabase: SupabaseClient,
  runId: string,
  ttlSeconds: number = LOCK_TTL_SECONDS,
  invocationId?: string
): Promise<boolean> {
  const newLeaseUntil = new Date(Date.now() + ttlSeconds * 1000).toISOString();

  // Step 1: Try ownership-safe renew (must match BOTH run_id AND invocation_id)
  const query = supabase
    .from('sync_locks')
    .update({ lease_until: newLeaseUntil, updated_at: new Date().toISOString() })
    .eq('lock_name', LOCK_NAME)
    .eq('run_id', runId);

  // If invocationId provided, require it to match for renewal
  if (invocationId) {
    query.eq('invocation_id', invocationId);
  }

  const { data: renewed, error: renewErr } = await query
    .select('lock_name')
    .maybeSingle();

  if (!renewErr && renewed) {
    return true; // Renewed successfully
  }

  // Step 2: Try fresh acquisition via RPC (includes invocation_id)
  const { data: acquired } = await supabase.rpc('try_acquire_sync_lock', {
    p_lock_name: LOCK_NAME,
    p_run_id: runId,
    p_ttl_seconds: ttlSeconds,
    p_invocation_id: invocationId || null
  });

  return acquired === true;
}

/**
 * Renew the lease for an already-owned lock (ownership-safe).
 * Requires matching invocation_id if provided.
 * Returns true if renewal succeeded (we own the lock), false otherwise.
 */
export async function renewLockLease(
  supabase: SupabaseClient,
  runId: string,
  ttlSeconds: number = LOCK_TTL_SECONDS,
  invocationId?: string
): Promise<boolean> {
  const newLeaseUntil = new Date(Date.now() + ttlSeconds * 1000).toISOString();
  const query = supabase
    .from('sync_locks')
    .update({ lease_until: newLeaseUntil, updated_at: new Date().toISOString() })
    .eq('lock_name', LOCK_NAME)
    .eq('run_id', runId);

  if (invocationId) {
    query.eq('invocation_id', invocationId);
  }

  const { data, error } = await query
    .select('lock_name')
    .maybeSingle();

  return !error && !!data;
}

/**
 * Assert that the current runId (and optionally invocationId) owns the global_sync lock
 * and it hasn't expired.
 * Returns { owned: true } or { owned: false, holder_run_id, holder_invocation_id, lease_until }.
 * Does NOT throw; caller decides how to handle.
 */
export async function assertLockOwned(
  supabase: SupabaseClient,
  runId: string,
  invocationId?: string
): Promise<{ owned: boolean; holder_run_id?: string; holder_invocation_id?: string; lease_until?: string }> {
  const { data: lock, error } = await supabase
    .from('sync_locks')
    .select('run_id, invocation_id, lease_until')
    .eq('lock_name', LOCK_NAME)
    .maybeSingle();

  if (error || !lock) {
    return { owned: false, holder_run_id: 'none' };
  }

  const leaseUntil = new Date(lock.lease_until).getTime();
  if (lock.run_id !== runId) {
    return { owned: false, holder_run_id: lock.run_id, holder_invocation_id: lock.invocation_id, lease_until: lock.lease_until };
  }
  
  // If invocationId provided, verify it matches
  if (invocationId && lock.invocation_id && lock.invocation_id !== invocationId) {
    return { owned: false, holder_run_id: lock.run_id, holder_invocation_id: lock.invocation_id, lease_until: lock.lease_until };
  }
  
  if (Date.now() > leaseUntil) {
    return { owned: false, holder_run_id: lock.run_id, holder_invocation_id: lock.invocation_id, lease_until: lock.lease_until };
  }

  return { owned: true };
}
