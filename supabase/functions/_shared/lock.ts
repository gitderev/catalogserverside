/**
 * _shared/lock.ts - Lock ownership helpers for global_sync lock.
 *
 * Provides ownership-safe acquire/renew and assert functions.
 * These replace any brute-force delete on sync_locks.
 *
 * TTL_SECONDS must match the value used in run-full-sync (120s).
 */

type SupabaseClient = {
  from: (table: string) => any;
  rpc: (fn: string, params: Record<string, unknown>) => Promise<{ data: any; error: any }>;
};

export const LOCK_NAME = 'global_sync';
export const LOCK_TTL_SECONDS = 120;

/**
 * Acquire or renew the global_sync lock for a given runId.
 *
 * 1. Try ownership-safe UPDATE (renew lease if we already own it).
 * 2. If no rows updated, try RPC try_acquire_sync_lock (new acquisition).
 * 3. Returns true if lock is held by runId, false otherwise.
 */
export async function acquireOrRenewGlobalLock(
  supabase: SupabaseClient,
  runId: string,
  ttlSeconds: number = LOCK_TTL_SECONDS
): Promise<boolean> {
  const newLeaseUntil = new Date(Date.now() + ttlSeconds * 1000).toISOString();

  // Step 1: Try ownership-safe renew
  const { data: renewed, error: renewErr } = await supabase
    .from('sync_locks')
    .update({ lease_until: newLeaseUntil, updated_at: new Date().toISOString() })
    .eq('lock_name', LOCK_NAME)
    .eq('run_id', runId)
    .select('lock_name')
    .maybeSingle();

  if (!renewErr && renewed) {
    return true; // Renewed successfully
  }

  // Step 2: Try fresh acquisition via RPC
  const { data: acquired } = await supabase.rpc('try_acquire_sync_lock', {
    p_lock_name: LOCK_NAME,
    p_run_id: runId,
    p_ttl_seconds: ttlSeconds
  });

  return acquired === true;
}

/**
 * Renew the lease for an already-owned lock (ownership-safe).
 * Returns true if renewal succeeded (we own the lock), false otherwise.
 */
export async function renewLockLease(
  supabase: SupabaseClient,
  runId: string,
  ttlSeconds: number = LOCK_TTL_SECONDS
): Promise<boolean> {
  const newLeaseUntil = new Date(Date.now() + ttlSeconds * 1000).toISOString();
  const { data, error } = await supabase
    .from('sync_locks')
    .update({ lease_until: newLeaseUntil, updated_at: new Date().toISOString() })
    .eq('lock_name', LOCK_NAME)
    .eq('run_id', runId)
    .select('lock_name')
    .maybeSingle();

  return !error && !!data;
}

/**
 * Assert that the current runId owns the global_sync lock and it hasn't expired.
 * Returns { owned: true } or { owned: false, holder_run_id, lease_until }.
 * Does NOT throw; caller decides how to handle.
 */
export async function assertLockOwned(
  supabase: SupabaseClient,
  runId: string
): Promise<{ owned: boolean; holder_run_id?: string; lease_until?: string }> {
  const { data: lock, error } = await supabase
    .from('sync_locks')
    .select('run_id, lease_until')
    .eq('lock_name', LOCK_NAME)
    .maybeSingle();

  if (error || !lock) {
    return { owned: false, holder_run_id: 'none' };
  }

  const leaseUntil = new Date(lock.lease_until).getTime();
  if (lock.run_id !== runId) {
    return { owned: false, holder_run_id: lock.run_id, lease_until: lock.lease_until };
  }
  if (Date.now() > leaseUntil) {
    return { owned: false, holder_run_id: lock.run_id, lease_until: lock.lease_until };
  }

  return { owned: true };
}
