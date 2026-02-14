
-- ============================================================
-- P1 Hardening: Lock down sync_locks, log_sync_event, 
-- try_acquire_sync_lock, release_sync_lock to service_role only
-- ============================================================

-- Step 0: Verify all required objects exist
DO $$
BEGIN
  -- Verify sync_locks table
  IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='sync_locks') THEN
    RAISE EXCEPTION 'MISSING: public.sync_locks table does not exist';
  END IF;

  -- Verify log_sync_event function
  IF NOT EXISTS (
    SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
    WHERE n.nspname='public' AND p.proname='log_sync_event'
  ) THEN
    RAISE EXCEPTION 'MISSING: public.log_sync_event function does not exist';
  END IF;

  -- Verify try_acquire_sync_lock function
  IF NOT EXISTS (
    SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
    WHERE n.nspname='public' AND p.proname='try_acquire_sync_lock'
  ) THEN
    RAISE EXCEPTION 'MISSING: public.try_acquire_sync_lock function does not exist';
  END IF;

  -- Verify release_sync_lock function
  IF NOT EXISTS (
    SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
    WHERE n.nspname='public' AND p.proname='release_sync_lock'
  ) THEN
    RAISE EXCEPTION 'MISSING: public.release_sync_lock function does not exist';
  END IF;
END $$;

-- Step 1: Drop permissive policy on sync_locks
DROP POLICY IF EXISTS "service_role_sync_locks" ON public.sync_locks;
DROP POLICY IF EXISTS "Service role can manage locks" ON public.sync_locks;

-- Step 2: Ensure RLS is enabled
ALTER TABLE public.sync_locks ENABLE ROW LEVEL SECURITY;

-- Step 3: Revoke ALL table privileges from public, anon, authenticated
REVOKE ALL ON public.sync_locks FROM PUBLIC;
REVOKE ALL ON public.sync_locks FROM anon;
REVOKE ALL ON public.sync_locks FROM authenticated;

-- Step 4: Grant table privileges to service_role (needed by SECURITY DEFINER owned by postgres,
-- but explicit grant ensures service_role direct access too)
GRANT ALL ON public.sync_locks TO service_role;

-- Step 5: Revoke EXECUTE on all three RPC functions from public, anon, authenticated
REVOKE EXECUTE ON FUNCTION public.log_sync_event(uuid, text, text, jsonb) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.log_sync_event(uuid, text, text, jsonb) FROM anon;
REVOKE EXECUTE ON FUNCTION public.log_sync_event(uuid, text, text, jsonb) FROM authenticated;

REVOKE EXECUTE ON FUNCTION public.try_acquire_sync_lock(text, uuid, integer) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.try_acquire_sync_lock(text, uuid, integer) FROM anon;
REVOKE EXECUTE ON FUNCTION public.try_acquire_sync_lock(text, uuid, integer) FROM authenticated;

REVOKE EXECUTE ON FUNCTION public.release_sync_lock(text, uuid) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.release_sync_lock(text, uuid) FROM anon;
REVOKE EXECUTE ON FUNCTION public.release_sync_lock(text, uuid) FROM authenticated;

-- Step 6: Grant EXECUTE only to service_role
GRANT EXECUTE ON FUNCTION public.log_sync_event(uuid, text, text, jsonb) TO service_role;
GRANT EXECUTE ON FUNCTION public.try_acquire_sync_lock(text, uuid, integer) TO service_role;
GRANT EXECUTE ON FUNCTION public.release_sync_lock(text, uuid) TO service_role;
