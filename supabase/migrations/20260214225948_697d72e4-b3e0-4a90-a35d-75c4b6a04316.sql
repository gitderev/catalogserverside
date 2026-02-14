
-- P1-A: Atomic log_sync_event RPC
-- P1-B: sync_locks table + try_acquire_sync_lock + release_sync_lock RPCs

-- ============================================================
-- P1-A: log_sync_event RPC
-- ============================================================
CREATE OR REPLACE FUNCTION public.log_sync_event(
  p_run_id uuid,
  p_level text,
  p_message text,
  p_details jsonb DEFAULT '{}'::jsonb
)
RETURNS TABLE(event_id uuid, new_warning_count integer)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_event_id uuid;
  v_warning_count integer;
BEGIN
  -- Validate run_id exists
  IF NOT EXISTS (SELECT 1 FROM public.sync_runs WHERE id = p_run_id) THEN
    RAISE EXCEPTION 'Sync run % not found', p_run_id;
  END IF;

  -- Validate level
  IF p_level NOT IN ('INFO', 'WARN', 'ERROR') THEN
    RAISE EXCEPTION 'Invalid level: %. Must be INFO, WARN, or ERROR', p_level;
  END IF;

  -- Insert the event using details column
  INSERT INTO public.sync_events (run_id, level, message, details)
  VALUES (p_run_id, p_level, p_message, p_details)
  RETURNING id INTO v_event_id;

  -- If WARN, atomically increment warning_count
  IF p_level = 'WARN' THEN
    UPDATE public.sync_runs
    SET warning_count = warning_count + 1
    WHERE id = p_run_id
    RETURNING warning_count INTO v_warning_count;
  ELSE
    SELECT warning_count INTO v_warning_count
    FROM public.sync_runs WHERE id = p_run_id;
  END IF;

  RETURN QUERY SELECT v_event_id, v_warning_count;
END;
$$;

-- ============================================================
-- P1-B: sync_locks table (recreate with correct schema)
-- ============================================================
-- Drop existing table and recreate with correct columns
DROP TABLE IF EXISTS public.sync_locks;

CREATE TABLE public.sync_locks (
  lock_name text PRIMARY KEY,
  run_id uuid NOT NULL,
  acquired_at timestamptz NOT NULL DEFAULT now(),
  lease_until timestamptz NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE public.sync_locks ENABLE ROW LEVEL SECURITY;

-- Service role policy (SECURITY DEFINER RPCs handle access)
CREATE POLICY "service_role_sync_locks" ON public.sync_locks
  FOR ALL USING (true) WITH CHECK (true);

-- ============================================================
-- P1-B: try_acquire_sync_lock RPC
-- ============================================================
CREATE OR REPLACE FUNCTION public.try_acquire_sync_lock(
  p_lock_name text,
  p_run_id uuid,
  p_ttl_seconds integer
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_lease_until timestamptz;
BEGIN
  -- Delete expired locks
  DELETE FROM public.sync_locks
  WHERE lock_name = p_lock_name AND lease_until < now();

  v_lease_until := now() + (p_ttl_seconds * interval '1 second');

  -- Attempt insert
  BEGIN
    INSERT INTO public.sync_locks (lock_name, run_id, lease_until, updated_at)
    VALUES (p_lock_name, p_run_id, v_lease_until, now());
    RETURN true;
  EXCEPTION WHEN unique_violation THEN
    RETURN false;
  END;
END;
$$;

-- ============================================================
-- P1-B: release_sync_lock RPC
-- ============================================================
CREATE OR REPLACE FUNCTION public.release_sync_lock(
  p_lock_name text,
  p_run_id uuid
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_rows integer;
BEGIN
  DELETE FROM public.sync_locks
  WHERE lock_name = p_lock_name AND run_id = p_run_id;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows > 0;
END;
$$;
