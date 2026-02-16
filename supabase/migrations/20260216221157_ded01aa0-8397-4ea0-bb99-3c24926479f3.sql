
-- 1. Add invocation_id to sync_locks for non-reentrant locking per run_id
ALTER TABLE public.sync_locks ADD COLUMN IF NOT EXISTS invocation_id uuid;

-- 2. Update try_acquire_sync_lock to support invocation_id
CREATE OR REPLACE FUNCTION public.try_acquire_sync_lock(
  p_lock_name text,
  p_run_id uuid,
  p_ttl_seconds integer,
  p_invocation_id uuid DEFAULT NULL
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
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
    INSERT INTO public.sync_locks (lock_name, run_id, lease_until, updated_at, invocation_id)
    VALUES (p_lock_name, p_run_id, v_lease_until, now(), p_invocation_id);
    RETURN true;
  EXCEPTION WHEN unique_violation THEN
    RETURN false;
  END;
END;
$$;

-- 3. Atomic step merge RPC: uses jsonb || operator for merge without overwriting other steps
CREATE OR REPLACE FUNCTION public.merge_sync_run_step(
  p_run_id uuid,
  p_step_name text,
  p_patch jsonb
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $$
BEGIN
  UPDATE public.sync_runs
  SET steps = jsonb_set(
    steps,
    ARRAY[p_step_name],
    COALESCE(steps->p_step_name, '{}'::jsonb) || p_patch,
    true
  )
  WHERE id = p_run_id;
END;
$$;

-- 4. Atomic set current_step + step state in one operation
CREATE OR REPLACE FUNCTION public.set_step_in_progress(
  p_run_id uuid,
  p_step_name text,
  p_extra jsonb DEFAULT '{}'::jsonb
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $$
DECLARE
  v_existing jsonb;
  v_merged jsonb;
BEGIN
  SELECT COALESCE(steps->p_step_name, '{}'::jsonb) INTO v_existing
  FROM public.sync_runs WHERE id = p_run_id;

  v_merged := v_existing || '{"status":"in_progress"}'::jsonb || p_extra;

  -- If no started_at exists, add it
  IF NOT (v_existing ? 'started_at') THEN
    v_merged := v_merged || jsonb_build_object('started_at', now()::text);
  END IF;

  UPDATE public.sync_runs
  SET steps = jsonb_set(
    jsonb_set(steps, ARRAY[p_step_name], v_merged, true),
    '{current_step}',
    to_jsonb(p_step_name),
    true
  )
  WHERE id = p_run_id;
END;
$$;

-- 5. Tighten RLS on sync_config: admin-only SELECT and UPDATE
DROP POLICY IF EXISTS sync_config_select_authenticated ON public.sync_config;
DROP POLICY IF EXISTS sync_config_update_authenticated ON public.sync_config;

CREATE POLICY sync_config_select_admin ON public.sync_config
FOR SELECT USING (
  EXISTS (SELECT 1 FROM admin_users au WHERE au.user_id = auth.uid())
);

CREATE POLICY sync_config_update_admin ON public.sync_config
FOR UPDATE USING (
  EXISTS (SELECT 1 FROM admin_users au WHERE au.user_id = auth.uid())
) WITH CHECK (
  EXISTS (SELECT 1 FROM admin_users au WHERE au.user_id = auth.uid())
);

-- 6. Tighten RLS on sync_runs: admin-only SELECT, INSERT, UPDATE
DROP POLICY IF EXISTS sync_runs_select_authenticated ON public.sync_runs;
DROP POLICY IF EXISTS sync_runs_insert_authenticated ON public.sync_runs;
DROP POLICY IF EXISTS sync_runs_update_authenticated ON public.sync_runs;

CREATE POLICY sync_runs_select_admin ON public.sync_runs
FOR SELECT USING (
  EXISTS (SELECT 1 FROM admin_users au WHERE au.user_id = auth.uid())
);

CREATE POLICY sync_runs_insert_admin ON public.sync_runs
FOR INSERT WITH CHECK (
  EXISTS (SELECT 1 FROM admin_users au WHERE au.user_id = auth.uid())
);

CREATE POLICY sync_runs_update_admin ON public.sync_runs
FOR UPDATE USING (
  EXISTS (SELECT 1 FROM admin_users au WHERE au.user_id = auth.uid())
) WITH CHECK (
  EXISTS (SELECT 1 FROM admin_users au WHERE au.user_id = auth.uid())
);

-- 7. max_attempts validation trigger on sync_config
CREATE OR REPLACE FUNCTION public.validate_max_attempts()
RETURNS trigger
LANGUAGE plpgsql
SET search_path TO 'public'
AS $$
BEGIN
  IF NEW.max_attempts > 5 THEN
    NEW.max_attempts := 5;
  END IF;
  IF NEW.max_attempts < 1 THEN
    NEW.max_attempts := 1;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS enforce_max_attempts ON public.sync_config;
CREATE TRIGGER enforce_max_attempts
BEFORE INSERT OR UPDATE ON public.sync_config
FOR EACH ROW
EXECUTE FUNCTION public.validate_max_attempts();
