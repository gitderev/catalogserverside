
-- Drop any legacy check constraint on sync_runs.status (idempotent)
DO $$
BEGIN
  -- Try dropping common constraint names
  ALTER TABLE public.sync_runs DROP CONSTRAINT IF EXISTS sync_runs_status_check;
  ALTER TABLE public.sync_runs DROP CONSTRAINT IF EXISTS check_status;
  ALTER TABLE public.sync_runs DROP CONSTRAINT IF EXISTS valid_status;
  
  -- Also try to find and drop any check constraint on the status column dynamically
  DECLARE
    constraint_name text;
  BEGIN
    FOR constraint_name IN
      SELECT con.conname
      FROM pg_constraint con
      JOIN pg_attribute att ON att.attnum = ANY(con.conkey) AND att.attrelid = con.conrelid
      WHERE con.conrelid = 'public.sync_runs'::regclass
        AND att.attname = 'status'
        AND con.contype = 'c'
    LOOP
      EXECUTE format('ALTER TABLE public.sync_runs DROP CONSTRAINT IF EXISTS %I', constraint_name);
    END LOOP;
  END;
END
$$;

-- Add check constraint with all valid statuses including success_with_warning
ALTER TABLE public.sync_runs ADD CONSTRAINT sync_runs_status_check
  CHECK (status IN ('running', 'success', 'success_with_warning', 'failed', 'timeout', 'skipped'));
