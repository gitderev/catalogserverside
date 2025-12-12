-- Create sync_locks table for distributed locking
-- Used by run-full-sync to ensure only one active pipeline at a time

CREATE TABLE IF NOT EXISTS public.sync_locks (
  lock_key TEXT PRIMARY KEY,
  locked_by TEXT NOT NULL,
  locked_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
  run_id TEXT
);

-- Enable RLS but allow service role full access
ALTER TABLE public.sync_locks ENABLE ROW LEVEL SECURITY;

-- Policy for service role access (edge functions)
CREATE POLICY "Service role can manage locks" ON public.sync_locks
  FOR ALL
  USING (true)
  WITH CHECK (true);

-- Add comment
COMMENT ON TABLE public.sync_locks IS 'Distributed lock table for pipeline orchestration. Prevents concurrent runs.';
COMMENT ON COLUMN public.sync_locks.lock_key IS 'Unique lock identifier (e.g., "pipeline")';
COMMENT ON COLUMN public.sync_locks.locked_by IS 'Identifier of the process holding the lock';
COMMENT ON COLUMN public.sync_locks.expires_at IS 'Lock expiration time for automatic release';
COMMENT ON COLUMN public.sync_locks.run_id IS 'Associated sync run ID';