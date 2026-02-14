
-- ============================================================
-- Extend sync_config for schedule_type, notifications, etc.
-- ============================================================

-- Add schedule_type column ('hours' or 'daily')
ALTER TABLE public.sync_config 
  ADD COLUMN IF NOT EXISTS schedule_type text NOT NULL DEFAULT 'hours'
    CHECK (schedule_type IN ('hours', 'daily'));

-- Add run timeout
ALTER TABLE public.sync_config 
  ADD COLUMN IF NOT EXISTS run_timeout_minutes integer NOT NULL DEFAULT 60;

-- Add max_attempts (separate from legacy max_retries)
ALTER TABLE public.sync_config 
  ADD COLUMN IF NOT EXISTS max_attempts integer NOT NULL DEFAULT 3;

-- Add notification columns
ALTER TABLE public.sync_config 
  ADD COLUMN IF NOT EXISTS notification_mode text NOT NULL DEFAULT 'never'
    CHECK (notification_mode IN ('never', 'only_on_problem', 'always'));

ALTER TABLE public.sync_config 
  ADD COLUMN IF NOT EXISTS notify_on_warning boolean NOT NULL DEFAULT true;

-- Add last_disabled_reason
ALTER TABLE public.sync_config 
  ADD COLUMN IF NOT EXISTS last_disabled_reason text;

-- Add constraint: frequency_minutes >= 60 and multiple of 60
-- (using a trigger instead of CHECK for flexibility)
CREATE OR REPLACE FUNCTION public.validate_sync_config()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = public
AS $$
BEGIN
  -- For 'hours' mode, frequency_minutes must be >= 60 and multiple of 60
  IF NEW.schedule_type = 'hours' THEN
    IF NEW.frequency_minutes < 60 THEN
      RAISE EXCEPTION 'frequency_minutes must be >= 60 for hours mode';
    END IF;
    IF NEW.frequency_minutes % 60 != 0 THEN
      RAISE EXCEPTION 'frequency_minutes must be a multiple of 60 for hours mode';
    END IF;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS validate_sync_config_trigger ON public.sync_config;
CREATE TRIGGER validate_sync_config_trigger
  BEFORE INSERT OR UPDATE ON public.sync_config
  FOR EACH ROW
  EXECUTE FUNCTION public.validate_sync_config();

-- ============================================================
-- Extend sync_runs for warning_count, file_manifest
-- ============================================================

ALTER TABLE public.sync_runs 
  ADD COLUMN IF NOT EXISTS warning_count integer NOT NULL DEFAULT 0;

ALTER TABLE public.sync_runs 
  ADD COLUMN IF NOT EXISTS file_manifest jsonb NOT NULL DEFAULT '{}'::jsonb;

-- ============================================================
-- Create sync_events table for per-run events/warnings
-- ============================================================
CREATE TABLE IF NOT EXISTS public.sync_events (
  id uuid NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
  run_id uuid NOT NULL,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  level text NOT NULL CHECK (level IN ('INFO', 'WARN', 'ERROR')),
  step text,
  message text NOT NULL,
  details jsonb
);

ALTER TABLE public.sync_events ENABLE ROW LEVEL SECURITY;

-- RLS: admin-only read access
CREATE POLICY "sync_events_select_admin"
  ON public.sync_events
  FOR SELECT
  USING (EXISTS (
    SELECT 1 FROM admin_users au WHERE au.user_id = auth.uid()
  ));

-- Service role can insert (for edge functions)
CREATE POLICY "sync_events_insert_service"
  ON public.sync_events
  FOR INSERT
  WITH CHECK (true);

-- Create index for efficient queries
CREATE INDEX IF NOT EXISTS idx_sync_events_run_id ON public.sync_events(run_id);
CREATE INDEX IF NOT EXISTS idx_sync_events_level ON public.sync_events(level);

-- ============================================================
-- Update sync_config RLS to allow admin access for new columns
-- (existing policies already cover admin-only for sync_config)
-- ============================================================

-- ============================================================
-- Update sync_runs: allow 'success_with_warning' and 'timeout' status values
-- (No constraint on status column exists, just text - no changes needed)
-- ============================================================
