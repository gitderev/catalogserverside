-- =====================================================
-- SYNC SYSTEM: Tables for automatic catalog synchronization
-- Creates sync_config and sync_runs tables with RLS
-- =====================================================

-- =====================================================
-- TABLE: sync_config - Configuration for sync scheduling
-- Single row table (id = 1) for global sync settings
-- =====================================================
CREATE TABLE public.sync_config (
  id integer PRIMARY KEY DEFAULT 1,
  enabled boolean NOT NULL DEFAULT false,
  frequency_minutes integer NOT NULL DEFAULT 60,
  daily_time time without time zone,
  max_retries integer NOT NULL DEFAULT 5,
  retry_delay_minutes integer NOT NULL DEFAULT 5,
  updated_at timestamptz NOT NULL DEFAULT now(),
  
  -- Constraint: only valid frequency values
  CONSTRAINT sync_config_frequency_check CHECK (
    frequency_minutes IN (60, 120, 180, 360, 720, 1440)
  ),
  
  -- Constraint: single row only
  CONSTRAINT sync_config_single_row CHECK (id = 1)
);

-- Insert default configuration row
INSERT INTO public.sync_config (id, enabled, frequency_minutes, max_retries, retry_delay_minutes)
VALUES (1, false, 60, 5, 5);

-- Enable RLS
ALTER TABLE public.sync_config ENABLE ROW LEVEL SECURITY;

-- RLS Policies: any authenticated user can read/update
CREATE POLICY "sync_config_select_authenticated"
ON public.sync_config
FOR SELECT
TO authenticated
USING (auth.uid() IS NOT NULL);

CREATE POLICY "sync_config_update_authenticated"
ON public.sync_config
FOR UPDATE
TO authenticated
USING (auth.uid() IS NOT NULL)
WITH CHECK (auth.uid() IS NOT NULL);

-- Trigger to update updated_at on changes
CREATE OR REPLACE FUNCTION public.update_sync_config_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql SET search_path = public;

CREATE TRIGGER sync_config_updated_at_trigger
BEFORE UPDATE ON public.sync_config
FOR EACH ROW
EXECUTE FUNCTION public.update_sync_config_updated_at();

-- =====================================================
-- TABLE: sync_runs - Tracking each sync job execution
-- =====================================================
CREATE TABLE public.sync_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  status text NOT NULL DEFAULT 'running',
  trigger_type text NOT NULL,
  attempt integer NOT NULL DEFAULT 1,
  runtime_ms bigint,
  error_message text,
  error_details jsonb,
  steps jsonb NOT NULL DEFAULT '{}'::jsonb,
  metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
  cancel_requested boolean NOT NULL DEFAULT false,
  cancelled_by_user boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  
  -- Constraint: valid status values
  CONSTRAINT sync_runs_status_check CHECK (
    status IN ('running', 'success', 'failed', 'timeout', 'skipped')
  ),
  
  -- Constraint: valid trigger_type values
  CONSTRAINT sync_runs_trigger_type_check CHECK (
    trigger_type IN ('cron', 'manual')
  ),
  
  -- Constraint: attempt range (1-5 for cron, 1 for manual)
  CONSTRAINT sync_runs_attempt_check CHECK (
    attempt >= 1 AND attempt <= 5
  )
);

-- Create index for faster queries on status and started_at
CREATE INDEX idx_sync_runs_status ON public.sync_runs(status);
CREATE INDEX idx_sync_runs_started_at ON public.sync_runs(started_at DESC);
CREATE INDEX idx_sync_runs_trigger_type ON public.sync_runs(trigger_type);

-- Enable RLS
ALTER TABLE public.sync_runs ENABLE ROW LEVEL SECURITY;

-- RLS Policies: any authenticated user can read, insert, and update
CREATE POLICY "sync_runs_select_authenticated"
ON public.sync_runs
FOR SELECT
TO authenticated
USING (auth.uid() IS NOT NULL);

CREATE POLICY "sync_runs_insert_authenticated"
ON public.sync_runs
FOR INSERT
TO authenticated
WITH CHECK (auth.uid() IS NOT NULL);

CREATE POLICY "sync_runs_update_authenticated"
ON public.sync_runs
FOR UPDATE
TO authenticated
USING (auth.uid() IS NOT NULL)
WITH CHECK (auth.uid() IS NOT NULL);