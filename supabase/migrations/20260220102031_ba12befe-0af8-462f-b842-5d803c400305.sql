-- Create merge_sync_run_metrics RPC for atomic metrics updates
CREATE OR REPLACE FUNCTION public.merge_sync_run_metrics(p_run_id uuid, p_patch jsonb)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
BEGIN
  UPDATE public.sync_runs
  SET metrics = public.jsonb_deep_merge(COALESCE(metrics, '{}'::jsonb), COALESCE(p_patch, '{}'::jsonb))
  WHERE id = p_run_id;
END;
$function$;