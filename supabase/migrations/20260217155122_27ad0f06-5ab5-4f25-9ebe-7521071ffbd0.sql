
-- Wrapper function for pg_cron: reads CRON_SECRET from vault and calls cron-tick edge function
CREATE OR REPLACE FUNCTION public.invoke_cron_tick()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $$
DECLARE
  v_key text;
  v_url text := 'https://emsestdnnrajhncpkpai.supabase.co/functions/v1/cron-tick';
BEGIN
  -- Read auth key from vault (name: cron_tick_auth_key)
  SELECT decrypted_secret INTO v_key
  FROM vault.decrypted_secrets
  WHERE name = 'cron_tick_auth_key'
  LIMIT 1;

  IF v_key IS NULL THEN
    RAISE LOG 'invoke_cron_tick: cron_tick_auth_key not found in vault, skipping';
    RETURN;
  END IF;

  PERFORM net.http_post(
    url := v_url,
    headers := jsonb_build_object(
      'Content-Type', 'application/json',
      'x-cron-secret', v_key
    ),
    body := jsonb_build_object('timestamp', now()::text, 'source', 'pg_cron')
  );
END;
$$;
