
-- ============================================================
-- jsonb_deep_merge: recursive deep merge for JSONB objects.
-- Rules:
--   object + object = recursive merge (preserves nested keys)
--   array = overwrite (patch wins)
--   scalar/null = overwrite (patch wins)
-- Backward compatible: existing shallow merge behavior unchanged
-- for non-object values; only adds recursive handling for nested objects.
-- ============================================================
CREATE OR REPLACE FUNCTION public.jsonb_deep_merge(base jsonb, patch jsonb)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
SET search_path TO 'public'
AS $$
DECLARE
  result jsonb := base;
  key text;
  base_val jsonb;
  patch_val jsonb;
BEGIN
  -- If either is not an object, patch wins (overwrite semantics)
  IF jsonb_typeof(base) != 'object' OR jsonb_typeof(patch) != 'object' THEN
    RETURN patch;
  END IF;

  FOR key IN SELECT jsonb_object_keys(patch)
  LOOP
    patch_val := patch -> key;
    base_val := base -> key;

    IF base_val IS NOT NULL
       AND jsonb_typeof(base_val) = 'object'
       AND jsonb_typeof(patch_val) = 'object' THEN
      -- Recursive merge for nested objects
      result := jsonb_set(result, ARRAY[key], public.jsonb_deep_merge(base_val, patch_val));
    ELSE
      -- Overwrite (scalar, array, null, or key doesn't exist in base)
      result := jsonb_set(result, ARRAY[key], patch_val);
    END IF;
  END LOOP;

  RETURN result;
END;
$$;

-- ============================================================
-- Update merge_sync_run_step to use jsonb_deep_merge instead of
-- shallow || operator. This preserves nested fields (e.g. retry
-- sub-keys) when patching only a subset.
--
-- Example:
--   existing: {"retry":{"retry_attempt":1,"next_retry_at":"..."}}
--   patch:    {"retry":{"retry_attempt":2}}
--   result:   {"retry":{"retry_attempt":2,"next_retry_at":"..."}}
-- ============================================================
CREATE OR REPLACE FUNCTION public.merge_sync_run_step(p_run_id uuid, p_step_name text, p_patch jsonb)
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
    public.jsonb_deep_merge(COALESCE(steps->p_step_name, '{}'::jsonb), p_patch),
    true
  )
  WHERE id = p_run_id;
END;
$$;
