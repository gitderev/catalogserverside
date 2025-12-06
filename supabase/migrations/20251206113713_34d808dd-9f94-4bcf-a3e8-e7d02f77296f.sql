-- Create admin_users table for authorized administrators
CREATE TABLE IF NOT EXISTS public.admin_users (
  user_id uuid PRIMARY KEY REFERENCES auth.users (id) ON DELETE CASCADE,
  created_at timestamptz DEFAULT now()
);

-- Drop existing permissive policies on fee_config
DROP POLICY IF EXISTS "fee_config_select_any" ON public.fee_config;
DROP POLICY IF EXISTS "fee_config_insert_any" ON public.fee_config;
DROP POLICY IF EXISTS "fee_config_update_any" ON public.fee_config;

-- Ensure RLS is enabled on fee_config
ALTER TABLE public.fee_config ENABLE ROW LEVEL SECURITY;

-- Create secure admin-only policies for fee_config
CREATE POLICY "fee_config_admin_select"
ON public.fee_config
FOR SELECT
TO authenticated
USING (
  auth.uid() IN (SELECT user_id FROM public.admin_users)
);

CREATE POLICY "fee_config_admin_insert"
ON public.fee_config
FOR INSERT
TO authenticated
WITH CHECK (
  auth.uid() IN (SELECT user_id FROM public.admin_users)
);

CREATE POLICY "fee_config_admin_update"
ON public.fee_config
FOR UPDATE
TO authenticated
USING (
  auth.uid() IN (SELECT user_id FROM public.admin_users)
)
WITH CHECK (
  auth.uid() IN (SELECT user_id FROM public.admin_users)
);