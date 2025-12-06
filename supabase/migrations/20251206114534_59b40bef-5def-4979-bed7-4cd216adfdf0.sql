-- Fix admin_users RLS: only allow users to see their own row
DROP POLICY IF EXISTS "admin_users_select_self" ON public.admin_users;
DROP POLICY IF EXISTS "admin_users_select_any" ON public.admin_users;
DROP POLICY IF EXISTS "admin_users_select_public" ON public.admin_users;

-- Ensure RLS is enabled
ALTER TABLE public.admin_users ENABLE ROW LEVEL SECURITY;

-- Create restrictive SELECT policy - users can only see their own row
CREATE POLICY "admin_users_select_self"
ON public.admin_users
FOR SELECT
TO authenticated
USING (auth.uid() = user_id);

-- Fix fee_config RLS: drop all permissive policies
DROP POLICY IF EXISTS "fee_config_select_any" ON public.fee_config;
DROP POLICY IF EXISTS "fee_config_insert_any" ON public.fee_config;
DROP POLICY IF EXISTS "fee_config_update_any" ON public.fee_config;
DROP POLICY IF EXISTS "fee_config_admin_select" ON public.fee_config;
DROP POLICY IF EXISTS "fee_config_admin_insert" ON public.fee_config;
DROP POLICY IF EXISTS "fee_config_admin_update" ON public.fee_config;

-- Ensure RLS is enabled
ALTER TABLE public.fee_config ENABLE ROW LEVEL SECURITY;

-- Create admin-only SELECT policy
CREATE POLICY "fee_config_admin_select"
ON public.fee_config
FOR SELECT
TO authenticated
USING (
  auth.uid() IN (SELECT user_id FROM public.admin_users)
);

-- Create admin-only INSERT policy
CREATE POLICY "fee_config_admin_insert"
ON public.fee_config
FOR INSERT
TO authenticated
WITH CHECK (
  auth.uid() IN (SELECT user_id FROM public.admin_users)
);

-- Create admin-only UPDATE policy
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