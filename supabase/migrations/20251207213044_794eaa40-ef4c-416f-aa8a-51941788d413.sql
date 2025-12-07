-- =====================================================
-- SECURITY FIX: RLS policies for fee_config and admin_users
-- Risolve PUBLIC_SENSITIVE_DATA e PUBLIC_USER_DATA
-- =====================================================

-- =====================================================
-- STEP 1: Rimuovi le policy RESTRICTIVE esistenti su fee_config
-- Le policy RESTRICTIVE senza policy PERMISSIVE non proteggono efficacemente
-- =====================================================
DROP POLICY IF EXISTS "fee_config_admin_update" ON public.fee_config;
DROP POLICY IF EXISTS "fee_config_admin_select" ON public.fee_config;
DROP POLICY IF EXISTS "fee_config_admin_insert" ON public.fee_config;

-- =====================================================
-- STEP 2: Crea policy PERMISSIVE per fee_config
-- Solo gli utenti presenti in admin_users possono accedere
-- =====================================================

-- SELECT: Solo admin possono leggere
CREATE POLICY "fee_config_select_admin_only"
ON public.fee_config
FOR SELECT
TO authenticated
USING (
  EXISTS (
    SELECT 1 FROM public.admin_users au
    WHERE au.user_id = auth.uid()
  )
);

-- INSERT: Solo admin possono inserire
CREATE POLICY "fee_config_insert_admin_only"
ON public.fee_config
FOR INSERT
TO authenticated
WITH CHECK (
  EXISTS (
    SELECT 1 FROM public.admin_users au
    WHERE au.user_id = auth.uid()
  )
);

-- UPDATE: Solo admin possono aggiornare
CREATE POLICY "fee_config_update_admin_only"
ON public.fee_config
FOR UPDATE
TO authenticated
USING (
  EXISTS (
    SELECT 1 FROM public.admin_users au
    WHERE au.user_id = auth.uid()
  )
)
WITH CHECK (
  EXISTS (
    SELECT 1 FROM public.admin_users au
    WHERE au.user_id = auth.uid()
  )
);

-- DELETE: Solo admin possono eliminare
CREATE POLICY "fee_config_delete_admin_only"
ON public.fee_config
FOR DELETE
TO authenticated
USING (
  EXISTS (
    SELECT 1 FROM public.admin_users au
    WHERE au.user_id = auth.uid()
  )
);

-- =====================================================
-- STEP 3: Rimuovi la policy RESTRICTIVE esistente su admin_users
-- =====================================================
DROP POLICY IF EXISTS "admin_users_select_self" ON public.admin_users;

-- =====================================================
-- STEP 4: Crea policy PERMISSIVE per admin_users
-- Ogni utente autenticato può vedere solo la propria riga
-- =====================================================

-- SELECT: Utente può vedere solo la propria riga
CREATE POLICY "admin_users_select_own_row"
ON public.admin_users
FOR SELECT
TO authenticated
USING (auth.uid() = user_id);