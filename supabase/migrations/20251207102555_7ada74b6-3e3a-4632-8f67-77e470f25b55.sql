-- Step 1: Drop all public policies for ftp-import and mapping-files buckets
DROP POLICY IF EXISTS "Public read access for ftp-import" ON storage.objects;
DROP POLICY IF EXISTS "Service role can update ftp-import" ON storage.objects;
DROP POLICY IF EXISTS "Service role can upload to ftp-import" ON storage.objects;
DROP POLICY IF EXISTS "Anyone can update mapping files" ON storage.objects;
DROP POLICY IF EXISTS "Anyone can upload mapping files" ON storage.objects;
DROP POLICY IF EXISTS "Public can download mapping files" ON storage.objects;

-- Also drop any existing admin policies to avoid conflicts before recreating
DROP POLICY IF EXISTS "Admin read access for ftp-import" ON storage.objects;
DROP POLICY IF EXISTS "Admin upload to ftp-import" ON storage.objects;
DROP POLICY IF EXISTS "Admin update ftp-import" ON storage.objects;
DROP POLICY IF EXISTS "Admin read mapping files" ON storage.objects;
DROP POLICY IF EXISTS "Admin upload mapping files" ON storage.objects;
DROP POLICY IF EXISTS "Admin update mapping files" ON storage.objects;
DROP POLICY IF EXISTS "storage_select_admin_ftp_mapping" ON storage.objects;
DROP POLICY IF EXISTS "storage_insert_admin_ftp_mapping" ON storage.objects;
DROP POLICY IF EXISTS "storage_update_admin_ftp_mapping" ON storage.objects;
DROP POLICY IF EXISTS "storage_delete_admin_ftp_mapping" ON storage.objects;

-- Step 2: Create new admin-only policies for ftp-import and mapping-files

-- SELECT policy: admins can read ftp-import and mapping-files, other buckets pass through
CREATE POLICY "storage_select_admin_ftp_mapping"
ON storage.objects
FOR SELECT
TO authenticated
USING (
  (bucket_id IN ('ftp-import', 'mapping-files') AND auth.uid() IN (SELECT user_id FROM public.admin_users))
  OR bucket_id NOT IN ('ftp-import', 'mapping-files')
);

-- INSERT policy: admins can upload to ftp-import and mapping-files, other buckets pass through
CREATE POLICY "storage_insert_admin_ftp_mapping"
ON storage.objects
FOR INSERT
TO authenticated
WITH CHECK (
  (bucket_id IN ('ftp-import', 'mapping-files') AND auth.uid() IN (SELECT user_id FROM public.admin_users))
  OR bucket_id NOT IN ('ftp-import', 'mapping-files')
);

-- UPDATE policy: admins can update in ftp-import and mapping-files, other buckets pass through
CREATE POLICY "storage_update_admin_ftp_mapping"
ON storage.objects
FOR UPDATE
TO authenticated
USING (
  (bucket_id IN ('ftp-import', 'mapping-files') AND auth.uid() IN (SELECT user_id FROM public.admin_users))
  OR bucket_id NOT IN ('ftp-import', 'mapping-files')
)
WITH CHECK (
  (bucket_id IN ('ftp-import', 'mapping-files') AND auth.uid() IN (SELECT user_id FROM public.admin_users))
  OR bucket_id NOT IN ('ftp-import', 'mapping-files')
);

-- DELETE policy: admins can delete from ftp-import and mapping-files, other buckets pass through
CREATE POLICY "storage_delete_admin_ftp_mapping"
ON storage.objects
FOR DELETE
TO authenticated
USING (
  (bucket_id IN ('ftp-import', 'mapping-files') AND auth.uid() IN (SELECT user_id FROM public.admin_users))
  OR bucket_id NOT IN ('ftp-import', 'mapping-files')
);