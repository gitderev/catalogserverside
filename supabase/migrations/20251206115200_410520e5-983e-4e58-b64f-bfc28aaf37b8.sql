-- Make ftp-import bucket private
UPDATE storage.buckets SET public = false WHERE id = 'ftp-import';

-- Make mapping-files bucket private
UPDATE storage.buckets SET public = false WHERE id = 'mapping-files';

-- Drop any existing permissive policies on storage.objects for these buckets
DROP POLICY IF EXISTS "Public access ftp-import" ON storage.objects;
DROP POLICY IF EXISTS "Public access mapping-files" ON storage.objects;
DROP POLICY IF EXISTS "ftp_import_public_select" ON storage.objects;
DROP POLICY IF EXISTS "mapping_files_public_select" ON storage.objects;
DROP POLICY IF EXISTS "Allow public read ftp-import" ON storage.objects;
DROP POLICY IF EXISTS "Allow public read mapping-files" ON storage.objects;

-- Create admin-only SELECT policy for ftp-import bucket
CREATE POLICY "ftp_import_admin_select"
ON storage.objects
FOR SELECT
TO authenticated
USING (
  bucket_id = 'ftp-import' 
  AND auth.uid() IN (SELECT user_id FROM public.admin_users)
);

-- Create admin-only INSERT policy for ftp-import bucket
CREATE POLICY "ftp_import_admin_insert"
ON storage.objects
FOR INSERT
TO authenticated
WITH CHECK (
  bucket_id = 'ftp-import' 
  AND auth.uid() IN (SELECT user_id FROM public.admin_users)
);

-- Create admin-only UPDATE policy for ftp-import bucket
CREATE POLICY "ftp_import_admin_update"
ON storage.objects
FOR UPDATE
TO authenticated
USING (
  bucket_id = 'ftp-import' 
  AND auth.uid() IN (SELECT user_id FROM public.admin_users)
)
WITH CHECK (
  bucket_id = 'ftp-import' 
  AND auth.uid() IN (SELECT user_id FROM public.admin_users)
);

-- Create admin-only DELETE policy for ftp-import bucket
CREATE POLICY "ftp_import_admin_delete"
ON storage.objects
FOR DELETE
TO authenticated
USING (
  bucket_id = 'ftp-import' 
  AND auth.uid() IN (SELECT user_id FROM public.admin_users)
);

-- Create admin-only SELECT policy for mapping-files bucket
CREATE POLICY "mapping_files_admin_select"
ON storage.objects
FOR SELECT
TO authenticated
USING (
  bucket_id = 'mapping-files' 
  AND auth.uid() IN (SELECT user_id FROM public.admin_users)
);

-- Create admin-only INSERT policy for mapping-files bucket
CREATE POLICY "mapping_files_admin_insert"
ON storage.objects
FOR INSERT
TO authenticated
WITH CHECK (
  bucket_id = 'mapping-files' 
  AND auth.uid() IN (SELECT user_id FROM public.admin_users)
);

-- Create admin-only UPDATE policy for mapping-files bucket
CREATE POLICY "mapping_files_admin_update"
ON storage.objects
FOR UPDATE
TO authenticated
USING (
  bucket_id = 'mapping-files' 
  AND auth.uid() IN (SELECT user_id FROM public.admin_users)
)
WITH CHECK (
  bucket_id = 'mapping-files' 
  AND auth.uid() IN (SELECT user_id FROM public.admin_users)
);

-- Create admin-only DELETE policy for mapping-files bucket
CREATE POLICY "mapping_files_admin_delete"
ON storage.objects
FOR DELETE
TO authenticated
USING (
  bucket_id = 'mapping-files' 
  AND auth.uid() IN (SELECT user_id FROM public.admin_users)
);