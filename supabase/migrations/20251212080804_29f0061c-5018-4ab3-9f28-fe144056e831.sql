-- ============================================================
-- STOCK LOCATION: Storage Policies for ftp-import bucket
-- ============================================================
-- These policies allow authenticated admin users to upload/overwrite
-- stock location files while keeping the bucket private.
-- Edge functions use service role which bypasses RLS.

-- Policy: Admins can upload/update stock-location/latest.txt
CREATE POLICY "admin_upload_stock_location_latest"
ON storage.objects
FOR INSERT
TO authenticated
WITH CHECK (
  bucket_id = 'ftp-import' AND
  name = 'stock-location/latest.txt' AND
  auth.uid() IN (SELECT user_id FROM public.admin_users)
);

-- Policy: Admins can update (overwrite) stock-location/latest.txt
CREATE POLICY "admin_update_stock_location_latest"
ON storage.objects
FOR UPDATE
TO authenticated
USING (
  bucket_id = 'ftp-import' AND
  name = 'stock-location/latest.txt' AND
  auth.uid() IN (SELECT user_id FROM public.admin_users)
)
WITH CHECK (
  bucket_id = 'ftp-import' AND
  name = 'stock-location/latest.txt' AND
  auth.uid() IN (SELECT user_id FROM public.admin_users)
);

-- Policy: Admins can upload to stock-location/manual/* path
CREATE POLICY "admin_upload_stock_location_manual"
ON storage.objects
FOR INSERT
TO authenticated
WITH CHECK (
  bucket_id = 'ftp-import' AND
  name LIKE 'stock-location/manual/%' AND
  auth.uid() IN (SELECT user_id FROM public.admin_users)
);

-- Policy: Admins can update files in stock-location/manual/* path
CREATE POLICY "admin_update_stock_location_manual"
ON storage.objects
FOR UPDATE
TO authenticated
USING (
  bucket_id = 'ftp-import' AND
  name LIKE 'stock-location/manual/%' AND
  auth.uid() IN (SELECT user_id FROM public.admin_users)
)
WITH CHECK (
  bucket_id = 'ftp-import' AND
  name LIKE 'stock-location/manual/%' AND
  auth.uid() IN (SELECT user_id FROM public.admin_users)
);

-- Policy: Admins can read stock-location files
CREATE POLICY "admin_read_stock_location"
ON storage.objects
FOR SELECT
TO authenticated
USING (
  bucket_id = 'ftp-import' AND
  (name = 'stock-location/latest.txt' OR name LIKE 'stock-location/manual/%') AND
  auth.uid() IN (SELECT user_id FROM public.admin_users)
);