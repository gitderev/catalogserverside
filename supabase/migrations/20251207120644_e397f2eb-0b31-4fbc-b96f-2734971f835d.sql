-- Create the "exports" storage bucket (private, no public access)
INSERT INTO storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
VALUES (
  'exports', 
  'exports', 
  false,  -- Private bucket
  52428800,  -- 50MB max file size
  ARRAY['application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'application/vnd.ms-excel']
)
ON CONFLICT (id) DO NOTHING;

-- RLS policy: Only authenticated admin users can SELECT from exports bucket
CREATE POLICY "exports_admin_select" ON storage.objects
FOR SELECT
TO authenticated
USING (
  bucket_id = 'exports' 
  AND auth.uid() IN (SELECT user_id FROM public.admin_users)
);

-- RLS policy: Only authenticated admin users can INSERT into exports bucket
CREATE POLICY "exports_admin_insert" ON storage.objects
FOR INSERT
TO authenticated
WITH CHECK (
  bucket_id = 'exports' 
  AND auth.uid() IN (SELECT user_id FROM public.admin_users)
);

-- RLS policy: Only authenticated admin users can UPDATE in exports bucket
CREATE POLICY "exports_admin_update" ON storage.objects
FOR UPDATE
TO authenticated
USING (
  bucket_id = 'exports' 
  AND auth.uid() IN (SELECT user_id FROM public.admin_users)
)
WITH CHECK (
  bucket_id = 'exports' 
  AND auth.uid() IN (SELECT user_id FROM public.admin_users)
);

-- RLS policy: Only authenticated admin users can DELETE from exports bucket
CREATE POLICY "exports_admin_delete" ON storage.objects
FOR DELETE
TO authenticated
USING (
  bucket_id = 'exports' 
  AND auth.uid() IN (SELECT user_id FROM public.admin_users)
);