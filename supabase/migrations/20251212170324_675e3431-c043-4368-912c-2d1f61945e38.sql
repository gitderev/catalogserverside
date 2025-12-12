-- Create the "pipeline" storage bucket for JSON indices (private, accepts JSON)
INSERT INTO storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
VALUES (
  'pipeline', 
  'pipeline', 
  false,  -- Private bucket
  104857600,  -- 100MB max file size
  ARRAY['application/json', 'application/octet-stream', 'text/plain']
)
ON CONFLICT (id) DO NOTHING;

-- RLS policy: Only authenticated admin users can SELECT from pipeline bucket
CREATE POLICY "pipeline_admin_select" ON storage.objects
FOR SELECT
TO authenticated
USING (
  bucket_id = 'pipeline' 
  AND auth.uid() IN (SELECT user_id FROM public.admin_users)
);

-- RLS policy: Only authenticated admin users can INSERT into pipeline bucket
CREATE POLICY "pipeline_admin_insert" ON storage.objects
FOR INSERT
TO authenticated
WITH CHECK (
  bucket_id = 'pipeline' 
  AND auth.uid() IN (SELECT user_id FROM public.admin_users)
);

-- RLS policy: Only authenticated admin users can UPDATE in pipeline bucket
CREATE POLICY "pipeline_admin_update" ON storage.objects
FOR UPDATE
TO authenticated
USING (
  bucket_id = 'pipeline' 
  AND auth.uid() IN (SELECT user_id FROM public.admin_users)
)
WITH CHECK (
  bucket_id = 'pipeline' 
  AND auth.uid() IN (SELECT user_id FROM public.admin_users)
);

-- RLS policy: Only authenticated admin users can DELETE from pipeline bucket
CREATE POLICY "pipeline_admin_delete" ON storage.objects
FOR DELETE
TO authenticated
USING (
  bucket_id = 'pipeline' 
  AND auth.uid() IN (SELECT user_id FROM public.admin_users)
);