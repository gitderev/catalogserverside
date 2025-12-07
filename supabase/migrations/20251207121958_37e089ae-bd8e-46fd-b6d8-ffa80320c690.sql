-- Drop existing RLS policies on storage.objects for exports bucket
DROP POLICY IF EXISTS "Authenticated admins can upload exports" ON storage.objects;
DROP POLICY IF EXISTS "Authenticated admins can read exports" ON storage.objects;
DROP POLICY IF EXISTS "Authenticated admins can update exports" ON storage.objects;
DROP POLICY IF EXISTS "Authenticated admins can delete exports" ON storage.objects;

-- Create new RLS policies that allow ANY authenticated user to access exports bucket
CREATE POLICY "Authenticated users can upload exports"
ON storage.objects
FOR INSERT
TO authenticated
WITH CHECK (bucket_id = 'exports');

CREATE POLICY "Authenticated users can read exports"
ON storage.objects
FOR SELECT
TO authenticated
USING (bucket_id = 'exports');

CREATE POLICY "Authenticated users can update exports"
ON storage.objects
FOR UPDATE
TO authenticated
USING (bucket_id = 'exports')
WITH CHECK (bucket_id = 'exports');

CREATE POLICY "Authenticated users can delete exports"
ON storage.objects
FOR DELETE
TO authenticated
USING (bucket_id = 'exports');

-- Service role will still have access via SUPABASE_SERVICE_ROLE_KEY for edge function