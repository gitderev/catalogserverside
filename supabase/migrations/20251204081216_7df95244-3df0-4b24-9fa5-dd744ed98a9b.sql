-- Create storage bucket for mapping files
INSERT INTO storage.buckets (id, name, public)
VALUES ('mapping-files', 'mapping-files', true)
ON CONFLICT (id) DO NOTHING;

-- Allow public download
CREATE POLICY "Public can download mapping files"
ON storage.objects FOR SELECT
USING (bucket_id = 'mapping-files');

-- Allow anonymous upload with upsert
CREATE POLICY "Anyone can upload mapping files"
ON storage.objects FOR INSERT
WITH CHECK (bucket_id = 'mapping-files');

-- Allow anonymous update for upsert
CREATE POLICY "Anyone can update mapping files"
ON storage.objects FOR UPDATE
USING (bucket_id = 'mapping-files');