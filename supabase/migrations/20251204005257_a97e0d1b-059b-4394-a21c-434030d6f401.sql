-- Create storage bucket for FTP imports
INSERT INTO storage.buckets (id, name, public)
VALUES ('ftp-import', 'ftp-import', true)
ON CONFLICT (id) DO NOTHING;

-- Allow public read access to ftp-import bucket
CREATE POLICY "Public read access for ftp-import"
ON storage.objects FOR SELECT
USING (bucket_id = 'ftp-import');

-- Allow service role to upload files
CREATE POLICY "Service role can upload to ftp-import"
ON storage.objects FOR INSERT
WITH CHECK (bucket_id = 'ftp-import');

-- Allow service role to update files (for upsert)
CREATE POLICY "Service role can update ftp-import"
ON storage.objects FOR UPDATE
USING (bucket_id = 'ftp-import');