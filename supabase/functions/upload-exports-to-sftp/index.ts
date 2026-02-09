import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";

/**
 * Edge Function: upload-exports-to-sftp
 * 
 * Uploads export files from the "exports" bucket to an external SFTP server.
 * This function ONLY transfers files from the bucket - it does NOT regenerate them.
 * The files must already be generated and saved by the client.
 * 
 * Features:
 * - Retry policy: 5 attempts per file with progressive backoff (1s, 2s, 4s, 8s, 16s)
 * - Per-file error handling: continues with other files if one fails
 * - Detailed error reporting with phase information
 */

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

interface FileSpec {
  bucket: string;
  path: string;
  filename: string;
}

interface RequestBody {
  files: FileSpec[];
}

interface UploadResult {
  filename: string;
  uploaded: boolean;
  attemptsUsed: number;
  errorDetails?: {
    message: string;
    phase: 'download_bucket' | 'upload_sftp';
    remotePath?: string;
    stack?: string;
  };
}

// Progressive backoff delays in milliseconds
const BACKOFF_DELAYS = [1000, 2000, 4000, 8000, 16000];
const MAX_SFTP_RETRIES = 5;
const MAX_BUCKET_RETRIES = 2;

serve(async (req) => {
  // Handle CORS preflight
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  console.log('[upload-exports-to-sftp] Request received');

  // Only accept POST requests
  if (req.method !== 'POST') {
    console.log('[upload-exports-to-sftp] Invalid method:', req.method);
    return new Response(
      JSON.stringify({ status: 'error', message: 'Method not allowed. Use POST.' }),
      { status: 405, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }

  try {
    // =========================================================================
    // 1. AUTHENTICATION
    // =========================================================================
    const authHeader = req.headers.get('Authorization');
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      console.log('[upload-exports-to-sftp] Missing or invalid Authorization header');
      return new Response(
        JSON.stringify({ status: 'error', message: 'Autenticazione richiesta. Token JWT mancante o non valido.' }),
        { status: 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    const jwt = authHeader.replace('Bearer ', '');
    
    const supabaseUrl = Deno.env.get('SUPABASE_URL')!;
    const supabaseAnonKey = Deno.env.get('SUPABASE_ANON_KEY')!;
    const supabaseServiceKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
    
    // Check if this is a service role key
    let isServiceRole = false;
    if (jwt === supabaseServiceKey) {
      console.log('[upload-exports-to-sftp] Service role authentication');
      isServiceRole = true;
    } else {
      const userClient = createClient(supabaseUrl, supabaseAnonKey, {
        global: { headers: { Authorization: `Bearer ${jwt}` } }
      });
      
      const { data: userData, error: userError } = await userClient.auth.getUser();
      
      if (userError || !userData?.user) {
        console.log('[upload-exports-to-sftp] JWT validation failed:', userError?.message);
        return new Response(
          JSON.stringify({ status: 'error', message: 'Token JWT non valido o scaduto.' }),
          { status: 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      console.log('[upload-exports-to-sftp] User authenticated:', userData.user.id);
    }

    // =========================================================================
    // 2. VALIDATE REQUEST BODY
    // =========================================================================
    let body: RequestBody;
    try {
      body = await req.json();
    } catch (e) {
      console.log('[upload-exports-to-sftp] Invalid JSON body');
      return new Response(
        JSON.stringify({ status: 'error', message: 'Body JSON non valido.' }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    if (!body.files || !Array.isArray(body.files) || body.files.length === 0) {
      console.log('[upload-exports-to-sftp] Invalid files array:', body.files);
      return new Response(
        JSON.stringify({ 
          status: 'error', 
          message: 'Il campo "files" deve contenere almeno 1 file.' 
        }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Validate each file spec
    for (let i = 0; i < body.files.length; i++) {
      const file = body.files[i];
      if (!file.bucket || !file.path || !file.filename) {
        console.log('[upload-exports-to-sftp] Invalid file spec:', file);
        return new Response(
          JSON.stringify({ 
            status: 'error', 
            message: `File ${i + 1}: campi bucket, path e filename sono obbligatori.` 
          }),
          { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
    }

    console.log('[upload-exports-to-sftp] Request body validated, files:', body.files.length);

    // =========================================================================
    // 3. READ FILES FROM BUCKET (with retry)
    // =========================================================================
    const serviceClient = createClient(supabaseUrl, supabaseServiceKey);
    const results: UploadResult[] = [];
    const fileContents: { filename: string; data: Uint8Array }[] = [];
    
    for (const fileSpec of body.files) {
      console.log(`[upload-exports-to-sftp] Reading file: ${fileSpec.path}`);
      
      let fileData: Blob | null = null;
      let lastError: string = '';
      
      // Retry bucket download up to MAX_BUCKET_RETRIES times
      for (let attempt = 1; attempt <= MAX_BUCKET_RETRIES; attempt++) {
        const { data, error } = await serviceClient.storage
          .from(fileSpec.bucket)
          .download(fileSpec.path);

        if (!error && data) {
          fileData = data;
          break;
        }
        
        lastError = error?.message || 'Unknown bucket error';
        console.warn(`[upload-exports-to-sftp] Bucket download attempt ${attempt}/${MAX_BUCKET_RETRIES} failed for ${fileSpec.path}: ${lastError}`);
        
        if (attempt < MAX_BUCKET_RETRIES) {
          await new Promise(r => setTimeout(r, 1000 * attempt));
        }
      }
      
      if (!fileData) {
        console.error(`[upload-exports-to-sftp] Failed to read file ${fileSpec.path} after ${MAX_BUCKET_RETRIES} attempts`);
        results.push({
          filename: fileSpec.filename,
          uploaded: false,
          attemptsUsed: 0,
          errorDetails: {
            message: lastError || 'File non trovato nel bucket',
            phase: 'download_bucket'
          }
        });
        continue; // Continue with other files
      }

      const arrayBuffer = await fileData.arrayBuffer();
      fileContents.push({
        filename: fileSpec.filename,
        data: new Uint8Array(arrayBuffer)
      });
      
      console.log(`[upload-exports-to-sftp] File read successfully: ${fileSpec.filename} (${arrayBuffer.byteLength} bytes)`);
    }

    // Check if any files were read successfully
    if (fileContents.length === 0) {
      console.error('[upload-exports-to-sftp] No files could be read from bucket');
      return new Response(
        JSON.stringify({ 
          status: 'error', 
          message: 'Nessun file è stato letto correttamente dal bucket.',
          results 
        }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    console.log(`[upload-exports-to-sftp] ${fileContents.length} files read from bucket`);

    // =========================================================================
    // 4. SFTP CONFIGURATION CHECK
    // =========================================================================
    const sftpHost = Deno.env.get('SFTP_HOST');
    const sftpPort = Deno.env.get('SFTP_PORT');
    const sftpUser = Deno.env.get('SFTP_USER');
    const sftpPassword = Deno.env.get('SFTP_PASSWORD');
    const sftpBaseDir = Deno.env.get('SFTP_BASE_DIR');

    console.log('[upload-exports-to-sftp] SFTP Config:', {
      host: sftpHost,
      port: sftpPort,
      user: sftpUser,
      hasPassword: !!sftpPassword,
      baseDir: sftpBaseDir
    });

    if (!sftpHost || !sftpPort || !sftpUser || !sftpPassword || !sftpBaseDir) {
      console.error('[upload-exports-to-sftp] Missing SFTP configuration');
      return new Response(
        JSON.stringify({ 
          status: 'error', 
          message: 'Configurazione SFTP incompleta.',
          results: body.files.map(f => ({
            filename: f.filename,
            uploaded: false,
            attemptsUsed: 0,
            errorDetails: { message: 'Configurazione SFTP mancante', phase: 'upload_sftp' as const }
          }))
        }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // =========================================================================
    // 5. SFTP UPLOAD with retry
    // =========================================================================
    let Client: { new(): unknown } | null = null;
    
    try {
      const ssh2Module = await import("npm:ssh2@1.15.0");
      Client = ssh2Module.Client;
      console.log('[upload-exports-to-sftp] SSH2 library loaded');
    } catch (importError: unknown) {
      console.error('[upload-exports-to-sftp] Failed to load SSH2:', importError instanceof Error ? importError.message : String(importError));
      return new Response(
        JSON.stringify({ 
          status: 'error', 
          message: 'Libreria SFTP non disponibile.',
          results: body.files.map(f => ({
            filename: f.filename,
            uploaded: false,
            attemptsUsed: 0,
            errorDetails: { message: 'SFTP library not available', phase: 'upload_sftp' as const }
          }))
        }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any -- ssh2 Client/SFTP APIs are untyped in Deno; narrowing not feasible without @types/ssh2
    let conn: any = null;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any -- ssh2 SFTP session is untyped in Deno
    let sftp: any = null;
    
    try {
      console.log('[upload-exports-to-sftp] Connecting to SFTP...');
      
      conn = new Client!();
      
      await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          conn.end();
          reject(new Error('Timeout connessione SFTP (30 secondi)'));
        }, 30000);
        
        conn.on('ready', () => {
          clearTimeout(timeout);
          console.log('[upload-exports-to-sftp] SSH connection established');
          resolve();
        });
        
        conn.on('error', (err: Error) => {
          clearTimeout(timeout);
          console.error('[upload-exports-to-sftp] SSH connection error:', err.message);
          reject(err);
        });
        
        conn.connect({
          host: sftpHost,
          port: parseInt(sftpPort, 10),
          username: sftpUser,
          password: sftpPassword,
          readyTimeout: 30000,
          algorithms: {
            kex: [
              'ecdh-sha2-nistp256',
              'ecdh-sha2-nistp384',
              'ecdh-sha2-nistp521',
              'diffie-hellman-group-exchange-sha256',
              'diffie-hellman-group14-sha256',
              'diffie-hellman-group14-sha1'
            ]
          }
        });
      });
      
      // eslint-disable-next-line @typescript-eslint/no-explicit-any -- ssh2 sftp callback signature is untyped in Deno
      sftp = await new Promise<any>((resolve, reject) => {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any -- ssh2 sftp callback param is untyped
        conn.sftp((err: Error | null, sftpSession: any) => {
          if (err) {
            console.error('[upload-exports-to-sftp] SFTP session error:', err.message);
            reject(err);
          } else {
            console.log('[upload-exports-to-sftp] SFTP session established');
            resolve(sftpSession);
          }
        });
      });
      
      // Verify directory exists
      await new Promise<void>((resolve, reject) => {
        sftp.stat(sftpBaseDir, (err: Error | null, stats: { isDirectory(): boolean }) => {
          if (err) {
            console.error('[upload-exports-to-sftp] SFTP directory check failed:', err.message);
            reject(new Error(`Cartella SFTP "${sftpBaseDir}" non accessibile.`));
          } else if (!stats.isDirectory()) {
            reject(new Error(`Percorso SFTP "${sftpBaseDir}" non è una directory.`));
          } else {
            console.log('[upload-exports-to-sftp] SFTP directory verified:', sftpBaseDir);
            resolve();
          }
        });
      });
      
      // Upload each file with retry
      for (const file of fileContents) {
        const remotePath = `${sftpBaseDir}/${file.filename}`;
        let uploaded = false;
        let lastError = '';
        let attemptsUsed = 0;
        
        for (let attempt = 1; attempt <= MAX_SFTP_RETRIES && !uploaded; attempt++) {
          attemptsUsed = attempt;
          console.log(`[upload-exports-to-sftp] Uploading ${file.filename}, attempt ${attempt}/${MAX_SFTP_RETRIES}`);
          
          try {
            await new Promise<void>((resolve, reject) => {
              const writeStream = sftp.createWriteStream(remotePath);
              
              writeStream.on('close', () => {
                console.log(`[upload-exports-to-sftp] File uploaded: ${file.filename}`);
                resolve();
              });
              
              writeStream.on('error', (err: Error) => {
                console.error(`[upload-exports-to-sftp] Write error for ${file.filename}:`, err.message);
                reject(err);
              });
              
              writeStream.end(file.data);
            });
            
            uploaded = true;
          } catch (err: unknown) {
            lastError = err instanceof Error ? err.message : String(err);
            console.warn(`[upload-exports-to-sftp] Attempt ${attempt} failed for ${file.filename}: ${lastError}`);
            
            if (attempt < MAX_SFTP_RETRIES) {
              const delay = BACKOFF_DELAYS[attempt - 1] || BACKOFF_DELAYS[BACKOFF_DELAYS.length - 1];
              console.log(`[upload-exports-to-sftp] Waiting ${delay}ms before retry...`);
              await new Promise(r => setTimeout(r, delay));
            }
          }
        }
        
        results.push({
          filename: file.filename,
          uploaded,
          attemptsUsed,
          ...(uploaded ? {} : {
            errorDetails: {
              message: lastError || `Upload fallito dopo ${MAX_SFTP_RETRIES} tentativi`,
              phase: 'upload_sftp' as const,
              remotePath
            }
          })
        });
      }
      
    } catch (sftpConnectionError: unknown) {
      console.error('[upload-exports-to-sftp] SFTP connection error:', sftpConnectionError instanceof Error ? sftpConnectionError.message : String(sftpConnectionError));
      
      // Mark all pending files as failed
      for (const file of fileContents) {
        const existingResult = results.find(r => r.filename === file.filename);
        if (!existingResult) {
          results.push({
            filename: file.filename,
            uploaded: false,
            attemptsUsed: 0,
            errorDetails: {
              message: sftpConnectionError instanceof Error ? sftpConnectionError.message : String(sftpConnectionError),
              phase: 'upload_sftp',
              stack: sftpConnectionError instanceof Error ? sftpConnectionError.stack : undefined
            }
          });
        }
      }
    } finally {
      if (conn) {
        try {
          conn.end();
          console.log('[upload-exports-to-sftp] SSH connection closed');
        } catch (e) { /* ignore */ }
      }
    }

    // =========================================================================
    // 6. FINAL RESPONSE
    // =========================================================================
    const allSucceeded = results.length === body.files.length && results.every(r => r.uploaded === true);
    const someSucceeded = results.some(r => r.uploaded === true);
    
    if (allSucceeded) {
      console.log('[upload-exports-to-sftp] SUCCESS: All files uploaded');
      return new Response(
        JSON.stringify({ status: 'ok', results }),
        { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    } else {
      const failedCount = results.filter(r => !r.uploaded).length;
      const successCount = results.filter(r => r.uploaded).length;
      console.error(`[upload-exports-to-sftp] PARTIAL: ${successCount} succeeded, ${failedCount} failed`);
      return new Response(
        JSON.stringify({ 
          status: 'error', 
          message: `${successCount}/${body.files.length} file caricati. ${failedCount} falliti.`,
          results 
        }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

  } catch (error: unknown) {
    const msg = error instanceof Error ? error.message : String(error);
    const stack = error instanceof Error ? error.stack : undefined;
    console.error('[upload-exports-to-sftp] Unexpected error:', msg);
    console.error('[upload-exports-to-sftp] Stack:', stack);
    
    return new Response(
      JSON.stringify({ 
        status: 'error', 
        message: `Errore interno: ${msg}` 
      }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
