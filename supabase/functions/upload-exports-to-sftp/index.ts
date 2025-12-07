import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";

/**
 * Edge Function: upload-exports-to-sftp
 * 
 * Uploads three export files from the "exports" bucket to an external SFTP server.
 */

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Expected file configuration - supports both XLSX and CSV formats
const EXPECTED_FILES_XLSX = [
  { bucket: 'exports', path: 'Catalogo EAN.xlsx', filename: 'Catalogo EAN.xlsx' },
  { bucket: 'exports', path: 'Export ePrice.xlsx', filename: 'Export ePrice.xlsx' },
  { bucket: 'exports', path: 'Export Mediaworld.xlsx', filename: 'Export Mediaworld.xlsx' }
];

const EXPECTED_FILES_CSV = [
  { bucket: 'exports', path: 'Catalogo EAN.csv', filename: 'Catalogo EAN.csv' },
  { bucket: 'exports', path: 'Export ePrice.csv', filename: 'Export ePrice.csv' },
  { bucket: 'exports', path: 'Export Mediaworld.csv', filename: 'Export Mediaworld.csv' }
];

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
  error?: string;
}

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
    // 1. AUTHENTICATION: Validate JWT from Authorization header
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
    
    // Validate JWT by getting user
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

    const userId = userData.user.id;
    console.log('[upload-exports-to-sftp] User authenticated:', userId);

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

    if (!body.files || !Array.isArray(body.files) || body.files.length !== 3) {
      console.log('[upload-exports-to-sftp] Invalid files array:', body.files);
      return new Response(
        JSON.stringify({ 
          status: 'error', 
          message: 'Il campo "files" deve contenere esattamente 3 file.' 
        }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Validate each file spec - accept both XLSX and CSV formats
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

      // Check if file matches expected XLSX or CSV format
      const expectedXlsx = EXPECTED_FILES_XLSX[i];
      const expectedCsv = EXPECTED_FILES_CSV[i];
      const matchesXlsx = file.bucket === expectedXlsx?.bucket && file.path === expectedXlsx?.path && file.filename === expectedXlsx?.filename;
      const matchesCsv = file.bucket === expectedCsv?.bucket && file.path === expectedCsv?.path && file.filename === expectedCsv?.filename;
      
      if (!matchesXlsx && !matchesCsv) {
        console.log('[upload-exports-to-sftp] File spec mismatch:', { file, expectedXlsx, expectedCsv });
        return new Response(
          JSON.stringify({ 
            status: 'error', 
            message: `File ${i + 1}: configurazione non valida. Attesi formati XLSX o CSV.` 
          }),
          { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
    }

    console.log('[upload-exports-to-sftp] Request body validated');

    // =========================================================================
    // 3. READ FILES FROM BUCKET (using service role for guaranteed access)
    // =========================================================================
    const serviceClient = createClient(supabaseUrl, supabaseServiceKey);
    const fileContents: { filename: string; data: Uint8Array }[] = [];
    
    for (const fileSpec of body.files) {
      console.log(`[upload-exports-to-sftp] Reading file: ${fileSpec.path}`);
      
      const { data: fileData, error: fileError } = await serviceClient.storage
        .from(fileSpec.bucket)
        .download(fileSpec.path);

      if (fileError || !fileData) {
        console.error(`[upload-exports-to-sftp] Error reading file ${fileSpec.path}:`, fileError?.message);
        return new Response(
          JSON.stringify({ 
            status: 'error', 
            message: `Impossibile leggere il file "${fileSpec.filename}" dal bucket "${fileSpec.bucket}". Verifica che il file esista.`,
            results: body.files.map(f => ({
              filename: f.filename,
              uploaded: false,
              error: f.filename === fileSpec.filename ? 'File non trovato nel bucket' : 'Upload non tentato'
            }))
          }),
          { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      const arrayBuffer = await fileData.arrayBuffer();
      fileContents.push({
        filename: fileSpec.filename,
        data: new Uint8Array(arrayBuffer)
      });
      
      console.log(`[upload-exports-to-sftp] File read successfully: ${fileSpec.filename} (${arrayBuffer.byteLength} bytes)`);
    }

    console.log('[upload-exports-to-sftp] All files read from bucket');

    // =========================================================================
    // 4. SFTP CONFIGURATION CHECK
    // =========================================================================
    const sftpHost = Deno.env.get('SFTP_HOST');
    const sftpPort = Deno.env.get('SFTP_PORT');
    const sftpUser = Deno.env.get('SFTP_USER');
    const sftpPassword = Deno.env.get('SFTP_PASSWORD');
    const sftpBaseDir = Deno.env.get('SFTP_BASE_DIR');

    console.log('[upload-exports-to-sftp] SFTP config check:', {
      hasHost: !!sftpHost,
      hasPort: !!sftpPort,
      hasUser: !!sftpUser,
      hasPassword: !!sftpPassword,
      baseDir: sftpBaseDir
    });

    if (!sftpHost || !sftpPort || !sftpUser || !sftpPassword || !sftpBaseDir) {
      console.error('[upload-exports-to-sftp] Missing SFTP configuration');
      return new Response(
        JSON.stringify({ 
          status: 'error', 
          message: 'Configurazione SFTP incompleta. Verifica le variabili ambiente SFTP_HOST, SFTP_PORT, SFTP_USER, SFTP_PASSWORD, SFTP_BASE_DIR.',
          results: body.files.map(f => ({
            filename: f.filename,
            uploaded: false,
            error: 'Configurazione SFTP mancante'
          }))
        }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // =========================================================================
    // 5. SFTP UPLOAD
    // =========================================================================
    let sftpLibraryAvailable = false;
    let Client: any = null;
    
    try {
      // Try to dynamically import ssh2
      const ssh2Module = await import("npm:ssh2@1.15.0");
      Client = ssh2Module.Client;
      sftpLibraryAvailable = true;
      console.log('[upload-exports-to-sftp] SSH2 library loaded successfully');
    } catch (importError: any) {
      console.error('[upload-exports-to-sftp] Failed to load SSH2 library:', importError.message);
      return new Response(
        JSON.stringify({ 
          status: 'error', 
          message: 'Libreria SFTP non disponibile in questo ambiente. Il runtime Deno Edge Functions potrebbe non supportare SSH2.',
          results: body.files.map(f => ({
            filename: f.filename,
            uploaded: false,
            error: 'SFTP library not available'
          }))
        }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Proceed with SFTP upload
    const results: UploadResult[] = [];
    let conn: any = null;
    let sftp: any = null;
    
    try {
      console.log('[upload-exports-to-sftp] Attempting SSH connection...');
      
      conn = new Client();
      
      // Create SSH connection with timeout
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
      
      // Get SFTP session
      sftp = await new Promise<any>((resolve, reject) => {
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
        sftp.stat(sftpBaseDir, (err: Error | null, stats: any) => {
          if (err) {
            console.error('[upload-exports-to-sftp] SFTP directory check failed:', err.message);
            reject(new Error(`La cartella SFTP "${sftpBaseDir}" non esiste o non è accessibile.`));
          } else if (!stats.isDirectory()) {
            reject(new Error(`Il percorso SFTP "${sftpBaseDir}" non è una directory.`));
          } else {
            console.log('[upload-exports-to-sftp] SFTP directory verified:', sftpBaseDir);
            resolve();
          }
        });
      });
      
      // Upload each file with retry (max 3 attempts per file)
      for (const file of fileContents) {
        const remotePath = `${sftpBaseDir}/${file.filename}`;
        let uploaded = false;
        let lastError = '';
        
        for (let attempt = 1; attempt <= 3 && !uploaded; attempt++) {
          console.log(`[upload-exports-to-sftp] Uploading ${file.filename}, attempt ${attempt}`);
          
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
              
              // Write the data directly as Uint8Array (Deno compatible)
              writeStream.end(file.data);
            });
            
            uploaded = true;
          } catch (err: any) {
            lastError = err.message || 'Unknown error';
            console.log(`[upload-exports-to-sftp] Attempt ${attempt} failed for ${file.filename}: ${lastError}`);
            
            if (attempt < 3) {
              // Wait before retry (1s, 2s)
              await new Promise(r => setTimeout(r, 1000 * attempt));
            }
          }
        }
        
        results.push({
          filename: file.filename,
          uploaded,
          ...(uploaded ? {} : { error: lastError || 'Upload fallito dopo 3 tentativi' })
        });
      }
      
    } catch (sftpConnectionError: any) {
      console.error('[upload-exports-to-sftp] SFTP connection/setup error:', sftpConnectionError.message);
      
      // Close connection if open
      if (conn) {
        try { conn.end(); } catch (e) { /* ignore */ }
      }
      
      return new Response(
        JSON.stringify({ 
          status: 'error', 
          message: `Errore connessione SFTP: ${sftpConnectionError.message}`,
          results: body.files.map(f => ({
            filename: f.filename,
            uploaded: false,
            error: sftpConnectionError.message
          }))
        }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    } finally {
      // Always close connection
      if (conn) {
        try {
          conn.end();
          console.log('[upload-exports-to-sftp] SSH connection closed');
        } catch (e) { /* ignore */ }
      }
    }

    // =========================================================================
    // 6. DETERMINE FINAL RESPONSE
    // =========================================================================
    const allSucceeded = results.every(r => r.uploaded === true);
    
    if (allSucceeded) {
      console.log('[upload-exports-to-sftp] SUCCESS: All files uploaded successfully');
      // Return HTTP 200 with status: "ok"
      return new Response(
        JSON.stringify({ 
          status: 'ok', 
          results 
        }),
        { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    } else {
      console.error('[upload-exports-to-sftp] FAILURE: Some files failed to upload');
      // Return HTTP 500 with status: "error"
      return new Response(
        JSON.stringify({ 
          status: 'error', 
          message: 'Alcuni file non sono stati caricati correttamente sul server SFTP.',
          results 
        }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

  } catch (error: any) {
    console.error('[upload-exports-to-sftp] Unexpected error:', error.message);
    console.error('[upload-exports-to-sftp] Stack:', error.stack);
    
    return new Response(
      JSON.stringify({ 
        status: 'error', 
        message: `Errore interno del server: ${error.message}` 
      }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
