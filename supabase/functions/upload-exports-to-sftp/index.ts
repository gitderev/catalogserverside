import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";

/**
 * Edge Function: upload-exports-to-sftp
 * 
 * Uploads three export files from the "exports" bucket to an external SFTP server.
 * 
 * SFTP LIMITATION:
 * The Deno runtime used by Supabase Edge Functions does NOT have native SFTP support.
 * The ssh2 library is NOT compatible with Deno. This function will:
 * 1. Validate authentication (any authenticated user)
 * 2. Validate the request body
 * 3. Read files from the "exports" bucket
 * 4. Return a deterministic error indicating SFTP is not supported in this runtime
 */

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Expected file configuration
const EXPECTED_FILES = [
  { bucket: 'exports', path: 'Catalogo EAN.xlsx', filename: 'Catalogo EAN.xlsx' },
  { bucket: 'exports', path: 'Export ePrice.xlsx', filename: 'Export ePrice.xlsx' },
  { bucket: 'exports', path: 'Export Mediaworld.xlsx', filename: 'Export Mediaworld.xlsx' }
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
    // Any authenticated user can use this function (no admin check)
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
    
    // Create Supabase client with user's JWT to validate authentication
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

    // Validate each file spec
    for (let i = 0; i < body.files.length; i++) {
      const file = body.files[i];
      const expected = EXPECTED_FILES[i];
      
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

      if (file.bucket !== expected.bucket || file.path !== expected.path || file.filename !== expected.filename) {
        console.log('[upload-exports-to-sftp] File spec mismatch:', { file, expected });
        return new Response(
          JSON.stringify({ 
            status: 'error', 
            message: `File ${i + 1}: configurazione non valida. Atteso: ${JSON.stringify(expected)}` 
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
    // 4. SFTP UPLOAD - DETERMINISTIC RUNTIME LIMITATION
    // =========================================================================
    // The Deno runtime used by Supabase Edge Functions does NOT support SFTP.
    // The ssh2 library requires Node.js and is NOT compatible with Deno.
    // This is a known limitation and we return a clear, deterministic error.
    
    console.log('[upload-exports-to-sftp] SFTP not supported in Deno Edge Functions runtime');
    
    return new Response(
      JSON.stringify({ 
        status: 'error', 
        message: 'Upload SFTP non eseguibile: il runtime Supabase Edge (Deno) non supporta SFTP. I file sono stati salvati correttamente nel bucket "exports".',
        results: body.files.map(f => ({
          filename: f.filename,
          uploaded: false,
          error: 'Runtime Deno non supporta SFTP'
        })),
        files_in_bucket: true,
        technical_note: 'Per abilitare SFTP, è necessario un servizio esterno (gateway SFTP-to-API) o una funzione Node.js separata.'
      }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );

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
