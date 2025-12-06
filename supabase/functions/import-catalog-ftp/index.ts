import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

interface FileResult {
  filename: string;
  path: string;
  size: number;
}

interface ErrorResponse {
  status: "error";
  code: string;
  message: string;
}

// File configurations
const FILE_CONFIG = {
  material: { ftpName: "MaterialFile.txt", folder: "material", prefix: "MaterialFile", key: "materialFile" },
  stock: { ftpName: "StockFileData_790813.txt", folder: "stock", prefix: "StockFileData", key: "stockFile" },
  price: { ftpName: "pricefileData_790813.txt", folder: "price", prefix: "pricefileData", key: "priceFile" },
} as const;

type FileType = keyof typeof FILE_CONFIG;

const BUCKET_NAME = "ftp-import";

class FTPClient {
  private conn: Deno.Conn | null = null;
  private reader: ReadableStreamDefaultReader<Uint8Array> | null = null;
  private buffer = "";
  private useTLS: boolean;

  constructor(useTLS: boolean = false) {
    this.useTLS = useTLS;
  }

  private async readResponse(): Promise<string> {
    if (!this.reader) throw new Error("Not connected");
    
    while (true) {
      const lines = this.buffer.split("\r\n");
      for (let i = 0; i < lines.length - 1; i++) {
        const line = lines[i];
        if (line.match(/^\d{3} /)) {
          const response = lines.slice(0, i + 1).join("\r\n");
          this.buffer = lines.slice(i + 1).join("\r\n");
          return response;
        }
      }
      
      const { value, done } = await this.reader.read();
      if (done) throw new Error("Connection closed unexpectedly");
      this.buffer += new TextDecoder().decode(value);
    }
  }

  private async sendCommand(cmd: string): Promise<string> {
    if (!this.conn) throw new Error("Not connected");
    const encoder = new TextEncoder();
    await this.conn.write(encoder.encode(cmd + "\r\n"));
    return await this.readResponse();
  }

  private parseResponse(response: string): { code: number; message: string } {
    const match = response.match(/^(\d{3})/);
    if (!match) throw new Error("Invalid FTP response");
    return { code: parseInt(match[1]), message: response };
  }

  async connect(host: string, port: number): Promise<void> {
    console.log(`Connecting to ${host}:${port}...`);
    
    if (this.useTLS) {
      this.conn = await Deno.connectTls({ hostname: host, port });
    } else {
      this.conn = await Deno.connect({ hostname: host, port });
    }
    
    this.reader = this.conn.readable.getReader();
    const welcome = await this.readResponse();
    console.log("Connected to FTP");
    
    const { code } = this.parseResponse(welcome);
    if (code !== 220) {
      throw new Error(`Unexpected welcome code: ${code}`);
    }
  }

  async login(user: string, pass: string): Promise<void> {
    console.log("Logging in...");
    
    const userResp = await this.sendCommand(`USER ${user}`);
    const { code: userCode } = this.parseResponse(userResp);
    
    if (userCode === 331) {
      const passResp = await this.sendCommand(`PASS ${pass}`);
      const { code: passCode } = this.parseResponse(passResp);
      if (passCode !== 230) {
        throw new Error(`Login failed`);
      }
    } else if (userCode !== 230) {
      throw new Error(`Login failed`);
    }
    
    console.log("Logged in");
  }

  async cwd(dir: string): Promise<void> {
    const resp = await this.sendCommand(`CWD ${dir}`);
    const { code } = this.parseResponse(resp);
    if (code !== 250) {
      throw new Error(`CWD_FAILED`);
    }
  }

  async setPassiveMode(): Promise<{ host: string; port: number }> {
    const resp = await this.sendCommand("PASV");
    const { code } = this.parseResponse(resp);
    if (code !== 227) {
      throw new Error(`PASV failed`);
    }
    
    const match = resp.match(/\((\d+),(\d+),(\d+),(\d+),(\d+),(\d+)\)/);
    if (!match) {
      throw new Error("Could not parse PASV");
    }
    
    const host = `${match[1]}.${match[2]}.${match[3]}.${match[4]}`;
    const port = parseInt(match[5]) * 256 + parseInt(match[6]);
    
    return { host, port };
  }

  async setBinaryMode(): Promise<void> {
    await this.sendCommand("TYPE I");
  }

  async downloadFile(filename: string): Promise<Uint8Array> {
    console.log(`Downloading: ${filename}`);
    
    await this.setBinaryMode();
    
    const { host, port } = await this.setPassiveMode();
    const dataConn = await Deno.connect({ hostname: host, port });
    
    const retrResp = await this.sendCommand(`RETR ${filename}`);
    const { code: retrCode } = this.parseResponse(retrResp);
    
    if (retrCode === 550) {
      try { dataConn.close(); } catch (_) {}
      throw new Error(`FILE_NOT_FOUND:${filename}`);
    }
    
    if (retrCode !== 125 && retrCode !== 150) {
      try { dataConn.close(); } catch (_) {}
      throw new Error(`FTP_DOWNLOAD_FAILED`);
    }
    
    const chunks: Uint8Array[] = [];
    let totalSize = 0;
    
    try {
      const buf = new Uint8Array(131072); // 128KB buffer
      while (true) {
        const n = await dataConn.read(buf);
        if (n === null) break;
        chunks.push(buf.slice(0, n));
        totalSize += n;
        
        if (totalSize % 10485760 < 131072) {
          console.log(`Downloaded: ${Math.round(totalSize / 1048576)} MB`);
        }
      }
    } catch (_e) {}
    
    try { dataConn.close(); } catch (_) {}
    
    try { await this.readResponse(); } catch (_) {}
    
    console.log(`Combining ${chunks.length} chunks, total: ${totalSize} bytes`);
    const result = new Uint8Array(totalSize);
    let offset = 0;
    for (const chunk of chunks) {
      result.set(chunk, offset);
      offset += chunk.length;
    }
    chunks.length = 0;
    
    console.log(`Download complete: ${totalSize} bytes`);
    return result;
  }

  async close(): Promise<void> {
    if (this.conn) {
      try { await this.sendCommand("QUIT"); } catch (_) {}
      if (this.reader) {
        try { this.reader.releaseLock(); } catch (_) {}
      }
      try { this.conn.close(); } catch (_) {}
      this.conn = null;
      this.reader = null;
    }
  }
}

// Authentication check: verify user is authenticated and is admin
async function checkAuthAndAdmin(req: Request, supabaseUrl: string, supabaseAnonKey: string): Promise<{ authorized: boolean; error?: string; status?: number }> {
  const authHeader = req.headers.get('authorization');
  
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return { authorized: false, error: 'Token di autenticazione mancante', status: 401 };
  }
  
  const token = authHeader.replace('Bearer ', '');
  
  // Create a Supabase client with the user's token
  const supabase = createClient(supabaseUrl, supabaseAnonKey, {
    global: {
      headers: {
        Authorization: `Bearer ${token}`
      }
    }
  });
  
  // Get the authenticated user
  const { data: { user }, error: userError } = await supabase.auth.getUser();
  
  if (userError || !user) {
    console.log('Authentication failed:', userError?.message || 'No user');
    return { authorized: false, error: 'Autenticazione non valida', status: 401 };
  }
  
  console.log(`User authenticated: ${user.id}`);
  
  // Check if user is admin
  const { data: adminData, error: adminError } = await supabase
    .from('admin_users')
    .select('user_id')
    .eq('user_id', user.id)
    .single();
  
  if (adminError || !adminData) {
    console.log(`User ${user.id} is not an admin`);
    return { authorized: false, error: 'Accesso non autorizzato. Solo gli amministratori possono importare file.', status: 403 };
  }
  
  console.log(`User ${user.id} is admin, proceeding`);
  return { authorized: true };
}

serve(async (req) => {
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  if (req.method !== 'POST') {
    return new Response(
      JSON.stringify({ status: "error", code: "METHOD_NOT_ALLOWED", message: "Only POST" } as ErrorResponse),
      { status: 405, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }

  let client: FTPClient | null = null;

  try {
    // Read environment variables first for auth check
    const supabaseUrl = Deno.env.get('SUPABASE_URL');
    const supabaseAnonKey = Deno.env.get('SUPABASE_ANON_KEY');
    const supabaseServiceKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');

    if (!supabaseUrl || !supabaseAnonKey) {
      return new Response(
        JSON.stringify({ status: "error", code: "CONFIG_MISSING", message: "Supabase config missing" } as ErrorResponse),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Check authentication and admin status
    const authResult = await checkAuthAndAdmin(req, supabaseUrl, supabaseAnonKey);
    if (!authResult.authorized) {
      return new Response(
        JSON.stringify({ status: "error", code: "UNAUTHORIZED", message: authResult.error } as ErrorResponse),
        { status: authResult.status || 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Parse request body
    const body = await req.json().catch(() => ({}));
    const fileType = body.fileType as string | undefined;

    // Validate fileType
    if (!fileType || !["material", "stock", "price"].includes(fileType)) {
      return new Response(
        JSON.stringify({ 
          status: "error", 
          code: "INVALID_FILE_TYPE", 
          message: "fileType deve essere uno tra: material, stock, price." 
        } as ErrorResponse),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    const config = FILE_CONFIG[fileType as FileType];
    console.log(`Processing single file: ${fileType} -> ${config.ftpName}`);

    // Read FTP environment variables
    const ftpHost = Deno.env.get('FTP_HOST');
    const ftpUser = Deno.env.get('FTP_USER');
    const ftpPass = Deno.env.get('FTP_PASSWORD');
    const ftpPort = parseInt(Deno.env.get('FTP_PORT') || '21');
    const ftpInputDir = Deno.env.get('FTP_INPUT_DIR') || '/';
    const ftpUseTLS = Deno.env.get('FTP_USE_TLS') === 'true';

    if (!ftpHost || !ftpUser || !ftpPass) {
      return new Response(
        JSON.stringify({ status: "error", code: "CONFIG_MISSING", message: "FTP config missing" } as ErrorResponse),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    if (!supabaseServiceKey) {
      return new Response(
        JSON.stringify({ status: "error", code: "CONFIG_MISSING", message: "Supabase service key missing" } as ErrorResponse),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Use service role key for storage operations (bypasses RLS)
    const supabase = createClient(supabaseUrl, supabaseServiceKey);
    client = new FTPClient(ftpUseTLS);

    // Connect to FTP
    await client.connect(ftpHost, ftpPort);
    await client.login(ftpUser, ftpPass);
    await client.cwd(ftpInputDir);

    const timestamp = Date.now();

    // Download the requested file
    let fileBuffer: Uint8Array;
    try {
      fileBuffer = await client.downloadFile(config.ftpName);
    } catch (e) {
      const errMsg = e instanceof Error ? e.message : String(e);
      if (errMsg.includes("FILE_NOT_FOUND")) {
        return new Response(
          JSON.stringify({ status: "error", code: "FILE_NOT_FOUND", message: `File ${config.ftpName} non trovato` } as ErrorResponse),
          { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
      throw e;
    }

    const fileSize = fileBuffer.length;
    console.log(`File size: ${fileSize} bytes (${Math.round(fileSize / 1048576)} MB)`);

    // Close FTP before upload to free memory
    await client.close();
    client = null;
    console.log(`FTP connection closed`);

    // Upload to Storage
    const newFilename = `${config.prefix}_${timestamp}.txt`;
    const storagePath = `${config.folder}/${newFilename}`;
    
    console.log(`Uploading to Storage: ${storagePath}`);
    
    const { error: uploadError } = await supabase.storage
      .from(BUCKET_NAME)
      .upload(storagePath, fileBuffer, { contentType: "text/plain", upsert: true });

    // Clear buffer immediately
    fileBuffer = new Uint8Array(0);

    if (uploadError) {
      console.error(`Upload error:`, uploadError.message);
      return new Response(
        JSON.stringify({ status: "error", code: "STORAGE_ERROR", message: uploadError.message } as ErrorResponse),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    console.log(`Upload complete`);

    // Create signed URL for the file (valid for 1 hour)
    const { data: signedUrlData, error: signedUrlError } = await supabase.storage
      .from(BUCKET_NAME)
      .createSignedUrl(storagePath, 3600);

    const fileResult: FileResult & { url?: string } = {
      filename: newFilename,
      path: storagePath,
      size: fileSize,
    };

    if (signedUrlData && !signedUrlError) {
      fileResult.url = signedUrlData.signedUrl;
    }

    // Build response with dynamic key
    const response = {
      status: "ok" as const,
      files: {
        [config.key]: fileResult,
      },
    };

    console.log(`${fileType} file processed successfully`);
    
    return new Response(
      JSON.stringify(response),
      { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );

  } catch (e) {
    console.error("Error:", e);
    const errMsg = e instanceof Error ? e.message : String(e);
    
    return new Response(
      JSON.stringify({ status: "error", code: "FTP_RUNTIME_ERROR", message: errMsg.substring(0, 200) } as ErrorResponse),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
    
  } finally {
    if (client) {
      try { await client.close(); } catch (_) {}
    }
  }
});
