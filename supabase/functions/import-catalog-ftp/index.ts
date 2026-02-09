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
  stockLocation: { ftpPattern: /^790813_StockFile_(\d{8})\.txt$/, folder: "stock-location", prefix: "790813_StockFile", key: "stockLocationFile" },
} as const;

type FileType = keyof typeof FILE_CONFIG;

// Stock location file patterns
const STOCK_LOCATION_EXACT = "790813_StockFile.txt"; // Priority: exact match first
const STOCK_LOCATION_DATED_REGEX = /^790813_StockFile_(\d{8})\.txt$/; // Fallback: dated pattern

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
    // Log command without password
    if (cmd.startsWith('PASS ')) {
      console.log('[FTPClient] Sending: PASS ****');
    } else {
      console.log(`[FTPClient] Sending: ${cmd}`);
    }
    await this.conn.write(encoder.encode(cmd + "\r\n"));
    const response = await this.readResponse();
    console.log(`[FTPClient] Response: ${response.substring(0, 100)}`);
    return response;
  }

  private parseResponse(response: string): { code: number; message: string } {
    const match = response.match(/^(\d{3})/);
    if (!match) throw new Error("Invalid FTP response");
    return { code: parseInt(match[1]), message: response };
  }

  async connect(host: string, port: number): Promise<void> {
    console.log(`[FTPClient] Connecting to ${host}:${port} (TLS: ${this.useTLS})...`);
    
    try {
      if (this.useTLS) {
        this.conn = await Deno.connectTls({ hostname: host, port });
      } else {
        this.conn = await Deno.connect({ hostname: host, port });
      }
      
      this.reader = this.conn.readable.getReader();
      const welcome = await this.readResponse();
      console.log(`[FTPClient] Welcome message received: ${welcome.substring(0, 50)}...`);
      
      const { code } = this.parseResponse(welcome);
      if (code !== 220) {
        throw new Error(`Unexpected welcome code: ${code} - ${welcome}`);
      }
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      console.error(`[FTPClient] Connection failed: ${msg}`);
      throw new Error(`FTP connection failed: ${msg}`);
    }
  }

  async login(user: string, pass: string): Promise<void> {
    console.log(`[FTPClient] Logging in as user: ${user}...`);
    
    const userResp = await this.sendCommand(`USER ${user}`);
    const { code: userCode } = this.parseResponse(userResp);
    
    if (userCode === 331) {
      const passResp = await this.sendCommand(`PASS ${pass}`);
      const { code: passCode, message: passMessage } = this.parseResponse(passResp);
      if (passCode !== 230) {
        console.error(`[FTPClient] Login failed with code ${passCode}: ${passMessage}`);
        throw new Error(`FTP authentication failed: invalid credentials (code ${passCode})`);
      }
    } else if (userCode !== 230) {
      console.error(`[FTPClient] USER command failed with code ${userCode}`);
      throw new Error(`FTP authentication failed: user rejected (code ${userCode})`);
    }
    
    console.log('[FTPClient] Login successful');
  }

  async cwd(dir: string): Promise<void> {
    console.log(`[FTPClient] Changing directory to: ${dir}`);
    const resp = await this.sendCommand(`CWD ${dir}`);
    const { code, message } = this.parseResponse(resp);
    if (code !== 250) {
      console.error(`[FTPClient] CWD failed: ${message}`);
      throw new Error(`FTP directory change failed: ${dir} (code ${code})`);
    }
  }

  async setPassiveMode(): Promise<{ host: string; port: number }> {
    const resp = await this.sendCommand("PASV");
    const { code, message } = this.parseResponse(resp);
    if (code !== 227) {
      console.error(`[FTPClient] PASV failed: ${message}`);
      throw new Error(`FTP passive mode failed (code ${code})`);
    }
    
    const match = resp.match(/\((\d+),(\d+),(\d+),(\d+),(\d+),(\d+)\)/);
    if (!match) {
      throw new Error("Could not parse PASV response");
    }
    
    const host = `${match[1]}.${match[2]}.${match[3]}.${match[4]}`;
    const port = parseInt(match[5]) * 256 + parseInt(match[6]);
    
    console.log(`[FTPClient] Passive mode: ${host}:${port}`);
    return { host, port };
  }

  async setBinaryMode(): Promise<void> {
    await this.sendCommand("TYPE I");
  }

  async downloadFile(filename: string): Promise<Uint8Array> {
    console.log(`[FTPClient] Downloading: ${filename}`);
    
    await this.setBinaryMode();
    
    const { host, port } = await this.setPassiveMode();
    const dataConn = await Deno.connect({ hostname: host, port });
    
    const retrResp = await this.sendCommand(`RETR ${filename}`);
    const { code: retrCode, message: retrMessage } = this.parseResponse(retrResp);
    
    if (retrCode === 550) {
      try { dataConn.close(); } catch (_) {}
      console.error(`[FTPClient] File not found: ${filename}`);
      throw new Error(`FTP file not found: ${filename}`);
    }
    
    if (retrCode !== 125 && retrCode !== 150) {
      try { dataConn.close(); } catch (_) {}
      console.error(`[FTPClient] RETR failed: ${retrMessage}`);
      throw new Error(`FTP download failed: ${filename} (code ${retrCode})`);
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
          console.log(`[FTPClient] Downloaded: ${Math.round(totalSize / 1048576)} MB`);
        }
      }
    } catch (_e) {}
    
    try { dataConn.close(); } catch (_) {}
    
    try { await this.readResponse(); } catch (_) {}
    
    console.log(`[FTPClient] Combining ${chunks.length} chunks, total: ${totalSize} bytes`);
    const result = new Uint8Array(totalSize);
    let offset = 0;
    for (const chunk of chunks) {
      result.set(chunk, offset);
      offset += chunk.length;
    }
    chunks.length = 0;
    
    console.log(`[FTPClient] Download complete: ${totalSize} bytes`);
    return result;
  }

  async listFiles(): Promise<Array<{ name: string; modified?: Date }>> {
    console.log(`[FTPClient] Listing directory files...`);
    
    const { host, port } = await this.setPassiveMode();
    const dataConn = await Deno.connect({ hostname: host, port });
    
    const listResp = await this.sendCommand("LIST");
    const { code: listCode, message: listMessage } = this.parseResponse(listResp);
    
    if (listCode !== 125 && listCode !== 150) {
      try { dataConn.close(); } catch (_) {}
      console.error(`[FTPClient] LIST failed: ${listMessage}`);
      throw new Error(`FTP LIST failed (code ${listCode})`);
    }
    
    const chunks: Uint8Array[] = [];
    let totalSize = 0;
    
    try {
      const buf = new Uint8Array(8192);
      while (true) {
        const n = await dataConn.read(buf);
        if (n === null) break;
        chunks.push(buf.slice(0, n));
        totalSize += n;
      }
    } catch (_e) {}
    
    try { dataConn.close(); } catch (_) {}
    try { await this.readResponse(); } catch (_) {}
    
    const result = new Uint8Array(totalSize);
    let offset = 0;
    for (const chunk of chunks) {
      result.set(chunk, offset);
      offset += chunk.length;
    }
    
    const listOutput = new TextDecoder().decode(result);
    const lines = listOutput.split(/\r?\n/).filter(l => l.trim());
    
    const files: Array<{ name: string; modified?: Date }> = [];
    for (const line of lines) {
      // Parse LIST output (Unix-style: permissions, size, date, name)
      const parts = line.trim().split(/\s+/);
      if (parts.length >= 9) {
        const name = parts.slice(8).join(' ');
        files.push({ name });
      } else if (parts.length >= 1) {
        // Simple filename only
        files.push({ name: parts[parts.length - 1] });
      }
    }
    
    console.log(`[FTPClient] Listed ${files.length} files`);
    return files;
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
async function checkAuth(req: Request, supabaseUrl: string, supabaseAnonKey: string, supabaseServiceKey: string): Promise<{ authorized: boolean; error?: string; status?: number; isServiceRole?: boolean }> {
  const authHeader = req.headers.get('authorization');
  
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return { authorized: false, error: 'Token di autenticazione mancante', status: 401 };
  }
  
  const token = authHeader.replace('Bearer ', '');
  
  // Check if this is a service role key (for internal calls from run-full-sync)
  if (token === supabaseServiceKey) {
    console.log('[import-catalog-ftp] Service role authentication - internal call');
    return { authorized: true, isServiceRole: true };
  }
  
  // Otherwise validate as user JWT
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
    console.log('[import-catalog-ftp] Authentication failed:', userError?.message || 'No user');
    return { authorized: false, error: 'Autenticazione non valida', status: 401 };
  }
  
  console.log(`[import-catalog-ftp] User authenticated: ${user.id}`);
  
  // Check if user is admin
  const { data: adminData, error: adminError } = await supabase
    .from('admin_users')
    .select('user_id')
    .eq('user_id', user.id)
    .single();
  
  if (adminError || !adminData) {
    console.log(`[import-catalog-ftp] User ${user.id} is not an admin`);
    return { authorized: false, error: 'Accesso non autorizzato. Solo gli amministratori possono importare file.', status: 403 };
  }
  
  console.log(`[import-catalog-ftp] User ${user.id} is admin, proceeding`);
  return { authorized: true, isServiceRole: false };
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
    const authResult = await checkAuth(req, supabaseUrl, supabaseAnonKey, supabaseServiceKey || '');
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
    if (!fileType || !["material", "stock", "price", "stockLocation"].includes(fileType)) {
      return new Response(
        JSON.stringify({ 
          status: "error", 
          code: "INVALID_FILE_TYPE", 
          message: "fileType deve essere uno tra: material, stock, price, stockLocation." 
        } as ErrorResponse),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Get optional run_id for per-run file storage
    const runId = body.run_id as string | undefined;
    
    console.log(`[import-catalog-ftp] Processing file: ${fileType}${runId ? ` (run: ${runId})` : ''}`);

    // Read FTP environment variables and log diagnostic info
    const ftpHost = Deno.env.get('FTP_HOST');
    const ftpUser = Deno.env.get('FTP_USER');
    const ftpPass = Deno.env.get('FTP_PASSWORD');
    const ftpPort = parseInt(Deno.env.get('FTP_PORT') || '21');
    const ftpInputDir = Deno.env.get('FTP_INPUT_DIR') || '/';
    const ftpUseTLS = Deno.env.get('FTP_USE_TLS') === 'true';

    // Diagnostic log (without password)
    console.log(`[import-catalog-ftp] FTP Config: host=${ftpHost}, port=${ftpPort}, user=${ftpUser}, hasPassword=${!!ftpPass}, inputDir=${ftpInputDir}, useTLS=${ftpUseTLS}`);

    if (!ftpHost || !ftpUser || !ftpPass) {
      console.error('[import-catalog-ftp] Missing FTP credentials');
      return new Response(
        JSON.stringify({ 
          status: "error", 
          code: "CONFIG_MISSING", 
          message: "Configurazione FTP incompleta. Verifica FTP_HOST, FTP_USER, FTP_PASSWORD." 
        } as ErrorResponse),
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

    // Connect to FTP with detailed logging
    console.log(`[import-catalog-ftp] Initiating FTP connection...`);
    await client.connect(ftpHost, ftpPort);
    console.log(`[import-catalog-ftp] FTP connected, attempting login...`);
    await client.login(ftpUser, ftpPass);
    console.log(`[import-catalog-ftp] FTP login successful, changing to directory: ${ftpInputDir}`);
    await client.cwd(ftpInputDir);

    const timestamp = Date.now();

    // Handle stockLocation differently - needs to list files and select latest
    if (fileType === 'stockLocation') {
      console.log(`[import-catalog-ftp] Stock location mode: listing files...`);
      
      let fileBuffer: Uint8Array;
      let selectedFilename = '';
      
      try {
        // List files in directory for diagnostic and selection
        const files = await client.listFiles();
        console.log(`[import-catalog-ftp] Directory listing - total files: ${files.length}`);
        
        // Log all files for diagnosis
        const allFilenames = files.map(f => f.name);
        console.log(`[import-catalog-ftp] All files in directory: ${allFilenames.slice(0, 50).join(', ')}${allFilenames.length > 50 ? '...' : ''}`);
        
        // Find files containing "790813_StockFile"
        const stockFileCandidates = files.filter(f => f.name.includes('790813_StockFile'));
        console.log(`[import-catalog-ftp] Files containing '790813_StockFile': ${stockFileCandidates.length}`);
        console.log(`[import-catalog-ftp] Stock file candidates: ${stockFileCandidates.map(f => f.name).join(', ')}`);
        
        // Priority 1: Exact match "790813_StockFile.txt"
        const exactMatch = files.find(f => f.name === STOCK_LOCATION_EXACT);
        console.log(`[import-catalog-ftp] Exact match '${STOCK_LOCATION_EXACT}': ${exactMatch ? 'FOUND' : 'NOT FOUND'}`);
        
        if (exactMatch) {
          selectedFilename = exactMatch.name;
          console.log(`[import-catalog-ftp] Using exact match: ${selectedFilename}`);
        } else {
          // Priority 2: Fallback to dated pattern 790813_StockFile_YYYYMMDD.txt
          console.log(`[import-catalog-ftp] Falling back to dated pattern...`);
          
          const datedFiles = files
            .filter(f => STOCK_LOCATION_DATED_REGEX.test(f.name))
            .map(f => {
              const match = f.name.match(STOCK_LOCATION_DATED_REGEX);
              return { name: f.name, date: match ? match[1] : '' };
            })
            .filter(f => f.date)
            .sort((a, b) => {
              // Sort by date descending, then by filename descending
              if (a.date !== b.date) return b.date.localeCompare(a.date);
              return b.name.localeCompare(a.name);
            });
          
          console.log(`[import-catalog-ftp] Dated pattern matches: ${datedFiles.length}`);
          console.log(`[import-catalog-ftp] Dated files found: ${datedFiles.map(f => `${f.name}(${f.date})`).join(', ')}`);
          
          if (datedFiles.length > 0) {
            selectedFilename = datedFiles[0].name;
            console.log(`[import-catalog-ftp] Selected latest dated file: ${selectedFilename} (date: ${datedFiles[0].date})`);
          }
        }
        
        // No file found at all
        if (!selectedFilename) {
          console.error(`[import-catalog-ftp] No stock location file found. Checked exact: ${STOCK_LOCATION_EXACT}, pattern: 790813_StockFile_YYYYMMDD.txt`);
          await client.close();
          return new Response(
            JSON.stringify({ 
              status: "error", 
              code: "FILE_NOT_FOUND", 
              message: `Nessun file stock location trovato (cercato: ${STOCK_LOCATION_EXACT} o 790813_StockFile_YYYYMMDD.txt)` 
            } as ErrorResponse),
            { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
          );
        }
        
        console.log(`[import-catalog-ftp] Final selected stock location file: ${selectedFilename}`);
        
        // Download the selected file
        fileBuffer = await client.downloadFile(selectedFilename);
      } catch (e) {
        const errMsg = e instanceof Error ? e.message : String(e);
        console.error(`[import-catalog-ftp] Stock location download failed: ${errMsg}`);
        await client.close();
        return new Response(
          JSON.stringify({ 
            status: "error", 
            code: "FILE_NOT_FOUND", 
            message: `Errore download stock location: ${errMsg}` 
          } as ErrorResponse),
          { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
      
      const fileSize = fileBuffer.length;
      console.log(`[import-catalog-ftp] Stock location file downloaded: ${fileSize} bytes`);
      
      // Close FTP
      await client.close();
      client = null;
      
      // Upload to Storage - always save to latest.txt
      const latestPath = 'stock-location/latest.txt';
      console.log(`[import-catalog-ftp] Uploading stock location to: ${latestPath}`);
      
      const { error: latestError } = await supabase.storage
        .from(BUCKET_NAME)
        .upload(latestPath, fileBuffer, { contentType: "text/plain", upsert: true });
      
      if (latestError) {
        console.error(`[import-catalog-ftp] Storage upload error (latest):`, latestError.message);
      }
      
      // If run_id provided, also save to per-run path
      if (runId) {
        const runPath = `stock-location/runs/${runId}.txt`;
        console.log(`[import-catalog-ftp] Uploading stock location to per-run path: ${runPath}`);
        
        const { error: runError } = await supabase.storage
          .from(BUCKET_NAME)
          .upload(runPath, fileBuffer, { contentType: "text/plain", upsert: true });
        
        if (runError) {
          console.error(`[import-catalog-ftp] Storage upload error (per-run):`, runError.message);
        }
      }
      
      // Clear buffer
      fileBuffer = new Uint8Array(0);
      
      const fileResult = {
        filename: selectedFilename,
        path: latestPath,
        size: fileSize,
      };
      
      const response = {
        status: "ok" as const,
        files: {
          stockLocationFile: fileResult,
        },
        selectedFile: selectedFilename,
      };
      
      console.log(`[import-catalog-ftp] Stock location file processed successfully`);
      
      return new Response(
        JSON.stringify(response),
        { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Standard file handling (material, stock, price)
    const config = FILE_CONFIG[fileType as 'material' | 'stock' | 'price'];
    
    // Download the requested file
    let fileBuffer: Uint8Array;
    try {
      console.log(`[import-catalog-ftp] Starting download of ${config.ftpName}...`);
      fileBuffer = await client.downloadFile(config.ftpName);
    } catch (e) {
      const errMsg = e instanceof Error ? e.message : String(e);
      console.error(`[import-catalog-ftp] Download failed: ${errMsg}`);
      if (errMsg.includes("not found")) {
        return new Response(
          JSON.stringify({ status: "error", code: "FILE_NOT_FOUND", message: `File ${config.ftpName} non trovato sul server FTP` } as ErrorResponse),
          { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
      throw e;
    }

    const fileSize = fileBuffer.length;
    console.log(`[import-catalog-ftp] File downloaded: ${fileSize} bytes (${Math.round(fileSize / 1048576)} MB)`);

    // Close FTP before upload to free memory
    await client.close();
    client = null;
    console.log(`[import-catalog-ftp] FTP connection closed`);

    // Upload to Storage
    const newFilename = `${config.prefix}_${timestamp}.txt`;
    const storagePath = `${config.folder}/${newFilename}`;
    
    console.log(`[import-catalog-ftp] Uploading to Storage: ${storagePath}`);
    
    const { error: uploadError } = await supabase.storage
      .from(BUCKET_NAME)
      .upload(storagePath, fileBuffer, { contentType: "text/plain", upsert: true });

    // Clear buffer immediately
    fileBuffer = new Uint8Array(0);

    if (uploadError) {
      console.error(`[import-catalog-ftp] Storage upload error:`, uploadError.message);
      return new Response(
        JSON.stringify({ status: "error", code: "STORAGE_ERROR", message: uploadError.message } as ErrorResponse),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    console.log(`[import-catalog-ftp] Upload complete: ${storagePath}`);

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

    console.log(`[import-catalog-ftp] ${fileType} file processed successfully`);
    
    return new Response(
      JSON.stringify(response),
      { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );

  } catch (e) {
    console.error("[import-catalog-ftp] Error:", e);
    const errMsg = e instanceof Error ? e.message : String(e);
    
    // Provide user-friendly error messages
    let userMessage = errMsg;
    let errorCode = "FTP_RUNTIME_ERROR";
    
    if (errMsg.toLowerCase().includes('authentication') || errMsg.toLowerCase().includes('credentials') || errMsg.includes('530')) {
      userMessage = "Errore FTP: credenziali non valide. Verifica host, utente e password.";
      errorCode = "FTP_AUTH_ERROR";
    } else if (errMsg.toLowerCase().includes('connection') || errMsg.toLowerCase().includes('connect')) {
      userMessage = "Errore FTP: impossibile connettersi al server. Verifica host e porta.";
      errorCode = "FTP_CONNECTION_ERROR";
    } else if (errMsg.toLowerCase().includes('timeout')) {
      userMessage = "Errore FTP: timeout connessione.";
      errorCode = "FTP_TIMEOUT";
    }
    
    return new Response(
      JSON.stringify({ status: "error", code: errorCode, message: userMessage } as ErrorResponse),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
    
  } finally {
    if (client) {
      try { await client.close(); } catch (_) {}
    }
  }
});
