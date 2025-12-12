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

const STOCK_LOCATION_EXACT = "790813_StockFile.txt";
const STOCK_LOCATION_DATED_REGEX = /^790813_StockFile_(\d{8})\.txt$/;
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
    if (cmd.startsWith('PASS ')) console.log('[FTPClient] Sending: PASS ****');
    else console.log(`[FTPClient] Sending: ${cmd}`);
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
      if (this.useTLS) this.conn = await Deno.connectTls({ hostname: host, port });
      else this.conn = await Deno.connect({ hostname: host, port });
      this.reader = this.conn.readable.getReader();
      const welcome = await this.readResponse();
      const { code } = this.parseResponse(welcome);
      if (code !== 220) throw new Error(`Unexpected welcome code: ${code}`);
    } catch (e: any) {
      throw new Error(`FTP connection failed: ${e.message}`);
    }
  }

  async login(user: string, pass: string): Promise<void> {
    const userResp = await this.sendCommand(`USER ${user}`);
    const { code: userCode } = this.parseResponse(userResp);
    if (userCode === 331) {
      const passResp = await this.sendCommand(`PASS ${pass}`);
      const { code: passCode } = this.parseResponse(passResp);
      if (passCode !== 230) throw new Error(`FTP authentication failed (code ${passCode})`);
    } else if (userCode !== 230) {
      throw new Error(`FTP authentication failed (code ${userCode})`);
    }
  }

  async cwd(dir: string): Promise<void> {
    const resp = await this.sendCommand(`CWD ${dir}`);
    const { code } = this.parseResponse(resp);
    if (code !== 250) throw new Error(`FTP directory change failed: ${dir}`);
  }

  async setPassiveMode(): Promise<{ host: string; port: number }> {
    const resp = await this.sendCommand("PASV");
    const { code } = this.parseResponse(resp);
    if (code !== 227) throw new Error(`FTP passive mode failed`);
    const match = resp.match(/\((\d+),(\d+),(\d+),(\d+),(\d+),(\d+)\)/);
    if (!match) throw new Error("Could not parse PASV response");
    return { host: `${match[1]}.${match[2]}.${match[3]}.${match[4]}`, port: parseInt(match[5]) * 256 + parseInt(match[6]) };
  }

  async setBinaryMode(): Promise<void> { await this.sendCommand("TYPE I"); }

  async downloadFile(filename: string): Promise<Uint8Array> {
    console.log(`[FTPClient] Downloading: ${filename}`);
    await this.setBinaryMode();
    const { host, port } = await this.setPassiveMode();
    const dataConn = await Deno.connect({ hostname: host, port });
    const retrResp = await this.sendCommand(`RETR ${filename}`);
    const { code: retrCode } = this.parseResponse(retrResp);
    if (retrCode === 550) { try { dataConn.close(); } catch (_) {} throw new Error(`FTP file not found: ${filename}`); }
    if (retrCode !== 125 && retrCode !== 150) { try { dataConn.close(); } catch (_) {} throw new Error(`FTP download failed (code ${retrCode})`); }
    
    const chunks: Uint8Array[] = [];
    let totalSize = 0;
    try {
      const buf = new Uint8Array(131072);
      while (true) {
        const n = await dataConn.read(buf);
        if (n === null) break;
        chunks.push(buf.slice(0, n));
        totalSize += n;
      }
    } catch (_) {}
    try { dataConn.close(); } catch (_) {}
    try { await this.readResponse(); } catch (_) {}
    
    const result = new Uint8Array(totalSize);
    let offset = 0;
    for (const chunk of chunks) { result.set(chunk, offset); offset += chunk.length; }
    console.log(`[FTPClient] Download complete: ${totalSize} bytes`);
    return result;
  }

  async listFiles(): Promise<Array<{ name: string }>> {
    const { host, port } = await this.setPassiveMode();
    const dataConn = await Deno.connect({ hostname: host, port });
    const listResp = await this.sendCommand("LIST");
    const { code: listCode } = this.parseResponse(listResp);
    if (listCode !== 125 && listCode !== 150) { try { dataConn.close(); } catch (_) {} throw new Error(`FTP LIST failed`); }
    
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
    } catch (_) {}
    try { dataConn.close(); } catch (_) {}
    try { await this.readResponse(); } catch (_) {}
    
    const result = new Uint8Array(totalSize);
    let offset = 0;
    for (const chunk of chunks) { result.set(chunk, offset); offset += chunk.length; }
    const listOutput = new TextDecoder().decode(result);
    const lines = listOutput.split(/\r?\n/).filter(l => l.trim());
    
    return lines.map(line => {
      const parts = line.trim().split(/\s+/);
      return { name: parts.length >= 9 ? parts.slice(8).join(' ') : parts[parts.length - 1] };
    });
  }

  async close(): Promise<void> {
    if (this.conn) {
      try { await this.sendCommand("QUIT"); } catch (_) {}
      if (this.reader) { try { this.reader.releaseLock(); } catch (_) {} }
      try { this.conn.close(); } catch (_) {}
      this.conn = null;
      this.reader = null;
    }
  }
}

async function checkAuth(req: Request, supabaseUrl: string, supabaseAnonKey: string, supabaseServiceKey: string): Promise<{ authorized: boolean; error?: string; status?: number; isServiceRole?: boolean }> {
  const authHeader = req.headers.get('authorization');
  if (!authHeader || !authHeader.startsWith('Bearer ')) return { authorized: false, error: 'Token mancante', status: 401 };
  
  const token = authHeader.replace('Bearer ', '');
  if (token === supabaseServiceKey) return { authorized: true, isServiceRole: true };
  
  const supabase = createClient(supabaseUrl, supabaseAnonKey, { global: { headers: { Authorization: `Bearer ${token}` } } });
  const { data: { user }, error: userError } = await supabase.auth.getUser();
  if (userError || !user) return { authorized: false, error: 'Autenticazione non valida', status: 401 };
  
  const { data: adminData } = await supabase.from('admin_users').select('user_id').eq('user_id', user.id).single();
  if (!adminData) return { authorized: false, error: 'Solo gli amministratori possono importare file', status: 403 };
  
  return { authorized: true, isServiceRole: false };
}

serve(async (req) => {
  if (req.method === 'OPTIONS') return new Response(null, { headers: corsHeaders });
  if (req.method !== 'POST') {
    return new Response(JSON.stringify({ status: "error", code: "METHOD_NOT_ALLOWED", message: "Only POST" } as ErrorResponse), 
      { status: 405, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
  }

  let client: FTPClient | null = null;

  try {
    const supabaseUrl = Deno.env.get('SUPABASE_URL');
    const supabaseAnonKey = Deno.env.get('SUPABASE_ANON_KEY');
    const supabaseServiceKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');

    if (!supabaseUrl || !supabaseAnonKey || !supabaseServiceKey) {
      return new Response(JSON.stringify({ status: "error", code: "CONFIG_MISSING", message: "Supabase config missing" } as ErrorResponse),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    const authResult = await checkAuth(req, supabaseUrl, supabaseAnonKey, supabaseServiceKey);
    if (!authResult.authorized) {
      return new Response(JSON.stringify({ status: "error", code: "UNAUTHORIZED", message: authResult.error } as ErrorResponse),
        { status: authResult.status || 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    const body = await req.json().catch(() => ({}));
    const fileType = body.fileType as string | undefined;
    const runId = body.run_id as string | undefined;

    if (!fileType || !["material", "stock", "price", "stockLocation"].includes(fileType)) {
      return new Response(JSON.stringify({ status: "error", code: "INVALID_FILE_TYPE", message: "fileType deve essere: material, stock, price, stockLocation" } as ErrorResponse),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    console.log(`[import-catalog-ftp] Processing: ${fileType}${runId ? ` (run: ${runId})` : ''}`);

    const ftpHost = Deno.env.get('FTP_HOST');
    const ftpUser = Deno.env.get('FTP_USER');
    const ftpPass = Deno.env.get('FTP_PASSWORD');
    const ftpPort = parseInt(Deno.env.get('FTP_PORT') || '21');
    const ftpInputDir = Deno.env.get('FTP_INPUT_DIR') || '/';
    const ftpUseTLS = Deno.env.get('FTP_USE_TLS') === 'true';

    if (!ftpHost || !ftpUser || !ftpPass) {
      return new Response(JSON.stringify({ status: "error", code: "CONFIG_MISSING", message: "FTP config incompleta" } as ErrorResponse),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    const supabase = createClient(supabaseUrl, supabaseServiceKey);
    client = new FTPClient(ftpUseTLS);

    await client.connect(ftpHost, ftpPort);
    await client.login(ftpUser, ftpPass);
    await client.cwd(ftpInputDir);

    const timestamp = Date.now();

    // Handle stockLocation
    if (fileType === 'stockLocation') {
      let fileBuffer: Uint8Array;
      let selectedFilename = '';
      
      const files = await client.listFiles();
      const exactMatch = files.find(f => f.name === STOCK_LOCATION_EXACT);
      
      if (exactMatch) {
        selectedFilename = exactMatch.name;
      } else {
        const datedFiles = files.filter(f => STOCK_LOCATION_DATED_REGEX.test(f.name))
          .map(f => ({ name: f.name, date: f.name.match(STOCK_LOCATION_DATED_REGEX)?.[1] || '' }))
          .filter(f => f.date)
          .sort((a, b) => b.date.localeCompare(a.date));
        if (datedFiles.length > 0) selectedFilename = datedFiles[0].name;
      }
      
      if (!selectedFilename) {
        await client.close();
        return new Response(JSON.stringify({ status: "error", code: "FILE_NOT_FOUND", message: "Nessun file stock location trovato" } as ErrorResponse),
          { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      fileBuffer = await client.downloadFile(selectedFilename);
      await client.close();
      client = null;
      
      const fileSize = fileBuffer.length;
      
      // Save to latest path
      const latestPath = 'stock-location/latest.txt';
      await supabase.storage.from(BUCKET_NAME).upload(latestPath, fileBuffer, { contentType: "text/plain", upsert: true });
      
      // If run_id provided, save to per-run path (DETERMINISTIC)
      let runPath: string | undefined;
      if (runId) {
        runPath = `runs/${runId}/stock-location.txt`;
        console.log(`[import-catalog-ftp] Saving stock-location to per-run path: ${runPath}`);
        await supabase.storage.from(BUCKET_NAME).upload(runPath, fileBuffer, { contentType: "text/plain", upsert: true });
      }
      
      return new Response(JSON.stringify({
        status: "ok",
        files: { stockLocationFile: { filename: selectedFilename, path: runPath || latestPath, size: fileSize } },
        path: runPath || latestPath
      }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    // Standard file handling (material, stock, price)
    const config = FILE_CONFIG[fileType as 'material' | 'stock' | 'price'];
    
    let fileBuffer: Uint8Array;
    try {
      fileBuffer = await client.downloadFile(config.ftpName);
    } catch (e) {
      const errMsg = e instanceof Error ? e.message : String(e);
      if (errMsg.includes("not found")) {
        return new Response(JSON.stringify({ status: "error", code: "FILE_NOT_FOUND", message: `File ${config.ftpName} non trovato` } as ErrorResponse),
          { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      throw e;
    }

    const fileSize = fileBuffer.length;
    await client.close();
    client = null;

    // Save to timestamped path (for latest listing)
    const newFilename = `${config.prefix}_${timestamp}.txt`;
    const storagePath = `${config.folder}/${newFilename}`;
    await supabase.storage.from(BUCKET_NAME).upload(storagePath, fileBuffer, { contentType: "text/plain", upsert: true });

    // If run_id provided, save to per-run path (DETERMINISTIC INPUT)
    let runPath: string | undefined;
    if (runId) {
      runPath = `runs/${runId}/${fileType}.txt`;
      console.log(`[import-catalog-ftp] Saving ${fileType} to per-run path: ${runPath}`);
      await supabase.storage.from(BUCKET_NAME).upload(runPath, fileBuffer, { contentType: "text/plain", upsert: true });
    }

    fileBuffer = new Uint8Array(0); // Clear memory

    const fileResult: FileResult & { url?: string } = {
      filename: newFilename,
      path: runPath || storagePath,
      size: fileSize,
    };

    return new Response(JSON.stringify({
      status: "ok",
      files: { [config.key]: fileResult },
      path: runPath || storagePath // Return path for orchestrator to save
    }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

  } catch (e) {
    console.error("[import-catalog-ftp] Error:", e);
    const errMsg = e instanceof Error ? e.message : String(e);
    let userMessage = errMsg;
    let errorCode = "FTP_RUNTIME_ERROR";
    
    if (errMsg.toLowerCase().includes('authentication') || errMsg.includes('530')) {
      userMessage = "Errore FTP: credenziali non valide";
      errorCode = "FTP_AUTH_ERROR";
    } else if (errMsg.toLowerCase().includes('connection')) {
      userMessage = "Errore FTP: impossibile connettersi";
      errorCode = "FTP_CONNECTION_ERROR";
    }
    
    return new Response(JSON.stringify({ status: "error", code: errorCode, message: userMessage } as ErrorResponse),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
  } finally {
    if (client) { try { await client.close(); } catch (_) {} }
  }
});
