import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

interface FileResult {
  filename: string;
  path: string;
  url: string;
  size: number;
}

interface SuccessResponse {
  status: "ok";
  files: {
    materialFile: FileResult;
    priceFile: FileResult;
    stockFile: FileResult;
  };
}

interface ErrorResponse {
  status: "error";
  code: string;
  message: string;
}

const FILE_CONFIG = {
  materialFile: { ftpName: "MaterialFile.txt", folder: "material", prefix: "MaterialFile" },
  priceFile: { ftpName: "pricefileData_790813.txt", folder: "price", prefix: "pricefileData" },
  stockFile: { ftpName: "StockFileData_790813.txt", folder: "stock", prefix: "StockFileData" },
};

const TIMEOUT_MS = 300000; // 5 minutes for large files
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
    console.log("Welcome:", welcome);
    
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
        throw new Error(`Login failed: ${passResp}`);
      }
    } else if (userCode !== 230) {
      throw new Error(`Login failed: ${userResp}`);
    }
    
    console.log("Logged in successfully");
  }

  async cwd(dir: string): Promise<void> {
    console.log(`Changing directory to: ${dir}`);
    const resp = await this.sendCommand(`CWD ${dir}`);
    const { code } = this.parseResponse(resp);
    if (code !== 250) {
      throw new Error(`CWD_FAILED:${resp}`);
    }
  }

  async setPassiveMode(): Promise<{ host: string; port: number }> {
    const resp = await this.sendCommand("PASV");
    const { code } = this.parseResponse(resp);
    if (code !== 227) {
      throw new Error(`PASV failed: ${resp}`);
    }
    
    const match = resp.match(/\((\d+),(\d+),(\d+),(\d+),(\d+),(\d+)\)/);
    if (!match) {
      throw new Error("Could not parse PASV response");
    }
    
    const host = `${match[1]}.${match[2]}.${match[3]}.${match[4]}`;
    const port = parseInt(match[5]) * 256 + parseInt(match[6]);
    
    return { host, port };
  }

  async setBinaryMode(): Promise<void> {
    const resp = await this.sendCommand("TYPE I");
    const { code } = this.parseResponse(resp);
    if (code !== 200) {
      console.warn("Could not set binary mode:", resp);
    }
  }

  async downloadFileAsStream(filename: string): Promise<{ stream: ReadableStream<Uint8Array>; dataConn: Deno.Conn }> {
    console.log(`Downloading file: ${filename}`);
    
    await this.setBinaryMode();
    
    const { host, port } = await this.setPassiveMode();
    console.log(`Passive mode: ${host}:${port}`);
    
    const dataConn = await Deno.connect({ hostname: host, port });
    
    const retrResp = await this.sendCommand(`RETR ${filename}`);
    const { code: retrCode } = this.parseResponse(retrResp);
    
    if (retrCode === 550) {
      dataConn.close();
      throw new Error(`FILE_NOT_FOUND:${filename}`);
    }
    
    if (retrCode !== 125 && retrCode !== 150) {
      dataConn.close();
      throw new Error(`FTP_DOWNLOAD_FAILED:${retrResp}`);
    }
    
    return { stream: dataConn.readable, dataConn };
  }

  async waitForTransferComplete(): Promise<void> {
    const completeResp = await this.readResponse();
    const { code: completeCode } = this.parseResponse(completeResp);
    if (completeCode !== 226 && completeCode !== 250) {
      console.warn("Unexpected transfer complete response:", completeResp);
    }
  }

  async close(): Promise<void> {
    if (this.conn) {
      try {
        await this.sendCommand("QUIT");
      } catch (_e) {
        // Ignore errors during quit
      }
      
      if (this.reader) {
        try {
          this.reader.releaseLock();
        } catch (_e) {
          // Ignore
        }
      }
      
      try {
        this.conn.close();
      } catch (_e) {
        // Ignore
      }
      
      this.conn = null;
      this.reader = null;
    }
  }
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, errorCode: string): Promise<T> {
  let timeoutId: number;
  
  const timeoutPromise = new Promise<never>((_, reject) => {
    timeoutId = setTimeout(() => {
      reject(new Error(`${errorCode}:Operation timed out after ${timeoutMs}ms`));
    }, timeoutMs);
  });
  
  try {
    const result = await Promise.race([promise, timeoutPromise]);
    clearTimeout(timeoutId!);
    return result;
  } catch (e) {
    clearTimeout(timeoutId!);
    throw e;
  }
}

async function streamToBuffer(stream: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let totalSize = 0;
  
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      chunks.push(value);
      totalSize += value.length;
    }
  } finally {
    reader.releaseLock();
  }
  
  // Combine chunks into single buffer
  const result = new Uint8Array(totalSize);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.length;
  }
  
  // Clear chunks array to help GC
  chunks.length = 0;
  
  return result;
}

serve(async (req) => {
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  if (req.method !== 'POST') {
    return new Response(
      JSON.stringify({ status: "error", code: "METHOD_NOT_ALLOWED", message: "Only POST method is allowed" } as ErrorResponse),
      { status: 405, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }

  let client: FTPClient | null = null;

  try {
    // Read FTP secrets
    const ftpHost = Deno.env.get('FTP_HOST');
    const ftpUser = Deno.env.get('FTP_USER');
    const ftpPass = Deno.env.get('FTP_PASSWORD');
    const ftpPort = parseInt(Deno.env.get('FTP_PORT') || '21');
    const ftpInputDir = Deno.env.get('FTP_INPUT_DIR') || '/';
    const ftpUseTLS = Deno.env.get('FTP_USE_TLS') === 'true';

    // Read Supabase secrets
    const supabaseUrl = Deno.env.get('SUPABASE_URL');
    const supabaseServiceKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');

    console.log(`FTP Config: host=${ftpHost ? 'SET' : 'UNSET'}, user=${ftpUser ? 'SET' : 'UNSET'}, port=${ftpPort}, dir=${ftpInputDir}`);

    if (!ftpHost || !ftpUser || !ftpPass) {
      console.error("Missing required FTP configuration");
      return new Response(
        JSON.stringify({ 
          status: "error", 
          code: "CONFIG_MISSING", 
          message: "Required FTP configuration is missing (FTP_HOST, FTP_USER, FTP_PASSWORD)." 
        } as ErrorResponse),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    if (!supabaseUrl || !supabaseServiceKey) {
      console.error("Missing Supabase configuration");
      return new Response(
        JSON.stringify({ 
          status: "error", 
          code: "CONFIG_MISSING", 
          message: "Required Supabase configuration is missing." 
        } as ErrorResponse),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Create Supabase client with service role
    const supabase = createClient(supabaseUrl, supabaseServiceKey);

    // Create FTP client
    client = new FTPClient(ftpUseTLS);

    // Connect with timeout
    await withTimeout(client.connect(ftpHost, ftpPort), TIMEOUT_MS, "FTP_TIMEOUT");

    // Login
    await withTimeout(client.login(ftpUser, ftpPass), TIMEOUT_MS, "FTP_TIMEOUT");

    // Change to input directory
    try {
      await withTimeout(client.cwd(ftpInputDir), TIMEOUT_MS, "FTP_TIMEOUT");
    } catch (e) {
      const errMsg = e instanceof Error ? e.message : String(e);
      if (errMsg.includes("CWD_FAILED") || errMsg.includes("550")) {
        return new Response(
          JSON.stringify({ 
            status: "error", 
            code: "FTP_INPUT_DIR_NOT_FOUND", 
            message: `The FTP input directory '${ftpInputDir}' does not exist.` 
          } as ErrorResponse),
          { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
      throw e;
    }

    const timestamp = Date.now();
    const results: { [key: string]: FileResult } = {};

    // Process each file: download from FTP and upload to Storage
    for (const [key, config] of Object.entries(FILE_CONFIG)) {
      console.log(`Processing ${key}: ${config.ftpName}`);
      
      let dataConn: Deno.Conn | null = null;
      
      try {
        // Download file from FTP
        const downloadResult = await withTimeout(
          client.downloadFileAsStream(config.ftpName),
          TIMEOUT_MS,
          "FTP_TIMEOUT"
        );
        dataConn = downloadResult.dataConn;
        
        // Read stream to buffer (single copy)
        const fileBuffer = await streamToBuffer(downloadResult.stream);
        const fileSize = fileBuffer.length;
        
        console.log(`Downloaded ${config.ftpName}: ${fileSize} bytes`);
        
        // Close data connection
        try {
          dataConn.close();
        } catch (_e) {
          // Ignore close errors
        }
        dataConn = null;
        
        // Wait for FTP transfer complete
        await client.waitForTransferComplete();
        
        // Generate unique filename with timestamp
        const newFilename = `${config.prefix}_${timestamp}.txt`;
        const storagePath = `${config.folder}/${newFilename}`;
        
        console.log(`Uploading to Storage: ${storagePath}`);
        
        // Upload to Supabase Storage
        const { error: uploadError } = await supabase.storage
          .from(BUCKET_NAME)
          .upload(storagePath, fileBuffer, {
            contentType: "text/plain",
            upsert: true,
          });
        
        if (uploadError) {
          console.error(`Upload error for ${storagePath}:`, uploadError);
          throw new Error(`STORAGE_UPLOAD_FAILED:${uploadError.message}`);
        }
        
        console.log(`Uploaded ${storagePath} successfully`);
        
        // Get public URL
        const { data: urlData } = supabase.storage
          .from(BUCKET_NAME)
          .getPublicUrl(storagePath);
        
        results[key] = {
          filename: newFilename,
          path: storagePath,
          url: urlData.publicUrl,
          size: fileSize,
        };
        
      } catch (e) {
        // Ensure data connection is closed on error
        if (dataConn) {
          try {
            dataConn.close();
          } catch (_e) {
            // Ignore
          }
        }
        
        const errMsg = e instanceof Error ? e.message : String(e);
        
        if (errMsg.includes("FILE_NOT_FOUND")) {
          return new Response(
            JSON.stringify({ 
              status: "error", 
              code: "FILE_NOT_FOUND", 
              message: `Il file '${config.ftpName}' non è stato trovato sul server FTP.` 
            } as ErrorResponse),
            { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
          );
        }
        
        if (errMsg.includes("FTP_TIMEOUT")) {
          return new Response(
            JSON.stringify({ 
              status: "error", 
              code: "FTP_TIMEOUT", 
              message: `Timeout durante il download di '${config.ftpName}'.` 
            } as ErrorResponse),
            { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
          );
        }
        
        if (errMsg.includes("STORAGE_UPLOAD_FAILED")) {
          return new Response(
            JSON.stringify({ 
              status: "error", 
              code: "STORAGE_UPLOAD_FAILED", 
              message: `Errore durante l'upload di '${config.ftpName}' su Storage.` 
            } as ErrorResponse),
            { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
          );
        }
        
        throw e;
      }
    }

    // Close FTP connection
    await client.close();
    client = null;

    // Return success response with URLs only
    const response: SuccessResponse = {
      status: "ok",
      files: {
        materialFile: results.materialFile,
        priceFile: results.priceFile,
        stockFile: results.stockFile,
      },
    };

    console.log("Successfully processed all files");
    
    return new Response(
      JSON.stringify(response),
      { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );

  } catch (e) {
    console.error("Unexpected error:", e);
    
    const errMsg = e instanceof Error ? e.message : String(e);
    
    if (errMsg.includes("FTP_TIMEOUT")) {
      return new Response(
        JSON.stringify({ 
          status: "error", 
          code: "FTP_TIMEOUT", 
          message: "Operazione FTP scaduta." 
        } as ErrorResponse),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    return new Response(
      JSON.stringify({ 
        status: "error", 
        code: "FTP_RUNTIME_ERROR", 
        message: `Errore imprevisto: ${errMsg.substring(0, 200)}` 
      } as ErrorResponse),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
    
  } finally {
    if (client) {
      try {
        await client.close();
      } catch (_e) {
        console.error("Error closing FTP connection");
      }
    }
  }
});
