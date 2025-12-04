import { serve } from "https://deno.land/std@0.168.0/http/server.ts";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

interface FileInfo {
  filename: string;
  content: string;
  last_modified: string | null;
  size: number | null;
}

interface SuccessResponse {
  status: "ok";
  files: {
    materialFile: FileInfo;
    priceFile: FileInfo;
    stockFile: FileInfo;
  };
}

interface ErrorResponse {
  status: "error";
  code: string;
  message: string;
}

const FILE_NAMES = {
  materialFile: "MaterialFile.txt",
  priceFile: "pricefileData_790813.txt",
  stockFile: "StockFileData_790813.txt",
};

const TIMEOUT_MS = 60000; // 60 seconds timeout

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
      // Check if we have a complete response in buffer
      const lines = this.buffer.split("\r\n");
      for (let i = 0; i < lines.length - 1; i++) {
        const line = lines[i];
        // FTP responses: 3 digits, space or dash, then text
        // Multi-line responses end with "XXX " (space after code)
        if (line.match(/^\d{3} /)) {
          // This is the final line of response
          const response = lines.slice(0, i + 1).join("\r\n");
          this.buffer = lines.slice(i + 1).join("\r\n");
          return response;
        }
      }
      
      // Need more data
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
      this.conn = await Deno.connectTls({
        hostname: host,
        port: port,
      });
    } else {
      this.conn = await Deno.connect({
        hostname: host,
        port: port,
      });
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
      // Password required
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
    
    // Parse PASV response: 227 Entering Passive Mode (h1,h2,h3,h4,p1,p2)
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

  async downloadFile(filename: string): Promise<{ content: Uint8Array; size: number | null; lastModified: string | null }> {
    console.log(`Downloading file: ${filename}`);
    
    // Try to get file info first
    let size: number | null = null;
    let lastModified: string | null = null;
    
    try {
      const sizeResp = await this.sendCommand(`SIZE ${filename}`);
      const sizeMatch = sizeResp.match(/^213 (\d+)/);
      if (sizeMatch) {
        size = parseInt(sizeMatch[1]);
      }
    } catch (e) {
      console.log("SIZE command not supported");
    }
    
    try {
      const mdtmResp = await this.sendCommand(`MDTM ${filename}`);
      const mdtmMatch = mdtmResp.match(/^213 (\d{14})/);
      if (mdtmMatch) {
        const ts = mdtmMatch[1];
        // Format: YYYYMMDDHHmmss
        lastModified = `${ts.slice(0, 4)}-${ts.slice(4, 6)}-${ts.slice(6, 8)}T${ts.slice(8, 10)}:${ts.slice(10, 12)}:${ts.slice(12, 14)}Z`;
      }
    } catch (e) {
      console.log("MDTM command not supported");
    }
    
    // Set binary mode
    await this.setBinaryMode();
    
    // Enter passive mode and get data connection info
    const { host, port } = await this.setPassiveMode();
    console.log(`Passive mode: ${host}:${port}`);
    
    // Open data connection
    const dataConn = await Deno.connect({
      hostname: host,
      port: port,
    });
    
    // Send RETR command
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
    
    // Read data
    const chunks: Uint8Array[] = [];
    const reader = dataConn.readable.getReader();
    
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        chunks.push(value);
      }
    } finally {
      reader.releaseLock();
      dataConn.close();
    }
    
    // Wait for transfer complete response
    const completeResp = await this.readResponse();
    const { code: completeCode } = this.parseResponse(completeResp);
    if (completeCode !== 226 && completeCode !== 250) {
      console.warn("Unexpected transfer complete response:", completeResp);
    }
    
    // Combine chunks
    const totalLength = chunks.reduce((acc, chunk) => acc + chunk.length, 0);
    const content = new Uint8Array(totalLength);
    let offset = 0;
    for (const chunk of chunks) {
      content.set(chunk, offset);
      offset += chunk.length;
    }
    
    console.log(`Downloaded ${filename}: ${content.length} bytes`);
    
    return { content, size: size ?? content.length, lastModified };
  }

  async close(): Promise<void> {
    if (this.conn) {
      try {
        await this.sendCommand("QUIT");
      } catch (e) {
        // Ignore errors during quit
      }
      
      if (this.reader) {
        try {
          this.reader.releaseLock();
        } catch (e) {
          // Ignore
        }
      }
      
      try {
        this.conn.close();
      } catch (e) {
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

serve(async (req) => {
  // Handle CORS preflight requests
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
    // Read secrets
    const ftpHost = Deno.env.get('FTP_HOST');
    const ftpUser = Deno.env.get('FTP_USER');
    const ftpPass = Deno.env.get('FTP_PASSWORD');
    const ftpPort = parseInt(Deno.env.get('FTP_PORT') || '21');
    const ftpInputDir = Deno.env.get('FTP_INPUT_DIR') || '/';
    const ftpUseTLS = Deno.env.get('FTP_USE_TLS') === 'true';

    console.log(`FTP Config: host=${ftpHost ? 'SET' : 'UNSET'}, user=${ftpUser ? 'SET' : 'UNSET'}, pass=${ftpPass ? 'SET' : 'UNSET'}, port=${ftpPort}, dir=${ftpInputDir}, tls=${ftpUseTLS}`);

    // Check required secrets
    if (!ftpHost || !ftpUser || !ftpPass) {
      console.error("Missing required FTP configuration");
      return new Response(
        JSON.stringify({ 
          status: "error", 
          code: "CONFIG_MISSING", 
          message: "Required FTP configuration is missing. Please ensure FTP_HOST, FTP_USER, and FTP_PASSWORD secrets are configured." 
        } as ErrorResponse),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Create FTP client
    client = new FTPClient(ftpUseTLS);

    // Connect with timeout
    await withTimeout(
      client.connect(ftpHost, ftpPort),
      TIMEOUT_MS,
      "FTP_TIMEOUT"
    );

    // Login
    await withTimeout(
      client.login(ftpUser, ftpPass),
      TIMEOUT_MS,
      "FTP_TIMEOUT"
    );

    // Change to input directory
    try {
      await withTimeout(
        client.cwd(ftpInputDir),
        TIMEOUT_MS,
        "FTP_TIMEOUT"
      );
    } catch (e) {
      const errMsg = e instanceof Error ? e.message : String(e);
      if (errMsg.includes("CWD_FAILED") || errMsg.includes("550")) {
        return new Response(
          JSON.stringify({ 
            status: "error", 
            code: "FTP_INPUT_DIR_NOT_FOUND", 
            message: `The FTP input directory '${ftpInputDir}' does not exist or is not accessible.` 
          } as ErrorResponse),
          { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
      throw e;
    }

    // Download files
    const files: { [key: string]: FileInfo } = {};
    
    for (const [key, filename] of Object.entries(FILE_NAMES)) {
      try {
        const { content, size, lastModified } = await withTimeout(
          client.downloadFile(filename),
          TIMEOUT_MS,
          "FTP_TIMEOUT"
        );
        
        // Convert to UTF-8 string without modifications
        const textContent = new TextDecoder('utf-8').decode(content);
        
        files[key] = {
          filename,
          content: textContent,
          last_modified: lastModified,
          size: size,
        };
      } catch (e) {
        const errMsg = e instanceof Error ? e.message : String(e);
        
        if (errMsg.includes("FILE_NOT_FOUND")) {
          return new Response(
            JSON.stringify({ 
              status: "error", 
              code: "FILE_NOT_FOUND", 
              message: `The file '${filename}' was not found on the FTP server.` 
            } as ErrorResponse),
            { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
          );
        }
        
        if (errMsg.includes("FTP_TIMEOUT")) {
          return new Response(
            JSON.stringify({ 
              status: "error", 
              code: "FTP_TIMEOUT", 
              message: `The FTP operation timed out while downloading '${filename}'.` 
            } as ErrorResponse),
            { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
          );
        }
        
        if (errMsg.includes("FTP_DOWNLOAD_FAILED")) {
          return new Response(
            JSON.stringify({ 
              status: "error", 
              code: "FTP_DOWNLOAD_FAILED", 
              message: `Failed to download '${filename}' from the FTP server.` 
            } as ErrorResponse),
            { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
          );
        }
        
        throw e;
      }
    }

    // Close connection
    await client.close();
    client = null;

    // Return success response
    const response: SuccessResponse = {
      status: "ok",
      files: {
        materialFile: files.materialFile,
        priceFile: files.priceFile,
        stockFile: files.stockFile,
      },
    };

    console.log("Successfully downloaded all files");
    
    return new Response(
      JSON.stringify(response),
      { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );

  } catch (e) {
    console.error("Unexpected error:", e);
    
    const errMsg = e instanceof Error ? e.message : String(e);
    
    // Handle timeout errors
    if (errMsg.includes("FTP_TIMEOUT")) {
      return new Response(
        JSON.stringify({ 
          status: "error", 
          code: "FTP_TIMEOUT", 
          message: "The FTP operation timed out." 
        } as ErrorResponse),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
    
    return new Response(
      JSON.stringify({ 
        status: "error", 
        code: "UNEXPECTED_ERROR", 
        message: `An unexpected error occurred: ${errMsg}` 
      } as ErrorResponse),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
    
  } finally {
    // Always close connection
    if (client) {
      try {
        await client.close();
      } catch (e) {
        console.error("Error closing FTP connection:", e);
      }
    }
  }
});
