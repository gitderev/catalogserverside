import { serve } from "https://deno.land/std@0.168.0/http/server.ts";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Rate limiting: 20 requests per minute per IP (more lenient for validation)
const RATE_LIMIT_WINDOW_MS = 60 * 1000; // 1 minute
const RATE_LIMIT_MAX_REQUESTS = 20;
const rateLimitMap = new Map<string, { count: number; windowStart: number }>();

function getClientIP(req: Request): string {
  return req.headers.get('x-forwarded-for')?.split(',')[0]?.trim() || 
         req.headers.get('x-real-ip') || 
         'unknown';
}

function checkRateLimit(ip: string): boolean {
  const now = Date.now();
  const entry = rateLimitMap.get(ip);
  
  if (!entry || now - entry.windowStart > RATE_LIMIT_WINDOW_MS) {
    rateLimitMap.set(ip, { count: 1, windowStart: now });
    return true;
  }
  
  if (entry.count >= RATE_LIMIT_MAX_REQUESTS) {
    return false;
  }
  
  entry.count++;
  return true;
}

// Constant-time string comparison to prevent timing attacks
function timeSafeCompare(a: string, b: string): boolean {
  if (a.length !== b.length) {
    // Still do some work to avoid length-based timing
    let dummy = 0;
    for (let i = 0; i < a.length; i++) {
      dummy ^= a.charCodeAt(i);
    }
    return false;
  }
  
  let result = 0;
  for (let i = 0; i < a.length; i++) {
    result |= a.charCodeAt(i) ^ b.charCodeAt(i);
  }
  return result === 0;
}

// Validate HMAC-SHA256 token
async function validateToken(token: string, secret: string): Promise<boolean> {
  try {
    const decoded = atob(token);
    const lastDotIndex = decoded.lastIndexOf('.');
    
    if (lastDotIndex === -1) {
      console.log('Token validation failed: invalid format');
      return false;
    }
    
    const payload = decoded.substring(0, lastDotIndex);
    const providedSignature = decoded.substring(lastDotIndex + 1);
    
    // Parse payload
    const parts = payload.split(':');
    if (parts.length !== 2) {
      console.log('Token validation failed: invalid payload format');
      return false;
    }
    
    const [status, expiryStr] = parts;
    const expiry = parseInt(expiryStr, 10);
    
    // Check expiry
    if (Date.now() > expiry) {
      console.log('Token expired');
      return false;
    }
    
    // Verify HMAC signature
    const encoder = new TextEncoder();
    const keyData = encoder.encode(secret);
    const payloadData = encoder.encode(payload);
    
    const cryptoKey = await crypto.subtle.importKey(
      'raw',
      keyData,
      { name: 'HMAC', hash: 'SHA-256' },
      false,
      ['sign']
    );
    
    const expectedSignature = await crypto.subtle.sign('HMAC', cryptoKey, payloadData);
    const expectedSignatureHex = Array.from(new Uint8Array(expectedSignature))
      .map(b => b.toString(16).padStart(2, '0'))
      .join('');
    
    // Use constant-time comparison
    const signatureValid = timeSafeCompare(providedSignature, expectedSignatureHex);
    
    return signatureValid && status === 'authenticated';
  } catch (error) {
    console.error('Token validation error:', error);
    return false;
  }
}

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  // Rate limiting check
  const clientIP = getClientIP(req);
  if (!checkRateLimit(clientIP)) {
    console.log(`Rate limit exceeded for IP: ${clientIP}`);
    return new Response(
      JSON.stringify({ valid: false, error: 'Troppe richieste. Riprova tra un minuto.' }),
      { status: 429, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }

  try {
    const { token } = await req.json();
    
    if (!token) {
      return new Response(
        JSON.stringify({ valid: false }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    const secret = Deno.env.get('AUTH_TOKEN_SECRET') || Deno.env.get('APP_ACCESS_PASSWORD');
    
    if (!secret) {
      console.error('AUTH_TOKEN_SECRET not configured');
      return new Response(
        JSON.stringify({ valid: false }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    const isValid = await validateToken(token, secret);
    
    return new Response(
      JSON.stringify({ valid: isValid }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  } catch (error) {
    console.error('Error in auth-validate:', error);
    return new Response(
      JSON.stringify({ valid: false }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
