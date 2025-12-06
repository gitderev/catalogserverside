import { serve } from "https://deno.land/std@0.168.0/http/server.ts";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Rate limiting: 10 requests per minute per IP
const RATE_LIMIT_WINDOW_MS = 60 * 1000; // 1 minute
const RATE_LIMIT_MAX_REQUESTS = 10;
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

// Generate HMAC-SHA256 token
async function generateToken(secret: string): Promise<string> {
  const expiry = Date.now() + (24 * 60 * 60 * 1000); // 24 hours
  const payload = `authenticated:${expiry}`;
  
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
  
  const signature = await crypto.subtle.sign('HMAC', cryptoKey, payloadData);
  const signatureHex = Array.from(new Uint8Array(signature))
    .map(b => b.toString(16).padStart(2, '0'))
    .join('');
  
  // Format: base64(payload.signature)
  return btoa(`${payload}.${signatureHex}`);
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
      JSON.stringify({ success: false, error: 'Troppe richieste. Riprova tra un minuto.' }),
      { status: 429, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }

  try {
    const { password } = await req.json();
    
    if (!password) {
      console.log('Login attempt: no password provided');
      return new Response(
        JSON.stringify({ success: false, error: 'Password richiesta' }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    const correctPassword = Deno.env.get('APP_ACCESS_PASSWORD');
    const tokenSecret = Deno.env.get('AUTH_TOKEN_SECRET') || correctPassword;
    
    if (!correctPassword) {
      console.error('APP_ACCESS_PASSWORD not configured');
      return new Response(
        JSON.stringify({ success: false, error: 'Configurazione server non valida' }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    if (!tokenSecret) {
      console.error('AUTH_TOKEN_SECRET not configured');
      return new Response(
        JSON.stringify({ success: false, error: 'Configurazione server non valida' }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    if (password === correctPassword) {
      console.log('Login successful');
      const token = await generateToken(tokenSecret);
      
      return new Response(
        JSON.stringify({ success: true, token }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    } else {
      console.log('Login failed: incorrect password');
      return new Response(
        JSON.stringify({ success: false, error: 'Password errata, riprova' }),
        { status: 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }
  } catch (error) {
    console.error('Error in auth-login:', error);
    return new Response(
      JSON.stringify({ success: false, error: 'Errore del server' }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
