import { serve } from "https://deno.land/std@0.168.0/http/server.ts";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Validate the token
function validateToken(token: string, secret: string): boolean {
  try {
    const decoded = atob(token);
    const parts = decoded.split(':');
    
    if (parts.length !== 3) return false;
    
    const [status, expiryStr, signature] = parts;
    const expiry = parseInt(expiryStr, 10);
    
    // Check expiry
    if (Date.now() > expiry) {
      console.log('Token expired');
      return false;
    }
    
    // Verify signature
    const payload = `${status}:${expiryStr}`;
    const encoder = new TextEncoder();
    const data = encoder.encode(payload + secret);
    let hash = 0;
    for (let i = 0; i < data.length; i++) {
      hash = ((hash << 5) - hash) + data[i];
      hash = hash & hash;
    }
    const expectedSignature = Math.abs(hash).toString(36);
    
    return signature === expectedSignature && status === 'authenticated';
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

  try {
    const { token } = await req.json();
    
    if (!token) {
      return new Response(
        JSON.stringify({ valid: false }),
        { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    const secret = Deno.env.get('APP_ACCESS_PASSWORD');
    
    if (!secret) {
      console.error('APP_ACCESS_PASSWORD not configured');
      return new Response(
        JSON.stringify({ valid: false }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    const isValid = validateToken(token, secret);
    
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
