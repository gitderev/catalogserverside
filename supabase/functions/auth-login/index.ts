import { serve } from "https://deno.land/std@0.168.0/http/server.ts";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Generate a simple signed token
function generateToken(secret: string): string {
  const expiry = Date.now() + (24 * 60 * 60 * 1000); // 24 hours
  const payload = `authenticated:${expiry}`;
  
  // Create a simple hash-based signature
  const encoder = new TextEncoder();
  const data = encoder.encode(payload + secret);
  let hash = 0;
  for (let i = 0; i < data.length; i++) {
    hash = ((hash << 5) - hash) + data[i];
    hash = hash & hash;
  }
  const signature = Math.abs(hash).toString(36);
  
  return btoa(`${payload}:${signature}`);
}

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
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
    
    if (!correctPassword) {
      console.error('APP_ACCESS_PASSWORD not configured');
      return new Response(
        JSON.stringify({ success: false, error: 'Configurazione server non valida' }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    if (password === correctPassword) {
      console.log('Login successful');
      const token = generateToken(correctPassword);
      
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
