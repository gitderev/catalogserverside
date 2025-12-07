import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.39.3';

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
    const adminEmail = Deno.env.get('ADMIN_SUPABASE_EMAIL');
    const adminPassword = Deno.env.get('ADMIN_SUPABASE_PASSWORD');
    const supabaseUrl = Deno.env.get('SUPABASE_URL');
    const supabaseAnonKey = Deno.env.get('SUPABASE_ANON_KEY');
    
    if (!correctPassword) {
      console.error('APP_ACCESS_PASSWORD not configured');
      return new Response(
        JSON.stringify({ success: false, error: 'Configurazione server non valida' }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    if (!adminEmail || !adminPassword) {
      console.error('Admin credentials not configured (ADMIN_SUPABASE_EMAIL or ADMIN_SUPABASE_PASSWORD missing)');
      return new Response(
        JSON.stringify({ success: false, error: 'Configurazione server non valida' }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    if (!supabaseUrl || !supabaseAnonKey) {
      console.error('Supabase credentials not configured');
      return new Response(
        JSON.stringify({ success: false, error: 'Configurazione server non valida' }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Verify the access password first
    if (password !== correctPassword) {
      console.log('Login failed: incorrect password');
      return new Response(
        JSON.stringify({ success: false, error: 'Password errata, riprova' }),
        { status: 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Password is correct - now sign in as the technical admin user
    console.log('Access password correct, signing in as admin user...');
    
    const supabase = createClient(supabaseUrl, supabaseAnonKey);
    
    const { data: authData, error: authError } = await supabase.auth.signInWithPassword({
      email: adminEmail,
      password: adminPassword,
    });

    if (authError || !authData.session) {
      console.error('Admin sign-in failed:', authError?.message);
      return new Response(
        JSON.stringify({ success: false, error: 'Errore interno di autenticazione' }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    console.log('Admin sign-in successful, returning session tokens');
    
    return new Response(
      JSON.stringify({ 
        success: true, 
        access_token: authData.session.access_token,
        refresh_token: authData.session.refresh_token,
        expires_in: authData.session.expires_in,
        expires_at: authData.session.expires_at
      }),
      { headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  } catch (error) {
    console.error('Error in auth-login:', error);
    return new Response(
      JSON.stringify({ success: false, error: 'Errore del server' }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
