import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

serve(async (req) => {
  // Handle CORS preflight
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  if (req.method !== 'POST') {
    return new Response(
      JSON.stringify({ status: 'error', message: 'Method not allowed' }),
      { status: 405, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }

  try {
    const supabaseUrl = Deno.env.get('SUPABASE_URL')!;
    const supabaseAnonKey = Deno.env.get('SUPABASE_ANON_KEY')!;
    const supabaseServiceKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;

    // Validate JWT
    const authHeader = req.headers.get('Authorization');
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return new Response(
        JSON.stringify({ status: 'error', message: 'Autenticazione richiesta' }),
        { status: 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    const jwt = authHeader.replace('Bearer ', '');
    const userClient = createClient(supabaseUrl, supabaseAnonKey, {
      global: { headers: { Authorization: `Bearer ${jwt}` } }
    });

    const { data: userData, error: userError } = await userClient.auth.getUser();
    if (userError || !userData?.user) {
      return new Response(
        JSON.stringify({ status: 'error', message: 'Token non valido' }),
        { status: 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    console.log(`[stop-sync] Request from user: ${userData.user.id}`);

    // Parse request body
    const body = await req.json().catch(() => ({}));
    const runId = body.run_id as string;

    if (!runId) {
      return new Response(
        JSON.stringify({ status: 'error', message: 'Campo "run_id" obbligatorio' }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Use service role client for updates
    const supabase = createClient(supabaseUrl, supabaseServiceKey);

    // Find the run
    const { data: run, error: runError } = await supabase
      .from('sync_runs')
      .select('id, status')
      .eq('id', runId)
      .single();

    if (runError || !run) {
      console.log(`[stop-sync] Run not found: ${runId}`);
      return new Response(
        JSON.stringify({ status: 'error', message: 'Sincronizzazione non trovata' }),
        { status: 404, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    if (run.status !== 'running') {
      console.log(`[stop-sync] Run not running: ${runId}, status: ${run.status}`);
      return new Response(
        JSON.stringify({ 
          status: 'error', 
          message: `La sincronizzazione non è in esecuzione (stato attuale: ${run.status})` 
        }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Set cancel flag
    const { error: updateError } = await supabase
      .from('sync_runs')
      .update({ 
        cancel_requested: true,
        cancelled_by_user: true
      })
      .eq('id', runId);

    if (updateError) {
      console.error('[stop-sync] Error updating run:', updateError);
      return new Response(
        JSON.stringify({ status: 'error', message: 'Errore durante la richiesta di interruzione' }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    console.log(`[stop-sync] Cancel requested for run: ${runId}`);

    return new Response(
      JSON.stringify({ 
        status: 'ok', 
        message: 'Richiesta di interruzione inviata. La sincronizzazione verrà interrotta al prossimo step.',
        run_id: runId
      }),
      { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );

  } catch (error: any) {
    console.error('[stop-sync] Unexpected error:', error);
    
    return new Response(
      JSON.stringify({ status: 'error', message: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});