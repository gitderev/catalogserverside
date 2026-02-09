import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

/**
 * force-reset-sync
 * 
 * Forza lo stato di una run (o di tutte le run in running) a "failed"
 * e imposta finished_at all'orario corrente.
 * 
 * Valori di stato possibili nella tabella sync_runs:
 * - running: sincronizzazione in corso
 * - success: sincronizzazione completata con successo
 * - failed: sincronizzazione fallita (per errore, timeout, o reset manuale)
 * - timeout: sincronizzazione interrotta per superamento tempo massimo
 * - skipped: sincronizzazione saltata (es. frequenza non ancora raggiunta)
 */

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

    console.log(`[force-reset-sync] Request from user: ${userData.user.id}`);

    // Parse request body
    const body = await req.json().catch(() => ({}));
    const runId = body.run_id as string | undefined;
    const resetAll = body.reset_all === true;

    // Use service role client for updates
    const supabase = createClient(supabaseUrl, supabaseServiceKey);

    const now = new Date().toISOString();
    let affectedRuns: string[] = [];

    if (runId) {
      // Reset specific run
      const { data: run, error: runError } = await supabase
        .from('sync_runs')
        .select('id, status, started_at')
        .eq('id', runId)
        .single();

      if (runError || !run) {
        console.log(`[force-reset-sync] Run not found: ${runId}`);
        return new Response(
          JSON.stringify({ status: 'error', message: 'Sincronizzazione non trovata' }),
          { status: 404, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      if (run.status !== 'running') {
        console.log(`[force-reset-sync] Run already completed: ${runId}, status: ${run.status}`);
        return new Response(
          JSON.stringify({ 
            status: 'ok', 
            message: `La sincronizzazione è già terminata (stato: ${run.status})`,
            already_completed: true
          }),
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      // Force update to failed
      const { error: updateError } = await supabase
        .from('sync_runs')
        .update({ 
          status: 'failed',
          finished_at: now,
          error_message: 'Reset forzato manualmente',
          error_details: { 
            forced_reset: true,
            reset_by: userData.user.id,
            reset_at: now
          },
          cancel_requested: true,
          cancelled_by_user: true
        })
        .eq('id', runId);

      if (updateError) {
        console.error('[force-reset-sync] Error updating run:', updateError);
        return new Response(
          JSON.stringify({ status: 'error', message: 'Errore durante il reset' }),
          { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      affectedRuns.push(runId);
      console.log(`[force-reset-sync] Run reset: ${runId}`);

    } else if (resetAll) {
      // Reset all running runs
      const { data: runningRuns, error: fetchError } = await supabase
        .from('sync_runs')
        .select('id')
        .eq('status', 'running');

      if (fetchError) {
        console.error('[force-reset-sync] Error fetching running runs:', fetchError);
        return new Response(
          JSON.stringify({ status: 'error', message: 'Errore durante la ricerca delle run attive' }),
          { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      if (!runningRuns || runningRuns.length === 0) {
        return new Response(
          JSON.stringify({ 
            status: 'ok', 
            message: 'Nessuna sincronizzazione attiva da resettare',
            affected_count: 0
          }),
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      // Update all running runs
      const { error: updateError } = await supabase
        .from('sync_runs')
        .update({ 
          status: 'failed',
          finished_at: now,
          error_message: 'Reset forzato manualmente (reset globale)',
          error_details: { 
            forced_reset: true,
            reset_all: true,
            reset_by: userData.user.id,
            reset_at: now
          },
          cancel_requested: true,
          cancelled_by_user: true
        })
        .eq('status', 'running');

      if (updateError) {
        console.error('[force-reset-sync] Error updating runs:', updateError);
        return new Response(
          JSON.stringify({ status: 'error', message: 'Errore durante il reset' }),
          { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      affectedRuns = runningRuns.map(r => r.id);
      console.log(`[force-reset-sync] Reset ${affectedRuns.length} runs`);

    } else {
      return new Response(
        JSON.stringify({ 
          status: 'error', 
          message: 'Specificare run_id o reset_all=true' 
        }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    return new Response(
      JSON.stringify({ 
        status: 'ok', 
        message: `Reset completato per ${affectedRuns.length} sincronizzazione/i`,
        affected_runs: affectedRuns,
        affected_count: affectedRuns.length
      }),
      { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );

  } catch (error: unknown) {
    console.error('[force-reset-sync] Unexpected error:', error);
    
    return new Response(
      JSON.stringify({ status: 'error', message: error instanceof Error ? error.message : String(error) }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
