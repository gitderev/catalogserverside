import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";

/**
 * send-sync-notification - Sends email via Brevo Transactional API
 * 
 * Called after a sync run completes (or times out).
 * Checks notification_mode in sync_config to decide if email should be sent.
 * If email send fails, logs WARN but does NOT fail the run.
 * 
 * Generates signed URLs for real latest/ paths only.
 */

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

function errMsg(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

const SENDER_EMAIL = 'contact@alterside.com';
const RECIPIENT_EMAIL = 'contact@alterside.com';
const BREVO_API_URL = 'https://api.brevo.com/v3/smtp/email';

serve(async (req) => {
  if (req.method === 'OPTIONS') return new Response(null, { headers: corsHeaders });

  try {
    const supabaseUrl = Deno.env.get('SUPABASE_URL')!;
    const supabaseServiceKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
    const brevoApiKey = Deno.env.get('BREVO_API_KEY');
    const supabase = createClient(supabaseUrl, supabaseServiceKey);

    const body = await req.json().catch(() => ({}));
    const runId = body.run_id as string;
    const status = body.status as string;

    if (!runId) {
      return new Response(
        JSON.stringify({ status: 'error', message: 'run_id required' }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    console.log(`[send-sync-notification] Processing run ${runId}, status: ${status}`);

    // Load config for notification_mode
    const { data: config } = await supabase
      .from('sync_config')
      .select('notification_mode, notify_on_warning')
      .eq('id', 1)
      .single();

    const notificationMode = config?.notification_mode || 'never';
    const notifyOnWarning = config?.notify_on_warning ?? true;

    const shouldSend = checkShouldSend(notificationMode, status, notifyOnWarning);
    
    if (!shouldSend) {
      console.log(`[send-sync-notification] Skipped: mode=${notificationMode}, status=${status}`);
      return new Response(
        JSON.stringify({ status: 'skipped', reason: `notification_mode=${notificationMode}` }),
        { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    if (!brevoApiKey) {
      console.warn('[send-sync-notification] BREVO_API_KEY not configured, skipping email');
      await supabase.from('sync_events').insert({
        run_id: runId,
        level: 'WARN',
        step: 'notification',
        message: 'Email non inviata: BREVO_API_KEY non configurata'
      });
      return new Response(
        JSON.stringify({ status: 'skipped', reason: 'no_api_key' }),
        { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Load run details
    const { data: run } = await supabase
      .from('sync_runs')
      .select('*')
      .eq('id', runId)
      .single();

    if (!run) {
      return new Response(
        JSON.stringify({ status: 'error', message: 'Run not found' }),
        { status: 404, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Load events for this run
    const { data: events } = await supabase
      .from('sync_events')
      .select('level, step, message')
      .eq('run_id', runId)
      .order('created_at', { ascending: true })
      .limit(50);

    // Generate signed URLs for real latest/ file paths
    const fileLinks = await generateFileLinks(supabase);

    // Build email
    const subject = buildSubject(run.status, run.attempt);
    const htmlContent = buildEmailHtml(run, events || [], fileLinks);

    // Send via Brevo (non-blocking: failure = WARN, not fatal)
    try {
      const brevoResp = await fetch(BREVO_API_URL, {
        method: 'POST',
        headers: {
          'accept': 'application/json',
          'api-key': brevoApiKey,
          'content-type': 'application/json'
        },
        body: JSON.stringify({
          sender: { name: 'Alterside Sync', email: SENDER_EMAIL },
          to: [{ email: RECIPIENT_EMAIL }],
          subject,
          htmlContent
        })
      });

      if (!brevoResp.ok) {
        const errText = await brevoResp.text();
        throw new Error(`Brevo API ${brevoResp.status}: ${errText}`);
      }

      console.log(`[send-sync-notification] Email sent for run ${runId}`);
      
      await supabase.from('sync_events').insert({
        run_id: runId,
        level: 'INFO',
        step: 'notification',
        message: `Email inviata a ${RECIPIENT_EMAIL}`
      });

    } catch (emailError: unknown) {
      console.warn(`[send-sync-notification] Email send failed (non-blocking):`, errMsg(emailError));
      
      await supabase.from('sync_events').insert({
        run_id: runId,
        level: 'WARN',
        step: 'notification',
        message: `Invio email fallito: ${errMsg(emailError)}`
      });
    }

    return new Response(
      JSON.stringify({ status: 'ok' }),
      { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );

  } catch (error: unknown) {
    console.error('[send-sync-notification] Error:', errMsg(error));
    return new Response(
      JSON.stringify({ status: 'error', message: errMsg(error) }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});

function checkShouldSend(mode: string, status: string, notifyOnWarning: boolean): boolean {
  if (mode === 'never') return false;
  if (mode === 'always') return true;
  if (mode === 'only_on_problem') {
    if (['failed', 'timeout'].includes(status)) return true;
    if (status === 'success_with_warning' && notifyOnWarning) return true;
    return false;
  }
  return false;
}

function buildSubject(status: string, attempt: number): string {
  const statusMap: Record<string, string> = {
    'success': '✅ Sync completata',
    'success_with_warning': '⚠️ Sync completata con avvisi',
    'failed': '❌ Sync fallita',
    'timeout': '⏱️ Sync timeout'
  };
  const prefix = statusMap[status] || `Sync ${status}`;
  const attemptStr = attempt > 1 ? ` (tentativo ${attempt})` : '';
  return `${prefix}${attemptStr} - Alterside Catalog`;
}

function buildEmailHtml(
  run: Record<string, unknown>,
  events: Array<Record<string, unknown>>,
  fileLinks: Array<{ name: string; url: string | null; path: string }>
): string {
  const statusColors: Record<string, string> = {
    'success': '#22c55e',
    'success_with_warning': '#f59e0b',
    'failed': '#ef4444',
    'timeout': '#f97316'
  };
  const color = statusColors[run.status as string] || '#64748b';
  
  const startedAt = run.started_at ? new Date(run.started_at as string).toLocaleString('it-IT', { timeZone: 'Europe/Rome' }) : '-';
  const finishedAt = run.finished_at ? new Date(run.finished_at as string).toLocaleString('it-IT', { timeZone: 'Europe/Rome' }) : '-';
  const runtimeSec = run.runtime_ms ? Math.round((run.runtime_ms as number) / 1000) : '-';

  const metrics = run.metrics as Record<string, unknown> || {};
  const warningCount = (run.warning_count as number) || 0;

  let eventsHtml = '';
  if (events.length > 0) {
    const eventRows = events.map(e => {
      const levelColor = e.level === 'ERROR' ? '#ef4444' : e.level === 'WARN' ? '#f59e0b' : '#64748b';
      return `<tr><td style="color:${levelColor};font-weight:600;">${e.level}</td><td>${e.step || '-'}</td><td>${e.message}</td></tr>`;
    }).join('');
    eventsHtml = `
      <h3>Eventi</h3>
      <table style="width:100%;border-collapse:collapse;font-size:13px;">
        <tr style="background:#f1f5f9;"><th style="text-align:left;padding:6px;">Livello</th><th style="text-align:left;padding:6px;">Step</th><th style="text-align:left;padding:6px;">Messaggio</th></tr>
        ${eventRows}
      </table>`;
  }

  // Build files section - include valid links, mention missing ones
  let filesHtml = '';
  if (fileLinks.length > 0) {
    const items = fileLinks.map(f => {
      if (f.url) {
        return `<li><a href="${f.url}">${f.name}</a> (link valido 7 giorni)</li>`;
      } else {
        return `<li>${f.name} — <em>link non disponibile, file in: ${f.path}</em></li>`;
      }
    }).join('');
    filesHtml = `
      <h3>File Export</h3>
      <ul>${items}</ul>`;
  }

  // Recommended actions for failures
  let actionsHtml = '';
  if (['failed', 'timeout'].includes(run.status as string)) {
    actionsHtml = `
      <h3 style="color:#ef4444;">Azioni consigliate</h3>
      <ol>
        <li>Controlla i log degli eventi sopra per identificare lo step fallito</li>
        <li>Verifica le credenziali FTP/SFTP se l'errore è di connessione</li>
        <li>Se il problema è un timeout, verifica che i file sorgente non siano troppo grandi</li>
        <li>Riattiva la sincronizzazione automatica dalla dashboard dopo aver risolto il problema</li>
      </ol>`;
  }

  return `
    <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;">
      <h2 style="color:${color};">Sincronizzazione ${run.status}</h2>
      <table style="width:100%;font-size:14px;">
        <tr><td><strong>Avviata:</strong></td><td>${startedAt}</td></tr>
        <tr><td><strong>Completata:</strong></td><td>${finishedAt}</td></tr>
        <tr><td><strong>Durata:</strong></td><td>${runtimeSec}s</td></tr>
        <tr><td><strong>Tentativo:</strong></td><td>${run.attempt || 1}</td></tr>
        <tr><td><strong>Avvisi:</strong></td><td>${warningCount}</td></tr>
        <tr><td><strong>Prodotti elaborati:</strong></td><td>${metrics.products_processed || 0}</td></tr>
      </table>
      ${run.error_message ? `<p style="color:#ef4444;"><strong>Errore:</strong> ${run.error_message}</p>` : ''}
      ${eventsHtml}
      ${filesHtml}
      ${actionsHtml}
    </div>`;
}

async function generateFileLinks(
  supabase: ReturnType<typeof createClient>
): Promise<Array<{ name: string; url: string | null; path: string }>> {
  // Exact real paths in latest/
  const files = [
    { name: 'Catalogo EAN (XLSX)', path: 'latest/catalogo_ean.xlsx' },
    { name: 'Amazon Listing Loader', path: 'latest/amazon_listing_loader.xlsm' },
    { name: 'Amazon Price Inventory', path: 'latest/amazon_price_inventory.txt' },
    { name: 'Export Mediaworld', path: 'latest/Export Mediaworld.csv' },
    { name: 'Export ePrice', path: 'latest/Export ePrice.csv' }
  ];

  const results = [];
  for (const file of files) {
    try {
      const { data } = await supabase.storage
        .from('exports')
        .createSignedUrl(file.path, 7 * 24 * 60 * 60); // 7 days
      
      if (data?.signedUrl) {
        results.push({ name: file.name, url: data.signedUrl, path: file.path });
      } else {
        console.warn(`[send-sync-notification] WARN: No signed URL for ${file.path}`);
        results.push({ name: file.name, url: null, path: file.path });
      }
    } catch (e: unknown) {
      console.warn(`[send-sync-notification] WARN: Failed to generate signed URL for ${file.path}: ${errMsg(e)}`);
      results.push({ name: file.name, url: null, path: file.path });
    }
  }
  return results;
}
