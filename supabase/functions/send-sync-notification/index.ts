import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";
import { getExpectedSteps } from "../_shared/expectedSteps.ts";

/**
 * send-sync-notification - Sends email via SMTP
 * 
 * SMTP configuration via Supabase Edge Functions secrets:
 *   SMTP_HOST     - SMTP server hostname
 *   SMTP_PORT     - SMTP server port (e.g. 587, 465, 25)
 *   SMTP_USER     - SMTP authentication username
 *   SMTP_PASS     - SMTP authentication password
 *   SMTP_FROM     - Sender email address
 *   SMTP_TO       - Recipient email(s), comma-separated
 *   SMTP_SECURE   - "true" for implicit TLS (port 465), "false" for STARTTLS
 * 
 * Non-blocking: failures produce ERROR log but don't change run status.
 * Returns { status: "completed" } or { status: "failed", error: "..." }.
 */

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

function errMsg(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

serve(async (req) => {
  if (req.method === 'OPTIONS') return new Response(null, { headers: corsHeaders });

  try {
    const supabaseUrl = Deno.env.get('SUPABASE_URL')!;
    const supabaseServiceKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
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

    // ========== Check notification mode ==========
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
        JSON.stringify({ status: 'completed', reason: `notification_mode=${notificationMode}` }),
        { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // ========== Validate SMTP configuration ==========
    const smtpHost = Deno.env.get('SMTP_HOST');
    const smtpPort = Deno.env.get('SMTP_PORT');
    const smtpUser = Deno.env.get('SMTP_USER');
    const smtpPass = Deno.env.get('SMTP_PASS');
    const smtpFrom = Deno.env.get('SMTP_FROM');
    const smtpTo = Deno.env.get('SMTP_TO');
    const smtpSecure = Deno.env.get('SMTP_SECURE');

    const missingEnv: string[] = [];
    if (!smtpHost) missingEnv.push('SMTP_HOST');
    if (!smtpPort) missingEnv.push('SMTP_PORT');
    if (!smtpUser) missingEnv.push('SMTP_USER');
    if (!smtpPass) missingEnv.push('SMTP_PASS');
    if (!smtpFrom) missingEnv.push('SMTP_FROM');
    if (!smtpTo) missingEnv.push('SMTP_TO');

    if (missingEnv.length > 0) {
      console.error(`[send-sync-notification] SMTP misconfigured: missing ${missingEnv.join(', ')}`);
      try {
        await supabase.rpc('log_sync_event', {
          p_run_id: runId,
          p_level: 'ERROR',
          p_message: 'notification_failed',
          p_details: { step: 'notification', reason: 'SMTP misconfigured', missing_env: missingEnv }
        });
      } catch (_) {}
      return new Response(
        JSON.stringify({ status: 'failed', error: 'SMTP misconfigured', missing_env: missingEnv }),
        { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // Log config (no credentials)
    const recipients = smtpTo!.split(',').map(s => s.trim()).filter(Boolean);
    console.log(`[send-sync-notification] SMTP config: host=${smtpHost}, port=${smtpPort}, from=${smtpFrom}, to_count=${recipients.length}`);

    // ========== Load run details ==========
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

    // ========== Build email content ==========
    const subject = buildSubject(runId, run.status, run.attempt);
    const textBody = await buildTextBody(run, supabase);

    // ========== Send via SMTP using nodemailer ==========
    try {
      const nodemailer = await import("npm:nodemailer@6.9.10");
      
      const useSecure = smtpSecure === 'true';
      const port = parseInt(smtpPort!, 10);
      
      const transporter = nodemailer.default.createTransport({
        host: smtpHost,
        port,
        secure: useSecure,
        auth: {
          user: smtpUser,
          pass: smtpPass,
        },
        // STARTTLS is used automatically when secure=false and port != 465
        tls: {
          rejectUnauthorized: false, // Allow self-signed certs
        },
        connectionTimeout: 15000,
        greetingTimeout: 10000,
        socketTimeout: 15000,
      });

      const info = await transporter.sendMail({
        from: smtpFrom,
        to: recipients.join(', '),
        subject,
        text: textBody,
      });

      console.log(`[send-sync-notification] Email sent: messageId=${info.messageId}`);
      try {
        await supabase.rpc('log_sync_event', {
          p_run_id: runId,
          p_level: 'INFO',
          p_message: 'notification_completed',
          p_details: { step: 'notification', provider: 'smtp', message_id: info.messageId, to_count: recipients.length }
        });
      } catch (_) {}

      return new Response(
        JSON.stringify({ status: 'completed' }),
        { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    } catch (smtpError: unknown) {
      const smtpErrMsg = errMsg(smtpError);
      console.error(`[send-sync-notification] SMTP send failed: ${smtpErrMsg}`);
      
      // Retry once after 2s
      try {
        await new Promise(r => setTimeout(r, 2000));
        const nodemailer = await import("npm:nodemailer@6.9.10");
        const useSecure = smtpSecure === 'true';
        const port = parseInt(smtpPort!, 10);
        const transporter = nodemailer.default.createTransport({
          host: smtpHost, port, secure: useSecure,
          auth: { user: smtpUser, pass: smtpPass },
          tls: { rejectUnauthorized: false },
          connectionTimeout: 15000, greetingTimeout: 10000, socketTimeout: 15000,
        });
        const info = await transporter.sendMail({
          from: smtpFrom, to: recipients.join(', '), subject, text: textBody,
        });
        console.log(`[send-sync-notification] Email sent on retry: messageId=${info.messageId}`);
        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'INFO', p_message: 'notification_completed',
            p_details: { step: 'notification', provider: 'smtp', message_id: info.messageId, retry: true }
          });
        } catch (_) {}
        return new Response(
          JSON.stringify({ status: 'completed' }),
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      } catch (retryError: unknown) {
        const retryErrMsg = errMsg(retryError);
        console.error(`[send-sync-notification] SMTP retry also failed: ${retryErrMsg}`);
        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'ERROR', p_message: 'notification_failed',
            p_details: { step: 'notification', provider: 'smtp', error: retryErrMsg, attempts: 2 }
          });
        } catch (_) {}
        return new Response(
          JSON.stringify({ status: 'failed', error: retryErrMsg }),
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }
    }

  } catch (error: unknown) {
    console.error('[send-sync-notification] Error:', errMsg(error));
    return new Response(
      JSON.stringify({ status: 'failed', error: errMsg(error) }),
      { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
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

function buildSubject(runId: string, status: string, attempt: number): string {
  const statusMap: Record<string, string> = {
    'success': 'SUCCESS',
    'success_with_warning': 'WARNING',
    'failed': 'FAILED',
    'timeout': 'TIMEOUT'
  };
  const tag = statusMap[status] || status.toUpperCase();
  return `[CATALOG SYNC] run ${runId.substring(0, 8)} - ${tag}`;
}

async function buildTextBody(run: Record<string, unknown>, supabase: ReturnType<typeof createClient>): Promise<string> {
  const metrics = run.metrics as Record<string, unknown> || {};
  const steps = run.steps as Record<string, unknown> || {};
  const warningCount = (run.warning_count as number) || 0;
  const runtimeSec = run.runtime_ms ? Math.round((run.runtime_ms as number) / 1000) : 0;
  const currentStep = (steps as Record<string, unknown>).current_step as string || 'N/A';

  // Use shared expected steps (single source of truth, no manual/cron divergence)
  const expectedSteps = getExpectedSteps(run.trigger_type as string);
  
  const missingSteps = expectedSteps.filter(s => {
    const st = steps[s] as Record<string, unknown> | undefined;
    return !st || !st.status;
  });

  // Derive failing step
  let failingStep = 'N/A';
  for (const s of expectedSteps) {
    const st = steps[s] as Record<string, unknown> | undefined;
    if (st?.status === 'failed') { failingStep = s; break; }
  }

  const lines: string[] = [
    `CATALOG SYNC REPORT`,
    `====================`,
    ``,
    `Run ID:       ${run.id}`,
    `Trigger:      ${run.trigger_type}`,
    `Status:       ${run.status}`,
    `Attempt:      ${run.attempt || 1}`,
    `Runtime:      ${runtimeSec}s`,
    `Warnings:     ${warningCount}`,
    `Current Step: ${currentStep}`,
    `Failing Step: ${failingStep}`,
    `Started:      ${run.started_at}`,
    `Finished:     ${run.finished_at || 'N/A'}`,
  ];

  if (run.error_message) {
    lines.push(`Error:        ${String(run.error_message).substring(0, 500)}`);
  }

  lines.push('');

  // Step timeline
  lines.push(`STEP TIMELINE`);
  lines.push(`-------------`);
  for (const s of expectedSteps) {
    const st = steps[s] as Record<string, unknown> | undefined;
    const status = st?.status || 'pending';
    const dur = st?.duration_ms ? `${Math.round(st.duration_ms as number)}ms` : '-';
    const rows = st?.rows_written || st?.rows || '';
    lines.push(`  ${s.padEnd(22)} ${String(status).padEnd(12)} ${dur}${rows ? ` (${rows} rows)` : ''}`);
  }
  lines.push('');

  // Metrics
  const productsTotal = metrics.products_total || metrics.productCount || 0;
  const productsProcessed = metrics.products_processed || 0;
  if (productsTotal || productsProcessed) {
    lines.push(`Products total:     ${productsTotal}`);
    lines.push(`Products processed: ${productsProcessed}`);
  }

  // Export counts from step states
  for (const stepName of ['export_ean', 'export_ean_xlsx', 'export_amazon', 'export_mediaworld', 'export_eprice']) {
    const st = steps[stepName] as Record<string, unknown> | undefined;
    if (st?.rows || st?.rows_written) {
      lines.push(`${stepName}: ${st.rows_written || st.rows} rows`);
    }
  }

  if (missingSteps.length > 0) {
    lines.push('');
    lines.push(`Missing steps: ${missingSteps.join(', ')}`);
  }

  if (warningCount > 0) {
    lines.push('');
    lines.push(`⚠️ ${warningCount} warning(s) recorded during this run.`);
  }

  // Last 30 events extract
  try {
    const { data: evts } = await supabase
      .from('sync_events')
      .select('created_at, level, message, step, details')
      .eq('run_id', run.id as string)
      .order('created_at', { ascending: false })
      .limit(30);
    
    if (evts && evts.length > 0) {
      lines.push('');
      lines.push(`RECENT EVENTS (last ${evts.length})`);
      lines.push(`---------------------------------`);
      for (const e of evts) {
        const ts = new Date(e.created_at).toISOString().substring(11, 19);
        const det = e.details as Record<string, unknown> | null;
        let detStr = '';
        if (det) {
          // Show only relevant diagnostic fields, truncated
          const pick: string[] = [];
          for (const k of ['stage', 'duration_ms', 'http_status', 'code', 'body_snippet', 'retry_attempt', 'heap_mb', 'rows', 'error']) {
            if (det[k] !== undefined) {
              const v = String(det[k]);
              pick.push(`${k}=${v.substring(0, 120)}`);
            }
          }
          if (pick.length) detStr = ` | ${pick.join(', ')}`;
        }
        lines.push(`  ${ts} ${e.level} ${e.message}${e.step ? ` [${e.step}]` : ''}${detStr}`);
      }
    }
  } catch { /* non-blocking */ }

  return lines.join('\n');
}
