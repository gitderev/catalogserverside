import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Maximum runtime: 30 minutes in milliseconds
const MAX_RUNTIME_MS = 30 * 60 * 1000;

interface SyncConfig {
  enabled: boolean;
  frequency_minutes: number;
  daily_time: string | null;
  max_retries: number;
  retry_delay_minutes: number;
}

interface SyncRun {
  id: string;
  started_at: string;
  finished_at: string | null;
  status: string;
  trigger_type: string;
  attempt: number;
  runtime_ms: number | null;
  error_message: string | null;
  error_details: any;
  steps: any;
  metrics: any;
  cancel_requested: boolean;
  cancelled_by_user: boolean;
}

interface StepResult {
  status: 'success' | 'failed' | 'skipped';
  duration_ms: number;
  [key: string]: any;
}

interface PipelineMetrics {
  products_total: number;
  products_processed: number;
  products_ean_mapped: number;
  products_ean_missing: number;
  products_ean_invalid: number;
  products_after_override: number;
  mediaworld_export_rows: number;
  mediaworld_export_skipped: number;
  eprice_export_rows: number;
  eprice_export_skipped: number;
  exported_files_count: number;
  sftp_uploaded_files: number;
  warnings: string[];
}

// Initialize empty metrics
function initMetrics(): PipelineMetrics {
  return {
    products_total: 0,
    products_processed: 0,
    products_ean_mapped: 0,
    products_ean_missing: 0,
    products_ean_invalid: 0,
    products_after_override: 0,
    mediaworld_export_rows: 0,
    mediaworld_export_skipped: 0,
    eprice_export_rows: 0,
    eprice_export_skipped: 0,
    exported_files_count: 0,
    sftp_uploaded_files: 0,
    warnings: []
  };
}

// Check if timeout exceeded
function isTimeoutExceeded(startTime: number): boolean {
  return Date.now() - startTime > MAX_RUNTIME_MS;
}

// Update run record in database
async function updateRun(
  supabase: any,
  runId: string,
  updates: Partial<SyncRun>
): Promise<void> {
  const { error } = await supabase
    .from('sync_runs')
    .update(updates)
    .eq('id', runId);
  
  if (error) {
    console.error('[run-full-sync] Error updating run:', error);
  }
}

// Check if cancellation was requested
async function isCancelRequested(supabase: any, runId: string): Promise<boolean> {
  const { data, error } = await supabase
    .from('sync_runs')
    .select('cancel_requested')
    .eq('id', runId)
    .single();
  
  if (error) {
    console.error('[run-full-sync] Error checking cancel status:', error);
    return false;
  }
  
  return data?.cancel_requested === true;
}

// Finalize run with success or failure
async function finalizeRun(
  supabase: any,
  runId: string,
  status: 'success' | 'failed' | 'timeout',
  startTime: number,
  steps: Record<string, StepResult>,
  metrics: PipelineMetrics,
  errorMessage?: string,
  errorDetails?: any,
  cancelledByUser: boolean = false
): Promise<void> {
  const finishedAt = new Date().toISOString();
  const runtimeMs = Date.now() - startTime;
  
  await updateRun(supabase, runId, {
    status,
    finished_at: finishedAt,
    runtime_ms: runtimeMs,
    steps,
    metrics,
    error_message: errorMessage || null,
    error_details: errorDetails || null,
    cancelled_by_user: cancelledByUser
  });
}

// =====================================================================
// PRICING UTILITIES (ported from src/utils/pricing.ts)
// =====================================================================

function parseEuroLike(input: unknown): number {
  if (typeof input === 'number' && isFinite(input)) return input;
  let s = String(input ?? '').trim();
  s = s.replace(/[^\d.,\s%\-]/g, '').trim();
  s = s.split(/\s+/)[0] ?? '';
  s = s.replace(/%/g, '').trim();
  if (!s) return NaN;
  if (s.includes('.') && s.includes(',')) s = s.replace(/\./g, '').replace(',', '.');
  else s = s.replace(',', '.');
  const n = parseFloat(s);
  return Number.isFinite(n) ? n : NaN;
}

function toCents(x: unknown, fallback = 0): number {
  const n = parseEuroLike(x);
  return Number.isFinite(n) ? Math.round(n * 100) : Math.round(fallback * 100);
}

function toComma99Cents(cents: number): number {
  if (cents % 100 === 99) return cents;
  const euros = Math.floor(cents / 100);
  let target = euros * 100 + 99;
  if (target < cents) {
    target = (euros + 1) * 100 + 99;
  }
  return target;
}

function formatCents(cents: number): string {
  const euros = Math.floor(cents / 100);
  const centsPart = cents % 100;
  return `${euros},${centsPart.toString().padStart(2, '0')}`;
}

// =====================================================================
// EAN UTILITIES (ported from src/utils/ean.ts)
// =====================================================================

interface EANResult {
  ok: boolean;
  value?: string;
  reason?: string;
  original?: string;
}

function normalizeEAN(raw: unknown): EANResult {
  const original = (raw ?? '').toString().trim();
  if (!original) return { ok: false, reason: 'EAN mancante', original };
  const compact = original.replace(/[\s-]+/g, '');
  if (!/^\d+$/.test(compact)) {
    return { ok: false, reason: 'EAN contiene caratteri non numerici', original };
  }
  if (compact.length === 12) {
    return { ok: true, value: '0' + compact, original, reason: 'padded_12_to_13' };
  }
  if (compact.length === 13) {
    return { ok: true, value: compact, original, reason: 'valid_13' };
  }
  if (compact.length === 14) {
    if (compact.startsWith('0')) {
      return { ok: true, value: compact.substring(1), original, reason: 'trimmed_14_to_13' };
    } else {
      return { ok: true, value: compact, original, reason: 'valid_14' };
    }
  }
  return { ok: false, reason: `EAN lunghezza ${compact.length} non valida`, original };
}

// =====================================================================
// FILE PARSING UTILITIES
// =====================================================================

function parseTabDelimited(text: string): any[] {
  const lines = text.split('\n');
  if (lines.length < 2) return [];
  
  const headers = lines[0].split('\t').map(h => h.trim());
  const data: any[] = [];
  
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    
    const values = line.split('\t');
    const row: any = {};
    
    for (let j = 0; j < headers.length; j++) {
      row[headers[j]] = values[j]?.trim() ?? '';
    }
    
    data.push(row);
  }
  
  return data;
}

function parseDistributorPrice(raw: any): number {
  if (raw === null || raw === undefined) return 0;
  let value = String(raw).trim();
  if (value === '') return 0;
  value = value.replace(',', '.');
  if (value.startsWith('.')) {
    value = '0' + value;
  }
  const num = parseFloat(value);
  if (isNaN(num)) return 0;
  return num;
}

// =====================================================================
// MAIN SERVER HANDLER
// =====================================================================

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

  const supabaseUrl = Deno.env.get('SUPABASE_URL')!;
  const supabaseServiceKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
  const supabaseAnonKey = Deno.env.get('SUPABASE_ANON_KEY')!;

  try {
    // Parse request body
    const body = await req.json().catch(() => ({}));
    const trigger = body.trigger as string;

    if (!trigger || !['cron', 'manual'].includes(trigger)) {
      return new Response(
        JSON.stringify({ status: 'error', message: 'Campo "trigger" deve essere "cron" o "manual"' }),
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    console.log(`[run-full-sync] Request received with trigger: ${trigger}`);

    // Create Supabase client based on trigger type
    let supabase: any;
    let userId: string | null = null;

    if (trigger === 'manual') {
      // For manual triggers, validate JWT
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

      userId = userData.user.id;
      console.log(`[run-full-sync] Manual trigger by user: ${userId}`);
    }

    // Use service role client for all operations
    supabase = createClient(supabaseUrl, supabaseServiceKey);

    // Check for existing running job
    const { data: runningJobs, error: runningError } = await supabase
      .from('sync_runs')
      .select('id, started_at')
      .eq('status', 'running')
      .limit(1);

    if (runningError) {
      console.error('[run-full-sync] Error checking running jobs:', runningError);
      return new Response(
        JSON.stringify({ status: 'error', message: 'Errore verifica job in esecuzione' }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    if (runningJobs && runningJobs.length > 0) {
      console.log(`[run-full-sync] Job already running: ${runningJobs[0].id}`);
      return new Response(
        JSON.stringify({ 
          status: 'error', 
          message: 'Una sincronizzazione è già in corso',
          running_job_id: runningJobs[0].id 
        }),
        { status: 409, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    // For cron triggers, check if sync is enabled and if it's time to run
    if (trigger === 'cron') {
      const { data: config, error: configError } = await supabase
        .from('sync_config')
        .select('*')
        .eq('id', 1)
        .single();

      if (configError || !config) {
        console.log('[run-full-sync] Config not found or error');
        return new Response(
          JSON.stringify({ status: 'skipped', message: 'Configurazione non trovata' }),
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      if (!config.enabled) {
        console.log('[run-full-sync] Sync is disabled');
        return new Response(
          JSON.stringify({ status: 'skipped', message: 'Sincronizzazione automatica disattivata' }),
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      // Check last cron job to determine if we should run
      const { data: lastCronJobs, error: lastCronError } = await supabase
        .from('sync_runs')
        .select('*')
        .eq('trigger_type', 'cron')
        .order('started_at', { ascending: false })
        .limit(1);

      if (!lastCronError && lastCronJobs && lastCronJobs.length > 0) {
        const lastJob = lastCronJobs[0];
        const now = Date.now();
        const lastStarted = new Date(lastJob.started_at).getTime();
        const lastFinished = lastJob.finished_at ? new Date(lastJob.finished_at).getTime() : null;
        const frequencyMs = config.frequency_minutes * 60 * 1000;
        const retryDelayMs = config.retry_delay_minutes * 60 * 1000;

        // Check if we should retry a failed job
        if (['failed', 'timeout'].includes(lastJob.status) && lastJob.attempt < config.max_retries) {
          if (lastFinished && now - lastFinished >= retryDelayMs) {
            console.log(`[run-full-sync] Starting retry attempt ${lastJob.attempt + 1}`);
          } else {
            console.log('[run-full-sync] Retry delay not elapsed yet');
            return new Response(
              JSON.stringify({ status: 'skipped', message: 'In attesa retry delay' }),
              { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
            );
          }
        } else if (lastJob.attempt >= config.max_retries && ['failed', 'timeout'].includes(lastJob.status)) {
          // Max retries reached, wait for next frequency cycle
          if (now - lastStarted < frequencyMs) {
            console.log('[run-full-sync] Max retries reached, waiting for next cycle');
            return new Response(
              JSON.stringify({ status: 'skipped', message: 'Max retry raggiunto, attesa prossimo ciclo' }),
              { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
            );
          }
        } else {
          // Check if frequency has elapsed for a new job
          const lastAttempt1Jobs = await supabase
            .from('sync_runs')
            .select('started_at')
            .eq('trigger_type', 'cron')
            .eq('attempt', 1)
            .order('started_at', { ascending: false })
            .limit(1);

          if (lastAttempt1Jobs.data && lastAttempt1Jobs.data.length > 0) {
            const lastAttempt1Started = new Date(lastAttempt1Jobs.data[0].started_at).getTime();
            if (now - lastAttempt1Started < frequencyMs) {
              console.log('[run-full-sync] Frequency not elapsed yet');
              return new Response(
                JSON.stringify({ status: 'skipped', message: 'Frequenza non ancora trascorsa' }),
                { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
              );
            }
          }

          // For daily runs, check time
          if (config.frequency_minutes === 1440 && config.daily_time) {
            const nowDate = new Date();
            const [hours, minutes] = config.daily_time.split(':').map(Number);
            const scheduledTime = new Date(nowDate);
            scheduledTime.setHours(hours, minutes, 0, 0);
            
            if (nowDate < scheduledTime) {
              console.log('[run-full-sync] Daily time not reached yet');
              return new Response(
                JSON.stringify({ status: 'skipped', message: 'Orario giornaliero non raggiunto' }),
                { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
              );
            }
          }
        }
      }
    }

    // Determine attempt number for cron jobs
    let attempt = 1;
    if (trigger === 'cron') {
      const { data: lastCronJobs } = await supabase
        .from('sync_runs')
        .select('attempt, status')
        .eq('trigger_type', 'cron')
        .order('started_at', { ascending: false })
        .limit(1);

      if (lastCronJobs && lastCronJobs.length > 0) {
        const lastJob = lastCronJobs[0];
        if (['failed', 'timeout'].includes(lastJob.status) && lastJob.attempt < 5) {
          attempt = lastJob.attempt + 1;
        }
      }
    }

    // Get fee configuration from database
    const { data: feeConfigData, error: feeConfigError } = await supabase
      .from('fee_config')
      .select('*')
      .limit(1)
      .single();
    
    const feeConfig = {
      feeDrev: feeConfigData?.fee_drev ?? 1.05,
      feeMkt: feeConfigData?.fee_mkt ?? 1.08,
      shippingCost: feeConfigData?.shipping_cost ?? 6.00,
      mediaworldPrepDays: feeConfigData?.mediaworld_preparation_days ?? 3,
      epricePrepDays: feeConfigData?.eprice_preparation_days ?? 1
    };

    console.log('[run-full-sync] Fee config:', feeConfig);

    // Create new sync run record
    const runId = crypto.randomUUID();
    const startTime = Date.now();
    const { error: insertError } = await supabase
      .from('sync_runs')
      .insert({
        id: runId,
        started_at: new Date().toISOString(),
        status: 'running',
        trigger_type: trigger,
        attempt,
        steps: {},
        metrics: initMetrics()
      });

    if (insertError) {
      console.error('[run-full-sync] Error creating run record:', insertError);
      return new Response(
        JSON.stringify({ status: 'error', message: 'Errore creazione record sync' }),
        { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

    console.log(`[run-full-sync] Created run: ${runId}, attempt: ${attempt}`);

    // Initialize tracking
    const steps: Record<string, StepResult> = {};
    const metrics = initMetrics();

    // =========================================================================
    // PIPELINE EXECUTION - 9 STEPS
    // =========================================================================

    try {
      // STEP 1: FTP Import
      if (isTimeoutExceeded(startTime)) {
        await finalizeRun(supabase, runId, 'timeout', startTime, steps, metrics,
          'Runtime massimo di 30 minuti superato durante la sincronizzazione',
          { step: 'import_ftp', completed_steps: Object.keys(steps) });
        return new Response(
          JSON.stringify({ status: 'timeout', run_id: runId }),
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', startTime, steps, metrics,
          'Sincronizzazione interrotta manualmente dall\'utente',
          { cancelled_at_step: 'import_ftp' }, true);
        return new Response(
          JSON.stringify({ status: 'cancelled', run_id: runId }),
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      console.log('[run-full-sync] Step 1: FTP Import');
      const step1Start = Date.now();
      
      // Import all three files from FTP
      const ftpResults: any = { files_downloaded: [] };
      const fileTypes = ['material', 'stock', 'price'];
      
      for (const fileType of fileTypes) {
        try {
          const ftpResponse = await fetch(`${supabaseUrl}/functions/v1/import-catalog-ftp`, {
            method: 'POST',
            headers: {
              'Authorization': `Bearer ${supabaseServiceKey}`,
              'Content-Type': 'application/json'
            },
            body: JSON.stringify({ fileType })
          });

          const ftpData = await ftpResponse.json();
          
          if (ftpData.status === 'error') {
            throw new Error(`FTP import ${fileType} failed: ${ftpData.message}`);
          }
          
          ftpResults.files_downloaded.push(fileType);
          console.log(`[run-full-sync] FTP downloaded: ${fileType}`);
        } catch (ftpErr: any) {
          steps.import_ftp = {
            status: 'failed',
            duration_ms: Date.now() - step1Start,
            error: ftpErr.message,
            files_downloaded: ftpResults.files_downloaded
          };
          
          await finalizeRun(supabase, runId, 'failed', startTime, steps, metrics,
            `Errore import FTP: ${ftpErr.message}`,
            { step: 'import_ftp', file_type: fileType, error: ftpErr.message });
          
          return new Response(
            JSON.stringify({ status: 'failed', run_id: runId, error: ftpErr.message }),
            { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
          );
        }
      }

      steps.import_ftp = {
        status: 'success',
        duration_ms: Date.now() - step1Start,
        files_downloaded: ftpResults.files_downloaded
      };
      await updateRun(supabase, runId, { steps, metrics });

      // =====================================================================
      // STEP 2: Parse and Merge
      // =====================================================================
      if (isTimeoutExceeded(startTime)) {
        await finalizeRun(supabase, runId, 'timeout', startTime, steps, metrics,
          'Runtime massimo di 30 minuti superato durante la sincronizzazione',
          { step: 'parse_merge', completed_steps: Object.keys(steps) });
        return new Response(
          JSON.stringify({ status: 'timeout', run_id: runId }),
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', startTime, steps, metrics,
          'Sincronizzazione interrotta manualmente dall\'utente',
          { cancelled_at_step: 'parse_merge' }, true);
        return new Response(
          JSON.stringify({ status: 'cancelled', run_id: runId }),
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      console.log('[run-full-sync] Step 2: Parse and merge');
      const step2Start = Date.now();
      
      // Get latest files from each folder in ftp-import bucket
      const getLatestFile = async (folder: string): Promise<{ path: string; data: string } | null> => {
        const { data: files, error } = await supabase.storage
          .from('ftp-import')
          .list(folder, { sortBy: { column: 'created_at', order: 'desc' }, limit: 1 });
        
        if (error || !files || files.length === 0) {
          console.error(`[run-full-sync] No files in ${folder}:`, error?.message);
          return null;
        }
        
        const filePath = `${folder}/${files[0].name}`;
        const { data: fileData, error: downloadError } = await supabase.storage
          .from('ftp-import')
          .download(filePath);
        
        if (downloadError || !fileData) {
          console.error(`[run-full-sync] Error downloading ${filePath}:`, downloadError?.message);
          return null;
        }
        
        const text = await fileData.text();
        return { path: filePath, data: text };
      };

      const materialFile = await getLatestFile('material');
      const stockFile = await getLatestFile('stock');
      const priceFile = await getLatestFile('price');

      if (!materialFile || !stockFile || !priceFile) {
        steps.parse_merge = {
          status: 'failed',
          duration_ms: Date.now() - step2Start,
          error: 'File sorgente mancanti nel bucket ftp-import'
        };
        await finalizeRun(supabase, runId, 'failed', startTime, steps, metrics,
          'File sorgente mancanti nel bucket ftp-import',
          { step: 'parse_merge', missing_files: { material: !materialFile, stock: !stockFile, price: !priceFile } });
        return new Response(
          JSON.stringify({ status: 'failed', run_id: runId, error: 'File sorgente mancanti' }),
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      // Parse tab-delimited files
      const materialData = parseTabDelimited(materialFile.data);
      const stockData = parseTabDelimited(stockFile.data);
      const priceData = parseTabDelimited(priceFile.data);

      console.log(`[run-full-sync] Parsed - Material: ${materialData.length}, Stock: ${stockData.length}, Price: ${priceData.length}`);

      // Build lookup maps
      const stockMap = new Map<string, { ExistingStock: number }>();
      for (const row of stockData) {
        const matnr = String(row.Matnr ?? '').trim();
        if (matnr) {
          stockMap.set(matnr, {
            ExistingStock: parseInt(row.ExistingStock) || 0
          });
        }
      }

      const priceMap = new Map<string, { ListPrice: number; CustBestPrice: number; Surcharge: number }>();
      for (const row of priceData) {
        const matnr = String(row.Matnr ?? '').trim();
        if (matnr) {
          priceMap.set(matnr, {
            ListPrice: parseDistributorPrice(row.ListPrice),
            CustBestPrice: parseDistributorPrice(row.CustBestPrice),
            Surcharge: parseDistributorPrice(row.Surcharge)
          });
        }
      }

      // Merge data
      let mergedProducts: any[] = [];
      let skippedNoStock = 0;
      let skippedNoPrice = 0;
      let skippedLowStock = 0;
      let skippedNoValidPrice = 0;

      for (const row of materialData) {
        const matnr = String(row.Matnr ?? '').trim();
        if (!matnr) continue;

        const stockInfo = stockMap.get(matnr);
        const priceInfo = priceMap.get(matnr);

        if (!stockInfo) { skippedNoStock++; continue; }
        if (!priceInfo) { skippedNoPrice++; continue; }
        if (stockInfo.ExistingStock < 2) { skippedLowStock++; continue; }

        const hasBest = priceInfo.CustBestPrice > 0;
        const hasListPrice = priceInfo.ListPrice > 0;

        if (!hasBest && !hasListPrice) { skippedNoValidPrice++; continue; }

        mergedProducts.push({
          Matnr: matnr,
          ManufPartNr: String(row.ManufPartNr ?? '').trim(),
          EAN: String(row.EAN ?? '').trim(),
          ShortDescription: String(row.ShortDescription ?? '').trim(),
          ExistingStock: stockInfo.ExistingStock,
          ListPrice: priceInfo.ListPrice,
          CustBestPrice: priceInfo.CustBestPrice,
          Surcharge: priceInfo.Surcharge
        });
      }

      metrics.products_total = materialData.length;
      metrics.products_processed = mergedProducts.length;

      console.log(`[run-full-sync] Merged products: ${mergedProducts.length} (skipped: stock=${skippedNoStock}, price=${skippedNoPrice}, lowStock=${skippedLowStock}, noValidPrice=${skippedNoValidPrice})`);

      steps.parse_merge = {
        status: 'success',
        duration_ms: Date.now() - step2Start,
        products_parsed: mergedProducts.length,
        skipped_no_stock: skippedNoStock,
        skipped_no_price: skippedNoPrice,
        skipped_low_stock: skippedLowStock,
        skipped_no_valid_price: skippedNoValidPrice
      };
      await updateRun(supabase, runId, { steps, metrics });

      // =====================================================================
      // STEP 3: EAN Mapping
      // =====================================================================
      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', startTime, steps, metrics,
          'Sincronizzazione interrotta manualmente dall\'utente',
          { cancelled_at_step: 'ean_mapping' }, true);
        return new Response(
          JSON.stringify({ status: 'cancelled', run_id: runId }),
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      console.log('[run-full-sync] Step 3: EAN Mapping');
      const step3Start = Date.now();
      
      // Try to load EAN mapping file from mapping-files bucket
      let eanMapped = 0;
      let eanMissing = 0;
      let mappingWarning: string | null = null;

      try {
        const { data: mappingFiles, error: listError } = await supabase.storage
          .from('mapping-files')
          .list('ean', { limit: 1, sortBy: { column: 'created_at', order: 'desc' } });

        if (!listError && mappingFiles && mappingFiles.length > 0) {
          const mappingPath = `ean/${mappingFiles[0].name}`;
          const { data: mappingData, error: downloadError } = await supabase.storage
            .from('mapping-files')
            .download(mappingPath);

          if (!downloadError && mappingData) {
            const mappingText = await mappingData.text();
            const mappingLines = mappingText.split('\n');
            
            // Build mapping: mpn -> ean
            const eanMappingMap = new Map<string, string>();
            for (let i = 1; i < mappingLines.length; i++) {
              const line = mappingLines[i]?.trim();
              if (!line) continue;
              const parts = line.split(';');
              if (parts.length >= 2) {
                const mpn = parts[0]?.trim();
                const ean = parts[1]?.trim();
                if (mpn && ean) {
                  eanMappingMap.set(mpn, ean);
                }
              }
            }

            console.log(`[run-full-sync] Loaded EAN mapping with ${eanMappingMap.size} entries`);

            // Apply mapping to products with empty EAN
            for (const product of mergedProducts) {
              if (!product.EAN && product.ManufPartNr) {
                const mappedEAN = eanMappingMap.get(product.ManufPartNr);
                if (mappedEAN) {
                  product.EAN = mappedEAN;
                  eanMapped++;
                } else {
                  eanMissing++;
                }
              }
            }
          } else {
            mappingWarning = 'File mapping EAN non leggibile';
          }
        } else {
          mappingWarning = 'File mapping EAN non presente';
        }
      } catch (mappingErr: any) {
        mappingWarning = `Errore lettura mapping EAN: ${mappingErr.message}`;
      }

      if (mappingWarning) {
        metrics.warnings.push(mappingWarning);
        console.log(`[run-full-sync] EAN mapping warning: ${mappingWarning}`);
      }

      metrics.products_ean_mapped = eanMapped;
      metrics.products_ean_missing = eanMissing;

      steps.ean_mapping = {
        status: 'success',
        duration_ms: Date.now() - step3Start,
        products_ean_mapped: eanMapped,
        products_ean_missing: eanMissing,
        products_ean_invalid: 0,
        warning: mappingWarning
      };
      await updateRun(supabase, runId, { steps, metrics });

      // =====================================================================
      // STEP 4: Pricing
      // =====================================================================
      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', startTime, steps, metrics,
          'Sincronizzazione interrotta manualmente dall\'utente',
          { cancelled_at_step: 'pricing' }, true);
        return new Response(
          JSON.stringify({ status: 'cancelled', run_id: runId }),
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      console.log('[run-full-sync] Step 4: Pricing');
      const step4Start = Date.now();

      // Calculate prices for each product
      for (const product of mergedProducts) {
        const hasBest = product.CustBestPrice > 0;
        const hasListPrice = product.ListPrice > 0;
        const surcharge = (product.Surcharge > 0) ? product.Surcharge : 0;

        let basePriceCents = 0;
        let baseRoute = '';

        if (hasBest) {
          basePriceCents = Math.round((product.CustBestPrice + surcharge) * 100);
          baseRoute = 'cbp';
        } else if (hasListPrice) {
          basePriceCents = Math.round(product.ListPrice * 100);
          baseRoute = 'listprice';
        }

        product.basePriceCents = basePriceCents;
        product._route = baseRoute;

        // Calculate final price in cents
        const shippingCents = Math.round(feeConfig.shippingCost * 100);
        const afterShippingCents = basePriceCents + shippingCents;
        const afterIvaCents = Math.round(afterShippingCents * 1.22);
        const afterFeeDeRevCents = Math.round(afterIvaCents * feeConfig.feeDrev);
        const afterFeesCents = Math.round(afterFeeDeRevCents * feeConfig.feeMkt);
        const finalCents = toComma99Cents(afterFeesCents);
        const finalEuros = finalCents / 100;
        
        product['Prezzo Finale'] = finalEuros.toFixed(2).replace('.', ',');
        product._finalCents = finalCents;

        // Calculate ListPrice con Fee
        let listPriceConFee: number | string = '';
        const normListPrice = product.ListPrice;
        const normCustBestPrice = product.CustBestPrice;
        
        const shouldUseAltRule = normListPrice <= 0 || (normCustBestPrice > 0 && normListPrice < normCustBestPrice);
        
        if (shouldUseAltRule && normCustBestPrice > 0) {
          const baseLP = normCustBestPrice * 1.25;
          const valoreCandidat = ((baseLP + feeConfig.shippingCost) * 1.22) * feeConfig.feeDrev * feeConfig.feeMkt;
          const candidatoCeil = Math.ceil(valoreCandidat);
          const minimoConsentito = Math.ceil(finalEuros * 1.25);
          listPriceConFee = Math.max(candidatoCeil, minimoConsentito);
        } else if (normListPrice > 0) {
          const subtotConIvaLP = (normListPrice + feeConfig.shippingCost) * 1.22;
          const postFeeLP = subtotConIvaLP * feeConfig.feeDrev * feeConfig.feeMkt;
          listPriceConFee = Math.ceil(postFeeLP);
        }
        
        product['ListPrice con Fee'] = listPriceConFee;
      }

      steps.pricing = {
        status: 'success',
        duration_ms: Date.now() - step4Start,
        products_priced: mergedProducts.length
      };
      await updateRun(supabase, runId, { steps, metrics });

      // =====================================================================
      // STEP 5: Override
      // =====================================================================
      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', startTime, steps, metrics,
          'Sincronizzazione interrotta manualmente dall\'utente',
          { cancelled_at_step: 'override' }, true);
        return new Response(
          JSON.stringify({ status: 'cancelled', run_id: runId }),
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      console.log('[run-full-sync] Step 5: Override');
      const step5Start = Date.now();
      
      // Check if override file exists
      const { data: overrideFiles } = await supabase.storage
        .from('mapping-files')
        .list('override', { limit: 1, sortBy: { column: 'created_at', order: 'desc' } });
      
      if (overrideFiles && overrideFiles.length > 0) {
        // Override file exists - for now mark as skipped since full XLSX parsing is complex
        // In production, this would parse XLSX and apply overrides
        steps.override = {
          status: 'skipped',
          duration_ms: Date.now() - step5Start,
          reason: 'Override parsing non implementato in server-side (file presente ma non elaborato)'
        };
        metrics.warnings.push('File override presente ma non elaborato automaticamente');
      } else {
        steps.override = {
          status: 'skipped',
          duration_ms: Date.now() - step5Start,
          reason: 'File override non presente'
        };
      }
      
      metrics.products_after_override = mergedProducts.length;
      await updateRun(supabase, runId, { steps, metrics });

      // =====================================================================
      // STEP 6: Export EAN Catalog
      // =====================================================================
      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', startTime, steps, metrics,
          'Sincronizzazione interrotta manualmente dall\'utente',
          { cancelled_at_step: 'export_ean' }, true);
        return new Response(
          JSON.stringify({ status: 'cancelled', run_id: runId }),
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      console.log('[run-full-sync] Step 6: Export EAN Catalog');
      const step6Start = Date.now();

      // Filter and normalize for EAN export
      const eanValidProducts: any[] = [];
      const eanDiscarded: any[] = [];
      const eanStats = {
        ean_validi_13: 0,
        ean_padded_12_to_13: 0,
        ean_trimmed_14_to_13: 0,
        ean_validi_14: 0,
        ean_mancanti: 0,
        ean_non_numerici: 0,
        ean_lunghezze_invalid: 0,
        ean_duplicati_risolti: 0
      };

      for (const product of mergedProducts) {
        const eanResult = normalizeEAN(product.EAN);
        
        if (!eanResult.ok) {
          if (eanResult.reason === 'EAN mancante') eanStats.ean_mancanti++;
          else if (eanResult.reason === 'EAN contiene caratteri non numerici') eanStats.ean_non_numerici++;
          else if (eanResult.reason?.startsWith('EAN lunghezza')) eanStats.ean_lunghezze_invalid++;
          eanDiscarded.push({ ...product, discard_reason: eanResult.reason });
          continue;
        }

        if (eanResult.reason === 'padded_12_to_13') eanStats.ean_padded_12_to_13++;
        else if (eanResult.reason === 'valid_13') eanStats.ean_validi_13++;
        else if (eanResult.reason === 'trimmed_14_to_13') eanStats.ean_trimmed_14_to_13++;
        else if (eanResult.reason === 'valid_14') eanStats.ean_validi_14++;

        eanValidProducts.push({ ...product, EAN: eanResult.value });
      }

      // Deduplicate by EAN, keeping highest price
      const byEAN = new Map<string, any>();
      for (const p of eanValidProducts) {
        const key = p.EAN;
        const priceStr = p['Prezzo Finale'] || '0';
        const price = parseFloat(String(priceStr).replace(',', '.')) || 0;
        
        const existing = byEAN.get(key);
        if (!existing) {
          byEAN.set(key, { product: p, price });
        } else if (price > existing.price) {
          eanStats.ean_duplicati_risolti++;
          byEAN.set(key, { product: p, price });
        } else {
          eanStats.ean_duplicati_risolti++;
        }
      }

      const eanCatalog = Array.from(byEAN.values()).map(v => v.product);
      
      metrics.products_ean_invalid = eanDiscarded.length;

      console.log(`[run-full-sync] EAN catalog: ${eanCatalog.length} products (${eanDiscarded.length} discarded)`);

      // Generate Excel for EAN catalog
      const eanExcelData = eanCatalog.map(p => ({
        Matnr: p.Matnr,
        ManufPartNr: p.ManufPartNr,
        EAN: p.EAN,
        ShortDescription: p.ShortDescription,
        ExistingStock: p.ExistingStock,
        ListPrice: p.ListPrice,
        CustBestPrice: p.CustBestPrice,
        'Prezzo Finale': p['Prezzo Finale'],
        'ListPrice con Fee': p['ListPrice con Fee']
      }));

      // Create simple CSV for EAN catalog (XLSX library not available in Deno Edge)
      const eanCsvHeaders = ['Matnr', 'ManufPartNr', 'EAN', 'ShortDescription', 'ExistingStock', 'ListPrice', 'CustBestPrice', 'Prezzo Finale', 'ListPrice con Fee'];
      const eanCsvRows = eanExcelData.map(row => 
        eanCsvHeaders.map(h => {
          const val = row[h as keyof typeof row];
          const str = String(val ?? '');
          return str.includes(',') || str.includes('"') || str.includes('\n') 
            ? `"${str.replace(/"/g, '""')}"` 
            : str;
        }).join(';')
      );
      const eanCsvContent = [eanCsvHeaders.join(';'), ...eanCsvRows].join('\n');

      // Upload EAN catalog
      const eanFilename = 'Catalogo EAN.csv';
      const { error: eanUploadError } = await supabase.storage
        .from('exports')
        .upload(eanFilename, new Blob([eanCsvContent], { type: 'text/csv' }), { upsert: true });

      if (eanUploadError) {
        console.error('[run-full-sync] EAN catalog upload error:', eanUploadError);
        steps.export_ean = {
          status: 'failed',
          duration_ms: Date.now() - step6Start,
          error: eanUploadError.message
        };
        await finalizeRun(supabase, runId, 'failed', startTime, steps, metrics,
          `Errore upload Catalogo EAN: ${eanUploadError.message}`,
          { step: 'export_ean', error: eanUploadError.message });
        return new Response(
          JSON.stringify({ status: 'failed', run_id: runId }),
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      steps.export_ean = {
        status: 'success',
        duration_ms: Date.now() - step6Start,
        export_rows: eanCatalog.length,
        discarded_rows: eanDiscarded.length,
        ean_stats: eanStats
      };
      metrics.exported_files_count++;
      await updateRun(supabase, runId, { steps, metrics });

      // =====================================================================
      // STEP 7: Export Mediaworld
      // =====================================================================
      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', startTime, steps, metrics,
          'Sincronizzazione interrotta manualmente dall\'utente',
          { cancelled_at_step: 'export_mediaworld' }, true);
        return new Response(
          JSON.stringify({ status: 'cancelled', run_id: runId }),
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      console.log('[run-full-sync] Step 7: Export Mediaworld');
      const step7Start = Date.now();

      // Build Mediaworld export
      const mwHeaders = [
        'SKU offerta', 'ID Prodotto', 'Tipo ID prodotto', 'Descrizione offerta', 
        'Descrizione interna offerta', 'Prezzo dell\'offerta', 'Info aggiuntive prezzo offerta',
        'Quantità dell\'offerta', 'Avviso quantità minima', 'Stato dell\'offerta',
        'Data di inizio della disponibilità', 'Data di conclusione della disponibilità',
        'Classe logistica', 'Prezzo scontato', 'Data di inizio dello sconto',
        'Data di termine dello sconto', 'Tempo di preparazione della spedizione (in giorni)',
        'Aggiorna/Cancella', 'Tipo di prezzo che verrà barrato quando verrà definito un prezzo scontato.',
        'Obbligo di ritiro RAEE', 'Orario di cut-off (solo se la consegna il giorno successivo è abilitata)',
        'VAT Rate % (Turkey only)'
      ];

      const mwRows: string[][] = [];
      let mwSkipped = 0;

      for (const product of eanCatalog) {
        const sku = product.ManufPartNr;
        const ean = product.EAN;
        const stock = product.ExistingStock;
        
        if (!sku || sku.length > 40 || !ean || ean.length < 12 || stock <= 0) {
          mwSkipped++;
          continue;
        }

        const listPriceConFee = product['ListPrice con Fee'];
        const prezzoFinale = product['Prezzo Finale'];

        if (!listPriceConFee || !prezzoFinale) {
          mwSkipped++;
          continue;
        }

        // Parse prices
        const listPriceNum = typeof listPriceConFee === 'number' ? listPriceConFee : parseFloat(String(listPriceConFee).replace(',', '.'));
        const prezzoFinaleNum = typeof prezzoFinale === 'number' ? prezzoFinale : parseFloat(String(prezzoFinale).replace(',', '.'));

        if (!Number.isFinite(listPriceNum) || listPriceNum <= 0 || !Number.isFinite(prezzoFinaleNum) || prezzoFinaleNum <= 0) {
          mwSkipped++;
          continue;
        }

        mwRows.push([
          sku,
          ean,
          'EAN',
          product.ShortDescription || '',
          '',
          listPriceNum.toFixed(2),
          '',
          String(stock),
          '',
          'Nuovo',
          '',
          '',
          'Consegna gratuita',
          prezzoFinaleNum.toFixed(2),
          '',
          '',
          String(feeConfig.mediaworldPrepDays),
          '',
          'recommended-retail-price',
          '',
          '',
          ''
        ]);
      }

      const mwCsvContent = [
        mwHeaders.join(';'),
        ...mwRows.map(row => row.map(cell => 
          cell.includes(';') || cell.includes('"') || cell.includes('\n') 
            ? `"${cell.replace(/"/g, '""')}"` 
            : cell
        ).join(';'))
      ].join('\n');

      const mwFilename = 'Export Mediaworld.csv';
      const { error: mwUploadError } = await supabase.storage
        .from('exports')
        .upload(mwFilename, new Blob([mwCsvContent], { type: 'text/csv' }), { upsert: true });

      if (mwUploadError) {
        console.error('[run-full-sync] Mediaworld export upload error:', mwUploadError);
      }

      metrics.mediaworld_export_rows = mwRows.length;
      metrics.mediaworld_export_skipped = mwSkipped;

      steps.export_mediaworld = {
        status: mwUploadError ? 'failed' : 'success',
        duration_ms: Date.now() - step7Start,
        export_rows: mwRows.length,
        skipped_rows: mwSkipped,
        error: mwUploadError?.message
      };
      metrics.exported_files_count++;
      await updateRun(supabase, runId, { steps, metrics });

      // =====================================================================
      // STEP 8: Export ePrice
      // =====================================================================
      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', startTime, steps, metrics,
          'Sincronizzazione interrotta manualmente dall\'utente',
          { cancelled_at_step: 'export_eprice' }, true);
        return new Response(
          JSON.stringify({ status: 'cancelled', run_id: runId }),
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      console.log('[run-full-sync] Step 8: Export ePrice');
      const step8Start = Date.now();

      // Build ePrice export (similar format to Mediaworld but different columns)
      const epHeaders = ['EAN', 'SKU', 'Titolo', 'Prezzo', 'Quantita', 'Tempo Consegna'];
      const epRows: string[][] = [];
      let epSkipped = 0;

      for (const product of eanCatalog) {
        const ean = product.EAN;
        const sku = product.ManufPartNr;
        const stock = product.ExistingStock;
        const prezzoFinale = product['Prezzo Finale'];

        if (!ean || !sku || stock <= 0 || !prezzoFinale) {
          epSkipped++;
          continue;
        }

        const prezzoFinaleNum = typeof prezzoFinale === 'number' ? prezzoFinale : parseFloat(String(prezzoFinale).replace(',', '.'));
        if (!Number.isFinite(prezzoFinaleNum) || prezzoFinaleNum <= 0) {
          epSkipped++;
          continue;
        }

        epRows.push([
          ean,
          sku,
          product.ShortDescription || '',
          prezzoFinaleNum.toFixed(2).replace('.', ','),
          String(stock),
          String(feeConfig.epricePrepDays)
        ]);
      }

      const epCsvContent = [
        epHeaders.join(';'),
        ...epRows.map(row => row.map(cell => 
          cell.includes(';') || cell.includes('"') || cell.includes('\n') 
            ? `"${cell.replace(/"/g, '""')}"` 
            : cell
        ).join(';'))
      ].join('\n');

      const epFilename = 'Export ePrice.csv';
      const { error: epUploadError } = await supabase.storage
        .from('exports')
        .upload(epFilename, new Blob([epCsvContent], { type: 'text/csv' }), { upsert: true });

      if (epUploadError) {
        console.error('[run-full-sync] ePrice export upload error:', epUploadError);
      }

      metrics.eprice_export_rows = epRows.length;
      metrics.eprice_export_skipped = epSkipped;

      steps.export_eprice = {
        status: epUploadError ? 'failed' : 'success',
        duration_ms: Date.now() - step8Start,
        export_rows: epRows.length,
        skipped_rows: epSkipped,
        error: epUploadError?.message
      };
      metrics.exported_files_count++;
      await updateRun(supabase, runId, { steps, metrics });

      // =====================================================================
      // STEP 9: SFTP Upload
      // =====================================================================
      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', startTime, steps, metrics,
          'Sincronizzazione interrotta manualmente dall\'utente',
          { cancelled_at_step: 'upload_sftp' }, true);
        return new Response(
          JSON.stringify({ status: 'cancelled', run_id: runId }),
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      console.log('[run-full-sync] Step 9: SFTP Upload');
      const step9Start = Date.now();
      
      try {
        const sftpResponse = await fetch(`${supabaseUrl}/functions/v1/upload-exports-to-sftp`, {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${supabaseServiceKey}`,
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            files: [
              { bucket: 'exports', path: 'Catalogo EAN.csv', filename: 'Catalogo EAN.csv' },
              { bucket: 'exports', path: 'Export ePrice.csv', filename: 'Export ePrice.csv' },
              { bucket: 'exports', path: 'Export Mediaworld.csv', filename: 'Export Mediaworld.csv' }
            ]
          })
        });

        const sftpData = await sftpResponse.json();
        
        if (sftpData.status === 'error') {
          throw new Error(`SFTP upload failed: ${sftpData.message}`);
        }
        
        const uploadedCount = sftpData.results?.filter((r: any) => r.uploaded).length || 0;
        
        steps.upload_sftp = {
          status: 'success',
          duration_ms: Date.now() - step9Start,
          files_uploaded: uploadedCount
        };
        metrics.sftp_uploaded_files = uploadedCount;
        
      } catch (sftpErr: any) {
        steps.upload_sftp = {
          status: 'failed',
          duration_ms: Date.now() - step9Start,
          error: sftpErr.message,
          files_uploaded: 0
        };
        
        await finalizeRun(supabase, runId, 'failed', startTime, steps, metrics,
          `Errore upload SFTP: ${sftpErr.message}`,
          { step: 'upload_sftp', error: sftpErr.message });
        
        return new Response(
          JSON.stringify({ status: 'failed', run_id: runId, error: sftpErr.message }),
          { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
        );
      }

      // =========================================================================
      // SUCCESS
      // =========================================================================
      await finalizeRun(supabase, runId, 'success', startTime, steps, metrics);
      
      console.log(`[run-full-sync] Pipeline completed successfully: ${runId}`);
      
      return new Response(
        JSON.stringify({ status: 'success', run_id: runId, metrics }),
        { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );

    } catch (pipelineError: any) {
      console.error('[run-full-sync] Pipeline error:', pipelineError);
      
      await finalizeRun(supabase, runId, 'failed', startTime, steps, metrics,
        `Errore pipeline: ${pipelineError.message}`,
        { error: pipelineError.message, stack: pipelineError.stack });
      
      return new Response(
        JSON.stringify({ status: 'failed', run_id: runId, error: pipelineError.message }),
        { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
      );
    }

  } catch (error: any) {
    console.error('[run-full-sync] Unexpected error:', error);
    
    return new Response(
      JSON.stringify({ status: 'error', message: error.message }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
});
