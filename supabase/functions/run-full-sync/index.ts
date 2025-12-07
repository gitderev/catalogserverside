import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Maximum runtime: 30 minutes
const MAX_RUNTIME_MS = 30 * 60 * 1000;

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

// Utility functions
function parseEuroLike(input: unknown): number {
  if (typeof input === 'number' && isFinite(input)) return input;
  let s = String(input ?? '').trim().replace(/[^\d.,\s%\-]/g, '').split(/\s+/)[0]?.replace(/%/g, '').trim() ?? '';
  if (!s) return NaN;
  if (s.includes('.') && s.includes(',')) s = s.replace(/\./g, '').replace(',', '.');
  else s = s.replace(',', '.');
  return parseFloat(s);
}

function toComma99Cents(cents: number): number {
  if (cents % 100 === 99) return cents;
  const euros = Math.floor(cents / 100);
  let target = euros * 100 + 99;
  if (target < cents) target = (euros + 1) * 100 + 99;
  return target;
}

function normalizeEAN(raw: unknown): { ok: boolean; value?: string; reason?: string } {
  const original = (raw ?? '').toString().trim();
  if (!original) return { ok: false, reason: 'EAN mancante' };
  const compact = original.replace(/[\s-]+/g, '');
  if (!/^\d+$/.test(compact)) return { ok: false, reason: 'EAN non numerico' };
  if (compact.length === 12) return { ok: true, value: '0' + compact, reason: 'padded' };
  if (compact.length === 13) return { ok: true, value: compact, reason: 'valid_13' };
  if (compact.length === 14) return { ok: true, value: compact.startsWith('0') ? compact.substring(1) : compact, reason: 'valid_14' };
  return { ok: false, reason: `lunghezza ${compact.length}` };
}

async function updateRun(supabase: any, runId: string, updates: any): Promise<void> {
  await supabase.from('sync_runs').update(updates).eq('id', runId);
}

async function isCancelRequested(supabase: any, runId: string): Promise<boolean> {
  const { data } = await supabase.from('sync_runs').select('cancel_requested').eq('id', runId).single();
  return data?.cancel_requested === true;
}

async function finalizeRun(supabase: any, runId: string, status: string, startTime: number, steps: any, metrics: any, errorMessage?: string, errorDetails?: any, cancelledByUser = false): Promise<void> {
  await supabase.from('sync_runs').update({
    status, finished_at: new Date().toISOString(), runtime_ms: Date.now() - startTime,
    steps, metrics, error_message: errorMessage || null, error_details: errorDetails || null, cancelled_by_user: cancelledByUser
  }).eq('id', runId);
}

serve(async (req) => {
  if (req.method === 'OPTIONS') return new Response(null, { headers: corsHeaders });
  if (req.method !== 'POST') return new Response(JSON.stringify({ status: 'error', message: 'Method not allowed' }), { status: 405, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

  const supabaseUrl = Deno.env.get('SUPABASE_URL')!;
  const supabaseServiceKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
  const supabaseAnonKey = Deno.env.get('SUPABASE_ANON_KEY')!;

  try {
    const body = await req.json().catch(() => ({}));
    const trigger = body.trigger as string;
    if (!trigger || !['cron', 'manual'].includes(trigger)) {
      return new Response(JSON.stringify({ status: 'error', message: 'trigger deve essere cron o manual' }), { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    console.log(`[run-full-sync] Trigger: ${trigger}`);

    // Auth for manual triggers
    if (trigger === 'manual') {
      const authHeader = req.headers.get('Authorization');
      if (!authHeader?.startsWith('Bearer ')) {
        return new Response(JSON.stringify({ status: 'error', message: 'Auth required' }), { status: 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      const jwt = authHeader.replace('Bearer ', '');
      const userClient = createClient(supabaseUrl, supabaseAnonKey, { global: { headers: { Authorization: `Bearer ${jwt}` } } });
      const { data: userData, error: userError } = await userClient.auth.getUser();
      if (userError || !userData?.user) {
        return new Response(JSON.stringify({ status: 'error', message: 'Invalid token' }), { status: 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      console.log(`[run-full-sync] Manual by user: ${userData.user.id}`);
    }

    const supabase = createClient(supabaseUrl, supabaseServiceKey);

    // Check running jobs
    const { data: runningJobs } = await supabase.from('sync_runs').select('id').eq('status', 'running').limit(1);
    if (runningJobs?.length) {
      return new Response(JSON.stringify({ status: 'error', message: 'Sync già in corso' }), { status: 409, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    // For cron, check if enabled
    if (trigger === 'cron') {
      const { data: config } = await supabase.from('sync_config').select('enabled, frequency_minutes').eq('id', 1).single();
      if (!config?.enabled) {
        console.log('[run-full-sync] Disabled');
        return new Response(JSON.stringify({ status: 'skipped', message: 'Disabled' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      // Check last run time
      const { data: lastRun } = await supabase.from('sync_runs').select('started_at').eq('trigger_type', 'cron').eq('attempt', 1).order('started_at', { ascending: false }).limit(1);
      if (lastRun?.length) {
        const elapsed = Date.now() - new Date(lastRun[0].started_at).getTime();
        if (elapsed < config.frequency_minutes * 60 * 1000) {
          console.log('[run-full-sync] Frequency not elapsed');
          return new Response(JSON.stringify({ status: 'skipped', message: 'Frequency not elapsed' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }
      }
    }

    // Get fee config
    const { data: feeData } = await supabase.from('fee_config').select('*').limit(1).single();
    const feeConfig = {
      feeDrev: feeData?.fee_drev ?? 1.05,
      feeMkt: feeData?.fee_mkt ?? 1.08,
      shippingCost: feeData?.shipping_cost ?? 6.00,
      mediaworldPrepDays: feeData?.mediaworld_preparation_days ?? 3,
      epricePrepDays: feeData?.eprice_preparation_days ?? 1
    };

    // Create run
    const runId = crypto.randomUUID();
    const startTime = Date.now();
    await supabase.from('sync_runs').insert({ id: runId, started_at: new Date().toISOString(), status: 'running', trigger_type: trigger, attempt: 1, steps: {}, metrics: initMetrics() });
    console.log(`[run-full-sync] Run created: ${runId}`);

    const steps: Record<string, StepResult> = {};
    const metrics = initMetrics();

    try {
      // ===== STEP 1: FTP Import =====
      console.log('[run-full-sync] Step 1: FTP Import');
      const s1 = Date.now();
      
      for (const fileType of ['material', 'stock', 'price']) {
        console.log(`[run-full-sync] Importing ${fileType}...`);
        const resp = await fetch(`${supabaseUrl}/functions/v1/import-catalog-ftp`, {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${supabaseServiceKey}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ fileType })
        });
        const data = await resp.json();
        if (data.status === 'error') throw new Error(`FTP ${fileType}: ${data.message}`);
        console.log(`[run-full-sync] ${fileType} imported`);
      }
      
      steps.import_ftp = { status: 'success', duration_ms: Date.now() - s1 };
      await updateRun(supabase, runId, { steps });

      // ===== STEP 2: Parse & Merge =====
      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', startTime, steps, metrics, 'Interrotta', { step: 'parse' }, true);
        return new Response(JSON.stringify({ status: 'cancelled' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      console.log('[run-full-sync] Step 2: Parse & Merge');
      const s2 = Date.now();

      // Get latest files
      const getLatest = async (folder: string) => {
        const { data: files } = await supabase.storage.from('ftp-import').list(folder, { sortBy: { column: 'created_at', order: 'desc' }, limit: 1 });
        if (!files?.length) return null;
        const { data } = await supabase.storage.from('ftp-import').download(`${folder}/${files[0].name}`);
        return data ? await data.text() : null;
      };

      const [materialTxt, stockTxt, priceTxt] = await Promise.all([getLatest('material'), getLatest('stock'), getLatest('price')]);
      if (!materialTxt || !stockTxt || !priceTxt) {
        steps.parse_merge = { status: 'failed', duration_ms: Date.now() - s2, error: 'File mancanti' };
        await finalizeRun(supabase, runId, 'failed', startTime, steps, metrics, 'File sorgente mancanti');
        return new Response(JSON.stringify({ status: 'failed' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }

      // Parse with minimal memory
      const parseTab = (txt: string) => {
        const lines = txt.split('\n');
        const headers = lines[0]?.split('\t').map(h => h.trim()) || [];
        return lines.slice(1).filter(l => l.trim()).map(l => {
          const vals = l.split('\t');
          const row: any = {};
          headers.forEach((h, i) => row[h] = vals[i]?.trim() ?? '');
          return row;
        });
      };

      const stockMap = new Map<string, number>();
      for (const r of parseTab(stockTxt)) {
        const m = r.Matnr?.trim();
        if (m) stockMap.set(m, parseInt(r.ExistingStock) || 0);
      }

      const priceMap = new Map<string, { lp: number; cbp: number; sur: number }>();
      for (const r of parseTab(priceTxt)) {
        const m = r.Matnr?.trim();
        if (m) {
          const parse = (v: any) => parseFloat(String(v || '0').replace(',', '.')) || 0;
          priceMap.set(m, { lp: parse(r.ListPrice), cbp: parse(r.CustBestPrice), sur: parse(r.Surcharge) });
        }
      }

      // Merge - process in streaming fashion
      let products: any[] = [];
      let skipped = { noStock: 0, noPrice: 0, lowStock: 0, noValid: 0 };
      
      for (const r of parseTab(materialTxt)) {
        const m = r.Matnr?.trim();
        if (!m) continue;
        const stock = stockMap.get(m);
        const price = priceMap.get(m);
        if (stock === undefined) { skipped.noStock++; continue; }
        if (!price) { skipped.noPrice++; continue; }
        if (stock < 2) { skipped.lowStock++; continue; }
        if (price.lp <= 0 && price.cbp <= 0) { skipped.noValid++; continue; }
        
        products.push({
          Matnr: m, MPN: r.ManufPartNr?.trim() || '', EAN: r.EAN?.trim() || '',
          Desc: r.ShortDescription?.trim() || '', Stock: stock,
          LP: price.lp, CBP: price.cbp, Sur: price.sur
        });
      }
      
      // Clear maps to free memory
      stockMap.clear();
      priceMap.clear();

      metrics.products_total = products.length + skipped.noStock + skipped.noPrice + skipped.lowStock + skipped.noValid;
      metrics.products_processed = products.length;
      console.log(`[run-full-sync] Merged: ${products.length} products`);

      steps.parse_merge = { status: 'success', duration_ms: Date.now() - s2, products: products.length, skipped };
      await updateRun(supabase, runId, { steps, metrics });

      // ===== STEP 3: EAN Mapping =====
      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', startTime, steps, metrics, 'Interrotta', { step: 'ean' }, true);
        return new Response(JSON.stringify({ status: 'cancelled' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }

      console.log('[run-full-sync] Step 3: EAN Mapping');
      const s3 = Date.now();
      let eanMapped = 0, eanMissing = 0;

      try {
        const { data: files } = await supabase.storage.from('mapping-files').list('ean', { limit: 1, sortBy: { column: 'created_at', order: 'desc' } });
        if (files?.length) {
          const { data: blob } = await supabase.storage.from('mapping-files').download(`ean/${files[0].name}`);
          if (blob) {
            const mappingMap = new Map<string, string>();
            for (const line of (await blob.text()).split('\n').slice(1)) {
              const [mpn, ean] = line.split(';').map(s => s?.trim());
              if (mpn && ean) mappingMap.set(mpn, ean);
            }
            console.log(`[run-full-sync] EAN mapping: ${mappingMap.size} entries`);
            
            for (const p of products) {
              if (!p.EAN && p.MPN) {
                const mapped = mappingMap.get(p.MPN);
                if (mapped) { p.EAN = mapped; eanMapped++; }
                else eanMissing++;
              }
            }
            mappingMap.clear();
          }
        }
      } catch (e: any) {
        metrics.warnings.push(`EAN mapping: ${e.message}`);
      }

      metrics.products_ean_mapped = eanMapped;
      metrics.products_ean_missing = eanMissing;
      steps.ean_mapping = { status: 'success', duration_ms: Date.now() - s3, mapped: eanMapped, missing: eanMissing };
      await updateRun(supabase, runId, { steps, metrics });

      // ===== STEP 4: Pricing =====
      console.log('[run-full-sync] Step 4: Pricing');
      const s4 = Date.now();

      for (const p of products) {
        const hasCBP = p.CBP > 0;
        const hasLP = p.LP > 0;
        let baseCents = hasCBP ? Math.round((p.CBP + p.Sur) * 100) : (hasLP ? Math.round(p.LP * 100) : 0);
        
        const shipCents = Math.round(feeConfig.shippingCost * 100);
        const finalCents = toComma99Cents(Math.round(Math.round(Math.round((baseCents + shipCents) * 1.22) * feeConfig.feeDrev) * feeConfig.feeMkt));
        const finalEuros = finalCents / 100;
        
        p.PF = finalEuros.toFixed(2).replace('.', ',');
        p.PFNum = finalEuros;

        // ListPrice con Fee
        const normLP = p.LP, normCBP = p.CBP;
        const useAlt = normLP <= 0 || (normCBP > 0 && normLP < normCBP);
        if (useAlt && normCBP > 0) {
          const base = normCBP * 1.25;
          const val = ((base + feeConfig.shippingCost) * 1.22) * feeConfig.feeDrev * feeConfig.feeMkt;
          p.LPF = Math.max(Math.ceil(val), Math.ceil(finalEuros * 1.25));
        } else if (normLP > 0) {
          p.LPF = Math.ceil(((normLP + feeConfig.shippingCost) * 1.22) * feeConfig.feeDrev * feeConfig.feeMkt);
        } else {
          p.LPF = '';
        }
      }

      steps.pricing = { status: 'success', duration_ms: Date.now() - s4, priced: products.length };
      await updateRun(supabase, runId, { steps });

      // ===== STEP 5: Override (skipped) =====
      console.log('[run-full-sync] Step 5: Override');
      steps.override = { status: 'skipped', duration_ms: 0, reason: 'Non implementato server-side' };
      metrics.products_after_override = products.length;
      await updateRun(supabase, runId, { steps, metrics });

      // ===== STEP 6: Export EAN =====
      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', startTime, steps, metrics, 'Interrotta', { step: 'export_ean' }, true);
        return new Response(JSON.stringify({ status: 'cancelled' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }

      console.log('[run-full-sync] Step 6: Export EAN');
      const s6 = Date.now();

      // Filter valid EAN and deduplicate
      const byEAN = new Map<string, any>();
      let discarded = 0;
      
      for (const p of products) {
        const ean = normalizeEAN(p.EAN);
        if (!ean.ok) { discarded++; continue; }
        
        const existing = byEAN.get(ean.value!);
        if (!existing || p.PFNum > existing.PFNum) {
          byEAN.set(ean.value!, { ...p, EAN: ean.value });
        }
      }

      const eanCatalog = Array.from(byEAN.values());
      byEAN.clear();
      metrics.products_ean_invalid = discarded;
      console.log(`[run-full-sync] EAN catalog: ${eanCatalog.length}`);

      // Generate CSV
      const eanHeaders = ['Matnr', 'ManufPartNr', 'EAN', 'ShortDescription', 'ExistingStock', 'ListPrice', 'CustBestPrice', 'Prezzo Finale', 'ListPrice con Fee'];
      const eanRows = eanCatalog.map(p => [p.Matnr, p.MPN, p.EAN, p.Desc, p.Stock, p.LP, p.CBP, p.PF, p.LPF].map(v => {
        const s = String(v ?? '');
        return s.includes(';') || s.includes('"') ? `"${s.replace(/"/g, '""')}"` : s;
      }).join(';'));
      const eanCSV = [eanHeaders.join(';'), ...eanRows].join('\n');

      await supabase.storage.from('exports').upload('Catalogo EAN.csv', new Blob([eanCSV], { type: 'text/csv' }), { upsert: true });

      steps.export_ean = { status: 'success', duration_ms: Date.now() - s6, rows: eanCatalog.length, discarded };
      metrics.exported_files_count++;
      await updateRun(supabase, runId, { steps, metrics });

      // ===== STEP 7: Export Mediaworld =====
      console.log('[run-full-sync] Step 7: Mediaworld');
      const s7 = Date.now();

      const mwHeaders = ['SKU offerta', 'ID Prodotto', 'Tipo ID prodotto', 'Descrizione offerta', 'Descrizione interna offerta', "Prezzo dell'offerta", 'Info aggiuntive prezzo offerta', "Quantità dell'offerta", 'Avviso quantità minima', "Stato dell'offerta", 'Data di inizio della disponibilità', 'Data di conclusione della disponibilità', 'Classe logistica', 'Prezzo scontato', 'Data di inizio dello sconto', 'Data di termine dello sconto', 'Tempo di preparazione della spedizione (in giorni)', 'Aggiorna/Cancella', 'Tipo di prezzo che verrà barrato quando verrà definito un prezzo scontato.', 'Obbligo di ritiro RAEE', 'Orario di cut-off (solo se la consegna il giorno successivo è abilitata)', 'VAT Rate % (Turkey only)'];
      
      let mwRows: string[] = [], mwSkipped = 0;
      for (const p of eanCatalog) {
        if (!p.MPN || p.MPN.length > 40 || !p.EAN || p.EAN.length < 12 || p.Stock <= 0 || !p.LPF || !p.PFNum) { mwSkipped++; continue; }
        const lpf = typeof p.LPF === 'number' ? p.LPF.toFixed(2) : String(p.LPF);
        const pf = p.PFNum.toFixed(2);
        mwRows.push([p.MPN, p.EAN, 'EAN', p.Desc, '', lpf, '', String(p.Stock), '', 'Nuovo', '', '', 'Consegna gratuita', pf, '', '', String(feeConfig.mediaworldPrepDays), '', 'recommended-retail-price', '', '', ''].map(c => c.includes(';') ? `"${c}"` : c).join(';'));
      }

      const mwCSV = [mwHeaders.join(';'), ...mwRows].join('\n');
      await supabase.storage.from('exports').upload('Export Mediaworld.csv', new Blob([mwCSV], { type: 'text/csv' }), { upsert: true });

      metrics.mediaworld_export_rows = mwRows.length;
      metrics.mediaworld_export_skipped = mwSkipped;
      steps.export_mediaworld = { status: 'success', duration_ms: Date.now() - s7, rows: mwRows.length, skipped: mwSkipped };
      metrics.exported_files_count++;
      await updateRun(supabase, runId, { steps, metrics });

      // ===== STEP 8: Export ePrice =====
      console.log('[run-full-sync] Step 8: ePrice');
      const s8 = Date.now();

      const epHeaders = ['EAN', 'SKU', 'Titolo', 'Prezzo', 'Quantita', 'Tempo Consegna'];
      let epRows: string[] = [], epSkipped = 0;
      
      for (const p of eanCatalog) {
        if (!p.EAN || !p.MPN || p.Stock <= 0 || !p.PFNum) { epSkipped++; continue; }
        epRows.push([p.EAN, p.MPN, p.Desc, p.PF, String(p.Stock), String(feeConfig.epricePrepDays)].map(c => String(c).includes(';') ? `"${c}"` : c).join(';'));
      }

      const epCSV = [epHeaders.join(';'), ...epRows].join('\n');
      await supabase.storage.from('exports').upload('Export ePrice.csv', new Blob([epCSV], { type: 'text/csv' }), { upsert: true });

      metrics.eprice_export_rows = epRows.length;
      metrics.eprice_export_skipped = epSkipped;
      steps.export_eprice = { status: 'success', duration_ms: Date.now() - s8, rows: epRows.length, skipped: epSkipped };
      metrics.exported_files_count++;
      await updateRun(supabase, runId, { steps, metrics });

      // Clear products array to free memory before SFTP
      products = [];

      // ===== STEP 9: SFTP Upload =====
      if (await isCancelRequested(supabase, runId)) {
        await finalizeRun(supabase, runId, 'failed', startTime, steps, metrics, 'Interrotta', { step: 'sftp' }, true);
        return new Response(JSON.stringify({ status: 'cancelled' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }

      console.log('[run-full-sync] Step 9: SFTP Upload');
      const s9 = Date.now();

      try {
        const sftpResp = await fetch(`${supabaseUrl}/functions/v1/upload-exports-to-sftp`, {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${supabaseServiceKey}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({
            files: [
              { bucket: 'exports', path: 'Catalogo EAN.csv', filename: 'Catalogo EAN.csv' },
              { bucket: 'exports', path: 'Export ePrice.csv', filename: 'Export ePrice.csv' },
              { bucket: 'exports', path: 'Export Mediaworld.csv', filename: 'Export Mediaworld.csv' }
            ]
          })
        });
        const sftpData = await sftpResp.json();
        if (sftpData.status === 'error') throw new Error(sftpData.message);
        
        const uploaded = sftpData.results?.filter((r: any) => r.uploaded).length || 0;
        steps.upload_sftp = { status: 'success', duration_ms: Date.now() - s9, uploaded };
        metrics.sftp_uploaded_files = uploaded;
      } catch (e: any) {
        steps.upload_sftp = { status: 'failed', duration_ms: Date.now() - s9, error: e.message };
        await finalizeRun(supabase, runId, 'failed', startTime, steps, metrics, `SFTP: ${e.message}`, { step: 'sftp' });
        return new Response(JSON.stringify({ status: 'failed', error: e.message }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }

      // ===== SUCCESS =====
      await finalizeRun(supabase, runId, 'success', startTime, steps, metrics);
      console.log(`[run-full-sync] Success: ${runId}`);
      
      return new Response(JSON.stringify({ status: 'success', run_id: runId, metrics }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

    } catch (err: any) {
      console.error('[run-full-sync] Error:', err);
      await finalizeRun(supabase, runId, 'failed', startTime, steps, metrics, err.message, { error: err.message });
      return new Response(JSON.stringify({ status: 'failed', error: err.message }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

  } catch (err: any) {
    console.error('[run-full-sync] Fatal:', err);
    return new Response(JSON.stringify({ status: 'error', message: err.message }), { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
  }
});
