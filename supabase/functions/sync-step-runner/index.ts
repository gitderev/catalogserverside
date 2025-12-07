import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

/**
 * sync-step-runner - STEP PROCESSOR CON CHUNKING
 * 
 * Esegue singoli step della pipeline usando streaming e chunking.
 * I dati intermedi sono salvati come TSV nello storage bucket 'exports'.
 * 
 * File intermedi:
 * - exports/_pipeline/products.tsv (dopo parse_merge)
 * - exports/_pipeline/ean_catalog.tsv (dopo export_ean)
 * 
 * Steps:
 * - parse_merge: Parse e merge file FTP (streaming line-by-line)
 * - ean_mapping: Mapping EAN da file CSV
 * - pricing: Calcolo prezzi finali
 * - export_ean: Generazione catalogo EAN
 * - export_mediaworld: Generazione export Mediaworld
 * - export_eprice: Generazione export ePrice
 */

const PRODUCTS_FILE_PATH = '_pipeline/products.tsv';
const EAN_CATALOG_FILE_PATH = '_pipeline/ean_catalog.tsv';

// ========== UTILITY FUNCTIONS ==========

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
  if (compact.length === 12) return { ok: true, value: '0' + compact };
  if (compact.length === 13) return { ok: true, value: compact };
  if (compact.length === 14) return { ok: true, value: compact.startsWith('0') ? compact.substring(1) : compact };
  return { ok: false, reason: `lunghezza ${compact.length}` };
}

async function getLatestFile(supabase: any, folder: string): Promise<{ content: string | null; fileName: string | null }> {
  console.log(`[storage] Looking for latest file in ftp-import/${folder}`);
  const { data: files, error: listError } = await supabase.storage.from('ftp-import').list(folder, { 
    sortBy: { column: 'created_at', order: 'desc' }, limit: 1 
  });
  
  if (listError) {
    console.error(`[storage] Error listing ftp-import/${folder}:`, listError);
    return { content: null, fileName: null };
  }
  
  if (!files?.length) {
    console.log(`[storage] No files found in ftp-import/${folder}`);
    return { content: null, fileName: null };
  }
  
  const fileName = files[0].name;
  console.log(`[storage] Found file: ${fileName}, downloading...`);
  
  const { data, error: downloadError } = await supabase.storage.from('ftp-import').download(`${folder}/${fileName}`);
  
  if (downloadError) {
    console.error(`[storage] Error downloading ftp-import/${folder}/${fileName}:`, downloadError);
    return { content: null, fileName };
  }
  
  const content = data ? await data.text() : null;
  console.log(`[storage] Downloaded ${fileName}, size: ${content?.length || 0} bytes`);
  return { content, fileName };
}

async function uploadToStorage(supabase: any, bucket: string, path: string, content: string, contentType: string): Promise<{ success: boolean; error?: string }> {
  console.log(`[storage] Uploading to ${bucket}/${path}, size: ${content.length} bytes`);
  
  const { data, error } = await supabase.storage.from(bucket).upload(path, 
    new Blob([content], { type: contentType }), { upsert: true });
  
  if (error) {
    console.error(`[storage] Upload failed for ${bucket}/${path}:`, error);
    return { success: false, error: error.message };
  }
  
  // Verify the file was uploaded by checking if we can list it
  const folder = path.includes('/') ? path.substring(0, path.lastIndexOf('/')) : '';
  const fileName = path.includes('/') ? path.substring(path.lastIndexOf('/') + 1) : path;
  
  const { data: files } = await supabase.storage.from(bucket).list(folder, { search: fileName });
  const fileExists = files?.some((f: any) => f.name === fileName);
  
  if (!fileExists) {
    console.error(`[storage] Upload verification failed: file not found after upload`);
    return { success: false, error: 'File not found after upload' };
  }
  
  console.log(`[storage] Upload successful and verified: ${bucket}/${path}`);
  return { success: true };
}

async function downloadFromStorage(supabase: any, bucket: string, path: string): Promise<{ content: string | null; error?: string }> {
  console.log(`[storage] Downloading from ${bucket}/${path}`);
  
  // First check if file exists
  const folder = path.includes('/') ? path.substring(0, path.lastIndexOf('/')) : '';
  const fileName = path.includes('/') ? path.substring(path.lastIndexOf('/') + 1) : path;
  
  const { data: files, error: listError } = await supabase.storage.from(bucket).list(folder);
  
  if (listError) {
    console.error(`[storage] Error listing ${bucket}/${folder}:`, listError);
    return { content: null, error: `Error listing folder: ${listError.message}` };
  }
  
  console.log(`[storage] Files in ${bucket}/${folder}:`, files?.map((f: any) => f.name).join(', ') || 'none');
  
  const fileExists = files?.some((f: any) => f.name === fileName);
  if (!fileExists) {
    console.error(`[storage] File not found: ${bucket}/${path}`);
    return { content: null, error: `File not found: ${path}` };
  }
  
  const { data, error: downloadError } = await supabase.storage.from(bucket).download(path);
  
  if (downloadError) {
    console.error(`[storage] Download failed for ${bucket}/${path}:`, downloadError);
    return { content: null, error: downloadError.message };
  }
  
  const content = data ? await data.text() : null;
  console.log(`[storage] Downloaded ${bucket}/${path}, size: ${content?.length || 0} bytes`);
  return { content };
}

async function updateStepResult(supabase: any, runId: string, stepName: string, result: any): Promise<void> {
  const { data: run } = await supabase.from('sync_runs').select('steps, metrics').eq('id', runId).single();
  const steps = { ...(run?.steps || {}), [stepName]: result, current_step: stepName };
  const metrics = { ...(run?.metrics || {}), ...result.metrics };
  await supabase.from('sync_runs').update({ steps, metrics }).eq('id', runId);
}

// ========== STEP: PARSE_MERGE (STREAMING) ==========
async function stepParseMerge(supabase: any, runId: string): Promise<{ success: boolean; error?: string }> {
  console.log(`[sync:step:parse_merge] Starting for run ${runId}`);
  const startTime = Date.now();
  
  try {
    // 1. Build stock index
    const { content: stockTxt, fileName: stockFileName } = await getLatestFile(supabase, 'stock');
    if (!stockTxt) {
      const error = 'Stock file mancante o non leggibile';
      await updateStepResult(supabase, runId, 'parse_merge', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    console.log(`[sync:step:parse_merge] Building stock index from ${stockFileName}...`);
    const stockMap = new Map<string, number>();
    const stockLines = stockTxt.split('\n');
    const stockHeaders = stockLines[0]?.split('\t').map(h => h.trim()) || [];
    const matnrIdx = stockHeaders.indexOf('Matnr');
    const stockIdx = stockHeaders.indexOf('ExistingStock');
    
    if (matnrIdx === -1 || stockIdx === -1) {
      const error = `Stock file headers non validi: Matnr=${matnrIdx}, ExistingStock=${stockIdx}`;
      await updateStepResult(supabase, runId, 'parse_merge', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    for (let i = 1; i < stockLines.length; i++) {
      const vals = stockLines[i].split('\t');
      const m = vals[matnrIdx]?.trim();
      if (m) stockMap.set(m, parseInt(vals[stockIdx]) || 0);
    }
    console.log(`[sync:step:parse_merge] Stock entries: ${stockMap.size}`);
    
    // 2. Build price index
    const { content: priceTxt, fileName: priceFileName } = await getLatestFile(supabase, 'price');
    if (!priceTxt) {
      const error = 'Price file mancante o non leggibile';
      await updateStepResult(supabase, runId, 'parse_merge', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    console.log(`[sync:step:parse_merge] Building price index from ${priceFileName}...`);
    const priceMap = new Map<string, { lp: number; cbp: number; sur: number }>();
    const priceLines = priceTxt.split('\n');
    const priceHeaders = priceLines[0]?.split('\t').map(h => h.trim()) || [];
    const priceMatnrIdx = priceHeaders.indexOf('Matnr');
    const lpIdx = priceHeaders.indexOf('ListPrice');
    const cbpIdx = priceHeaders.indexOf('CustBestPrice');
    const surIdx = priceHeaders.indexOf('Surcharge');
    
    if (priceMatnrIdx === -1) {
      const error = `Price file headers non validi: Matnr=${priceMatnrIdx}`;
      await updateStepResult(supabase, runId, 'parse_merge', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    for (let i = 1; i < priceLines.length; i++) {
      const vals = priceLines[i].split('\t');
      const m = vals[priceMatnrIdx]?.trim();
      if (m) {
        const parse = (v: any) => parseFloat(String(v || '0').replace(',', '.')) || 0;
        priceMap.set(m, { lp: parse(vals[lpIdx]), cbp: parse(vals[cbpIdx]), sur: parse(vals[surIdx]) });
      }
    }
    console.log(`[sync:step:parse_merge] Price entries: ${priceMap.size}`);
    
    // 3. Process material file
    const { content: materialTxt, fileName: materialFileName } = await getLatestFile(supabase, 'material');
    if (!materialTxt) {
      const error = 'Material file mancante o non leggibile';
      await updateStepResult(supabase, runId, 'parse_merge', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    console.log(`[sync:step:parse_merge] Processing material file ${materialFileName}...`);
    const materialLines = materialTxt.split('\n');
    const materialHeaders = materialLines[0]?.split('\t').map(h => h.trim()) || [];
    const mMatnrIdx = materialHeaders.indexOf('Matnr');
    const mMpnIdx = materialHeaders.indexOf('ManufPartNr');
    const mEanIdx = materialHeaders.indexOf('EAN');
    const mDescIdx = materialHeaders.indexOf('ShortDescription');
    
    if (mMatnrIdx === -1) {
      const error = `Material file headers non validi: Matnr=${mMatnrIdx}`;
      await updateStepResult(supabase, runId, 'parse_merge', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    const skipped = { noStock: 0, noPrice: 0, lowStock: 0, noValid: 0 };
    let productCount = 0;
    
    const outputLines: string[] = ['Matnr\tMPN\tEAN\tDesc\tStock\tLP\tCBP\tSur'];
    
    for (let i = 1; i < materialLines.length; i++) {
      const line = materialLines[i];
      if (!line.trim()) continue;
      
      const vals = line.split('\t');
      const m = vals[mMatnrIdx]?.trim();
      if (!m) continue;
      
      const stock = stockMap.get(m);
      const price = priceMap.get(m);
      
      if (stock === undefined) { skipped.noStock++; continue; }
      if (!price) { skipped.noPrice++; continue; }
      if (stock < 2) { skipped.lowStock++; continue; }
      if (price.lp <= 0 && price.cbp <= 0) { skipped.noValid++; continue; }
      
      outputLines.push(`${m}\t${vals[mMpnIdx]?.trim() || ''}\t${vals[mEanIdx]?.trim() || ''}\t${vals[mDescIdx]?.trim() || ''}\t${stock}\t${price.lp}\t${price.cbp}\t${price.sur}`);
      productCount++;
    }
    
    console.log(`[sync:step:parse_merge] Products: ${productCount}, skipped: ${JSON.stringify(skipped)}`);
    
    // Save as TSV with verification
    const csvContent = outputLines.join('\n');
    const uploadResult = await uploadToStorage(supabase, 'exports', PRODUCTS_FILE_PATH, csvContent, 'text/tab-separated-values');
    
    if (!uploadResult.success) {
      const error = `Failed to save products file: ${uploadResult.error}`;
      await updateStepResult(supabase, runId, 'parse_merge', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    await updateStepResult(supabase, runId, 'parse_merge', {
      status: 'success', duration_ms: Date.now() - startTime, 
      products: productCount, skipped,
      files: { stock: stockFileName, price: priceFileName, material: materialFileName },
      output_file: PRODUCTS_FILE_PATH,
      metrics: { products_total: productCount + Object.values(skipped).reduce((a, b) => a + b, 0), products_processed: productCount }
    });
    
    console.log(`[sync:step:parse_merge] Completed in ${Date.now() - startTime}ms, saved to ${PRODUCTS_FILE_PATH}`);
    return { success: true };
    
  } catch (e: any) {
    console.error(`[sync:step:parse_merge] Error:`, e);
    await updateStepResult(supabase, runId, 'parse_merge', { status: 'failed', error: e.message, metrics: {} });
    return { success: false, error: e.message };
  }
}

// ========== HELPER: Load/Save Products ==========
async function loadProductsTSV(supabase: any, runId: string): Promise<{ products: any[] | null; error?: string }> {
  console.log(`[sync:products] Loading products for run ${runId} from exports/${PRODUCTS_FILE_PATH}`);
  
  const { content, error } = await downloadFromStorage(supabase, 'exports', PRODUCTS_FILE_PATH);
  
  if (error || !content) {
    console.error(`[sync:products] Failed to load products: ${error || 'empty content'}`);
    return { products: null, error: error || 'Products file not found or empty' };
  }
  
  const lines = content.split('\n');
  const products: any[] = [];
  
  console.log(`[sync:products] Parsing ${lines.length} lines...`);
  
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line.trim()) continue;
    const vals = line.split('\t');
    products.push({
      Matnr: vals[0] || '', MPN: vals[1] || '', EAN: vals[2] || '', Desc: vals[3] || '',
      Stock: parseInt(vals[4]) || 0, LP: parseFloat(vals[5]) || 0, CBP: parseFloat(vals[6]) || 0, Sur: parseFloat(vals[7]) || 0,
      PF: vals[8] || '', PFNum: parseFloat(vals[9]) || 0, LPF: vals[10] || ''
    });
  }
  
  console.log(`[sync:products] Loaded ${products.length} products`);
  return { products };
}

async function saveProductsTSV(supabase: any, products: any[]): Promise<{ success: boolean; error?: string }> {
  const lines = ['Matnr\tMPN\tEAN\tDesc\tStock\tLP\tCBP\tSur\tPF\tPFNum\tLPF'];
  for (const p of products) {
    lines.push(`${p.Matnr}\t${p.MPN}\t${p.EAN}\t${p.Desc}\t${p.Stock}\t${p.LP}\t${p.CBP}\t${p.Sur}\t${p.PF || ''}\t${p.PFNum || ''}\t${p.LPF || ''}`);
  }
  return await uploadToStorage(supabase, 'exports', PRODUCTS_FILE_PATH, lines.join('\n'), 'text/tab-separated-values');
}

// ========== STEP: EAN_MAPPING ==========
async function stepEanMapping(supabase: any, runId: string): Promise<{ success: boolean; error?: string }> {
  console.log(`[sync:step:ean_mapping] Starting for run ${runId}`);
  const startTime = Date.now();
  
  try {
    // Load products with detailed error handling
    const { products, error: loadError } = await loadProductsTSV(supabase, runId);
    
    if (loadError || !products) {
      const error = loadError || 'Products file not found';
      console.error(`[sync:step:ean_mapping] Failed to load products: ${error}`);
      await updateStepResult(supabase, runId, 'ean_mapping', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    console.log(`[sync:step:ean_mapping] Loaded ${products.length} products`);
    
    let eanMapped = 0, eanMissing = 0;
    
    // Load EAN mapping file
    console.log(`[sync:step:ean_mapping] Looking for EAN mapping file in mapping-files/ean`);
    const { data: files, error: listError } = await supabase.storage.from('mapping-files').list('ean', { 
      limit: 1, sortBy: { column: 'created_at', order: 'desc' } 
    });
    
    if (listError) {
      console.error(`[sync:step:ean_mapping] Error listing mapping files:`, listError);
    }
    
    console.log(`[sync:step:ean_mapping] Mapping files found: ${files?.map((f: any) => f.name).join(', ') || 'none'}`);
    
    if (files?.length) {
      const mappingFileName = files[0].name;
      console.log(`[sync:step:ean_mapping] Using mapping file: ${mappingFileName}`);
      
      const { data: mappingBlob, error: downloadError } = await supabase.storage.from('mapping-files').download(`ean/${mappingFileName}`);
      
      if (downloadError) {
        console.error(`[sync:step:ean_mapping] Error downloading mapping file:`, downloadError);
      } else if (mappingBlob) {
        const mappingText = await mappingBlob.text();
        const mappingMap = new Map<string, string>();
        
        for (const line of mappingText.split('\n').slice(1)) {
          const [mpn, ean] = line.split(';').map(s => s?.trim());
          if (mpn && ean) mappingMap.set(mpn, ean);
        }
        console.log(`[sync:step:ean_mapping] Mapping entries loaded: ${mappingMap.size}`);
        
        for (const p of products) {
          if (!p.EAN && p.MPN) {
            const mapped = mappingMap.get(p.MPN);
            if (mapped) { p.EAN = mapped; eanMapped++; }
            else eanMissing++;
          }
        }
      }
    } else {
      console.log(`[sync:step:ean_mapping] No EAN mapping file found, skipping mapping`);
    }
    
    // Save updated products
    const saveResult = await saveProductsTSV(supabase, products);
    if (!saveResult.success) {
      const error = `Failed to save updated products: ${saveResult.error}`;
      await updateStepResult(supabase, runId, 'ean_mapping', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    await updateStepResult(supabase, runId, 'ean_mapping', {
      status: 'success', duration_ms: Date.now() - startTime, mapped: eanMapped, missing: eanMissing,
      metrics: { products_ean_mapped: eanMapped, products_ean_missing: eanMissing }
    });
    
    console.log(`[sync:step:ean_mapping] Completed: mapped=${eanMapped}, missing=${eanMissing}`);
    return { success: true };
    
  } catch (e: any) {
    console.error(`[sync:step:ean_mapping] Error:`, e);
    await updateStepResult(supabase, runId, 'ean_mapping', { status: 'failed', error: e.message, metrics: {} });
    return { success: false, error: e.message };
  }
}

// ========== STEP: PRICING ==========
async function stepPricing(supabase: any, runId: string, feeConfig: any): Promise<{ success: boolean; error?: string }> {
  console.log(`[sync:step:pricing] Starting for run ${runId}`);
  const startTime = Date.now();
  
  try {
    const { products, error: loadError } = await loadProductsTSV(supabase, runId);
    
    if (loadError || !products) {
      const error = loadError || 'Products file not found';
      await updateStepResult(supabase, runId, 'pricing', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    console.log(`[sync:step:pricing] Processing ${products.length} products with feeConfig:`, feeConfig);
    
    for (const p of products) {
      const hasCBP = p.CBP > 0;
      const hasLP = p.LP > 0;
      let baseCents = hasCBP ? Math.round((p.CBP + p.Sur) * 100) : (hasLP ? Math.round(p.LP * 100) : 0);
      
      const shipCents = Math.round(feeConfig.shippingCost * 100);
      const finalCents = toComma99Cents(Math.round(Math.round(Math.round((baseCents + shipCents) * 1.22) * feeConfig.feeDrev) * feeConfig.feeMkt));
      const finalEuros = finalCents / 100;
      
      p.PF = finalEuros.toFixed(2).replace('.', ',');
      p.PFNum = finalEuros;

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
    
    const saveResult = await saveProductsTSV(supabase, products);
    if (!saveResult.success) {
      const error = `Failed to save priced products: ${saveResult.error}`;
      await updateStepResult(supabase, runId, 'pricing', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    await updateStepResult(supabase, runId, 'pricing', {
      status: 'success', duration_ms: Date.now() - startTime, priced: products.length, metrics: {}
    });
    
    console.log(`[sync:step:pricing] Completed in ${Date.now() - startTime}ms`);
    return { success: true };
    
  } catch (e: any) {
    console.error(`[sync:step:pricing] Error:`, e);
    await updateStepResult(supabase, runId, 'pricing', { status: 'failed', error: e.message, metrics: {} });
    return { success: false, error: e.message };
  }
}

// ========== STEP: EXPORT_EAN ==========
async function stepExportEan(supabase: any, runId: string): Promise<{ success: boolean; error?: string }> {
  console.log(`[sync:step:export_ean] Starting for run ${runId}`);
  const startTime = Date.now();
  
  try {
    const { products, error: loadError } = await loadProductsTSV(supabase, runId);
    
    if (loadError || !products) {
      const error = loadError || 'Products file not found';
      await updateStepResult(supabase, runId, 'export_ean', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    console.log(`[sync:step:export_ean] Processing ${products.length} products`);
    
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
    console.log(`[sync:step:export_ean] EAN catalog: ${eanCatalog.length}, discarded: ${discarded}`);
    
    // Save EAN catalog for next steps
    const eanLines = ['Matnr\tMPN\tEAN\tDesc\tStock\tLP\tCBP\tPF\tPFNum\tLPF'];
    for (const p of eanCatalog) {
      eanLines.push(`${p.Matnr}\t${p.MPN}\t${p.EAN}\t${p.Desc}\t${p.Stock}\t${p.LP}\t${p.CBP}\t${p.PF}\t${p.PFNum}\t${p.LPF}`);
    }
    
    const catalogResult = await uploadToStorage(supabase, 'exports', EAN_CATALOG_FILE_PATH, eanLines.join('\n'), 'text/tab-separated-values');
    if (!catalogResult.success) {
      const error = `Failed to save EAN catalog: ${catalogResult.error}`;
      await updateStepResult(supabase, runId, 'export_ean', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    // Generate CSV export
    const eanHeaders = ['Matnr', 'ManufPartNr', 'EAN', 'ShortDescription', 'ExistingStock', 'ListPrice', 'CustBestPrice', 'Prezzo Finale', 'ListPrice con Fee'];
    const eanRows = eanCatalog.map(p => [p.Matnr, p.MPN, p.EAN, p.Desc, p.Stock, p.LP, p.CBP, p.PF, p.LPF].map(v => {
      const s = String(v ?? '');
      return s.includes(';') || s.includes('"') ? `"${s.replace(/"/g, '""')}"` : s;
    }).join(';'));
    const eanCSV = [eanHeaders.join(';'), ...eanRows].join('\n');
    
    const csvResult = await uploadToStorage(supabase, 'exports', 'Catalogo EAN.csv', eanCSV, 'text/csv');
    if (!csvResult.success) {
      const error = `Failed to save Catalogo EAN.csv: ${csvResult.error}`;
      await updateStepResult(supabase, runId, 'export_ean', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    await updateStepResult(supabase, runId, 'export_ean', {
      status: 'success', duration_ms: Date.now() - startTime, rows: eanCatalog.length, discarded,
      metrics: { products_ean_invalid: discarded, products_after_override: products.length, exported_files_count: 1 }
    });
    
    console.log(`[sync:step:export_ean] Completed in ${Date.now() - startTime}ms`);
    return { success: true };
    
  } catch (e: any) {
    console.error(`[sync:step:export_ean] Error:`, e);
    await updateStepResult(supabase, runId, 'export_ean', { status: 'failed', error: e.message, metrics: {} });
    return { success: false, error: e.message };
  }
}

// ========== HELPER: Load EAN Catalog ==========
async function loadEanCatalog(supabase: any, runId: string): Promise<{ products: any[] | null; error?: string }> {
  console.log(`[sync:ean_catalog] Loading EAN catalog for run ${runId}`);
  
  const { content, error } = await downloadFromStorage(supabase, 'exports', EAN_CATALOG_FILE_PATH);
  
  if (error || !content) {
    return { products: null, error: error || 'EAN catalog not found' };
  }
  
  const lines = content.split('\n');
  const products: any[] = [];
  
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line.trim()) continue;
    const vals = line.split('\t');
    products.push({
      Matnr: vals[0] || '', MPN: vals[1] || '', EAN: vals[2] || '', Desc: vals[3] || '',
      Stock: parseInt(vals[4]) || 0, LP: parseFloat(vals[5]) || 0, CBP: parseFloat(vals[6]) || 0,
      PF: vals[7] || '', PFNum: parseFloat(vals[8]) || 0, LPF: vals[9] || ''
    });
  }
  
  console.log(`[sync:ean_catalog] Loaded ${products.length} EAN products`);
  return { products };
}

// ========== STEP: EXPORT_MEDIAWORLD ==========
async function stepExportMediaworld(supabase: any, runId: string, prepDays: number): Promise<{ success: boolean; error?: string }> {
  console.log(`[sync:step:export_mediaworld] Starting for run ${runId}`);
  const startTime = Date.now();
  
  try {
    const { products: eanCatalog, error: loadError } = await loadEanCatalog(supabase, runId);
    
    if (loadError || !eanCatalog) {
      const error = loadError || 'EAN catalog not found';
      await updateStepResult(supabase, runId, 'export_mediaworld', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    console.log(`[sync:step:export_mediaworld] Processing ${eanCatalog.length} products`);
    
    const mwHeaders = ['SKU offerta', 'ID Prodotto', 'Tipo ID prodotto', 'Descrizione offerta', 'Descrizione interna offerta', "Prezzo dell'offerta", 'Info aggiuntive prezzo offerta', "Quantità dell'offerta", 'Avviso quantità minima', "Stato dell'offerta", 'Data di inizio della disponibilità', 'Data di conclusione della disponibilità', 'Classe logistica', 'Prezzo scontato', 'Data di inizio dello sconto', 'Data di termine dello sconto', 'Tempo di preparazione della spedizione (in giorni)', 'Aggiorna/Cancella', 'Tipo di prezzo che verrà barrato quando verrà definito un prezzo scontato.', 'Obbligo di ritiro RAEE', 'Orario di cut-off (solo se la consegna il giorno successivo è abilitata)', 'VAT Rate % (Turkey only)'];
    
    const mwRows: string[] = [];
    let mwSkipped = 0;
    
    for (const p of eanCatalog) {
      if (!p.MPN || p.MPN.length > 40 || !p.EAN || p.EAN.length < 12 || p.Stock <= 0 || !p.LPF || !p.PFNum) { 
        mwSkipped++; continue; 
      }
      const lpf = typeof p.LPF === 'number' ? p.LPF.toFixed(2) : String(p.LPF);
      const pf = p.PFNum.toFixed(2);
      mwRows.push([p.MPN, p.EAN, 'EAN', p.Desc, '', lpf, '', String(p.Stock), '', 'Nuovo', '', '', 'Consegna gratuita', pf, '', '', String(prepDays), '', 'recommended-retail-price', '', '', ''].map(c => c.includes(';') ? `"${c}"` : c).join(';'));
    }
    
    const mwCSV = [mwHeaders.join(';'), ...mwRows].join('\n');
    const saveResult = await uploadToStorage(supabase, 'exports', 'Export Mediaworld.csv', mwCSV, 'text/csv');
    
    if (!saveResult.success) {
      const error = `Failed to save Export Mediaworld.csv: ${saveResult.error}`;
      await updateStepResult(supabase, runId, 'export_mediaworld', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    await updateStepResult(supabase, runId, 'export_mediaworld', {
      status: 'success', duration_ms: Date.now() - startTime, rows: mwRows.length, skipped: mwSkipped,
      metrics: { mediaworld_export_rows: mwRows.length, mediaworld_export_skipped: mwSkipped }
    });
    
    console.log(`[sync:step:export_mediaworld] Completed: ${mwRows.length} rows, ${mwSkipped} skipped`);
    return { success: true };
    
  } catch (e: any) {
    console.error(`[sync:step:export_mediaworld] Error:`, e);
    await updateStepResult(supabase, runId, 'export_mediaworld', { status: 'failed', error: e.message, metrics: {} });
    return { success: false, error: e.message };
  }
}

// ========== STEP: EXPORT_EPRICE ==========
async function stepExportEprice(supabase: any, runId: string, prepDays: number): Promise<{ success: boolean; error?: string }> {
  console.log(`[sync:step:export_eprice] Starting for run ${runId}`);
  const startTime = Date.now();
  
  try {
    const { products: eanCatalog, error: loadError } = await loadEanCatalog(supabase, runId);
    
    if (loadError || !eanCatalog) {
      const error = loadError || 'EAN catalog not found';
      await updateStepResult(supabase, runId, 'export_eprice', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    console.log(`[sync:step:export_eprice] Processing ${eanCatalog.length} products`);
    
    const epHeaders = ['EAN', 'SKU', 'Titolo', 'Prezzo', 'Quantita', 'Tempo Consegna'];
    const epRows: string[] = [];
    let epSkipped = 0;
    
    for (const p of eanCatalog) {
      if (!p.EAN || !p.MPN || p.Stock <= 0 || !p.PFNum) { epSkipped++; continue; }
      epRows.push([p.EAN, p.MPN, p.Desc, p.PF, String(p.Stock), String(prepDays)].map(c => String(c).includes(';') ? `"${c}"` : c).join(';'));
    }
    
    const epCSV = [epHeaders.join(';'), ...epRows].join('\n');
    const saveResult = await uploadToStorage(supabase, 'exports', 'Export ePrice.csv', epCSV, 'text/csv');
    
    if (!saveResult.success) {
      const error = `Failed to save Export ePrice.csv: ${saveResult.error}`;
      await updateStepResult(supabase, runId, 'export_eprice', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    await updateStepResult(supabase, runId, 'export_eprice', {
      status: 'success', duration_ms: Date.now() - startTime, rows: epRows.length, skipped: epSkipped,
      metrics: { eprice_export_rows: epRows.length, eprice_export_skipped: epSkipped }
    });
    
    console.log(`[sync:step:export_eprice] Completed: ${epRows.length} rows, ${epSkipped} skipped`);
    return { success: true };
    
  } catch (e: any) {
    console.error(`[sync:step:export_eprice] Error:`, e);
    await updateStepResult(supabase, runId, 'export_eprice', { status: 'failed', error: e.message, metrics: {} });
    return { success: false, error: e.message };
  }
}

// ========== MAIN HANDLER ==========
serve(async (req) => {
  if (req.method === 'OPTIONS') return new Response(null, { headers: corsHeaders });
  if (req.method !== 'POST') {
    return new Response(JSON.stringify({ status: 'error', message: 'Method not allowed' }), 
      { status: 405, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
  }

  const supabaseUrl = Deno.env.get('SUPABASE_URL')!;
  const supabaseServiceKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
  const supabase = createClient(supabaseUrl, supabaseServiceKey);

  try {
    const body = await req.json();
    const { run_id, step, fee_config } = body;
    
    if (!run_id || !step) {
      return new Response(JSON.stringify({ status: 'error', message: 'run_id and step required' }), 
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }
    
    console.log(`[sync-step-runner] ========================================`);
    console.log(`[sync-step-runner] Executing step: ${step} for run: ${run_id}`);
    console.log(`[sync-step-runner] ========================================`);
    
    let result: { success: boolean; error?: string };
    
    switch (step) {
      case 'parse_merge':
        result = await stepParseMerge(supabase, run_id);
        break;
      case 'ean_mapping':
        result = await stepEanMapping(supabase, run_id);
        break;
      case 'pricing':
        result = await stepPricing(supabase, run_id, fee_config);
        break;
      case 'export_ean':
        result = await stepExportEan(supabase, run_id);
        break;
      case 'export_mediaworld':
        result = await stepExportMediaworld(supabase, run_id, fee_config?.mediaworldPrepDays || 3);
        break;
      case 'export_eprice':
        result = await stepExportEprice(supabase, run_id, fee_config?.epricePrepDays || 1);
        break;
      default:
        return new Response(JSON.stringify({ status: 'error', message: `Unknown step: ${step}` }), 
          { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }
    
    console.log(`[sync-step-runner] Step ${step} result: ${result.success ? 'SUCCESS' : 'FAILED'} ${result.error || ''}`);
    
    return new Response(JSON.stringify({ status: result.success ? 'ok' : 'error', ...result }), 
      { status: result.success ? 200 : 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    
  } catch (e: any) {
    console.error('[sync-step-runner] Fatal error:', e);
    return new Response(JSON.stringify({ status: 'error', message: e.message }), 
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
  }
});
