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
 * - exports/_pipeline/indices.json (indici stock/price per chunking)
 * - exports/_pipeline/partial_products.tsv (prodotti parziali durante chunking)
 * 
 * Steps:
 * - parse_merge: Parse e merge file FTP (CHUNKED - 5000 righe per invocazione)
 * - ean_mapping: Mapping EAN da file CSV
 * - pricing: Calcolo prezzi finali
 * - export_ean: Generazione catalogo EAN
 * - export_mediaworld: Generazione export Mediaworld
 * - export_eprice: Generazione export ePrice
 */

// ========== CONFIGURAZIONE CHUNKING ==========
const CHUNK_SIZE = 5000; // Righe di material file processate per ogni invocazione

const PRODUCTS_FILE_PATH = '_pipeline/products.tsv';
const PARTIAL_PRODUCTS_FILE_PATH = '_pipeline/partial_products.tsv';
const INDICES_FILE_PATH = '_pipeline/indices.json';
const EAN_CATALOG_FILE_PATH = '_pipeline/ean_catalog.tsv';

// ========== COLUMN ALIASES (case-insensitive matching) ==========
const COLUMN_ALIASES: Record<string, string[]> = {
  'Matnr': ['matnr', 'mat_nr', 'material_nr', 'materialnr', 'material', 'sku', 'product_id', 'productid', 'id'],
  'ManufPartNr': ['manufpartnr', 'manuf_part_nr', 'mpn', 'manufacturer_part_nr', 'partnr', 'part_nr', 'partno', 'part_no'],
  'EAN': ['ean', 'ean13', 'ean_code', 'barcode', 'upc', 'gtin'],
  'ShortDescription': ['shortdescription', 'short_description', 'description', 'desc', 'name', 'product_name', 'title'],
  'ExistingStock': ['existingstock', 'existing_stock', 'stock', 'qty', 'quantity', 'available', 'available_stock', 'onhand', 'on_hand'],
  'ListPrice': ['listprice', 'list_price', 'price', 'retail_price', 'retailprice', 'msrp'],
  'CustBestPrice': ['custbestprice', 'cust_best_price', 'cost', 'cost_price', 'costprice', 'buy_price', 'buyprice', 'wholesale'],
  'Surcharge': ['surcharge', 'sur_charge', 'fee', 'markup', 'extra_cost', 'extracost'],
};

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

function detectDelimiter(firstLine: string): string {
  const delimiters = ['\t', ';', ',', '|'];
  let bestDelimiter = '\t';
  let maxCount = 0;
  
  for (const d of delimiters) {
    const count = (firstLine.match(new RegExp(d.replace(/[|\\{}()[\]^$+*?.]/g, '\\$&'), 'g')) || []).length;
    if (count > maxCount) {
      maxCount = count;
      bestDelimiter = d;
    }
  }
  
  console.log(`[parser] Detected delimiter: "${bestDelimiter === '\t' ? 'TAB' : bestDelimiter}" (${maxCount} occurrences)`);
  return bestDelimiter;
}

function findColumnIndex(headers: string[], columnName: string): { index: number; matchedAs: string } {
  const normalizedHeaders = headers.map(h => h.trim().toLowerCase().replace(/[\s_-]+/g, ''));
  const aliases = COLUMN_ALIASES[columnName] || [columnName.toLowerCase()];
  
  for (const alias of aliases) {
    const normalizedAlias = alias.toLowerCase().replace(/[\s_-]+/g, '');
    const idx = normalizedHeaders.indexOf(normalizedAlias);
    if (idx !== -1) return { index: idx, matchedAs: headers[idx] };
  }
  
  for (const alias of aliases) {
    const normalizedAlias = alias.toLowerCase().replace(/[\s_-]+/g, '');
    for (let i = 0; i < normalizedHeaders.length; i++) {
      if (normalizedHeaders[i].includes(normalizedAlias) || normalizedAlias.includes(normalizedHeaders[i])) {
        return { index: i, matchedAs: headers[i] };
      }
    }
  }
  
  return { index: -1, matchedAs: '' };
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

async function deleteFromStorage(supabase: any, bucket: string, path: string): Promise<void> {
  try {
    await supabase.storage.from(bucket).remove([path]);
    console.log(`[storage] Deleted ${bucket}/${path}`);
  } catch (e) {
    console.log(`[storage] Failed to delete ${bucket}/${path}:`, e);
  }
}

// ========== PARSE_MERGE STATE MANAGEMENT ==========

interface ParseMergeState {
  status: 'pending' | 'building_indices' | 'in_progress' | 'completed' | 'failed';
  offset: number;
  productCount: number;
  skipped: { noStock: number; noPrice: number; lowStock: number; noValid: number };
  materialBytes: number;
  startTime: number;
  error?: string;
}

async function getParseMergeState(supabase: any, runId: string): Promise<ParseMergeState | null> {
  const { data: run } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
  return run?.steps?.parse_merge || null;
}

async function updateParseMergeState(supabase: any, runId: string, state: Partial<ParseMergeState>): Promise<void> {
  const { data: run } = await supabase.from('sync_runs').select('steps, metrics').eq('id', runId).single();
  const currentSteps = run?.steps || {};
  const currentMetrics = run?.metrics || {};
  
  const updatedParseMerge = { ...currentSteps.parse_merge, ...state };
  const updatedSteps = { ...currentSteps, parse_merge: updatedParseMerge, current_step: 'parse_merge' };
  
  // Update metrics if completed
  const updatedMetrics = state.status === 'completed' ? {
    ...currentMetrics,
    products_total: (state.productCount || 0) + Object.values(state.skipped || {}).reduce((a: number, b: any) => a + (b || 0), 0),
    products_processed: state.productCount || 0
  } : currentMetrics;
  
  await supabase.from('sync_runs').update({ steps: updatedSteps, metrics: updatedMetrics }).eq('id', runId);
  console.log(`[parse_merge] State updated: status=${state.status}, offset=${state.offset}, products=${state.productCount}`);
}

// ========== INDICES STORAGE (for chunked processing) ==========

interface StoredIndices {
  stockIndex: Record<string, number>;
  priceIndex: Record<string, [number, number, number]>;
  materialMeta: {
    delimiter: string;
    matnrIdx: number;
    mpnIdx: number;
    eanIdx: number;
    descIdx: number;
    headerEndPos: number;
  };
}

async function saveIndicesToStorage(supabase: any, indices: StoredIndices): Promise<{ success: boolean; error?: string }> {
  const json = JSON.stringify(indices);
  console.log(`[parse_merge] Saving indices to storage: ${json.length} bytes`);
  return await uploadToStorage(supabase, 'exports', INDICES_FILE_PATH, json, 'application/json');
}

async function loadIndicesFromStorage(supabase: any): Promise<{ indices: StoredIndices | null; error?: string }> {
  const { content, error } = await downloadFromStorage(supabase, 'exports', INDICES_FILE_PATH);
  if (error || !content) {
    return { indices: null, error: error || 'Empty content' };
  }
  try {
    const indices = JSON.parse(content) as StoredIndices;
    console.log(`[parse_merge] Loaded indices: stock=${Object.keys(indices.stockIndex).length}, price=${Object.keys(indices.priceIndex).length}`);
    return { indices };
  } catch (e: any) {
    return { indices: null, error: `JSON parse error: ${e.message}` };
  }
}

// ========== STEP: PARSE_MERGE (CHUNKED VERSION) ==========

async function stepParseMerge(supabase: any, runId: string): Promise<{ success: boolean; error?: string; status?: string }> {
  console.log(`[parse_merge] Starting for run ${runId}, CHUNK_SIZE=${CHUNK_SIZE}`);
  const invocationStart = Date.now();
  
  try {
    // Check current state
    let state = await getParseMergeState(supabase, runId);
    
    // If already completed, skip
    if (state?.status === 'completed') {
      console.log(`[parse_merge] Already completed, skipping`);
      return { success: true, status: 'completed' };
    }
    
    // If failed, don't retry automatically
    if (state?.status === 'failed') {
      console.log(`[parse_merge] Previously failed: ${state.error}`);
      return { success: false, error: state.error, status: 'failed' };
    }
    
    // PHASE 1: Build indices (first invocation only)
    if (!state || state.status === 'pending' || state.status === 'building_indices') {
      console.log(`[parse_merge] Phase 1: Building indices...`);
      
      await updateParseMergeState(supabase, runId, {
        status: 'building_indices',
        offset: 0,
        productCount: 0,
        skipped: { noStock: 0, noPrice: 0, lowStock: 0, noValid: 0 },
        startTime: Date.now()
      });
      
      // Build stock index
      console.log(`[parse_merge] Loading stock file...`);
      const stockResult = await getLatestFile(supabase, 'stock');
      if (!stockResult.content) {
        const error = 'Stock file mancante o non leggibile';
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      const stockFirstNewline = stockResult.content.indexOf('\n');
      const stockHeaderLine = stockResult.content.substring(0, stockFirstNewline).trim();
      const stockDelimiter = detectDelimiter(stockHeaderLine);
      const stockHeaders = stockHeaderLine.split(stockDelimiter).map(h => h.trim());
      
      const stockMatnr = findColumnIndex(stockHeaders, 'Matnr');
      const stockQty = findColumnIndex(stockHeaders, 'ExistingStock');
      
      console.log(`[parse_merge] Stock columns: Matnr=${stockMatnr.index}, ExistingStock=${stockQty.index}`);
      
      if (stockMatnr.index === -1 || stockQty.index === -1) {
        const error = `Stock headers non validi. Matnr=${stockMatnr.index}, ExistingStock=${stockQty.index}`;
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      // Build stock index inline
      const stockIndex: Record<string, number> = Object.create(null);
      let pos = stockFirstNewline + 1;
      const stockContent = stockResult.content;
      while (pos < stockContent.length) {
        let lineEnd = stockContent.indexOf('\n', pos);
        if (lineEnd === -1) lineEnd = stockContent.length;
        const line = stockContent.substring(pos, lineEnd);
        pos = lineEnd + 1;
        if (!line.trim()) continue;
        const vals = line.split(stockDelimiter);
        const key = vals[stockMatnr.index]?.trim();
        if (key) stockIndex[key] = parseInt(vals[stockQty.index]) || 0;
      }
      console.log(`[parse_merge] Stock index: ${Object.keys(stockIndex).length} entries`);
      
      // Build price index
      console.log(`[parse_merge] Loading price file...`);
      const priceResult = await getLatestFile(supabase, 'price');
      if (!priceResult.content) {
        const error = 'Price file mancante o non leggibile';
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      const priceFirstNewline = priceResult.content.indexOf('\n');
      const priceHeaderLine = priceResult.content.substring(0, priceFirstNewline).trim();
      const priceDelimiter = detectDelimiter(priceHeaderLine);
      const priceHeaders = priceHeaderLine.split(priceDelimiter).map(h => h.trim());
      
      const priceMatnr = findColumnIndex(priceHeaders, 'Matnr');
      const priceLp = findColumnIndex(priceHeaders, 'ListPrice');
      const priceCbp = findColumnIndex(priceHeaders, 'CustBestPrice');
      const priceSur = findColumnIndex(priceHeaders, 'Surcharge');
      
      console.log(`[parse_merge] Price columns: Matnr=${priceMatnr.index}, LP=${priceLp.index}, CBP=${priceCbp.index}, Sur=${priceSur.index}`);
      
      if (priceMatnr.index === -1) {
        const error = `Price headers non validi. Matnr=${priceMatnr.index}`;
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      const parseNum = (v: any) => parseFloat(String(v || '0').replace(',', '.')) || 0;
      
      const priceIndex: Record<string, [number, number, number]> = Object.create(null);
      pos = priceFirstNewline + 1;
      const priceContent = priceResult.content;
      while (pos < priceContent.length) {
        let lineEnd = priceContent.indexOf('\n', pos);
        if (lineEnd === -1) lineEnd = priceContent.length;
        const line = priceContent.substring(pos, lineEnd);
        pos = lineEnd + 1;
        if (!line.trim()) continue;
        const vals = line.split(priceDelimiter);
        const key = vals[priceMatnr.index]?.trim();
        if (key) {
          priceIndex[key] = [
            priceLp.index >= 0 ? parseNum(vals[priceLp.index]) : 0,
            priceCbp.index >= 0 ? parseNum(vals[priceCbp.index]) : 0,
            priceSur.index >= 0 ? parseNum(vals[priceSur.index]) : 0
          ];
        }
      }
      console.log(`[parse_merge] Price index: ${Object.keys(priceIndex).length} entries`);
      
      // Get material file metadata (header only)
      console.log(`[parse_merge] Loading material file header...`);
      const materialResult = await getLatestFile(supabase, 'material');
      if (!materialResult.content) {
        const error = 'Material file mancante o non leggibile';
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      const matFirstNewline = materialResult.content.indexOf('\n');
      if (matFirstNewline === -1) {
        const error = 'Material file vuoto';
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      const matHeaderLine = materialResult.content.substring(0, matFirstNewline).trim();
      const matDelimiter = detectDelimiter(matHeaderLine);
      const matHeaders = matHeaderLine.split(matDelimiter).map(h => h.trim());
      
      const matMatnr = findColumnIndex(matHeaders, 'Matnr');
      const matMpn = findColumnIndex(matHeaders, 'ManufPartNr');
      const matEan = findColumnIndex(matHeaders, 'EAN');
      const matDesc = findColumnIndex(matHeaders, 'ShortDescription');
      
      console.log(`[parse_merge] Material columns: Matnr=${matMatnr.index}, MPN=${matMpn.index}, EAN=${matEan.index}, Desc=${matDesc.index}`);
      
      if (matMatnr.index === -1) {
        const error = `Material headers non validi. Matnr=${matMatnr.index}`;
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      // Save indices to storage
      const indices: StoredIndices = {
        stockIndex,
        priceIndex,
        materialMeta: {
          delimiter: matDelimiter,
          matnrIdx: matMatnr.index,
          mpnIdx: matMpn.index,
          eanIdx: matEan.index,
          descIdx: matDesc.index,
          headerEndPos: matFirstNewline + 1
        }
      };
      
      const saveResult = await saveIndicesToStorage(supabase, indices);
      if (!saveResult.success) {
        const error = `Failed to save indices: ${saveResult.error}`;
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      // Initialize partial products file with header
      const headerTSV = 'Matnr\tMPN\tEAN\tDesc\tStock\tLP\tCBP\tSur\n';
      await uploadToStorage(supabase, 'exports', PARTIAL_PRODUCTS_FILE_PATH, headerTSV, 'text/tab-separated-values');
      
      // Update state to in_progress
      await updateParseMergeState(supabase, runId, {
        status: 'in_progress',
        offset: 0,
        productCount: 0,
        skipped: { noStock: 0, noPrice: 0, lowStock: 0, noValid: 0 },
        materialBytes: materialResult.content.length,
        startTime: Date.now()
      });
      
      console.log(`[parse_merge] Phase 1 complete: indices saved, ready for chunked processing`);
      console.log(`[parse_merge] Invocation took ${Date.now() - invocationStart}ms`);
      return { success: true, status: 'in_progress' };
    }
    
    // PHASE 2: Process chunks
    if (state.status === 'in_progress') {
      console.log(`[parse_merge] Phase 2: Processing chunk from offset ${state.offset}...`);
      
      // Load indices from storage
      const { indices, error: indicesError } = await loadIndicesFromStorage(supabase);
      if (!indices) {
        const error = `Failed to load indices: ${indicesError}`;
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      // Load material file content
      const materialResult = await getLatestFile(supabase, 'material');
      if (!materialResult.content) {
        const error = 'Material file mancante durante chunk processing';
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      const { stockIndex, priceIndex, materialMeta } = indices;
      const materialContent = materialResult.content;
      const materialSize = materialContent.length;
      
      // Calculate starting position
      let pos = materialMeta.headerEndPos;
      let currentLineNum = 0;
      
      // Skip to offset (lines already processed)
      while (pos < materialSize && currentLineNum < state.offset) {
        let lineEnd = materialContent.indexOf('\n', pos);
        if (lineEnd === -1) lineEnd = materialSize;
        pos = lineEnd + 1;
        currentLineNum++;
      }
      
      console.log(`[parse_merge] Starting at line ${currentLineNum}, byte position ${pos}/${materialSize}`);
      
      // Process CHUNK_SIZE lines
      const skipped = { ...state.skipped };
      let productCount = state.productCount;
      let linesProcessed = 0;
      let chunkTSV = '';
      
      while (pos < materialSize && linesProcessed < CHUNK_SIZE) {
        let lineEnd = materialContent.indexOf('\n', pos);
        if (lineEnd === -1) lineEnd = materialSize;
        
        const line = materialContent.substring(pos, lineEnd);
        pos = lineEnd + 1;
        currentLineNum++;
        linesProcessed++;
        
        if (!line.trim()) continue;
        
        const vals = line.split(materialMeta.delimiter);
        const m = vals[materialMeta.matnrIdx]?.trim();
        if (!m) continue;
        
        const stock = stockIndex[m];
        const price = priceIndex[m];
        
        if (stock === undefined) { skipped.noStock++; continue; }
        if (!price) { skipped.noPrice++; continue; }
        if (stock < 2) { skipped.lowStock++; continue; }
        
        const lp = price[0], cbp = price[1], sur = price[2];
        if (lp <= 0 && cbp <= 0) { skipped.noValid++; continue; }
        
        const mpn = materialMeta.mpnIdx >= 0 ? (vals[materialMeta.mpnIdx]?.trim() || '') : '';
        const ean = materialMeta.eanIdx >= 0 ? (vals[materialMeta.eanIdx]?.trim() || '') : '';
        const desc = materialMeta.descIdx >= 0 ? (vals[materialMeta.descIdx]?.trim() || '') : '';
        
        chunkTSV += `${m}\t${mpn}\t${ean}\t${desc}\t${stock}\t${lp}\t${cbp}\t${sur}\n`;
        productCount++;
      }
      
      console.log(`[parse_merge] Chunk processed: ${linesProcessed} lines, ${productCount - state.productCount} new products, total=${productCount}`);
      
      // Append chunk to partial products file
      if (chunkTSV.length > 0) {
        const { content: existingContent } = await downloadFromStorage(supabase, 'exports', PARTIAL_PRODUCTS_FILE_PATH);
        const updatedContent = (existingContent || '') + chunkTSV;
        await uploadToStorage(supabase, 'exports', PARTIAL_PRODUCTS_FILE_PATH, updatedContent, 'text/tab-separated-values');
      }
      
      // Check if finished
      const isFinished = pos >= materialSize;
      
      if (isFinished) {
        console.log(`[parse_merge] All chunks processed, finalizing...`);
        
        // Move partial to final products file
        const { content: finalContent } = await downloadFromStorage(supabase, 'exports', PARTIAL_PRODUCTS_FILE_PATH);
        if (finalContent) {
          await uploadToStorage(supabase, 'exports', PRODUCTS_FILE_PATH, finalContent, 'text/tab-separated-values');
        }
        
        // Cleanup intermediate files
        await deleteFromStorage(supabase, 'exports', PARTIAL_PRODUCTS_FILE_PATH);
        await deleteFromStorage(supabase, 'exports', INDICES_FILE_PATH);
        
        const durationMs = Date.now() - state.startTime;
        await updateParseMergeState(supabase, runId, {
          status: 'completed',
          offset: currentLineNum,
          productCount,
          skipped
        });
        
        console.log(`[parse_merge] COMPLETED: ${productCount} products in ${durationMs}ms, skipped=${JSON.stringify(skipped)}`);
        console.log(`[parse_merge] Invocation took ${Date.now() - invocationStart}ms`);
        return { success: true, status: 'completed' };
      } else {
        // More chunks to process
        await updateParseMergeState(supabase, runId, {
          status: 'in_progress',
          offset: currentLineNum,
          productCount,
          skipped
        });
        
        console.log(`[parse_merge] Chunk complete, ${currentLineNum}/${Math.ceil(materialSize / 100)} lines processed, more to go`);
        console.log(`[parse_merge] Invocation took ${Date.now() - invocationStart}ms`);
        return { success: true, status: 'in_progress' };
      }
    }
    
    // Unknown state
    const error = `Unknown parse_merge state: ${state?.status}`;
    await updateParseMergeState(supabase, runId, { status: 'failed', error });
    return { success: false, error, status: 'failed' };
    
  } catch (e: any) {
    console.error(`[parse_merge] Error:`, e);
    await updateParseMergeState(supabase, runId, { status: 'failed', error: e.message });
    return { success: false, error: e.message, status: 'failed' };
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
    const { products, error: loadError } = await loadProductsTSV(supabase, runId);
    
    if (loadError || !products) {
      const error = loadError || 'Products file not found';
      console.error(`[sync:step:ean_mapping] Failed to load products: ${error}`);
      await updateStepResult(supabase, runId, 'ean_mapping', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    console.log(`[sync:step:ean_mapping] Loaded ${products.length} products`);
    
    let eanMapped = 0, eanMissing = 0;
    
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

async function updateStepResult(supabase: any, runId: string, stepName: string, result: any): Promise<void> {
  const { data: run } = await supabase.from('sync_runs').select('steps, metrics').eq('id', runId).single();
  const steps = { ...(run?.steps || {}), [stepName]: result, current_step: stepName };
  const metrics = { ...(run?.metrics || {}), ...result.metrics };
  await supabase.from('sync_runs').update({ steps, metrics }).eq('id', runId);
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
    
    const feeDrev = feeConfig?.feeDrev || 1.05;
    const feeMkt = feeConfig?.feeMkt || 1.08;
    const shippingCost = feeConfig?.shippingCost || 6.00;
    
    console.log(`[sync:step:pricing] Processing ${products.length} products with fees: DREV=${feeDrev}, MKT=${feeMkt}, SHIP=${shippingCost}`);
    
    for (const p of products) {
      const base = p.CBP > 0 ? p.CBP : (p.LP > 0 ? p.LP : 0);
      if (base <= 0) {
        p.PF = '';
        p.PFNum = 0;
        p.LPF = '';
        continue;
      }
      
      const baseCents = Math.round(base * 100);
      const shippingCents = Math.round(shippingCost * 100);
      const afterShipping = baseCents + shippingCents;
      const afterIva = Math.round(afterShipping * 1.22);
      const afterFees = Math.round(afterIva * feeDrev * feeMkt);
      const finalCents = toComma99Cents(afterFees);
      
      p.PFNum = finalCents / 100;
      p.PF = (finalCents / 100).toFixed(2).replace('.', ',');
      
      // ListPrice con Fee
      if (p.LP > 0) {
        const lpAfterShipping = Math.round(p.LP * 100) + shippingCents;
        const lpAfterIva = Math.round(lpAfterShipping * 1.22);
        const lpAfterFees = Math.round(lpAfterIva * feeDrev * feeMkt);
        p.LPF = Math.ceil(lpAfterFees / 100).toString();
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
      status: 'success', duration_ms: Date.now() - startTime,
      metrics: { products_priced: products.length }
    });
    
    console.log(`[sync:step:pricing] Completed: ${products.length} products priced`);
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
    
    const eanRows: string[] = [];
    let eanSkipped = 0;
    
    const headers = ['EAN', 'MPN', 'Matnr', 'Descrizione', 'Prezzo', 'ListPrice con Fee', 'Stock'];
    
    for (const p of products) {
      const norm = normalizeEAN(p.EAN);
      if (!norm.ok) {
        eanSkipped++;
        continue;
      }
      
      eanRows.push([
        norm.value,
        p.MPN || '',
        p.Matnr || '',
        (p.Desc || '').replace(/;/g, ','),
        p.PF || '',
        p.LPF || '',
        String(p.Stock || 0)
      ].join(';'));
    }
    
    const eanCSV = [headers.join(';'), ...eanRows].join('\n');
    
    // Save to both locations
    await uploadToStorage(supabase, 'exports', EAN_CATALOG_FILE_PATH, eanCSV, 'text/csv');
    const saveResult = await uploadToStorage(supabase, 'exports', 'Catalogo EAN.csv', eanCSV, 'text/csv');
    
    if (!saveResult.success) {
      const error = `Failed to save Catalogo EAN.csv: ${saveResult.error}`;
      await updateStepResult(supabase, runId, 'export_ean', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    await updateStepResult(supabase, runId, 'export_ean', {
      status: 'success', duration_ms: Date.now() - startTime, rows: eanRows.length, skipped: eanSkipped,
      metrics: { ean_export_rows: eanRows.length, ean_export_skipped: eanSkipped }
    });
    
    console.log(`[sync:step:export_ean] Completed: ${eanRows.length} rows, ${eanSkipped} skipped`);
    return { success: true };
    
  } catch (e: any) {
    console.error(`[sync:step:export_ean] Error:`, e);
    await updateStepResult(supabase, runId, 'export_ean', { status: 'failed', error: e.message, metrics: {} });
    return { success: false, error: e.message };
  }
}

// ========== STEP: EXPORT_MEDIAWORLD ==========
async function stepExportMediaworld(supabase: any, runId: string, prepDays: number): Promise<{ success: boolean; error?: string }> {
  console.log(`[sync:step:export_mediaworld] Starting for run ${runId}, prepDays=${prepDays}`);
  const startTime = Date.now();
  
  try {
    const { products, error: loadError } = await loadProductsTSV(supabase, runId);
    
    if (loadError || !products) {
      const error = loadError || 'Products file not found';
      await updateStepResult(supabase, runId, 'export_mediaworld', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    const mwRows: string[] = [];
    let mwSkipped = 0;
    
    const leadTime = prepDays + 2;
    const headers = ['sku', 'ean', 'price', 'leadtime-to-ship', 'quantity'];
    
    for (const p of products) {
      const norm = normalizeEAN(p.EAN);
      if (!norm.ok) {
        mwSkipped++;
        continue;
      }
      
      if (!p.PFNum || p.PFNum <= 0) {
        mwSkipped++;
        continue;
      }
      
      mwRows.push([
        p.Matnr || '',
        norm.value,
        p.PFNum.toFixed(2).replace('.', ','),
        String(leadTime),
        String(Math.min(p.Stock || 0, 99))
      ].join(';'));
    }
    
    const mwCSV = [headers.join(';'), ...mwRows].join('\n');
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
  console.log(`[sync:step:export_eprice] Starting for run ${runId}, prepDays=${prepDays}`);
  const startTime = Date.now();
  
  try {
    const { products, error: loadError } = await loadProductsTSV(supabase, runId);
    
    if (loadError || !products) {
      const error = loadError || 'Products file not found';
      await updateStepResult(supabase, runId, 'export_eprice', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    const epRows: string[] = [];
    let epSkipped = 0;
    
    const headers = ['sku', 'ean', 'price', 'quantity', 'leadtime'];
    
    for (const p of products) {
      const norm = normalizeEAN(p.EAN);
      if (!norm.ok) {
        epSkipped++;
        continue;
      }
      
      if (!p.PFNum || p.PFNum <= 0) {
        epSkipped++;
        continue;
      }
      
      epRows.push([
        p.Matnr || '',
        norm.value,
        p.PFNum.toFixed(2).replace('.', ','),
        String(Math.min(p.Stock || 0, 99)),
        String(prepDays)
      ].join(';'));
    }
    
    const epCSV = [headers.join(';'), ...epRows].join('\n');
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
    
    let result: { success: boolean; error?: string; status?: string };
    
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
    
    console.log(`[sync-step-runner] Step ${step} result: success=${result.success}, status=${result.status || 'N/A'}, error=${result.error || 'none'}`);
    
    // Return status in response for orchestrator to handle in_progress
    return new Response(JSON.stringify({ 
      status: result.success ? 'ok' : 'error', 
      step_status: result.status,
      ...result 
    }), { status: result.success ? 200 : 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    
  } catch (e: any) {
    console.error('[sync-step-runner] Fatal error:', e);
    return new Response(JSON.stringify({ status: 'error', message: e.message }), 
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
  }
});
