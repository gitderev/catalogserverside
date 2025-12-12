import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";
import * as XLSX from "https://esm.sh/xlsx@0.18.5";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

/**
 * sync-step-runner - STEP PROCESSOR CON HTTP RANGE CHUNKING
 * 
 * Esegue singoli step della pipeline usando HTTP Range per letture byte-accurate.
 * Output per-chunk su Storage, finalizzazione a segmenti senza append crescente.
 * 
 * parse_merge phases:
 * - preparing_material: Read 256KB header, determine delimiter/columns
 * - material_ready: Header parsed, ready for chunked processing
 * - processing_chunk: Read 500 lines per tick via HTTP Range
 * - finalizing: Concatenate chunks into segments
 * - completed / failed
 */

// ========== CONFIGURATION ==========
const CHUNK_SIZE_LINES = 500;
const INITIAL_RANGE_BYTES = 2 * 1024 * 1024; // 2MB initial read
const MAX_RANGE_BYTES = 6 * 1024 * 1024; // 6MB max per tick
const RANGE_EXTEND_BYTES = 1 * 1024 * 1024; // 1MB extension
const MAX_CARRY_BYTES = 64 * 1024; // 64KB max carry
const HEADER_PROBE_BYTES = 256 * 1024; // 256KB for header detection
const CHUNKS_PER_SEGMENT = 10; // Chunks per segment during finalization
const SEGMENT_MAX_BYTES = 5 * 1024 * 1024; // 5MB max per segment

// ========== PATH FUNCTIONS ==========
function productsFilePath(runId: string): string { return `_pipeline/${runId}/products.tsv`; }
function chunkFilePath(runId: string, chunkIndex: number): string { 
  const padded = String(chunkIndex).padStart(6, '0');
  return `_pipeline/${runId}/partial/chunk${padded}.tsv`; 
}
function segmentFilePath(runId: string, segmentIndex: number): string {
  const padded = String(segmentIndex).padStart(4, '0');
  return `_pipeline/${runId}/final/segments/segment${padded}.tsv`;
}
function manifestFilePath(runId: string): string { return `_pipeline/${runId}/final/manifest.json`; }

// Export paths
function exportEanPath(runId: string): string { return `runs/${runId}/Catalogo EAN.xlsx`; }
function exportMediaworldPath(runId: string): string { return `runs/${runId}/Export Mediaworld.xlsx`; }
function exportEpricePath(runId: string): string { return `runs/${runId}/Export ePrice.xlsx`; }

// ========== LOCATION ID CONSTANTS ==========
const LOCATION_ID_IT = 4242;
const LOCATION_ID_EU = 4254;
const LOCATION_ID_EU_DUPLICATE = 4255;

// ========== STOCK LOCATION WARNINGS TYPE ==========
interface StockLocationWarnings {
  missing_location_file: number;
  invalid_location_parse: number;
  missing_location_data: number;
  split_mismatch: number;
  multi_mpn_per_matnr: number;
  orphan_4255: number;
  decode_fallback_used: number;
  invalid_stock_value: number;
}

function createEmptyWarnings(): StockLocationWarnings {
  return {
    missing_location_file: 0, invalid_location_parse: 0, missing_location_data: 0,
    split_mismatch: 0, multi_mpn_per_matnr: 0, orphan_4255: 0, decode_fallback_used: 0, invalid_stock_value: 0
  };
}

// ========== RESOLVE MARKETPLACE STOCK ==========
interface ResolveMarketplaceStockResult {
  exportQty: number;
  leadDays: number;
  shouldExport: boolean;
  source: 'IT' | 'EU_FALLBACK' | 'NONE';
}

function resolveMarketplaceStock(stockIT: number, stockEU: number, includeEU: boolean, daysIT: number, daysEU: number): ResolveMarketplaceStockResult {
  if (!includeEU) {
    const exportQty = stockIT;
    const shouldExport = exportQty >= 2;
    return { exportQty, leadDays: shouldExport ? daysIT : 0, shouldExport, source: shouldExport ? 'IT' : 'NONE' };
  }
  if (stockIT >= 2) return { exportQty: stockIT, leadDays: daysIT, shouldExport: true, source: 'IT' };
  const combined = stockIT + stockEU;
  const shouldExport = combined >= 2;
  return { exportQty: combined, leadDays: shouldExport ? daysEU : 0, shouldExport, source: shouldExport ? 'EU_FALLBACK' : 'NONE' };
}

// ========== UPDATE HELPERS ==========
async function updateLocationWarnings(supabase: any, runId: string, warnings: StockLocationWarnings): Promise<void> {
  try {
    await supabase.from('sync_runs').update({ location_warnings: warnings }).eq('id', runId);
  } catch (e) { console.error(`[sync-step-runner] Failed to update location_warnings:`, e); }
}

async function updateExportFilePath(supabase: any, runId: string, exportKey: 'ean' | 'eprice' | 'mediaworld', filePath: string): Promise<void> {
  try {
    const { data: run } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
    const currentSteps = run?.steps || {};
    if (!currentSteps.exports) currentSteps.exports = { files: {} };
    if (!currentSteps.exports.files) currentSteps.exports.files = {};
    currentSteps.exports.files[exportKey] = filePath;
    await supabase.from('sync_runs').update({ steps: currentSteps }).eq('id', runId);
    console.log(`[sync-step-runner] Saved export file path: ${exportKey} -> ${filePath}`);
  } catch (e) { console.error(`[sync-step-runner] Failed to update export file path:`, e); }
}

// ========== COLUMN ALIASES ==========
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
    if (count > maxCount) { maxCount = count; bestDelimiter = d; }
  }
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

// ========== STORAGE FUNCTIONS ==========
async function uploadToStorage(supabase: any, bucket: string, path: string, content: string | Uint8Array, contentType: string): Promise<{ success: boolean; error?: string }> {
  const blob = typeof content === 'string' 
    ? new Blob([content], { type: contentType })
    : new Blob([content], { type: contentType });
  const { error } = await supabase.storage.from(bucket).upload(path, blob, { upsert: true });
  if (error) {
    console.error(`[storage] Upload failed for ${bucket}/${path}:`, error);
    return { success: false, error: error.message };
  }
  return { success: true };
}

async function downloadFromStorage(supabase: any, bucket: string, path: string): Promise<{ content: string | null; error?: string }> {
  const { data, error } = await supabase.storage.from(bucket).download(path);
  if (error) return { content: null, error: error.message };
  const content = data ? await data.text() : null;
  return { content };
}

async function downloadBytesFromStorage(supabase: any, bucket: string, path: string): Promise<{ bytes: Uint8Array | null; error?: string }> {
  const { data, error } = await supabase.storage.from(bucket).download(path);
  if (error) return { bytes: null, error: error.message };
  const buffer = data ? await data.arrayBuffer() : null;
  return { bytes: buffer ? new Uint8Array(buffer) : null };
}

async function deleteFromStorage(supabase: any, bucket: string, path: string): Promise<void> {
  try { await supabase.storage.from(bucket).remove([path]); } catch (_) {}
}

async function listStorageFiles(supabase: any, bucket: string, folder: string): Promise<string[]> {
  const { data, error } = await supabase.storage.from(bucket).list(folder);
  if (error || !data) return [];
  return data.map((f: any) => f.name);
}

// ========== PARSE_MERGE STATE ==========
interface ParseMergeState {
  status: 'pending' | 'running' | 'completed' | 'failed';
  phase: 'preparing_material' | 'material_ready' | 'processing_chunk' | 'waiting_retry' | 'finalizing' | 'completed' | 'failed';
  chunkIndex: number;
  offsetBytes: number;
  chunkSizeLines: number;
  carryBytesB64: string;
  productCount: number;
  skipped: { noStock: number; noPrice: number; lowStock: number; noValid: number };
  materialMeta?: {
    delimiter: string;
    headerLine: string;
    headerMap: Record<string, number>;
    newline: string;
    totalBytes: number;
    headerEndBytes: number;
  };
  attempt: number;
  attemptAt: number;
  startTime: number;
  durationMs: number;
  last_error?: string;
  last_error_at?: string;
  productsPath?: string;
  finalize?: {
    segmentIndex: number;
    nextChunkToPack: number;
    totalChunks: number;
    segmentPaths: string[];
  };
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
  
  const updatedMetrics = state.status === 'completed' ? {
    ...currentMetrics,
    products_total: (state.productCount || 0) + Object.values(state.skipped || {}).reduce((a: number, b: any) => a + (b || 0), 0),
    products_processed: state.productCount || 0
  } : currentMetrics;
  
  await supabase.from('sync_runs').update({ steps: updatedSteps, metrics: updatedMetrics }).eq('id', runId);
}

// ========== GET INPUT FILE PATHS FROM IMPORT_FTP STEP ==========
async function getInputFilePaths(supabase: any, runId: string): Promise<{ materialPath?: string; pricePath?: string; stockPath?: string; stockLocationPath?: string }> {
  const { data: run } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
  const inputFiles = run?.steps?.import_ftp?.details?.input_files || {};
  return {
    materialPath: inputFiles.materialPath,
    pricePath: inputFiles.pricePath,
    stockPath: inputFiles.stockPath,
    stockLocationPath: inputFiles.stockLocationPath
  };
}

// ========== HTTP RANGE FETCH ==========
async function fetchRange(url: string, start: number, end: number): Promise<{ bytes: Uint8Array; contentRange: string | null; status: number }> {
  const resp = await fetch(url, {
    headers: { 'Range': `bytes=${start}-${end}` }
  });
  const bytes = new Uint8Array(await resp.arrayBuffer());
  return {
    bytes,
    contentRange: resp.headers.get('Content-Range'),
    status: resp.status
  };
}

function parseContentRange(header: string | null): { start: number; end: number; total: number } | null {
  if (!header) return null;
  const match = header.match(/bytes (\d+)-(\d+)\/(\d+|\*)/);
  if (!match) return null;
  return {
    start: parseInt(match[1]),
    end: parseInt(match[2]),
    total: match[3] === '*' ? -1 : parseInt(match[3])
  };
}

// ========== STEP: PARSE_MERGE (HTTP RANGE BASED) ==========
async function stepParseMerge(supabase: any, runId: string): Promise<{ success: boolean; error?: string; status?: string }> {
  console.log(`[parse_merge] Starting for run ${runId}`);
  const invocationStart = Date.now();
  
  try {
    let state = await getParseMergeState(supabase, runId);
    
    // Already completed
    if (state?.status === 'completed' || state?.phase === 'completed') {
      console.log(`[parse_merge] Already completed`);
      return { success: true, status: 'completed' };
    }
    
    // Previously failed
    if (state?.status === 'failed' || state?.phase === 'failed') {
      return { success: false, error: state.last_error || 'Previously failed', status: 'failed' };
    }
    
    // Get input file paths
    const inputFiles = await getInputFilePaths(supabase, runId);
    
    // ========== HARD-BLOCK: Validate required input files ==========
    if (!inputFiles.materialPath) {
      const error = 'HARD-BLOCK: materialPath mancante in steps.import_ftp.details.input_files';
      await updateParseMergeState(supabase, runId, { status: 'failed', phase: 'failed', last_error: error, last_error_at: new Date().toISOString() });
      return { success: false, error, status: 'failed' };
    }
    if (!inputFiles.pricePath) {
      const error = 'HARD-BLOCK: pricePath mancante in steps.import_ftp.details.input_files';
      await updateParseMergeState(supabase, runId, { status: 'failed', phase: 'failed', last_error: error, last_error_at: new Date().toISOString() });
      return { success: false, error, status: 'failed' };
    }
    if (!inputFiles.stockPath) {
      const error = 'HARD-BLOCK: stockPath mancante in steps.import_ftp.details.input_files';
      await updateParseMergeState(supabase, runId, { status: 'failed', phase: 'failed', last_error: error, last_error_at: new Date().toISOString() });
      return { success: false, error, status: 'failed' };
    }
    
    // ========== PHASE: PREPARING_MATERIAL ==========
    if (!state || state.status === 'pending' || state.phase === 'preparing_material' || !state.materialMeta) {
      console.log(`[parse_merge] Phase: preparing_material`);
      
      // Save initial checkpoint
      await updateParseMergeState(supabase, runId, {
        status: 'running',
        phase: 'preparing_material',
        chunkIndex: 0,
        offsetBytes: 0,
        chunkSizeLines: CHUNK_SIZE_LINES,
        carryBytesB64: '',
        productCount: 0,
        skipped: { noStock: 0, noPrice: 0, lowStock: 0, noValid: 0 },
        attempt: (state?.attempt || 0) + 1,
        attemptAt: Date.now(),
        startTime: state?.startTime || Date.now(),
        durationMs: 0
      });
      
      // Get signed URL for material file
      const { data: signedData, error: signedError } = await supabase.storage
        .from('ftp-import')
        .createSignedUrl(inputFiles.materialPath, 3600);
      
      if (signedError || !signedData?.signedUrl) {
        const error = `Failed to get signed URL for material: ${signedError?.message || 'Unknown'}`;
        await updateParseMergeState(supabase, runId, { status: 'failed', phase: 'failed', last_error: error });
        return { success: false, error, status: 'failed' };
      }
      
      // PROBE: Check HTTP Range support
      const probeResult = await fetchRange(signedData.signedUrl, 0, 1);
      if (probeResult.status !== 206) {
        const error = `RangeNotSupported: Material file storage non supporta HTTP Range (status ${probeResult.status})`;
        await updateParseMergeState(supabase, runId, { status: 'failed', phase: 'failed', last_error: error });
        return { success: false, error, status: 'failed' };
      }
      
      const totalRange = parseContentRange(probeResult.contentRange);
      const totalBytes = totalRange?.total || -1;
      console.log(`[parse_merge] Material file total bytes: ${totalBytes}`);
      
      // Read header (256KB)
      const headerResult = await fetchRange(signedData.signedUrl, 0, HEADER_PROBE_BYTES - 1);
      let headerText = new TextDecoder('utf-8').decode(headerResult.bytes);
      
      // Remove BOM
      if (headerText.charCodeAt(0) === 0xFEFF) {
        headerText = headerText.substring(1);
      }
      
      // Detect newline
      const newline = headerText.includes('\r\n') ? '\r\n' : '\n';
      const firstNewlineIdx = headerText.indexOf(newline);
      if (firstNewlineIdx === -1) {
        const error = 'Material file: no newline found in header probe';
        await updateParseMergeState(supabase, runId, { status: 'failed', phase: 'failed', last_error: error });
        return { success: false, error, status: 'failed' };
      }
      
      const headerLine = headerText.substring(0, firstNewlineIdx).trim();
      const delimiter = detectDelimiter(headerLine);
      const headers = headerLine.split(delimiter).map(h => h.trim());
      
      console.log(`[parse_merge] Headers: [${headers.slice(0, 10).join(', ')}...]`);
      console.log(`[parse_merge] Delimiter: "${delimiter === '\t' ? 'TAB' : delimiter}", Newline: "${newline === '\r\n' ? 'CRLF' : 'LF'}"`);
      
      // Build header map
      const headerMap: Record<string, number> = {};
      const requiredCols = ['Matnr', 'ManufPartNr', 'EAN', 'ShortDescription'];
      for (const col of requiredCols) {
        const found = findColumnIndex(headers, col);
        headerMap[col] = found.index;
        console.log(`[parse_merge] Column ${col}: index=${found.index} (${found.matchedAs})`);
      }
      
      if (headerMap['Matnr'] === -1) {
        const error = `MissingRequiredColumn: Matnr not found. Headers: [${headers.join(', ')}]`;
        await updateParseMergeState(supabase, runId, { status: 'failed', phase: 'failed', last_error: error });
        return { success: false, error, status: 'failed' };
      }
      
      // Calculate header end position in bytes (after first newline)
      const headerBytes = new TextEncoder().encode(headerText.substring(0, firstNewlineIdx + newline.length));
      const headerEndBytes = headerBytes.length;
      
      // Build stock and price indices
      console.log(`[parse_merge] Building stock index from ${inputFiles.stockPath}...`);
      const { bytes: stockBytes, error: stockError } = await downloadBytesFromStorage(supabase, 'ftp-import', inputFiles.stockPath);
      if (stockError || !stockBytes) {
        const error = `Failed to download stock file: ${stockError}`;
        await updateParseMergeState(supabase, runId, { status: 'failed', phase: 'failed', last_error: error });
        return { success: false, error, status: 'failed' };
      }
      
      const stockContent = new TextDecoder('utf-8').decode(stockBytes);
      const stockLines = stockContent.split(/\r?\n/);
      const stockHeaderLine = stockLines[0] || '';
      const stockDelimiter = detectDelimiter(stockHeaderLine);
      const stockHeaders = stockHeaderLine.split(stockDelimiter).map(h => h.trim());
      const stockMatnrIdx = findColumnIndex(stockHeaders, 'Matnr').index;
      const stockQtyIdx = findColumnIndex(stockHeaders, 'ExistingStock').index;
      
      const stockIndex: Record<string, number> = {};
      for (let i = 1; i < stockLines.length; i++) {
        const line = stockLines[i];
        if (!line.trim()) continue;
        const vals = line.split(stockDelimiter);
        const key = vals[stockMatnrIdx]?.trim();
        if (key) stockIndex[key] = parseInt(vals[stockQtyIdx]) || 0;
      }
      console.log(`[parse_merge] Stock index built: ${Object.keys(stockIndex).length} entries`);
      
      console.log(`[parse_merge] Building price index from ${inputFiles.pricePath}...`);
      const { bytes: priceBytes, error: priceError } = await downloadBytesFromStorage(supabase, 'ftp-import', inputFiles.pricePath);
      if (priceError || !priceBytes) {
        const error = `Failed to download price file: ${priceError}`;
        await updateParseMergeState(supabase, runId, { status: 'failed', phase: 'failed', last_error: error });
        return { success: false, error, status: 'failed' };
      }
      
      const priceContent = new TextDecoder('utf-8').decode(priceBytes);
      const priceLines = priceContent.split(/\r?\n/);
      const priceHeaderLine = priceLines[0] || '';
      const priceDelimiter = detectDelimiter(priceHeaderLine);
      const priceHeaders = priceHeaderLine.split(priceDelimiter).map(h => h.trim());
      const priceMatnrIdx = findColumnIndex(priceHeaders, 'Matnr').index;
      const priceLpIdx = findColumnIndex(priceHeaders, 'ListPrice').index;
      const priceCbpIdx = findColumnIndex(priceHeaders, 'CustBestPrice').index;
      const priceSurIdx = findColumnIndex(priceHeaders, 'Surcharge').index;
      
      const parseNum = (v: any) => parseFloat(String(v || '0').replace(',', '.')) || 0;
      const priceIndex: Record<string, [number, number, number]> = {};
      for (let i = 1; i < priceLines.length; i++) {
        const line = priceLines[i];
        if (!line.trim()) continue;
        const vals = line.split(priceDelimiter);
        const key = vals[priceMatnrIdx]?.trim();
        if (key) {
          priceIndex[key] = [
            priceLpIdx >= 0 ? parseNum(vals[priceLpIdx]) : 0,
            priceCbpIdx >= 0 ? parseNum(vals[priceCbpIdx]) : 0,
            priceSurIdx >= 0 ? parseNum(vals[priceSurIdx]) : 0
          ];
        }
      }
      console.log(`[parse_merge] Price index built: ${Object.keys(priceIndex).length} entries`);
      
      // Save indices to storage for later chunks
      await uploadToStorage(supabase, 'pipeline', `indices/${runId}/stock.json`, JSON.stringify(stockIndex), 'application/json');
      await uploadToStorage(supabase, 'pipeline', `indices/${runId}/price.json`, JSON.stringify(priceIndex), 'application/json');
      
      // Save material metadata and move to material_ready
      await updateParseMergeState(supabase, runId, {
        status: 'running',
        phase: 'material_ready',
        materialMeta: {
          delimiter,
          headerLine,
          headerMap,
          newline,
          totalBytes: totalBytes > 0 ? totalBytes : 0,
          headerEndBytes
        },
        offsetBytes: headerEndBytes,
        chunkIndex: 0,
        durationMs: Date.now() - invocationStart
      });
      
      console.log(`[parse_merge] Phase preparing_material complete in ${Date.now() - invocationStart}ms`);
      return { success: true, status: 'material_ready' };
    }
    
    // ========== PHASE: PROCESSING_CHUNK ==========
    if (state.phase === 'material_ready' || state.phase === 'processing_chunk') {
      console.log(`[parse_merge] Phase: processing_chunk (offset=${state.offsetBytes}, chunk=${state.chunkIndex})`);
      
      // Get signed URL
      const { data: signedData } = await supabase.storage
        .from('ftp-import')
        .createSignedUrl(inputFiles.materialPath, 3600);
      
      if (!signedData?.signedUrl) {
        const error = 'Failed to get signed URL for chunk processing';
        await updateParseMergeState(supabase, runId, { phase: 'waiting_retry', last_error: error, last_error_at: new Date().toISOString() });
        return { success: false, error, status: 'waiting_retry' };
      }
      
      // Load indices from storage
      const { content: stockJson } = await downloadFromStorage(supabase, 'pipeline', `indices/${runId}/stock.json`);
      const { content: priceJson } = await downloadFromStorage(supabase, 'pipeline', `indices/${runId}/price.json`);
      
      if (!stockJson || !priceJson) {
        const error = 'Indices not found - need to restart from preparing_material';
        await updateParseMergeState(supabase, runId, { status: 'failed', phase: 'failed', last_error: error });
        return { success: false, error, status: 'failed' };
      }
      
      const stockIndex: Record<string, number> = JSON.parse(stockJson);
      const priceIndex: Record<string, [number, number, number]> = JSON.parse(priceJson);
      
      const meta = state.materialMeta!;
      let offsetBytes = state.offsetBytes;
      const totalBytes = meta.totalBytes;
      
      // Prepend carry bytes from previous chunk
      let carryBytes: Uint8Array = new Uint8Array(0);
      if (state.carryBytesB64) {
        try {
          const binary = atob(state.carryBytesB64);
          carryBytes = new Uint8Array(binary.length);
          for (let i = 0; i < binary.length; i++) carryBytes[i] = binary.charCodeAt(i);
        } catch (_) {}
      }
      
      // Read range with extension until we get enough lines
      let rangeStart = offsetBytes;
      let rangeEnd = Math.min(rangeStart + INITIAL_RANGE_BYTES - 1, totalBytes > 0 ? totalBytes - 1 : rangeStart + INITIAL_RANGE_BYTES - 1);
      let fetchedBytes = new Uint8Array(0);
      let linesComplete = 0;
      let totalBytesRead = 0;
      
      while (linesComplete < CHUNK_SIZE_LINES && totalBytesRead < MAX_RANGE_BYTES) {
        const result = await fetchRange(signedData.signedUrl, rangeStart, rangeEnd);
        fetchedBytes = result.bytes;
        totalBytesRead = fetchedBytes.length;
        
        // Combine carry + fetched
        const combined = new Uint8Array(carryBytes.length + fetchedBytes.length);
        combined.set(carryBytes, 0);
        combined.set(fetchedBytes, carryBytes.length);
        
        // Decode and count lines
        let text = new TextDecoder('utf-8').decode(combined);
        if (text.charCodeAt(0) === 0xFEFF) text = text.substring(1);
        
        // Normalize newlines
        text = text.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
        
        // Count complete lines
        const lastNewline = text.lastIndexOf('\n');
        if (lastNewline === -1) {
          linesComplete = 0;
        } else {
          linesComplete = text.substring(0, lastNewline).split('\n').length;
        }
        
        // Check if we reached EOF
        const contentRange = parseContentRange(result.contentRange);
        if (contentRange && contentRange.end >= contentRange.total - 1) {
          // EOF reached
          break;
        }
        
        if (linesComplete < CHUNK_SIZE_LINES && totalBytesRead < MAX_RANGE_BYTES) {
          // Extend range
          rangeStart = rangeEnd + 1;
          rangeEnd = Math.min(rangeStart + RANGE_EXTEND_BYTES - 1, totalBytes > 0 ? totalBytes - 1 : rangeStart + RANGE_EXTEND_BYTES - 1);
          // Append to fetchedBytes
          const extended = await fetchRange(signedData.signedUrl, rangeStart, rangeEnd);
          const newFetched = new Uint8Array(fetchedBytes.length + extended.bytes.length);
          newFetched.set(fetchedBytes, 0);
          newFetched.set(extended.bytes, fetchedBytes.length);
          fetchedBytes = newFetched;
          totalBytesRead = fetchedBytes.length;
        } else {
          break;
        }
      }
      
      // Combine carry + all fetched
      const allBytes = new Uint8Array(carryBytes.length + fetchedBytes.length);
      allBytes.set(carryBytes, 0);
      allBytes.set(fetchedBytes, carryBytes.length);
      
      let text = new TextDecoder('utf-8').decode(allBytes);
      if (text.charCodeAt(0) === 0xFEFF) text = text.substring(1);
      text = text.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
      
      // Find complete lines
      const lastNewline = text.lastIndexOf('\n');
      let completeText = lastNewline >= 0 ? text.substring(0, lastNewline) : '';
      let remainderText = lastNewline >= 0 ? text.substring(lastNewline + 1) : text;
      
      // Process up to CHUNK_SIZE_LINES
      const lines = completeText.split('\n');
      const linesToProcess = lines.slice(0, CHUNK_SIZE_LINES);
      const leftoverLines = lines.slice(CHUNK_SIZE_LINES);
      
      if (leftoverLines.length > 0) {
        // Put back leftover complete lines into remainder
        remainderText = leftoverLines.join('\n') + '\n' + remainderText;
      }
      
      // Calculate bytes consumed
      const processedText = linesToProcess.join('\n') + (linesToProcess.length > 0 ? '\n' : '');
      const processedBytes = new TextEncoder().encode(processedText).length;
      const newOffsetBytes = offsetBytes + processedBytes - carryBytes.length;
      
      // Process lines
      const { delimiter, headerMap } = meta;
      const matnrIdx = headerMap['Matnr'];
      const mpnIdx = headerMap['ManufPartNr'];
      const eanIdx = headerMap['EAN'];
      const descIdx = headerMap['ShortDescription'];
      
      const chunkProducts: string[] = [];
      let productCount = state.productCount || 0;
      const skipped = { ...state.skipped };
      
      for (const line of linesToProcess) {
        if (!line.trim()) continue;
        const vals = line.split(delimiter);
        const matnr = vals[matnrIdx]?.trim() || '';
        if (!matnr) { skipped.noValid++; continue; }
        
        const mpn = mpnIdx >= 0 ? vals[mpnIdx]?.trim() || '' : '';
        const ean = eanIdx >= 0 ? vals[eanIdx]?.trim() || '' : '';
        const desc = descIdx >= 0 ? vals[descIdx]?.trim() || '' : '';
        
        const stock = stockIndex[matnr] ?? 0;
        const priceData = priceIndex[matnr];
        const lp = priceData?.[0] || 0;
        const cbp = priceData?.[1] || 0;
        const sur = priceData?.[2] || 0;
        
        if (stock < 2) { skipped.lowStock++; continue; }
        if (cbp <= 0 && lp <= 0) { skipped.noPrice++; continue; }
        
        chunkProducts.push(`${matnr}\t${mpn}\t${ean}\t${desc}\t${stock}\t${lp}\t${cbp}\t${sur}\t\t\t`);
        productCount++;
      }
      
      // Save chunk to storage
      if (chunkProducts.length > 0) {
        const chunkPath = chunkFilePath(runId, state.chunkIndex);
        await uploadToStorage(supabase, 'exports', chunkPath, chunkProducts.join('\n'), 'text/tab-separated-values');
        console.log(`[parse_merge] Chunk ${state.chunkIndex} saved: ${chunkProducts.length} products`);
      }
      
      // Calculate new carry
      const newCarryBytes = new TextEncoder().encode(remainderText);
      if (newCarryBytes.length > MAX_CARRY_BYTES) {
        const error = `CarryTooLarge: ${newCarryBytes.length} bytes exceeds ${MAX_CARRY_BYTES} limit`;
        await updateParseMergeState(supabase, runId, { status: 'failed', phase: 'failed', last_error: error });
        return { success: false, error, status: 'failed' };
      }
      
      const newCarryB64 = newCarryBytes.length > 0 ? btoa(String.fromCharCode(...newCarryBytes)) : '';
      
      // Check for EOF
      const isEOF = newOffsetBytes >= totalBytes || (linesToProcess.length < CHUNK_SIZE_LINES && remainderText.trim() === '');
      
      if (isEOF) {
        console.log(`[parse_merge] EOF reached, moving to finalization`);
        await updateParseMergeState(supabase, runId, {
          phase: 'finalizing',
          offsetBytes: newOffsetBytes,
          chunkIndex: state.chunkIndex + 1,
          productCount,
          skipped,
          carryBytesB64: '',
          durationMs: Date.now() - (state.startTime || Date.now()),
          finalize: {
            segmentIndex: 0,
            nextChunkToPack: 0,
            totalChunks: state.chunkIndex + 1,
            segmentPaths: []
          }
        });
        return { success: true, status: 'finalizing' };
      }
      
      // More chunks to process
      await updateParseMergeState(supabase, runId, {
        phase: 'processing_chunk',
        offsetBytes: newOffsetBytes,
        chunkIndex: state.chunkIndex + 1,
        productCount,
        skipped,
        carryBytesB64: newCarryB64,
        durationMs: Date.now() - (state.startTime || Date.now())
      });
      
      console.log(`[parse_merge] Chunk ${state.chunkIndex} complete: offset=${newOffsetBytes}, products=${productCount}, took ${Date.now() - invocationStart}ms`);
      return { success: true, status: 'processing_chunk' };
    }
    
    // ========== PHASE: FINALIZING ==========
    if (state.phase === 'finalizing') {
      console.log(`[parse_merge] Phase: finalizing (segment=${state.finalize?.segmentIndex}, nextChunk=${state.finalize?.nextChunkToPack})`);
      
      const finalize = state.finalize!;
      const chunksToProcess = Math.min(CHUNKS_PER_SEGMENT, finalize.totalChunks - finalize.nextChunkToPack);
      
      if (chunksToProcess <= 0) {
        // All chunks processed, create manifest and finalize
        const manifestPath = manifestFilePath(runId);
        const manifest = {
          totalProducts: state.productCount,
          segments: finalize.segmentPaths,
          skipped: state.skipped,
          createdAt: new Date().toISOString()
        };
        await uploadToStorage(supabase, 'exports', manifestPath, JSON.stringify(manifest, null, 2), 'application/json');
        
        // If single segment, also create products.tsv for compatibility
        let productsPath: string;
        if (finalize.segmentPaths.length === 1) {
          // Copy segment to products.tsv
          const { content } = await downloadFromStorage(supabase, 'exports', finalize.segmentPaths[0]);
          if (content) {
            const header = 'Matnr\tMPN\tEAN\tDesc\tStock\tLP\tCBP\tSur\tPF\tPFNum\tLPF';
            await uploadToStorage(supabase, 'exports', productsFilePath(runId), header + '\n' + content, 'text/tab-separated-values');
          }
          productsPath = 'exports/' + productsFilePath(runId);
        } else {
          productsPath = 'exports/' + manifestPath;
        }
        
        // Cleanup indices
        await deleteFromStorage(supabase, 'pipeline', `indices/${runId}/stock.json`);
        await deleteFromStorage(supabase, 'pipeline', `indices/${runId}/price.json`);
        
        await updateParseMergeState(supabase, runId, {
          status: 'completed',
          phase: 'completed',
          productsPath,
          durationMs: Date.now() - (state.startTime || Date.now())
        });
        
        console.log(`[parse_merge] COMPLETED: ${state.productCount} products, ${finalize.segmentPaths.length} segments`);
        return { success: true, status: 'completed' };
      }
      
      // Concatenate chunks into segment
      let segmentContent = '';
      for (let i = 0; i < chunksToProcess; i++) {
        const chunkIdx = finalize.nextChunkToPack + i;
        const chunkPath = chunkFilePath(runId, chunkIdx);
        const { content } = await downloadFromStorage(supabase, 'exports', chunkPath);
        if (content) {
          segmentContent += (segmentContent ? '\n' : '') + content;
        }
      }
      
      // Save segment
      const segPath = segmentFilePath(runId, finalize.segmentIndex);
      await uploadToStorage(supabase, 'exports', segPath, segmentContent, 'text/tab-separated-values');
      
      // Update finalize state
      const newFinalize = {
        ...finalize,
        segmentIndex: finalize.segmentIndex + 1,
        nextChunkToPack: finalize.nextChunkToPack + chunksToProcess,
        segmentPaths: [...finalize.segmentPaths, segPath]
      };
      
      await updateParseMergeState(supabase, runId, {
        finalize: newFinalize,
        durationMs: Date.now() - (state.startTime || Date.now())
      });
      
      console.log(`[parse_merge] Segment ${finalize.segmentIndex} created from chunks ${finalize.nextChunkToPack}-${finalize.nextChunkToPack + chunksToProcess - 1}`);
      return { success: true, status: 'finalizing' };
    }
    
    const error = `Unknown parse_merge phase: ${state?.phase}`;
    await updateParseMergeState(supabase, runId, { status: 'failed', phase: 'failed', last_error: error });
    return { success: false, error, status: 'failed' };
    
  } catch (e: any) {
    console.error(`[parse_merge] Error:`, e);
    // Check for WORKER_LIMIT
    if (e.message?.includes('WORKER_LIMIT') || e.message?.includes('546')) {
      await updateParseMergeState(supabase, runId, {
        phase: 'waiting_retry',
        last_error: e.message,
        last_error_at: new Date().toISOString()
      });
      return { success: false, error: e.message, status: 'waiting_retry' };
    }
    await updateParseMergeState(supabase, runId, { status: 'failed', phase: 'failed', last_error: e.message });
    return { success: false, error: e.message, status: 'failed' };
  }
}

// ========== HELPER: Load Products (supports manifest/segments) ==========
async function loadProductsTSV(supabase: any, runId: string): Promise<{ products: any[] | null; error?: string }> {
  // Get productsPath from parse_merge state
  const state = await getParseMergeState(supabase, runId);
  let productsPath = state?.productsPath;
  
  // Fallback to default path
  if (!productsPath) {
    productsPath = 'exports/' + productsFilePath(runId);
  }
  
  // Remove exports/ prefix for download
  const downloadPath = productsPath.replace(/^exports\//, '');
  
  console.log(`[sync:products] Loading products for run ${runId} from ${downloadPath}`);
  
  // Check if it's a manifest
  if (downloadPath.endsWith('manifest.json')) {
    const { content: manifestContent, error: manifestError } = await downloadFromStorage(supabase, 'exports', downloadPath);
    if (manifestError || !manifestContent) {
      return { products: null, error: manifestError || 'Manifest not found' };
    }
    
    const manifest = JSON.parse(manifestContent);
    const products: any[] = [];
    
    // Load each segment
    for (const segPath of manifest.segments) {
      const { content, error } = await downloadFromStorage(supabase, 'exports', segPath);
      if (error || !content) {
        console.error(`[sync:products] Failed to load segment ${segPath}: ${error}`);
        continue;
      }
      
      for (const line of content.split('\n')) {
        if (!line.trim()) continue;
        const vals = line.split('\t');
        products.push({
          Matnr: vals[0] || '', MPN: vals[1] || '', EAN: vals[2] || '', Desc: vals[3] || '',
          Stock: parseInt(vals[4]) || 0, LP: parseFloat(vals[5]) || 0, CBP: parseFloat(vals[6]) || 0, Sur: parseFloat(vals[7]) || 0,
          PF: vals[8] || '', PFNum: parseFloat(vals[9]) || 0, LPF: vals[10] || ''
        });
      }
    }
    
    console.log(`[sync:products] Loaded ${products.length} products from ${manifest.segments.length} segments`);
    return { products };
  }
  
  // Regular TSV file
  const { content, error } = await downloadFromStorage(supabase, 'exports', downloadPath);
  if (error || !content) {
    return { products: null, error: error || 'Products file not found' };
  }
  
  const lines = content.split('\n');
  const products: any[] = [];
  
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

async function saveProductsTSV(supabase: any, runId: string, products: any[]): Promise<{ success: boolean; error?: string }> {
  const lines = ['Matnr\tMPN\tEAN\tDesc\tStock\tLP\tCBP\tSur\tPF\tPFNum\tLPF'];
  for (const p of products) {
    lines.push(`${p.Matnr}\t${p.MPN}\t${p.EAN}\t${p.Desc}\t${p.Stock}\t${p.LP}\t${p.CBP}\t${p.Sur}\t${p.PF || ''}\t${p.PFNum || ''}\t${p.LPF || ''}`);
  }
  return await uploadToStorage(supabase, 'exports', productsFilePath(runId), lines.join('\n'), 'text/tab-separated-values');
}

async function updateStepResult(supabase: any, runId: string, stepName: string, result: any): Promise<void> {
  const { data: run } = await supabase.from('sync_runs').select('steps, metrics').eq('id', runId).single();
  const steps = { ...(run?.steps || {}), [stepName]: { ...(run?.steps?.[stepName] || {}), ...result }, current_step: stepName };
  const metrics = { ...(run?.metrics || {}), ...result.metrics };
  await supabase.from('sync_runs').update({ steps, metrics }).eq('id', runId);
}

// ========== STEP: EAN_MAPPING ==========
async function stepEanMapping(supabase: any, runId: string): Promise<{ success: boolean; error?: string }> {
  console.log(`[sync:step:ean_mapping] Starting for run ${runId}`);
  const startTime = Date.now();
  
  try {
    const { products, error: loadError } = await loadProductsTSV(supabase, runId);
    if (loadError || !products) {
      const error = loadError || 'Products file not found';
      await updateStepResult(supabase, runId, 'ean_mapping', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    let eanMapped = 0, eanMissing = 0;
    
    const { data: files } = await supabase.storage.from('mapping-files').list('ean', { limit: 1, sortBy: { column: 'created_at', order: 'desc' } });
    
    if (files?.length) {
      const { data: mappingBlob } = await supabase.storage.from('mapping-files').download(`ean/${files[0].name}`);
      if (mappingBlob) {
        const mappingText = await mappingBlob.text();
        const mappingMap = new Map<string, string>();
        for (const line of mappingText.split('\n').slice(1)) {
          const [mpn, ean] = line.split(';').map(s => s?.trim());
          if (mpn && ean) mappingMap.set(mpn, ean);
        }
        
        for (const p of products) {
          if (!p.EAN && p.MPN) {
            const mapped = mappingMap.get(p.MPN);
            if (mapped) { p.EAN = mapped; eanMapped++; }
            else eanMissing++;
          }
        }
      }
    }
    
    const saveResult = await saveProductsTSV(supabase, runId, products);
    if (!saveResult.success) {
      await updateStepResult(supabase, runId, 'ean_mapping', { status: 'failed', error: saveResult.error, metrics: {} });
      return { success: false, error: saveResult.error };
    }
    
    await updateStepResult(supabase, runId, 'ean_mapping', {
      status: 'completed', duration_ms: Date.now() - startTime, mapped: eanMapped, missing: eanMissing,
      metrics: { products_ean_mapped: eanMapped, products_ean_missing: eanMissing }
    });
    
    return { success: true };
  } catch (e: any) {
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
      await updateStepResult(supabase, runId, 'pricing', { status: 'failed', error: loadError, metrics: {} });
      return { success: false, error: loadError };
    }
    
    const feeDrev = feeConfig?.feeDrev || 1.05;
    const feeMkt = feeConfig?.feeMkt || 1.08;
    const shippingCost = feeConfig?.shippingCost || 6.00;
    
    for (const p of products) {
      const base = p.CBP > 0 ? p.CBP : (p.LP > 0 ? p.LP : 0);
      if (base <= 0) { p.PF = ''; p.PFNum = 0; p.LPF = ''; continue; }
      
      const baseCents = Math.round(base * 100);
      const shippingCents = Math.round(shippingCost * 100);
      const afterShipping = baseCents + shippingCents;
      const afterIva = Math.round(afterShipping * 1.22);
      const afterFees = Math.round(afterIva * feeDrev * feeMkt);
      const finalCents = toComma99Cents(afterFees);
      
      p.PFNum = finalCents / 100;
      p.PF = (finalCents / 100).toFixed(2).replace('.', ',');
      
      if (p.LP > 0) {
        const lpAfterShipping = Math.round(p.LP * 100) + shippingCents;
        const lpAfterIva = Math.round(lpAfterShipping * 1.22);
        const lpAfterFees = Math.round(lpAfterIva * feeDrev * feeMkt);
        p.LPF = Math.ceil(lpAfterFees / 100).toString();
      } else {
        p.LPF = '';
      }
    }
    
    const saveResult = await saveProductsTSV(supabase, runId, products);
    if (!saveResult.success) {
      await updateStepResult(supabase, runId, 'pricing', { status: 'failed', error: saveResult.error, metrics: {} });
      return { success: false, error: saveResult.error };
    }
    
    await updateStepResult(supabase, runId, 'pricing', {
      status: 'completed', duration_ms: Date.now() - startTime,
      metrics: { products_priced: products.length }
    });
    
    return { success: true };
  } catch (e: any) {
    await updateStepResult(supabase, runId, 'pricing', { status: 'failed', error: e.message, metrics: {} });
    return { success: false, error: e.message };
  }
}

// ========== XLSX GENERATION ==========
function createXLSXBuffer(headers: string[], rows: string[][]): Uint8Array {
  const wsData = [headers, ...rows];
  const ws = XLSX.utils.aoa_to_sheet(wsData);
  ws['!cols'] = headers.map(() => ({ width: 20 }));
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Export');
  const xlsxBuffer = XLSX.write(wb, { type: 'array', bookType: 'xlsx' });
  return new Uint8Array(xlsxBuffer);
}

// ========== STEP: EXPORT_EAN ==========
async function stepExportEan(supabase: any, runId: string): Promise<{ success: boolean; error?: string }> {
  console.log(`[sync:step:export_ean] Starting for run ${runId}`);
  const startTime = Date.now();
  
  try {
    const { products, error: loadError } = await loadProductsTSV(supabase, runId);
    if (loadError || !products) {
      await updateStepResult(supabase, runId, 'export_ean', { status: 'failed', error: loadError, metrics: {} });
      return { success: false, error: loadError };
    }
    
    const eanRows: string[][] = [];
    let eanSkipped = 0;
    const headers = ['EAN', 'MPN', 'Matnr', 'Descrizione', 'Prezzo', 'ListPrice con Fee', 'Stock'];
    
    for (const p of products) {
      const norm = normalizeEAN(p.EAN);
      if (!norm.ok) { eanSkipped++; continue; }
      eanRows.push([norm.value || '', p.MPN || '', p.Matnr || '', (p.Desc || '').replace(/;/g, ','), p.PF || '', p.LPF || '', String(p.Stock || 0)]);
    }
    
    const xlsxBuffer = createXLSXBuffer(headers, eanRows);
    const eanPath = exportEanPath(runId);
    const saveResult = await uploadToStorage(supabase, 'exports', eanPath, xlsxBuffer, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    
    if (!saveResult.success) {
      await updateStepResult(supabase, runId, 'export_ean', { status: 'failed', error: saveResult.error, metrics: {} });
      return { success: false, error: saveResult.error };
    }
    
    await updateExportFilePath(supabase, runId, 'ean', `exports/${eanPath}`);
    await updateStepResult(supabase, runId, 'export_ean', {
      status: 'completed', duration_ms: Date.now() - startTime, rows: eanRows.length, skipped: eanSkipped,
      metrics: { ean_export_rows: eanRows.length, ean_export_skipped: eanSkipped }
    });
    
    return { success: true };
  } catch (e: any) {
    await updateStepResult(supabase, runId, 'export_ean', { status: 'failed', error: e.message, metrics: {} });
    return { success: false, error: e.message };
  }
}

// ========== STEP: EXPORT_MEDIAWORLD ==========
async function stepExportMediaworld(supabase: any, runId: string, feeConfig: any): Promise<{ success: boolean; error?: string }> {
  const includeEu = feeConfig?.mediaworldIncludeEu;
  const itDays = feeConfig?.mediaworldItPrepDays;
  const euDays = feeConfig?.mediaworldEuPrepDays;
  
  if (typeof includeEu !== 'boolean' || typeof itDays !== 'number' || typeof euDays !== 'number') {
    const error = `FAIL-FAST: Mediaworld config invalido: includeEu=${includeEu}, itDays=${itDays}, euDays=${euDays}`;
    await updateStepResult(supabase, runId, 'export_mediaworld', { status: 'failed', error, metrics: {} });
    return { success: false, error };
  }
  
  console.log(`[sync:step:export_mediaworld] Starting for run ${runId}`);
  const startTime = Date.now();
  const warnings = createEmptyWarnings();
  
  try {
    const { products, error: loadError } = await loadProductsTSV(supabase, runId);
    if (loadError || !products) {
      await updateStepResult(supabase, runId, 'export_mediaworld', { status: 'failed', error: loadError, metrics: {} });
      return { success: false, error: loadError };
    }
    
    // Get stock location from input files
    const inputFiles = await getInputFilePaths(supabase, runId);
    let stockLocationIndex: Record<string, { stockIT: number; stockEU: number }> | null = null;
    
    if (inputFiles.stockLocationPath) {
      const { content } = await downloadFromStorage(supabase, 'ftp-import', inputFiles.stockLocationPath);
      if (content) {
        stockLocationIndex = {};
        const lines = content.replace(/\r\n/g, '\n').split('\n');
        const headers = lines[0]?.split(';').map((h: string) => h.trim().toLowerCase()) || [];
        const matnrIdx = headers.indexOf('matnr');
        const stockIdx = headers.indexOf('stock');
        const locationIdx = headers.indexOf('locationid');
        
        if (matnrIdx >= 0 && stockIdx >= 0 && locationIdx >= 0) {
          for (let i = 1; i < lines.length; i++) {
            const vals = lines[i].split(';');
            const matnr = vals[matnrIdx]?.trim();
            if (!matnr) continue;
            const stock = parseInt(vals[stockIdx]) || 0;
            const locationId = parseInt(vals[locationIdx]) || 0;
            if (!stockLocationIndex[matnr]) stockLocationIndex[matnr] = { stockIT: 0, stockEU: 0 };
            if (locationId === LOCATION_ID_IT) stockLocationIndex[matnr].stockIT += stock;
            else if (locationId === LOCATION_ID_EU) stockLocationIndex[matnr].stockEU += stock;
          }
        }
      }
    } else {
      warnings.missing_location_file++;
    }
    
    const mwRows: string[][] = [];
    let mwSkipped = 0;
    const headers = ['sku', 'ean', 'price', 'leadtime-to-ship', 'quantity'];
    
    for (const p of products) {
      const norm = normalizeEAN(p.EAN);
      if (!norm.ok) { mwSkipped++; continue; }
      if (!p.PFNum || p.PFNum <= 0) { mwSkipped++; continue; }
      
      let stockIT = p.Stock || 0;
      let stockEU = 0;
      if (stockLocationIndex?.[p.Matnr]) {
        stockIT = stockLocationIndex[p.Matnr].stockIT;
        stockEU = stockLocationIndex[p.Matnr].stockEU;
      } else if (stockLocationIndex) {
        warnings.missing_location_data++;
        stockIT = 0; stockEU = 0;
      }
      
      const stockResult = resolveMarketplaceStock(stockIT, stockEU, includeEu, itDays, euDays);
      if (!stockResult.shouldExport) { mwSkipped++; continue; }
      
      mwRows.push([p.Matnr || '', norm.value || '', p.PFNum.toFixed(2).replace('.', ','), String(stockResult.leadDays), String(Math.min(stockResult.exportQty, 99))]);
    }
    
    const xlsxBuffer = createXLSXBuffer(headers, mwRows);
    const mwPath = exportMediaworldPath(runId);
    const saveResult = await uploadToStorage(supabase, 'exports', mwPath, xlsxBuffer, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    
    if (!saveResult.success) {
      await updateStepResult(supabase, runId, 'export_mediaworld', { status: 'failed', error: saveResult.error, metrics: {} });
      return { success: false, error: saveResult.error };
    }
    
    await updateLocationWarnings(supabase, runId, warnings);
    await updateExportFilePath(supabase, runId, 'mediaworld', `exports/${mwPath}`);
    await updateStepResult(supabase, runId, 'export_mediaworld', {
      status: 'completed', duration_ms: Date.now() - startTime, rows: mwRows.length, skipped: mwSkipped,
      metrics: { mediaworld_export_rows: mwRows.length, mediaworld_export_skipped: mwSkipped }
    });
    
    return { success: true };
  } catch (e: any) {
    await updateStepResult(supabase, runId, 'export_mediaworld', { status: 'failed', error: e.message, metrics: {} });
    return { success: false, error: e.message };
  }
}

// ========== STEP: EXPORT_EPRICE ==========
async function stepExportEprice(supabase: any, runId: string, feeConfig: any): Promise<{ success: boolean; error?: string }> {
  const includeEu = feeConfig?.epriceIncludeEu;
  const itDays = feeConfig?.epriceItPrepDays;
  const euDays = feeConfig?.epriceEuPrepDays;
  
  if (typeof includeEu !== 'boolean' || typeof itDays !== 'number' || typeof euDays !== 'number') {
    const error = `FAIL-FAST: ePrice config invalido`;
    await updateStepResult(supabase, runId, 'export_eprice', { status: 'failed', error, metrics: {} });
    return { success: false, error };
  }
  
  console.log(`[sync:step:export_eprice] Starting for run ${runId}`);
  const startTime = Date.now();
  
  try {
    const { products, error: loadError } = await loadProductsTSV(supabase, runId);
    if (loadError || !products) {
      await updateStepResult(supabase, runId, 'export_eprice', { status: 'failed', error: loadError, metrics: {} });
      return { success: false, error: loadError };
    }
    
    // Get stock location
    const inputFiles = await getInputFilePaths(supabase, runId);
    let stockLocationIndex: Record<string, { stockIT: number; stockEU: number }> | null = null;
    
    if (inputFiles.stockLocationPath) {
      const { content } = await downloadFromStorage(supabase, 'ftp-import', inputFiles.stockLocationPath);
      if (content) {
        stockLocationIndex = {};
        const lines = content.replace(/\r\n/g, '\n').split('\n');
        const headers = lines[0]?.split(';').map((h: string) => h.trim().toLowerCase()) || [];
        const matnrIdx = headers.indexOf('matnr');
        const stockIdx = headers.indexOf('stock');
        const locationIdx = headers.indexOf('locationid');
        
        if (matnrIdx >= 0 && stockIdx >= 0 && locationIdx >= 0) {
          for (let i = 1; i < lines.length; i++) {
            const vals = lines[i].split(';');
            const matnr = vals[matnrIdx]?.trim();
            if (!matnr) continue;
            const stock = parseInt(vals[stockIdx]) || 0;
            const locationId = parseInt(vals[locationIdx]) || 0;
            if (!stockLocationIndex[matnr]) stockLocationIndex[matnr] = { stockIT: 0, stockEU: 0 };
            if (locationId === 4242) stockLocationIndex[matnr].stockIT += stock;
            else if (locationId === 4254) stockLocationIndex[matnr].stockEU += stock;
          }
        }
      }
    }
    
    const epRows: string[][] = [];
    let epSkipped = 0;
    const exportHeaders = ['sku', 'ean', 'price', 'quantity', 'leadtime'];
    
    for (const p of products) {
      const norm = normalizeEAN(p.EAN);
      if (!norm.ok) { epSkipped++; continue; }
      if (!p.PFNum || p.PFNum <= 0) { epSkipped++; continue; }
      
      let stockIT = p.Stock || 0;
      let stockEU = 0;
      if (stockLocationIndex?.[p.Matnr]) {
        stockIT = stockLocationIndex[p.Matnr].stockIT;
        stockEU = stockLocationIndex[p.Matnr].stockEU;
      }
      
      const stockResult = resolveMarketplaceStock(stockIT, stockEU, includeEu, itDays, euDays);
      if (!stockResult.shouldExport) { epSkipped++; continue; }
      
      epRows.push([p.Matnr || '', norm.value || '', p.PFNum.toFixed(2).replace('.', ','), String(Math.min(stockResult.exportQty, 99)), String(stockResult.leadDays)]);
    }
    
    const xlsxBuffer = createXLSXBuffer(exportHeaders, epRows);
    const epPath = exportEpricePath(runId);
    const saveResult = await uploadToStorage(supabase, 'exports', epPath, xlsxBuffer, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    
    if (!saveResult.success) {
      await updateStepResult(supabase, runId, 'export_eprice', { status: 'failed', error: saveResult.error, metrics: {} });
      return { success: false, error: saveResult.error };
    }
    
    await updateExportFilePath(supabase, runId, 'eprice', `exports/${epPath}`);
    await updateStepResult(supabase, runId, 'export_eprice', {
      status: 'completed', duration_ms: Date.now() - startTime, rows: epRows.length, skipped: epSkipped,
      metrics: { eprice_export_rows: epRows.length, eprice_export_skipped: epSkipped }
    });
    
    return { success: true };
  } catch (e: any) {
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
    
    console.log(`[sync-step-runner] Executing step: ${step} for run: ${run_id}`);
    
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
        result = await stepExportMediaworld(supabase, run_id, fee_config);
        break;
      case 'export_eprice':
        result = await stepExportEprice(supabase, run_id, fee_config);
        break;
      default:
        return new Response(JSON.stringify({ status: 'error', message: `Unknown step: ${step}` }), 
          { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }
    
    console.log(`[sync-step-runner] Step ${step} result: success=${result.success}, status=${result.status || 'N/A'}`);
    
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
