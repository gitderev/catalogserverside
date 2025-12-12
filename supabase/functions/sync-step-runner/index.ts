import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";
import * as XLSX from "https://esm.sh/xlsx@0.18.5";

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
 * 
 * IT/EU Stock Split: Uses resolveMarketplaceStock with golden cases validation
 */

// ========== CONFIGURAZIONE CHUNKING ==========
const CHUNK_SIZE = 5000; // Righe di material file processate per ogni invocazione

// Path functions for run_id-versioned intermediate files (avoids race conditions)
function productsFilePath(runId: string): string { return `_pipeline/${runId}/products.tsv`; }
function partialProductsFilePath(runId: string): string { return `_pipeline/${runId}/partial_products.tsv`; }
function eanCatalogFilePath(runId: string): string { return `_pipeline/${runId}/ean_catalog.tsv`; }

// Export file paths (per-run + latest)
function exportEanPath(runId: string): string { return `runs/${runId}/Catalogo EAN.xlsx`; }
function exportMediaworldPath(runId: string): string { return `runs/${runId}/Export Mediaworld.xlsx`; }
function exportEpricePath(runId: string): string { return `runs/${runId}/Export ePrice.xlsx`; }

// Legacy paths for fallback/compatibility
const LEGACY_PRODUCTS_FILE_PATH = '_pipeline/products.tsv';
const LEGACY_PARTIAL_PRODUCTS_FILE_PATH = '_pipeline/partial_products.tsv';

// ========== LOCATION ID CONSTANTS ==========
const LOCATION_ID_IT = 4242;
const LOCATION_ID_EU = 4254;
const LOCATION_ID_EU_DUPLICATE = 4255; // Ignored in calculations

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
    missing_location_file: 0,
    invalid_location_parse: 0,
    missing_location_data: 0,
    split_mismatch: 0,
    multi_mpn_per_matnr: 0,
    orphan_4255: 0,
    decode_fallback_used: 0,
    invalid_stock_value: 0
  };
}

// ========== RESOLVE MARKETPLACE STOCK - PURE FUNCTION ==========
interface ResolveMarketplaceStockResult {
  exportQty: number;
  leadDays: number;
  shouldExport: boolean;
  source: 'IT' | 'EU_FALLBACK' | 'NONE';
}

function resolveMarketplaceStock(
  stockIT: number,
  stockEU: number,
  includeEU: boolean,
  daysIT: number,
  daysEU: number
): ResolveMarketplaceStockResult {
  if (!includeEU) {
    const exportQty = stockIT;
    const shouldExport = exportQty >= 2;
    return {
      exportQty,
      leadDays: shouldExport ? daysIT : 0,
      shouldExport,
      source: shouldExport ? 'IT' : 'NONE'
    };
  }
  if (stockIT >= 2) {
    return { exportQty: stockIT, leadDays: daysIT, shouldExport: true, source: 'IT' };
  }
  const combined = stockIT + stockEU;
  const shouldExport = combined >= 2;
  return {
    exportQty: combined,
    leadDays: shouldExport ? daysEU : 0,
    shouldExport,
    source: shouldExport ? 'EU_FALLBACK' : 'NONE'
  };
}

// ========== GOLDEN CASES VALIDATION ==========
interface GoldenCase {
  stockIT: number;
  stockEU: number;
  includeEU: boolean;
  expectedExportQty: number;
  expectedShouldExport: boolean;
  expectedSource: 'IT' | 'EU_FALLBACK' | 'NONE';
  expectedLeadSource: 'IT' | 'EU';
}

const GOLDEN_CASES: GoldenCase[] = [
  { stockIT: 2, stockEU: 10, includeEU: true, expectedExportQty: 2, expectedShouldExport: true, expectedSource: 'IT', expectedLeadSource: 'IT' },
  { stockIT: 1, stockEU: 1, includeEU: true, expectedExportQty: 2, expectedShouldExport: true, expectedSource: 'EU_FALLBACK', expectedLeadSource: 'EU' },
  { stockIT: 1, stockEU: 0, includeEU: true, expectedExportQty: 1, expectedShouldExport: false, expectedSource: 'NONE', expectedLeadSource: 'IT' },
  { stockIT: 0, stockEU: 2, includeEU: true, expectedExportQty: 2, expectedShouldExport: true, expectedSource: 'EU_FALLBACK', expectedLeadSource: 'EU' },
  { stockIT: 2, stockEU: 0, includeEU: false, expectedExportQty: 2, expectedShouldExport: true, expectedSource: 'IT', expectedLeadSource: 'IT' },
  { stockIT: 1, stockEU: 10, includeEU: false, expectedExportQty: 1, expectedShouldExport: false, expectedSource: 'NONE', expectedLeadSource: 'IT' },
];

function validateGoldenCases(logPrefix: string, daysIT: number = 2, daysEU: number = 5): { passed: number; failed: number } {
  let passed = 0;
  let failed = 0;
  console.log(`${logPrefix} ========== GOLDEN CASES VALIDATION START ==========`);
  
  for (let i = 0; i < GOLDEN_CASES.length; i++) {
    const tc = GOLDEN_CASES[i];
    const result = resolveMarketplaceStock(tc.stockIT, tc.stockEU, tc.includeEU, daysIT, daysEU);
    const expectedLeadDays = tc.expectedLeadSource === 'IT' ? daysIT : daysEU;
    
    const qtyMatch = result.exportQty === tc.expectedExportQty;
    const exportMatch = result.shouldExport === tc.expectedShouldExport;
    const sourceMatch = result.source === tc.expectedSource;
    const leadMatch = !tc.expectedShouldExport || result.leadDays === expectedLeadDays;
    
    const success = qtyMatch && exportMatch && sourceMatch && leadMatch;
    const status = success ? 'PASS' : 'FAIL';
    
    if (success) passed++; else failed++;
    
    console.log(
      `${logPrefix} Case ${i + 1}: ${status} | IT=${tc.stockIT} EU=${tc.stockEU} includeEU=${tc.includeEU} → qty=${result.exportQty} lead=${result.leadDays} export=${result.shouldExport}`,
      success ? '' : `(expected: qty=${tc.expectedExportQty} lead=${expectedLeadDays} export=${tc.expectedShouldExport})`
    );
  }
  
  console.log(`${logPrefix} ========== GOLDEN CASES VALIDATION END: ${passed}/${GOLDEN_CASES.length} PASSED, ${failed} FAILED ==========`);
  return { passed, failed };
}

// ========== UPDATE LOCATION WARNINGS IN DB ==========
async function updateLocationWarnings(supabase: any, runId: string, warnings: StockLocationWarnings): Promise<void> {
  try {
    await supabase.from('sync_runs').update({ location_warnings: warnings }).eq('id', runId);
    console.log(`[sync-step-runner] Updated location_warnings for run ${runId}:`, warnings);
  } catch (e) {
    console.error(`[sync-step-runner] Failed to update location_warnings:`, e);
  }
}

// ========== UPDATE EXPORT FILE PATH IN DB ==========
// Saves export file paths to sync_runs.steps.exports.files for UI download and SFTP upload
async function updateExportFilePath(supabase: any, runId: string, exportKey: 'ean' | 'eprice' | 'mediaworld', filePath: string): Promise<void> {
  try {
    const { data: run } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
    const currentSteps = run?.steps || {};
    
    // Initialize exports.files structure if not present
    if (!currentSteps.exports) {
      currentSteps.exports = { files: {} };
    }
    if (!currentSteps.exports.files) {
      currentSteps.exports.files = {};
    }
    
    // Set the file path for this export type
    currentSteps.exports.files[exportKey] = filePath;
    
    await supabase.from('sync_runs').update({ steps: currentSteps }).eq('id', runId);
    console.log(`[sync-step-runner] Saved export file path: ${exportKey} -> ${filePath}`);
  } catch (e) {
    console.error(`[sync-step-runner] Failed to update export file path:`, e);
  }
}

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

async function uploadToStorage(supabase: any, bucket: string, path: string, content: string | Uint8Array, contentType: string): Promise<{ success: boolean; error?: string }> {
  const size = typeof content === 'string' ? content.length : content.byteLength;
  console.log(`[storage] Uploading to ${bucket}/${path}, size: ${size} bytes`);
  
  const blob = typeof content === 'string' 
    ? new Blob([content], { type: contentType })
    : new Blob([content], { type: contentType });
  
  const { data, error } = await supabase.storage.from(bucket).upload(path, blob, { upsert: true });
  
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

// Sub-phases for building indices to stay within memory limits:
// - pending: initial state
// - building_stock_index: loading and parsing stock file
// - building_price_index: loading and parsing price file  
// - preparing_material: reading material file metadata
// - in_progress: chunked material processing
// - completed / failed

interface ParseMergeState {
  status: 'pending' | 'building_stock_index' | 'building_price_index' | 'preparing_material' | 'in_progress' | 'completed' | 'failed';
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
  console.log(`[parse_merge] State updated: status=${state.status}, offset=${state.offset ?? 'N/A'}, products=${state.productCount ?? 'N/A'}`);
}

// ========== INDICES STORAGE (versioned by run_id in 'pipeline' bucket) ==========

// Path functions for run_id-versioned indices (avoids race conditions with queued/FIFO runs)
function stockIndexPath(runId: string): string { return `stock-index/${runId}.json`; }
function priceIndexPath(runId: string): string { return `price-index/${runId}.json`; }
function materialMetaPath(runId: string): string { return `material-meta/${runId}.json`; }

// Bucket for pipeline indices (NOT exports - exports bucket only accepts XLSX)
const PIPELINE_BUCKET = 'pipeline';

interface MaterialMeta {
  delimiter: string;
  matnrIdx: number;
  mpnIdx: number;
  eanIdx: number;
  descIdx: number;
  headerEndPos: number;
  totalBytes: number;
}

async function saveStockIndex(supabase: any, runId: string, stockIndex: Record<string, number>): Promise<{ success: boolean; error?: string }> {
  const path = stockIndexPath(runId);
  const json = JSON.stringify(stockIndex);
  const bytes = new TextEncoder().encode(json);
  console.log(`[parse_merge:indices] Saving stock index: run_id=${runId}, bucket=${PIPELINE_BUCKET}, path=${path}, contentType=application/json, bytes=${bytes.length}`);
  const result = await uploadToStorage(supabase, PIPELINE_BUCKET, path, bytes, 'application/json');
  if (!result.success) {
    console.error(`[parse_merge:indices] FAILED to save stock index: run_id=${runId}, bucket=${PIPELINE_BUCKET}, path=${path}, contentType=application/json, bytes=${bytes.length}, error=${result.error}`);
  }
  return result;
}

async function loadStockIndex(supabase: any, runId: string): Promise<{ index: Record<string, number> | null; error?: string }> {
  const path = stockIndexPath(runId);
  console.log(`[parse_merge:indices] Loading stock index: run_id=${runId}, bucket=${PIPELINE_BUCKET}, path=${path}`);
  const { content, error } = await downloadFromStorage(supabase, PIPELINE_BUCKET, path);
  if (error || !content) return { index: null, error: error || 'Empty content' };
  try {
    const index = JSON.parse(content);
    console.log(`[parse_merge:indices] Loaded stock index: ${Object.keys(index).length} entries`);
    return { index };
  } catch (e: any) {
    return { index: null, error: `JSON parse error: ${e.message}` };
  }
}

async function savePriceIndex(supabase: any, runId: string, priceIndex: Record<string, [number, number, number]>): Promise<{ success: boolean; error?: string }> {
  const path = priceIndexPath(runId);
  const json = JSON.stringify(priceIndex);
  const bytes = new TextEncoder().encode(json);
  console.log(`[parse_merge:indices] Saving price index: run_id=${runId}, bucket=${PIPELINE_BUCKET}, path=${path}, contentType=application/json, bytes=${bytes.length}`);
  const result = await uploadToStorage(supabase, PIPELINE_BUCKET, path, bytes, 'application/json');
  if (!result.success) {
    console.error(`[parse_merge:indices] FAILED to save price index: run_id=${runId}, bucket=${PIPELINE_BUCKET}, path=${path}, contentType=application/json, bytes=${bytes.length}, error=${result.error}`);
  }
  return result;
}

async function loadPriceIndex(supabase: any, runId: string): Promise<{ index: Record<string, [number, number, number]> | null; error?: string }> {
  const path = priceIndexPath(runId);
  console.log(`[parse_merge:indices] Loading price index: run_id=${runId}, bucket=${PIPELINE_BUCKET}, path=${path}`);
  const { content, error } = await downloadFromStorage(supabase, PIPELINE_BUCKET, path);
  if (error || !content) return { index: null, error: error || 'Empty content' };
  try {
    const index = JSON.parse(content);
    console.log(`[parse_merge:indices] Loaded price index: ${Object.keys(index).length} entries`);
    return { index };
  } catch (e: any) {
    return { index: null, error: `JSON parse error: ${e.message}` };
  }
}

async function saveMaterialMeta(supabase: any, runId: string, meta: MaterialMeta): Promise<{ success: boolean; error?: string }> {
  const path = materialMetaPath(runId);
  const json = JSON.stringify(meta);
  const bytes = new TextEncoder().encode(json);
  console.log(`[parse_merge:indices] Saving material meta: run_id=${runId}, bucket=${PIPELINE_BUCKET}, path=${path}, contentType=application/json, bytes=${bytes.length}`);
  const result = await uploadToStorage(supabase, PIPELINE_BUCKET, path, bytes, 'application/json');
  if (!result.success) {
    console.error(`[parse_merge:indices] FAILED to save material meta: run_id=${runId}, bucket=${PIPELINE_BUCKET}, path=${path}, contentType=application/json, bytes=${bytes.length}, error=${result.error}`);
  }
  return result;
}

async function loadMaterialMeta(supabase: any, runId: string): Promise<{ meta: MaterialMeta | null; error?: string }> {
  const path = materialMetaPath(runId);
  console.log(`[parse_merge:indices] Loading material meta: run_id=${runId}, bucket=${PIPELINE_BUCKET}, path=${path}`);
  const { content, error } = await downloadFromStorage(supabase, PIPELINE_BUCKET, path);
  if (error || !content) return { meta: null, error: error || 'Empty content' };
  try {
    return { meta: JSON.parse(content) };
  } catch (e: any) {
    return { meta: null, error: `JSON parse error: ${e.message}` };
  }
}

async function cleanupIndexFiles(supabase: any, runId: string): Promise<void> {
  await deleteFromStorage(supabase, PIPELINE_BUCKET, stockIndexPath(runId));
  await deleteFromStorage(supabase, PIPELINE_BUCKET, priceIndexPath(runId));
  await deleteFromStorage(supabase, PIPELINE_BUCKET, materialMetaPath(runId));
  await deleteFromStorage(supabase, 'exports', partialProductsFilePath(runId));
}

// ========== STEP: PARSE_MERGE (MULTI-PHASE CHUNKED VERSION) ==========
// Split into multiple invocations to stay within memory limits:
// Phase 1a: building_stock_index - load and parse stock file, save index
// Phase 1b: building_price_index - load and parse price file, save index  
// Phase 1c: preparing_material - read material file metadata, save it
// Phase 2: in_progress - chunked material processing

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
    
    // ========== PHASE 1a: BUILD STOCK INDEX ==========
    if (!state || state.status === 'pending') {
      console.log(`[parse_merge] Phase 1a: Building stock index...`);
      
      await updateParseMergeState(supabase, runId, {
        status: 'building_stock_index',
        offset: 0,
        productCount: 0,
        skipped: { noStock: 0, noPrice: 0, lowStock: 0, noValid: 0 },
        startTime: Date.now()
      });
      
      // Load and parse stock file
      console.log(`[parse_merge:indices] Loading stock file from ftp-import/stock...`);
      const stockResult = await getLatestFile(supabase, 'stock');
      if (!stockResult.content) {
        const error = `Stock file mancante o non leggibile in ftp-import/stock`;
        console.error(`[parse_merge:indices] ${error}`);
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      console.log(`[parse_merge:indices] Stock file loaded: ${stockResult.fileName}, ${stockResult.content.length} bytes`);
      
      const stockFirstNewline = stockResult.content.indexOf('\n');
      if (stockFirstNewline === -1) {
        const error = 'Stock file vuoto o senza header';
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      const stockHeaderLine = stockResult.content.substring(0, stockFirstNewline).trim();
      const stockDelimiter = detectDelimiter(stockHeaderLine);
      const stockHeaders = stockHeaderLine.split(stockDelimiter).map(h => h.trim());
      
      console.log(`[parse_merge:indices] Stock headers: [${stockHeaders.join(', ')}]`);
      
      const stockMatnr = findColumnIndex(stockHeaders, 'Matnr');
      const stockQty = findColumnIndex(stockHeaders, 'ExistingStock');
      
      console.log(`[parse_merge:indices] Stock column mapping: Matnr=${stockMatnr.index} (${stockMatnr.matchedAs}), ExistingStock=${stockQty.index} (${stockQty.matchedAs})`);
      
      if (stockMatnr.index === -1 || stockQty.index === -1) {
        const error = `Stock headers non validi. Trovati: [${stockHeaders.join(', ')}]. Matnr=${stockMatnr.index}, ExistingStock=${stockQty.index}`;
        console.error(`[parse_merge:indices] ${error}`);
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      // Build stock index
      const stockIndex: Record<string, number> = Object.create(null);
      let pos = stockFirstNewline + 1;
      const stockContent = stockResult.content;
      let stockLineCount = 0;
      
      while (pos < stockContent.length) {
        let lineEnd = stockContent.indexOf('\n', pos);
        if (lineEnd === -1) lineEnd = stockContent.length;
        const line = stockContent.substring(pos, lineEnd);
        pos = lineEnd + 1;
        if (!line.trim()) continue;
        stockLineCount++;
        const vals = line.split(stockDelimiter);
        const key = vals[stockMatnr.index]?.trim();
        if (key) stockIndex[key] = parseInt(vals[stockQty.index]) || 0;
      }
      
      console.log(`[parse_merge:indices] Stock index built: ${Object.keys(stockIndex).length} entries from ${stockLineCount} lines`);
      
      // Save stock index (versioned by run_id in pipeline bucket)
      const saveResult = await saveStockIndex(supabase, runId, stockIndex);
      if (!saveResult.success) {
        const error = `Failed to save stock index: run_id=${runId}, bucket=${PIPELINE_BUCKET}, path=${stockIndexPath(runId)}, error=${saveResult.error}`;
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      // Update state to next phase
      await updateParseMergeState(supabase, runId, { status: 'building_price_index' });
      
      console.log(`[parse_merge] Phase 1a complete in ${Date.now() - invocationStart}ms, stock index saved`);
      return { success: true, status: 'building_price_index' };
    }
    
    // ========== PHASE 1b: BUILD PRICE INDEX ==========
    if (state.status === 'building_stock_index' || state.status === 'building_price_index') {
      console.log(`[parse_merge] Phase 1b: Building price index...`);
      
      await updateParseMergeState(supabase, runId, { status: 'building_price_index' });
      
      // Load and parse price file
      console.log(`[parse_merge:indices] Loading price file from ftp-import/price...`);
      const priceResult = await getLatestFile(supabase, 'price');
      if (!priceResult.content) {
        const error = `Price file mancante o non leggibile in ftp-import/price`;
        console.error(`[parse_merge:indices] ${error}`);
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      console.log(`[parse_merge:indices] Price file loaded: ${priceResult.fileName}, ${priceResult.content.length} bytes`);
      
      const priceFirstNewline = priceResult.content.indexOf('\n');
      if (priceFirstNewline === -1) {
        const error = 'Price file vuoto o senza header';
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      const priceHeaderLine = priceResult.content.substring(0, priceFirstNewline).trim();
      const priceDelimiter = detectDelimiter(priceHeaderLine);
      const priceHeaders = priceHeaderLine.split(priceDelimiter).map(h => h.trim());
      
      console.log(`[parse_merge:indices] Price headers: [${priceHeaders.join(', ')}]`);
      
      const priceMatnr = findColumnIndex(priceHeaders, 'Matnr');
      const priceLp = findColumnIndex(priceHeaders, 'ListPrice');
      const priceCbp = findColumnIndex(priceHeaders, 'CustBestPrice');
      const priceSur = findColumnIndex(priceHeaders, 'Surcharge');
      
      console.log(`[parse_merge:indices] Price column mapping: Matnr=${priceMatnr.index}, LP=${priceLp.index}, CBP=${priceCbp.index}, Sur=${priceSur.index}`);
      
      if (priceMatnr.index === -1) {
        const error = `Price headers non validi. Trovati: [${priceHeaders.join(', ')}]. Matnr non trovato.`;
        console.error(`[parse_merge:indices] ${error}`);
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      const parseNum = (v: any) => parseFloat(String(v || '0').replace(',', '.')) || 0;
      
      // Build price index
      const priceIndex: Record<string, [number, number, number]> = Object.create(null);
      let pos = priceFirstNewline + 1;
      const priceContent = priceResult.content;
      let priceLineCount = 0;
      
      while (pos < priceContent.length) {
        let lineEnd = priceContent.indexOf('\n', pos);
        if (lineEnd === -1) lineEnd = priceContent.length;
        const line = priceContent.substring(pos, lineEnd);
        pos = lineEnd + 1;
        if (!line.trim()) continue;
        priceLineCount++;
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
      
      console.log(`[parse_merge:indices] Price index built: ${Object.keys(priceIndex).length} entries from ${priceLineCount} lines`);
      
      // Save price index (versioned by run_id in pipeline bucket)
      const saveResult = await savePriceIndex(supabase, runId, priceIndex);
      if (!saveResult.success) {
        const error = `Failed to save price index: run_id=${runId}, bucket=${PIPELINE_BUCKET}, path=${priceIndexPath(runId)}, error=${saveResult.error}`;
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      // Update state to next phase
      await updateParseMergeState(supabase, runId, { status: 'preparing_material' });
      
      console.log(`[parse_merge] Phase 1b complete in ${Date.now() - invocationStart}ms, price index saved`);
      return { success: true, status: 'preparing_material' };
    }
    
    // ========== PHASE 1c: PREPARE MATERIAL METADATA ==========
    if (state.status === 'preparing_material') {
      console.log(`[parse_merge] Phase 1c: Preparing material file metadata...`);
      
      // Load material file
      console.log(`[parse_merge:indices] Loading material file from ftp-import/material...`);
      const materialResult = await getLatestFile(supabase, 'material');
      if (!materialResult.content) {
        const error = `Material file mancante o non leggibile in ftp-import/material`;
        console.error(`[parse_merge:indices] ${error}`);
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      console.log(`[parse_merge:indices] Material file loaded: ${materialResult.fileName}, ${materialResult.content.length} bytes`);
      
      const matFirstNewline = materialResult.content.indexOf('\n');
      if (matFirstNewline === -1) {
        const error = 'Material file vuoto o senza header';
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      const matHeaderLine = materialResult.content.substring(0, matFirstNewline).trim();
      const matDelimiter = detectDelimiter(matHeaderLine);
      const matHeaders = matHeaderLine.split(matDelimiter).map(h => h.trim());
      
      console.log(`[parse_merge:indices] Material headers: [${matHeaders.join(', ')}]`);
      
      const matMatnr = findColumnIndex(matHeaders, 'Matnr');
      const matMpn = findColumnIndex(matHeaders, 'ManufPartNr');
      const matEan = findColumnIndex(matHeaders, 'EAN');
      const matDesc = findColumnIndex(matHeaders, 'ShortDescription');
      
      console.log(`[parse_merge:indices] Material column mapping: Matnr=${matMatnr.index}, MPN=${matMpn.index}, EAN=${matEan.index}, Desc=${matDesc.index}`);
      
      if (matMatnr.index === -1) {
        const error = `Material headers non validi. Trovati: [${matHeaders.join(', ')}]. Matnr non trovato.`;
        console.error(`[parse_merge:indices] ${error}`);
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      // Save material metadata
      const meta: MaterialMeta = {
        delimiter: matDelimiter,
        matnrIdx: matMatnr.index,
        mpnIdx: matMpn.index,
        eanIdx: matEan.index,
        descIdx: matDesc.index,
        headerEndPos: matFirstNewline + 1,
        totalBytes: materialResult.content.length
      };
      
      // Save material metadata (versioned by run_id in pipeline bucket)
      const saveResult = await saveMaterialMeta(supabase, runId, meta);
      if (!saveResult.success) {
        const error = `Failed to save material metadata: run_id=${runId}, bucket=${PIPELINE_BUCKET}, path=${materialMetaPath(runId)}, error=${saveResult.error}`;
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      // Initialize partial products file with header (versioned by run_id)
      const headerTSV = 'Matnr\tMPN\tEAN\tDesc\tStock\tLP\tCBP\tSur\n';
      await uploadToStorage(supabase, 'exports', partialProductsFilePath(runId), headerTSV, 'text/tab-separated-values');
      
      // Update state to chunked processing
      await updateParseMergeState(supabase, runId, {
        status: 'in_progress',
        offset: 0,
        productCount: 0,
        skipped: { noStock: 0, noPrice: 0, lowStock: 0, noValid: 0 },
        materialBytes: materialResult.content.length
      });
      
      console.log(`[parse_merge] Phase 1c complete in ${Date.now() - invocationStart}ms, ready for chunked processing`);
      return { success: true, status: 'in_progress' };
    }
    
    // ========== PHASE 2: CHUNKED MATERIAL PROCESSING ==========
    if (state.status === 'in_progress') {
      console.log(`[parse_merge] Phase 2: Processing chunk from offset ${state.offset}...`);
      
      // Load indices and metadata from storage (versioned by run_id from pipeline bucket)
      const { index: stockIndex, error: stockError } = await loadStockIndex(supabase, runId);
      if (!stockIndex) {
        const error = `Failed to load stock index: run_id=${runId}, bucket=${PIPELINE_BUCKET}, path=${stockIndexPath(runId)}, error=${stockError}`;
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      const { index: priceIndex, error: priceError } = await loadPriceIndex(supabase, runId);
      if (!priceIndex) {
        const error = `Failed to load price index: run_id=${runId}, bucket=${PIPELINE_BUCKET}, path=${priceIndexPath(runId)}, error=${priceError}`;
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      const { meta: materialMeta, error: metaError } = await loadMaterialMeta(supabase, runId);
      if (!materialMeta) {
        const error = `Failed to load material metadata: run_id=${runId}, bucket=${PIPELINE_BUCKET}, path=${materialMetaPath(runId)}, error=${metaError}`;
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
      
      // Append chunk to partial products file (versioned by run_id)
      if (chunkTSV.length > 0) {
        const { content: existingContent } = await downloadFromStorage(supabase, 'exports', partialProductsFilePath(runId));
        const updatedContent = (existingContent || '') + chunkTSV;
        await uploadToStorage(supabase, 'exports', partialProductsFilePath(runId), updatedContent, 'text/tab-separated-values');
      }
      
      // Check if finished
      const isFinished = pos >= materialSize;
      
      if (isFinished) {
        console.log(`[parse_merge] All chunks processed, finalizing...`);
        
        // Move partial to final products file (versioned by run_id)
        const { content: finalContent } = await downloadFromStorage(supabase, 'exports', partialProductsFilePath(runId));
        if (finalContent) {
          await uploadToStorage(supabase, 'exports', productsFilePath(runId), finalContent, 'text/tab-separated-values');
        }
        
        // Cleanup intermediate files
        // Cleanup intermediate files (versioned by run_id)
        await cleanupIndexFiles(supabase, runId);
        
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
        
        console.log(`[parse_merge] Chunk complete, ${currentLineNum} lines processed, more to go`);
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
  const path = productsFilePath(runId);
  console.log(`[sync:products] Loading products for run ${runId} from exports/${path}`);
  
  const { content, error } = await downloadFromStorage(supabase, 'exports', path);
  
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

async function saveProductsTSV(supabase: any, runId: string, products: any[]): Promise<{ success: boolean; error?: string }> {
  const lines = ['Matnr\tMPN\tEAN\tDesc\tStock\tLP\tCBP\tSur\tPF\tPFNum\tLPF'];
  for (const p of products) {
    lines.push(`${p.Matnr}\t${p.MPN}\t${p.EAN}\t${p.Desc}\t${p.Stock}\t${p.LP}\t${p.CBP}\t${p.Sur}\t${p.PF || ''}\t${p.PFNum || ''}\t${p.LPF || ''}`);
  }
  return await uploadToStorage(supabase, 'exports', productsFilePath(runId), lines.join('\n'), 'text/tab-separated-values');
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

// ========== XLSX GENERATION HELPER ==========
function createXLSXBuffer(headers: string[], rows: string[][]): Uint8Array {
  const wsData = [headers, ...rows];
  const ws = XLSX.utils.aoa_to_sheet(wsData);
  
  // Set column widths
  const colWidths = headers.map(() => ({ width: 20 }));
  ws['!cols'] = colWidths;
  
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Export');
  
  // Write to binary buffer
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
      const error = loadError || 'Products file not found';
      await updateStepResult(supabase, runId, 'export_ean', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    const eanRows: string[][] = [];
    let eanSkipped = 0;
    
    const headers = ['EAN', 'MPN', 'Matnr', 'Descrizione', 'Prezzo', 'ListPrice con Fee', 'Stock'];
    
    for (const p of products) {
      const norm = normalizeEAN(p.EAN);
      if (!norm.ok) {
        eanSkipped++;
        continue;
      }
      
      eanRows.push([
        norm.value || '',
        p.MPN || '',
        p.Matnr || '',
        (p.Desc || '').replace(/;/g, ','),
        p.PF || '',
        p.LPF || '',
        String(p.Stock || 0)
      ]);
    }
    
    // Generate XLSX buffer
    const xlsxBuffer = createXLSXBuffer(headers, eanRows);
    
    // Save to exports bucket as XLSX (per-run path)
    const eanPath = exportEanPath(runId);
    const saveResult = await uploadToStorage(supabase, 'exports', eanPath, xlsxBuffer, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    
    if (!saveResult.success) {
      const error = `Failed to save ${eanPath}: ${saveResult.error}`;
      await updateStepResult(supabase, runId, 'export_ean', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    // Save export file path to sync_runs.steps.exports.files
    await updateExportFilePath(supabase, runId, 'ean', `exports/${eanPath}`);
    
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

// ========== STEP: EXPORT_MEDIAWORLD (with IT/EU stock support) ==========
async function stepExportMediaworld(supabase: any, runId: string, feeConfig: any): Promise<{ success: boolean; error?: string }> {
  // FAIL-FAST: No defaults - these must be set by orchestrator after validation
  const includeEu = feeConfig?.mediaworldIncludeEu;
  const itDays = feeConfig?.mediaworldItPrepDays;
  const euDays = feeConfig?.mediaworldEuPrepDays;
  
  // Validate required fields (should already be validated by orchestrator, but double-check)
  if (typeof includeEu !== 'boolean' || typeof itDays !== 'number' || typeof euDays !== 'number') {
    const error = `FAIL-FAST: Mediaworld config mancante o invalido: includeEu=${includeEu}, itDays=${itDays}, euDays=${euDays}`;
    console.error(`[sync:step:export_mediaworld] ${error}`);
    await updateStepResult(supabase, runId, 'export_mediaworld', { status: 'failed', error, metrics: {} });
    return { success: false, error };
  }
  
  console.log(`[sync:step:export_mediaworld] Starting for run ${runId}, IT days=${itDays}, EU days=${euDays}, includeEU=${includeEu}`);
  const startTime = Date.now();
  
  // Initialize warnings
  const warnings = createEmptyWarnings();
  
  try {
    const { products, error: loadError } = await loadProductsTSV(supabase, runId);
    
    if (loadError || !products) {
      const error = loadError || 'Products file not found';
      await updateStepResult(supabase, runId, 'export_mediaworld', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    // Load stock location index from per-run file with robust parsing
    let stockLocationIndex: Record<string, { stockIT: number; stockEU: number }> | null = null;
    const stockLocationPath = `stock-location/runs/${runId}.txt`;
    const { content: stockLocationContent } = await downloadFromStorage(supabase, 'ftp-import', stockLocationPath);
    
    // Track 4255 vs 4254 entries for orphan detection
    const entries4254 = new Set<string>();
    const entries4255 = new Set<string>();
    
    if (stockLocationContent) {
      stockLocationIndex = {};
      const lines = stockLocationContent.replace(/\r\n/g, '\n').split('\n');
      const headers = lines[0]?.split(';').map((h: string) => h.trim().toLowerCase()) || [];
      const matnrIdx = headers.indexOf('matnr');
      const stockIdx = headers.indexOf('stock');
      const locationIdx = headers.indexOf('locationid');
      
      if (matnrIdx >= 0 && stockIdx >= 0 && locationIdx >= 0) {
        for (let i = 1; i < lines.length; i++) {
          const vals = lines[i].split(';');
          const matnr = vals[matnrIdx]?.trim();
          if (!matnr) continue;
          
          const stockRaw = vals[stockIdx]?.trim() || '0';
          let stock = parseInt(stockRaw, 10);
          if (isNaN(stock) || !Number.isFinite(stock)) {
            stock = 0;
            warnings.invalid_stock_value++;
          }
          
          const locationId = parseInt(vals[locationIdx]) || 0;
          
          if (!stockLocationIndex[matnr]) stockLocationIndex[matnr] = { stockIT: 0, stockEU: 0 };
          
          if (locationId === LOCATION_ID_IT) {
            stockLocationIndex[matnr].stockIT += stock;
          } else if (locationId === LOCATION_ID_EU) {
            stockLocationIndex[matnr].stockEU += stock;
            entries4254.add(matnr);
          } else if (locationId === LOCATION_ID_EU_DUPLICATE) {
            // LocationID 4255 is ignored in calculations
            entries4255.add(matnr);
          }
        }
        
        // Check for orphan_4255 warnings
        for (const matnr of entries4255) {
          if (!entries4254.has(matnr)) {
            warnings.orphan_4255++;
          }
        }
        
        console.log(`[sync:step:export_mediaworld] Loaded stock location: ${Object.keys(stockLocationIndex).length} entries`);
      } else {
        warnings.invalid_location_parse++;
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
      
      // IT/EU stock resolution using resolveMarketplaceStock
      let stockIT = p.Stock || 0;
      let stockEU = 0;
      
      if (stockLocationIndex && stockLocationIndex[p.Matnr]) {
        stockIT = stockLocationIndex[p.Matnr].stockIT;
        stockEU = stockLocationIndex[p.Matnr].stockEU;
      } else if (stockLocationIndex) {
        // Matnr not found in location file - fallback StockIT=0 StockEU=0
        warnings.missing_location_data++;
        stockIT = 0;
        stockEU = 0;
      }
      // If stockLocationIndex is null (file missing), use fallback: stockIT = p.Stock, stockEU = 0
      
      const stockResult = resolveMarketplaceStock(stockIT, stockEU, includeEu, itDays, euDays);
      
      if (!stockResult.shouldExport) { mwSkipped++; continue; }
      
      // Mediaworld server-side: leadtime-to-ship = stockResult.leadDays (NO offset)
      // The exported value matches exactly the UI configuration
      const leadTimeToShip = stockResult.leadDays;
      
      mwRows.push([
        p.Matnr || '',
        norm.value || '',
        p.PFNum.toFixed(2).replace('.', ','),
        String(leadTimeToShip),
        String(Math.min(stockResult.exportQty, 99))
      ]);
    }
    
    // Generate XLSX buffer (per-run path)
    const xlsxBuffer = createXLSXBuffer(headers, mwRows);
    const mwPath = exportMediaworldPath(runId);
    const saveResult = await uploadToStorage(supabase, 'exports', mwPath, xlsxBuffer, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    
    if (!saveResult.success) {
      const error = `Failed to save ${mwPath}: ${saveResult.error}`;
      await updateStepResult(supabase, runId, 'export_mediaworld', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    // Update location_warnings in sync_runs
    await updateLocationWarnings(supabase, runId, warnings);
    
    // Save export file path to sync_runs.steps.exports.files
    await updateExportFilePath(supabase, runId, 'mediaworld', `exports/${mwPath}`);
    
    await updateStepResult(supabase, runId, 'export_mediaworld', {
      status: 'success', duration_ms: Date.now() - startTime, rows: mwRows.length, skipped: mwSkipped,
      metrics: { mediaworld_export_rows: mwRows.length, mediaworld_export_skipped: mwSkipped }
    });
    
    console.log(`[sync:step:export_mediaworld] Completed: ${mwRows.length} rows, ${mwSkipped} skipped, warnings:`, warnings);
    return { success: true };
    
  } catch (e: any) {
    console.error(`[sync:step:export_mediaworld] Error:`, e);
    await updateStepResult(supabase, runId, 'export_mediaworld', { status: 'failed', error: e.message, metrics: {} });
    return { success: false, error: e.message };
  }
}

// ========== STEP: EXPORT_EPRICE (with IT/EU stock support) ==========
async function stepExportEprice(supabase: any, runId: string, feeConfig: any): Promise<{ success: boolean; error?: string }> {
  // FAIL-FAST: No defaults - these must be set by orchestrator after validation
  const includeEu = feeConfig?.epriceIncludeEu;
  const itDays = feeConfig?.epriceItPrepDays;
  const euDays = feeConfig?.epriceEuPrepDays;
  
  // Validate required fields (should already be validated by orchestrator, but double-check)
  if (typeof includeEu !== 'boolean' || typeof itDays !== 'number' || typeof euDays !== 'number') {
    const error = `FAIL-FAST: ePrice config mancante o invalido: includeEu=${includeEu}, itDays=${itDays}, euDays=${euDays}`;
    console.error(`[sync:step:export_eprice] ${error}`);
    await updateStepResult(supabase, runId, 'export_eprice', { status: 'failed', error, metrics: {} });
    return { success: false, error };
  }
  
  console.log(`[sync:step:export_eprice] Starting for run ${runId}, IT days=${itDays}, EU days=${euDays}, includeEU=${includeEu}`);
  const startTime = Date.now();
  
  try {
    const { products, error: loadError } = await loadProductsTSV(supabase, runId);
    
    if (loadError || !products) {
      const error = loadError || 'Products file not found';
      await updateStepResult(supabase, runId, 'export_eprice', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    // Load stock location (same as Mediaworld)
    let stockLocationIndex: Record<string, { stockIT: number; stockEU: number }> | null = null;
    const stockLocationPath = `stock-location/runs/${runId}.txt`;
    const { content: stockLocationContent } = await downloadFromStorage(supabase, 'ftp-import', stockLocationPath);
    
    if (stockLocationContent) {
      stockLocationIndex = {};
      const lines = stockLocationContent.replace(/\r\n/g, '\n').split('\n');
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
    
    const epRows: string[][] = [];
    let epSkipped = 0;
    const exportHeaders = ['sku', 'ean', 'price', 'quantity', 'leadtime'];
    
    for (const p of products) {
      const norm = normalizeEAN(p.EAN);
      if (!norm.ok) { epSkipped++; continue; }
      if (!p.PFNum || p.PFNum <= 0) { epSkipped++; continue; }
      
      // IT/EU stock resolution using resolveMarketplaceStock (no +2 for ePrice)
      let stockIT = p.Stock || 0;
      let stockEU = 0;
      if (stockLocationIndex && stockLocationIndex[p.Matnr]) {
        stockIT = stockLocationIndex[p.Matnr].stockIT;
        stockEU = stockLocationIndex[p.Matnr].stockEU;
      }
      
      const stockResult = resolveMarketplaceStock(stockIT, stockEU, includeEu, itDays, euDays);
      
      if (!stockResult.shouldExport) { epSkipped++; continue; }
      
      epRows.push([
        p.Matnr || '',
        norm.value || '',
        p.PFNum.toFixed(2).replace('.', ','),
        String(Math.min(stockResult.exportQty, 99)),
        String(stockResult.leadDays)  // ePrice: no +2
      ]);
    }
    
    // Generate XLSX buffer
    const xlsxBuffer = createXLSXBuffer(exportHeaders, epRows);
    const saveResult = await uploadToStorage(supabase, 'exports', 'Export ePrice.xlsx', xlsxBuffer, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    
    if (!saveResult.success) {
      const error = `Failed to save Export ePrice.xlsx: ${saveResult.error}`;
      await updateStepResult(supabase, runId, 'export_eprice', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    // Save export file path to sync_runs.steps.exports.files
    await updateExportFilePath(supabase, runId, 'eprice', 'exports/Export ePrice.xlsx');
    
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
    
    // Run golden cases validation on first step (parse_merge) for debugging
    if (step === 'parse_merge') {
      const itDays = fee_config?.mediaworldItPrepDays || 3;
      const euDays = fee_config?.mediaworldEuPrepDays || 5;
      validateGoldenCases('[server]', itDays, euDays);
    }
    
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
