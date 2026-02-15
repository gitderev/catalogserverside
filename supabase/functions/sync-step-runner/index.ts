import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";

type SupabaseClient = ReturnType<typeof createClient>;

interface Product {
  Matnr: string;
  MPN: string;
  EAN: string;
  Desc: string;
  Stock: number;
  LP: number;
  CBP: number;
  Sur: number;
  PF: string;
  PFNum: number;
  LPF: string;
}

interface FeeConfig {
  feeDrev?: number;
  feeMkt?: number;
  shippingCost?: number;
  mediaworldIncludeEu?: boolean;
  mediaworldItPrepDays?: number;
  mediaworldEuPrepDays?: number;
  epriceIncludeEu?: boolean;
  epriceItPrepDays?: number;
  epriceEuPrepDays?: number;
  epricePrepDays?: number;
}

interface StepResultData {
  status: string;
  error?: string;
  duration_ms?: number;
  metrics: Record<string, number>;
  [key: string]: unknown;
}

function errMsg(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

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
const CHUNK_SIZE = 1000; // Ridotto da 5000 a 1000 per evitare WORKER_LIMIT

const PRODUCTS_FILE_PATH = '_pipeline/products.tsv';
const PARTIAL_PRODUCTS_FILE_PATH = '_pipeline/partial_products.tsv'; // legacy, kept for cleanup
const CHUNKS_DIR = '_pipeline/parse_merge_chunks'; // each chunk is {run_id}/{chunk_index}.tsv
const MATERIAL_CHUNKS_DIR = '_pipeline/material_chunks'; // chunk_files fallback
const MATERIAL_SOURCE_FILE = '_pipeline/material_source.tsv';

// Budget per invocazione: limita byte letti per evitare WORKER_LIMIT
// Ridurre TIME_BUDGET_MS se continua WORKER_LIMIT (e.g. 6000)
const TIME_BUDGET_MS = 8000; // 8s wall-clock safety
const MAX_FETCH_BYTES = 2 * 1024 * 1024; // 2MB per Range request
const RANGE_FETCH_MARGIN = 64 * 1024; // 64KB tolerance on first chunk
const MAX_PARTIAL_LINE_BYTES = 256 * 1024; // 256KB max for carried partial line
const MAX_TOTAL_CHUNKS = 50;
const MAX_TOTAL_SIZE_BYTES = 40 * 1024 * 1024; // 40MB
const MAX_FINALIZE_PART_SIZE = 10 * 1024 * 1024; // 10MB triggers part-files strategy
const EAN_CATALOG_FILE_PATH = '_pipeline/ean_catalog.tsv';

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

// ========== UPDATE LOCATION WARNINGS + LOG WARN EVENTS VIA RPC ==========
async function updateLocationWarnings(supabase: SupabaseClient, runId: string, warnings: StockLocationWarnings): Promise<void> {
  try {
    await supabase.from('sync_runs').update({ location_warnings: warnings }).eq('id', runId);
    console.log(`[sync-step-runner] Updated location_warnings for run ${runId}:`, warnings);
    
    // Register each non-zero warning as a WARN event via atomic RPC
    const warningEntries = Object.entries(warnings).filter(([_, v]) => v > 0);
    for (const [key, count] of warningEntries) {
      await supabase.rpc('log_sync_event', {
        p_run_id: runId,
        p_level: 'WARN',
        p_message: `Location warning: ${key} (count: ${count})`,
        p_details: { step: 'export', location_warning_type: key, count }
      });
    }
  } catch (e: unknown) {
    console.error(`[sync-step-runner] Failed to update location_warnings:`, e);
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

async function getLatestFile(supabase: SupabaseClient, folder: string): Promise<{ content: string | null; fileName: string | null }> {
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

async function uploadToStorage(supabase: SupabaseClient, bucket: string, path: string, content: string, contentType: string): Promise<{ success: boolean; error?: string }> {
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
  const fileExists = files?.some((f: { name: string }) => f.name === fileName);
  
  if (!fileExists) {
    console.error(`[storage] Upload verification failed: file not found after upload`);
    return { success: false, error: 'File not found after upload' };
  }
  
  console.log(`[storage] Upload successful and verified: ${bucket}/${path}`);
  return { success: true };
}

async function downloadFromStorage(supabase: SupabaseClient, bucket: string, path: string): Promise<{ content: string | null; error?: string }> {
  console.log(`[storage] Downloading from ${bucket}/${path}`);
  
  const folder = path.includes('/') ? path.substring(0, path.lastIndexOf('/')) : '';
  const fileName = path.includes('/') ? path.substring(path.lastIndexOf('/') + 1) : path;
  
  const { data: files, error: listError } = await supabase.storage.from(bucket).list(folder);
  
  if (listError) {
    console.error(`[storage] Error listing ${bucket}/${folder}:`, listError);
    return { content: null, error: `Error listing folder: ${listError.message}` };
  }
  
  console.log(`[storage] Files in ${bucket}/${folder}:`, files?.map((f: { name: string }) => f.name).join(', ') || 'none');
  
  const fileExists = files?.some((f: { name: string }) => f.name === fileName);
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

async function deleteFromStorage(supabase: SupabaseClient, bucket: string, path: string): Promise<void> {
  try {
    await supabase.storage.from(bucket).remove([path]);
    console.log(`[storage] Deleted ${bucket}/${path}`);
  } catch (e: unknown) {
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
  status: 'pending' | 'building_stock_index' | 'building_price_index' | 'preparing_material' | 'in_progress' | 'finalizing' | 'completed' | 'failed';
  offset: number; // legacy line offset, kept for backward compat
  cursor_pos: number; // byte position in material file
  chunk_index: number; // current chunk number (output chunk)
  productCount: number;
  skipped: { noStock: number; noPrice: number; lowStock: number; noValid: number };
  materialBytes: number;
  startTime: number;
  error?: string;
  partial_line: string; // incomplete line carried across invocations (max MAX_PARTIAL_LINE_BYTES)
  material_path: string; // path in exports bucket for Range-based fetch
  finalize_chunk_idx: number; // tracks finalization progress
  mode: 'range' | 'chunk_files'; // fetch strategy
  total_chunks_split: number; // number of material chunk files (chunk_files mode)
  material_chunk_index: number; // current material chunk being processed (chunk_files mode)
}

async function getParseMergeState(supabase: SupabaseClient, runId: string): Promise<ParseMergeState | null> {
  const { data: run } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
  return run?.steps?.parse_merge || null;
}

async function updateParseMergeState(supabase: SupabaseClient, runId: string, state: Partial<ParseMergeState>): Promise<void> {
  const { data: run } = await supabase.from('sync_runs').select('steps, metrics').eq('id', runId).single();
  const currentSteps = run?.steps || {};
  const currentMetrics = run?.metrics || {};
  
  const updatedParseMerge = { ...currentSteps.parse_merge, ...state };
  const updatedSteps = { ...currentSteps, parse_merge: updatedParseMerge, current_step: 'parse_merge' };
  
  // Update metrics if completed
  const updatedMetrics = state.status === 'completed' ? {
    ...currentMetrics,
    products_total: (state.productCount || 0) + Object.values(state.skipped || {}).reduce((a: number, b: unknown) => a + (Number(b) || 0), 0),
    products_processed: state.productCount || 0
  } : currentMetrics;
  
  await supabase.from('sync_runs').update({ steps: updatedSteps, metrics: updatedMetrics }).eq('id', runId);
  console.log(`[parse_merge] State updated: status=${state.status}, offset=${state.offset ?? 'N/A'}, products=${state.productCount ?? 'N/A'}`);
}

// ========== INDICES STORAGE (split into separate files for memory efficiency) ==========

const STOCK_INDEX_FILE = '_pipeline/stock_index.json';
const PRICE_INDEX_FILE = '_pipeline/price_index.json';
const MATERIAL_META_FILE = '_pipeline/material_meta.json';

interface MaterialMeta {
  delimiter: string;
  matnrIdx: number;
  mpnIdx: number;
  eanIdx: number;
  descIdx: number;
  headerEndPos: number;
  totalBytes: number;
  source_bucket: string; // 'ftp-import'
  source_path: string;   // e.g. 'material/filename.tsv'
}

async function saveStockIndex(supabase: SupabaseClient, stockIndex: Record<string, number>): Promise<{ success: boolean; error?: string }> {
  const json = JSON.stringify(stockIndex);
  console.log(`[parse_merge:indices] Saving stock index: ${Object.keys(stockIndex).length} entries, ${json.length} bytes`);
  return await uploadToStorage(supabase, 'exports', STOCK_INDEX_FILE, json, 'application/json');
}

async function loadStockIndex(supabase: SupabaseClient): Promise<{ index: Record<string, number> | null; error?: string }> {
  const { content, error } = await downloadFromStorage(supabase, 'exports', STOCK_INDEX_FILE);
  if (error || !content) return { index: null, error: error || 'Empty content' };
  try {
    const index = JSON.parse(content);
    console.log(`[parse_merge:indices] Loaded stock index: ${Object.keys(index).length} entries`);
    return { index };
  } catch (e: unknown) {
    return { index: null, error: `JSON parse error: ${errMsg(e)}` };
  }
}

async function savePriceIndex(supabase: SupabaseClient, priceIndex: Record<string, [number, number, number]>): Promise<{ success: boolean; error?: string }> {
  const json = JSON.stringify(priceIndex);
  console.log(`[parse_merge:indices] Saving price index: ${Object.keys(priceIndex).length} entries, ${json.length} bytes`);
  return await uploadToStorage(supabase, 'exports', PRICE_INDEX_FILE, json, 'application/json');
}

async function loadPriceIndex(supabase: SupabaseClient): Promise<{ index: Record<string, [number, number, number]> | null; error?: string }> {
  const { content, error } = await downloadFromStorage(supabase, 'exports', PRICE_INDEX_FILE);
  if (error || !content) return { index: null, error: error || 'Empty content' };
  try {
    const index = JSON.parse(content);
    console.log(`[parse_merge:indices] Loaded price index: ${Object.keys(index).length} entries`);
    return { index };
  } catch (e: unknown) {
    return { index: null, error: `JSON parse error: ${errMsg(e)}` };
  }
}

async function saveMaterialMeta(supabase: SupabaseClient, meta: MaterialMeta): Promise<{ success: boolean; error?: string }> {
  const json = JSON.stringify(meta);
  console.log(`[parse_merge:indices] Saving material meta: headerEndPos=${meta.headerEndPos}, totalBytes=${meta.totalBytes}`);
  return await uploadToStorage(supabase, 'exports', MATERIAL_META_FILE, json, 'application/json');
}

async function loadMaterialMeta(supabase: SupabaseClient): Promise<{ meta: MaterialMeta | null; error?: string }> {
  const { content, error } = await downloadFromStorage(supabase, 'exports', MATERIAL_META_FILE);
  if (error || !content) return { meta: null, error: error || 'Empty content' };
  try {
    return { meta: JSON.parse(content) };
  } catch (e: unknown) {
    return { meta: null, error: `JSON parse error: ${errMsg(e)}` };
  }
}

async function cleanupIndexFiles(supabase: SupabaseClient, runId?: string): Promise<void> {
  await deleteFromStorage(supabase, 'exports', STOCK_INDEX_FILE);
  await deleteFromStorage(supabase, 'exports', PRICE_INDEX_FILE);
  await deleteFromStorage(supabase, 'exports', MATERIAL_META_FILE);
  await deleteFromStorage(supabase, 'exports', MATERIAL_SOURCE_FILE);
  await deleteFromStorage(supabase, 'exports', PARTIAL_PRODUCTS_FILE_PATH);
  
  // Clean up chunk files and material chunks if runId provided
  if (runId) {
    for (const dir of [`${CHUNKS_DIR}/${runId}`, `${MATERIAL_CHUNKS_DIR}/${runId}`]) {
      try {
        const { data: files } = await supabase.storage.from('exports').list(dir);
        if (files && files.length > 0) {
          const paths = files.map((f: { name: string }) => `${dir}/${f.name}`);
          await supabase.storage.from('exports').remove(paths);
          console.log(`[parse_merge] Cleaned up ${paths.length} files from ${dir}`);
        }
      } catch (e: unknown) {
        console.log(`[parse_merge] Cleanup warning for ${dir}:`, e);
      }
    }
  }
}

// ========== STEP: PARSE_MERGE (MULTI-PHASE CHUNKED VERSION) ==========
// Split into multiple invocations to stay within memory limits:
// Phase 1a: building_stock_index - load and parse stock file, save index
// Phase 1b: building_price_index - load and parse price file, save index  
// Phase 1c: preparing_material - read material file metadata, save it
// Phase 2: in_progress - chunked material processing

async function stepParseMerge(supabase: SupabaseClient, runId: string): Promise<{ success: boolean; error?: string; status?: string }> {
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
        cursor_pos: 0,
        chunk_index: 0,
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
      
      // Save stock index
      const saveResult = await saveStockIndex(supabase, stockIndex);
      if (!saveResult.success) {
        const error = `Failed to save stock index: ${saveResult.error}`;
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
      
      const parseNum = (v: unknown) => parseFloat(String(v || '0').replace(',', '.')) || 0;
      
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
      
      // Save price index
      const saveResult = await savePriceIndex(supabase, priceIndex);
      if (!saveResult.success) {
        const error = `Failed to save price index: ${saveResult.error}`;
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      // Update state to next phase
      await updateParseMergeState(supabase, runId, { status: 'preparing_material' });
      
      console.log(`[parse_merge] Phase 1b complete in ${Date.now() - invocationStart}ms, price index saved`);
      return { success: true, status: 'preparing_material' };
    }
    
    // ========== PHASE 1c: PREPARE MATERIAL METADATA + optional chunk_files split ==========
    if (state.status === 'preparing_material') {
      console.log(`[parse_merge] Phase 1c: Preparing material metadata (Range-only, no full download)...`);
      
      // 1. List material files to get filename (never download full file)
      const { data: matFiles, error: matListError } = await supabase.storage
        .from('ftp-import').list('material', { sortBy: { column: 'created_at', order: 'desc' }, limit: 1 });
      
      if (matListError || !matFiles?.length) {
        const error = `Material file mancante in ftp-import/material: ${matListError?.message || 'nessun file'}`;
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      const matFileName = matFiles[0].name;
      const matFilePath = `material/${matFileName}`;
      console.log(`[parse_merge] Material file found: ${matFileName}`);
      
      // 2. Create signed URL from ftp-import
      const { data: signedData, error: signedError } = await supabase.storage
        .from('ftp-import').createSignedUrl(matFilePath, 600);
      
      if (signedError || !signedData?.signedUrl) {
        const error = `Signed URL failed for material: ${signedError?.message || 'no URL'}`;
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      // 3. HEAD request to get total file size without downloading
      let totalBytes = 0;
      try {
        const headResp = await fetch(signedData.signedUrl, { method: 'HEAD' });
        totalBytes = parseInt(headResp.headers.get('content-length') || '0');
        console.log(`[parse_merge] Material file size from HEAD: ${totalBytes} bytes`);
      } catch (e) {
        console.warn(`[parse_merge] HEAD request failed, will get size from Range response:`, e);
      }
      
      // 4. Range fetch ONLY the header (first 8KB) — never loads full file
      const HEADER_FETCH_BYTES = 8192;
      const headerResp = await fetch(signedData.signedUrl, {
        headers: { 'Range': `bytes=0-${HEADER_FETCH_BYTES - 1}` }
      });
      const headerStatus = headerResp.status;
      const headerRawBytes = new Uint8Array(await headerResp.arrayBuffer());
      const headerText = new TextDecoder().decode(headerRawBytes);
      
      // If we got the full file via 200 (Range ignored) and it's small, that's OK for header detection
      // But we need to know if Range works for subsequent fetches
      if (totalBytes === 0 && headerStatus === 200) {
        totalBytes = headerRawBytes.byteLength; // This IS the full file if Range was ignored
      }
      if (totalBytes === 0) {
        // Try parsing Content-Range header for total
        const contentRange = headerResp.headers.get('content-range') || '';
        const match = contentRange.match(/\/(\d+)/);
        if (match) totalBytes = parseInt(match[1]);
      }
      
      if (!headerText || headerText.length === 0) {
        const error = 'Material file vuoto';
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      const matFirstNewline = headerText.indexOf('\n');
      if (matFirstNewline === -1) {
        const error = 'Material file: header line non trovata (nessun newline nei primi 8KB)';
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      const matHeaderLine = headerText.substring(0, matFirstNewline).trim();
      const matDelimiter = detectDelimiter(matHeaderLine);
      const matHeaders = matHeaderLine.split(matDelimiter).map(h => h.trim());
      
      const matMatnr = findColumnIndex(matHeaders, 'Matnr');
      const matMpn = findColumnIndex(matHeaders, 'ManufPartNr');
      const matEan = findColumnIndex(matHeaders, 'EAN');
      const matDesc = findColumnIndex(matHeaders, 'ShortDescription');
      
      if (matMatnr.index === -1) {
        const error = `Material headers non validi. Trovati: [${matHeaders.join(', ')}]. Matnr non trovato.`;
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      // headerEndPos in bytes (UTF-8)
      const headerEndPos = new TextEncoder().encode(headerText.substring(0, matFirstNewline + 1)).length;
      
      const meta: MaterialMeta = {
        delimiter: matDelimiter,
        matnrIdx: matMatnr.index,
        mpnIdx: matMpn.index,
        eanIdx: matEan.index,
        descIdx: matDesc.index,
        headerEndPos,
        totalBytes,
        source_bucket: 'ftp-import',
        source_path: matFilePath
      };
      
      await saveMaterialMeta(supabase, meta);
      
      // 5. Determine fetch mode: test Range support
      let mode: 'range' | 'chunk_files' = 'range';
      let totalChunksSplit = 0;
      
      if (headerStatus === 206) {
        // Range worked for header fetch, trust it
        mode = 'range';
      } else if (headerStatus === 200 && totalBytes <= MAX_FETCH_BYTES + RANGE_FETCH_MARGIN) {
        // File is small enough that 200 is OK (whole file fits in one fetch)
        mode = 'range'; // Will complete in one invocation anyway
      } else if (headerStatus === 200 && totalBytes > MAX_FETCH_BYTES + RANGE_FETCH_MARGIN) {
        // Range was ignored on a large file — need chunk_files fallback
        mode = 'chunk_files';
        console.log(`[parse_merge] Range ignored (HTTP 200, file ${totalBytes}b > limit). Using chunk_files.`);
        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'INFO',
            p_message: 'parse_merge_fallback_activated',
            p_details: { reason: 'HTTP 200 on Range request for large file', http_status: headerStatus, total_bytes: totalBytes, mode_switched_to: 'chunk_files' }
          });
        } catch (_e) { /* non-blocking */ }
      }
      
      // 6. For chunk_files mode: we need a splitting phase
      // Since we can't download the full file (it would OOM), we enter a multi-invocation
      // splitting phase that uses Range requests to split the file.
      // If Range doesn't even work for small ranges, we're stuck and must fail.
      if (mode === 'chunk_files') {
        // Enter splitting phase — will be handled in next invocations
        await updateParseMergeState(supabase, runId, {
          status: 'in_progress', // We use in_progress with mode=chunk_files
          offset: 0,
          cursor_pos: headerEndPos,
          chunk_index: 0,
          productCount: 0,
          skipped: { noStock: 0, noPrice: 0, lowStock: 0, noValid: 0 },
          materialBytes: totalBytes,
          material_path: matFilePath,
          partial_line: '',
          finalize_chunk_idx: 0,
          mode: 'chunk_files',
          total_chunks_split: 0, // Not pre-split; we'll use Range to read chunks
          material_chunk_index: 0
        });
        // NOTE: in chunk_files mode without pre-split, in_progress will attempt Range
        // from ftp-import. If Range works there, great. If not, it will fail with diagnostic.
      } else {
        await updateParseMergeState(supabase, runId, {
          status: 'in_progress',
          offset: 0,
          cursor_pos: headerEndPos,
          chunk_index: 0,
          productCount: 0,
          skipped: { noStock: 0, noPrice: 0, lowStock: 0, noValid: 0 },
          materialBytes: totalBytes,
          material_path: matFilePath,
          partial_line: '',
          finalize_chunk_idx: 0,
          mode: 'range',
          total_chunks_split: 0,
          material_chunk_index: 0
        });
      }
      
      console.log(`[parse_merge] Phase 1c complete in ${Date.now() - invocationStart}ms, mode=${mode}, totalBytes=${totalBytes}`);
      return { success: true, status: 'in_progress' };
    }
    
    // ========== PHASE 2: CHUNKED MATERIAL PROCESSING ==========
    // Supports two modes:
    //   range: fetch MAX_FETCH_BYTES via Range header from material_source.tsv
    //   chunk_files: download one pre-split chunk file per invocation
    if (state.status === 'in_progress') {
      const mode = state.mode || 'range';
      const cursorPos = state.cursor_pos ?? 0;
      const chunkIndex = state.chunk_index ?? 0; // output chunk index
      const partialLine = state.partial_line || '';
      const materialPath = state.material_path || MATERIAL_SOURCE_FILE;
      const totalBytes = state.materialBytes || 0;
      const materialChunkIndex = state.material_chunk_index ?? 0;
      const totalChunksSplit = state.total_chunks_split ?? 0;
      
      console.log(`[parse_merge] Phase 2: mode=${mode}, output_chunk=#${chunkIndex}, cursor=${cursorPos}/${totalBytes}, partial=${partialLine.length}b, mat_chunk=${materialChunkIndex}/${totalChunksSplit}`);
      
      // Guardrail: too many output chunks
      if (chunkIndex > MAX_TOTAL_CHUNKS) {
        const error = `Troppi output chunk (${chunkIndex}>${MAX_TOTAL_CHUNKS}). File troppo grande per il chunking attuale.`;
        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'ERROR', p_message: error,
            p_details: { step: 'parse_merge', chunk_index: chunkIndex, cursor_pos: cursorPos, total_bytes: totalBytes, mode, suggestion: 'Aumentare MAX_FETCH_BYTES o ridurre dimensione file input' }
          });
        } catch (_e) { /* non-blocking */ }
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      // Load indices
      const { index: stockIndex, error: stockError } = await loadStockIndex(supabase);
      if (!stockIndex) {
        const error = `Failed to load stock index: ${stockError}`;
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      const { index: priceIndex, error: priceError } = await loadPriceIndex(supabase);
      if (!priceIndex) {
        const error = `Failed to load price index: ${priceError}`;
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      const { meta: materialMeta, error: metaError } = await loadMaterialMeta(supabase);
      if (!materialMeta) {
        const error = `Failed to load material metadata: ${metaError}`;
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      // ----- Fetch raw text for this invocation -----
      // Both modes now use Range from source_bucket/source_path (ftp-import).
      // chunk_files mode is a fallback label only — it still uses Range but
      // with stricter diagnostics. If Range fails, we fail with clear error.
      let rawText = '';
      let httpStatus = 0;
      let bytesFetched = 0;
      let contentLengthHeader = '';
      let contentRangeHeader = '';
      let isEOF = false;
      
      const sourceBucket = materialMeta.source_bucket || 'ftp-import';
      const sourcePath = materialMeta.source_path || materialPath;
      
      if (cursorPos >= totalBytes && totalBytes > 0) {
        isEOF = true;
      } else {
        // Fetch via Range header from source bucket (ftp-import)
          const { data: signedData, error: signedError } = await supabase.storage
            .from(sourceBucket).createSignedUrl(sourcePath, 600);
          
          if (signedError || !signedData?.signedUrl) {
            const error = `Signed URL creation failed: ${signedError?.message || 'no URL returned'}`;
            await updateParseMergeState(supabase, runId, { status: 'failed', error });
            return { success: false, error, status: 'failed' };
          }
          
          const rangeEnd = Math.min(cursorPos + MAX_FETCH_BYTES - 1, totalBytes - 1);
          const resp = await fetch(signedData.signedUrl, {
            headers: { 'Range': `bytes=${cursorPos}-${rangeEnd}` }
          });
          httpStatus = resp.status;
          contentLengthHeader = resp.headers.get('content-length') || '';
          contentRangeHeader = resp.headers.get('content-range') || '';
          
          // TASK B: Range safety checks
          if (httpStatus === 416) {
            // 416 Range Not Satisfiable = EOF
            isEOF = true;
            try {
              await supabase.rpc('log_sync_event', {
                p_run_id: runId, p_level: 'INFO',
                p_message: 'parse_merge_eof_reached',
                p_details: { reason: 'HTTP 416 Range Not Satisfiable', cursor_pos: cursorPos, total_bytes: totalBytes }
              });
            } catch (_e) { /* non-blocking */ }
          } else if (cursorPos > 0 && httpStatus !== 206) {
            // cursor > 0 but didn't get 206: Range ignored
            const error = `Range non supportato o ignorato: HTTP ${httpStatus} a cursor_pos=${cursorPos}. Atteso 206.`;
            try {
              await supabase.rpc('log_sync_event', {
                p_run_id: runId, p_level: 'ERROR', p_message: error,
                p_details: { step: 'parse_merge', cursor_pos: cursorPos, http_status: httpStatus, content_range: contentRangeHeader, content_length: contentLengthHeader, suggestion: 'Range non supportato dallo storage. Ri-eseguire: il sistema userà chunk_files automaticamente al prossimo preparing_material.' }
              });
            } catch (_e) { /* non-blocking */ }
            await updateParseMergeState(supabase, runId, { status: 'failed', error });
            return { success: false, error, status: 'failed' };
          } else if (httpStatus !== 200 && httpStatus !== 206) {
            const error = `Fetch material failed: HTTP ${httpStatus}`;
            await updateParseMergeState(supabase, runId, { status: 'failed', error });
            return { success: false, error, status: 'failed' };
          } else {
            const rawBytes = new Uint8Array(await resp.arrayBuffer());
            bytesFetched = rawBytes.byteLength;
            
            // TASK B: first chunk (cursor=0) with HTTP 200 — check if full file was returned
            if (cursorPos === 0 && httpStatus === 200 && bytesFetched > MAX_FETCH_BYTES + RANGE_FETCH_MARGIN) {
              const error = `Server ha ignorato Range e ha restituito full content al primo chunk (${bytesFetched} bytes > ${MAX_FETCH_BYTES + RANGE_FETCH_MARGIN} limite).`;
              try {
                await supabase.rpc('log_sync_event', {
                  p_run_id: runId, p_level: 'ERROR', p_message: error,
                  p_details: { step: 'parse_merge', cursor_pos: 0, http_status: httpStatus, bytes_fetched: bytesFetched, content_length: contentLengthHeader, suggestion: 'Range non affidabile. Ri-eseguire: il sistema userà chunk_files al prossimo preparing_material.' }
                });
              } catch (_e) { /* non-blocking */ }
              await updateParseMergeState(supabase, runId, { status: 'failed', error });
              return { success: false, error, status: 'failed' };
            }
            
            rawText = new TextDecoder().decode(rawBytes);
          }
      }
      
      // ----- Handle EOF: process remaining partial line and go to finalizing -----
      if (isEOF) {
        const skipped = { ...state.skipped };
        let productCount = state.productCount;
        let extraChunkTSV = '';
        
        if (partialLine.trim()) {
          const vals = partialLine.split(materialMeta.delimiter);
          const m = vals[materialMeta.matnrIdx]?.trim();
          if (m) {
            const stock = stockIndex[m];
            const price = priceIndex[m];
            if (stock !== undefined && price && stock >= 2 && (price[0] > 0 || price[1] > 0)) {
              const mpn = materialMeta.mpnIdx >= 0 ? (vals[materialMeta.mpnIdx]?.trim() || '') : '';
              const ean = materialMeta.eanIdx >= 0 ? (vals[materialMeta.eanIdx]?.trim() || '') : '';
              const desc = materialMeta.descIdx >= 0 ? (vals[materialMeta.descIdx]?.trim() || '') : '';
              extraChunkTSV = `${m}\t${mpn}\t${ean}\t${desc}\t${stock}\t${price[0]}\t${price[1]}\t${price[2]}\n`;
              productCount++;
            } else {
              if (stock === undefined) skipped.noStock++;
              else if (!price) skipped.noPrice++;
              else if (stock < 2) skipped.lowStock++;
              else skipped.noValid++;
            }
          }
        }
        
        if (extraChunkTSV.length > 0) {
          const chunkPath = `${CHUNKS_DIR}/${runId}/${chunkIndex}.tsv`;
          await uploadToStorage(supabase, 'exports', chunkPath, extraChunkTSV, 'text/tab-separated-values');
        }
        
        await updateParseMergeState(supabase, runId, {
          status: 'finalizing', cursor_pos: cursorPos,
          chunk_index: chunkIndex + (extraChunkTSV.length > 0 ? 1 : 0),
          productCount, skipped, partial_line: '', finalize_chunk_idx: 0
        });
        console.log(`[parse_merge] EOF reached, moving to finalization. products=${productCount}`);
        return { success: true, status: 'finalizing' };
      }
      
      // ----- Parse lines from (partialLine + rawText) -----
      const partialLineBytesBefore = new TextEncoder().encode(partialLine).length;
      const fullText = partialLine + rawText;
      const skipped = { ...state.skipped };
      let productCount = state.productCount;
      let linesEmitted = 0;
      let chunkTSV = '';
      let pos = 0;
      
      while (pos < fullText.length) {
        const lineEnd = fullText.indexOf('\n', pos);
        if (lineEnd === -1) break; // rest is partial line for next invocation
        
        const line = fullText.substring(pos, lineEnd);
        pos = lineEnd + 1;
        
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
        linesEmitted++;
      }
      
      // TASK C: partial_line is strictly the text after last newline
      const newPartialLine = fullText.substring(pos);
      const newPartialLineBytes = new TextEncoder().encode(newPartialLine).length;
      
      // TASK C: partial line size guard
      if (newPartialLineBytes > MAX_PARTIAL_LINE_BYTES) {
        const error = `partial_line troppo grande: ${newPartialLineBytes} bytes > ${MAX_PARTIAL_LINE_BYTES}. Possibile riga lunghissima o errore di parsing.`;
        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'ERROR', p_message: error,
            p_details: { step: 'parse_merge', partial_line_bytes: newPartialLineBytes, max_partial_line_bytes: MAX_PARTIAL_LINE_BYTES, cursor_pos: cursorPos, chunk_index: chunkIndex, suggestion: 'Ridurre MAX_FETCH_BYTES o verificare che il file non contenga righe > 256KB' }
          });
        } catch (_e) { /* non-blocking */ }
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
      
      // Advance cursor_pos by bytes actually fetched
      const newCursorPos = cursorPos + bytesFetched;
      const newMaterialChunkIndex = materialChunkIndex + 1; // legacy, kept for logging
      const elapsedMs = Date.now() - invocationStart;
      
      // Save output chunk if non-empty
      if (chunkTSV.length > 0) {
        const chunkPath = `${CHUNKS_DIR}/${runId}/${chunkIndex}.tsv`;
        await uploadToStorage(supabase, 'exports', chunkPath, chunkTSV, 'text/tab-separated-values');
      }
      
      // TASK A: Comprehensive diagnostic logging
      try {
        await supabase.rpc('log_sync_event', {
          p_run_id: runId, p_level: 'INFO',
          p_message: 'parse_merge_chunk_progress',
          p_details: {
            step: 'parse_merge',
            mode,
            chunk_index: chunkIndex,
            material_chunk_index: mode === 'chunk_files' ? materialChunkIndex : null,
            cursor_pos_start: cursorPos,
            cursor_pos_end: newCursorPos,
            bytes_fetched: bytesFetched,
            content_length_header: contentLengthHeader || null,
            content_range_header: contentRangeHeader || null,
            http_status: httpStatus,
            partial_line_bytes_before: partialLineBytesBefore,
            partial_line_bytes_after: newPartialLineBytes,
            lines_emitted: linesEmitted,
            total_products: productCount,
            elapsed_ms: elapsedMs
          }
        });
      } catch (_e) { /* non-blocking */ }
      
      // Determine if fully read (both modes use cursor_pos now)
      const fileFullyRead = newCursorPos >= totalBytes;
      
      if (fileFullyRead && newPartialLine.trim() === '') {
        console.log(`[parse_merge] All material processed, moving to finalization`);
        await updateParseMergeState(supabase, runId, {
          status: 'finalizing', cursor_pos: newCursorPos,
          chunk_index: chunkIndex + (chunkTSV.length > 0 ? 1 : 0),
          productCount, skipped, partial_line: '', finalize_chunk_idx: 0,
          material_chunk_index: newMaterialChunkIndex
        });
        return { success: true, status: 'finalizing' };
      } else {
        await updateParseMergeState(supabase, runId, {
          status: fileFullyRead ? 'in_progress' : 'in_progress',
          cursor_pos: newCursorPos,
          chunk_index: chunkIndex + (chunkTSV.length > 0 ? 1 : 0),
          productCount, skipped,
          partial_line: newPartialLine,
          material_chunk_index: newMaterialChunkIndex
        });
        console.log(`[parse_merge] Chunk #${chunkIndex} done, mode=${mode}, cursor=${newCursorPos}, partial=${newPartialLineBytes}b, elapsed=${elapsedMs}ms`);
        return { success: true, status: 'in_progress' };
      }
    }
    
    // ========== PHASE 3: FINALIZATION (part-files strategy, time-budgeted) ==========
    // Concatenates output chunks into the final products.tsv.
    // Uses "part files" if total estimated size > MAX_FINALIZE_PART_SIZE to avoid OOM.
    if (state.status === 'finalizing') {
      const totalChunks = state.chunk_index ?? 0;
      const startIdx = state.finalize_chunk_idx ?? 0;
      console.log(`[parse_merge] Phase 3: Finalizing, chunks ${startIdx}..${totalChunks - 1} (total ${totalChunks})`);
      
      try {
        // Guardrail: chunk count
        if (totalChunks > MAX_TOTAL_CHUNKS) {
          const error = `Troppe parti da finalizzare: ${totalChunks} chunk (max ${MAX_TOTAL_CHUNKS})`;
          try {
            await supabase.rpc('log_sync_event', {
              p_run_id: runId, p_level: 'ERROR', p_message: error,
              p_details: { step: 'parse_merge', phase: 'finalization', chunk_count: totalChunks, max_chunks: MAX_TOTAL_CHUNKS }
            });
          } catch (_logErr) { /* non-blocking */ }
          await updateParseMergeState(supabase, runId, { status: 'failed', error });
          return { success: false, error, status: 'failed' };
        }
        
        // Build final content incrementally with time budget
        let finalContent = '';
        if (startIdx > 0) {
          // Resume: load partial result from previous finalization invocation
          const partialPath = `${CHUNKS_DIR}/${runId}/finalize_partial.tsv`;
          const { content: prev } = await downloadFromStorage(supabase, 'exports', partialPath);
          finalContent = prev || '';
        } else {
          finalContent = 'Matnr\tMPN\tEAN\tDesc\tStock\tLP\tCBP\tSur\n';
        }
        
        let i = startIdx;
        let bytesAppendedThisInvocation = 0;
        
        while (i < totalChunks) {
          // Time budget check
          const elapsed = Date.now() - invocationStart;
          if (elapsed > TIME_BUDGET_MS) {
            // Save progress and yield
            const partialPath = `${CHUNKS_DIR}/${runId}/finalize_partial.tsv`;
            await uploadToStorage(supabase, 'exports', partialPath, finalContent, 'text/tab-separated-values');
            await updateParseMergeState(supabase, runId, { finalize_chunk_idx: i });
            
            try {
              await supabase.rpc('log_sync_event', {
                p_run_id: runId, p_level: 'INFO',
                p_message: 'parse_merge_finalizing_progress',
                p_details: {
                  step: 'parse_merge', phase: 'finalization',
                  finalize_chunk_idx_start: startIdx, finalize_chunk_idx_end: i,
                  bytes_appended_this_invocation: bytesAppendedThisInvocation,
                  bytes_total_estimated: finalContent.length,
                  elapsed_ms: elapsed, total_chunks: totalChunks
                }
              });
            } catch (_e) { /* non-blocking */ }
            
            console.log(`[parse_merge] Finalization paused at chunk ${i}/${totalChunks}, bytes=${finalContent.length}, will resume`);
            return { success: true, status: 'finalizing' };
          }
          
          const chunkPath = `${CHUNKS_DIR}/${runId}/${i}.tsv`;
          const { content: chunkContent, error: chunkError } = await downloadFromStorage(supabase, 'exports', chunkPath);
          if (chunkError) {
            console.log(`[parse_merge] Chunk ${i} not found or empty, skipping: ${chunkError}`);
          }
          if (chunkContent) {
            finalContent += chunkContent;
            bytesAppendedThisInvocation += chunkContent.length;
          }
          i++;
          
          // Size guardrail
          if (finalContent.length > MAX_TOTAL_SIZE_BYTES) {
            const error = `Dimensione finale ${(finalContent.length / 1024 / 1024).toFixed(1)}MB supera il limite di ${MAX_TOTAL_SIZE_BYTES / 1024 / 1024}MB`;
            try {
              await supabase.rpc('log_sync_event', {
                p_run_id: runId, p_level: 'ERROR', p_message: error,
                p_details: { step: 'parse_merge', phase: 'finalization', current_size_bytes: finalContent.length, max_size_bytes: MAX_TOTAL_SIZE_BYTES, chunks_loaded: i, total_chunks: totalChunks }
              });
            } catch (_logErr) { /* non-blocking */ }
            await updateParseMergeState(supabase, runId, { status: 'failed', error });
            return { success: false, error, status: 'failed' };
          }
        }
        
        // All chunks loaded - upload final file
        await uploadToStorage(supabase, 'exports', PRODUCTS_FILE_PATH, finalContent, 'text/tab-separated-values');
        
        // Log finalization complete
        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'INFO',
            p_message: 'parse_merge_finalizing_progress',
            p_details: {
              step: 'parse_merge', phase: 'finalization_complete',
              finalize_chunk_idx_start: startIdx, finalize_chunk_idx_end: i,
              bytes_appended_this_invocation: bytesAppendedThisInvocation,
              bytes_total_estimated: finalContent.length,
              elapsed_ms: Date.now() - invocationStart, total_chunks: totalChunks
            }
          });
        } catch (_e) { /* non-blocking */ }
        
        // Cleanup all intermediate files
        await cleanupIndexFiles(supabase, runId);
        await deleteFromStorage(supabase, 'exports', `${CHUNKS_DIR}/${runId}/finalize_partial.tsv`);
        
        const durationMs = Date.now() - state.startTime;
        await updateParseMergeState(supabase, runId, {
          status: 'completed',
          productCount: state.productCount,
          skipped: state.skipped
        });
        
        console.log(`[parse_merge] COMPLETED: ${state.productCount} products in ${durationMs}ms`);
        return { success: true, status: 'completed' };
      } catch (e: unknown) {
        const errorMsg = errMsg(e);
        const error = `Finalization error: ${errorMsg}`;
        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'ERROR', p_message: error,
            p_details: { step: 'parse_merge', phase: 'finalization', finalize_chunk_idx: state.finalize_chunk_idx, total_chunks: state.chunk_index, suggestion: 'Verificare dimensione totale chunk e riprovare' }
          });
        } catch (_logErr) { /* non-blocking */ }
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
    }
    
    // Unknown state
    const error = `Unknown parse_merge state: ${state?.status}`;
    await updateParseMergeState(supabase, runId, { status: 'failed', error });
    return { success: false, error, status: 'failed' };
    
  } catch (e: unknown) {
    console.error(`[parse_merge] Error:`, e);
    await updateParseMergeState(supabase, runId, { status: 'failed', error: errMsg(e) });
    return { success: false, error: errMsg(e), status: 'failed' };
  }
}

// ========== HELPER: Load/Save Products ==========
async function loadProductsTSV(supabase: SupabaseClient, runId: string): Promise<{ products: Product[] | null; error?: string }> {
  console.log(`[sync:products] Loading products for run ${runId} from exports/${PRODUCTS_FILE_PATH}`);
  
  const { content, error } = await downloadFromStorage(supabase, 'exports', PRODUCTS_FILE_PATH);
  
  if (error || !content) {
    console.error(`[sync:products] Failed to load products: ${error || 'empty content'}`);
    return { products: null, error: error || 'Products file not found or empty' };
  }
  
  const lines = content.split('\n');
  const products: Product[] = [];
  
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

async function saveProductsTSV(supabase: SupabaseClient, products: Product[]): Promise<{ success: boolean; error?: string }> {
  const lines = ['Matnr\tMPN\tEAN\tDesc\tStock\tLP\tCBP\tSur\tPF\tPFNum\tLPF'];
  for (const p of products) {
    lines.push(`${p.Matnr}\t${p.MPN}\t${p.EAN}\t${p.Desc}\t${p.Stock}\t${p.LP}\t${p.CBP}\t${p.Sur}\t${p.PF || ''}\t${p.PFNum || ''}\t${p.LPF || ''}`);
  }
  return await uploadToStorage(supabase, 'exports', PRODUCTS_FILE_PATH, lines.join('\n'), 'text/tab-separated-values');
}

// ========== STEP: EAN_MAPPING ==========
async function stepEanMapping(supabase: SupabaseClient, runId: string): Promise<{ success: boolean; error?: string }> {
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
    
    console.log(`[sync:step:ean_mapping] Mapping files found: ${files?.map((f: { name: string }) => f.name).join(', ') || 'none'}`);
    
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
    
  } catch (e: unknown) {
    console.error(`[sync:step:ean_mapping] Error:`, e);
    await updateStepResult(supabase, runId, 'ean_mapping', { status: 'failed', error: errMsg(e), metrics: {} });
    return { success: false, error: errMsg(e) };
  }
}

async function updateStepResult(supabase: SupabaseClient, runId: string, stepName: string, result: StepResultData): Promise<void> {
  const { data: run } = await supabase.from('sync_runs').select('steps, metrics').eq('id', runId).single();
  const steps = { ...(run?.steps || {}), [stepName]: result, current_step: stepName };
  const metrics = { ...(run?.metrics || {}), ...result.metrics };
  await supabase.from('sync_runs').update({ steps, metrics }).eq('id', runId);
}

// ========== STEP: PRICING ==========
async function stepPricing(supabase: SupabaseClient, runId: string, feeConfig: FeeConfig): Promise<{ success: boolean; error?: string }> {
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
    
  } catch (e: unknown) {
    console.error(`[sync:step:pricing] Error:`, e);
    await updateStepResult(supabase, runId, 'pricing', { status: 'failed', error: errMsg(e), metrics: {} });
    return { success: false, error: errMsg(e) };
  }
}

// ========== STEP: EXPORT_EAN ==========
async function stepExportEan(supabase: SupabaseClient, runId: string): Promise<{ success: boolean; error?: string }> {
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
    
  } catch (e: unknown) {
    console.error(`[sync:step:export_ean] Error:`, e);
    await updateStepResult(supabase, runId, 'export_ean', { status: 'failed', error: errMsg(e), metrics: {} });
    return { success: false, error: errMsg(e) };
  }
}

// ========== STEP: EXPORT_MEDIAWORLD (with IT/EU stock support) ==========
async function stepExportMediaworld(supabase: SupabaseClient, runId: string, feeConfig: FeeConfig): Promise<{ success: boolean; error?: string }> {
  const includeEu = feeConfig?.mediaworldIncludeEu || false;
  const itDays = feeConfig?.mediaworldItPrepDays || 3;
  const euDays = feeConfig?.mediaworldEuPrepDays || 5;
  
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
    
    const mwRows: string[] = [];
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
        norm.value,
        p.PFNum.toFixed(2).replace('.', ','),
        String(leadTimeToShip),
        String(Math.min(stockResult.exportQty, 99))
      ].join(';'));
    }
    
    const mwCSV = [headers.join(';'), ...mwRows].join('\n');
    const saveResult = await uploadToStorage(supabase, 'exports', 'Export Mediaworld.csv', mwCSV, 'text/csv');
    
    if (!saveResult.success) {
      const error = `Failed to save Export Mediaworld.csv: ${saveResult.error}`;
      await updateStepResult(supabase, runId, 'export_mediaworld', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    // Update location_warnings in sync_runs
    await updateLocationWarnings(supabase, runId, warnings);
    
    await updateStepResult(supabase, runId, 'export_mediaworld', {
      status: 'success', duration_ms: Date.now() - startTime, rows: mwRows.length, skipped: mwSkipped,
      metrics: { mediaworld_export_rows: mwRows.length, mediaworld_export_skipped: mwSkipped }
    });
    
    console.log(`[sync:step:export_mediaworld] Completed: ${mwRows.length} rows, ${mwSkipped} skipped, warnings:`, warnings);
    return { success: true };
    
  } catch (e: unknown) {
    console.error(`[sync:step:export_mediaworld] Error:`, e);
    await updateStepResult(supabase, runId, 'export_mediaworld', { status: 'failed', error: errMsg(e), metrics: {} });
    return { success: false, error: errMsg(e) };
  }
}

// ========== STEP: EXPORT_EPRICE (with IT/EU stock support) ==========
async function stepExportEprice(supabase: SupabaseClient, runId: string, feeConfig: FeeConfig): Promise<{ success: boolean; error?: string }> {
  const includeEu = feeConfig?.epriceIncludeEu || false;
  const itDays = feeConfig?.epriceItPrepDays || feeConfig?.epricePrepDays || 1;
  const euDays = feeConfig?.epriceEuPrepDays || 3;
  
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
    
    const epRows: string[] = [];
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
        norm.value,
        p.PFNum.toFixed(2).replace('.', ','),
        String(Math.min(stockResult.exportQty, 99)),
        String(stockResult.leadDays)  // ePrice: no +2
      ].join(';'));
    }
    
    const epCSV = [exportHeaders.join(';'), ...epRows].join('\n');
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
    
  } catch (e: unknown) {
    console.error(`[sync:step:export_eprice] Error:`, e);
    await updateStepResult(supabase, runId, 'export_eprice', { status: 'failed', error: errMsg(e), metrics: {} });
    return { success: false, error: errMsg(e) };
  }
}

// ========== STEP: EXPORT_EAN_XLSX ==========
async function stepExportEanXlsx(supabase: SupabaseClient, runId: string): Promise<{ success: boolean; error?: string }> {
  console.log(`[sync:step:export_ean_xlsx] Starting for run ${runId}`);
  const startTime = Date.now();
  try {
    const XLSX = await import("npm:xlsx@0.18.5");
    const { content: csvContent, error: dlError } = await downloadFromStorage(supabase, 'exports', 'Catalogo EAN.csv');
    if (dlError || !csvContent) {
      const error = `Catalogo EAN.csv non trovato: ${dlError || 'empty'}`;
      await updateStepResult(supabase, runId, 'export_ean_xlsx', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    const lines = csvContent.split('\n').filter((l: string) => l.trim());
    const headers = lines[0].split(';');
    const rows = lines.slice(1).map((line: string) => {
      const vals = line.split(';');
      const row: Record<string, string> = {};
      headers.forEach((h: string, i: number) => { row[h] = vals[i] || ''; });
      return row;
    });
    const ws = XLSX.utils.json_to_sheet(rows);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'Catalogo EAN');
    const eanColIdx = headers.indexOf('EAN');
    if (eanColIdx >= 0) {
      for (let r = 1; r <= rows.length; r++) {
        const addr = XLSX.utils.encode_cell({ r, c: eanColIdx });
        if (ws[addr]) { ws[addr].t = 's'; ws[addr].z = '@'; }
      }
    }
    const xlsxBuffer = XLSX.write(wb, { bookType: 'xlsx', type: 'array' });
    const blob = new Blob([xlsxBuffer], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
    const { error: uploadError } = await supabase.storage.from('exports').upload(
      'catalogo_ean.xlsx', blob, { upsert: true, contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' }
    );
    if (uploadError) {
      const error = `Upload catalogo_ean.xlsx fallito: ${uploadError.message}`;
      await updateStepResult(supabase, runId, 'export_ean_xlsx', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    await updateStepResult(supabase, runId, 'export_ean_xlsx', {
      status: 'success', duration_ms: Date.now() - startTime, rows: rows.length,
      metrics: { ean_xlsx_rows: rows.length }
    });
    console.log(`[sync:step:export_ean_xlsx] Completed: ${rows.length} rows`);
    return { success: true };
  } catch (e: unknown) {
    console.error(`[sync:step:export_ean_xlsx] Error:`, e);
    await updateStepResult(supabase, runId, 'export_ean_xlsx', { status: 'failed', error: errMsg(e), metrics: {} });
    return { success: false, error: errMsg(e) };
  }
}

// ========== STEP: EXPORT_AMAZON (SERVER-SIDE) ==========
// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function stepExportAmazon(supabase: SupabaseClient, runId: string, feeConfig: any): Promise<{ success: boolean; error?: string }> {
  console.log(`[sync:step:export_amazon] Starting for run ${runId}`);
  const startTime = Date.now();
  try {
    const XLSX = await import("npm:xlsx@0.18.5");
    const { products, error: loadError } = await loadProductsTSV(supabase, runId);
    if (loadError || !products) {
      const error = loadError || 'Products file not found';
      await updateStepResult(supabase, runId, 'export_amazon', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    const amazonFeeDrev = feeConfig?.amazonFeeDrev ?? feeConfig?.feeDrev ?? 1.05;
    const amazonFeeMkt = feeConfig?.amazonFeeMkt ?? feeConfig?.feeMkt ?? 1.08;
    const amazonShipping = feeConfig?.amazonShippingCost ?? feeConfig?.shippingCost ?? 6.00;
    const itDays = feeConfig?.amazonItPrepDays ?? 3;
    const euDays = feeConfig?.amazonEuPrepDays ?? 5;

    // Load stock location
    let stockLocationIndex: Record<string, { stockIT: number; stockEU: number }> | null = null;
    const stockLocationPath = `stock-location/runs/${runId}.txt`;
    const { content: stockLocationContent } = await downloadFromStorage(supabase, 'ftp-import', stockLocationPath);
    if (stockLocationContent) {
      stockLocationIndex = {};
      const slLines = stockLocationContent.replace(/\r\n/g, '\n').split('\n');
      const slHeaders = slLines[0]?.split(';').map((h: string) => h.trim().toLowerCase()) || [];
      const mIdx = slHeaders.indexOf('matnr'), sIdx = slHeaders.indexOf('stock'), lIdx = slHeaders.indexOf('locationid');
      if (mIdx >= 0 && sIdx >= 0 && lIdx >= 0) {
        for (let i = 1; i < slLines.length; i++) {
          const vals = slLines[i].split(';');
          const matnr = vals[mIdx]?.trim();
          if (!matnr) continue;
          const stock = parseInt(vals[sIdx]) || 0;
          const locationId = parseInt(vals[lIdx]) || 0;
          if (!stockLocationIndex[matnr]) stockLocationIndex[matnr] = { stockIT: 0, stockEU: 0 };
          if (locationId === 4242) stockLocationIndex[matnr].stockIT += stock;
          else if (locationId === 4254) stockLocationIndex[matnr].stockEU += stock;
        }
      }
    }

    interface AmazonRec { sku: string; ean: string; quantity: number; leadDays: number; priceDisplay: string; }
    const validRecords: AmazonRec[] = [];
    let skipped = 0;
    for (const p of products) {
      const norm = normalizeEAN(p.EAN);
      if (!norm.ok || !norm.value || (norm.value.length !== 13 && norm.value.length !== 14)) { skipped++; continue; }
      if (!p.MPN || !p.MPN.trim()) { skipped++; continue; }
      let stockIT = p.Stock || 0, stockEU = 0;
      if (stockLocationIndex?.[p.Matnr]) { stockIT = stockLocationIndex[p.Matnr].stockIT; stockEU = stockLocationIndex[p.Matnr].stockEU; }
      const stockResult = resolveMarketplaceStock(stockIT, stockEU, true, itDays, euDays);
      if (!stockResult.shouldExport || stockResult.exportQty < 2) { skipped++; continue; }
      let baseCents = 0;
      if (p.CBP > 0) baseCents = Math.round((p.CBP + (p.Sur || 0)) * 100);
      else if (p.LP > 0) baseCents = Math.round(p.LP * 100);
      if (baseCents <= 0) { skipped++; continue; }
      const afterFees = Math.round(Math.round(Math.round((baseCents + Math.round(amazonShipping * 100)) * 1.22) * amazonFeeDrev) * amazonFeeMkt);
      const finalCents = toComma99Cents(afterFees);
      validRecords.push({ sku: p.MPN.replace(/[\x00-\x1f\x7f]/g, ''), ean: norm.value, quantity: stockResult.exportQty, leadDays: stockResult.leadDays, priceDisplay: (finalCents / 100).toFixed(2) });
    }
    if (validRecords.length === 0) {
      const error = 'Nessuna riga esportabile per Amazon';
      await updateStepResult(supabase, runId, 'export_amazon', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    console.log(`[sync:step:export_amazon] ${validRecords.length} valid, ${skipped} skipped`);

    // Load template
    let templateBuffer: ArrayBuffer | null = null;
    const { data: tplData } = await supabase.storage.from('exports').download('templates/amazon/ListingLoader.xlsm');
    if (tplData) { templateBuffer = await tplData.arrayBuffer(); console.log('[export_amazon] Template from Storage'); }
    if (!templateBuffer) {
      try {
        const resp = await fetch('https://catalogserverside.lovable.app/amazon/ListingLoader.xlsm');
        if (resp.ok) {
          const ct = resp.headers.get('content-type') || '';
          if (!ct.includes('text/html')) {
            const buf = await resp.arrayBuffer();
            const preview = new TextDecoder('ascii', { fatal: false }).decode(new Uint8Array(buf.slice(0, 64)));
            if (!preview.toLowerCase().includes('<html')) { templateBuffer = buf; console.log('[export_amazon] Template from public URL'); }
          }
        }
      } catch { /* ignore */ }
    }
    if (!templateBuffer) {
      const error = 'missing_amazon_template: Caricare ListingLoader.xlsm in exports/templates/amazon/ListingLoader.xlsm';
      await updateStepResult(supabase, runId, 'export_amazon', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }

    // Build XLSM
    const wb = XLSX.read(new Uint8Array(templateBuffer), { type: 'array', bookVBA: true });
    const ws = wb.Sheets['Modello'];
    if (!ws) { await updateStepResult(supabase, runId, 'export_amazon', { status: 'failed', error: 'Foglio Modello non trovato', metrics: {} }); return { success: false, error: 'Foglio Modello non trovato' }; }
    const COL_A=0,COL_B=1,COL_C=2,COL_H=7,COL_AF=31,COL_AG=32,COL_AH=33,COL_AK=36,COL_BJ=61;
    const DS = 4;
    if (ws['!ref']) {
      const range = XLSX.utils.decode_range(ws['!ref']);
      for (let R = DS; R <= range.e.r; R++) for (const C of [COL_A,COL_B,COL_C,COL_H,COL_AF,COL_AG,COL_AH,COL_AK,COL_BJ]) delete ws[XLSX.utils.encode_cell({ r: R, c: C })];
    }
    for (let i = 0; i < validRecords.length; i++) {
      const rec = validRecords[i], R = DS + i;
      ws[XLSX.utils.encode_cell({r:R,c:COL_A})]={t:'s',v:rec.sku};
      ws[XLSX.utils.encode_cell({r:R,c:COL_B})]={t:'s',v:'EAN'};
      ws[XLSX.utils.encode_cell({r:R,c:COL_C})]={t:'s',v:rec.ean,z:'@'};
      ws[XLSX.utils.encode_cell({r:R,c:COL_H})]={t:'s',v:'Nuovo'};
      ws[XLSX.utils.encode_cell({r:R,c:COL_AF})]={t:'s',v:'Default'};
      ws[XLSX.utils.encode_cell({r:R,c:COL_AG})]={t:'n',v:rec.quantity};
      ws[XLSX.utils.encode_cell({r:R,c:COL_AH})]={t:'n',v:rec.leadDays};
      ws[XLSX.utils.encode_cell({r:R,c:COL_AK})]={t:'s',v:rec.priceDisplay};
      ws[XLSX.utils.encode_cell({r:R,c:COL_BJ})]={t:'s',v:'Modello Amazon predefinito'};
    }
    const lastRow = DS + validRecords.length - 1;
    if (ws['!ref']) { const r = XLSX.utils.decode_range(ws['!ref']); r.e.r = Math.max(r.e.r, lastRow); r.e.c = Math.max(r.e.c, COL_BJ); ws['!ref'] = XLSX.utils.encode_range(r); }
    const hasVBA = Boolean((wb as unknown as Record<string,unknown>).vbaraw);
    const xlsmOut = XLSX.write(wb, { bookType: 'xlsm', bookVBA: hasVBA, type: 'array' });
    const xlsmBlob = new Blob([xlsmOut], { type: 'application/vnd.ms-excel.sheet.macroEnabled.12' });
    const { error: xlsmErr } = await supabase.storage.from('exports').upload('amazon_listing_loader.xlsm', xlsmBlob, { upsert: true, contentType: 'application/vnd.ms-excel.sheet.macroEnabled.12' });
    if (xlsmErr) { await updateStepResult(supabase, runId, 'export_amazon', { status: 'failed', error: xlsmErr.message, metrics: {} }); return { success: false, error: xlsmErr.message }; }

    // TXT
    const txtHeader = 'sku\tprice\tminimum-seller-allowed-price\tmaximum-seller-allowed-price\tquantity\tfulfillment-channel\thandling-time\n';
    const txtContent = txtHeader + validRecords.map(r => `${r.sku}\t${r.priceDisplay}\t\t\t${r.quantity}\t\t${r.leadDays}\n`).join('');
    const txtSave = await uploadToStorage(supabase, 'exports', 'amazon_price_inventory.txt', txtContent, 'text/plain');
    if (!txtSave.success) { await updateStepResult(supabase, runId, 'export_amazon', { status: 'failed', error: txtSave.error!, metrics: {} }); return { success: false, error: txtSave.error }; }

    await updateStepResult(supabase, runId, 'export_amazon', {
      status: 'success', duration_ms: Date.now() - startTime, rows: validRecords.length, skipped,
      metrics: { amazon_export_rows: validRecords.length, amazon_export_skipped: skipped }
    });
    console.log(`[sync:step:export_amazon] Completed: ${validRecords.length} rows`);
    return { success: true };
  } catch (e: unknown) {
    console.error(`[sync:step:export_amazon] Error:`, e);
    await updateStepResult(supabase, runId, 'export_amazon', { status: 'failed', error: errMsg(e), metrics: {} });
    return { success: false, error: errMsg(e) };
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
    
    if (step === 'parse_merge') {
      const itDays = fee_config?.mediaworldItPrepDays || 3;
      const euDays = fee_config?.mediaworldEuPrepDays || 5;
      validateGoldenCases('[server]', itDays, euDays);
    }
    
    let result: { success: boolean; error?: string; status?: string };
    
    switch (step) {
      case 'parse_merge': result = await stepParseMerge(supabase, run_id); break;
      case 'ean_mapping': result = await stepEanMapping(supabase, run_id); break;
      case 'pricing': result = await stepPricing(supabase, run_id, fee_config); break;
      case 'export_ean': result = await stepExportEan(supabase, run_id); break;
      case 'export_ean_xlsx': result = await stepExportEanXlsx(supabase, run_id); break;
      case 'export_amazon': result = await stepExportAmazon(supabase, run_id, fee_config); break;
      case 'export_mediaworld': result = await stepExportMediaworld(supabase, run_id, fee_config); break;
      case 'export_eprice': result = await stepExportEprice(supabase, run_id, fee_config); break;
      default:
        return new Response(JSON.stringify({ status: 'error', message: `Unknown step: ${step}` }), 
          { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }
    
    console.log(`[sync-step-runner] Step ${step} result: success=${result.success}, status=${result.status || 'N/A'}, error=${result.error || 'none'}`);
    
    return new Response(JSON.stringify({ 
      status: result.success ? 'ok' : 'error', 
      step_status: result.status,
      ...result 
    }), { status: result.success ? 200 : 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    
  } catch (e: unknown) {
    console.error('[sync-step-runner] Fatal error:', e);
    return new Response(JSON.stringify({ status: 'error', message: errMsg(e) }), 
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
  }
});
