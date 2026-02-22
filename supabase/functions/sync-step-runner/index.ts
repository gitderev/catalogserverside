import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";
import { assertLockOwned, renewLockLease, LOCK_TTL_SECONDS } from "../_shared/lock.ts";
import { unzipSync } from "https://esm.sh/fflate@0.8.2";

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
  // Per-export pricing (null = use global)
  eanFeeDrev?: number;
  eanFeeMkt?: number;
  eanShippingCost?: number;
  mediaworldFeeDrev?: number;
  mediaworldFeeMkt?: number;
  mediaworldShippingCost?: number;
  epriceFeeDrev?: number;
  epriceFeeMkt?: number;
  epriceShippingCost?: number;
  amazonIncludeEu?: boolean;
  amazonItPrepDays?: number;
  amazonEuPrepDays?: number;
  amazonFeeDrev?: number;
  amazonFeeMkt?: number;
  amazonShippingCost?: number;
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

// ========== TEMPLATE CONSTANTS ==========
const TEMPLATE_BUCKET = 'exports';
const TEMPLATE_BASE_PATH = 'templates';

// ========== SAFE RPC LOG (no .catch on PromiseLike) ==========
async function safeLogEvent(supabase: SupabaseClient, runId: string, level: string, message: string, details: Record<string, unknown> = {}): Promise<void> {
  try {
    const { error } = await supabase.rpc('log_sync_event', {
      p_run_id: runId, p_level: level, p_message: message, p_details: details
    });
    if (error) console.warn(`[safeLogEvent] rpc error: ${error.message}`);
  } catch (e: unknown) {
    console.warn(`[safeLogEvent] exception: ${errMsg(e)}`);
  }
}

// ========== SHA-256 TEMPLATE PINNING ==========
async function computeSHA256(data: Uint8Array): Promise<string> {
  const hash = await crypto.subtle.digest('SHA-256', data);
  return Array.from(new Uint8Array(hash)).map(b => b.toString(16).padStart(2, '0')).join('');
}

// Hardcoded SHA-256 checksums for pinned templates in storage
// To recompute: call sync-step-runner with step="compute_template_checksums"
const TEMPLATE_SHA256: Record<string, string> = {
  'Catalogo EAN.xlsx': '038574b628e23b845297f6eb82b9ba10028a140413bde0e6890927ba0a0afe23',
  'Export ePrice.xlsx': '8755f23f7e9e9a1fb4fdcbf0c6208789558ebf40533a6436395baa40f1581005',
  'Export Mediaworld.xlsx': 'aafeaadcd7567073ba595cb317af855b8fd291dca074a0a6acaac1336a589c0d',
};

async function verifyTemplateChecksum(
  templateBytes: Uint8Array,
  templateName: string,
  supabase: SupabaseClient,
  runId: string
): Promise<{ ok: boolean; actual: string; expected: string }> {
  const actual = await computeSHA256(templateBytes);
  const expected = TEMPLATE_SHA256[templateName];

  // BLOCKING: template must have a pinned checksum
  if (expected === undefined || expected === '' || expected === '__PLACEHOLDER__') {
    console.error(`[checksum] Template ${templateName}: NOT PINNED — BLOCKING (template_checksum_not_pinned)`);
    await safeLogEvent(supabase, runId, 'ERROR', 'template_checksum_not_pinned', { template: templateName, actual_sha256: actual });
    return { ok: false, actual, expected: '(not pinned)' };
  }

  // Log the compare operation explicitly
  console.log(`[checksum] INFO template_checksum_compare`, JSON.stringify({ templateName, expected, actual }));
  await safeLogEvent(supabase, runId, 'INFO', 'template_checksum_compare', { templateName, expected, actual });

  // BLOCKING: checksum must match
  if (actual !== expected) {
    console.error(`[checksum] ERROR template_checksum_mismatch`, JSON.stringify({ templateName, expected, actual }));
    await safeLogEvent(supabase, runId, 'ERROR', 'template_checksum_mismatch', { template: templateName, expected, actual, bytes: templateBytes.length });
    return { ok: false, actual, expected };
  }

  console.log(`[checksum] Template ${templateName}: OK (${templateBytes.length} bytes)`);
  await safeLogEvent(supabase, runId, 'INFO', 'template_checksum_ok', { template: templateName, sha256: actual, bytes: templateBytes.length });
  return { ok: true, actual, expected };
}

// ========== UNIFIED TEMPLATE LOADER (Storage API only) ==========
async function loadTemplateFromStorage(
  supabase: SupabaseClient,
  templateName: string,
  runId: string
): Promise<Uint8Array> {
  const path = `${TEMPLATE_BASE_PATH}/${templateName}`;
  console.log(`[template] INFO template_config`, JSON.stringify({ bucketName: TEMPLATE_BUCKET, basePath: TEMPLATE_BASE_PATH }));
  await safeLogEvent(supabase, runId, 'INFO', 'template_download_started', { templateName, bucketName: TEMPLATE_BUCKET, path });

  const { data, error } = await supabase.storage.from(TEMPLATE_BUCKET).download(path);

  if (error || !data) {
    const msg = `template_missing: ${TEMPLATE_BUCKET}/${path} — ${error?.message || 'data is null'}`;
    console.error(`[template] ERROR template_download_failed`, JSON.stringify({ templateName, bucketName: TEMPLATE_BUCKET, path, error_message: error?.message || 'null data' }));
    await safeLogEvent(supabase, runId, 'ERROR', 'template_download_failed', { templateName, bucketName: TEMPLATE_BUCKET, path, error_message: error?.message || 'null data' });
    throw new Error(msg);
  }

  const bytes = new Uint8Array(await data.arrayBuffer());

  if (bytes.length === 0) {
    const msg = `template_empty: ${TEMPLATE_BUCKET}/${path} has 0 bytes`;
    console.error(`[template] ERROR template_download_failed`, JSON.stringify({ templateName, bucketName: TEMPLATE_BUCKET, path, error_message: 'template_empty' }));
    await safeLogEvent(supabase, runId, 'ERROR', 'template_download_failed', { templateName, bucketName: TEMPLATE_BUCKET, path, error_message: 'template_empty' });
    throw new Error(msg);
  }

  console.log(`[template] INFO template_download_ok`, JSON.stringify({ templateName, size_bytes: bytes.length }));
  await safeLogEvent(supabase, runId, 'INFO', 'template_download_ok', { templateName, size_bytes: bytes.length });

  // SHA-256 checksum verification (blocking)
  const checksumResult = await verifyTemplateChecksum(bytes, templateName, supabase, runId);
  if (!checksumResult.ok) {
    throw new Error(`Template checksum failed for ${templateName}: expected=${checksumResult.expected}, actual=${checksumResult.actual}`);
  }

  return bytes;
}

// ========== ZIP-LEVEL XML COMPARISON (freeze panes + styles) ==========
function getZipSheetXmlPath(zipEntries: Record<string, Uint8Array>, sheetName: string): string | null {
  const workbookXml = new TextDecoder().decode(zipEntries['xl/workbook.xml']);
  const escaped = sheetName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  let rId: string | null = null;

  const m1 = workbookXml.match(new RegExp(`<sheet[^>]*name="${escaped}"[^>]*r:id="(rId\\d+)"`, 'i'));
  if (m1) { rId = m1[1]; }
  else {
    const m2 = workbookXml.match(new RegExp(`<sheet[^>]*r:id="(rId\\d+)"[^>]*name="${escaped}"`, 'i'));
    if (m2) { rId = m2[1]; }
  }
  if (!rId) return null;

  const relsXml = new TextDecoder().decode(zipEntries['xl/_rels/workbook.xml.rels']);
  const relMatch = relsXml.match(new RegExp(`<Relationship[^>]*Id="${rId}"[^>]*Target="([^"]+)"`, 'i'));
  if (!relMatch) return null;
  return `xl/${relMatch[1]}`;
}

function extractXmlSection(xml: string, tagName: string): string | null {
  const regex = new RegExp(`<${tagName}[\\s>][\\s\\S]*?<\\/${tagName}>`, 'i');
  const match = xml.match(regex);
  return match ? match[0] : null;
}

// Helper: find byte position of an ASCII needle in a Uint8Array (for prefix-only XML decode)
function findByteSequencePos(haystack: Uint8Array, needle: string): number {
  const needleBytes = new TextEncoder().encode(needle);
  const hLen = haystack.length;
  const nLen = needleBytes.length;
  for (let i = 0; i <= hLen - nLen; i++) {
    let match = true;
    for (let j = 0; j < nLen; j++) {
      if (haystack[i + j] !== needleBytes[j]) { match = false; break; }
    }
    if (match) return i;
  }
  return -1;
}

async function compareZipXmlIntegrity(
  templateBytes: Uint8Array,
  outputBytes: Uint8Array,
  dataSheetName: string,
  exportName: string,
  preExtractedTmplZip?: Record<string, Uint8Array>,
  logStage?: (stage: string, extra?: Record<string, unknown>) => Promise<void>
): Promise<{ errors: string[]; outZip?: Record<string, Uint8Array>; tmplZip?: Record<string, Uint8Array> }> {
  const errors: string[] = [];

  await logStage?.('validation_start');

  let tmplZip: Record<string, Uint8Array>;
  let outZip: Record<string, Uint8Array>;

  try {
    if (preExtractedTmplZip) {
      tmplZip = preExtractedTmplZip;
      await logStage?.('unzip_template_reused');
    } else {
      tmplZip = unzipSync(templateBytes);
      await logStage?.('unzip_template_done');
    }
  } catch (e: unknown) {
    errors.push(`zip_unpack_template_failed: ${errMsg(e)}`);
    return { errors };
  }
  try {
    outZip = unzipSync(outputBytes);
    await logStage?.('unzip_output_done', { entry_count: Object.keys(outZip).length });
  } catch (e: unknown) {
    errors.push(`zip_unpack_output_failed: ${errMsg(e)}`);
    return { errors };
  }

  // 1. Compare xl/styles.xml via SHA-256 digest (no string allocation)
  const tmplStyles = tmplZip['xl/styles.xml'];
  const outStyles = outZip['xl/styles.xml'];
  if (!tmplStyles) {
    errors.push('styles_xml_missing_in_template');
  } else if (!outStyles) {
    errors.push('styles_xml_missing_in_output');
  } else if (tmplStyles.byteLength !== outStyles.byteLength) {
    errors.push(`styles_xml_mismatch: xl/styles.xml differs (template=${tmplStyles.byteLength}b, output=${outStyles.byteLength}b)`);
  } else {
    const [sD1, sD2] = await Promise.all([
      crypto.subtle.digest('SHA-256', tmplStyles),
      crypto.subtle.digest('SHA-256', outStyles),
    ]);
    const h1 = new Uint8Array(sD1);
    const h2 = new Uint8Array(sD2);
    let stylesMatch = true;
    for (let i = 0; i < h1.length; i++) { if (h1[i] !== h2[i]) { stylesMatch = false; break; } }
    if (!stylesMatch) {
      errors.push(`styles_xml_mismatch: xl/styles.xml differs (template=${tmplStyles.byteLength}b, output=${outStyles.byteLength}b)`);
    } else {
      console.log(`[zip-verify:${exportName}] xl/styles.xml: IDENTICAL (${tmplStyles.byteLength} bytes)`);
    }
  }
  await logStage?.('compare_styles_done');

  // 2. Compare <sheetViews> in data sheet — decode only prefix before <sheetData to avoid huge allocations
  // sheetViews is always before sheetData in OOXML, so decoding only the prefix (typically a few KB)
  // avoids decoding the full data sheet XML (potentially 50MB+ for ~28k rows).
  const tmplSheetPath = getZipSheetXmlPath(tmplZip, dataSheetName);
  const outSheetPath = getZipSheetXmlPath(outZip, dataSheetName);

  if (!tmplSheetPath) {
    errors.push(`freeze_panes_check_failed: cannot resolve sheet XML for "${dataSheetName}" in template`);
  } else if (!outSheetPath) {
    errors.push(`freeze_panes_check_failed: cannot resolve sheet XML for "${dataSheetName}" in output`);
  } else {
    const tmplSheetBytes = tmplZip[tmplSheetPath];
    const outSheetBytes = outZip[outSheetPath];
    if (!tmplSheetBytes || !outSheetBytes) {
      if (!tmplSheetBytes) errors.push(`freeze_panes_check_failed: sheet XML bytes missing in template for "${dataSheetName}"`);
      if (!outSheetBytes) errors.push(`freeze_panes_check_failed: sheet XML bytes missing in output for "${dataSheetName}"`);
    } else {
      // Only decode prefix up to <sheetData (a few KB) instead of full sheet XML (potentially 50MB+)
      const tmplCut = findByteSequencePos(tmplSheetBytes, '<sheetData');
      const outCut = findByteSequencePos(outSheetBytes, '<sheetData');
      const tmplPrefix = new TextDecoder().decode(
        tmplSheetBytes.subarray(0, tmplCut > 0 ? tmplCut : Math.min(tmplSheetBytes.byteLength, 32768))
      );
      const outPrefix = new TextDecoder().decode(
        outSheetBytes.subarray(0, outCut > 0 ? outCut : Math.min(outSheetBytes.byteLength, 32768))
      );

      const tmplSheetViews = extractXmlSection(tmplPrefix, 'sheetViews');
      const outSheetViews = extractXmlSection(outPrefix, 'sheetViews');

      if (tmplSheetViews === null && outSheetViews === null) {
        console.log(`[zip-verify:${exportName}] sheetViews: both absent (OK)`);
      } else if (tmplSheetViews === null) {
        errors.push('freeze_panes_mismatch: template has no <sheetViews> but output does');
      } else if (outSheetViews === null) {
        errors.push('freeze_panes_mismatch: template has <sheetViews> but output does not');
      } else if (tmplSheetViews !== outSheetViews) {
        errors.push('freeze_panes_mismatch: <sheetViews> differs between template and output');
        console.error(`[zip-verify:${exportName}] sheetViews MISMATCH:\n  tmpl: ${tmplSheetViews.substring(0, 300)}\n  out:  ${outSheetViews.substring(0, 300)}`);
      } else {
        console.log(`[zip-verify:${exportName}] sheetViews (freeze panes): IDENTICAL`);
      }
    }
  }
  await logStage?.('compare_sheetviews_done');

  return { errors, outZip, tmplZip };
}

// ========== EXPORT VALIDATION: compare generated XLSX vs template ==========
// deno-lint-ignore no-explicit-any
async function validateExportVsTemplate(
  // deno-lint-ignore no-explicit-any
  XLSX: any,
  // deno-lint-ignore no-explicit-any
  generatedWb: any,
  templateBytes: Uint8Array,
  exportName: string,
  dataSheet: string,
  eanHeaderName: string,
  supabase: SupabaseClient,
  runId: string,
  options?: {
    protectedSheets?: string[];      // Mediaworld: ReferenceData, Columns
    headerCellsModifiedCount?: number; // Tracking from caller (must be 0)
    cellsWrittenBySheet?: Record<string, number>;
    // deno-lint-ignore no-explicit-any
    tmplWbOverride?: any; // Pre-parsed template workbook to avoid redundant XLSX.read
    preExtractedTmplZip?: Record<string, Uint8Array>; // Pre-extracted template ZIP entries to avoid redundant unzipSync
    logStage?: (stage: string, extra?: Record<string, unknown>) => Promise<void>; // Fine-grained stage telemetry
  },
  outputBytes?: Uint8Array
): Promise<{ passed: boolean; errors: string[]; warnings: string[] }> {
  const errors: string[] = [];
  const warnings: string[] = [];
  // Use pre-parsed template workbook if provided, otherwise parse (CPU-expensive for large templates)
  const tmplWb = options?.tmplWbOverride ?? XLSX.read(new Uint8Array(templateBytes), { type: 'array' });

  // ============================================================
  // 1. Sheet count and names (exact order)
  // ============================================================
  if (generatedWb.SheetNames.length !== tmplWb.SheetNames.length) {
    errors.push(`sheet_count: expected ${tmplWb.SheetNames.length}, got ${generatedWb.SheetNames.length}`);
  }
  if (JSON.stringify(generatedWb.SheetNames) !== JSON.stringify(tmplWb.SheetNames)) {
    errors.push(`sheet_names: expected ${JSON.stringify(tmplWb.SheetNames)}, got ${JSON.stringify(generatedWb.SheetNames)}`);
  }
  await options?.logStage?.('compare_workbook_done');

  // ============================================================
  // 2. Data sheet existence
  // ============================================================
  const tmplWs = tmplWb.Sheets[dataSheet];
  const genWs = generatedWb.Sheets[dataSheet];
  if (!tmplWs || !genWs) {
    errors.push(`sheet_missing: ${dataSheet}`);
    return logAndReturn(errors, warnings);
  }

  const tmplRange = tmplWs['!ref'] ? XLSX.utils.decode_range(tmplWs['!ref']) : null;
  if (!tmplRange) { errors.push(`template_empty_range: ${dataSheet}`); return logAndReturn(errors, warnings); }

  // ============================================================
  // 3. Header row 0: values MUST be identical (no writes allowed)
  // ============================================================
  let headerMismatchCount = 0;
  for (let c = 0; c <= tmplRange.e.c; c++) {
    const addr = XLSX.utils.encode_cell({ r: 0, c });
    const tv = tmplWs[addr]?.v?.toString() || '';
    const gv = genWs[addr]?.v?.toString() || '';
    if (tv !== gv) {
      errors.push(`header[${c}]: expected "${tv}", got "${gv}"`);
      headerMismatchCount++;
    }
  }
  if (headerMismatchCount > 0) {
    errors.push(`header_cells_modified_count: ${headerMismatchCount} (MUST be 0)`);
  }

  // ============================================================
  // 4. Caller-tracked header_cells_modified_count assert
  // ============================================================
  if (options?.headerCellsModifiedCount !== undefined && options.headerCellsModifiedCount > 0) {
    errors.push(`caller_header_cells_modified_count: ${options.headerCellsModifiedCount} (MUST be 0)`);
  }

  // Log cells_written_by_sheet
  if (options?.cellsWrittenBySheet) {
    console.log(`[validate:${exportName}] cells_written_by_sheet:`, options.cellsWrittenBySheet);
  }

  // ============================================================
  // 5. AutoFilter (exact match)
  // ============================================================
  const tf = JSON.stringify(tmplWs['!autofilter'] || null);
  const gf = JSON.stringify(genWs['!autofilter'] || null);
  if (tf !== gf) errors.push(`autofilter: expected ${tf}, got ${gf}`);

  // ============================================================
  // 6. Column widths (all template columns must match)
  // ============================================================
  const tmplCols = tmplWs['!cols'];
  const genCols = genWs['!cols'];
  if (tmplCols) {
    if (!genCols) {
      errors.push(`column_widths: template has !cols but generated does not`);
    } else {
      for (let i = 0; i < tmplCols.length; i++) {
        const tw = tmplCols[i]?.wch ?? tmplCols[i]?.wpx ?? tmplCols[i]?.width;
        const gw = genCols[i]?.wch ?? genCols[i]?.wpx ?? genCols[i]?.width;
        if (tw != null && gw != null && tw !== gw) {
          errors.push(`col_width[${i}]: expected ${tw}, got ${gw}`);
        } else if (tw != null && gw == null) {
          errors.push(`col_width[${i}]: template has width=${tw} but generated is null`);
        }
      }
    }
  }

  // ============================================================
  // 7. Freeze panes + 8. Styles: deterministic ZIP-level XML comparison (BLOCKING)
  // ============================================================
  // Instead of relying on xlsx library (which cannot read freeze panes or styleId),
  // we unzip both XLSX files and compare the raw XML sections directly.
  // This is a BLOCKING check: any mismatch fails validation.
  let extractedOutZip: Record<string, Uint8Array> | undefined;
  let extractedTmplZip: Record<string, Uint8Array> | undefined;
  if (outputBytes) {
    const zipResult = await compareZipXmlIntegrity(templateBytes, outputBytes, dataSheet, exportName, options?.preExtractedTmplZip, options?.logStage);
    extractedOutZip = zipResult.outZip;
    extractedTmplZip = zipResult.tmplZip;
    for (const e of zipResult.errors) {
      errors.push(e);
    }
    if (zipResult.errors.length === 0) {
      console.log(`[validate:${exportName}] ZIP-level integrity (freeze_panes + styles.xml): PASSED`);
    }
  } else {
    errors.push('zip_verification_impossible: outputBytes not provided (BLOCKING — cannot verify freeze_panes and styles)');
  }

  // ============================================================
  // 9. Number formats: compare cell.z for first data row
  // ============================================================
  const genRange = genWs['!ref'] ? XLSX.utils.decode_range(genWs['!ref']) : null;
  if (genRange) {
    // Determine first data row (Mediaworld=2, others=1)
    const firstDataRow = dataSheet === 'Data' ? 2 : 1;
    if (genRange.e.r >= firstDataRow) {
      for (let c = 0; c <= tmplRange.e.c; c++) {
        // Compare template's first data row format with generated first data row
        const tmplDataRow = dataSheet === 'Data' ? 2 : 1;
        const tmplAddr = XLSX.utils.encode_cell({ r: tmplDataRow, c });
        const genAddr = XLSX.utils.encode_cell({ r: firstDataRow, c });
        const tmplZ = tmplWs[tmplAddr]?.z || null;
        const genZ = genWs[genAddr]?.z || null;
        // Mismatch: one has format, other doesn't (or different format)
        if (tmplZ !== genZ) {
          // Only error if template has a specific format that output doesn't match
          // null vs null is OK, null vs '@' is mismatch
          if (tmplZ !== null || genZ !== null) {
            errors.push(`number_format[col${c},row${firstDataRow}]: template="${tmplZ}", generated="${genZ}"`);
          }
        }
      }
    }
  }

  // ============================================================
  // 10. EAN column: type check + leading zeros preservation
  // ============================================================
  if (genRange) {
    let eanCol = -1;
    // Find EAN column by case-insensitive match or "contains ean"
    for (let c = 0; c <= tmplRange.e.c; c++) {
      const hv = (tmplWs[XLSX.utils.encode_cell({ r: 0, c })]?.v || '').toString().toLowerCase().trim();
      if (hv === eanHeaderName.toLowerCase() || hv.includes('ean')) { eanCol = c; break; }
    }
    if (eanCol < 0) {
      errors.push(`ean_column "${eanHeaderName}" not found in template headers (also tried case-insensitive "ean" match)`);
    } else {
      const startRow = dataSheet === 'Data' ? 2 : 1;
      const checkLimit = Math.min(genRange.e.r, startRow + 99); // Check up to 100 rows
      for (let r = startRow; r <= checkLimit; r++) {
        const cell = genWs[XLSX.utils.encode_cell({ r, c: eanCol })];
        if (!cell) continue;
        if (cell.t !== 's') {
          errors.push(`ean_type_row${r}: expected 's' (string), got '${cell.t}'`);
        }
        // Leading zeros: EAN must be 13+ chars when present
        const val = String(cell.v || '');
        if (val && /^\d+$/.test(val) && val.length < 12) {
          errors.push(`ean_leading_zeros_row${r}: value "${val}" too short (${val.length} digits), possible zero truncation`);
        }
      }
    }
  }

  // ============================================================
  // 11. ePrice-specific: column count and header order
  // ============================================================
  if (exportName === 'Export ePrice') {
    // Count non-empty headers in template
    let tmplHeaderCount = 0;
    for (let c = 0; c <= tmplRange.e.c; c++) {
      const hv = (tmplWs[XLSX.utils.encode_cell({ r: 0, c })]?.v || '').toString().trim();
      if (hv) tmplHeaderCount++;
    }
    const genRangeEp = genWs['!ref'] ? XLSX.utils.decode_range(genWs['!ref']) : null;
    if (genRangeEp) {
      let genHeaderCount = 0;
      for (let c = 0; c <= genRangeEp.e.c; c++) {
        const hv = (genWs[XLSX.utils.encode_cell({ r: 0, c })]?.v || '').toString().trim();
        if (hv) genHeaderCount++;
      }
      if (tmplHeaderCount !== genHeaderCount) {
        errors.push(`eprice_header_count: expected ${tmplHeaderCount}, got ${genHeaderCount}`);
      }
    }
  }

  // ============================================================
  // 12. Protected sheets integrity (Mediaworld: ReferenceData, Columns)
  // Full ZIP-level XML comparison of each protected sheet's worksheet XML.
  // Uses preExtractedTmplZip (already available) and extractedOutZip (from step 7/8).
  // No hardcoded hashes; digest computed dynamically from template at runtime.
  // ============================================================
  if (options?.protectedSheets) {
    const tmplZipEntries = extractedTmplZip;
    if (!tmplZipEntries || !extractedOutZip) {
      // Cannot perform ZIP-level protected sheet validation without ZIP entries
      for (const sheetName of options.protectedSheets) {
        errors.push(`protected_sheet_validation_unavailable: ${sheetName} (ZIP entries not available for comparison)`);
      }
    } else {
      for (const sheetName of options.protectedSheets) {
        // 12a. Presence check in workbook objects
        const tmplSheet = tmplWb.Sheets[sheetName];
        const genSheet = generatedWb.Sheets[sheetName];
        if (!tmplSheet) { errors.push(`protected_sheet_missing_in_template: ${sheetName}`); continue; }
        if (!genSheet) { errors.push(`protected_sheet_missing_in_output: ${sheetName}`); continue; }

        // 12b. Resolve ZIP XML paths for this sheet in both template and output
        const tmplXmlPath = getZipSheetXmlPath(tmplZipEntries, sheetName);
        const outXmlPath = getZipSheetXmlPath(extractedOutZip, sheetName);
        if (!tmplXmlPath) {
          errors.push(`protected_sheet_xml_not_found_in_template: ${sheetName}`);
          continue;
        }
        if (!outXmlPath) {
          errors.push(`protected_sheet_xml_not_found_in_output: ${sheetName}`);
          continue;
        }

        // 12c. Compare worksheet XML via SHA-256 digest on raw Uint8Array (no string decode)
        let tmplXmlBytes: Uint8Array | null = tmplZipEntries[tmplXmlPath] ?? null;
        let outXmlBytes: Uint8Array | null = extractedOutZip[outXmlPath] ?? null;
        if (!tmplXmlBytes) {
          errors.push(`protected_sheet_xml_missing_in_template_zip: ${sheetName} (${tmplXmlPath})`);
          continue;
        }
        if (!outXmlBytes) {
          errors.push(`protected_sheet_xml_missing_in_output_zip: ${sheetName} (${outXmlPath})`);
          continue;
        }

        const tmplLen = tmplXmlBytes.byteLength;
        const outLen = outXmlBytes.byteLength;

        // Fast path: different lengths => guaranteed mismatch, skip digest
        if (tmplLen !== outLen) {
          errors.push(`protected_sheet_content_mismatch: ${sheetName} (template XML ${tmplLen}b vs output XML ${outLen}b)`);
          console.error(`[validate:${exportName}] Protected sheet ${sheetName}: SIZE MISMATCH (tmpl=${tmplLen}b, out=${outLen}b)`);
          tmplXmlBytes = null; outXmlBytes = null;
          continue;
        }

        // SHA-256 digest comparison on raw bytes (no TextDecoder allocation)
        const [tmplDigest, outDigest] = await Promise.all([
          crypto.subtle.digest('SHA-256', tmplXmlBytes),
          crypto.subtle.digest('SHA-256', outXmlBytes),
        ]);
        // Release byte references immediately after digest
        tmplXmlBytes = null; outXmlBytes = null;

        const tmplHash = new Uint8Array(tmplDigest);
        const outHash = new Uint8Array(outDigest);
        let match = true;
        for (let i = 0; i < tmplHash.length; i++) {
          if (tmplHash[i] !== outHash[i]) { match = false; break; }
        }

        if (!match) {
          errors.push(`protected_sheet_content_mismatch: ${sheetName} (template XML ${tmplLen}b vs output XML ${outLen}b)`);
          console.error(`[validate:${exportName}] Protected sheet ${sheetName}: XML MISMATCH (tmpl=${tmplLen}b, out=${outLen}b)`);
        } else {
          console.log(`[validate:${exportName}] Protected sheet ${sheetName}: XML IDENTICAL (${tmplLen}b)`);
        }
      }
    }
  }

  if (options?.protectedSheets) {
    await options?.logStage?.('compare_protected_sheets_done', {
      protected_sheets_count: options.protectedSheets.length,
      protected_sheets_names: options.protectedSheets,
    });
  }
  await options?.logStage?.('validation_done', { passed: errors.length === 0, error_count: errors.length });

  return logAndReturn(errors, warnings);

  async function logAndReturn(errs: string[], warns: string[]): Promise<{ passed: boolean; errors: string[]; warnings: string[] }> {
    if (errs.length > 0) {
      console.error(`[validate:${exportName}] FAILED (${errs.length} errors):`, errs);
      await safeLogEvent(supabase, runId, 'ERROR', 'validation_failed', { export_name: exportName, errors: errs.slice(0, 20), warnings: warns });
    } else {
      console.log(`[validate:${exportName}] PASSED (${warns.length} warnings)`);
      await safeLogEvent(supabase, runId, 'INFO', 'validation_ok', { export_name: exportName, warnings: warns });
    }
    return { passed: errs.length === 0, errors: errs, warnings: warns };
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
  // Lock guard: assert ownership and renew lease before writing steps/metrics
  const lockCheck = await assertLockOwned(supabase, runId);
  if (!lockCheck.owned) {
    console.error(`[parse_merge] LOCK NOT OWNED in updateParseMergeState: holder=${lockCheck.holder_run_id}`);
    throw new Error(`lock_ownership_lost: cannot update parse_merge state, lock held by ${lockCheck.holder_run_id}`);
  }
  await renewLockLease(supabase, runId, LOCK_TTL_SECONDS);

  // Use atomic RPC for step state merge
  await supabase.rpc('merge_sync_run_step', {
    p_run_id: runId,
    p_step_name: 'parse_merge',
    p_patch: { ...state, current_step: undefined } // current_step is set separately
  });

  // Also set current_step atomically — preserve sub-statuses (building_stock_index etc.)
  // by passing the actual status as p_extra so it overrides the default 'in_progress'
  await supabase.rpc('set_step_in_progress', {
    p_run_id: runId,
    p_step_name: 'parse_merge',
    p_extra: state.status ? { status: state.status } : {}
  });
  
  // Update metrics if completed
  if (state.status === 'completed') {
    const { data: run } = await supabase.from('sync_runs').select('metrics').eq('id', runId).single();
    const currentMetrics = run?.metrics || {};
    const updatedMetrics = {
      ...currentMetrics,
      products_total: (state.productCount || 0) + Object.values(state.skipped || {}).reduce((a: number, b: unknown) => a + (Number(b) || 0), 0),
      products_processed: state.productCount || 0
    };
    await supabase.from('sync_runs').update({ metrics: updatedMetrics }).eq('id', runId);
  }
  
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

// ========== ARTIFACT ERROR CLASSIFICATION ==========
// Conservative: only classify as "not found" if clear signal; anything else is non-recoverable
function isNotFoundArtifactError(errorStr: string | undefined | null): boolean {
  if (!errorStr) return false;
  const lower = errorStr.toLowerCase();
  return lower.includes('file not found') ||
    lower.includes('not found') ||
    lower.includes('nosuchkey') ||
    lower.includes('enoent') ||
    (lower.includes('404') && (lower.includes('object') || lower.includes('file') || lower.includes('key')));
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
    // Trigger on: no state, pending, OR in_progress without any resume markers
    // (the orchestrator sets in_progress before invoking us, but if Phase 1 never ran
    // there will be no materialBytes/cursor_pos/chunk_index)
    const isFirstExecution = !state || state.status === 'pending' ||
      (state.status === 'in_progress' && !state.materialBytes && !state.cursor_pos && !state.chunk_index);
    if (isFirstExecution) {
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
      // Fall through to Phase 1b inline (re-read state)
      state = await getParseMergeState(supabase, runId);
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
      // Fall through to Phase 1c inline (re-read state)
      state = await getParseMergeState(supabase, runId);
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
      // Fall through to Phase 2 inline (re-read state)
      state = await getParseMergeState(supabase, runId);
    }
    
    // ========== PHASE 2: CHUNKED MATERIAL PROCESSING ==========
    // Supports two modes:
    //   range: fetch MAX_FETCH_BYTES via Range header from material_source.tsv
    //   chunk_files: download one pre-split chunk file per invocation
    if (state.status === 'in_progress') {
      // PREFLIGHT: verify Phase 2 prerequisites
      // (A) resume markers exist OR (B) artifacts are readable
      const hasResumeMarkers = !!(state.materialBytes || state.cursor_pos || state.chunk_index);
      if (!hasResumeMarkers) {
        // No resume markers — verify artifacts are at least readable
        console.log(`[parse_merge] Phase 2 preflight: no resume markers, checking artifacts...`);
        const [stockCheck, priceCheck, metaCheck] = await Promise.all([
          loadStockIndex(supabase),
          loadPriceIndex(supabase),
          loadMaterialMeta(supabase),
        ]);
        const missingArtifacts: string[] = [];
        if (!stockCheck.index) missingArtifacts.push(STOCK_INDEX_FILE);
        if (!priceCheck.index) missingArtifacts.push(PRICE_INDEX_FILE);
        if (!metaCheck.meta) missingArtifacts.push(MATERIAL_META_FILE);

        if (missingArtifacts.length > 0) {
          const error = `pipeline_artifact_missing: Phase 2 cannot start, missing: ${missingArtifacts.join(', ')}`;
          console.error(`[parse_merge] ${error}`);
          try {
            await supabase.rpc('log_sync_event', {
              p_run_id: runId, p_level: 'ERROR', p_message: 'pipeline_artifact_missing',
              p_details: { step: 'parse_merge', missing_artifacts: missingArtifacts }
            });
          } catch (_e) { /* non-blocking */ }
          await updateParseMergeState(supabase, runId, { status: 'failed', error });
          return { success: false, error, status: 'failed' };
        }
        console.log(`[parse_merge] Phase 2 preflight passed: all artifacts readable (no resume markers, first Phase 2 entry)`);
      }
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
      
      // Load indices — with deterministic artifact-missing detection and rebuild logic
      let [stockResult, priceResult, metaResult] = await Promise.all([
        loadStockIndex(supabase),
        loadPriceIndex(supabase),
        loadMaterialMeta(supabase),
      ]);
      let missingPhase2: string[] = [];
      const collectMissing = () => {
        missingPhase2 = [];
        if (!stockResult.index) missingPhase2.push(STOCK_INDEX_FILE);
        if (!priceResult.index) missingPhase2.push(PRICE_INDEX_FILE);
        if (!metaResult.meta) missingPhase2.push(MATERIAL_META_FILE);
      };
      collectMissing();

      if (missingPhase2.length > 0) {
        // Classify errors: only "not found" is recoverable via rebuild
        const errorStrings = [
          !stockResult.index ? stockResult.error : null,
          !priceResult.index ? priceResult.error : null,
          !metaResult.meta ? metaResult.error : null,
        ].filter(Boolean) as string[];

        const allNotFound = errorStrings.length > 0 && errorStrings.every(e => isNotFoundArtifactError(e));
        if (!allNotFound) {
          // Non-recoverable (permissions, corruption, parse error, etc.)
          const error = `pipeline_artifact_error: non-recoverable errors loading Phase 2 artifacts: ${errorStrings.join('; ')}`;
          console.error(`[parse_merge] ${error}`);
          await updateParseMergeState(supabase, runId, { status: 'failed', error });
          return { success: false, error, status: 'failed' };
        }

        // Check rebuild stop rule
        const rebuildAlreadyAttempted = !!(state as Record<string, unknown>).artifact_rebuild_attempted;
        if (rebuildAlreadyAttempted) {
          const error = `pipeline_artifact_missing_after_rebuild: artifacts still missing after rebuild: ${missingPhase2.join(', ')}. Verifica che i file sorgente (stock, price, material) siano presenti e leggibili in ftp-import.`;
          console.error(`[parse_merge] ${error}`);
          try {
            await supabase.rpc('log_sync_event', {
              p_run_id: runId, p_level: 'ERROR', p_message: 'pipeline_artifact_missing_after_rebuild',
              p_details: { step: 'parse_merge', missing_artifacts: missingPhase2 }
            });
          } catch (_e) { /* non-blocking */ }
          await updateParseMergeState(supabase, runId, { status: 'failed', error });
          return { success: false, error, status: 'failed' };
        }

        // Attempt rebuild: log, set flag, reset state to re-run Phase 1, yield
        console.log(`[parse_merge] Artifacts missing with resume markers — attempting rebuild`);
        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'ERROR', p_message: 'pipeline_artifact_missing',
            p_details: { step: 'parse_merge', missing_artifacts: missingPhase2 }
          });
        } catch (_e) { /* non-blocking */ }

        const rebuildStart = Date.now();
        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'INFO', p_message: 'pipeline_artifact_rebuild_started',
            p_details: { step: 'parse_merge', missing_artifacts: missingPhase2, rebuild_attempt: 1 }
          });
        } catch (_e) { /* non-blocking */ }

        // Set rebuild flag and reset to pending so Phase 1 re-runs on next loop iteration
        await supabase.rpc('merge_sync_run_step', {
          p_run_id: runId,
          p_step_name: 'parse_merge',
          p_patch: {
            artifact_rebuild_attempted: true,
            artifact_rebuild_attempted_at: new Date().toISOString(),
            status: 'pending',
            materialBytes: 0,
            cursor_pos: 0,
            chunk_index: 0,
            partial_line: '',
            productCount: 0,
            skipped: { noStock: 0, noPrice: 0, lowStock: 0, noValid: 0 },
          }
        });

        // Verify persistence: re-read state and assert artifact_rebuild_attempted is true
        const verifyState = await getParseMergeState(supabase, runId);
        if (!(verifyState as Record<string, unknown> | null)?.artifact_rebuild_attempted) {
          const verifyError = `pipeline_artifact_rebuild_persist_failed: artifact_rebuild_attempted not persisted after merge. State persistence unreliable.`;
          console.error(`[parse_merge] ${verifyError}`);
          try {
            await supabase.rpc('log_sync_event', {
              p_run_id: runId, p_level: 'ERROR', p_message: 'pipeline_artifact_rebuild_persist_failed',
              p_details: { step: 'parse_merge', missing_artifacts: missingPhase2 }
            });
          } catch (_e) { /* non-blocking */ }
          await updateParseMergeState(supabase, runId, { status: 'failed', error: verifyError });
          return { success: false, error: verifyError, status: 'failed' };
        }

        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'INFO', p_message: 'pipeline_artifact_rebuild_completed',
            p_details: { step: 'parse_merge', duration_ms: Date.now() - rebuildStart, note: 'State reset to pending; Phase 1 will re-run on next invocation' }
          });
        } catch (_e) { /* non-blocking */ }

        // Return in_progress so orchestrator re-invokes (isFirstExecution will trigger Phase 1)
        return { success: true, status: 'in_progress' };
      }
      const stockIndex = stockResult.index!;
      const priceIndex = priceResult.index!;
      const materialMeta = metaResult.meta!;
      
      // ===== GUARDRAIL: detect cursor_pos regression to headerEndPos on resume =====
      // If persisted cursor_pos > 0 but we're about to start from headerEndPos,
      // something upstream wiped the state. Fail fast to avoid infinite re-processing.
      if (cursorPos <= materialMeta.headerEndPos && chunkIndex === 0 && state.productCount > 0) {
        const error = `parse_merge_resume_cursor_mismatch: cursor_pos=${cursorPos} regressed to headerEndPos=${materialMeta.headerEndPos} but productCount=${state.productCount} indicates prior progress. State may have been wiped.`;
        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'ERROR',
            p_message: 'parse_merge_resume_cursor_mismatch',
            p_details: {
              step: 'parse_merge',
              expected_cursor_pos: 'should be > headerEndPos after first chunk',
              actual_cursor_pos: cursorPos,
              header_end_pos: materialMeta.headerEndPos,
              chunk_index: chunkIndex,
              product_count: state.productCount,
              suggestion: 'Possible steps JSON overwrite during resume. Check orchestrator updateRun calls.'
            }
          });
        } catch (_e) { /* non-blocking */ }
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
            
            // CONTENT-RANGE VALIDATION (Codex fix): strict check when we expect 206
            if (httpStatus === 206 && contentRangeHeader) {
              // Parse Content-Range: bytes <start>-<end>/<total>
              const crMatch = contentRangeHeader.match(/bytes\s+(\d+)-(\d+)\/(\d+|\*)/);
              if (crMatch) {
                const receivedStart = parseInt(crMatch[1], 10);
                const receivedEnd = parseInt(crMatch[2], 10);
                const receivedTotal = crMatch[3] !== '*' ? parseInt(crMatch[3], 10) : null;

                // Validate: receivedStart must match requestedStart (cursorPos)
                if (receivedStart !== cursorPos) {
                  const error = `content_range_mismatch: requestedStart=${cursorPos} but receivedStart=${receivedStart}`;
                  try {
                    await supabase.rpc('log_sync_event', {
                      p_run_id: runId, p_level: 'ERROR', p_message: 'content_range_mismatch',
                      p_details: { step: 'parse_merge', requestedStart: cursorPos, receivedStart, receivedEnd, total: receivedTotal, content_range: contentRangeHeader }
                    });
                  } catch (_e) { /* non-blocking */ }
                  await updateParseMergeState(supabase, runId, { status: 'failed', error });
                  return { success: false, error, status: 'failed' };
                }

                // Validate: end >= start
                if (receivedEnd < receivedStart) {
                  const error = `content_range_mismatch: receivedEnd=${receivedEnd} < receivedStart=${receivedStart}`;
                  try {
                    await supabase.rpc('log_sync_event', {
                      p_run_id: runId, p_level: 'ERROR', p_message: 'content_range_mismatch',
                      p_details: { step: 'parse_merge', requestedStart: cursorPos, receivedStart, receivedEnd, total: receivedTotal, content_range: contentRangeHeader }
                    });
                  } catch (_e) { /* non-blocking */ }
                  await updateParseMergeState(supabase, runId, { status: 'failed', error });
                  return { success: false, error, status: 'failed' };
                }

                // Validate: byte count consistency
                const expectedBytes = receivedEnd - receivedStart + 1;
                if (Math.abs(bytesFetched - expectedBytes) > 1) {
                  const error = `content_range_mismatch: expected ${expectedBytes} bytes from Content-Range but got ${bytesFetched}`;
                  try {
                    await supabase.rpc('log_sync_event', {
                      p_run_id: runId, p_level: 'ERROR', p_message: 'content_range_mismatch',
                      p_details: { step: 'parse_merge', requestedStart: cursorPos, receivedStart, receivedEnd, total: receivedTotal, bytesFetched, expectedBytes, content_range: contentRangeHeader }
                    });
                  } catch (_e) { /* non-blocking */ }
                  await updateParseMergeState(supabase, runId, { status: 'failed', error });
                  return { success: false, error, status: 'failed' };
                }

                // DETERMINISTIC CURSOR: use receivedEnd+1 instead of cursorPos + bytesFetched
                // This is set below after rawText decode
              }
            }
            
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
      
      // DETERMINISTIC CURSOR: use Content-Range receivedEnd+1 if available, otherwise fallback to cursorPos + bytesFetched
      let newCursorPos: number;
      if (httpStatus === 206 && contentRangeHeader) {
        const crMatch = contentRangeHeader.match(/bytes\s+(\d+)-(\d+)\//);
        if (crMatch) {
          const receivedEnd = parseInt(crMatch[2], 10);
          newCursorPos = receivedEnd + 1;
        } else {
          newCursorPos = cursorPos + bytesFetched;
        }
      } else {
        newCursorPos = cursorPos + bytesFetched;
      }
      // Monotonicity guard: cursor must never regress
      if (newCursorPos < cursorPos) {
        const error = `cursor_regression: newCursorPos=${newCursorPos} < cursorPos=${cursorPos}`;
        try {
          await supabase.rpc('log_sync_event', {
            p_run_id: runId, p_level: 'ERROR', p_message: 'cursor_regression',
            p_details: { step: 'parse_merge', cursorPos, newCursorPos, bytesFetched, contentRangeHeader }
          });
        } catch (_e) { /* non-blocking */ }
        await updateParseMergeState(supabase, runId, { status: 'failed', error });
        return { success: false, error, status: 'failed' };
      }
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
  // Lock guard: assert ownership and renew lease before writing steps/metrics
  const lockCheck = await assertLockOwned(supabase, runId);
  if (!lockCheck.owned) {
    console.error(`[sync-step-runner] LOCK NOT OWNED in updateStepResult: step=${stepName}, holder=${lockCheck.holder_run_id}`);
    throw new Error(`lock_ownership_lost: cannot update ${stepName} result, lock held by ${lockCheck.holder_run_id}`);
  }
  await renewLockLease(supabase, runId, LOCK_TTL_SECONDS);

  // Atomic step update via RPC (no read-modify-write)
  const stepPatch = { ...result, current_step: undefined };
  const { error: stepErr } = await supabase.rpc('merge_sync_run_step', {
    p_run_id: runId,
    p_step_name: stepName,
    p_patch: stepPatch
  });
  if (stepErr) {
    console.error(`[sync-step-runner] merge_sync_run_step failed: ${stepErr.message}`);
    throw new Error(`rpc_merge_sync_run_step_failed: ${stepErr.message}`);
  }

  // Atomic current_step update via existing RPC (set_step_in_progress updates current_step atomically)
  // We use a direct jsonb_set for current_step only — set_step_in_progress also sets status=in_progress which we don't want here
  const { error: csErr } = await supabase.rpc('merge_sync_run_step', {
    p_run_id: runId,
    p_step_name: 'current_step',
    p_patch: stepName
  });
  // current_step is set as a top-level key in steps via merge_sync_run_step with a string value
  // Actually, merge_sync_run_step expects p_patch as jsonb and uses jsonb_set on steps[p_step_name]
  // So steps.current_step = stepName (as a JSON string) — this works because jsonb_set with a string jsonb value is valid
  if (csErr) {
    console.warn(`[sync-step-runner] current_step update warning: ${csErr.message}`);
  }

  // Atomic metrics update via RPC (no read-modify-write)
  if (result.metrics && Object.keys(result.metrics).length > 0) {
    const { error: metricsErr } = await supabase.rpc('merge_sync_run_metrics', {
      p_run_id: runId,
      p_patch: result.metrics
    });
    if (metricsErr) {
      // Deterministic failure if RPC is missing
      console.error(`[sync-step-runner] merge_sync_run_metrics failed: ${metricsErr.message}`);
      await safeLogEvent(supabase, runId, 'ERROR', 'missing_rpc_merge_sync_run_metrics', {
        code: 'missing_rpc_merge_sync_run_metrics',
        runId,
        stepName,
        error: metricsErr.message
      });
      throw new Error(`missing_rpc_merge_sync_run_metrics: ${metricsErr.message}`);
    }
  }
}

// ========== STEP: PRICING ==========
// ========== PRICING FEE VALIDATION ==========
function validatePricingFeeConfig(feeConfig: FeeConfig | null | undefined): {
  ok: boolean;
  missing_fields: string[];
  invalid_fields: string[];
  safe_summary: Record<string, string>;
} {
  const missing: string[] = [];
  const invalid: string[] = [];
  const safe: Record<string, string> = {};

  if (!feeConfig) {
    return {
      ok: false,
      missing_fields: ['feeDrev', 'feeMkt', 'shippingCost'],
      invalid_fields: [],
      safe_summary: { feeDrev: 'missing', feeMkt: 'missing', shippingCost: 'missing' }
    };
  }

  const requiredFields: Array<{ key: keyof FeeConfig; label: string; allowZero: boolean }> = [
    { key: 'feeDrev', label: 'feeDrev', allowZero: false },
    { key: 'feeMkt', label: 'feeMkt', allowZero: false },
    { key: 'shippingCost', label: 'shippingCost', allowZero: true },
  ];

  for (const { key, label, allowZero } of requiredFields) {
    const val = feeConfig[key];
    if (val === undefined || val === null) {
      missing.push(label);
      safe[label] = 'missing';
    } else if (typeof val !== 'number' || isNaN(val) || !isFinite(val)) {
      invalid.push(label);
      safe[label] = String(val);
    } else if (!allowZero && val <= 0) {
      invalid.push(label);
      safe[label] = String(val);
    } else if (val < 0) {
      invalid.push(label);
      safe[label] = String(val);
    } else {
      safe[label] = String(val);
    }
  }

  return { ok: missing.length === 0 && invalid.length === 0, missing_fields: missing, invalid_fields: invalid, safe_summary: safe };
}

async function stepPricing(supabase: SupabaseClient, runId: string, feeConfig: FeeConfig): Promise<{ success: boolean; error?: string }> {
  console.log(`[sync:step:pricing] Starting for run ${runId}`);
  const startTime = Date.now();
  
  try {
    // ========== FEE CONFIG VALIDATION ==========
    if (!feeConfig) {
      const error = 'Errore calcolo prezzi: configurazione fee non disponibile';
      console.error(`[sync:step:pricing] ${error}`);
      try {
        await supabase.rpc('log_sync_event', {
          p_run_id: runId, p_level: 'ERROR', p_message: 'pricing_config_missing_source',
          p_details: { step: 'pricing', source: 'fee_config (payload from orchestrator)', reason: 'feeConfig is null or undefined' }
        });
      } catch (_e) { /* non-blocking */ }
      await updateStepResult(supabase, runId, 'pricing', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }

    const validation = validatePricingFeeConfig(feeConfig);
    if (!validation.ok) {
      const missingStr = validation.missing_fields.length > 0 ? `Mancano: ${validation.missing_fields.join(', ')}` : '';
      const invalidStr = validation.invalid_fields.length > 0 ? `Non validi: ${validation.invalid_fields.join(', ')}` : '';
      const parts = [missingStr, invalidStr].filter(Boolean).join('. ');
      const error = `Errore calcolo prezzi: configurazione fee non valida. ${parts}.`;
      console.error(`[sync:step:pricing] ${error}`);
      try {
        await supabase.rpc('log_sync_event', {
          p_run_id: runId, p_level: 'ERROR', p_message: 'pricing_config_invalid',
          p_details: {
            step: 'pricing',
            missing_fields: validation.missing_fields,
            invalid_fields: validation.invalid_fields,
            safe_values_summary: validation.safe_summary
          }
        });
      } catch (_e) { /* non-blocking */ }
      await updateStepResult(supabase, runId, 'pricing', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }

    const { products, error: loadError } = await loadProductsTSV(supabase, runId);
    
    if (loadError || !products) {
      const error = loadError || 'Products file not found';
      await updateStepResult(supabase, runId, 'pricing', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    const feeDrev = feeConfig.feeDrev!;
    const feeMkt = feeConfig.feeMkt!;
    const shippingCost = feeConfig.shippingCost!;
    
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

// ========== STEP: EXPORT_EAN (38-column Catalogo EAN) ==========
const EAN_38_HEADERS = [
  'Matnr','ManufPartNr','EAN','ShortDescription','ExistingStock','StockIT','StockEU',
  'CustBestPrice','Surcharge','Costo di Spedizione','IVA','Prezzo con spediz e IVA',
  'FeeDeRev','Fee Marketplace','Subtotale post-fee','Prezzo Finale','ListPrice',
  'ListPrice con Fee','basePriceCents','final_price_ean','listprice_with_fee_ean',
  'final_price_mediaworld','listprice_with_fee_mediaworld','final_price_eprice',
  'listprice_with_fee_eprice','_finalCents_ean','_finalCents_mediaworld',
  '_finalCents_eprice','mediaworldPricingError','epricePricingError','_route',
  '_finalCents','__override','__overrideSource','__overrideStockIT',
  '__overrideStockEU','__overrideLeadDaysIT','__overrideLeadDaysEU'
];

async function stepExportEan(supabase: SupabaseClient, runId: string, feeConfig: FeeConfig): Promise<{ success: boolean; error?: string }> {
  console.log(`[sync:step:export_ean] Starting 38-col for run ${runId}`);
  const startTime = Date.now();

  try {
    // 1. Load products
    const { products, error: loadError } = await loadProductsTSV(supabase, runId);
    if (loadError || !products) {
      const error = loadError || 'Products file not found';
      await updateStepResult(supabase, runId, 'export_ean', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }

    // 2. EAN-specific fees (with global fallback)
    const feeDrev = feeConfig?.eanFeeDrev ?? feeConfig?.feeDrev ?? 1.05;
    const feeMkt = feeConfig?.eanFeeMkt ?? feeConfig?.feeMkt ?? 1.08;
    const shippingCost = feeConfig?.eanShippingCost ?? feeConfig?.shippingCost ?? 6.00;
    const shippingCents = Math.round(shippingCost * 100);
    // MW-specific fees (with global fallback)
    const mwFeeDrev = feeConfig?.mediaworldFeeDrev ?? feeConfig?.feeDrev ?? 1.05;
    const mwFeeMkt = feeConfig?.mediaworldFeeMkt ?? feeConfig?.feeMkt ?? 1.08;
    const mwShippingCost = feeConfig?.mediaworldShippingCost ?? feeConfig?.shippingCost ?? 6.00;
    const mwShippingCents = Math.round(mwShippingCost * 100);
    // ePrice-specific fees (with global fallback)
    const epFeeDrev = feeConfig?.epriceFeeDrev ?? feeConfig?.feeDrev ?? 1.05;
    const epFeeMkt = feeConfig?.epriceFeeMkt ?? feeConfig?.feeMkt ?? 1.08;
    const epShippingCost = feeConfig?.epriceShippingCost ?? feeConfig?.shippingCost ?? 6.00;
    const epShippingCents = Math.round(epShippingCost * 100);
    console.log(`[sync:step:export_ean] Fees: EAN=${feeDrev}/${feeMkt}/${shippingCost} MW=${mwFeeDrev}/${mwFeeMkt}/${mwShippingCost} EP=${epFeeDrev}/${epFeeMkt}/${epShippingCost}`);

    // 3. Load stock-location
    let stockLocIndex: Record<string, { stockIT: number; stockEU: number }> | null = null;
    {
      const { content } = await downloadFromStorage(supabase, 'ftp-import', `stock-location/runs/${runId}.txt`);
      if (content) {
        stockLocIndex = {};
        const lines = content.replace(/\r\n/g, '\n').split('\n');
        const hdrs = lines[0]?.split(';').map((h: string) => h.trim().toLowerCase()) || [];
        const mI = hdrs.indexOf('matnr'), sI = hdrs.indexOf('stock'), lI = hdrs.indexOf('locationid');
        if (mI >= 0 && sI >= 0 && lI >= 0) {
          for (let i = 1; i < lines.length; i++) {
            const v = lines[i].split(';');
            const m = v[mI]?.trim();
            if (!m) continue;
            const s = parseInt(v[sI]) || 0;
            const l = parseInt(v[lI]) || 0;
            if (!stockLocIndex[m]) stockLocIndex[m] = { stockIT: 0, stockEU: 0 };
            if (l === 4242) stockLocIndex[m].stockIT += s;
            else if (l === 4254) stockLocIndex[m].stockEU += s;
          }
        }
        console.log(`[sync:step:export_ean] Stock location: ${Object.keys(stockLocIndex).length} entries`);
      } else {
        await safeLogEvent(supabase, runId, 'WARN', 'stock_location_missing_fallback_used', { step: 'export_ean' });
      }
    }

    // 4. Load override file
    const overrideByEan = new Map<string, Record<string, unknown>>();
    const overrideBySku = new Map<string, Record<string, unknown>>();
    let allOverrideRows: Record<string, unknown>[] = [];
    {
      const { data: files } = await supabase.storage.from('mapping-files').list('', { search: 'override' });
      const ovrFile = files?.find((f: { name: string }) =>
        f.name.toLowerCase().includes('override') && (f.name.toLowerCase().endsWith('.xlsx') || f.name.toLowerCase().endsWith('.xls'))
      );
      if (ovrFile) {
        const { data: ovrData } = await supabase.storage.from('mapping-files').download(ovrFile.name);
        if (ovrData) {
          const XLSX = await import("npm:xlsx@0.18.5");
          const wb = XLSX.read(new Uint8Array(await ovrData.arrayBuffer()), { type: 'array' });
          const wsName = wb.SheetNames[0];
          if (wsName) {
            allOverrideRows = XLSX.utils.sheet_to_json(wb.Sheets[wsName], { defval: '' });
            for (const row of allOverrideRows) {
              const ean = normalizeEAN(row.EAN || row.ean);
              const sku = String(row.SKU || row.sku || row.MPN || row.mpn || '').trim();
              if (ean.ok && ean.value) overrideByEan.set(ean.value, row);
              if (sku) overrideBySku.set(sku.toLowerCase(), row);
            }
          }
        }
      }
      console.log(`[sync:step:export_ean] Override: ${overrideByEan.size} byEAN, ${overrideBySku.size} bySKU`);
    }

    // 5. Build 38-column rows
    const round2 = (v: number) => Math.round(v * 100) / 100;
    const eanRows: string[] = [];
    let eanSkipped = 0;
    const matchedOvrKeys = new Set<string>();

    for (const p of products) {
      if (!p.Matnr?.trim()) { eanSkipped++; continue; }
      const norm = normalizeEAN(p.EAN);
      if (!norm.ok || !norm.value) { eanSkipped++; continue; }
      const ean = norm.value;

      // Stock
      let stockIT: number, stockEU: number;
      if (stockLocIndex && stockLocIndex[p.Matnr]) {
        stockIT = stockLocIndex[p.Matnr].stockIT;
        stockEU = stockLocIndex[p.Matnr].stockEU;
      } else {
        stockIT = p.Stock || 0;
        stockEU = 0;
      }
      let existingStock = stockIT + stockEU;

      // Override detection
      let ovrRow: Record<string, unknown> | undefined;
      if (p.MPN) ovrRow = overrideBySku.get(p.MPN.trim().toLowerCase());
      if (!ovrRow) ovrRow = overrideByEan.get(ean);

      let isOvr = false, ovrSource = '', ovrStockIT = '', ovrStockEU = '';
      let ovrLeadIT = '', ovrLeadEU = '';
      let overrideOfferPrice: number | null = null, overrideListPrice: number | null = null;

      if (ovrRow) {
        isOvr = true;
        ovrSource = 'existing';
        const oSku = String(ovrRow.SKU || ovrRow.sku || ovrRow.MPN || ovrRow.mpn || '').trim();
        if (oSku) matchedOvrKeys.add(oSku.toLowerCase());
        const oEan = normalizeEAN(ovrRow.EAN || ovrRow.ean);
        if (oEan.ok && oEan.value) matchedOvrKeys.add('ean:' + oEan.value);

        const hasSIT = Object.keys(ovrRow).some(k => k.toLowerCase() === 'stockit');
        const hasSEU = Object.keys(ovrRow).some(k => k.toLowerCase() === 'stockeu');
        if (hasSIT && hasSEU) {
          ovrStockIT = String(parseInt(String(ovrRow[Object.keys(ovrRow).find(k => k.toLowerCase() === 'stockit')!])) || 0);
          ovrStockEU = String(parseInt(String(ovrRow[Object.keys(ovrRow).find(k => k.toLowerCase() === 'stockeu')!])) || 0);
        } else {
          const qty = ovrRow.Quantity ?? ovrRow.quantity;
          if (qty !== undefined && qty !== '') {
            const qv = parseInt(String(qty)) || 0;
            existingStock = qv;
            ovrStockIT = String(qv);
            ovrStockEU = '0';
          }
        }
        const ldItK = Object.keys(ovrRow).find(k => k.toLowerCase() === 'leaddaysit');
        const ldEuK = Object.keys(ovrRow).find(k => k.toLowerCase() === 'leaddayseu');
        if (ldItK) ovrLeadIT = String(parseInt(String(ovrRow[ldItK])) || 0);
        if (ldEuK) ovrLeadEU = String(parseInt(String(ovrRow[ldEuK])) || 0);

        const op = ovrRow.OfferPrice ?? ovrRow.offerPrice ?? ovrRow.offer_price;
        if (op !== undefined && op !== '') overrideOfferPrice = parseFloat(String(op)) || null;
        const olp = ovrRow.ListPrice ?? ovrRow.listPrice ?? ovrRow.list_price;
        if (olp !== undefined && olp !== '') overrideListPrice = parseFloat(String(olp)) || null;
      }

      // Pricing
      const cbp = p.CBP || 0, sur = p.Sur || 0, lp = p.LP || 0;
      let basePriceCents: number, route: string;
      if (cbp > 0) { basePriceCents = Math.round((cbp + sur) * 100); route = 'cbp'; }
      else { basePriceCents = Math.round(lp * 100); route = 'lp'; }

      const afterShippingCents = basePriceCents + shippingCents;
      const afterIvaCents = Math.round(afterShippingCents * 1.22);
      const afterFeesCents = Math.round(Math.round(afterIvaCents * feeDrev) * feeMkt);
      const finalCents = toComma99Cents(afterFeesCents);
      const pfStr = (finalCents / 100).toFixed(2).replace('.', ',');

      const ivaVal = round2((basePriceCents / 100 + shippingCost) * 0.22);
      const prezzoConSpedIva = round2((basePriceCents / 100 + shippingCost) * 1.22);
      const subtotalePostFee = round2(afterFeesCents / 100);

      // EAN ListPrice con Fee
      let listPriceConFee: number;
      if (overrideListPrice !== null) {
        listPriceConFee = Math.floor(overrideListPrice);
      } else if (lp > 0) {
        const lpAfterFees = (lp + shippingCost) * 1.22 * feeDrev * feeMkt;
        listPriceConFee = Math.ceil(lpAfterFees);
      } else if (cbp > 0) {
        const fbBase = cbp * 1.25;
        const fbAfterFees = (fbBase + shippingCost) * 1.22 * feeDrev * feeMkt;
        const candidato = Math.ceil(fbAfterFees);
        const minimo = Math.ceil((finalCents / 100) * 1.25);
        listPriceConFee = Math.max(candidato, minimo);
      } else {
        listPriceConFee = Math.ceil((finalCents / 100) * 1.25);
      }

      // MW per-export pricing
      let mwFinalPrice: number;
      if (overrideOfferPrice !== null) {
        mwFinalPrice = overrideOfferPrice;
      } else {
        const mwAfterShip = basePriceCents + mwShippingCents;
        const mwAfterIva = Math.round(mwAfterShip * 1.22);
        const mwAfterFees = Math.round(Math.round(mwAfterIva * mwFeeDrev) * mwFeeMkt);
        mwFinalPrice = toComma99Cents(mwAfterFees) / 100;
      }
      let mwListPriceConFee: number;
      if (overrideListPrice !== null) {
        mwListPriceConFee = Math.floor(overrideListPrice);
      } else if (lp > 0) {
        mwListPriceConFee = Math.ceil((lp + mwShippingCost) * 1.22 * mwFeeDrev * mwFeeMkt);
      } else if (cbp > 0) {
        const fb = cbp * 1.25;
        const cand = Math.ceil((fb + mwShippingCost) * 1.22 * mwFeeDrev * mwFeeMkt);
        const min = Math.ceil(mwFinalPrice * 1.25);
        mwListPriceConFee = Math.max(cand, min);
      } else {
        mwListPriceConFee = Math.ceil(mwFinalPrice * 1.25);
      }

      // ePrice per-export pricing
      let epFinalPrice: number;
      if (overrideOfferPrice !== null) {
        epFinalPrice = overrideOfferPrice;
      } else {
        const epAfterShip = basePriceCents + epShippingCents;
        const epAfterIva = Math.round(epAfterShip * 1.22);
        const epAfterFees = Math.round(Math.round(epAfterIva * epFeeDrev) * epFeeMkt);
        epFinalPrice = toComma99Cents(epAfterFees) / 100;
      }
      let epListPriceConFee: number;
      if (overrideListPrice !== null) {
        epListPriceConFee = Math.floor(overrideListPrice);
      } else if (lp > 0) {
        epListPriceConFee = Math.ceil((lp + epShippingCost) * 1.22 * epFeeDrev * epFeeMkt);
      } else {
        epListPriceConFee = Math.ceil(epFinalPrice * 1.25);
      }

      // Display values
      const dPF = overrideOfferPrice !== null ? overrideOfferPrice.toFixed(2).replace('.', ',') : pfStr;
      const dLP = overrideListPrice !== null ? String(overrideListPrice) : String(lp);
      const dLPF = String(listPriceConFee);

      eanRows.push([
        p.Matnr, p.MPN || '', ean, (p.Desc || '').replace(/\t/g, ' '),
        String(existingStock), String(stockIT), String(stockEU),
        String(cbp), String(sur), String(shippingCost), String(ivaVal), String(prezzoConSpedIva),
        String(feeDrev), String(feeMkt), String(subtotalePostFee), dPF, dLP, dLPF,
        String(basePriceCents),
        overrideOfferPrice !== null ? overrideOfferPrice.toFixed(2).replace('.', ',') : pfStr,
        String(listPriceConFee),
        String(mwFinalPrice), String(mwListPriceConFee),
        String(epFinalPrice), String(epListPriceConFee),
        String(finalCents), String(Math.round(mwFinalPrice * 100)), String(Math.round(epFinalPrice * 100)),
        'false', 'false', route, String(finalCents),
        isOvr ? 'true' : '', isOvr ? ovrSource : '',
        ovrStockIT, ovrStockEU, ovrLeadIT, ovrLeadEU
      ].join('\t'));
    }

    // 6. OVR-only rows
    for (const ovrRow of allOverrideRows) {
      const oSku = String(ovrRow.SKU || ovrRow.sku || ovrRow.MPN || ovrRow.mpn || '').trim();
      const oEan = normalizeEAN(ovrRow.EAN || ovrRow.ean);
      if (oSku && matchedOvrKeys.has(oSku.toLowerCase())) continue;
      if (oEan.ok && oEan.value && matchedOvrKeys.has('ean:' + oEan.value)) continue;
      if (!oEan.ok || !oEan.value || !oSku) continue;

      const matnr = oSku.startsWith('OVR-') ? oSku : 'OVR-' + oSku;
      const mpn = oSku.startsWith('OVR-') ? oSku.substring(4) : oSku;
      const hasSIT = Object.keys(ovrRow).some(k => k.toLowerCase() === 'stockit');
      const hasSEU = Object.keys(ovrRow).some(k => k.toLowerCase() === 'stockeu');
      let oES = 0, oSI = 0, oSE = 0, oOSI = '0', oOSE = '0';
      if (hasSIT && hasSEU) {
        oOSI = String(parseInt(String(ovrRow[Object.keys(ovrRow).find(k => k.toLowerCase() === 'stockit')!])) || 0);
        oOSE = String(parseInt(String(ovrRow[Object.keys(ovrRow).find(k => k.toLowerCase() === 'stockeu')!])) || 0);
        oES = parseInt(oOSI) + parseInt(oOSE); oSI = oES;
      } else {
        const qty = ovrRow.Quantity ?? ovrRow.quantity;
        if (qty !== undefined && qty !== '') {
          const qv = Math.floor(parseFloat(String(qty)) || 0);
          oOSI = String(qv); oES = qv; oSI = qv;
        }
      }
      const op = parseFloat(String(ovrRow.OfferPrice ?? ovrRow.offerPrice ?? ovrRow.offer_price ?? 0)) || 0;
      const olp = parseFloat(String(ovrRow.ListPrice ?? ovrRow.listPrice ?? ovrRow.list_price ?? 0)) || 0;
      const opStr = op.toFixed(2).replace('.', ',');

      eanRows.push([
        matnr, mpn, oEan.value, `Override item ${oSku}`,
        String(oES), String(oSI), '0',
        '0', '0', '0', '0', '0',
        '0', '0', '0', opStr, String(olp), String(Math.floor(olp) || 0),
        '', opStr, '',
        String(op), String(Math.floor(olp) || 0), String(op), String(Math.floor(olp) || 0),
        '', '', '',
        'false', 'false', '', '',
        'true', 'existing',
        oOSI, oOSE, '', ''
      ].join('\t'));
    }

    // 7. Save TSV (tab-separated, UTF-8, LF line endings)
    const eanTSV = [EAN_38_HEADERS.join('\t'), ...eanRows].join('\n');
    await uploadToStorage(supabase, 'exports', EAN_CATALOG_FILE_PATH, eanTSV, 'text/tab-separated-values');
    // Also save as CSV for backward compatibility with export_ean_xlsx
    const eanCSV = [EAN_38_HEADERS.join(';'), ...eanRows.map(r => r.replace(/\t/g, ';'))].join('\n');
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
    console.log(`[sync:step:export_ean] Completed: ${eanRows.length} rows (38 cols), ${eanSkipped} skipped`);
    return { success: true };
  } catch (e: unknown) {
    console.error(`[sync:step:export_ean] Error:`, e);
    await updateStepResult(supabase, runId, 'export_ean', { status: 'failed', error: errMsg(e), metrics: {} });
    return { success: false, error: errMsg(e) };
  }
}

// ========== STEP: EXPORT_MEDIAWORLD (template-based, TSV data, no cap) ==========
async function stepExportMediaworld(supabase: SupabaseClient, runId: string, feeConfig: FeeConfig): Promise<{ success: boolean; error?: string }> {
  // includeEU: null/undefined → true (backward compat, matches client)
  const includeEu = feeConfig?.mediaworldIncludeEu == null ? true : !!feeConfig.mediaworldIncludeEu;
  const itDays = feeConfig?.mediaworldItPrepDays || 3;
  const euDays = feeConfig?.mediaworldEuPrepDays || 5;
  
  console.log(`[sync:step:export_mediaworld] Starting for run ${runId}, IT=${itDays}, EU=${euDays}, includeEU=${includeEu}`);
  const startTime = Date.now();
  
  try {
    // --- Load template via loadTemplateFromStorage (checksum-pinned) ---
    let templateBytes: Uint8Array;
    try {
      templateBytes = await loadTemplateFromStorage(supabase, 'Export Mediaworld.xlsx', runId);
    } catch (e: unknown) {
      const error = `mediaworld_template_load_failed: ${errMsg(e)}`;
      await safeLogEvent(supabase, runId, 'ERROR', 'mediaworld_template_load_failed', { templateName: 'Export Mediaworld.xlsx', reason: errMsg(e) });
      await updateStepResult(supabase, runId, 'export_mediaworld', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }

    const XLSX = await import("npm:xlsx@0.18.5");
    // deno-lint-ignore no-explicit-any
    let wb: any = XLSX.read(templateBytes, { type: 'array', cellStyles: true });
    templateBytes = null as unknown as Uint8Array; // free memory

    // --- Validate 3 sheets ---
    const requiredSheets = ['Data', 'ReferenceData', 'Columns'];
    const missingSheets = requiredSheets.filter(s => !wb.SheetNames.includes(s));
    if (missingSheets.length > 0) {
      const error = `mediaworld_template_sheet_missing: missing ${missingSheets.join(', ')}`;
      await safeLogEvent(supabase, runId, 'ERROR', 'mediaworld_template_sheet_missing', { sheetNames: wb.SheetNames, missing: missingSheets });
      await updateStepResult(supabase, runId, 'export_mediaworld', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }

    const ws = wb.Sheets['Data'];

    // --- Snapshot A1:V2 (22 cols × 2 rows) ---
    const COL_LETTERS = ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V'];
    const headerSnapshot: (string | number | boolean | undefined)[][] = [];
    for (let r = 1; r <= 2; r++) {
      const rowVals: (string | number | boolean | undefined)[] = [];
      for (const c of COL_LETTERS) {
        const cell = ws[`${c}${r}`];
        rowVals.push(cell?.v);
      }
      headerSnapshot.push(rowVals);
    }

    // --- Deterministic cleanup: clear data rows (r >= 3, cols A-V) ---
    let lastRowToClear = 2;
    const wsKeys = Object.keys(ws).filter(k => !k.startsWith('!'));
    for (const key of wsKeys) {
      const colMatch = key.match(/^([A-V])(\d+)$/);
      if (!colMatch) continue;
      const rowNum = parseInt(colMatch[2], 10);
      if (rowNum < 3) continue;
      const cell = ws[key];
      if (cell && cell.v !== undefined && cell.v !== null && cell.v !== '') {
        if (rowNum > lastRowToClear) lastRowToClear = rowNum;
      }
    }
    if (lastRowToClear >= 3) {
      for (let r = 3; r <= lastRowToClear; r++) {
        for (const c of COL_LETTERS) {
          const addr = `${c}${r}`;
          if (ws[addr]) {
            ws[addr].v = undefined;
            delete ws[addr].w;
            ws[addr].t = 'z';
          }
        }
      }
    }

    // --- Load TSV catalog (source of truth) ---
    const { content: tsvContent, error: tsvError } = await downloadFromStorage(supabase, 'exports', EAN_CATALOG_FILE_PATH);
    if (tsvError || !tsvContent) {
      const error = `ean_catalog_missing_or_schema_mismatch: ${tsvError || 'file not found'}`;
      await safeLogEvent(supabase, runId, 'ERROR', 'ean_catalog_missing_or_schema_mismatch', { step: 'export_mediaworld', error });
      await updateStepResult(supabase, runId, 'export_mediaworld', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }

    const tsvLines = tsvContent.split('\n');
    if (tsvLines.length < 2) {
      const error = 'ean_catalog_empty: no data rows in TSV';
      await updateStepResult(supabase, runId, 'export_mediaworld', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }

    const tsvHeaders = tsvLines[0].split('\t').map((h: string) => h.trim());
    const requiredCols = ['ManufPartNr', 'EAN', 'ShortDescription', 'StockIT', 'StockEU', 'final_price_mediaworld', 'listprice_with_fee_mediaworld'];
    const colIdx: Record<string, number> = {};
    for (let i = 0; i < tsvHeaders.length; i++) colIdx[tsvHeaders[i]] = i;
    const missingCols = requiredCols.filter(c => colIdx[c] === undefined);
    if (missingCols.length > 0) {
      const error = `ean_catalog_missing_or_schema_mismatch: missing: ${missingCols.join(', ')}. Found: ${tsvHeaders.join(', ')}`;
      await safeLogEvent(supabase, runId, 'ERROR', 'ean_catalog_missing_or_schema_mismatch', { step: 'export_mediaworld', missing: missingCols, found: tsvHeaders });
      await updateStepResult(supabase, runId, 'export_mediaworld', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }

    // --- Write data rows starting at row 3 ---
    let mwWritten = 0;
    let mwSkipped = 0;
    const EAN_RE = /^[0-9]{13,14}$/;

    for (let i = 1; i < tsvLines.length; i++) {
      const line = tsvLines[i];
      if (!line.trim()) continue;
      const vals = line.split('\t');

      const mpn = vals[colIdx['ManufPartNr']] || '';
      const ean = vals[colIdx['EAN']] || '';
      const desc = vals[colIdx['ShortDescription']] || '';
      const stockIT = parseFloat(vals[colIdx['StockIT']]) || 0;
      const stockEU = parseFloat(vals[colIdx['StockEU']]) || 0;
      const finalPriceMW = parseFloat(vals[colIdx['final_price_mediaworld']]) || 0;
      const lpFeeMW = parseFloat(vals[colIdx['listprice_with_fee_mediaworld']]) || 0;

      // Skip rules (same as client)
      if (!EAN_RE.test(ean)) { mwSkipped++; continue; }
      if (!Number.isFinite(lpFeeMW) || lpFeeMW <= 0) { mwSkipped++; continue; }
      if (!Number.isFinite(finalPriceMW) || finalPriceMW <= 0) { mwSkipped++; continue; }

      const stockResult = resolveMarketplaceStock(stockIT, stockEU, includeEu, itDays, euDays);
      if (!stockResult.shouldExport) { mwSkipped++; continue; }

      const r = 3 + mwWritten; // Excel row (1-based)

      // A: sku (string)
      ws[`A${r}`] = { v: mpn, t: 's' };
      // B: EAN (text, preserve leading zeros)
      ws[`B${r}`] = { v: String(ean), t: 's', z: '@' };
      // C: product-id-type
      ws[`C${r}`] = { v: 'EAN', t: 's' };
      // D: description
      ws[`D${r}`] = { v: desc, t: 's' };
      // F: price (numeric, format '0')
      ws[`F${r}`] = { v: lpFeeMW, t: 'n', z: '0' };
      // H: quantity (numeric, NO CAP)
      ws[`H${r}`] = { v: stockResult.exportQty, t: 'n' };
      // J: state
      ws[`J${r}`] = { v: 'Nuovo', t: 's' };
      // M: logistic-class
      ws[`M${r}`] = { v: 'Consegna gratuita', t: 's' };
      // N: discount-price (text with comma and 2 decimals)
      ws[`N${r}`] = { v: finalPriceMW.toFixed(2).replace('.', ','), t: 's', z: '@' };
      // Q: leadtime-to-ship (numeric integer)
      ws[`Q${r}`] = { v: stockResult.leadDays, t: 'n', z: '#,##0' };
      // S: strike-price-type
      ws[`S${r}`] = { v: 'recommended-retail-price', t: 's' };

      mwWritten++;
    }

    // --- Update !ref ---
    const lastDataRow = Math.max(2, 2 + mwWritten);
    ws['!ref'] = `A1:V${lastDataRow}`;

    // --- Verify A1:V2 unchanged ---
    for (let r = 1; r <= 2; r++) {
      for (let ci = 0; ci < COL_LETTERS.length; ci++) {
        const addr = `${COL_LETTERS[ci]}${r}`;
        const currentVal = ws[addr]?.v;
        const snapshotVal = headerSnapshot[r - 1][ci];
        if (currentVal !== snapshotVal) {
          const error = `mediaworld_headers_modified: cell ${addr} changed from "${snapshotVal}" to "${currentVal}"`;
          await safeLogEvent(supabase, runId, 'ERROR', 'mediaworld_headers_modified', { cell: addr, expected: snapshotVal, actual: currentVal });
          await updateStepResult(supabase, runId, 'export_mediaworld', { status: 'failed', error, metrics: {} });
          return { success: false, error };
        }
      }
    }

    // --- Validate sheets still present ---
    const postMissingSheets = requiredSheets.filter(s => !wb.SheetNames.includes(s));
    if (postMissingSheets.length > 0) {
      const error = `mediaworld_template_sheet_missing: post-write missing ${postMissingSheets.join(', ')}`;
      await updateStepResult(supabase, runId, 'export_mediaworld', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }

    // --- Validate cell formats on first data row ---
    if (mwWritten > 0) {
      const b3 = ws['B3'];
      const f3 = ws['F3'];
      const n3 = ws['N3'];
      const formatErrors: string[] = [];
      if (!b3 || b3.t !== 's' || b3.z !== '@') formatErrors.push(`B3: expected t='s' z='@', got t='${b3?.t}' z='${b3?.z}'`);
      if (f3 && f3.v !== undefined) {
        if (f3.t !== 'n' || f3.z !== '0') formatErrors.push(`F3: expected t='n' z='0', got t='${f3?.t}' z='${f3?.z}'`);
      }
      if (!n3 || n3.t !== 's' || n3.z !== '@') formatErrors.push(`N3: expected t='s' z='@', got t='${n3?.t}' z='${n3?.z}'`);
      if (n3 && typeof n3.v === 'string' && !/,\d{2}$/.test(n3.v)) formatErrors.push(`N3: expected comma+2decimals, got '${n3.v}'`);
      if (formatErrors.length > 0) {
        const error = `mediaworld_cell_format_invalid: ${formatErrors.join('; ')}`;
        await safeLogEvent(supabase, runId, 'ERROR', 'mediaworld_cell_format_invalid', { errors: formatErrors });
        await updateStepResult(supabase, runId, 'export_mediaworld', { status: 'failed', error, metrics: {} });
        return { success: false, error };
      }
    }

    // --- Serialize and upload ---
    // deno-lint-ignore no-explicit-any
    let xlsxBuffer: any = XLSX.write(wb, { bookType: 'xlsx', type: 'buffer', cellStyles: true });
    let mwOutputBytes: Uint8Array;
    if (xlsxBuffer instanceof Uint8Array) { mwOutputBytes = xlsxBuffer; }
    else if (xlsxBuffer instanceof ArrayBuffer) { mwOutputBytes = new Uint8Array(xlsxBuffer); }
    else if (Array.isArray(xlsxBuffer)) { mwOutputBytes = new Uint8Array(xlsxBuffer); }
    else {
      await updateStepResult(supabase, runId, 'export_mediaworld', { status: 'failed', error: 'mw_write_buffer_type_unexpected', metrics: {} });
      return { success: false, error: 'mw_write_buffer_type_unexpected' };
    }
    xlsxBuffer = null;
    wb = null;

    if (mwWritten > 0 && mwOutputBytes.length < 20_000) {
      const error = `Post-write sanity: output ${mwOutputBytes.length} bytes < 20KB with ${mwWritten} rows`;
      await updateStepResult(supabase, runId, 'export_mediaworld', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }

    let mwBlob: Blob | null = new Blob([mwOutputBytes], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
    mwOutputBytes = null as unknown as Uint8Array;

    const { error: uploadError } = await supabase.storage.from('exports').upload(
      'Export Mediaworld.xlsx', mwBlob, { upsert: true, contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' }
    );
    mwBlob = null;

    if (uploadError) {
      const error = `Upload Export Mediaworld.xlsx fallito: ${uploadError.message}`;
      await updateStepResult(supabase, runId, 'export_mediaworld', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }

    await safeLogEvent(supabase, runId, 'INFO', 'export_mediaworld_completed', { file: 'Export Mediaworld.xlsx', rows_written: mwWritten, rows_skipped: mwSkipped });
    await updateStepResult(supabase, runId, 'export_mediaworld', {
      status: 'success', duration_ms: Date.now() - startTime, rows: mwWritten, skipped: mwSkipped,
      metrics: { mediaworld_export_rows: mwWritten, mediaworld_export_skipped: mwSkipped, mediaworld_format: 'xlsx_template' },
      validation_passed: true
    } as StepResultData);

    console.log(`[sync:step:export_mediaworld] Completed: ${mwWritten} rows, ${mwSkipped} skipped (template-based, 3 sheets, no cap)`);
    return { success: true };

  } catch (e: unknown) {
    console.error(`[sync:step:export_mediaworld] Error:`, e);
    await updateStepResult(supabase, runId, 'export_mediaworld', { status: 'failed', error: errMsg(e), metrics: {} });
    return { success: false, error: errMsg(e) };
  }
}

// ========== STEP: EXPORT_EPRICE (TSV-based, no cap) ==========
async function stepExportEprice(supabase: SupabaseClient, runId: string, feeConfig: FeeConfig): Promise<{ success: boolean; error?: string }> {
  // includeEU: null/undefined → true (backward compat, matches client)
  const includeEu = feeConfig?.epriceIncludeEu == null ? true : !!feeConfig.epriceIncludeEu;
  const itDays = feeConfig?.epriceItPrepDays || feeConfig?.epricePrepDays || 1;
  const euDays = feeConfig?.epriceEuPrepDays || 3;
  
  console.log(`[sync:step:export_eprice] Starting for run ${runId}, IT=${itDays}, EU=${euDays}, includeEU=${includeEu}`);
  const startTime = Date.now();
  
  try {
    // Load from TSV catalog (source of truth)
    const { content: tsvContent, error: tsvError } = await downloadFromStorage(supabase, 'exports', EAN_CATALOG_FILE_PATH);
    if (tsvError || !tsvContent) {
      const error = `ean_catalog_missing_or_schema_mismatch: ${tsvError || 'file not found'}`;
      await safeLogEvent(supabase, runId, 'ERROR', 'ean_catalog_missing_or_schema_mismatch', { step: 'export_eprice', error });
      await updateStepResult(supabase, runId, 'export_eprice', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }

    const tsvLines = tsvContent.split('\n');
    if (tsvLines.length < 2) {
      const error = 'ean_catalog_empty: no data rows';
      await updateStepResult(supabase, runId, 'export_eprice', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }

    const tsvHeaders = tsvLines[0].split('\t').map((h: string) => h.trim());
    const requiredCols = ['ManufPartNr', 'Matnr', 'EAN', 'StockIT', 'StockEU', 'final_price_eprice'];
    const colIdx: Record<string, number> = {};
    for (let i = 0; i < tsvHeaders.length; i++) colIdx[tsvHeaders[i]] = i;
    const missingCols = requiredCols.filter(c => colIdx[c] === undefined);
    if (missingCols.length > 0) {
      const error = `ean_catalog_missing_or_schema_mismatch: missing: ${missingCols.join(', ')}. Found: ${tsvHeaders.join(', ')}`;
      await safeLogEvent(supabase, runId, 'ERROR', 'ean_catalog_missing_or_schema_mismatch', { step: 'export_eprice', missing: missingCols, found: tsvHeaders });
      await updateStepResult(supabase, runId, 'export_eprice', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }

    // Load ePrice template
    const XLSX = await import("npm:xlsx@0.18.5");
    let epTemplateBytes: Uint8Array;
    try {
      epTemplateBytes = await loadTemplateFromStorage(supabase, 'Export ePrice.xlsx', runId);
    } catch (e: unknown) {
      const error = `Template Export ePrice.xlsx non trovato: ${errMsg(e)}`;
      await updateStepResult(supabase, runId, 'export_eprice', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }

    const wb = XLSX.read(epTemplateBytes, { type: 'array' });
    const templateSheetNames = [...wb.SheetNames];
    const ws = wb.Sheets['Tracciato_Inserimento_Offerte'];
    if (!ws) {
      const error = 'Template: foglio Tracciato_Inserimento_Offerte non trovato';
      await updateStepResult(supabase, runId, 'export_eprice', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }

    // Capture header row 0
    const header0Values: string[] = [];
    for (let c = 0; c <= 7; c++) {
      const cell = ws[XLSX.utils.encode_cell({ r: 0, c })];
      header0Values.push(cell?.v?.toString() || '');
    }

    // Clear data rows (keep header at row 0)
    const epTmplRange = ws['!ref'] ? XLSX.utils.decode_range(ws['!ref']) : null;
    if (epTmplRange) {
      for (let R = 1; R <= epTmplRange.e.r; R++) {
        for (let C = 0; C <= epTmplRange.e.c; C++) {
          delete ws[XLSX.utils.encode_cell({ r: R, c: C })];
        }
      }
    }

    const EP_DATA_START = 1;
    let epWritten = 0;
    let epSkipped = 0;
    const EAN_RE = /^[0-9]{13,14}$/;

    for (let i = 1; i < tsvLines.length; i++) {
      const line = tsvLines[i];
      if (!line.trim()) continue;
      const vals = line.split('\t');

      const mpn = vals[colIdx['ManufPartNr']] || '';
      const matnr = vals[colIdx['Matnr']] || '';
      const ean = vals[colIdx['EAN']] || '';
      const stockIT = parseFloat(vals[colIdx['StockIT']]) || 0;
      const stockEU = parseFloat(vals[colIdx['StockEU']]) || 0;
      const finalPriceEP = parseFloat(vals[colIdx['final_price_eprice']]) || 0;

      if (!EAN_RE.test(ean)) { epSkipped++; continue; }
      if (!finalPriceEP || finalPriceEP <= 0) { epSkipped++; continue; }

      // ePrice: IT-first with fixed IT fulfillment-latency = 1
      let exportQty: number, fulfillmentLatency: number, shouldExport: boolean;
      if (stockIT >= 2) {
        exportQty = stockIT; fulfillmentLatency = 1; shouldExport = true;
      } else if (includeEu && (stockIT + stockEU) >= 2) {
        exportQty = stockIT + stockEU; fulfillmentLatency = euDays; shouldExport = true;
      } else {
        shouldExport = false; exportQty = 0; fulfillmentLatency = 0;
      }

      if (!shouldExport) { epSkipped++; continue; }

      const r = EP_DATA_START + epWritten;
      ws[XLSX.utils.encode_cell({ r, c: 0 })] = { v: mpn || matnr || '', t: 's' };
      ws[XLSX.utils.encode_cell({ r, c: 1 })] = { v: ean, t: 's', z: '@' }; // EAN as text
      ws[XLSX.utils.encode_cell({ r, c: 2 })] = { v: 'EAN', t: 's' };
      ws[XLSX.utils.encode_cell({ r, c: 3 })] = { v: Math.round(finalPriceEP * 100) / 100, t: 'n', z: '0.00' };
      ws[XLSX.utils.encode_cell({ r, c: 4 })] = { v: exportQty, t: 'n' }; // NO CAP
      ws[XLSX.utils.encode_cell({ r, c: 5 })] = { v: 11, t: 'n' };
      ws[XLSX.utils.encode_cell({ r, c: 6 })] = { v: fulfillmentLatency, t: 'n' };
      ws[XLSX.utils.encode_cell({ r, c: 7 })] = { v: 'K', t: 's' };
      epWritten++;
    }

    // Update range
    if (epWritten > 0) {
      ws['!ref'] = XLSX.utils.encode_range({ s: { r: 0, c: 0 }, e: { r: EP_DATA_START + epWritten - 1, c: 7 } });
    }

    // Light validation
    const epValidationErrors: string[] = [];
    if (JSON.stringify(wb.SheetNames) !== JSON.stringify(templateSheetNames)) {
      epValidationErrors.push(`sheet_names: expected ${JSON.stringify(templateSheetNames)}, got ${JSON.stringify(wb.SheetNames)}`);
    }
    for (let c = 0; c <= 7; c++) {
      const cell = ws[XLSX.utils.encode_cell({ r: 0, c })];
      const currentVal = cell?.v?.toString() || '';
      if (currentVal !== header0Values[c]) {
        epValidationErrors.push(`header[${c}]: expected "${header0Values[c]}", got "${currentVal}"`);
      }
    }
    if (epWritten > 0) {
      const eanCheckLimit = Math.min(epWritten, 100);
      for (let j = 0; j < eanCheckLimit; j++) {
        const cell = ws[XLSX.utils.encode_cell({ r: EP_DATA_START + j, c: 1 })];
        if (cell && cell.t !== 's') epValidationErrors.push(`ean_type_row${EP_DATA_START + j}: '${cell.t}'`);
      }
    }
    if (epValidationErrors.length > 0) {
      const error = `Validation failed: ${epValidationErrors.join('; ')}`;
      await safeLogEvent(supabase, runId, 'ERROR', 'validation_failed', { export_name: 'Export ePrice', errors: epValidationErrors });
      await updateStepResult(supabase, runId, 'export_eprice', { status: 'failed', error, metrics: {}, validation_passed: false } as StepResultData);
      return { success: false, error };
    }

    // Serialize and upload
    const xlsxBuffer = XLSX.write(wb, { bookType: 'xlsx', type: 'array', compression: false, bookSST: false });
    const epOutputBytes = new Uint8Array(xlsxBuffer);
    const epOutSize = epOutputBytes.length;

    if (epWritten > 0 && epOutSize <= 20000) {
      const error = `size_too_small: ${epOutSize} bytes with ${epWritten} rows`;
      await updateStepResult(supabase, runId, 'export_eprice', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }

    const epBlob = new Blob([epOutputBytes], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
    const { error: uploadError } = await supabase.storage.from('exports').upload(
      'Export ePrice.xlsx', epBlob, { upsert: true, contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' }
    );

    if (uploadError) {
      const error = `Upload Export ePrice.xlsx fallito: ${uploadError.message}`;
      await updateStepResult(supabase, runId, 'export_eprice', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }

    await safeLogEvent(supabase, runId, 'INFO', 'export_saved', { step: 'export_eprice', file: 'Export ePrice.xlsx', rows: epWritten });
    await updateStepResult(supabase, runId, 'export_eprice', {
      status: 'success', duration_ms: Date.now() - startTime, rows: epWritten, skipped: epSkipped,
      metrics: { eprice_export_rows: epWritten, eprice_export_skipped: epSkipped, format: 'xlsx' },
      validation_passed: true
    } as StepResultData);

    console.log(`[sync:step:export_eprice] Completed: ${epWritten} rows, ${epSkipped} skipped (TSV-based, no cap)`);
    return { success: true };

  } catch (e: unknown) {
    console.error(`[sync:step:export_eprice] Error:`, e);
    await updateStepResult(supabase, runId, 'export_eprice', { status: 'failed', error: errMsg(e), metrics: {} });
    return { success: false, error: errMsg(e) };
  }
}

// ========== EAN CELL TYPE HELPER ==========
const EAN_TEXT_FORMAT_COLS = new Set(['EAN']);
const EAN_ALWAYS_STRING_COLS = new Set(['Prezzo Finale', 'final_price_ean']);

function makeEanCell(colName: string, val: string): Record<string, unknown> | null {
  if (val === '' || val === undefined || val === null) return null;
  if (EAN_TEXT_FORMAT_COLS.has(colName)) return { v: val, t: 's', z: '@' };
  if (EAN_ALWAYS_STRING_COLS.has(colName)) return { v: val, t: 's' };
  // Try number: no commas, no % signs, not boolean text
  if (/^-?\d+(\.\d+)?$/.test(val)) return { v: Number(val), t: 'n' };
  return { v: val, t: 's' };
}

// ========== LIGHTWEIGHT EAN VALIDATION (no ZIP unzip, no double parse) ==========
function validateEanLightweight(
  // deno-lint-ignore no-explicit-any
  XLSX: any,
  // deno-lint-ignore no-explicit-any
  wb: any,
  // deno-lint-ignore no-explicit-any
  templateWs: any,
  dataSheetName: string,
  rowsWritten: number,
  headerCount: number,
  eanColIdx: number,
): { passed: boolean; errors: string[] } {
  const errors: string[] = [];

  // 1. Sheet must exist
  if (!wb.SheetNames.includes(dataSheetName)) {
    errors.push(`sheet_missing: ${dataSheetName}`);
    return { passed: false, errors };
  }

  const ws = wb.Sheets[dataSheetName];
  if (!ws) { errors.push(`sheet_null: ${dataSheetName}`); return { passed: false, errors }; }

  // 2. Header row identical to template (covers 38-header check)
  const tmplRange = templateWs['!ref'] ? XLSX.utils.decode_range(templateWs['!ref']) : null;
  if (tmplRange) {
    for (let c = 0; c <= tmplRange.e.c; c++) {
      const addr = XLSX.utils.encode_cell({ r: 0, c });
      const tv = templateWs[addr]?.v?.toString() || '';
      const gv = ws[addr]?.v?.toString() || '';
      if (tv !== gv) errors.push(`header[${c}]: expected "${tv}", got "${gv}"`);
    }
  }

  // 3. AutoFilter preserved
  const tf = JSON.stringify(templateWs['!autofilter'] || null);
  const gf = JSON.stringify(ws['!autofilter'] || null);
  if (tf !== gf) errors.push(`autofilter: expected ${tf}, got ${gf}`);

  // 4. Column widths preserved
  const tmplCols = templateWs['!cols'];
  const genCols = ws['!cols'];
  if (tmplCols && !genCols) {
    errors.push('column_widths: template has !cols but output does not');
  } else if (tmplCols && genCols) {
    for (let i = 0; i < tmplCols.length; i++) {
      const tw = tmplCols[i]?.wch ?? tmplCols[i]?.wpx ?? tmplCols[i]?.width;
      const gw = genCols[i]?.wch ?? genCols[i]?.wpx ?? genCols[i]?.width;
      if (tw != null && gw != null && tw !== gw) errors.push(`col_width[${i}]: expected ${tw}, got ${gw}`);
    }
  }

  // 5. EAN column: digits only, length 13/14, text type (sample 200 rows)
  if (eanColIdx >= 0) {
    const checkLimit = Math.min(rowsWritten, 200);
    let eanViolations = 0;
    const eanExamples: string[] = [];
    for (let r = 1; r <= checkLimit; r++) {
      const cell = ws[XLSX.utils.encode_cell({ r, c: eanColIdx })];
      if (!cell) continue;
      if (cell.t !== 's') { eanViolations++; if (eanExamples.length < 3) eanExamples.push(`row${r}:type=${cell.t}`); continue; }
      const val = String(cell.v || '');
      if (val && !/^\d{13,14}$/.test(val)) { eanViolations++; if (eanExamples.length < 3) eanExamples.push(`row${r}:${val}`); }
    }
    if (eanViolations > 0 && (eanViolations / checkLimit) > 0.01) {
      errors.push(`ean_invalid_values_detected: ${eanViolations}/${checkLimit} violations, examples: ${eanExamples.join(', ')}`);
    }
  }

  // 6. Column count check
  const genRange = ws['!ref'] ? XLSX.utils.decode_range(ws['!ref']) : null;
  if (genRange && tmplRange && genRange.e.c < tmplRange.e.c) {
    errors.push(`column_count: expected >= ${tmplRange.e.c + 1}, got ${genRange.e.c + 1}`);
  }

  // 7. finalCents % 100 == 99 check (find _finalCents column, sample standard rows)
  if (tmplRange) {
    let fcColIdx = -1;
    let matnrColIdx = -1;
    for (let c = 0; c <= tmplRange.e.c; c++) {
      const hdr = ws[XLSX.utils.encode_cell({ r: 0, c })]?.v?.toString() || '';
      if (hdr === '_finalCents') fcColIdx = c;
      if (hdr === 'Matnr') matnrColIdx = c;
    }
    if (fcColIdx >= 0) {
      const fcLimit = Math.min(rowsWritten, 200);
      const fcExamples: string[] = [];
      for (let r = 1; r <= fcLimit; r++) {
        // Skip OVR rows
        if (matnrColIdx >= 0) {
          const mCell = ws[XLSX.utils.encode_cell({ r, c: matnrColIdx })];
          if (mCell && String(mCell.v || '').startsWith('OVR-')) continue;
        }
        const fcCell = ws[XLSX.utils.encode_cell({ r, c: fcColIdx })];
        if (!fcCell || fcCell.v === '' || fcCell.v === null || fcCell.v === undefined) continue;
        const fcVal = Number(fcCell.v);
        if (!isNaN(fcVal) && fcVal > 0 && fcVal % 100 !== 99) {
          if (fcExamples.length < 3) fcExamples.push(`row${r}:${fcVal}`);
        }
      }
      if (fcExamples.length > 0) {
        errors.push(`ean_final_price_not_comma99: ${fcExamples.join(', ')}`);
      }
    }
  }

  return { passed: errors.length === 0, errors };
}

// ========== STEP: EXPORT_EAN_XLSX (STREAMING - low memory) ==========
// Reads CSV line-by-line from storage using signed URL + Range requests
// to avoid loading entire file into memory. Builds XLSX incrementally.
// NOTE: Uses lightweight validation (no ZIP unzip) to stay within CPU budget.
const XLSX_STREAM_CHUNK_BYTES = 512 * 1024; // 512KB per range fetch

async function stepExportEanXlsx(supabase: SupabaseClient, runId: string): Promise<{ success: boolean; error?: string }> {
  console.log(`[sync:step:export_ean_xlsx] Starting for run ${runId} (streaming mode)`);
  const startTime = Date.now();
  let lastStage = 'init';
  const heap = () => getMemMB() ?? 0;
  const logEanStage = async (stage: string, stageStart: number, extra: Record<string, unknown> = {}): Promise<void> => {
    lastStage = stage;
    await safeLogEvent(supabase, runId, 'INFO', 'export_ean_xlsx_stage', {
      step: 'export_ean_xlsx', stage, duration_ms: Date.now() - stageStart,
      size_bytes: extra.size_bytes ?? 0, rows: extra.rows ?? 0, heap_mb: heap(),
      integrity_mode: extra.integrity_mode ?? 'none', ...extra
    });
  };
  try {
    // Idempotency: check if already completed
    const { data: runCheck } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
    const existingResult = runCheck?.steps?.export_ean_xlsx;
    if (existingResult?.status === 'success' || existingResult?.status === 'completed') {
      console.log(`[sync:step:export_ean_xlsx] Already completed, noop`);
      return { success: true };
    }

    // 1. Get signed URL for Catalogo EAN.csv
    const csvPath = 'Catalogo EAN.csv';
    const { data: signedData, error: signedError } = await supabase.storage
      .from('exports').createSignedUrl(csvPath, 600);
    
    if (signedError || !signedData?.signedUrl) {
      const { content: csvContent, error: dlError } = await downloadFromStorage(supabase, 'exports', csvPath);
      if (dlError || !csvContent) {
        const error = `Catalogo EAN.csv non trovato: ${dlError || signedError?.message || 'empty'}`;
        await updateStepResult(supabase, runId, 'export_ean_xlsx', { status: 'failed', error, metrics: {} });
        return { success: false, error };
      }
      return processEanXlsxFromContent(supabase, runId, csvContent, startTime);
    }

    const signedUrl = signedData.signedUrl;
    
    // 2. HEAD to get file size
    let totalBytes = 0;
    try {
      const headResp = await fetch(signedUrl, { method: 'HEAD' });
      totalBytes = parseInt(headResp.headers.get('content-length') || '0');
    } catch (_) { /* will handle below */ }

    // 3. For small files (< 2MB), just download directly
    if (totalBytes > 0 && totalBytes < 2 * 1024 * 1024) {
      const resp = await fetch(signedUrl);
      const csvContent = await resp.text();
      return processEanXlsxFromContent(supabase, runId, csvContent, startTime);
    }

    // 4. STREAMING: Read CSV via Range requests, build XLSX incrementally
    const XLSX = await import("npm:xlsx@0.18.5");
    
    // Load EAN template from storage bucket (unified loader with checksum)
    let eanTemplateBytes: Uint8Array | null;
    const t0 = Date.now();
    try {
      eanTemplateBytes = await loadTemplateFromStorage(supabase, 'Catalogo EAN.xlsx', runId);
    } catch (e: unknown) {
      const error = `Template Catalogo EAN.xlsx non trovato in storage: ${errMsg(e)}`;
      await updateStepResult(supabase, runId, 'export_ean_xlsx', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    await logEanStage('template_download_ok', t0, { size_bytes: eanTemplateBytes!.length });

    // Parse template with sheetRows:2 — only header + 1 format row (avoids parsing ~28K data rows from 16MB template)
    const t1 = Date.now();
    const templateWb = XLSX.read(eanTemplateBytes, { type: 'array', sheetRows: 2, cellStyles: false, bookDeps: false, bookFiles: false, bookProps: false, bookSheets: false, bookVBA: false });
    await logEanStage('template_parse_done', t1);
    console.log(`[sync:step:export_ean_xlsx] TIMING template_parse: ${Date.now() - t1}ms (sheetRows=2, write_path_optimized)`);

    // Free template bytes immediately
    eanTemplateBytes = null;

    const templateWsRef = templateWb.Sheets['Catalogo_EAN'];
    if (!templateWsRef) {
      const error = 'Template Catalogo EAN.xlsx: foglio Catalogo_EAN non trovato';
      await updateStepResult(supabase, runId, 'export_ean_xlsx', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    // Save template header/autofilter/cols for validation (row 0)
    const tmplEanRange = templateWsRef['!ref'] ? XLSX.utils.decode_range(templateWsRef['!ref']) : null;

    // Clear data rows from template (with sheetRows:2 there's at most row 1)
    if (tmplEanRange) {
      for (let R = 1; R <= tmplEanRange.e.r; R++) {
        for (let C = 0; C <= tmplEanRange.e.c; C++) {
          delete templateWsRef[XLSX.utils.encode_cell({ r: R, c: C })];
        }
      }
    }
    
    const ws = templateWsRef;
    let rowsWritten = 0;
    let headers: string[] = [];
    let eanColIdx = -1;
    let cursorPos = 0;
    let partialLine = '';
    let headerParsed = false;
    let chunkCount = 0;

    // Read template headers for column name mapping
    const tmplHeaders: string[] = [];
    if (tmplEanRange) {
      for (let c = 0; c <= tmplEanRange.e.c; c++) {
        const cell = ws[XLSX.utils.encode_cell({ r: 0, c })];
        tmplHeaders.push(cell?.v?.toString() || '');
      }
    }
    let csvToTmplCol: number[] = [];

    const t2 = Date.now();
    while (true) {
      const rangeEnd = cursorPos + XLSX_STREAM_CHUNK_BYTES - 1;
      const rangeHeader = `bytes=${cursorPos}-${rangeEnd}`;
      
      let resp: Response;
      try {
        resp = await fetch(signedUrl, { headers: { 'Range': rangeHeader } });
      } catch (e) {
        const error = `Range fetch failed at pos ${cursorPos}: ${errMsg(e)}`;
        await updateStepResult(supabase, runId, 'export_ean_xlsx', { status: 'failed', error, metrics: {} });
        return { success: false, error };
      }

      const rawBytes = new Uint8Array(await resp.arrayBuffer());
      if (rawBytes.length === 0) break;

      const chunk = new TextDecoder().decode(rawBytes);
      const combined = partialLine + chunk;
      const lines = combined.split('\n');
      partialLine = lines.pop() || '';

      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed) continue;

        if (!headerParsed) {
          headers = trimmed.split(';');
          eanColIdx = headers.indexOf('EAN');
          // Build CSV→template column mapping by name
          csvToTmplCol = headers.map(h => tmplHeaders.indexOf(h));
          headerParsed = true;
          continue;
        }

        const vals = trimmed.split(';');
        rowsWritten++;
        const r = rowsWritten;
        for (let c = 0; c < headers.length; c++) {
          const tmplC = csvToTmplCol[c];
          if (tmplC < 0) continue;
          const val = vals[c] || '';
          const cell = makeEanCell(headers[c], val);
          if (!cell) continue;
          ws[XLSX.utils.encode_cell({ r, c: tmplC })] = cell;
        }
      }

      chunkCount++;
      
      if (resp.status === 200 || rawBytes.length < XLSX_STREAM_CHUNK_BYTES) {
        if (partialLine.trim() && headerParsed) {
          const vals = partialLine.trim().split(';');
          rowsWritten++;
          const r = rowsWritten;
          for (let c = 0; c < headers.length; c++) {
            const tmplC = csvToTmplCol[c];
            if (tmplC < 0) continue;
            const val = vals[c] || '';
            const cell = makeEanCell(headers[c], val);
            if (!cell) continue;
            ws[XLSX.utils.encode_cell({ r, c: tmplC })] = cell;
          }
        }
        break;
      }

      cursorPos += rawBytes.length;
    }
    await logEanStage('data_filled_done', t2, { rows: rowsWritten });
    console.log(`[sync:step:export_ean_xlsx] TIMING csv_stream: ${Date.now() - t2}ms, rows=${rowsWritten}, chunks=${chunkCount}`);

    if (!headerParsed || rowsWritten === 0) {
      const error = 'Catalogo EAN.csv vuoto o senza dati';
      await updateStepResult(supabase, runId, 'export_ean_xlsx', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }

    // 5. Update range — use template column width (not CSV width)
    const maxCol = tmplEanRange ? tmplEanRange.e.c : headers.length - 1;
    ws['!ref'] = XLSX.utils.encode_range({ s: { r: 0, c: 0 }, e: { r: rowsWritten, c: maxCol } });
    const wb = templateWb;
    
    // 6. Lightweight validation (no ZIP unzip, O(n_columns))
    const t3 = Date.now();
    await logEanStage('before_validation', t3);
    const eanValidation = validateEanLightweight(
      XLSX, wb, ws, 'Catalogo_EAN', rowsWritten, headers.length, eanColIdx
    );
    await logEanStage('after_validation', t3, { passed: eanValidation.passed, integrity_mode: 'light' });

    if (!eanValidation.passed) {
      const error = `Pre-SFTP validation failed for Catalogo EAN.xlsx: ${eanValidation.errors.join('; ')}`;
      await safeLogEvent(supabase, runId, 'ERROR', 'validation_failed', { export_name: 'Catalogo EAN', errors: eanValidation.errors });
      await updateStepResult(supabase, runId, 'export_ean_xlsx', { status: 'failed', error, metrics: {}, validation_passed: false, validation_warnings: [] } as StepResultData);
      return { success: false, error };
    }

    // 7. Serialize XLSX
    const t4 = Date.now();
    await logEanStage('before_write', t4);
    const xlsxBuffer = XLSX.write(wb, { bookType: 'xlsx', type: 'array', compression: false, bookSST: false });
    const outputSize = xlsxBuffer.byteLength || xlsxBuffer.length || 0;
    await logEanStage('after_write', t4, { size_bytes: outputSize });
    console.log(`[sync:step:export_ean_xlsx] TIMING xlsx_write: ${Date.now() - t4}ms, output_size_bytes=${outputSize}`);

    // 8. Upload — use xlsxBuffer directly, no copy
    const t5 = Date.now();
    await logEanStage('before_upload', t5, { size_bytes: outputSize });
    const blob = new Blob([xlsxBuffer], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
    
    const { error: uploadError } = await supabase.storage.from('exports').upload(
      'Catalogo EAN.xlsx', blob, { upsert: true, contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' }
    );
    await logEanStage('after_upload', t5, { duration_ms: Date.now() - t5, size_bytes: outputSize });

    if (uploadError) {
      const error = `Upload Catalogo EAN.xlsx fallito: ${uploadError.message}`;
      await updateStepResult(supabase, runId, 'export_ean_xlsx', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }

    const elapsed = Date.now() - startTime;

    // Slow path warnings (observability only, non-blocking)
    const writeDur = Date.now() - t4;
    const uploadDur = Date.now() - t5;
    if (writeDur > 10000 || uploadDur > 10000 || elapsed > 30000) {
      await safeLogEvent(supabase, runId, 'WARN', 'export_ean_xlsx_stage', {
        step: 'export_ean_xlsx', stage: 'slow_path', duration_ms: elapsed,
        write_ms: writeDur, upload_ms: uploadDur, size_bytes: outputSize, rows: rowsWritten, heap_mb: heap()
      });
    }

    await logEanStage('completed', startTime, { rows: rowsWritten, size_bytes: outputSize, duration_ms: elapsed });
    await updateStepResult(supabase, runId, 'export_ean_xlsx', {
      status: 'success', duration_ms: elapsed, rows: rowsWritten,
      metrics: { ean_xlsx_rows: rowsWritten },
      rows_written: rowsWritten, total_products: rowsWritten,
      stream_chunks: chunkCount, validation_passed: true, validation_warnings: []
    } as StepResultData);
    console.log(`[sync:step:export_ean_xlsx] Completed: ${rowsWritten} rows in ${elapsed}ms (${chunkCount} stream chunks, sheetRows=2)`);
    return { success: true };
  } catch (e: unknown) {
    console.error(`[sync:step:export_ean_xlsx] Error:`, e);
    await safeLogEvent(supabase, runId, 'ERROR', 'export_ean_xlsx_stage', {
      step: 'export_ean_xlsx', stage: 'failed', last_stage: lastStage, reason: errMsg(e),
      duration_ms: Date.now() - startTime, heap_mb: heap(), size_bytes: 0, rows: 0, integrity_mode: 'none'
    });
    await updateStepResult(supabase, runId, 'export_ean_xlsx', { status: 'failed', error: errMsg(e), metrics: {} });
    return { success: false, error: errMsg(e) };
  }
}

/** Fallback: process CSV content already in memory (small files) */
async function processEanXlsxFromContent(supabase: SupabaseClient, runId: string, csvContent: string, startTime: number): Promise<{ success: boolean; error?: string }> {
  let lastStage = 'init';
  const heapMb = () => getMemMB() ?? 0;
  const logStg = async (stage: string, t0: number, extra: Record<string, unknown> = {}): Promise<void> => {
    lastStage = stage;
    await safeLogEvent(supabase, runId, 'INFO', 'export_ean_xlsx_stage', {
      step: 'export_ean_xlsx', stage, duration_ms: Date.now() - t0,
      size_bytes: extra.size_bytes ?? 0, rows: extra.rows ?? 0, heap_mb: heapMb(),
      integrity_mode: extra.integrity_mode ?? 'none', ...extra
    });
  };
  try {
    const XLSX = await import("npm:xlsx@0.18.5");
    
    // Load EAN template
    let eanTmplBytes: Uint8Array | null;
    const t0 = Date.now();
    try {
      eanTmplBytes = await loadTemplateFromStorage(supabase, 'Catalogo EAN.xlsx', runId);
    } catch (e: unknown) {
      const error = `Template Catalogo EAN.xlsx non trovato: ${errMsg(e)}`;
      await updateStepResult(supabase, runId, 'export_ean_xlsx', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    await logStg('template_download_ok', t0, { size_bytes: eanTmplBytes!.length });

    // Parse with sheetRows:2 to avoid parsing all ~28K data rows
    const t1 = Date.now();
    const wb = XLSX.read(eanTmplBytes, { type: 'array', sheetRows: 2, cellStyles: false, bookDeps: false, bookFiles: false, bookProps: false, bookSheets: false, bookVBA: false });
    await logStg('template_parse_done', t1);
    // Free template bytes immediately
    eanTmplBytes = null;

    const ws = wb.Sheets['Catalogo_EAN'];
    if (!ws) {
      const error = 'Template Catalogo EAN.xlsx: foglio Catalogo_EAN non trovato';
      await updateStepResult(supabase, runId, 'export_ean_xlsx', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    
    // Clear data rows (with sheetRows:2 there's at most row 1)
    const tmplRange = ws['!ref'] ? XLSX.utils.decode_range(ws['!ref']) : null;
    if (tmplRange) {
      for (let R = 1; R <= tmplRange.e.r; R++) {
        for (let C = 0; C <= tmplRange.e.c; C++) {
          delete ws[XLSX.utils.encode_cell({ r: R, c: C })];
        }
      }
    }

    // Read template headers for column name mapping
    const tmplHeaders: string[] = [];
    if (tmplRange) {
      for (let c = 0; c <= tmplRange.e.c; c++) {
        const cell = ws[XLSX.utils.encode_cell({ r: 0, c })];
        tmplHeaders.push(cell?.v?.toString() || '');
      }
    }
    
    const lines = csvContent.split('\n');
    const headerLine = lines[0]?.trim();
    if (!headerLine) {
      const error = 'Catalogo EAN.csv vuoto o senza header';
      await updateStepResult(supabase, runId, 'export_ean_xlsx', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    const headers = headerLine.split(';');
    const eanColIdx = headers.indexOf('EAN');
    // Build CSV→template column mapping by name
    const csvToTmplCol = headers.map(h => tmplHeaders.indexOf(h));

    let rowsWritten = 0;
    const t2 = Date.now();
    for (let lineIdx = 1; lineIdx < lines.length; lineIdx++) {
      const line = lines[lineIdx].trim();
      if (!line) continue;
      const vals = line.split(';');
      rowsWritten++;
      const r = rowsWritten;
      for (let c = 0; c < headers.length; c++) {
        const tmplC = csvToTmplCol[c];
        if (tmplC < 0) continue;
        const val = vals[c] || '';
        const cell = makeEanCell(headers[c], val);
        if (!cell) continue;
        ws[XLSX.utils.encode_cell({ r, c: tmplC })] = cell;
      }
    }
    await logStg('data_filled_done', t2, { rows: rowsWritten });
    // Use template column width for !ref
    const maxCol = tmplRange ? tmplRange.e.c : headers.length - 1;
    ws['!ref'] = XLSX.utils.encode_range({ s: { r: 0, c: 0 }, e: { r: rowsWritten, c: maxCol } });
    
    // Lightweight validation
    const t3 = Date.now();
    await logStg('before_validation', t3);
    const val = validateEanLightweight(XLSX, wb, ws, 'Catalogo_EAN', rowsWritten, headers.length, eanColIdx);
    await logStg('after_validation', t3, { passed: val.passed, integrity_mode: 'light' });

    if (!val.passed) {
      const error = `Validation failed: ${val.errors.join('; ')}`;
      await safeLogEvent(supabase, runId, 'ERROR', 'validation_failed', { export_name: 'Catalogo EAN', errors: val.errors });
      await updateStepResult(supabase, runId, 'export_ean_xlsx', { status: 'failed', error, metrics: {}, validation_passed: false, validation_warnings: [] } as StepResultData);
      return { success: false, error };
    }

    // Serialize XLSX
    const t4 = Date.now();
    await logStg('before_write', t4);
    const xlsxBuffer = XLSX.write(wb, { bookType: 'xlsx', type: 'array', compression: false, bookSST: false });
    const outputSize = xlsxBuffer.byteLength || xlsxBuffer.length || 0;
    await logStg('after_write', t4, { size_bytes: outputSize });

    // Upload — no extra copy
    const t5 = Date.now();
    await logStg('before_upload', t5, { size_bytes: outputSize });
    const blob = new Blob([xlsxBuffer], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
    const { error: uploadError } = await supabase.storage.from('exports').upload(
      'Catalogo EAN.xlsx', blob, { upsert: true, contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' }
    );
    await logStg('after_upload', t5, { duration_ms: Date.now() - t5, size_bytes: outputSize });

    if (uploadError) {
      const error = `Upload Catalogo EAN.xlsx fallito: ${uploadError.message}`;
      await updateStepResult(supabase, runId, 'export_ean_xlsx', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    const elapsed = Date.now() - startTime;

    // Slow path warnings
    if ((Date.now() - t4) > 10000 || (Date.now() - t5) > 10000 || elapsed > 30000) {
      await safeLogEvent(supabase, runId, 'WARN', 'export_ean_xlsx_stage', {
        step: 'export_ean_xlsx', stage: 'slow_path', duration_ms: elapsed,
        size_bytes: outputSize, rows: rowsWritten, heap_mb: heapMb()
      });
    }

    await logStg('completed', startTime, { rows: rowsWritten, size_bytes: outputSize, duration_ms: elapsed });
    await updateStepResult(supabase, runId, 'export_ean_xlsx', {
      status: 'success', duration_ms: elapsed, rows: rowsWritten,
      metrics: { ean_xlsx_rows: rowsWritten },
      rows_written: rowsWritten, total_products: rowsWritten,
      validation_passed: true, validation_warnings: []
    } as StepResultData);
    console.log(`[sync:step:export_ean_xlsx] Completed (direct, sheetRows=2): ${rowsWritten} rows in ${elapsed}ms`);
    return { success: true };
  } catch (e: unknown) {
    console.error(`[sync:step:export_ean_xlsx] Error (direct):`, e);
    await safeLogEvent(supabase, runId, 'ERROR', 'export_ean_xlsx_stage', {
      step: 'export_ean_xlsx', stage: 'failed', last_stage: lastStage, reason: errMsg(e),
      duration_ms: Date.now() - startTime, heap_mb: heapMb() ?? 0, size_bytes: 0, rows: 0, integrity_mode: 'none'
    });
    await updateStepResult(supabase, runId, 'export_ean_xlsx', { status: 'failed', error: errMsg(e), metrics: {} });
    return { success: false, error: errMsg(e) };
  }
}

// ========== STEP: OVERRIDE_PRODUCTS ==========
async function stepOverrideProducts(supabase: SupabaseClient, runId: string): Promise<{ success: boolean; error?: string }> {
  console.log(`[sync:step:override_products] Starting for run ${runId}`);
  const startTime = Date.now();
  try {
    // Idempotency
    const { data: runCheck } = await supabase.from('sync_runs').select('steps').eq('id', runId).single();
    const existingResult = runCheck?.steps?.override_products;
    if (existingResult?.status === 'success' || existingResult?.status === 'completed') {
      console.log(`[sync:step:override_products] Already completed, noop`);
      return { success: true };
    }

    // Check for override file in mapping-files bucket
    const { data: files } = await supabase.storage.from('mapping-files').list('', { search: 'override' });
    const overrideFile = files?.find((f: { name: string }) =>
      f.name.toLowerCase().includes('override') && (f.name.toLowerCase().endsWith('.xlsx') || f.name.toLowerCase().endsWith('.xls'))
    );

    if (!overrideFile) {
      console.log(`[sync:step:override_products] No override file found in mapping-files, completing with 0 overrides`);
      const elapsed = Date.now() - startTime;
      await updateStepResult(supabase, runId, 'override_products', {
        status: 'success', duration_ms: elapsed,
        metrics: { override_count: 0, reason: 'no_override_file' },
        override_count: 0
      } as StepResultData);
      return { success: true };
    }

    // Download override file
    const { data: overrideData, error: dlError } = await supabase.storage.from('mapping-files').download(overrideFile.name);
    if (dlError || !overrideData) {
      const error = `Failed to download override file: ${dlError?.message || 'empty'}`;
      await updateStepResult(supabase, runId, 'override_products', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }

    // Parse XLSX
    const XLSX = await import("npm:xlsx@0.18.5");
    const arrayBuffer = await overrideData.arrayBuffer();
    const wb = XLSX.read(new Uint8Array(arrayBuffer), { type: 'array' });
    const wsName = wb.SheetNames[0];
    if (!wsName) {
      const elapsed = Date.now() - startTime;
      await updateStepResult(supabase, runId, 'override_products', {
        status: 'success', duration_ms: elapsed,
        metrics: { override_count: 0, reason: 'empty_override_file' }, override_count: 0
      } as StepResultData);
      return { success: true };
    }
    const ws = wb.Sheets[wsName];
    const rows: Record<string, unknown>[] = XLSX.utils.sheet_to_json(ws, { defval: '' });

    if (rows.length === 0) {
      const elapsed = Date.now() - startTime;
      await updateStepResult(supabase, runId, 'override_products', {
        status: 'success', duration_ms: elapsed,
        metrics: { override_count: 0, reason: 'no_rows_in_override' }, override_count: 0
      } as StepResultData);
      return { success: true };
    }

    // Build override index by EAN and SKU
    const overrideByEan = new Map<string, Record<string, unknown>>();
    const overrideBySku = new Map<string, Record<string, unknown>>();
    for (const row of rows) {
      const ean = normalizeEAN(row.EAN || row.ean);
      const sku = String(row.SKU || row.sku || row.MPN || row.mpn || '').trim().toLowerCase();
      if (ean.ok && ean.value) overrideByEan.set(ean.value, row);
      if (sku) overrideBySku.set(sku, row);
    }

    // Load products TSV
    const { content: productsContent, error: prodError } = await downloadFromStorage(supabase, 'exports', PRODUCTS_FILE_PATH);
    if (prodError || !productsContent) {
      const error = `Products file not found: ${prodError || 'empty'}`;
      await updateStepResult(supabase, runId, 'override_products', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }

    const lines = productsContent.split('\n');
    const headerLine = lines[0];
    if (!headerLine) {
      await updateStepResult(supabase, runId, 'override_products', { status: 'failed', error: 'Empty products file', metrics: {} });
      return { success: false, error: 'Empty products file' };
    }
    const headers = headerLine.split('\t');
    const eanIdx = headers.indexOf('EAN');
    const mpnIdx = headers.indexOf('MPN');
    const stockIdx = headers.indexOf('Stock');
    const lpIdx = headers.indexOf('LP');
    const cbpIdx = headers.indexOf('CBP');

    let overrideCount = 0;
    const outputLines = [headerLine];

    for (let i = 1; i < lines.length; i++) {
      const line = lines[i];
      if (!line.trim()) continue;
      const fields = line.split('\t');

      const ean = eanIdx >= 0 ? normalizeEAN(fields[eanIdx]) : { ok: false as const };
      const sku = mpnIdx >= 0 ? (fields[mpnIdx]?.trim().toLowerCase() || '') : '';

      let overrideRow: Record<string, unknown> | undefined;
      if (sku) overrideRow = overrideBySku.get(sku);
      if (!overrideRow && ean.ok && ean.value) overrideRow = overrideByEan.get(ean.value);

      if (overrideRow) {
        overrideCount++;
        const qty = overrideRow.Quantity ?? overrideRow.quantity;
        const lp = overrideRow.ListPrice ?? overrideRow.listPrice ?? overrideRow.list_price;
        const op = overrideRow.OfferPrice ?? overrideRow.offerPrice ?? overrideRow.offer_price;
        if (qty !== undefined && qty !== '' && stockIdx >= 0) fields[stockIdx] = String(parseInt(String(qty)) || 0);
        if (lp !== undefined && lp !== '' && lpIdx >= 0) fields[lpIdx] = String(parseFloat(String(lp)) || 0);
        if (op !== undefined && op !== '' && cbpIdx >= 0) fields[cbpIdx] = String(parseFloat(String(op)) || 0);
      }
      outputLines.push(fields.join('\t'));
    }

    // Write back products.tsv
    const newContent = outputLines.join('\n');
    const uploadResult = await uploadToStorage(supabase, 'exports', PRODUCTS_FILE_PATH, newContent, 'text/tab-separated-values');
    if (!uploadResult.success) {
      await updateStepResult(supabase, runId, 'override_products', { status: 'failed', error: uploadResult.error!, metrics: {} });
      return { success: false, error: uploadResult.error };
    }

    const elapsed = Date.now() - startTime;
    await updateStepResult(supabase, runId, 'override_products', {
      status: 'success', duration_ms: elapsed,
      metrics: { override_count: overrideCount, total_override_rows: rows.length },
      override_count: overrideCount, total_override_rows: rows.length
    } as StepResultData);
    console.log(`[sync:step:override_products] Completed: ${overrideCount} overrides applied from ${rows.length} rows in ${elapsed}ms`);
    return { success: true };
  } catch (e: unknown) {
    console.error(`[sync:step:override_products] Error:`, e);
    await updateStepResult(supabase, runId, 'override_products', { status: 'failed', error: errMsg(e), metrics: {} });
    return { success: false, error: errMsg(e) };
  }
}

// Helper: get memory info if available
function getMemMB(): number | null {
  try { return Math.round((Deno as any).memoryUsage().heapUsed / 1024 / 1024); } catch { return null; }
}

// Helper: log stage event - fire-and-forget by default to save CPU budget.
// Use await logStage(...) only for the first sentinel event.
async function logStage(supabase: SupabaseClient, runId: string, stage: string, stageStartMs: number, extra: Record<string, unknown> = {}): Promise<void> {
  const mem = getMemMB();
  const durationMs = Date.now() - stageStartMs;
  await safeLogEvent(supabase, runId, 'INFO', 'export_amazon_stage', { step: 'export_amazon', stage, heap_mb: mem, duration_ms: durationMs, ...extra });
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function stepExportAmazon(supabase: SupabaseClient, runId: string, feeConfig: any): Promise<{ success: boolean; error?: string }> {
  console.log(`[sync:step:export_amazon] Starting for run ${runId}`);
  const startTime = Date.now();
  let stageStart = Date.now();
  try {
    // Sentinel stage — awaited to guarantee at least one event before potential crash
    await logStage(supabase, runId, 'before_data_preparation', stageStart);

    // Phase 1: Download products + stock location IN PARALLEL
    const amazonFeeDrev = feeConfig?.amazonFeeDrev ?? feeConfig?.feeDrev ?? 1.05;
    const amazonFeeMkt = feeConfig?.amazonFeeMkt ?? feeConfig?.feeMkt ?? 1.08;
    const amazonShipping = feeConfig?.amazonShippingCost ?? feeConfig?.shippingCost ?? 6.00;
    const itDays = feeConfig?.amazonItPrepDays ?? 3;
    const euDays = feeConfig?.amazonEuPrepDays ?? 5;

    const stockLocationPath = `stock-location/runs/${runId}.txt`;
    const [prodResult, slResult] = await Promise.all([
      downloadFromStorage(supabase, 'exports', PRODUCTS_FILE_PATH),
      downloadFromStorage(supabase, 'ftp-import', stockLocationPath),
    ]);

    if (prodResult.error || !prodResult.content) {
      const error = prodResult.error || 'Products file not found';
      await updateStepResult(supabase, runId, 'export_amazon', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }

    // Build stock location index (lightweight map)
    let stockLocationIndex: Record<string, { stockIT: number; stockEU: number }> | null = null;
    if (slResult.content) {
      stockLocationIndex = {};
      let slPos = 0;
      const slText = slResult.content;
      // Parse header
      const slFirstNl = slText.indexOf('\n');
      const slHeaderLine = slFirstNl >= 0 ? slText.substring(0, slFirstNl) : slText;
      const slHeaders = slHeaderLine.replace(/\r/g, '').split(';').map((h: string) => h.trim().toLowerCase());
      const mIdx = slHeaders.indexOf('matnr'), sIdx = slHeaders.indexOf('stock'), lIdx = slHeaders.indexOf('locationid');
      if (mIdx >= 0 && sIdx >= 0 && lIdx >= 0) {
        slPos = slFirstNl + 1;
        while (slPos < slText.length) {
          const nlIdx = slText.indexOf('\n', slPos);
          const lineEnd = nlIdx >= 0 ? nlIdx : slText.length;
          const line = slText.substring(slPos, lineEnd).replace(/\r/g, '');
          slPos = lineEnd + 1;
          if (!line) continue;
          const vals = line.split(';');
          const matnr = vals[mIdx]?.trim();
          if (!matnr) continue;
          const stock = parseInt(vals[sIdx]) || 0;
          const locationId = parseInt(vals[lIdx]) || 0;
          if (!stockLocationIndex[matnr]) stockLocationIndex[matnr] = { stockIT: 0, stockEU: 0 };
          if (locationId === 4242) stockLocationIndex[matnr].stockIT += stock;
          else if (locationId === 4254) stockLocationIndex[matnr].stockEU += stock;
        }
      }
      slResult.content = undefined as any; // free
    }

    // Phase 2: Parse products TSV using cursor (NO split, NO lines array)
    const skus: string[] = [];
    const eans: string[] = [];
    const quantities: number[] = [];
    const leadDaysArr: number[] = [];
    const prices: string[] = [];
    let skipped = 0;

    const tsv = prodResult.content;
    prodResult.content = undefined as any; // allow GC of the download result wrapper

    // Parse header line
    const firstNl = tsv.indexOf('\n');
    const headerLine = firstNl >= 0 ? tsv.substring(0, firstNl) : tsv;
    const headerCols = headerLine.replace(/\r/g, '').split('\t');
    const colIdx = {
      Matnr: headerCols.indexOf('Matnr'),
      MPN: headerCols.indexOf('MPN'),
      EAN: headerCols.indexOf('EAN'),
      Stock: headerCols.indexOf('Stock'),
      LP: headerCols.indexOf('LP'),
      CBP: headerCols.indexOf('CBP'),
      Sur: headerCols.indexOf('Sur'),
    };

    // Cursor-based line iteration — never creates a lines[] array
    let pos = firstNl + 1;
    while (pos < tsv.length) {
      const nlIdx = tsv.indexOf('\n', pos);
      const lineEnd = nlIdx >= 0 ? nlIdx : tsv.length;
      // Skip empty lines without allocating a substring for them
      if (lineEnd === pos || (lineEnd === pos + 1 && tsv.charCodeAt(pos) === 13)) { pos = lineEnd + 1; continue; }
      const line = tsv.substring(pos, lineEnd);
      pos = lineEnd + 1;

      const vals = line.split('\t');

      const ean = vals[colIdx.EAN] || '';
      const norm = normalizeEAN(ean);
      if (!norm.ok || !norm.value || (norm.value.length !== 13 && norm.value.length !== 14)) { skipped++; continue; }

      const mpn = vals[colIdx.MPN] || '';
      if (!mpn.trim()) { skipped++; continue; }

      const matnr = vals[colIdx.Matnr] || '';
      const stockRaw = parseInt(vals[colIdx.Stock]) || 0;
      const lp = parseFloat(vals[colIdx.LP]) || 0;
      const cbp = parseFloat(vals[colIdx.CBP]) || 0;
      const sur = parseFloat(vals[colIdx.Sur]) || 0;

      let stockIT = stockRaw, stockEU = 0;
      if (stockLocationIndex?.[matnr]) { stockIT = stockLocationIndex[matnr].stockIT; stockEU = stockLocationIndex[matnr].stockEU; }
      const stockResult = resolveMarketplaceStock(stockIT, stockEU, true, itDays, euDays);
      if (!stockResult.shouldExport || stockResult.exportQty < 2) { skipped++; continue; }

      let baseCents = 0;
      if (cbp > 0) baseCents = Math.round((cbp + sur) * 100);
      else if (lp > 0) baseCents = Math.round(lp * 100);
      if (baseCents <= 0) { skipped++; continue; }

      const afterFees = Math.round(Math.round(Math.round((baseCents + Math.round(amazonShipping * 100)) * 1.22) * amazonFeeDrev) * amazonFeeMkt);
      const finalCents = toComma99Cents(afterFees);

      skus.push(mpn.replace(/[\x00-\x1f\x7f]/g, ''));
      eans.push(norm.value);
      quantities.push(stockResult.exportQty);
      leadDaysArr.push(stockResult.leadDays);
      prices.push((finalCents / 100).toFixed(2));
    }

    // Free intermediates
    stockLocationIndex = null;

    const validCount = skus.length;
    if (validCount === 0) {
      const error = 'Nessuna riga esportabile per Amazon';
      await updateStepResult(supabase, runId, 'export_amazon', { status: 'failed', error, metrics: {} });
      return { success: false, error };
    }
    stageStart = Date.now();
    logStage(supabase, runId, 'after_data_preparation', startTime, { valid: validCount, skipped });
    console.log(`[sync:step:export_amazon] ${validCount} valid, ${skipped} skipped`);

    // Phase 2: Generate TXT first (no XLSX lib needed, low memory)
    const txtParts: string[] = ['sku\tprice\tminimum-seller-allowed-price\tmaximum-seller-allowed-price\tquantity\tfulfillment-channel\thandling-time\n'];
    for (let i = 0; i < validCount; i++) {
      txtParts.push(`${skus[i]}\t${prices[i]}\t\t\t${quantities[i]}\t\t${leadDaysArr[i]}\n`);
    }
    const txtContent = txtParts.join('');
    txtParts.length = 0;
    const txtSave = await uploadToStorage(supabase, 'exports', 'amazon_price_inventory.txt', txtContent, 'text/plain');
    if (!txtSave.success) { await updateStepResult(supabase, runId, 'export_amazon', { status: 'failed', error: txtSave.error!, metrics: {} }); return { success: false, error: txtSave.error }; }
    stageStart = Date.now();
    logStage(supabase, runId, 'after_txt_upload', stageStart);

    // Phase 3: Load XLSX lib and template
    stageStart = Date.now();
    logStage(supabase, runId, 'before_template_load', stageStart);
    const XLSX = await import("npm:xlsx@0.18.5");

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
    stageStart = Date.now();
    logStage(supabase, runId, 'after_template_load', stageStart);

    // Phase 4: Build XLSM - CPU-optimized: pre-compute cell addresses
    const wb = XLSX.read(new Uint8Array(templateBuffer), { type: 'array', bookVBA: true });
    templateBuffer = null;
    const ws = wb.Sheets['Modello'];
    if (!ws) { await updateStepResult(supabase, runId, 'export_amazon', { status: 'failed', error: 'Foglio Modello non trovato', metrics: {} }); return { success: false, error: 'Foglio Modello non trovato' }; }

    const colLetters = ['A','B','C','H','AF','AG','AH','AK','BJ'];
    const DS = 4;

    // Clear ALL existing cells from template beyond header rows to avoid serializing stale data.
    // This also dramatically reduces the effective cell count for XLSX.write.
    const templateRef = ws['!ref'] || 'A1';
    const templateRange = XLSX.utils.decode_range(templateRef);
    const keysToDelete: string[] = [];
    for (const key of Object.keys(ws)) {
      if (key.startsWith('!')) continue;
      // Parse row number from cell address (e.g. "A5" → 5)
      const rowMatch = key.match(/(\d+)$/);
      if (!rowMatch) continue;
      const cellRow = parseInt(rowMatch[1], 10);
      if (cellRow > DS) keysToDelete.push(key); // DS=4, so row 5+ are data rows (1-indexed in addresses)
    }
    for (const k of keysToDelete) delete ws[k];

    // Write cells using pre-computed addresses (no encode_cell calls)
    for (let i = 0; i < validCount; i++) {
      const row = DS + i + 1;
      ws[`A${row}`] = { t: 's', v: skus[i] };
      ws[`B${row}`] = { t: 's', v: 'EAN' };
      ws[`C${row}`] = { t: 's', v: eans[i], z: '@' };
      ws[`H${row}`] = { t: 's', v: 'Nuovo' };
      ws[`AF${row}`] = { t: 's', v: 'Default' };
      ws[`AG${row}`] = { t: 'n', v: quantities[i] };
      ws[`AH${row}`] = { t: 'n', v: leadDaysArr[i] };
      ws[`AK${row}`] = { t: 's', v: prices[i] };
      ws[`BJ${row}`] = { t: 's', v: 'Modello Amazon predefinito' };
    }

    // TRIM !ref to minimum effective range — critical for reducing XLSX.write CPU cost
    const lastDataRow = DS + validCount; // 1-indexed: header rows 1-4 + validCount data rows
    ws['!ref'] = `A1:BJ${lastDataRow}`;

    // Strip heavy template metadata that inflates serialization cost
    delete ws['!rows'];
    delete ws['!cols'];
    delete ws['!merges'];
    delete ws['!autofilter'];
    delete ws['!images'];

    // Count actual cells set for diagnostics
    let approxCellsSet = 0;
    for (const key of Object.keys(ws)) { if (!key.startsWith('!')) approxCellsSet++; }

    stageStart = Date.now();
    const writeOptions = { bookType: 'xlsm' as const, bookVBA: Boolean((wb as unknown as Record<string, unknown>).vbaraw), type: 'array' as const, compression: false, bookSST: false, cellStyles: false };
    await logStage(supabase, runId, 'before_xlsx_write', stageStart, {
      rows: validCount,
      write_options: { compression: false, bookSST: false, cellStyles: false },
      sheet_ref_before: templateRef,
      sheet_ref_after: ws['!ref'],
      last_row: lastDataRow,
      last_col: 'BJ',
      approx_cells_set_count: approxCellsSet,
      template_sheet_name: 'Modello',
      template_range_rows: templateRange.e.r + 1,
      template_range_cols: templateRange.e.c + 1,
    });

    const writeT0 = Date.now();
    const xlsmOut = XLSX.write(wb, writeOptions);
    const writeDurationMs = Date.now() - writeT0;
    const writeSizeBytes = xlsmOut.byteLength || xlsmOut.length || 0;
    stageStart = Date.now();
    await logStage(supabase, runId, 'after_xlsx_write', stageStart, { duration_ms: writeDurationMs, size_bytes: writeSizeBytes });

    const xlsmBlob = new Blob([xlsmOut], { type: 'application/vnd.ms-excel.sheet.macroEnabled.12' });
    const { error: xlsmErr } = await supabase.storage.from('exports').upload('amazon_listing_loader.xlsm', xlsmBlob, { upsert: true, contentType: 'application/vnd.ms-excel.sheet.macroEnabled.12' });
    if (xlsmErr) { await updateStepResult(supabase, runId, 'export_amazon', { status: 'failed', error: xlsmErr.message, metrics: {} }); return { success: false, error: xlsmErr.message }; }

    // ========== SELFTEST (only when EXPORT_AMAZON_SELFTEST=true) ==========
    if (Deno.env.get('EXPORT_AMAZON_SELFTEST') === 'true') {
      try {
        console.log('[export_amazon] Running selftest...');
        // Re-read the generated XLSM
        const { data: testBlob } = await supabase.storage.from('exports').download('amazon_listing_loader.xlsm');
        if (testBlob) {
          const testBuf = await testBlob.arrayBuffer();
          const testWb = XLSX.read(new Uint8Array(testBuf), { type: 'array' });
          const testWs = testWb.Sheets['Modello'];
          if (!testWs) throw new Error('selftest: sheet Modello not found in output');

          // Invariant 1: row count
          const testRange = XLSX.utils.decode_range(testWs['!ref'] || 'A1');
          const writtenRows = testRange.e.r - DS + 1;
          if (writtenRows < validCount) {
            throw new Error(`selftest: row_count mismatch: written=${writtenRows} expected=${validCount}`);
          }

          // Invariant 2: sample rows (first, middle, last)
          const sampleIndices = [0, Math.floor(validCount / 2), validCount - 1];
          for (const idx of sampleIndices) {
            const row = DS + idx + 1;
            const cellA = testWs[`A${row}`];
            const cellC = testWs[`C${row}`];
            const cellAK = testWs[`AK${row}`];
            const cellBJ = testWs[`BJ${row}`];

            // Check SKU matches
            if (!cellA || String(cellA.v) !== skus[idx]) {
              throw new Error(`selftest: SKU mismatch at row ${row}: got=${cellA?.v} expected=${skus[idx]}`);
            }
            // Check EAN matches and is text type
            if (!cellC || String(cellC.v) !== eans[idx]) {
              throw new Error(`selftest: EAN mismatch at row ${row}: got=${cellC?.v} expected=${eans[idx]}`);
            }
            if (cellC.t !== 's') {
              throw new Error(`selftest: EAN not string type at row ${row}: type=${cellC.t}`);
            }
            // Check price matches
            if (!cellAK || String(cellAK.v) !== prices[idx]) {
              throw new Error(`selftest: price mismatch at row ${row}: got=${cellAK?.v} expected=${prices[idx]}`);
            }
            // Check BJ column (beyond Z)
            if (!cellBJ || String(cellBJ.v) !== 'Modello Amazon predefinito') {
              throw new Error(`selftest: BJ mismatch at row ${row}: got=${cellBJ?.v}`);
            }
          }

          // Invariant 3: no formula injection in text cells
          for (const idx of sampleIndices) {
            const row = DS + idx + 1;
            for (const col of ['A', 'C', 'AK']) {
              const cell = testWs[`${col}${row}`];
              if (cell && cell.t === 's' && typeof cell.v === 'string') {
                const v = cell.v;
                if (v.startsWith('=') || v.startsWith('+') || v.startsWith('-') || v.startsWith('@')) {
                  throw new Error(`selftest: potential formula injection at ${col}${row}: starts with ${v[0]}`);
                }
              }
            }
          }

          // Invariant 4: EAN preserves leading zeros
          for (const idx of sampleIndices) {
            const row = DS + idx + 1;
            const cellC = testWs[`C${row}`];
            if (cellC && eans[idx].startsWith('0') && !String(cellC.v).startsWith('0')) {
              throw new Error(`selftest: EAN leading zero lost at row ${row}: got=${cellC.v} expected=${eans[idx]}`);
            }
          }

          console.log(`[export_amazon] Selftest PASSED: ${sampleIndices.length} sample rows verified, ${validCount} total rows`);
        }
      } catch (selfTestErr: unknown) {
        const reason = errMsg(selfTestErr);
        console.error(`[export_amazon] Selftest FAILED: ${reason}`);
        await supabase.rpc('log_sync_event', {
          p_run_id: runId, p_level: 'ERROR', p_message: 'step_failed',
          p_details: { step: 'export_amazon', reason: `selftest: ${reason}`, selftest: true }
        });
        await updateStepResult(supabase, runId, 'export_amazon', { status: 'failed', error: `selftest: ${reason}`, metrics: {} });
        return { success: false, error: `selftest: ${reason}` };
      }
    }

    const elapsed = Date.now() - startTime;
    await updateStepResult(supabase, runId, 'export_amazon', {
      status: 'success', duration_ms: elapsed, rows: validCount, skipped,
      metrics: { amazon_export_rows: validCount, amazon_export_skipped: skipped },
      rows_written: validCount,
      total_products: validCount
    });
    stageStart = Date.now();
    await logStage(supabase, runId, 'completed', stageStart, { rows: validCount, elapsed_ms: elapsed });
    console.log(`[sync:step:export_amazon] Completed: ${validCount} rows in ${elapsed}ms`);
    console.log(JSON.stringify({ diag_tag: 'xlsx_export_retry_decision', run_id: runId, step: 'export_amazon', decision: 'completed', rows_written: validCount, total_products: validCount, elapsed_ms: elapsed }));
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
    // Lock ownership: invocation_id passed by orchestrator for ownership enforcement
    const lockInvocationId = body.lock_invocation_id as string | undefined;
    
    // Special handler: compute template checksums (no run_id needed)
    if (step === 'compute_template_checksums') {
      const checksums: Record<string, { sha256: string; bytes: number }> = {};
      const templates = ['Catalogo EAN.xlsx', 'Export ePrice.xlsx', 'Export Mediaworld.xlsx'];
      for (const name of templates) {
        const { data: blob, error: dlErr } = await supabase.storage.from('exports').download(`templates/${name}`);
        if (dlErr || !blob) {
          return new Response(JSON.stringify({ status: 'error', message: `Failed to download ${name}: ${dlErr?.message}` }),
            { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }
        const bytes = new Uint8Array(await blob.arrayBuffer());
        const hash = await computeSHA256(bytes);
        checksums[name] = { sha256: hash, bytes: bytes.length };
      }
      return new Response(JSON.stringify({ status: 'ok', checksums }), 
        { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }
    
    if (!run_id || !step) {
      return new Response(JSON.stringify({ status: 'error', message: 'run_id and step required' }), 
        { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }
    
    // Lock ownership enforcement: if invocation_id provided, verify lock ownership before executing step
    if (lockInvocationId) {
      const lockCheck = await assertLockOwned(supabase, run_id, lockInvocationId);
      if (!lockCheck.owned) {
        console.error(`[sync-step-runner] LOCK NOT OWNED: step=${step}, our_inv=${lockInvocationId}, holder_run=${lockCheck.holder_run_id}, holder_inv=${lockCheck.holder_invocation_id}`);
        await safeLogEvent(supabase, run_id, 'ERROR', 'step_runner_lock_rejected', {
          step, lock_invocation_id: lockInvocationId,
          holder_run_id: lockCheck.holder_run_id,
          holder_invocation_id: lockCheck.holder_invocation_id
        });
        return new Response(JSON.stringify({ status: 'error', message: 'lock_ownership_rejected', step }),
          { status: 409, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
    } else {
      // No invocation_id: backward compatible but warn (no renew lease)
      console.warn(`[sync-step-runner] WARN: lock_invocation_id not provided for step=${step}, run=${run_id}`);
      await safeLogEvent(supabase, run_id, 'WARN', 'lock_invocation_missing', {
        step, context: 'sync-step-runner', run_id
      });
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
      case 'override_products': result = await stepOverrideProducts(supabase, run_id); break;
      case 'export_ean': result = await stepExportEan(supabase, run_id, fee_config); break;
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
