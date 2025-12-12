/**
 * Stock Location Split IT/EU Utilities
 * 
 * This module provides shared logic for handling IT/EU stock split.
 * Used by both client-side (UI) and server-side (edge functions) pipelines.
 */

// ============================================================
// TYPES
// ============================================================

export interface StockLocationEntry {
  Matnr: string;
  Stock: number;
  LocationID: number;
  ManufPartNo: string;
}

export interface StockLocationIndex {
  [matnr: string]: {
    stockIT: number;
    stockEU: number;
    mpnList: Set<string>;
  };
}

export interface StockLocationResult {
  stockIT: number;
  stockEU: number;
}

export interface ResolveMarketplaceStockResult {
  exportQty: number;
  leadDays: number;
  shouldExport: boolean;
  source: 'IT' | 'EU_FALLBACK' | 'NONE';
}

export interface StockLocationWarnings {
  missing_location_file: number;
  invalid_location_parse: number;
  missing_location_data: number;
  split_mismatch: number;
  multi_mpn_per_matnr: number;
  orphan_4255: number;
  decode_fallback_used: number;
  invalid_stock_value: number;
}

export interface FeeConfigExtended {
  feeDrev: number;
  feeMkt: number;
  shippingCost: number;
  mediaworldPreparationDays: number;
  epricePreparationDays: number;
  mediaworldIncludeEu: boolean;
  mediaworldItPreparationDays: number;
  mediaworldEuPreparationDays: number;
  epriceIncludeEu: boolean;
  epriceItPreparationDays: number;
  epriceEuPreparationDays: number;
}

// ============================================================
// CONSTANTS
// ============================================================

export const LOCATION_ID_IT = 4242;
export const LOCATION_ID_EU = 4254;
export const LOCATION_ID_EU_DUPLICATE = 4255; // Ignored in calculations

export const STOCK_LOCATION_REGEX = /^790813_StockFile_(\d{8})\.txt$/;

// Stock location storage keys relative to bucket
export const STOCK_LOCATION_LATEST_KEY = 'stock-location/latest.txt';
export const STOCK_LOCATION_RUNS_PREFIX = 'stock-location/runs/';
export const STOCK_LOCATION_MANUAL_PREFIX = 'stock-location/manual/';

// ============================================================
// RESOLVE MARKETPLACE STOCK - PURE FUNCTION
// ============================================================

/**
 * Calculates export quantity, lead days, and export eligibility based on IT/EU stock split.
 * This is a PURE FUNCTION that must produce identical results on client and server.
 * 
 * Rules:
 * - If includeEU is false: use only stockIT
 * - If includeEU is true: prefer IT if >= 2, otherwise fallback to IT+EU combined
 * - Minimum threshold: exportQty >= 2 for shouldExport to be true
 * 
 * @param stockIT Stock quantity in IT warehouse
 * @param stockEU Stock quantity in EU warehouse
 * @param includeEU Whether EU fallback is enabled
 * @param daysIT Lead time in days for IT shipment
 * @param daysEU Lead time in days for EU shipment
 * @returns Object with exportQty, leadDays, shouldExport, and source
 */
export function resolveMarketplaceStock(
  stockIT: number,
  stockEU: number,
  includeEU: boolean,
  daysIT: number,
  daysEU: number
): ResolveMarketplaceStockResult {
  // Case 1: EU not included - use IT only
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

  // Case 2: EU included
  // First check if IT alone is sufficient
  if (stockIT >= 2) {
    return {
      exportQty: stockIT,
      leadDays: daysIT,
      shouldExport: true,
      source: 'IT'
    };
  }

  // IT < 2, try combined IT + EU
  const combined = stockIT + stockEU;
  const shouldExport = combined >= 2;
  
  return {
    exportQty: combined,
    leadDays: shouldExport ? daysEU : 0,
    shouldExport,
    source: shouldExport ? 'EU_FALLBACK' : 'NONE'
  };
}

// ============================================================
// GOLDEN CASES VALIDATION
// ============================================================

interface GoldenCase {
  stockIT: number;
  stockEU: number;
  includeEU: boolean;
  expectedExportQty: number;
  expectedShouldExport: boolean;
  expectedSource: 'IT' | 'EU_FALLBACK' | 'NONE';
}

const GOLDEN_CASES: GoldenCase[] = [
  // includeEU ON
  { stockIT: 2, stockEU: 10, includeEU: true, expectedExportQty: 2, expectedShouldExport: true, expectedSource: 'IT' },
  { stockIT: 1, stockEU: 1, includeEU: true, expectedExportQty: 2, expectedShouldExport: true, expectedSource: 'EU_FALLBACK' },
  { stockIT: 1, stockEU: 0, includeEU: true, expectedExportQty: 1, expectedShouldExport: false, expectedSource: 'NONE' },
  { stockIT: 0, stockEU: 2, includeEU: true, expectedExportQty: 2, expectedShouldExport: true, expectedSource: 'EU_FALLBACK' },
  // includeEU OFF
  { stockIT: 2, stockEU: 0, includeEU: false, expectedExportQty: 2, expectedShouldExport: true, expectedSource: 'IT' },
  { stockIT: 1, stockEU: 10, includeEU: false, expectedExportQty: 1, expectedShouldExport: false, expectedSource: 'NONE' },
];

/**
 * Validates resolveMarketplaceStock against known golden cases.
 * Logs mismatches but does NOT fail the pipeline.
 * 
 * @param logPrefix Prefix for log messages (e.g., "[client]" or "[server]")
 */
export function validateGoldenCases(logPrefix: string): void {
  const daysIT = 2;
  const daysEU = 5;
  
  for (const tc of GOLDEN_CASES) {
    const result = resolveMarketplaceStock(tc.stockIT, tc.stockEU, tc.includeEU, daysIT, daysEU);
    
    const mismatch = 
      result.exportQty !== tc.expectedExportQty ||
      result.shouldExport !== tc.expectedShouldExport ||
      result.source !== tc.expectedSource;
    
    if (mismatch) {
      console.error(`${logPrefix} GOLDEN CASE MISMATCH:`, {
        input: { stockIT: tc.stockIT, stockEU: tc.stockEU, includeEU: tc.includeEU },
        expected: { exportQty: tc.expectedExportQty, shouldExport: tc.expectedShouldExport, source: tc.expectedSource },
        actual: { exportQty: result.exportQty, shouldExport: result.shouldExport, source: result.source }
      });
    }
  }
  
  console.log(`${logPrefix} Golden cases validation completed`);
}

// ============================================================
// STOCK LOCATION FILE PARSING
// ============================================================

/**
 * Creates empty warnings object with all counters at 0
 */
export function createEmptyWarnings(): StockLocationWarnings {
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

/**
 * Parses stock location file content into an index.
 * Handles CRLF/LF normalization and encoding fallback.
 * 
 * File format: CSV with ";" separator
 * Headers: Matnr;Stock;NextDelDate;ManufPartNo;LocationID;Category
 * 
 * @param content Raw file content as string
 * @param warnings Warnings object to increment counters
 * @returns Index mapping Matnr to { stockIT, stockEU, mpnList }
 */
export function parseStockLocationFile(
  content: string,
  warnings: StockLocationWarnings
): StockLocationIndex {
  const index: StockLocationIndex = {};
  
  // Normalize line endings
  const normalized = content.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
  const lines = normalized.split('\n');
  
  if (lines.length < 2) {
    console.warn('[stockLocation] File has fewer than 2 lines, cannot parse');
    return index;
  }
  
  // Parse header to find column indices
  const headerLine = lines[0].trim();
  const headers = headerLine.split(';').map(h => h.trim().toLowerCase());
  
  const matnrIdx = headers.findIndex(h => h === 'matnr');
  const stockIdx = headers.findIndex(h => h === 'stock');
  const locationIdx = headers.findIndex(h => h === 'locationid');
  const mpnIdx = headers.findIndex(h => h === 'manufpartno');
  
  if (matnrIdx === -1 || stockIdx === -1 || locationIdx === -1) {
    console.error('[stockLocation] Missing required headers. Found:', headers);
    return index;
  }
  
  console.log(`[stockLocation] Headers found: Matnr=${matnrIdx}, Stock=${stockIdx}, LocationID=${locationIdx}, ManufPartNo=${mpnIdx}`);
  
  // Track 4255 entries for orphan detection
  const entries4255: Set<string> = new Set();
  const entries4254: Set<string> = new Set();
  
  // Parse data lines
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    
    const values = line.split(';');
    const matnr = values[matnrIdx]?.trim();
    if (!matnr) continue;
    
    // Parse stock value
    const stockRaw = values[stockIdx]?.trim() || '0';
    let stock = parseInt(stockRaw, 10);
    if (isNaN(stock) || !Number.isFinite(stock)) {
      stock = 0;
      warnings.invalid_stock_value++;
    }
    
    // Parse location ID
    const locationRaw = values[locationIdx]?.trim() || '0';
    const locationId = parseInt(locationRaw, 10);
    
    // Get MPN if available
    const mpn = mpnIdx >= 0 ? (values[mpnIdx]?.trim() || '') : '';
    
    // Initialize entry if needed
    if (!index[matnr]) {
      index[matnr] = { stockIT: 0, stockEU: 0, mpnList: new Set() };
    }
    
    // Track MPN for multi_mpn_per_matnr warning
    if (mpn) {
      index[matnr].mpnList.add(mpn);
    }
    
    // Aggregate by location
    if (locationId === LOCATION_ID_IT) {
      index[matnr].stockIT += stock;
    } else if (locationId === LOCATION_ID_EU) {
      index[matnr].stockEU += stock;
      entries4254.add(matnr);
    } else if (locationId === LOCATION_ID_EU_DUPLICATE) {
      // LocationID 4255 is ignored in calculations
      entries4255.add(matnr);
    }
    // Other location IDs are ignored silently
  }
  
  // Check for multi_mpn_per_matnr warnings
  for (const matnr in index) {
    if (index[matnr].mpnList.size > 1) {
      warnings.multi_mpn_per_matnr++;
    }
  }
  
  // Check for orphan_4255 warnings (4255 exists but no 4254)
  for (const matnr of entries4255) {
    if (!entries4254.has(matnr)) {
      warnings.orphan_4255++;
    }
  }
  
  console.log(`[stockLocation] Parsed ${Object.keys(index).length} unique Matnr entries`);
  
  return index;
}

/**
 * Gets stock IT/EU for a Matnr from the index.
 * Returns fallback values if Matnr not found.
 * 
 * @param index Stock location index
 * @param matnr Product Matnr
 * @param existingStock Original ExistingStock value (used for fallback)
 * @param warnings Warnings object to increment if data missing
 * @param useFallback If true and matnr not found, use existingStock as IT
 * @returns StockIT and StockEU values
 */
export function getStockForMatnr(
  index: StockLocationIndex | null,
  matnr: string,
  existingStock: number,
  warnings: StockLocationWarnings,
  useFallback: boolean = false
): StockLocationResult {
  // If no index (file missing or failed), use fallback
  if (!index) {
    return { stockIT: existingStock, stockEU: 0 };
  }
  
  const entry = index[matnr];
  if (entry) {
    return { stockIT: entry.stockIT, stockEU: entry.stockEU };
  }
  
  // Matnr not in location file
  if (useFallback) {
    warnings.missing_location_data++;
    return { stockIT: 0, stockEU: 0 };
  }
  
  // Strict mode: return zeros
  warnings.missing_location_data++;
  return { stockIT: 0, stockEU: 0 };
}

/**
 * Checks for split mismatch between ExistingStock and IT+EU sum.
 * 
 * @param stockIT Stock in IT warehouse
 * @param stockEU Stock in EU warehouse  
 * @param existingStock Total stock from main stock file
 * @param warnings Warnings object to increment if mismatch
 */
export function checkSplitMismatch(
  stockIT: number,
  stockEU: number,
  existingStock: number,
  warnings: StockLocationWarnings
): void {
  const sum = stockIT + stockEU;
  if (sum !== existingStock) {
    warnings.split_mismatch++;
  }
}

/**
 * Extracts date from stock location filename.
 * 
 * @param filename Filename like "790813_StockFile_20251211.txt"
 * @returns Date string "20251211" or null if not matching
 */
export function extractDateFromFilename(filename: string): string | null {
  const match = filename.match(STOCK_LOCATION_REGEX);
  return match ? match[1] : null;
}

/**
 * Compares two stock location filenames for sorting.
 * Sorts by date descending, then by full filename descending.
 * 
 * @param a First filename
 * @param b Second filename
 * @returns Comparison result for sort (negative = a first)
 */
export function compareStockLocationFiles(a: string, b: string): number {
  const dateA = extractDateFromFilename(a);
  const dateB = extractDateFromFilename(b);
  
  // Both have valid dates - compare dates descending
  if (dateA && dateB) {
    if (dateA !== dateB) {
      return dateB.localeCompare(dateA); // Descending
    }
  }
  
  // Same date or one/both invalid - compare full filename descending
  return b.localeCompare(a);
}

/**
 * Decodes file content with UTF-8, falling back to Windows-1252 if needed.
 * 
 * @param data Raw file bytes
 * @param warnings Warnings object to increment if fallback used
 * @returns Decoded string
 */
export function decodeFileContent(data: Uint8Array, warnings: StockLocationWarnings): string {
  try {
    // Try UTF-8 first (strict mode)
    const decoder = new TextDecoder('utf-8', { fatal: true });
    return decoder.decode(data);
  } catch {
    // Fallback to Windows-1252
    warnings.decode_fallback_used++;
    const decoder = new TextDecoder('windows-1252');
    return decoder.decode(data);
  }
}
