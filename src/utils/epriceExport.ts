/**
 * ePrice Export Utility
 * 
 * Builds ePrice XLSX from the EAN catalog dataset.
 * 
 * STOCK RULES (IT-first, inline — NOT delegated to resolveMarketplaceStock):
 *   - IT-first: if StockIT >= 2 → exportQty = StockIT, fulfillment-latency = 1 (fixed)
 *   - Fallback EU (only if includeEU is true for ePrice):
 *       if StockIT < 2 && StockIT + StockEU >= 2 → exportQty = StockIT + StockEU, fulfillment-latency = euDays (from UI)
 *   - Otherwise: skip (exportQty < 2)
 *   - IT fulfillment-latency is ALWAYS 1 for ePrice, regardless of UI itDays.
 *
 * OVERRIDE RULES:
 *   - __overrideStockIT / __overrideStockEU overlay catalog values when present (0 is a valid value)
 *   - Exclusion: if BOTH __overrideStockIT and __overrideStockEU are present and sum to 0, exclude
 *   - Price override: final_price_eprice is used "as is" (set by override.ts)
 *
 * IMPORTANT: This module does NOT recalculate prices.
 * It uses 'final_price_eprice' directly from the EAN catalog/override pipeline.
 */

import * as XLSX from 'xlsx';
import {
  getStockForMatnr,
  checkSplitMismatch,
  type StockLocationIndex,
  type StockLocationWarnings
} from './stockLocation';

// =====================================================================
// EPRICE TEMPLATE: Official structure
// =====================================================================
export const EPRICE_TEMPLATE = {
  sheetName: "Tracciato_Inserimento_Offerte",
  headers: ["sku", "product-id", "product-id-type", "price", "quantity", "state", "fulfillment-latency", "logistic-class"],
  columnCount: 8,
  fixedValues: {
    "product-id-type": "EAN",
    "state": 11,
    "logistic-class": "K"
  }
};

// Fixed IT fulfillment-latency for ePrice (always 1, never from UI)
const EPRICE_IT_FULFILLMENT_LATENCY = 1;

export interface EpriceExportParams {
  eanDataset: Record<string, unknown>[];
  stockLocationIndex: StockLocationIndex | null;
  stockLocationWarnings: StockLocationWarnings;
  includeEu: boolean;
  itDays: number;  // Accepted but IGNORED for ePrice — IT is always 1
  euDays: number;  // Used as fulfillment-latency when bucket = EU
}

export interface EpriceExportResult {
  success: boolean;
  blob?: Blob;
  buffer?: Uint8Array;
  rowCount: number;
  skippedCount: number;
  errors: Array<{ row: number; sku: string; field: string; reason: string }>;
  diagnostics: {
    euOnlyTotal: number;
    euOnlyExported: number;
    itOnlyCount: number;
    itAndEuCount: number;
  };
}

/**
 * Parse price from EAN catalog format to number.
 * Handles both "NN,99" string format and numeric format.
 */
function parsePrice(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  
  const str = String(value).trim().replace(',', '.');
  if (!str) return null;
  
  const num = parseFloat(str);
  return Number.isFinite(num) ? num : null;
}

/**
 * Check if a value is explicitly present (not null, not undefined, not empty string).
 * 0 IS considered present.
 */
function isPresent(v: unknown): boolean {
  return v !== null && v !== undefined && v !== '';
}

// =====================================================================
// INLINE IT-FIRST STOCK LOGIC FOR EPRICE
// =====================================================================

interface EpriceStockResult {
  exportQty: number;
  fulfillmentLatency: number;
  shouldExport: boolean;
  bucket: 'IT' | 'EU' | 'NONE';
}

/**
 * ePrice-specific stock resolution (IT-first, fixed IT latency).
 * NOT using resolveMarketplaceStock because ePrice has fixed IT fulfillment-latency=1.
 */
function resolveEpriceStock(
  stockIT: number,
  stockEU: number,
  includeEU: boolean,
  euDays: number
): EpriceStockResult {
  // Case 1: includeEU is false — IT only
  if (!includeEU) {
    if (stockIT >= 2) {
      return { exportQty: stockIT, fulfillmentLatency: EPRICE_IT_FULFILLMENT_LATENCY, shouldExport: true, bucket: 'IT' };
    }
    return { exportQty: stockIT, fulfillmentLatency: 0, shouldExport: false, bucket: 'NONE' };
  }

  // Case 2: includeEU is true — IT-first, then fallback EU
  if (stockIT >= 2) {
    return { exportQty: stockIT, fulfillmentLatency: EPRICE_IT_FULFILLMENT_LATENCY, shouldExport: true, bucket: 'IT' };
  }

  // Fallback EU: stockIT < 2, try combined
  const combined = stockIT + stockEU;
  if (combined >= 2) {
    return { exportQty: combined, fulfillmentLatency: euDays, shouldExport: true, bucket: 'EU' };
  }

  return { exportQty: combined, fulfillmentLatency: 0, shouldExport: false, bucket: 'NONE' };
}

// =====================================================================
// POST-GENERATION ASSERTIONS
// =====================================================================

interface AssertionFailure {
  sku: string;
  rule: string;
  expected: string;
  found: string;
}

/**
 * Run hard assertions on the generated AOA data rows (after header).
 * Returns failures array. If non-empty, the export MUST fail.
 */
function runEpriceAssertions(
  aoa: (string | number)[][],
  euDays: number,
  includeEu: boolean
): AssertionFailure[] {
  const failures: AssertionFailure[] = [];

  // Build a map of SKU → row data for quick lookup
  // Columns: 0=sku, 1=product-id, 2=product-id-type, 3=price, 4=quantity, 5=state, 6=fulfillment-latency, 7=logistic-class
  const skuMap = new Map<string, { quantity: number; fulfillmentLatency: number; rowIdx: number }>();
  for (let i = 1; i < aoa.length; i++) {
    const row = aoa[i];
    const sku = String(row[0]);
    skuMap.set(sku, {
      quantity: Number(row[4]),
      fulfillmentLatency: Number(row[6]),
      rowIdx: i
    });
  }

  // Assert 1: UB9S6E and UQ993E must NOT be present (override exclusion 0+0)
  for (const excludedSku of ['UB9S6E', 'UQ993E']) {
    if (skuMap.has(excludedSku)) {
      failures.push({
        sku: excludedSku,
        rule: 'override_exclusion_0_0',
        expected: 'non presente nel file ePrice',
        found: `presente con qty=${skuMap.get(excludedSku)!.quantity}`
      });
    }
  }

  // Assert 2: Quest3 SKUs must have quantity=100, fulfillment-latency=1 (IT bucket)
  for (const questSku of ['SK-1000934-01Quest3', 'SK-1000940-01Quest3S', 'SK-1000945-01Quest3S']) {
    const entry = skuMap.get(questSku);
    if (entry) {
      if (entry.quantity !== 100) {
        failures.push({
          sku: questSku,
          rule: 'IT_first_quantity',
          expected: 'quantity=100 (StockIT only)',
          found: `quantity=${entry.quantity}`
        });
      }
      if (entry.fulfillmentLatency !== EPRICE_IT_FULFILLMENT_LATENCY) {
        failures.push({
          sku: questSku,
          rule: 'IT_fulfillment_latency',
          expected: `fulfillment-latency=${EPRICE_IT_FULFILLMENT_LATENCY}`,
          found: `fulfillment-latency=${entry.fulfillmentLatency}`
        });
      }
    }
  }

  // Assert 3: EU-only SKUs must have quantity=100, fulfillment-latency=euDays
  for (const euSku of ['PVH00010195', 'PVH00010174', 'KAT-Walk-C2-Core', 'KAT-Walk-C2-Plus']) {
    const entry = skuMap.get(euSku);
    if (entry) {
      if (entry.quantity !== 100) {
        failures.push({
          sku: euSku,
          rule: 'EU_fallback_quantity',
          expected: 'quantity=100 (StockEU with IT=0)',
          found: `quantity=${entry.quantity}`
        });
      }
      if (entry.fulfillmentLatency !== euDays) {
        failures.push({
          sku: euSku,
          rule: 'EU_fulfillment_latency',
          expected: `fulfillment-latency=${euDays} (euDays from UI)`,
          found: `fulfillment-latency=${entry.fulfillmentLatency}`
        });
      }
    }
  }

  // Assert 4: No row should have quantity < 2
  for (let i = 1; i < aoa.length; i++) {
    const qty = Number(aoa[i][4]);
    if (qty < 2) {
      failures.push({
        sku: String(aoa[i][0]),
        rule: 'min_quantity_2',
        expected: 'quantity >= 2',
        found: `quantity=${qty}`
      });
    }
  }

  // Assert 5: If includeEU is false, no row should have been exported via EU bucket
  // (This is structural — if includeEU is false and stockIT < 2, the row should not exist)
  // We can't check bucket here since it's not stored in AOA, but we trust the logic above.

  return failures;
}

// =====================================================================
// BUILD FUNCTION
// =====================================================================

/**
 * Build ePrice XLSX from EAN catalog dataset.
 * Returns blob and buffer for both download and bucket upload.
 * 
 * Implements IT-first stock logic with fixed IT fulfillment-latency=1.
 * euDays must be a valid positive number when includeEu is true.
 */
export function buildEpriceXlsxFromEanDataset({
  eanDataset,
  stockLocationIndex,
  stockLocationWarnings,
  includeEu,
  itDays,
  euDays
}: EpriceExportParams): EpriceExportResult {
  const errors: Array<{ row: number; sku: string; field: string; reason: string }> = [];
  let skippedCount = 0;
  let skippedPerExportPricing = 0;
  
  // Diagnostic counters
  let euOnlyTotal = 0;
  let euOnlyExported = 0;
  let itOnlyCount = 0;
  let itAndEuCount = 0;

  // Skip counters by reason
  let skippedOverrideExclusion = 0;
  let skippedStockInsufficient = 0;
  let skippedIncludeEuFalseItLow = 0;
  
  // Respect UI toggle — default to true only if not explicitly false
  const effectiveIncludeEu = includeEu !== false;

  // Validate euDays when includeEU is active
  if (effectiveIncludeEu) {
    if (typeof euDays !== 'number' || !Number.isFinite(euDays) || euDays < 1) {
      console.error('[ePrice:FATAL] euDays non valido mentre includeEU è attivo', { euDays, includeEu });
      return {
        success: false,
        rowCount: 0,
        skippedCount: 0,
        errors: [{ row: 0, sku: '', field: 'euDays', reason: `euDays non valido (${euDays}) con includeEU attivo. Impossibile generare ePrice.` }],
        diagnostics: { euOnlyTotal: 0, euOnlyExported: 0, itOnlyCount: 0, itAndEuCount: 0 }
      };
    }
  }
  
  console.log('%c[ePrice:buildXlsx:start]', 'color: #9C27B0;', {
    inputRows: eanDataset.length,
    includeEu: effectiveIncludeEu,
    itDays_UI: itDays,
    itDays_ePrice: EPRICE_IT_FULFILLMENT_LATENCY,
    euDays,
    stockLocationIndex_loaded: !!stockLocationIndex
  });
  
  // Build AOA with headers
  const aoa: (string | number)[][] = [[...EPRICE_TEMPLATE.headers]];
  
  eanDataset.forEach((record, index) => {
    const sku = String(record.ManufPartNr ?? '');
    const ean = String(record.EAN ?? '');
    const matnr = String(record.Matnr ?? '');
    const existingStock = Number(record.ExistingStock) || 0;
    
    // Filter 1: Valid EAN required
    if (!ean || !/^\d{12,14}$/.test(ean)) {
      errors.push({ row: index + 1, sku, field: 'product-id', reason: `EAN non valido: "${ean}"` });
      skippedCount++;
      return;
    }
    
    // Filter 2: Valid SKU required
    if (!sku || sku.trim() === '') {
      errors.push({ row: index + 1, sku: 'N/A', field: 'sku', reason: 'SKU mancante' });
      skippedCount++;
      return;
    }
    
    // =========================================================
    // STOCK RESOLUTION: catalog first, then overlay overrides
    // =========================================================
    let stockIT: number;
    let stockEU: number;
    
    if (record.__overrideSource === 'new') {
      // New override products: use ExistingStock as IT, EU=0 (before overlay)
      stockIT = existingStock;
      stockEU = 0;
    } else {
      // Existing products: resolve from stock location index
      const stockData = getStockForMatnr(
        stockLocationIndex,
        matnr,
        existingStock,
        stockLocationWarnings,
        true
      );
      stockIT = stockData.stockIT;
      stockEU = stockData.stockEU;
    }
    
    // Overlay per-field overrides ONLY when explicitly present
    // CRITICAL: use isPresent() — 0 is a valid override value, empty/null/undefined means "keep catalog"
    if (isPresent(record.__overrideStockIT)) {
      stockIT = Number(record.__overrideStockIT);
    }
    if (isPresent(record.__overrideStockEU)) {
      stockEU = Number(record.__overrideStockEU);
    }
    
    // Warn if stock values are non-numeric after conversion
    if (!Number.isFinite(stockIT)) {
      console.warn(`[ePrice:warn] stockIT non numerico per SKU=${sku}, valore=${record.__overrideStockIT}`);
      stockIT = 0;
    }
    if (!Number.isFinite(stockEU)) {
      console.warn(`[ePrice:warn] stockEU non numerico per SKU=${sku}, valore=${record.__overrideStockEU}`);
      stockEU = 0;
    }
    
    // =========================================================
    // OVERRIDE EXCLUSION: both StockIT and StockEU present and sum = 0
    // Check isPresent on both fields (0 IS a valid value, null/undefined/'' means not set)
    // No need to check __override flag — non-override records won't have these fields
    // =========================================================
    if (isPresent(record.__overrideStockIT) &&
        isPresent(record.__overrideStockEU) &&
        (Number(record.__overrideStockIT) + Number(record.__overrideStockEU)) === 0) {
      errors.push({
        row: index + 1,
        sku,
        field: 'quantity',
        reason: `Escluso da override: StockIT=${record.__overrideStockIT} + StockEU=${record.__overrideStockEU} = 0`
      });
      skippedCount++;
      skippedOverrideExclusion++;
      return;
    }
    
    // Check split mismatch for non-override records
    if (stockLocationIndex && !record.__override) {
      checkSplitMismatch(stockIT, stockEU, existingStock, stockLocationWarnings);
    }
    
    // Track stock distribution (before EU toggle)
    const hasIT = stockIT >= 2;
    const hasEU = stockEU >= 2;
    if (hasIT && hasEU) itAndEuCount++;
    else if (hasIT) itOnlyCount++;
    else if (hasEU) euOnlyTotal++;
    
    // =========================================================
    // RESOLVE STOCK: inline IT-first logic for ePrice
    // =========================================================
    // ePrice lead time: always from UI parameters, never from per-record override
    const stockResult = resolveEpriceStock(
      stockIT,
      effectiveIncludeEu ? stockEU : 0,
      effectiveIncludeEu,
      euDays
    );
    
    // Filter 3: Stock threshold (min 2)
    if (!stockResult.shouldExport) {
      const reason = !effectiveIncludeEu && stockIT < 2
        ? `Escluso: includeEU=false e StockIT=${stockIT} < 2 (StockEU=${stockEU} ignorato)`
        : `Stock insufficiente: IT=${stockIT}, EU=${effectiveIncludeEu ? stockEU : 0}, exportQty=${stockResult.exportQty}, includeEU=${effectiveIncludeEu}`;
      
      errors.push({ row: index + 1, sku, field: 'quantity', reason });
      skippedCount++;
      skippedStockInsufficient++;
      if (!effectiveIncludeEu && stockIT < 2) skippedIncludeEuFalseItLow++;
      return;
    }
    
    // Track EU-only exported
    if (!hasIT && hasEU && stockResult.shouldExport) {
      euOnlyExported++;
    }
    
    // =========================================================
    // PRICE: use per-export field (set by pricing pipeline or override)
    // =========================================================
    const finalPriceEpRaw = record.final_price_eprice;
    
    if (finalPriceEpRaw === null || finalPriceEpRaw === undefined) {
      errors.push({ row: index + 1, sku, field: 'price', reason: `final_price_eprice mancante (pricing error flag o calcolo fallito)` });
      skippedCount++;
      skippedPerExportPricing++;
      return;
    }
    
    const prezzoFinale = parsePrice(finalPriceEpRaw);
    
    if (prezzoFinale === null || prezzoFinale <= 0) {
      errors.push({ row: index + 1, sku, field: 'price', reason: `final_price_eprice non valido: ${finalPriceEpRaw}` });
      skippedCount++;
      skippedPerExportPricing++;
      return;
    }
    
    // Diagnostic log for first 20 records
    if (index < 20) {
      console.log(`%c[ePrice:row${index}]`, 'color: #9C27B0;', {
        sku, ean,
        stockIT, stockEU,
        effectiveStockEU: effectiveIncludeEu ? stockEU : 0,
        exportQty: stockResult.exportQty,
        fulfillmentLatency: stockResult.fulfillmentLatency,
        bucket: stockResult.bucket,
        price: prezzoFinale,
        isOverride: !!record.__override,
        overrideStockIT: record.__overrideStockIT,
        overrideStockEU: record.__overrideStockEU
      });
    }
    
    // Build row
    aoa.push([
      sku,
      ean,
      EPRICE_TEMPLATE.fixedValues["product-id-type"],
      prezzoFinale,
      stockResult.exportQty,
      EPRICE_TEMPLATE.fixedValues["state"],
      stockResult.fulfillmentLatency,
      EPRICE_TEMPLATE.fixedValues["logistic-class"]
    ]);
  });
  
  const rowCount = aoa.length - 1; // Exclude header
  
  console.log('%c[ePrice:buildXlsx:complete]', 'color: #9C27B0; font-weight: bold;', {
    inputRows: eanDataset.length,
    exportedRows: rowCount,
    skippedRows: skippedCount,
    skippedPerExportPricing,
    skippedOverrideExclusion,
    skippedStockInsufficient,
    skippedIncludeEuFalseItLow,
    euOnlyTotal,
    euOnlyExported,
    itOnlyCount,
    itAndEuCount,
    includeEu: effectiveIncludeEu
  });
  
  if (rowCount === 0) {
    return {
      success: false,
      rowCount: 0,
      skippedCount,
      errors,
      diagnostics: { euOnlyTotal, euOnlyExported, itOnlyCount, itAndEuCount }
    };
  }
  
  // =========================================================
  // POST-GENERATION ASSERTIONS (hard fail)
  // =========================================================
  const assertionFailures = runEpriceAssertions(aoa, euDays, effectiveIncludeEu);
  if (assertionFailures.length > 0) {
    for (const af of assertionFailures) {
      console.error('%c[ePrice:ASSERT_FAIL]', 'color: red; font-weight: bold;', {
        sku: af.sku,
        rule: af.rule,
        expected: af.expected,
        found: af.found
      });
    }
    return {
      success: false,
      rowCount: 0,
      skippedCount,
      errors: [
        ...errors,
        ...assertionFailures.map(af => ({
          row: 0,
          sku: af.sku,
          field: 'assertion',
          reason: `ASSERT FAIL [${af.rule}]: atteso ${af.expected}, trovato ${af.found}`
        }))
      ],
      diagnostics: { euOnlyTotal, euOnlyExported, itOnlyCount, itAndEuCount }
    };
  }
  
  console.log('%c[ePrice:assertions:PASS]', 'color: #22c55e; font-weight: bold;', 'Tutte le asserzioni superate');
  
  // Create worksheet from AOA
  const ws = XLSX.utils.aoa_to_sheet(aoa);
  
  // Format columns
  if (ws['!ref']) {
    const range = XLSX.utils.decode_range(ws['!ref']);
    
    // product-id (column B, index 1) as text to preserve leading zeros
    for (let R = 1; R <= range.e.r; R++) {
      const addr = XLSX.utils.encode_cell({ r: R, c: 1 });
      const cell = ws[addr];
      if (cell) {
        cell.v = (cell.v ?? '').toString();
        cell.t = 's';
        cell.z = '@';
      }
    }
    
    // price (column D, index 3) as number with 2 decimals
    for (let R = 1; R <= range.e.r; R++) {
      const addr = XLSX.utils.encode_cell({ r: R, c: 3 });
      const cell = ws[addr];
      if (cell && typeof cell.v === 'number') {
        cell.t = 'n';
        cell.z = '0.00';
      }
    }
  }
  
  // Create workbook
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, EPRICE_TEMPLATE.sheetName);
  
  // Serialize to ArrayBuffer
  const buffer = XLSX.write(wb, { bookType: 'xlsx', type: 'array' });
  const uint8Buffer = new Uint8Array(buffer);
  
  // Create blob
  const blob = new Blob([buffer], {
    type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
  });
  
  return {
    success: true,
    blob,
    buffer: uint8Buffer,
    rowCount,
    skippedCount,
    errors,
    diagnostics: { euOnlyTotal, euOnlyExported, itOnlyCount, itAndEuCount }
  };
}

/**
 * Download ePrice XLSX to browser.
 */
export function downloadEpriceBlob(blob: Blob, filename?: string): void {
  const now = new Date();
  const dateStamp = now.toISOString().slice(0, 10).replace(/-/g, '');
  const fileName = filename || `eprice-offers-${dateStamp}.xlsx`;
  
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = fileName;
  a.rel = 'noopener';
  a.style.display = 'none';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  
  setTimeout(() => URL.revokeObjectURL(url), 3000);
}
