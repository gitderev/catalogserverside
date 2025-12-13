/**
 * ePrice Export Utility
 * 
 * Builds ePrice XLSX from the EAN catalog dataset.
 * Uses resolveMarketplaceStock for IT/EU stock logic.
 * 
 * IMPORTANT: This module does NOT recalculate prices.
 * It uses the 'Prezzo Finale' directly from the EAN catalog.
 */

import * as XLSX from 'xlsx';
import {
  resolveMarketplaceStock,
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

export interface EpriceExportParams {
  eanDataset: any[];
  stockLocationIndex: StockLocationIndex | null;
  stockLocationWarnings: StockLocationWarnings;
  includeEu: boolean;
  itDays: number;
  euDays: number;
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
 * Build ePrice XLSX from EAN catalog dataset.
 * Returns blob and buffer for both download and bucket upload.
 * 
 * Uses resolveMarketplaceStock for quantity and leadDays.
 * includeEu defaults to true if not explicitly set to false.
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
  
  // Diagnostic counters
  let euOnlyTotal = 0;
  let euOnlyExported = 0;
  let itOnlyCount = 0;
  let itAndEuCount = 0;
  
  // Ensure includeEu defaults to true
  const effectiveIncludeEu = includeEu === false ? false : true;
  
  console.log('%c[ePrice:buildXlsx:start]', 'color: #9C27B0;', {
    inputRows: eanDataset.length,
    includeEu: effectiveIncludeEu,
    itDays,
    euDays,
    stockLocationIndex_loaded: !!stockLocationIndex
  });
  
  // Build AOA with headers
  const aoa: (string | number)[][] = [[...EPRICE_TEMPLATE.headers]];
  
  eanDataset.forEach((record, index) => {
    const sku = record.ManufPartNr || '';
    const ean = record.EAN || '';
    const matnr = record.Matnr || '';
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
    
    // OVERRIDE STOCK PRIORITY:
    // 1. If __overrideStockIT or __overrideStockEU is non-null, use those (missing side = 0)
    // 2. Else if __overrideSource === 'new', use ExistingStock as IT, EU = 0
    // 3. Else use getStockForMatnr
    let stockIT: number;
    let stockEU: number;
    
    if (record.__overrideStockIT != null || record.__overrideStockEU != null) {
      // Override provides explicit stock values
      stockIT = record.__overrideStockIT != null ? Number(record.__overrideStockIT) : 0;
      stockEU = record.__overrideStockEU != null ? Number(record.__overrideStockEU) : 0;
    } else if (record.__overrideSource === 'new') {
      // New override product without explicit stock: use ExistingStock as IT-only
      stockIT = existingStock;
      stockEU = 0;
    } else {
      // Standard catalog product: use getStockForMatnr
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
    
    // Apply includeEU rule: if false, treat EU as 0
    const effectiveStockEU = effectiveIncludeEu ? stockEU : 0;
    
    // Check split mismatch for non-override records
    if (stockLocationIndex && !record.__override) {
      checkSplitMismatch(stockIT, stockEU, existingStock, stockLocationWarnings);
    }
    
    // Track stock distribution
    const hasIT = stockIT >= 2;
    const hasEU = effectiveStockEU >= 2;
    if (hasIT && hasEU) itAndEuCount++;
    else if (hasIT) itOnlyCount++;
    else if (hasEU) euOnlyTotal++;
    
    // OVERRIDE LEAD DAYS PRIORITY:
    // Use per-record override if provided, else use global UI days
    const itDaysEff = record.__overrideLeadDaysIT != null ? Number(record.__overrideLeadDaysIT) : itDays;
    const euDaysEff = effectiveIncludeEu && record.__overrideLeadDaysEU != null 
      ? Number(record.__overrideLeadDaysEU) 
      : euDays;
    
    // Use resolveMarketplaceStock for quantity and lead time
    const stockResult = resolveMarketplaceStock(
      stockIT,
      effectiveStockEU,
      effectiveIncludeEu,
      itDaysEff,
      euDaysEff
    );
    
    // Filter 3: Stock threshold (min 2)
    if (!stockResult.shouldExport) {
      errors.push({ 
        row: index + 1, 
        sku, 
        field: 'quantity', 
        reason: `Stock insufficiente: IT=${stockIT}, EU=${effectiveStockEU}, includeEU=${effectiveIncludeEu}` 
      });
      skippedCount++;
      return;
    }
    
    // Track EU-only exported
    if (!hasIT && hasEU && stockResult.shouldExport) {
      euOnlyExported++;
    }
    
    // Get price from EAN catalog (NO recalculation)
    const prezzoFinale = parsePrice(record['Prezzo Finale']);
    
    if (prezzoFinale === null || prezzoFinale <= 0) {
      errors.push({ row: index + 1, sku, field: 'price', reason: `Prezzo Finale non valido: ${record['Prezzo Finale']}` });
      skippedCount++;
      return;
    }
    
    // Log first 20 records for diagnostics
    if (index < 20) {
      console.log(`%c[ePrice:row${index}]`, 'color: #9C27B0;', {
        sku, ean, stockIT, stockEU: effectiveStockEU,
        exportQty: stockResult.exportQty,
        leadDays: stockResult.leadDays,
        source: stockResult.source,
        price: prezzoFinale,
        isOverride: !!record.__override
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
      stockResult.leadDays,
      EPRICE_TEMPLATE.fixedValues["logistic-class"]
    ]);
  });
  
  const rowCount = aoa.length - 1; // Exclude header
  
  console.log('%c[ePrice:buildXlsx:complete]', 'color: #9C27B0; font-weight: bold;', {
    inputRows: eanDataset.length,
    exportedRows: rowCount,
    skippedRows: skippedCount,
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
