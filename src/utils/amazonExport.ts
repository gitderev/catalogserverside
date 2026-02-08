/**
 * Amazon Export Utility
 *
 * Generates two files atomically:
 * A) ListingLoader XLSM (catalog with VBA preserved from template if present)
 * B) Price/Inventory TXT (tab-delimited)
 *
 * Uses eanCatalogDataset as the single source of truth.
 * Calculates Amazon-specific pricing using existing pricing utilities.
 * Uses resolveMarketplaceStock for IT/EU stock logic (includeEU always true).
 *
 * OVERRIDE: Override rows use price "as is" without fee/IVA/,99 recalculation,
 * same logic as ePrice and MediaWorld exports.
 *
 * IMPORTANT: This module does NOT modify any existing export logic.
 */

import * as XLSX from 'xlsx';
import { normalizeEAN } from './ean';
import {
  toCents,
  applyRateCents,
  toComma99Cents,
  validateEnding99Cents,
  formatCents
} from './pricing';
import {
  resolveMarketplaceStock,
  getStockForMatnr,
  checkSplitMismatch,
  type StockLocationIndex,
  type StockLocationWarnings
} from './stockLocation';

// =====================================================================
// TYPES
// =====================================================================

export interface AmazonExportParams {
  eanDataset: any[];
  stockLocationIndex: StockLocationIndex | null;
  stockLocationWarnings: StockLocationWarnings;
  includeEu: boolean;
  itDays: number;
  euDays: number;
  feeDrev: number;
  feeMkt: number;
  shippingCost: number;
  onProgress?: (pct: number, label: string) => void;
}

export interface AmazonDiscardedRow {
  SKU: string;
  EAN: string;
  reason: string;
  quantityResult: number | null;
  leadDaysResult: number | null;
  prezzoAmazon: string | null;
}

export interface AmazonExportResult {
  success: boolean;
  xlsmBlob?: Blob;
  txtBlob?: Blob;
  discardedBlob?: Blob;
  rowCount: number;
  discardedCount: number;
  discardedRows: AmazonDiscardedRow[];
  reasonCounts: Map<string, number>;
  diagnostics: {
    totalInput: number;
    exported: number;
    discarded: number;
    xlsmRows: number;
    txtRows: number;
  };
  error?: string;
}

// =====================================================================
// COLUMN INDICES for ListingLoader XLSM "Modello" sheet
// =====================================================================
const COL = {
  A: 0,   // SKU
  B: 1,   // Tipo di ID esterna del prodotto
  C: 2,   // ID esterna del prodotto (EAN)
  H: 7,   // Condizione dell'articolo
  AF: 31, // Codice canale di gestione (IT)
  AG: 32, // Quantita (IT)
  AH: 33, // Tempo di gestione (IT)
  AK: 36, // Prezzo EUR (Vendita su Amazon, IT)
  BJ: 61, // Gruppo spedizione venditore (IT)
};

// =====================================================================
// HELPERS
// =====================================================================

function sanitizeSKU(sku: string): string {
  // Remove control characters
  return sku.replace(/[\x00-\x1f\x7f]/g, '');
}

function hardenExcelSKU(sku: string): string {
  const sanitized = sanitizeSKU(sku);
  // Prevent Excel formula injection
  if (/^[=+\-@]/.test(sanitized)) {
    return "'" + sanitized;
  }
  return sanitized;
}

function generateTimestamp(): string {
  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, '0');
  const d = String(now.getDate()).padStart(2, '0');
  const h = String(now.getHours()).padStart(2, '0');
  const min = String(now.getMinutes()).padStart(2, '0');
  return `${y}${m}${d}_${h}${min}`;
}

function resolveTemplateUrl(): string {
  // Use Vite's BASE_URL to handle deploys under subpaths
  let base = '/';
  try {
    base = import.meta.env.BASE_URL || '/';
  } catch {
    // fallback to root
  }
  if (!base.endsWith('/')) base += '/';
  return `${base}amazon/ListingLoader.xlsm`;
}

async function fetchXlsmTemplate(maxRetries: number = 3): Promise<ArrayBuffer> {
  const templateUrl = resolveTemplateUrl();
  const delays = [1000, 3000, 9000];

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const response = await fetch(templateUrl);

      if (!response.ok) {
        console.error('[Amazon:fetch:error]', {
          templateUrl,
          status: response.status,
          contentType: response.headers.get('content-type'),
        });
        throw new Error(`HTTP ${response.status} per ${templateUrl}`);
      }

      // Check content-type header for HTML
      const contentType = response.headers.get('content-type') || '';
      if (contentType.includes('text/html')) {
        console.error('[Amazon:fetch:html-content-type]', {
          templateUrl,
          status: response.status,
          contentType,
        });
        throw new Error(
          `Template non trovato o path errato: ricevuto HTML invece di XLSM (content-type: ${contentType}, url: ${templateUrl})`
        );
      }

      const buffer = await response.arrayBuffer();

      // Sniff first 128 bytes for HTML content (SPA fallback detection)
      const preview = new TextDecoder('ascii', { fatal: false }).decode(
        buffer.slice(0, 128)
      );
      const previewLower = preview.toLowerCase();
      if (previewLower.includes('<html') || previewLower.startsWith('<!doctype')) {
        console.error('[Amazon:fetch:html-sniff]', {
          templateUrl,
          status: response.status,
          contentType,
          bodyPreview: preview,
        });
        throw new Error(
          `Template non trovato o path errato: ricevuto HTML invece di XLSM (url: ${templateUrl})`
        );
      }

      return buffer;
    } catch (err) {
      if (attempt === maxRetries - 1) {
        throw new Error(
          `Fetch template XLSM fallito dopo ${maxRetries} tentativi: ${err instanceof Error ? err.message : String(err)}`
        );
      }
      console.warn(
        `[Amazon:fetch:retry] Tentativo ${attempt + 1}/${maxRetries} fallito, attesa ${delays[attempt]}ms...`,
        err
      );
      await new Promise(resolve => setTimeout(resolve, delays[attempt]));
    }
  }
  throw new Error('Unreachable');
}

function downloadBlob(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.rel = 'noopener';
  a.style.display = 'none';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  setTimeout(() => URL.revokeObjectURL(url), 3000);
}

// =====================================================================
// MAIN EXPORT FUNCTION
// =====================================================================

export async function buildAmazonExport(params: AmazonExportParams): Promise<AmazonExportResult> {
  const {
    eanDataset,
    stockLocationIndex,
    stockLocationWarnings,
    includeEu,
    itDays,
    euDays,
    feeDrev,
    feeMkt,
    shippingCost,
    onProgress
  } = params;

  const discardedRows: AmazonDiscardedRow[] = [];
  const reasonCounts = new Map<string, number>();
  const incReason = (r: string) => reasonCounts.set(r, (reasonCounts.get(r) || 0) + 1);

  // Records that pass all filters
  interface ValidRecord {
    sku: string;
    ean: string;
    quantity: number;
    leadDays: number;
    priceDisplay: string; // "NN,99" format for standard, exact format for overrides
    finalCents: number;
    isOverride: boolean;
  }
  const validRecords: ValidRecord[] = [];

  // Amazon ALWAYS uses EU fallback - hardcoded to prevent the bug where SKUs
  // with sufficient EU stock are incorrectly discarded when IT < 2
  const effectiveIncludeEu = true;
  if (includeEu === false) {
    console.warn('[Amazon:export] includeEu parameter was false but Amazon always uses EU fallback. Forcing includeEu=true.');
  }
  const warnings = stockLocationWarnings;

  console.log('%c[Amazon:export:start]', 'color: #FF6600; font-weight: bold;', {
    inputRows: eanDataset.length,
    includeEu: effectiveIncludeEu,
    itDays,
    euDays,
    feeDrev,
    feeMkt,
    shippingCost
  });

  onProgress?.(5, 'Filtraggio e calcolo prezzi Amazon...');

  // =====================================================================
  // PHASE 1: Filter and compute prices
  // =====================================================================
  for (let i = 0; i < eanDataset.length; i++) {
    const record = eanDataset[i];
    const sku = sanitizeSKU(String(record.ManufPartNr || '').trim());
    const rawEAN = record.EAN ?? '';
    const matnr = String(record.Matnr || '');
    const existingStock = Number(record.ExistingStock) || 0;

    // Yield every 500 rows
    if (i > 0 && i % 500 === 0) {
      await new Promise(r => setTimeout(r, 0));
      onProgress?.(5 + Math.round((i / eanDataset.length) * 40), `Elaborazione riga ${i}/${eanDataset.length}...`);
    }

    // Filter 1: SKU non vuoto
    if (!sku) {
      discardedRows.push({ SKU: '', EAN: rawEAN, reason: 'SKU mancante', quantityResult: null, leadDaysResult: null, prezzoAmazon: null });
      incReason('SKU mancante');
      continue;
    }

    // Filter 2: EAN must be 13 or 14 digits after normalization
    const eanResult = normalizeEAN(rawEAN);
    if (!eanResult.ok || !eanResult.value) {
      discardedRows.push({ SKU: sku, EAN: rawEAN, reason: eanResult.reason || 'EAN non valido', quantityResult: null, leadDaysResult: null, prezzoAmazon: null });
      incReason(eanResult.reason || 'EAN non valido');
      continue;
    }
    if (eanResult.value.length !== 13 && eanResult.value.length !== 14) {
      discardedRows.push({ SKU: sku, EAN: rawEAN, reason: `EAN non valido per Amazon: lunghezza ${eanResult.value.length} (atteso 13 o 14)`, quantityResult: null, leadDaysResult: null, prezzoAmazon: null });
      incReason('EAN lunghezza non valida');
      continue;
    }
    const eanNorm = eanResult.value;

    // Stock resolution: compute catalog stock first, then overlay per-field overrides
    let stockIT: number;
    let stockEU: number;

    if (record.__overrideSource === 'new') {
      stockIT = existingStock;
      stockEU = 0;
    } else {
      const stockData = getStockForMatnr(
        stockLocationIndex,
        matnr,
        existingStock,
        warnings,
        true
      );
      stockIT = stockData.stockIT;
      stockEU = stockData.stockEU;
    }
    // Selectively overlay per-field overrides (missing side keeps catalog value)
    if (record.__overrideStockIT != null) stockIT = Number(record.__overrideStockIT);
    if (record.__overrideStockEU != null) stockEU = Number(record.__overrideStockEU);

    // Override exclusion: both StockIT and StockEU present in override and sum = 0
    if (record.__override && record.__overrideStockIT != null && record.__overrideStockEU != null &&
        (Number(record.__overrideStockIT) + Number(record.__overrideStockEU)) === 0) {
      discardedRows.push({
        SKU: sku,
        EAN: eanNorm,
        reason: `Escluso da override: StockIT=${record.__overrideStockIT} + StockEU=${record.__overrideStockEU} = 0`,
        quantityResult: 0,
        leadDaysResult: 0,
        prezzoAmazon: null
      });
      incReason('Escluso da override (stock 0)');
      continue;
    }

    const effectiveStockEU = effectiveIncludeEu ? stockEU : 0;

    if (stockLocationIndex && !record.__override) {
      checkSplitMismatch(stockIT, stockEU, existingStock, warnings);
    }

    // Override lead days
    const itDaysEff = record.__overrideLeadDaysIT != null ? Number(record.__overrideLeadDaysIT) : itDays;
    const euDaysEff = effectiveIncludeEu && record.__overrideLeadDaysEU != null
      ? Number(record.__overrideLeadDaysEU)
      : euDays;

    const stockResult = resolveMarketplaceStock(
      stockIT,
      effectiveStockEU,
      effectiveIncludeEu,
      itDaysEff,
      euDaysEff
    );

    // Filter 3: shouldExport and quantity >= 2
    if (!stockResult.shouldExport || stockResult.exportQty < 2) {
      // VALIDATION: Detect stock/includeEU incoherence (safety net)
      if (stockIT < 2 && effectiveStockEU >= 2 && (stockIT + effectiveStockEU) >= 2) {
        const errMsg = `Incoerenza stock/includeEU rilevata: SKU=${sku} ha stockIT=${stockIT}, stockEU=${effectiveStockEU} (combinato=${stockIT + effectiveStockEU} >= 2) ma è stato scartato con exportQty=${stockResult.exportQty}. Bug nel calcolo stock.`;
        console.error('[Amazon:validation:FATAL]', { sku, stockIT, stockEU: effectiveStockEU, exportQty: stockResult.exportQty, leadDays: stockResult.leadDays });
        return {
          success: false,
          rowCount: 0,
          discardedCount: discardedRows.length,
          discardedRows,
          reasonCounts,
          diagnostics: { totalInput: eanDataset.length, exported: 0, discarded: discardedRows.length, xlsmRows: 0, txtRows: 0 },
          error: errMsg
        };
      }
      discardedRows.push({
        SKU: sku,
        EAN: eanNorm,
        reason: `Stock insufficiente: IT=${stockIT}, EU=${effectiveStockEU}, qty=${stockResult.exportQty}`,
        quantityResult: stockResult.exportQty,
        leadDaysResult: stockResult.leadDays,
        prezzoAmazon: null
      });
      incReason('Stock insufficiente');
      continue;
    }

    const quantityResult = stockResult.exportQty;
    const leadDaysResult = stockResult.leadDays;

    // Filter 4: leadDays >= 0
    if (!Number.isInteger(leadDaysResult) || leadDaysResult < 0) {
      discardedRows.push({ SKU: sku, EAN: eanNorm, reason: `Lead days non valido: ${leadDaysResult}`, quantityResult, leadDaysResult, prezzoAmazon: null });
      incReason('Lead days non valido');
      continue;
    }

    // =====================================================================
    // PRICING: Override or Amazon-specific calculation
    // Override rows use price "as is" (same logic as ePrice/MediaWorld)
    // =====================================================================
    let priceDisplay: string;
    let finalCents: number;
    const isOverride = record.__override === true;

    if (isOverride) {
      // Override price: use exactly as provided, no fees/IVA/shipping/rounding/,99
      const rawPrice = record['Prezzo Finale'];
      if (rawPrice === null || rawPrice === undefined || rawPrice === '') {
        discardedRows.push({ SKU: sku, EAN: eanNorm, reason: 'Override senza Prezzo Finale', quantityResult, leadDaysResult, prezzoAmazon: null });
        incReason('Override senza Prezzo Finale');
        continue;
      }
      const parsedPrice = parseFloat(String(rawPrice).replace(',', '.'));
      if (!Number.isFinite(parsedPrice) || parsedPrice <= 0) {
        discardedRows.push({ SKU: sku, EAN: eanNorm, reason: `Override Prezzo Finale non valido: ${rawPrice}`, quantityResult, leadDaysResult, prezzoAmazon: null });
        incReason('Override Prezzo Finale non valido');
        continue;
      }
      finalCents = Math.round(parsedPrice * 100);
      // Amazon prices use dot separator with 2 decimals
      priceDisplay = (finalCents / 100).toFixed(2);
    } else {
      // Standard pricing: Calculate Amazon-specific price in cents
      const hasBest = Number.isFinite(record.CustBestPrice) && record.CustBestPrice > 0;
      const hasListPrice = Number.isFinite(record.ListPrice) && record.ListPrice > 0;
      const surchargeValue = (Number.isFinite(record.Surcharge) && record.Surcharge >= 0) ? record.Surcharge : 0;

      let baseCents = 0;
      if (hasBest) {
        baseCents = Math.round((record.CustBestPrice + surchargeValue) * 100);
      } else if (hasListPrice) {
        baseCents = Math.round(record.ListPrice * 100);
      }

      if (baseCents <= 0) {
        discardedRows.push({ SKU: sku, EAN: eanNorm, reason: 'Prezzo base non disponibile', quantityResult, leadDaysResult, prezzoAmazon: null });
        incReason('Prezzo base non disponibile');
        continue;
      }

      const shippingCents = Math.round(shippingCost * 100);
      const afterShippingCents = baseCents + shippingCents;
      const afterIvaCents = Math.round(afterShippingCents * 1.22);
      const afterFeeDeRevCents = Math.round(afterIvaCents * feeDrev);
      const afterFeesCents = Math.round(afterFeeDeRevCents * feeMkt);
      finalCents = toComma99Cents(afterFeesCents);

      // Assert valid (only for non-override rows)
      if (!validateEnding99Cents(finalCents) || finalCents <= 0) {
        const errMsg = `Prezzo Amazon non valido: finalCents=${finalCents}, ending99=${validateEnding99Cents(finalCents)}`;
        console.error('[Amazon:pricing:FATAL]', { sku, ean: eanNorm, baseCents, afterFeesCents, finalCents });
        return {
          success: false,
          rowCount: 0,
          discardedCount: discardedRows.length,
          discardedRows,
          reasonCounts,
          diagnostics: { totalInput: eanDataset.length, exported: 0, discarded: discardedRows.length, xlsmRows: 0, txtRows: 0 },
          error: errMsg
        };
      }
      // Amazon prices use dot separator with 2 decimals
      priceDisplay = (finalCents / 100).toFixed(2);
    }

    // Log first 10 records
    if (validRecords.length < 10) {
      console.log(`%c[Amazon:row${validRecords.length}]`, 'color: #FF6600;', {
        sku, ean: eanNorm, stockIT, stockEU: effectiveStockEU,
        qty: quantityResult, lead: leadDaysResult,
        finalCents, priceDisplay, isOverride
      });
    }

    validRecords.push({
      sku,
      ean: eanNorm,
      quantity: quantityResult,
      leadDays: leadDaysResult,
      priceDisplay,
      finalCents,
      isOverride
    });
  }

  onProgress?.(50, `Filtrato: ${validRecords.length} validi, ${discardedRows.length} scartati`);

  if (validRecords.length === 0) {
    console.error('[Amazon:export:no_exportable_rows]', {
      step: 'no_exportable_rows',
      totalInput: eanDataset.length,
      discarded: discardedRows.length,
      topReasons: Array.from(reasonCounts.entries()).sort((a, b) => b[1] - a[1]).slice(0, 5)
    });
    return {
      success: false,
      rowCount: 0,
      discardedCount: discardedRows.length,
      discardedRows,
      reasonCounts,
      diagnostics: { totalInput: eanDataset.length, exported: 0, discarded: discardedRows.length, xlsmRows: 0, txtRows: 0 },
      error: 'Nessuna riga esportabile per Amazon'
    };
  }

  // =====================================================================
  // PHASE 2: Build XLSM from template
  // =====================================================================
  onProgress?.(55, 'Caricamento template ListingLoader...');

  let xlsmBuffer: ArrayBuffer;
  try {
    xlsmBuffer = await fetchXlsmTemplate();
  } catch (err) {
    const errMsg = `Template ListingLoader.xlsm non trovato o non accessibile. Aggiungere il file ufficiale Amazon in public/amazon/ListingLoader.xlsm e verificare che sia servito a /amazon/ListingLoader.xlsm. Dettaglio: ${err instanceof Error ? err.message : String(err)}`;
    console.error('[Amazon:template:FATAL]', errMsg);
    return {
      success: false,
      rowCount: 0,
      discardedCount: discardedRows.length,
      discardedRows,
      reasonCounts,
      diagnostics: { totalInput: eanDataset.length, exported: 0, discarded: discardedRows.length, xlsmRows: 0, txtRows: 0 },
      error: errMsg
    };
  }

  onProgress?.(60, 'Scrittura dati nel foglio Modello...');

  let wb: XLSX.WorkBook;
  try {
    wb = XLSX.read(xlsmBuffer, { type: 'array', bookVBA: true });
  } catch (err) {
    return {
      success: false,
      rowCount: 0,
      discardedCount: discardedRows.length,
      discardedRows,
      reasonCounts,
      diagnostics: { totalInput: eanDataset.length, exported: 0, discarded: discardedRows.length, xlsmRows: 0, txtRows: 0 },
      error: `Errore lettura template XLSM: ${err instanceof Error ? err.message : String(err)}`
    };
  }

  // Deterministic VBA flag: preserve macros if present, proceed without if absent
  const hasVBA = Boolean((wb as any).vbaraw && (wb as any).vbaraw.length > 0);
  if (hasVBA) {
    console.log('[Amazon:template] VBA rilevato nel template, macro saranno preservate.');
  } else {
    console.log('[Amazon:template] Template privo di VBA, XLSM sarà generato senza macro.');
  }

  // "Modello" sheet MUST exist in template
  const ws = wb.Sheets['Modello'];
  if (!ws) {
    const errMsg = 'Foglio "Modello" non trovato nel template ListingLoader.xlsm. Template non conforme.';
    console.error('[Amazon:template:FATAL]', errMsg);
    return {
      success: false,
      rowCount: 0,
      discardedCount: discardedRows.length,
      discardedRows,
      reasonCounts,
      diagnostics: { totalInput: eanDataset.length, exported: 0, discarded: discardedRows.length, xlsmRows: 0, txtRows: 0 },
      error: errMsg
    };
  }

  // Clean target columns from row 7 onwards
  if (ws['!ref']) {
    const range = XLSX.utils.decode_range(ws['!ref']);
    const targetCols = [COL.A, COL.B, COL.C, COL.H, COL.AF, COL.AG, COL.AH, COL.AK, COL.BJ];
    for (let R = 6; R <= range.e.r; R++) { // Row 7 = index 6
      for (const C of targetCols) {
        const addr = XLSX.utils.encode_cell({ r: R, c: C });
        delete ws[addr];
      }
    }
  }

  // Write data from row 7 (index 6)
  for (let i = 0; i < validRecords.length; i++) {
    const rec = validRecords[i];
    const R = 6 + i; // Row 7 = index 6

    // Yield every 500 rows
    if (i > 0 && i % 500 === 0) {
      await new Promise(r => setTimeout(r, 0));
      onProgress?.(60 + Math.round((i / validRecords.length) * 15), `Scrittura XLSM riga ${i}/${validRecords.length}...`);
    }

    // A: SKU (with Excel injection hardening)
    ws[XLSX.utils.encode_cell({ r: R, c: COL.A })] = { t: 's', v: hardenExcelSKU(rec.sku) };
    // B: Tipo ID = "EAN"
    ws[XLSX.utils.encode_cell({ r: R, c: COL.B })] = { t: 's', v: 'EAN' };
    // C: EAN 13 digits (no apostrophe)
    ws[XLSX.utils.encode_cell({ r: R, c: COL.C })] = { t: 's', v: rec.ean, z: '@' };
    // H: Condizione = "Nuovo"
    ws[XLSX.utils.encode_cell({ r: R, c: COL.H })] = { t: 's', v: 'Nuovo' };
    // AF: Codice canale = "Default"
    ws[XLSX.utils.encode_cell({ r: R, c: COL.AF })] = { t: 's', v: 'Default' };
    // AG: Quantita
    ws[XLSX.utils.encode_cell({ r: R, c: COL.AG })] = { t: 'n', v: rec.quantity };
    // AH: Tempo di gestione
    ws[XLSX.utils.encode_cell({ r: R, c: COL.AH })] = { t: 'n', v: rec.leadDays };
    // AK: Prezzo (string format "NN,99")
    ws[XLSX.utils.encode_cell({ r: R, c: COL.AK })] = { t: 's', v: rec.priceDisplay };
    // BJ: Gruppo spedizione = "Modello Amazon predefinito"
    ws[XLSX.utils.encode_cell({ r: R, c: COL.BJ })] = { t: 's', v: 'Modello Amazon predefinito' };
  }

  // Update sheet range
  const lastRow = 6 + validRecords.length - 1;
  const maxCol = COL.BJ;
  if (ws['!ref']) {
    const existingRange = XLSX.utils.decode_range(ws['!ref']);
    existingRange.e.r = Math.max(existingRange.e.r, lastRow);
    existingRange.e.c = Math.max(existingRange.e.c, maxCol);
    ws['!ref'] = XLSX.utils.encode_range(existingRange);
  } else {
    ws['!ref'] = XLSX.utils.encode_range({ s: { r: 0, c: 0 }, e: { r: lastRow, c: maxCol } });
  }

  onProgress?.(78, 'Serializzazione XLSM...');

  // Serialize XLSM
  let xlsmOut: ArrayBuffer;
  try {
    xlsmOut = XLSX.write(wb, {
      bookType: 'xlsm',
      bookVBA: hasVBA,
      type: 'array'
    });
  } catch (err) {
    return {
      success: false,
      rowCount: 0,
      discardedCount: discardedRows.length,
      discardedRows,
      reasonCounts,
      diagnostics: { totalInput: eanDataset.length, exported: 0, discarded: discardedRows.length, xlsmRows: 0, txtRows: 0 },
      error: `Errore serializzazione XLSM: ${err instanceof Error ? err.message : String(err)}`
    };
  }

  // Verify VBA is still present after write (only if template had VBA)
  if (hasVBA) {
    try {
      const verifyWb = XLSX.read(xlsmOut, { type: 'array', bookVBA: true });
      if (!(verifyWb as any).vbaraw) {
        const errMsg = 'Verifica post-write fallita: VBA perso durante serializzazione XLSM. Impossibile garantire integrità macro.';
        console.error('[Amazon:xlsm:FATAL]', errMsg);
        return {
          success: false,
          rowCount: 0,
          discardedCount: discardedRows.length,
          discardedRows,
          reasonCounts,
          diagnostics: { totalInput: eanDataset.length, exported: 0, discarded: discardedRows.length, xlsmRows: 0, txtRows: 0 },
          error: errMsg
        };
      }
    } catch (verifyErr) {
      console.warn('[Amazon:xlsm:verify] Verifica post-write non riuscita, proseguo con cautela:', verifyErr);
    }
  }

  const xlsmBlob = new Blob([xlsmOut], {
    type: 'application/vnd.ms-excel.sheet.macroEnabled.12'
  });

  // =====================================================================
  // PHASE 3: Build TXT (tab-delimited)
  // =====================================================================
  onProgress?.(82, 'Generazione file TXT...');

  const txtHeader = 'sku\tprice\tminimum-seller-allowed-price\tmaximum-seller-allowed-price\tquantity\tfulfillment-channel\thandling-time\n';
  const txtLines: string[] = [txtHeader];

  for (let i = 0; i < validRecords.length; i++) {
    const rec = validRecords[i];
    // SKU in TXT: no apostrophe, just sanitized
    const txtLine = `${sanitizeSKU(rec.sku)}\t${rec.priceDisplay}\t\t\t${rec.quantity}\t\t${rec.leadDays}\n`;
    txtLines.push(txtLine);
  }

  const txtContent = txtLines.join('');
  const txtBlob = new Blob([txtContent], { type: 'text/plain;charset=utf-8' });

  // =====================================================================
  // PHASE 4: Coherence assertion
  // =====================================================================
  onProgress?.(90, 'Verifica coerenza XLSM/TXT...');

  const xlsmRowCount = validRecords.length;
  const txtRowCount = txtLines.length - 1; // Exclude header

  if (xlsmRowCount !== txtRowCount) {
    return {
      success: false,
      rowCount: 0,
      discardedCount: discardedRows.length,
      discardedRows,
      reasonCounts,
      diagnostics: { totalInput: eanDataset.length, exported: 0, discarded: discardedRows.length, xlsmRows: xlsmRowCount, txtRows: txtRowCount },
      error: `Mismatch XLSM/TXT: ${xlsmRowCount} vs ${txtRowCount} righe`
    };
  }

  // Verify SKU order and values match
  for (let i = 0; i < validRecords.length; i++) {
    const rec = validRecords[i];
    const txtParts = txtLines[i + 1].split('\t');
    const txtSku = txtParts[0];
    const txtPrice = txtParts[1];
    const txtQty = txtParts[4];
    const txtLead = txtParts[6]?.replace('\n', '');

    if (sanitizeSKU(rec.sku) !== txtSku) {
      return {
        success: false,
        rowCount: 0,
        discardedCount: discardedRows.length,
        discardedRows,
        reasonCounts,
        diagnostics: { totalInput: eanDataset.length, exported: 0, discarded: discardedRows.length, xlsmRows: xlsmRowCount, txtRows: txtRowCount },
        error: `Mismatch SKU riga ${i}: XLSM="${rec.sku}" TXT="${txtSku}"`
      };
    }
    if (rec.priceDisplay !== txtPrice) {
      return {
        success: false,
        rowCount: 0,
        discardedCount: discardedRows.length,
        discardedRows,
        reasonCounts,
        diagnostics: { totalInput: eanDataset.length, exported: 0, discarded: discardedRows.length, xlsmRows: xlsmRowCount, txtRows: txtRowCount },
        error: `Mismatch price riga ${i}: XLSM="${rec.priceDisplay}" TXT="${txtPrice}"`
      };
    }
    if (String(rec.quantity) !== txtQty) {
      return {
        success: false,
        rowCount: 0,
        discardedCount: discardedRows.length,
        discardedRows,
        reasonCounts,
        diagnostics: { totalInput: eanDataset.length, exported: 0, discarded: discardedRows.length, xlsmRows: xlsmRowCount, txtRows: txtRowCount },
        error: `Mismatch quantity riga ${i}: XLSM=${rec.quantity} TXT=${txtQty}`
      };
    }
    if (String(rec.leadDays) !== txtLead) {
      return {
        success: false,
        rowCount: 0,
        discardedCount: discardedRows.length,
        discardedRows,
        reasonCounts,
        diagnostics: { totalInput: eanDataset.length, exported: 0, discarded: discardedRows.length, xlsmRows: xlsmRowCount, txtRows: txtRowCount },
        error: `Mismatch handling-time riga ${i}: XLSM=${rec.leadDays} TXT=${txtLead}`
      };
    }
  }

  // =====================================================================
  // PHASE 5: Build discarded XLSX (if any)
  // =====================================================================
  let discardedBlob: Blob | undefined;
  if (discardedRows.length > 0) {
    onProgress?.(95, 'Generazione file scarti...');
    const discardedWs = XLSX.utils.json_to_sheet(discardedRows);
    const discardedWb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(discardedWb, discardedWs, 'Scarti Amazon');
    const discardedBuffer = XLSX.write(discardedWb, { bookType: 'xlsx', type: 'array' });
    discardedBlob = new Blob([discardedBuffer], {
      type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    });
  }

  onProgress?.(100, 'Export Amazon completato');

  // Log summary
  console.log('%c[Amazon:export:complete]', 'color: #FF6600; font-weight: bold;', {
    totalInput: eanDataset.length,
    exported: validRecords.length,
    discarded: discardedRows.length,
    xlsmRows: xlsmRowCount,
    txtRows: txtRowCount,
    topReasons: Array.from(reasonCounts.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 20)
  });

  return {
    success: true,
    xlsmBlob,
    txtBlob,
    discardedBlob,
    rowCount: validRecords.length,
    discardedCount: discardedRows.length,
    discardedRows,
    reasonCounts,
    diagnostics: {
      totalInput: eanDataset.length,
      exported: validRecords.length,
      discarded: discardedRows.length,
      xlsmRows: xlsmRowCount,
      txtRows: txtRowCount
    }
  };
}

// =====================================================================
// DOWNLOAD FUNCTIONS
// =====================================================================

export function downloadAmazonFiles(result: AmazonExportResult): void {
  // Strict atomicity: only download if BOTH main files are present and export succeeded
  if (!result.success || !result.xlsmBlob || !result.txtBlob) {
    console.error('[Amazon:download:BLOCKED] Tentativo di download con risultato non valido', {
      success: result.success,
      hasXlsm: !!result.xlsmBlob,
      hasTxt: !!result.txtBlob
    });
    return;
  }

  const ts = generateTimestamp();

  downloadBlob(result.xlsmBlob, `amazon_listing_loader_${ts}.xlsm`);

  // Small delay to avoid browser blocking multiple downloads
  setTimeout(() => {
    downloadBlob(result.txtBlob!, `amazon_price_inventory_${ts}.txt`);
  }, 500);

  // Discarded file only on success
  if (result.discardedBlob) {
    setTimeout(() => {
      downloadBlob(result.discardedBlob!, `amazon_discarded_${ts}.xlsx`);
    }, 1000);
  }
}

