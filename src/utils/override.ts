/**
 * Override Prodotti - Utility per gestire override prezzi/quantità dal file XLSX
 * 
 * IMPORTANTE: Questa funzionalità è un LIVELLO AGGIUNTIVO sopra la pipeline esistente.
 * NON modifica la logica di pricing, semplicemente sovrascrive i valori finali.
 * 
 * PRICE RULE EXCEPTION: Override products keep exact OfferPrice (no forceEnding99)
 */

import * as XLSX from 'xlsx';
import { normalizeEAN } from './ean';

// =====================================================================
// INTERFACES
// =====================================================================

export interface OverrideItem {
  rawRowIndex: number;
  sku: string;
  ean: string;
  quantity?: number; // Optional - empty means don't overwrite catalog value
  listPrice?: number; // Optional - empty means don't overwrite catalog value
  offerPrice?: number; // Optional for existing product overrides, mandatory for new
  leadDays?: number; // Single lead days (applies to both IT/EU if per-side not set)
  stockIT?: number;
  stockEU?: number;
  leadDaysIT?: number;
  leadDaysEU?: number;
}

export interface OverrideIndex {
  byEan: Map<string, OverrideItem>;
  bySku: Map<string, OverrideItem>;
  validItems: OverrideItem[];
}

export interface OverrideError {
  rowIndex: number;
  field: string;
  value: string;
  reason: string;
}

export interface ParseOverrideResult {
  success: boolean;
  index: OverrideIndex | null;
  errors: OverrideError[];
  warnings: OverrideError[]; // NEW: separate warnings from errors
  validCount: number;
  invalidCount: number;
}

export interface OverrideStats {
  updatedExisting: number;
  addedNew: number;
  skippedRows: number;
  warnings: number;
}

export interface ApplyOverrideResult {
  catalog: Record<string, unknown>[];
  stats: OverrideStats;
  errors: OverrideError[];
}

// =====================================================================
// COLUMN NAMES - REQUIRED and OPTIONAL, CASE-INSENSITIVE
// =====================================================================

const REQUIRED_COLUMNS = ['SKU', 'EAN', 'Quantity', 'ListPrice', 'OfferPrice'];
const OPTIONAL_COLUMNS = ['StockIT', 'StockEU', 'LeadDaysIT', 'LeadDaysEU', 'LeadDays'];

// =====================================================================
// HELPER FUNCTIONS
// =====================================================================

/**
 * Trova l'indice delle colonne nell'header
 * Returns map with required and optional columns (optional may be -1 if not found)
 */
function findColumnIndices(headers: string[]): { required: Map<string, number> | null; optional: Map<string, number> } {
  const normalizedHeaders = headers.map(h => (h ?? '').toString().trim().toLowerCase());
  const requiredIndices = new Map<string, number>();
  const optionalIndices = new Map<string, number>();
  
  // Check required columns
  for (const col of REQUIRED_COLUMNS) {
    const idx = normalizedHeaders.indexOf(col.toLowerCase());
    if (idx === -1) {
      return { required: null, optional: optionalIndices }; // Missing required column
    }
    requiredIndices.set(col, idx);
  }
  
  // Check optional columns (may not exist)
  for (const col of OPTIONAL_COLUMNS) {
    const idx = normalizedHeaders.indexOf(col.toLowerCase());
    optionalIndices.set(col, idx); // -1 if not found
  }
  
  return { required: requiredIndices, optional: optionalIndices };
}

/**
 * Normalizza una stringa SKU
 */
function normalizeSku(raw: unknown): string {
  if (raw === null || raw === undefined) return '';
  return String(raw).trim();
}

/**
 * Parsa un valore numerico con max 2 decimali
 * Ritorna null se non valido o ha più di 2 decimali
 */
function parseNumericWithDecimals(raw: unknown): number | null {
  if (raw === null || raw === undefined || raw === '') return null;
  
  let value: number;
  
  if (typeof raw === 'number') {
    value = raw;
  } else {
    // Normalizza separatore decimale
    const str = String(raw).trim().replace(',', '.');
    value = parseFloat(str);
  }
  
  if (!Number.isFinite(value)) return null;
  
  // Verifica max 2 decimali
  const rounded = Math.round(value * 100) / 100;
  if (Math.abs(value - rounded) > 0.0001) {
    return null; // Più di 2 decimali
  }
  
  return rounded;
}

/**
 * Parsa quantity/stock/leadDays come intero >= 0
 */
function parseInteger(raw: unknown): number | null {
  if (raw === null || raw === undefined || raw === '') return null;
  
  let value: number;
  
  if (typeof raw === 'number') {
    value = raw;
  } else {
    const str = String(raw).trim();
    value = parseInt(str, 10);
  }
  
  if (!Number.isFinite(value) || !Number.isInteger(value) || value < 0) {
    return null;
  }
  
  return value;
}

/**
 * Forza ending ,99 sul prezzo (lavora in centesimi)
 * NOTE: NOT used for override products
 */
export function forceEnding99(price: number): number {
  const priceInCents = Math.round(price * 100);
  
  if (priceInCents % 100 === 99) {
    return price;
  }
  
  const nextBlock = priceInCents - (priceInCents % 100);
  let newCents = nextBlock + 99;
  
  if (newCents <= priceInCents) {
    newCents += 100;
  }
  
  return newCents / 100;
}

// =====================================================================
// MAIN FUNCTIONS
// =====================================================================

/**
 * Parsa il file override XLSX
 * 
 * PARSING RULES:
 * - SKU: mandatory, non-empty
 * - EAN: mandatory, valid via normalizeEAN
 * - Quantity: mandatory, integer >= 0
 * - ListPrice: mandatory, number with max 2 decimals
 * - OfferPrice: header required, but value optional per row (empty allowed for existing product overrides)
 * - StockIT/StockEU/LeadDaysIT/LeadDaysEU: headers optional, values optional integers >= 0
 * 
 * DUPLICATE HANDLING: Last row wins (no file invalidation)
 */
export function parseOverrideFile(data: ArrayBuffer): ParseOverrideResult {
  const errors: OverrideError[] = [];
  const warnings: OverrideError[] = [];
  
  try {
    const workbook = XLSX.read(data, { type: 'array' });
    const firstSheetName = workbook.SheetNames[0];
    
    if (!firstSheetName) {
      return {
        success: false,
        index: null,
        errors: [{ rowIndex: 0, field: 'file', value: '', reason: 'File vuoto o senza fogli' }],
        warnings: [],
        validCount: 0,
        invalidCount: 0
      };
    }
    
    const sheet = workbook.Sheets[firstSheetName];
    const jsonData = XLSX.utils.sheet_to_json(sheet, { header: 1 }) as unknown[][];
    
    if (jsonData.length < 2) {
      return {
        success: false,
        index: null,
        errors: [{ rowIndex: 0, field: 'file', value: '', reason: 'File senza righe dati' }],
        warnings: [],
        validCount: 0,
        invalidCount: 0
      };
    }
    
    // Prima riga è header
    const headerRow = jsonData[0] as string[];
    const { required: columnIndices, optional: optionalIndices } = findColumnIndices(headerRow);
    
    if (!columnIndices) {
      const missingCols = REQUIRED_COLUMNS.filter(col => 
        !headerRow.some(h => h && h.toString().trim().toLowerCase() === col.toLowerCase())
      );
      return {
        success: false,
        index: null,
        errors: [{ 
          rowIndex: 1, 
          field: 'header', 
          value: headerRow.join(', '), 
          reason: `Colonne mancanti: ${missingCols.join(', ')}. Richieste: ${REQUIRED_COLUMNS.join(', ')}` 
        }],
        warnings: [],
        validCount: 0,
        invalidCount: 0
      };
    }
    
    // Get optional column indices
    const stockITIdx = optionalIndices.get('StockIT') ?? -1;
    const stockEUIdx = optionalIndices.get('StockEU') ?? -1;
    const leadDaysITIdx = optionalIndices.get('LeadDaysIT') ?? -1;
    const leadDaysEUIdx = optionalIndices.get('LeadDaysEU') ?? -1;
    const leadDaysIdx = optionalIndices.get('LeadDays') ?? -1;
    
    // Parsa le righe dati - collect ALL valid items first
    const allValidItems: OverrideItem[] = [];
    let invalidCount = 0;
    
    for (let i = 1; i < jsonData.length; i++) {
      const row = jsonData[i];
      const rowIndex = i + 1; // Excel row number (1-indexed)
      
      if (!row || row.length === 0) continue;
      
      const skuRaw = row[columnIndices.get('SKU')!];
      const eanRaw = row[columnIndices.get('EAN')!];
      const quantityRaw = row[columnIndices.get('Quantity')!];
      const listPriceRaw = row[columnIndices.get('ListPrice')!];
      const offerPriceRaw = row[columnIndices.get('OfferPrice')!];
      
      // Optional columns
      const stockITRaw = stockITIdx >= 0 ? row[stockITIdx] : undefined;
      const stockEURaw = stockEUIdx >= 0 ? row[stockEUIdx] : undefined;
      const leadDaysITRaw = leadDaysITIdx >= 0 ? row[leadDaysITIdx] : undefined;
      const leadDaysEURaw = leadDaysEUIdx >= 0 ? row[leadDaysEUIdx] : undefined;
      const leadDaysRaw = leadDaysIdx >= 0 ? row[leadDaysIdx] : undefined;
      
      // Valida SKU (mandatory non-empty)
      const sku = normalizeSku(skuRaw);
      if (!sku) {
        errors.push({ rowIndex, field: 'SKU', value: String(skuRaw ?? ''), reason: 'SKU vuoto o mancante' });
        invalidCount++;
        continue;
      }
      
      // Valida EAN con normalizzazione esistente
      const eanResult = normalizeEAN(eanRaw);
      if (!eanResult.ok || !eanResult.value) {
        errors.push({ rowIndex, field: 'EAN', value: String(eanRaw ?? ''), reason: eanResult.reason || 'EAN non valido' });
        invalidCount++;
        continue;
      }
      const ean = eanResult.value;
      
      // Quantity: optional per row, validate if present
      let quantity: number | undefined = undefined;
      if (quantityRaw !== null && quantityRaw !== undefined && quantityRaw !== '') {
        const parsedQty = parseInteger(quantityRaw);
        if (parsedQty === null) {
          errors.push({ rowIndex, field: 'Quantity', value: String(quantityRaw), reason: 'Quantity non è un intero >= 0' });
          invalidCount++;
          continue;
        }
        quantity = parsedQty;
      }
      
      // ListPrice: optional per row, validate if present
      let listPrice: number | undefined = undefined;
      if (listPriceRaw !== null && listPriceRaw !== undefined && listPriceRaw !== '') {
        const parsedLP = parseNumericWithDecimals(listPriceRaw);
        if (parsedLP === null) {
          errors.push({ rowIndex, field: 'ListPrice', value: String(listPriceRaw), reason: 'ListPrice non valido (max 2 decimali)' });
          invalidCount++;
          continue;
        }
        listPrice = parsedLP;
      }
      
      // Parsa OfferPrice (optional per row - empty allowed)
      let offerPrice: number | undefined = undefined;
      if (offerPriceRaw !== null && offerPriceRaw !== undefined && offerPriceRaw !== '') {
        const parsedOP = parseNumericWithDecimals(offerPriceRaw);
        if (parsedOP === null) {
          errors.push({ rowIndex, field: 'OfferPrice', value: String(offerPriceRaw), reason: 'OfferPrice non valido (max 2 decimali)' });
          invalidCount++;
          continue;
        }
        offerPrice = parsedOP;
      }
      
      // Parse optional StockIT
      let stockIT: number | undefined = undefined;
      if (stockITRaw !== null && stockITRaw !== undefined && stockITRaw !== '') {
        const parsed = parseInteger(stockITRaw);
        if (parsed === null) {
          errors.push({ rowIndex, field: 'StockIT', value: String(stockITRaw), reason: 'StockIT non è un intero >= 0' });
          invalidCount++;
          continue;
        }
        stockIT = parsed;
      }
      
      // Parse optional StockEU
      let stockEU: number | undefined = undefined;
      if (stockEURaw !== null && stockEURaw !== undefined && stockEURaw !== '') {
        const parsed = parseInteger(stockEURaw);
        if (parsed === null) {
          errors.push({ rowIndex, field: 'StockEU', value: String(stockEURaw), reason: 'StockEU non è un intero >= 0' });
          invalidCount++;
          continue;
        }
        stockEU = parsed;
      }
      
      // Parse optional LeadDaysIT
      let leadDaysIT: number | undefined = undefined;
      if (leadDaysITRaw !== null && leadDaysITRaw !== undefined && leadDaysITRaw !== '') {
        const parsed = parseInteger(leadDaysITRaw);
        if (parsed === null) {
          errors.push({ rowIndex, field: 'LeadDaysIT', value: String(leadDaysITRaw), reason: 'LeadDaysIT non è un intero >= 0' });
          invalidCount++;
          continue;
        }
        leadDaysIT = parsed;
      }
      
      // Parse optional LeadDaysEU
      let leadDaysEU: number | undefined = undefined;
      if (leadDaysEURaw !== null && leadDaysEURaw !== undefined && leadDaysEURaw !== '') {
        const parsed = parseInteger(leadDaysEURaw);
        if (parsed === null) {
          errors.push({ rowIndex, field: 'LeadDaysEU', value: String(leadDaysEURaw), reason: 'LeadDaysEU non è un intero >= 0' });
          invalidCount++;
          continue;
        }
        leadDaysEU = parsed;
      }
      
      // Parse optional LeadDays (single value, applies to both IT/EU if per-side not set)
      let leadDays: number | undefined = undefined;
      if (leadDaysRaw !== null && leadDaysRaw !== undefined && leadDaysRaw !== '') {
        const parsed = parseInteger(leadDaysRaw);
        if (parsed === null) {
          errors.push({ rowIndex, field: 'LeadDays', value: String(leadDaysRaw), reason: 'LeadDays non è un intero >= 0' });
          invalidCount++;
          continue;
        }
        leadDays = parsed;
      }
      
      allValidItems.push({
        rawRowIndex: rowIndex,
        sku,
        ean,
        quantity,
        listPrice,
        offerPrice,
        leadDays,
        stockIT,
        stockEU,
        leadDaysIT,
        leadDaysEU
      });
    }
    
    // DUPLICATE HANDLING: Last row wins
    // Track EAN duplicates
    const eanToRows = new Map<string, number[]>();
    for (const item of allValidItems) {
      const rows = eanToRows.get(item.ean) || [];
      rows.push(item.rawRowIndex);
      eanToRows.set(item.ean, rows);
    }
    
    // Track SKU duplicates (case-insensitive)
    const skuToRows = new Map<string, number[]>();
    for (const item of allValidItems) {
      const normalizedSku = item.sku.toLowerCase();
      const rows = skuToRows.get(normalizedSku) || [];
      rows.push(item.rawRowIndex);
      skuToRows.set(normalizedSku, rows);
    }
    
    // Generate warnings for duplicates
    for (const [ean, rows] of eanToRows) {
      if (rows.length > 1) {
        warnings.push({
          rowIndex: rows[rows.length - 1], // Last row that was kept
          field: 'EAN',
          value: ean,
          reason: `Warning: EAN duplicato alle righe ${rows.join(', ')}. Mantenuta riga ${rows[rows.length - 1]} (ultima).`
        });
      }
    }
    
    for (const [sku, rows] of skuToRows) {
      if (rows.length > 1) {
        warnings.push({
          rowIndex: rows[rows.length - 1],
          field: 'SKU',
          value: sku,
          reason: `Warning: SKU duplicato (case-insensitive) alle righe ${rows.join(', ')}. Mantenuta riga ${rows[rows.length - 1]} (ultima).`
        });
      }
    }
    
    // Build final index with last-row-wins logic
    const byEan = new Map<string, OverrideItem>();
    const bySku = new Map<string, OverrideItem>();
    
    // Process in order so last item for each key wins
    for (const item of allValidItems) {
      byEan.set(item.ean, item);
      bySku.set(item.sku.toLowerCase(), item);
    }
    
    // Build validItems from unique EANs (deduplicated)
    const validItems = Array.from(byEan.values());
    
    return {
      success: validItems.length > 0,
      index: validItems.length > 0 ? { byEan, bySku, validItems } : null,
      errors,
      warnings,
      validCount: validItems.length,
      invalidCount
    };
    
  } catch (error) {
    return {
      success: false,
      index: null,
      errors: [{ 
        rowIndex: 0, 
        field: 'file', 
        value: '', 
        reason: `Errore parsing file: ${error instanceof Error ? error.message : 'Errore sconosciuto'}` 
      }],
      warnings: [],
      validCount: 0,
      invalidCount: 0
    };
  }
}

/**
 * Applica l'override al catalogo base
 * 
 * PRICE RULE: Override OfferPrice is used EXACTLY as provided (no forceEnding99)
 * 
 * METADATA FIELDS added to each record:
 * - __override: true
 * - __overrideSource: 'existing' | 'new'
 * - __overrideStockIT: number | null
 * - __overrideStockEU: number | null
 * - __overrideLeadDaysIT: number | null
 * - __overrideLeadDaysEU: number | null
 */
export function applyOverrideToCatalog(
  baseCatalog: Record<string, unknown>[],
  overrideIndex: OverrideIndex
): ApplyOverrideResult {
  const errors: OverrideError[] = [];
  const stats: OverrideStats = {
    updatedExisting: 0,
    addedNew: 0,
    skippedRows: 0,
    warnings: 0
  };
  
  // Crea un set di EAN già processati per controllo duplicati nel baseCatalog
  const baseCatalogDuplicates = new Map<string, number>();
  
  // Controlla duplicati nel baseCatalog
  for (const record of baseCatalog) {
    const eanResult = normalizeEAN(record.EAN);
    if (eanResult.ok && eanResult.value) {
      const count = baseCatalogDuplicates.get(eanResult.value) || 0;
      baseCatalogDuplicates.set(eanResult.value, count + 1);
    }
  }
  
  const catalogWithOverride: Record<string, unknown>[] = [];
  const matchedOverrideEans = new Set<string>();
  
  // Processa i record esistenti
  for (const record of baseCatalog) {
    const eanResult = normalizeEAN(record.EAN);
    
    if (!eanResult.ok || !eanResult.value) {
      catalogWithOverride.push({ ...record });
      continue;
    }
    
    const normalizedEan = eanResult.value;
    
    // Controlla se c'è duplicato nel baseCatalog per questo EAN
    if ((baseCatalogDuplicates.get(normalizedEan) || 0) > 1) {
      errors.push({
        rowIndex: 0,
        field: 'baseCatalog',
        value: normalizedEan,
        reason: `EAN duplicato nel catalogo base - override ignorato per questo EAN`
      });
      catalogWithOverride.push({ ...record });
      continue;
    }
    
    // MATCHING PRIORITY: SKU first, then EAN fallback
    // 1. Try to match by SKU (ManufPartNr) - case-insensitive
    const recordSku = String(record.ManufPartNr || '').trim().toLowerCase();
    let overrideItem = recordSku ? overrideIndex.bySku.get(recordSku) : undefined;
    let matchType = 'sku';
    
    // 2. Fallback to EAN match if SKU didn't match
    if (!overrideItem) {
      overrideItem = overrideIndex.byEan.get(normalizedEan);
      matchType = 'ean';
    }
    
    if (!overrideItem) {
      catalogWithOverride.push({ ...record });
      continue;
    }
    
    matchedOverrideEans.add(normalizedEan);
    
    // Crea copia del record per applicare override
    const updatedRecord = { ...record };
    
    // Log match type for debugging (first 5 only)
    if (stats.updatedExisting < 5) {
      console.log('%c[Override:match]', 'color: #9C27B0;', {
        matchType,
        recordSku,
        overrideSku: overrideItem.sku,
        recordEAN: normalizedEan,
        overrideEAN: overrideItem.ean
      });
    }
    
    // Applica overrides - only overwrite fields that have values
    
    // STOCK: If explicit StockIT/StockEU present, use them for ExistingStock
    const hasExplicitStock = overrideItem.stockIT !== undefined || overrideItem.stockEU !== undefined;
    if (hasExplicitStock) {
      const sIT = overrideItem.stockIT !== undefined ? overrideItem.stockIT : 0;
      const sEU = overrideItem.stockEU !== undefined ? overrideItem.stockEU : 0;
      updatedRecord.ExistingStock = sIT + sEU;
    } else if (overrideItem.quantity !== undefined) {
      // Fallback: Quantity → ExistingStock
      updatedRecord.ExistingStock = overrideItem.quantity;
    }
    // If neither stock nor quantity provided, keep catalog ExistingStock
    
    // ListPrice: only overwrite if provided
    if (overrideItem.listPrice !== undefined) {
      updatedRecord.ListPrice = overrideItem.listPrice;
      updatedRecord['ListPrice con Fee'] = overrideItem.listPrice;
      // Update per-export ListPrice fields so marketplace exports use override value
      updatedRecord.listprice_with_fee_mediaworld = overrideItem.listPrice;
      updatedRecord.listprice_with_fee_eprice = overrideItem.listPrice;
    }
    
    // OfferPrice -> "Prezzo Finale" WITHOUT forceEnding99
    if (overrideItem.offerPrice !== undefined) {
      updatedRecord['Prezzo Finale'] = overrideItem.offerPrice.toFixed(2).replace('.', ',');
      // Update per-export price fields so marketplace exports use override price "as is"
      updatedRecord.final_price_ean = overrideItem.offerPrice.toFixed(2).replace('.', ',');
      updatedRecord.final_price_eprice = overrideItem.offerPrice;
      updatedRecord.final_price_mediaworld = overrideItem.offerPrice;
      updatedRecord.mediaworldPricingError = false;
      updatedRecord.epricePricingError = false;
    }
    
    // Add override metadata
    updatedRecord.__override = true;
    updatedRecord.__overrideSource = 'existing';
    updatedRecord.__overrideStockIT = overrideItem.stockIT ?? null;
    updatedRecord.__overrideStockEU = overrideItem.stockEU ?? null;
    // LeadDays: per-side takes priority, then single LeadDays
    updatedRecord.__overrideLeadDaysIT = overrideItem.leadDaysIT ?? overrideItem.leadDays ?? null;
    updatedRecord.__overrideLeadDaysEU = overrideItem.leadDaysEU ?? overrideItem.leadDays ?? null;
    
    catalogWithOverride.push(updatedRecord);
    stats.updatedExisting++;
  }
  
  // Crea nuovi prodotti dagli override non matchati
  for (const item of overrideIndex.validItems) {
    if (matchedOverrideEans.has(item.ean)) continue;
    
    // Verifica requisiti per nuovo prodotto
    if (!item.sku) {
      errors.push({
        rowIndex: item.rawRowIndex,
        field: 'nuovo_prodotto',
        value: '',
        reason: 'Riga non idonea per nuovo prodotto: SKU mancante'
      });
      stats.skippedRows++;
      continue;
    }
    
    // OfferPrice mandatory for NEW products
    if (item.offerPrice === undefined) {
      errors.push({
        rowIndex: item.rawRowIndex,
        field: 'nuovo_prodotto',
        value: item.sku,
        reason: 'Riga non idonea per nuovo prodotto: OfferPrice mancante (obbligatorio per nuovi prodotti)'
      });
      stats.skippedRows++;
      continue;
    }
    
    // STOCK for new products
    const hasExplicitStock = item.stockIT !== undefined || item.stockEU !== undefined;
    let stockITValue: number;
    let stockEUValue: number;
    let existingStockValue: number;
    
    if (hasExplicitStock) {
      stockITValue = item.stockIT !== undefined ? item.stockIT : 0;
      stockEUValue = item.stockEU !== undefined ? item.stockEU : 0;
      existingStockValue = stockITValue + stockEUValue;
    } else if (item.quantity !== undefined) {
      // Fallback: Quantity as IT-only
      stockITValue = item.quantity;
      stockEUValue = 0;
      existingStockValue = item.quantity;
    } else {
      // No stock info at all - skip with warning
      errors.push({
        rowIndex: item.rawRowIndex,
        field: 'nuovo_prodotto',
        value: item.sku,
        reason: 'Riga non idonea per nuovo prodotto: StockIT/StockEU e Quantity tutti mancanti'
      });
      stats.skippedRows++;
      continue;
    }
    
    // Create new record WITHOUT forceEnding99 - use exact OfferPrice
    const newRecord: Record<string, unknown> = {
      Matnr: `OVR-${item.sku}`,
      ManufPartNr: item.sku,
      EAN: item.ean,
      ShortDescription: `Override item ${item.sku}`,
      ExistingStock: existingStockValue,
      ListPrice: item.listPrice ?? 0,
      CustBestPrice: 0,
      Surcharge: 0,
      'Costo di Spedizione': '0,00',
      IVA: '22%',
      'Prezzo con spediz e IVA': '0,00',
      FeeDeRev: 0,
      'Fee Marketplace': 0,
      'Subtotale post-fee': '0,00',
      // Use EXACT OfferPrice - NO forceEnding99
      'Prezzo Finale': item.offerPrice.toFixed(2).replace('.', ','),
      'ListPrice con Fee': item.listPrice ?? 0,
      // Per-export pricing: override price "as is"
      final_price_ean: item.offerPrice.toFixed(2).replace('.', ','),
      final_price_eprice: item.offerPrice,
      final_price_mediaworld: item.offerPrice,
      listprice_with_fee_mediaworld: item.listPrice ?? 0,
      listprice_with_fee_eprice: item.listPrice ?? 0,
      mediaworldPricingError: false,
      epricePricingError: false,
      // Override metadata
      __override: true,
      __overrideSource: 'new',
      __overrideStockIT: item.stockIT ?? null,
      __overrideStockEU: item.stockEU ?? null,
      __overrideLeadDaysIT: item.leadDaysIT ?? item.leadDays ?? null,
      __overrideLeadDaysEU: item.leadDaysEU ?? item.leadDays ?? null
    };
    
    catalogWithOverride.push(newRecord);
    stats.addedNew++;
  }
  
  return {
    catalog: catalogWithOverride,
    stats,
    errors
  };
}

/**
 * Valida che tutti i prezzi finali terminino con ,99
 * 
 * IMPORTANT: Records with __override === true are SKIPPED (override products keep exact price)
 */
export function validateEnding99Guard(catalog: Record<string, unknown>[]): { 
  valid: boolean; 
  failures: Array<{ index: number; ean: string; price: string }> 
} {
  const failures: Array<{ index: number; ean: string; price: string }> = [];
  
  for (let i = 0; i < catalog.length; i++) {
    const record = catalog[i];
    
    // SKIP override records - they keep exact price without ,99 enforcement
    if (record.__override === true) {
      continue;
    }
    
    const prezzoFinale = record['Prezzo Finale'];
    
    if (!prezzoFinale) continue;
    
    // Converti a numero per verifica
    let priceNum: number;
    if (typeof prezzoFinale === 'number') {
      priceNum = prezzoFinale;
    } else {
      priceNum = parseFloat(String(prezzoFinale).replace(',', '.'));
    }
    
    if (!Number.isFinite(priceNum)) continue;
    
    const cents = Math.round(priceNum * 100) % 100;
    if (cents !== 99) {
      failures.push({
        index: i,
        ean: record.EAN || 'N/A',
        price: String(prezzoFinale)
      });
    }
  }
  
  return {
    valid: failures.length === 0,
    failures
  };
}
