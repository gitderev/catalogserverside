/**
 * Override Prodotti - Utility per gestire override prezzi/quantità dal file XLSX
 * 
 * IMPORTANTE: Questa funzionalità è un LIVELLO AGGIUNTIVO sopra la pipeline esistente.
 * NON modifica la logica di pricing, semplicemente sovrascrive i valori finali.
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
  quantity?: number;
  listPrice?: number;
  offerPrice?: number;
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
  catalog: any[];
  stats: OverrideStats;
  errors: OverrideError[];
}

// =====================================================================
// COLUMN NAMES - ESATTI, CASE-INSENSITIVE, SENZA ALIAS
// =====================================================================

const REQUIRED_COLUMNS = ['SKU', 'EAN', 'Quantity', 'ListPrice', 'OfferPrice'];

// =====================================================================
// HELPER FUNCTIONS
// =====================================================================

/**
 * Trova l'indice delle colonne richieste nell'header
 * Ritorna null se una qualsiasi colonna manca
 */
function findColumnIndices(headers: string[]): Map<string, number> | null {
  const normalizedHeaders = headers.map(h => h.trim().toLowerCase());
  const indices = new Map<string, number>();
  
  for (const col of REQUIRED_COLUMNS) {
    const idx = normalizedHeaders.indexOf(col.toLowerCase());
    if (idx === -1) {
      return null; // Colonna mancante - file invalido
    }
    indices.set(col, idx);
  }
  
  return indices;
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
 * Parsa quantity come intero >= 0
 */
function parseQuantity(raw: unknown): number | null {
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
 * Ritorna null per overrideIndex se il file è completamente invalido
 */
export function parseOverrideFile(data: ArrayBuffer): ParseOverrideResult {
  const errors: OverrideError[] = [];
  
  try {
    const workbook = XLSX.read(data, { type: 'array' });
    const firstSheetName = workbook.SheetNames[0];
    
    if (!firstSheetName) {
      return {
        success: false,
        index: null,
        errors: [{ rowIndex: 0, field: 'file', value: '', reason: 'File vuoto o senza fogli' }],
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
        validCount: 0,
        invalidCount: 0
      };
    }
    
    // Prima riga è header
    const headerRow = jsonData[0] as string[];
    const columnIndices = findColumnIndices(headerRow);
    
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
        validCount: 0,
        invalidCount: 0
      };
    }
    
    // Parsa le righe dati
    const validItems: OverrideItem[] = [];
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
      
      // Valida SKU (obbligatorio non vuoto)
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
      
      // Parsa Quantity (opzionale ma se presente deve essere intero >= 0)
      let quantity: number | undefined = undefined;
      if (quantityRaw !== null && quantityRaw !== undefined && quantityRaw !== '') {
        const parsedQty = parseQuantity(quantityRaw);
        if (parsedQty === null) {
          errors.push({ rowIndex, field: 'Quantity', value: String(quantityRaw), reason: 'Quantity non è un intero >= 0' });
          invalidCount++;
          continue;
        }
        quantity = parsedQty;
      }
      
      // Parsa ListPrice (opzionale ma se presente deve essere numero con max 2 decimali)
      let listPrice: number | undefined = undefined;
      if (listPriceRaw !== null && listPriceRaw !== undefined && listPriceRaw !== '') {
        const parsedLP = parseNumericWithDecimals(listPriceRaw);
        if (parsedLP === null) {
          errors.push({ rowIndex, field: 'ListPrice', value: String(listPriceRaw), reason: 'ListPrice non valido o ha più di 2 decimali' });
          invalidCount++;
          continue;
        }
        listPrice = parsedLP;
      }
      
      // Parsa OfferPrice (opzionale ma se presente deve essere numero con max 2 decimali)
      let offerPrice: number | undefined = undefined;
      if (offerPriceRaw !== null && offerPriceRaw !== undefined && offerPriceRaw !== '') {
        const parsedOP = parseNumericWithDecimals(offerPriceRaw);
        if (parsedOP === null) {
          errors.push({ rowIndex, field: 'OfferPrice', value: String(offerPriceRaw), reason: 'OfferPrice non valido o ha più di 2 decimali' });
          invalidCount++;
          continue;
        }
        offerPrice = parsedOP;
      }
      
      validItems.push({
        rawRowIndex: rowIndex,
        sku,
        ean,
        quantity,
        listPrice,
        offerPrice
      });
    }
    
    // Controlla duplicati EAN
    const eanCounts = new Map<string, number[]>();
    for (const item of validItems) {
      const rows = eanCounts.get(item.ean) || [];
      rows.push(item.rawRowIndex);
      eanCounts.set(item.ean, rows);
    }
    
    const duplicateEans: string[] = [];
    for (const [ean, rows] of eanCounts) {
      if (rows.length > 1) {
        duplicateEans.push(`EAN ${ean} alle righe ${rows.join(', ')}`);
      }
    }
    
    if (duplicateEans.length > 0) {
      return {
        success: false,
        index: null,
        errors: [{ 
          rowIndex: 0, 
          field: 'duplicati_ean', 
          value: '', 
          reason: `EAN duplicati nel file override. File completamente invalidato. Duplicati: ${duplicateEans.join('; ')}` 
        }],
        validCount: 0,
        invalidCount: validItems.length + invalidCount
      };
    }
    
    // Controlla duplicati SKU
    const skuCounts = new Map<string, number[]>();
    for (const item of validItems) {
      const normalizedSku = item.sku.toLowerCase();
      const rows = skuCounts.get(normalizedSku) || [];
      rows.push(item.rawRowIndex);
      skuCounts.set(normalizedSku, rows);
    }
    
    const duplicateSkus: string[] = [];
    for (const [sku, rows] of skuCounts) {
      if (rows.length > 1) {
        duplicateSkus.push(`SKU ${sku} alle righe ${rows.join(', ')}`);
      }
    }
    
    if (duplicateSkus.length > 0) {
      return {
        success: false,
        index: null,
        errors: [{ 
          rowIndex: 0, 
          field: 'duplicati_sku', 
          value: '', 
          reason: `SKU duplicati nel file override. File completamente invalidato. Duplicati: ${duplicateSkus.join('; ')}` 
        }],
        validCount: 0,
        invalidCount: validItems.length + invalidCount
      };
    }
    
    // Costruisci l'indice
    const byEan = new Map<string, OverrideItem>();
    const bySku = new Map<string, OverrideItem>();
    
    for (const item of validItems) {
      byEan.set(item.ean, item);
      bySku.set(item.sku.toLowerCase(), item);
    }
    
    return {
      success: validItems.length > 0,
      index: {
        byEan,
        bySku,
        validItems
      },
      errors,
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
      validCount: 0,
      invalidCount: 0
    };
  }
}

/**
 * Applica l'override al catalogo base
 */
export function applyOverrideToCatalog(
  baseCatalog: any[],
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
  const processedEans = new Set<string>();
  const baseCatalogDuplicates = new Map<string, number>();
  
  // Controlla duplicati nel baseCatalog
  for (const record of baseCatalog) {
    const eanResult = normalizeEAN(record.EAN);
    if (eanResult.ok && eanResult.value) {
      const count = baseCatalogDuplicates.get(eanResult.value) || 0;
      baseCatalogDuplicates.set(eanResult.value, count + 1);
    }
  }
  
  const catalogWithOverride: any[] = [];
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
    
    // Cerca override per questo EAN
    const overrideItem = overrideIndex.byEan.get(normalizedEan);
    
    if (!overrideItem) {
      catalogWithOverride.push({ ...record });
      continue;
    }
    
    matchedOverrideEans.add(normalizedEan);
    
    // Crea copia del record per applicare override
    const updatedRecord = { ...record };
    
    // Verifica SKU match (warning se non coincide)
    const recordSku = String(record.ManufPartNr || '').trim();
    if (recordSku && overrideItem.sku && recordSku.toLowerCase() !== overrideItem.sku.toLowerCase()) {
      errors.push({
        rowIndex: overrideItem.rawRowIndex,
        field: 'SKU',
        value: `${overrideItem.sku} vs ${recordSku}`,
        reason: `Warning: SKU override "${overrideItem.sku}" non coincide con ManufPartNr del record "${recordSku}". Override applicato comunque.`
      });
      stats.warnings++;
    }
    
    // Applica overrides nell'ordine specificato
    
    // 1. Quantity -> ExistingStock
    if (overrideItem.quantity !== undefined) {
      updatedRecord.ExistingStock = overrideItem.quantity;
    }
    
    // 2. ListPrice -> ListPrice e "ListPrice con Fee"
    if (overrideItem.listPrice !== undefined) {
      updatedRecord.ListPrice = overrideItem.listPrice;
      updatedRecord['ListPrice con Fee'] = overrideItem.listPrice;
    }
    
    // 3. OfferPrice -> "Prezzo Finale" con forceEnding99
    if (overrideItem.offerPrice !== undefined) {
      const finalPrice = forceEnding99(overrideItem.offerPrice);
      // Formatta come stringa con virgola per coerenza con pipeline esistente
      updatedRecord['Prezzo Finale'] = finalPrice.toFixed(2).replace('.', ',');
    }
    
    // Marca come override
    updatedRecord.__override = true;
    updatedRecord.__overrideSource = 'existing';
    
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
    
    if (item.quantity === undefined) {
      errors.push({
        rowIndex: item.rawRowIndex,
        field: 'nuovo_prodotto',
        value: item.sku,
        reason: 'Riga non idonea per nuovo prodotto: Quantity mancante'
      });
      stats.skippedRows++;
      continue;
    }
    
    if (item.listPrice === undefined) {
      errors.push({
        rowIndex: item.rawRowIndex,
        field: 'nuovo_prodotto',
        value: item.sku,
        reason: 'Riga non idonea per nuovo prodotto: ListPrice mancante'
      });
      stats.skippedRows++;
      continue;
    }
    
    if (item.offerPrice === undefined) {
      errors.push({
        rowIndex: item.rawRowIndex,
        field: 'nuovo_prodotto',
        value: item.sku,
        reason: 'Riga non idonea per nuovo prodotto: OfferPrice mancante'
      });
      stats.skippedRows++;
      continue;
    }
    
    // Crea nuovo record
    const finalPrice = forceEnding99(item.offerPrice);
    
    const newRecord: any = {
      Matnr: `OVR-${item.sku}`,
      ManufPartNr: item.sku,
      EAN: item.ean,
      ShortDescription: `Override item ${item.sku}`,
      ExistingStock: item.quantity,
      ListPrice: item.listPrice,
      CustBestPrice: 0,
      Surcharge: 0,
      'Costo di Spedizione': '0,00',
      IVA: '22%',
      'Prezzo con spediz e IVA': '0,00',
      FeeDeRev: 0,
      'Fee Marketplace': 0,
      'Subtotale post-fee': '0,00',
      'Prezzo Finale': finalPrice.toFixed(2).replace('.', ','),
      'ListPrice con Fee': item.listPrice,
      __override: true,
      __overrideSource: 'new'
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
 */
export function validateEnding99Guard(catalog: any[]): { 
  valid: boolean; 
  failures: Array<{ index: number; ean: string; price: string }> 
} {
  const failures: Array<{ index: number; ean: string; price: string }> = [];
  
  for (let i = 0; i < catalog.length; i++) {
    const record = catalog[i];
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
