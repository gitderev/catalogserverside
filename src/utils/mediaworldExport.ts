import * as XLSX from 'xlsx';
import { 
  resolveMarketplaceStock, 
  getStockForMatnr, 
  checkSplitMismatch,
  type StockLocationIndex,
  type StockLocationWarnings
} from './stockLocation';

/**
 * Mediaworld Export Utility
 * 
 * IMPORTANTE: Questo modulo NON ricalcola i prezzi.
 * Usa DIRETTAMENTE i valori già calcolati nel catalogo EAN:
 * - "ListPrice con Fee" → Prezzo dell'offerta
 * - "Prezzo Finale" → Prezzo scontato
 * 
 * Ogni modifica alla logica di pricing nel Catalog Generator
 * si riflette automaticamente anche nell'export Mediaworld.
 * 
 * IT/EU Stock Split: Uses resolveMarketplaceStock for quantity and lead time.
 * 
 * LEAD TIME: Mediaworld lead time = exactly stockResult.leadDays (NO offset).
 * The value exported matches exactly the UI configuration (IT days or EU days).
 */

// =====================================================================
// TEMPLATE MEDIAWORLD UFFICIALE: Struttura e validazione
// Questi valori devono corrispondere esattamente al template ufficiale
// "mediaworld-template.xlsx"
// =====================================================================
const MEDIAWORLD_TEMPLATE = {
  sheetName: "Data",
  headers: [
    'SKU offerta',
    'ID Prodotto',
    'Tipo ID prodotto',
    'Descrizione offerta',
    'Descrizione interna offerta',
    "Prezzo dell'offerta",
    'Info aggiuntive prezzo offerta',
    "Quantità dell'offerta",
    'Avviso quantità minima',
    "Stato dell'offerta",
    'Data di inizio della disponibilità',
    'Data di conclusione della disponibilità',
    'Classe logistica',
    'Prezzo scontato',
    'Data di inizio dello sconto',
    'Data di termine dello sconto',
    'Tempo di preparazione della spedizione (in giorni)',
    'Aggiorna/Cancella',
    'Tipo di prezzo che verrà barrato quando verrà definito un prezzo scontato.',
    'Obbligo di ritiro RAEE',
    'Orario di cut-off (solo se la consegna il giorno successivo è abilitata)',
    'VAT Rate % (Turkey only)'
  ],
  columnCount: 22,
  fixedValues: {
    "Tipo ID prodotto": "EAN",
    "Stato dell'offerta": "Nuovo",
    "Classe logistica": "Consegna gratuita",
    "Tipo di prezzo che verrà barrato quando verrà definito un prezzo scontato.": "recommended-retail-price"
  },
  validations: {
    'SKU offerta': { type: "string", required: true, minLength: 1, maxLength: 40 },
    'ID Prodotto': { type: "string", required: true, pattern: /^\d{12,14}$/ },
    'Tipo ID prodotto': { type: "string", required: true, value: "EAN" },
    'Descrizione offerta': { type: "string", required: false },
    'Descrizione interna offerta': { type: "string", required: false },
    "Prezzo dell'offerta": { type: "number", required: true, min: 1, max: 100000, decimals: 2 },
    'Info aggiuntive prezzo offerta': { type: "string", required: false },
    "Quantità dell'offerta": { type: "integer", required: true, min: 1 },
    'Avviso quantità minima': { type: "string", required: false },
    "Stato dell'offerta": { type: "string", required: true, value: "Nuovo" },
    'Data di inizio della disponibilità': { type: "string", required: false },
    'Data di conclusione della disponibilità': { type: "string", required: false },
    'Classe logistica': { type: "string", required: true, value: "Consegna gratuita" },
    'Prezzo scontato': { type: "number", required: true, min: 1, decimals: 2 },
    'Data di inizio dello sconto': { type: "string", required: false },
    'Data di termine dello sconto': { type: "string", required: false },
    'Tempo di preparazione della spedizione (in giorni)': { type: "integer", required: true, min: 0, max: 365 },
    'Aggiorna/Cancella': { type: "string", required: false },
    'Tipo di prezzo che verrà barrato quando verrà definito un prezzo scontato.': { type: "string", required: true, value: "recommended-retail-price" },
    'Obbligo di ritiro RAEE': { type: "string", required: false },
    'Orario di cut-off (solo se la consegna il giorno successivo è abilitata)': { type: "string", required: false },
    'VAT Rate % (Turkey only)': { type: "string", required: false }
  }
};

// Keep MEDIAWORLD_HEADERS_ITALIAN for backward compatibility
const MEDIAWORLD_HEADERS_ITALIAN = MEDIAWORLD_TEMPLATE.headers;

interface MediaworldExportParams {
  processedData: any[];
  feeConfig: {
    feeDrev: number;
    feeMkt: number;
    shippingCost: number;
  };
  prepDays: number;
  // IT/EU stock config
  stockLocationIndex?: StockLocationIndex | null;
  stockLocationWarnings?: StockLocationWarnings;
  includeEu: boolean;
  itDays: number;
  euDays: number;
}

interface ValidationError {
  row: number;
  sku: string;
  field: string;
  reason: string;
}

interface TemplateValidation {
  type: "string" | "number" | "integer";
  required: boolean;
  minLength?: number;
  maxLength?: number;
  pattern?: RegExp;
  value?: string | number;
  min?: number;
  max?: number;
  decimals?: number;
}

/**
 * Validate Mediaworld file structure against official template
 */
function validateMediaworldStructure(
  ws: XLSX.WorkSheet,
  sheetName: string
): { isValid: boolean; errors: string[]; warnings: string[] } {
  const errors: string[] = [];
  const warnings: string[] = [];

  // 1. Validate sheet name
  if (sheetName !== MEDIAWORLD_TEMPLATE.sheetName) {
    warnings.push(`Nome foglio: "${sheetName}" invece di "${MEDIAWORLD_TEMPLATE.sheetName}"`);
  }

  // 2. Validate sheet exists and has data
  if (!ws['!ref']) {
    errors.push("Foglio vuoto o non valido");
    return { isValid: false, errors, warnings };
  }

  const range = XLSX.utils.decode_range(ws['!ref']);
  
  // 3. Validate column count
  const actualColumnCount = range.e.c - range.s.c + 1;
  if (actualColumnCount !== MEDIAWORLD_TEMPLATE.columnCount) {
    errors.push(`Numero colonne: ${actualColumnCount} invece di ${MEDIAWORLD_TEMPLATE.columnCount}`);
  }

  // 4. Validate headers (row 0)
  const actualHeaders: string[] = [];
  for (let C = range.s.c; C <= range.e.c; C++) {
    const addr = XLSX.utils.encode_cell({ r: 0, c: C });
    const cell = ws[addr];
    actualHeaders.push(cell?.v?.toString() || '');
  }

  // Check header order and names
  for (let i = 0; i < MEDIAWORLD_TEMPLATE.headers.length; i++) {
    const expected = MEDIAWORLD_TEMPLATE.headers[i];
    const actual = actualHeaders[i] || '';
    if (actual !== expected) {
      errors.push(`Header colonna ${i + 1}: "${actual}" invece di "${expected}"`);
    }
  }

  // 5. Validate data rows
  // Data starts at row 3 (index 2), after Italian headers (row 1) and technical codes (row 2)
  const DATA_START_ROW = 2;
  const dataRowCount = range.e.r - DATA_START_ROW + 1; // Number of data rows
  if (dataRowCount <= 0) {
    errors.push("Nessuna riga dati presente");
    return { isValid: false, errors, warnings };
  }

  // Sample validation of first 5 data rows
  const sampleSize = Math.min(5, dataRowCount);
  for (let i = 0; i < sampleSize; i++) {
    const R = DATA_START_ROW + i; // Actual row index (starting from 2)
    const rowNum = R + 1; // Excel row number (1-indexed)
    
    // Validate each column
    for (let C = 0; C < MEDIAWORLD_TEMPLATE.headers.length; C++) {
      const colName = MEDIAWORLD_TEMPLATE.headers[C];
      const addr = XLSX.utils.encode_cell({ r: R, c: C });
      const cell = ws[addr];
      const validation = MEDIAWORLD_TEMPLATE.validations[colName as keyof typeof MEDIAWORLD_TEMPLATE.validations] as TemplateValidation | undefined;
      
      if (!validation) continue;

      const value = cell?.v;

      // Required check
      if (validation.required && (value === undefined || value === null || value === '')) {
        errors.push(`Riga ${rowNum}, ${colName}: valore mancante`);
        continue;
      }

      // Skip validation for empty non-required fields
      if (!validation.required && (value === undefined || value === null || value === '')) {
        continue;
      }

      // Type validation
      if (validation.type === 'number') {
        if (typeof value !== 'number' || !Number.isFinite(value)) {
          errors.push(`Riga ${rowNum}, ${colName}: deve essere un numero (trovato: ${typeof value})`);
        } else {
          if (validation.min !== undefined && value < validation.min) {
            errors.push(`Riga ${rowNum}, ${colName}: valore ${value} sotto il minimo ${validation.min}`);
          }
          if (validation.max !== undefined && value > validation.max) {
            errors.push(`Riga ${rowNum}, ${colName}: valore ${value} sopra il massimo ${validation.max}`);
          }
        }
      } else if (validation.type === 'integer') {
        if (typeof value !== 'number' || !Number.isInteger(value)) {
          errors.push(`Riga ${rowNum}, ${colName}: deve essere un intero (trovato: ${value})`);
        } else {
          if (validation.min !== undefined && value < validation.min) {
            errors.push(`Riga ${rowNum}, ${colName}: valore ${value} sotto il minimo ${validation.min}`);
          }
          if (validation.max !== undefined && value > validation.max) {
            errors.push(`Riga ${rowNum}, ${colName}: valore ${value} sopra il massimo ${validation.max}`);
          }
        }
      } else if (validation.type === 'string') {
        if (validation.pattern && typeof value === 'string') {
          if (!validation.pattern.test(value)) {
            errors.push(`Riga ${rowNum}, ${colName}: formato non valido (${value})`);
          }
        }
        if (validation.maxLength !== undefined && typeof value === 'string' && value.length > validation.maxLength) {
          errors.push(`Riga ${rowNum}, ${colName}: lunghezza ${value.length} supera il massimo ${validation.maxLength}`);
        }
      }

      // Fixed value validation
      if (validation.value !== undefined) {
        if (value !== validation.value) {
          errors.push(`Riga ${rowNum}, ${colName}: valore "${value}" invece di "${validation.value}"`);
        }
      }
    }
  }

  // 6. Check price columns format (should be number with 2 decimals)
  const priceColumns = ["Prezzo dell'offerta", "Prezzo scontato"];
  for (const priceCol of priceColumns) {
    const priceColIndex = MEDIAWORLD_TEMPLATE.headers.indexOf(priceCol);
    if (priceColIndex >= 0) {
      // Check first 3 data rows (starting from row 3, index 2)
      for (let i = 0; i < Math.min(3, dataRowCount); i++) {
        const R = DATA_START_ROW + i;
        const addr = XLSX.utils.encode_cell({ r: R, c: priceColIndex });
        const cell = ws[addr];
        if (cell && cell.t !== 'n') {
          warnings.push(`Riga ${R + 1}, ${priceCol}: tipo cella "${cell.t}" invece di "n" (numero)`);
        }
        if (cell && cell.z !== '0.00') {
          warnings.push(`Riga ${R + 1}, ${priceCol}: formato "${cell.z || 'default'}" invece di "0.00"`);
        }
      }
    }
  }

  return {
    isValid: errors.length === 0,
    errors,
    warnings
  };
}

/**
 * Parse price from EAN catalog format to number
 * Handles both string format "34,99" (with comma) and numeric format 34.99
 * 
 * IMPORTANTE: NON arrotonda a interi. Mantiene i decimali originali.
 * I prezzi Mediaworld devono avere sempre due decimali (es. 175.99, non 176).
 */
function parsePrice(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  
  // If already a finite number, return as-is
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  
  // Handle string format: replace comma with dot, then parse
  const str = String(value).trim().replace(',', '.');
  if (!str) return null;
  
  const num = parseFloat(str);
  return Number.isFinite(num) ? num : null;
}

export async function exportMediaworldCatalog({
  processedData,
  feeConfig,
  prepDays,
  stockLocationIndex,
  stockLocationWarnings,
  includeEu,
  itDays,
  euDays
}: MediaworldExportParams): Promise<{ 
  success: boolean; 
  error?: string; 
  rowCount?: number;
  validationErrors?: ValidationError[];
  skippedCount?: number;
}> {
  try {
    // Validation arrays
    const validationErrors: ValidationError[] = [];
    let skippedCount = 0;
    
    // Load template from public folder
    const templateResponse = await fetch('/mediaworld-template.xlsx');
    if (!templateResponse.ok) {
      return { success: false, error: 'Template Mediaworld non trovato' };
    }
    
    const templateBuffer = await templateResponse.arrayBuffer();
    const templateWb = XLSX.read(templateBuffer, { type: 'array' });
    
    // NOTE: We will use templateWb directly (not create a new workbook)
    // This preserves ALL template structure: validations, named ranges, metadata
    
    // Build data rows for export (WITHOUT headers - will be inserted starting from row 3)
    const dataRows: (string | number)[][] = [];
    
    // Log per debug: tracciamento conversione prezzi
    let debugLogCount = 0;
    
    // Diagnostic counters for EU-only records
    let euOnlyCount = 0;
    let euOnlyExported = 0;
    
    // === LOG INIZIO EXPORT MEDIAWORLD (interno utility) ===
    console.log('%c[Mediaworld:util:start]', 'color: #00BCD4;', { 
      rows_input: processedData.length,
      includeEu,
      itDays,
      euDays,
      timestamp: new Date().toISOString()
    });
    
    // Process each record from EAN catalog data
    processedData.forEach((record, index) => {
      const sku = record.ManufPartNr || '';
      const ean = record.EAN || '';
      const matnr = record.Matnr || '';
      const existingStock = Number(record.ExistingStock) || 0;
      
      // === FILTER 1: Skip products without valid EAN ===
      if (!ean || ean.trim() === '' || ean.length < 12) {
        validationErrors.push({
          row: index + 1,
          sku: sku || 'N/A',
          field: 'ID Prodotto',
          reason: `EAN mancante o non valido: "${ean}"`
        });
        skippedCount++;
        return;
      }
      
      // === IT/EU STOCK LOGIC ===
      // Get IT/EU stock from location index (with fallback to existingStock)
      const warnings = stockLocationWarnings || { 
        missing_location_file: 0, invalid_location_parse: 0, missing_location_data: 0,
        split_mismatch: 0, multi_mpn_per_matnr: 0, orphan_4255: 0, 
        decode_fallback_used: 0, invalid_stock_value: 0 
      };
      
      const { stockIT, stockEU } = getStockForMatnr(
        stockLocationIndex || null,
        matnr,
        existingStock,
        warnings,
        true // useFallback
      );
      
      // Check split mismatch if location file is loaded
      if (stockLocationIndex) {
        checkSplitMismatch(stockIT, stockEU, existingStock, warnings);
      }
      
      // Use resolveMarketplaceStock for quantity and lead time
      const stockResult = resolveMarketplaceStock(
        stockIT,
        stockEU,
        includeEu,
        itDays,
        euDays
      );
      
      // Track EU-only records (IT < 2 and EU >= 2)
      const isEuOnly = stockIT < 2 && stockEU >= 2;
      if (isEuOnly) {
        euOnlyCount++;
      }
      
      // === FILTER 2: Skip products that don't meet threshold (min 2) ===
      if (!stockResult.shouldExport) {
        validationErrors.push({
          row: index + 1,
          sku,
          field: 'Quantità',
          reason: `Stock insufficiente: IT=${stockIT}, EU=${stockEU}, includeEU=${includeEu}`
        });
        skippedCount++;
        return;
      }
      
      // Track EU-only records that get exported when includeEU=true
      if (isEuOnly && stockResult.shouldExport) {
        euOnlyExported++;
      }
      
      // === VALIDATION: SKU offerta ===
      if (!sku || sku.trim() === '') {
        validationErrors.push({
          row: index + 1,
          sku: 'N/A',
          field: 'SKU offerta',
          reason: 'ManufPartNr mancante o vuoto'
        });
        skippedCount++;
        return;
      }
      
      if (sku.length > 40) {
        validationErrors.push({
          row: index + 1,
          sku,
          field: 'SKU offerta',
          reason: `SKU troppo lungo (${sku.length} caratteri, max 40)`
        });
        skippedCount++;
        return;
      }
      
      // SKU con "/" sono ora accettati (es. KVR16N11/8, SNA-DC2/35)
      
      // === GET PRICES FROM ALREADY CALCULATED EAN DATA ===
      // IMPORTANTE: NON ricalcoliamo i prezzi, usiamo DIRETTAMENTE quelli già calcolati nel catalogo EAN.
      // I prezzi vengono letti come stringhe con virgola (es. "175,99") e convertiti in numeri (175.99).
      // NON viene applicato alcun arrotondamento a interi.
      
      // "ListPrice con Fee" - already calculated in EAN pipeline
      const listPriceConFeeRaw = record['ListPrice con Fee'];
      const listPriceConFee = parsePrice(listPriceConFeeRaw);
      
      // "Prezzo Finale" - already calculated in EAN pipeline (format "NN,99" or number)
      const prezzoFinaleRaw = record['Prezzo Finale'];
      const prezzoFinale = parsePrice(prezzoFinaleRaw);
      
      // LOG ESTESO per i primi 10 record - traccia COMPLETA della conversione
      if (debugLogCount < 10) {
        const terminaCon99 = prezzoFinale !== null && (Math.round(prezzoFinale * 100) % 100) === 99;
        
        console.log(`%c[Mediaworld:export:row${index}]`, 'color: #00BCD4;', {
          EAN: ean,
          SKU: sku,
          'Prezzo Finale RAW (fonte Catalogo EAN)': prezzoFinaleRaw,
          'Prezzo Finale RAW type': typeof prezzoFinaleRaw,
          'parseFloat result': prezzoFinale,
          'valore finale in Prezzo scontato': prezzoFinale,
          'termina_con_99': terminaCon99,
          'ListPrice con Fee RAW': listPriceConFeeRaw,
          'ListPrice con Fee RAW type': typeof listPriceConFeeRaw,
          'valore in Prezzo offerta': listPriceConFee,
          'ExistingStock RAW': existingStock,
          'quantità esportata': stockResult.exportQty
        });
        
        // ALERT se mismatch
        if (!terminaCon99 && prezzoFinale !== null) {
          console.error(`%c[Mediaworld:warning:price-mismatch:row${index}] PREZZO NON TERMINA CON .99!`, 'color: red; font-weight: bold;', {
            EAN: ean,
            prezzoFinale: prezzoFinale,
            raw: prezzoFinaleRaw
          });
        }
        
        debugLogCount++;
      }
      
      // Validate prices are available
      if (listPriceConFee === null || listPriceConFee <= 0) {
        validationErrors.push({
          row: index + 1,
          sku,
          field: "Prezzo dell'offerta",
          reason: `ListPrice con Fee non valido: ${record['ListPrice con Fee']}`
        });
        skippedCount++;
        return;
      }
      
      if (prezzoFinale === null || prezzoFinale <= 0) {
        validationErrors.push({
          row: index + 1,
          sku,
          field: 'Prezzo scontato',
          reason: `Prezzo Finale non valido: ${record['Prezzo Finale']}`
        });
        skippedCount++;
        return;
      }
      
      // Nota: Il limite superiore di 100000€ è stato rimosso.
      // Il controllo prezzoFinale <= 0 è già coperto sopra (linee 394-403).
      
      // Log IT/EU stock calculation for first 20 records (diagnostic: NO offset applied)
      if (index < 20) {
        console.log(`%c[Mediaworld:export:IT_EU:row${index}]`, 'color: #00BCD4;', {
          EAN: ean,
          SKU: sku,
          stockIT, stockEU,
          includeEU: includeEu,
          exportQty: stockResult.exportQty,
          leadDays: stockResult.leadDays,
          exportedLeadDays: stockResult.leadDays, // NO offset - exported value = calculated value
          source: stockResult.source,
          verification: 'exportedLeadDays === leadDays (no offset)'
        });
      }
      
      // Mediaworld lead time: EXACTLY stockResult.leadDays (NO offset)
      // The exported value matches exactly the UI configuration
      const mediaworldLeadDays = stockResult.leadDays;
      
      // Build row according to Mediaworld template mapping
      // All 22 columns in exact order, empty strings for unused fields
      const row: (string | number)[] = [
        sku,                                         // Col 1: SKU offerta → ManufPartNr
        ean,                                         // Col 2: ID Prodotto → EAN normalizzato
        'EAN',                                       // Col 3: Tipo ID prodotto → "EAN" (fixed)
        record.ShortDescription || '',               // Col 4: Descrizione offerta → ShortDescription
        '',                                          // Col 5: Descrizione interna offerta → vuoto
        listPriceConFee,                             // Col 6: Prezzo dell'offerta → ListPrice con Fee (NUMBER)
        '',                                          // Col 7: Info aggiuntive prezzo offerta → vuoto
        stockResult.exportQty,                       // Col 8: Quantità dell'offerta → IT/EU resolved qty
        '',                                          // Col 9: Avviso quantità minima → vuoto
        'Nuovo',                                     // Col 10: Stato dell'offerta → "Nuovo" (fixed)
        '',                                          // Col 11: Data di inizio disponibilità → vuoto
        '',                                          // Col 12: Data di conclusione disponibilità → vuoto
        'Consegna gratuita',                         // Col 13: Classe logistica → "Consegna gratuita"
        prezzoFinale,                                // Col 14: Prezzo scontato → Prezzo Finale ESATTO (NUMBER)
        '',                                          // Col 15: Data di inizio dello sconto → vuoto
        '',                                          // Col 16: Data di termine dello sconto → vuoto
        mediaworldLeadDays,                          // Col 17: Tempo preparazione spedizione → leadDays (NO offset)
        '',                                          // Col 18: Aggiorna/Cancella → vuoto
        'recommended-retail-price',                  // Col 19: Tipo prezzo barrato → fixed
        '',                                          // Col 20: Obbligo di ritiro RAEE → vuoto
        '',                                          // Col 21: Orario di cut-off → vuoto
        ''                                           // Col 22: VAT Rate % (Turkey only) → vuoto
      ];
      
      // VERIFICA FINALE: il prezzo scontato deve terminare con .99
      if (prezzoFinale !== null) {
        const cents = Math.round(prezzoFinale * 100) % 100;
        if (cents !== 99) {
          console.error(`%c[Mediaworld:warning:not99:row${index}] PREZZO NON TERMINA CON .99!`, 'color: red; font-weight: bold;', {
            EAN: ean,
            prezzoFinale: prezzoFinale,
            cents: cents,
            raw: prezzoFinaleRaw
          });
        }
      }
      
      dataRows.push(row);
    });
    
    // Check if we have valid data rows after validation
    const validRowCount = dataRows.length; // No header row included in dataRows
    
    // === LOG RIEPILOGO INTERNO UTILITY MEDIAWORLD ===
    console.log('%c[Mediaworld:util:complete]', 'color: #00BCD4;', {
      rows_input: processedData.length,
      rows_exported: validRowCount,
      rows_skipped: skippedCount,
      euOnly_total: euOnlyCount,
      euOnly_exported: euOnlyExported,
      euOnly_skipped: euOnlyCount - euOnlyExported,
      includeEu,
      allineamento: processedData.length === (validRowCount + skippedCount) ? 'OK' : 'MISMATCH'
    });
    
    if (validRowCount === 0) {
      return { 
        success: false, 
        error: `Nessuna riga valida dopo la validazione. ${skippedCount} righe scartate.`,
        validationErrors,
        skippedCount
      };
    }
    
    // Log validation summary
    if (validationErrors.length > 0) {
      console.warn('Mediaworld export validation:', {
        totalInput: processedData.length,
        validOutput: validRowCount,
        skipped: skippedCount,
        errors: validationErrors.length
      });
    }
    
    // =====================================================================
    // CRITICAL FIX: Use the ORIGINAL template workbook directly
    // Do NOT create a new workbook - this preserves all validations,
    // named ranges, and metadata that Mediaworld requires
    // =====================================================================
    const wb = templateWb; // Use template workbook directly
    
    // Get the existing Data sheet from template - DO NOT recreate it
    const dataSheet = wb.Sheets['Data'];
    
    if (!dataSheet) {
      return { 
        success: false, 
        error: 'Foglio "Data" non trovato nel template Mediaworld',
        validationErrors,
        skippedCount
      };
    }
    
    // IMPORTANT: Write data starting from row 3 (rowIndex = 2, 0-indexed)
    // Row 1 (index 0) = Italian headers from template - DO NOT TOUCH
    // Row 2 (index 1) = Technical codes from template (sku, product-id, etc.) - DO NOT TOUCH
    // Row 3+ (index 2+) = Data rows - WRITE ONLY VALUES HERE
    const DATA_START_ROW = 2; // 0-indexed, corresponds to Excel row 3
    
    // Write each data row starting from row 3
    // Only write the cell values (.v), preserve any existing cell properties where possible
    dataRows.forEach((row, dataIndex) => {
      const rowIndex = DATA_START_ROW + dataIndex; // Row 2, 3, 4... (0-indexed)
      
      row.forEach((value, colIndex) => {
        const addr = XLSX.utils.encode_cell({ r: rowIndex, c: colIndex });
        
        // Create or update cell with just the value, preserving structure
        if (typeof value === 'number') {
          dataSheet[addr] = { v: value, t: 'n' };
        } else {
          dataSheet[addr] = { v: value, t: 's' };
        }
      });
    });
    
    // Update sheet range to include all data rows
    const lastRow = DATA_START_ROW + dataRows.length - 1;
    const lastCol = MEDIAWORLD_HEADERS_ITALIAN.length - 1;
    dataSheet['!ref'] = XLSX.utils.encode_range({
      s: { r: 0, c: 0 },
      e: { r: lastRow, c: lastCol }
    });
    
    // Force ID Prodotto (column B, index 1) to text format to preserve leading zeros
    // Data rows start at row 3 (index 2) since rows 1-2 are headers
    const eanCol = 1; // Column B (ID Prodotto)
    
    for (let R = DATA_START_ROW; R <= lastRow; R++) {
      const addr = XLSX.utils.encode_cell({ r: R, c: eanCol });
      const cell = dataSheet[addr];
      if (cell) {
        cell.v = (cell.v ?? '').toString();
        cell.t = 's';
        cell.z = '@';
        dataSheet[addr] = cell;
      }
    }
    
    // Format price columns with "0.00" to ensure two decimal places
    // Column indices: Prezzo dell'offerta (5) and Prezzo scontato (13)
    const priceCols = [5, 13];
    
    for (let R = DATA_START_ROW; R <= lastRow; R++) {
      for (const C of priceCols) {
        const addr = XLSX.utils.encode_cell({ r: R, c: C });
        const cell = dataSheet[addr];
        if (cell && typeof cell.v === 'number') {
          cell.t = 'n'; // number type
          cell.z = '0.00'; // format with 2 decimal places
          dataSheet[addr] = cell;
        }
      }
    }
    
    // === LOG DIAGNOSTICO: Verifica struttura prime 3 righe del foglio Data ===
    console.log('%c[Mediaworld:structure-check] Verifica righe 1-3 del foglio Data:', 'color: #FF9800; font-weight: bold;');
    for (let R = 0; R <= Math.min(2, lastRow); R++) {
      const rowData: Record<string, any> = {};
      const colsToCheck = [0, 1, 2, 5, 9, 13]; // SKU, EAN, Tipo, Prezzo offerta, State, Prezzo scontato
      const colNames = ['A (SKU)', 'B (EAN)', 'C (Tipo)', 'F (PrezzoOff)', 'J (State)', 'N (PrezzoSc)'];
      colsToCheck.forEach((C, idx) => {
        const addr = XLSX.utils.encode_cell({ r: R, c: C });
        const cell = dataSheet[addr];
        rowData[colNames[idx]] = cell ? cell.v : '(vuoto)';
      });
      const rowLabel = R === 0 ? 'Riga 1 (intestazioni IT)' : R === 1 ? 'Riga 2 (codici tecnici)' : 'Riga 3 (primo dato)';
      console.log(`  ${rowLabel}:`, rowData);
    }
    console.log('%c[Mediaworld:structure-check] Fine verifica', 'color: #FF9800;');
    
    // NOTE: The workbook already contains all sheets (Data, ReferenceData, Columns)
    // from the original template - no need to re-add them
    
    // Serialize to ArrayBuffer
    const wbout = XLSX.write(wb, { bookType: 'xlsx', type: 'array' });
    
    if (!wbout || wbout.length === 0) {
      return { success: false, error: 'Buffer vuoto durante la generazione del file' };
    }
    
    // Create blob and download
    const blob = new Blob([wbout], { 
      type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' 
    });
    
    const url = URL.createObjectURL(blob);
    
    // Generate filename with timestamp YYYYMMDD
    const now = new Date();
    const dateStamp = now.toISOString().slice(0, 10).replace(/-/g, '');
    const fileName = `mediaworld-offers-${dateStamp}.xlsx`;
    
    // Create anchor and trigger download
    const a = document.createElement('a');
    a.href = url;
    a.download = fileName;
    a.rel = 'noopener';
    a.style.display = 'none';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    
    // Delay URL revocation
    setTimeout(() => URL.revokeObjectURL(url), 3000);
    
    return { 
      success: true, 
      rowCount: validRowCount,
      validationErrors: validationErrors.length > 0 ? validationErrors : undefined,
      skippedCount: skippedCount > 0 ? skippedCount : undefined
    };
    
  } catch (error) {
    console.error('Errore export Mediaworld:', error);
    return { 
      success: false, 
      error: error instanceof Error ? error.message : 'Errore sconosciuto durante export',
      validationErrors: []
    };
  }
}
