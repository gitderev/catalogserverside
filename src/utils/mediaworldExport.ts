import * as XLSX from 'xlsx';

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
  const rowCount = range.e.r - range.s.r; // excluding header
  if (rowCount === 0) {
    errors.push("Nessuna riga dati presente");
    return { isValid: false, errors, warnings };
  }

  // Sample validation of first 5 data rows
  const sampleSize = Math.min(5, rowCount);
  for (let R = 1; R <= sampleSize; R++) {
    const rowNum = R + 1; // Excel row number (1-indexed + header)
    
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
      for (let R = 1; R <= Math.min(3, rowCount); R++) {
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
  prepDays
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
    
    // Extract ReferenceData and Columns sheets from template (copy exactly as-is)
    const referenceDataSheet = templateWb.Sheets['ReferenceData'];
    const columnsSheet = templateWb.Sheets['Columns'];
    
    // Build data rows with mapping
    // Row 1: Italian headers ONLY (no technical codes row)
    const dataRows: (string | number)[][] = [MEDIAWORLD_HEADERS_ITALIAN];
    
    // Process each record from EAN catalog data
    processedData.forEach((record, index) => {
      const sku = record.ManufPartNr || '';
      const ean = record.EAN || '';
      const existingStock = record.ExistingStock;
      
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
      
      // === FILTER 2: Skip products with stock <= 0 ===
      const stockValue = Number(existingStock);
      if (!Number.isFinite(stockValue) || stockValue <= 0) {
        validationErrors.push({
          row: index + 1,
          sku,
          field: 'Quantità',
          reason: `ExistingStock assente o <= 0: ${existingStock}`
        });
        skippedCount++;
        return;
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
      const listPriceConFee = parsePrice(record['ListPrice con Fee']);
      
      // "Prezzo Finale" - already calculated in EAN pipeline (format "NN,99" or number)
      const prezzoFinale = parsePrice(record['Prezzo Finale']);
      
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
        Math.floor(stockValue),                      // Col 8: Quantità dell'offerta → ExistingStock (INTEGER)
        '',                                          // Col 9: Avviso quantità minima → vuoto
        'Nuovo',                                     // Col 10: Stato dell'offerta → "Nuovo" (fixed)
        '',                                          // Col 11: Data di inizio disponibilità → vuoto
        '',                                          // Col 12: Data di conclusione disponibilità → vuoto
        'Consegna gratuita',                         // Col 13: Classe logistica → "Consegna gratuita"
        prezzoFinale,                                // Col 14: Prezzo scontato → Prezzo Finale (NUMBER)
        '',                                          // Col 15: Data di inizio dello sconto → vuoto
        '',                                          // Col 16: Data di termine dello sconto → vuoto
        prepDays,                                    // Col 17: Tempo preparazione spedizione (INTEGER)
        '',                                          // Col 18: Aggiorna/Cancella → vuoto
        'recommended-retail-price',                  // Col 19: Tipo prezzo barrato → fixed
        '',                                          // Col 20: Obbligo di ritiro RAEE → vuoto
        '',                                          // Col 21: Orario di cut-off → vuoto
        ''                                           // Col 22: VAT Rate % (Turkey only) → vuoto
      ];
      
      dataRows.push(row);
    });
    
    // Check if we have valid data rows after validation
    const validRowCount = dataRows.length - 1; // Exclude header row
    
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
    
    // Create new workbook with 3 sheets
    const wb = XLSX.utils.book_new();
    
    // Sheet 1: Data
    const dataSheet = XLSX.utils.aoa_to_sheet(dataRows);
    
    // Force ID Prodotto (column B, index 1) to text format to preserve leading zeros
    // Data rows start at row 2 (index 1) since row 1 is headers
    if (dataSheet['!ref']) {
      const range = XLSX.utils.decode_range(dataSheet['!ref']);
      const eanCol = 1; // Column B (ID Prodotto)
      
      // Start from R=1 (first data row after header)
      for (let R = 1; R <= range.e.r; R++) {
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
      
      for (let R = 1; R <= range.e.r; R++) {
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
    }
    
    XLSX.utils.book_append_sheet(wb, dataSheet, 'Data');
    
    // =====================================================================
    // VALIDAZIONE AUTOMATICA: Verifica struttura conforme al template
    // ufficiale Mediaworld prima del download
    // =====================================================================
    const structureValidation = validateMediaworldStructure(dataSheet, 'Data');
    
    if (!structureValidation.isValid) {
      console.error('Mediaworld template validation FAILED:', structureValidation.errors);
      return {
        success: false,
        error: `File non conforme al template Mediaworld: ${structureValidation.errors.join('; ')}`,
        validationErrors,
        skippedCount
      };
    }
    
    if (structureValidation.warnings.length > 0) {
      console.warn('Mediaworld template validation warnings:', structureValidation.warnings);
    }
    
    // Sheet 2: ReferenceData (copy from template exactly as-is)
    if (referenceDataSheet) {
      XLSX.utils.book_append_sheet(wb, referenceDataSheet, 'ReferenceData');
    } else {
      const emptyRefSheet = XLSX.utils.aoa_to_sheet([['ReferenceData']]);
      XLSX.utils.book_append_sheet(wb, emptyRefSheet, 'ReferenceData');
    }
    
    // Sheet 3: Columns (copy from template exactly as-is)
    if (columnsSheet) {
      XLSX.utils.book_append_sheet(wb, columnsSheet, 'Columns');
    } else {
      const emptyColSheet = XLSX.utils.aoa_to_sheet([['Columns']]);
      XLSX.utils.book_append_sheet(wb, emptyColSheet, 'Columns');
    }
    
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
