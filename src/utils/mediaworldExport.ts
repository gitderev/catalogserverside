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

// Mediaworld template column structure - Italian headers only (exact order from template)
const MEDIAWORLD_HEADERS_ITALIAN = [
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
];

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

/**
 * Parse "Prezzo Finale" from EAN catalog format to number
 * Handles both string format "34,99" and numeric format 34.99
 */
function parsePrezzoFinale(value: any): number | null {
  if (value === null || value === undefined || value === '') return null;
  
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  
  // Handle string format "34,99" -> 34.99
  const str = String(value).trim().replace(',', '.');
  const num = parseFloat(str);
  return Number.isFinite(num) ? num : null;
}

/**
 * Parse "ListPrice con Fee" from EAN catalog
 * This should already be an integer from EAN export
 */
function parseListPriceConFee(value: any): number | null {
  if (value === null || value === undefined || value === '') return null;
  
  if (typeof value === 'number' && Number.isFinite(value)) {
    return Math.round(value); // Ensure integer
  }
  
  const str = String(value).trim().replace(',', '.');
  const num = parseFloat(str);
  return Number.isFinite(num) ? Math.round(num) : null;
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
      
      if (sku.includes('/')) {
        validationErrors.push({
          row: index + 1,
          sku,
          field: 'SKU offerta',
          reason: 'SKU contiene carattere "/" non accettato'
        });
        skippedCount++;
        return;
      }
      
      // === GET PRICES FROM ALREADY CALCULATED EAN DATA ===
      // IMPORTANTE: NON ricalcoliamo i prezzi, usiamo quelli già calcolati
      
      // "ListPrice con Fee" - already calculated in EAN pipeline
      const listPriceConFee = parseListPriceConFee(record['ListPrice con Fee']);
      
      // "Prezzo Finale" - already calculated in EAN pipeline (format "NN,99" or number)
      const prezzoFinale = parsePrezzoFinale(record['Prezzo Finale']);
      
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
      
      // Price range validation (between 1€ and 100000€)
      if (prezzoFinale < 1 || prezzoFinale > 100000) {
        validationErrors.push({
          row: index + 1,
          sku,
          field: 'Prezzo scontato',
          reason: `Prezzo finale fuori range (${prezzoFinale}): deve essere tra 1€ e 100000€`
        });
        skippedCount++;
        return;
      }
      
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
      
      // NOTE: Prezzo dell'offerta (col 5) and Prezzo scontato (col 13) 
      // are kept as NUMBERS, not formatted as text
      // This ensures Excel treats them as numeric values with decimal points
    }
    
    XLSX.utils.book_append_sheet(wb, dataSheet, 'Data');
    
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
