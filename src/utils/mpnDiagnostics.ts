/**
 * MPN Diagnostics - Raw vs Post-Parse Analysis
 * 
 * Provides diagnostic utilities to detect if scientific notation
 * in MPN/SKU fields is present in the raw file or introduced by parsing/import.
 * 
 * DEFINITIONS:
 * - "Scientific notation" = entire string matches regex: ^[+-]?\d+(?:[.,]\d+)?[eE][+-]?\d+$
 * - "E+ substring" = contains "E+" but does NOT match scientific notation regex (valid SKUs)
 * 
 * WARNING GATING:
 * - coercion = (rawSci=0 && postSci>0) → WARNING: parser introduced scientific notation
 * - divergence = (rawSci>0 && postSci!=rawSci) → WARNING: parser altered distribution
 * - rawSci>0 && divergence=0 → INFO: format exists in source file
 * - E+ substring → INFO only (valid SKUs, never warning)
 */

// Build version for cache busting workers
export const MPN_DIAGNOSTICS_VERSION = '2025-12-14-v3';

// Known MPN/SKU column names (case-insensitive, in order of priority)
const MPN_COLUMN_ALIASES = [
  'mpn', 'manufacturerpartno', 'manufpartno', 'manufacturerpartno3',
  'sku', 'partno', 'manufpartnr', 'partnumber', 'articlenumber'
];

/**
 * Detects if a string is in TRUE scientific notation format (entire string)
 * Examples that match: "1.23E+05", "5e-3", "-2.5e+10", "123e5"
 * Examples that DON'T match: "ABC1234E+XYZ", "SKU-E+123"
 */
export function isScientificNotation(value: string): boolean {
  if (!value || typeof value !== 'string') return false;
  return /^[+-]?\d+(?:[.,]\d+)?[eE][+-]?\d+$/.test(value.trim());
}

/**
 * Checks if a string contains "E+" as a substring (case-insensitive)
 * AND is NOT in scientific notation format (valid SKUs like "ABC1234E+XYZ")
 */
export function containsEPlusSubstring(value: string): boolean {
  if (!value || typeof value !== 'string') return false;
  return /e\+/i.test(value) && !isScientificNotation(value);
}

/**
 * Result of MPN column detection
 */
export interface MPNColumnDetection {
  found: boolean;
  columnIndex: number;
  columnName: string;
  allHeaders: string[];
  delimiter: string;
  delimiterMethod: 'tab' | 'semicolon' | 'fallback';
  error?: string;
}

/**
 * Detects the delimiter used in a raw file by analyzing consistency
 * of column count across the first N non-empty lines
 */
export function detectDelimiter(rawContent: string, sampleLines: number = 50): { delimiter: string; method: 'tab' | 'semicolon' | 'fallback' } {
  const lines = rawContent.split('\n').filter(l => l.trim()).slice(0, sampleLines);
  if (lines.length === 0) return { delimiter: ';', method: 'fallback' };
  
  // Count columns for tab delimiter
  const tabCounts = lines.map(l => l.split('\t').length);
  const tabStdDev = calculateStdDev(tabCounts);
  const tabAvg = tabCounts.reduce((a, b) => a + b, 0) / tabCounts.length;
  
  // Count columns for semicolon delimiter
  const semicolonCounts = lines.map(l => l.split(';').length);
  const semicolonStdDev = calculateStdDev(semicolonCounts);
  const semicolonAvg = semicolonCounts.reduce((a, b) => a + b, 0) / semicolonCounts.length;
  
  // Choose delimiter with lowest standard deviation (most consistent column count)
  // and at least 2 columns on average
  if (tabAvg >= 2 && tabStdDev <= semicolonStdDev) {
    return { delimiter: '\t', method: 'tab' };
  } else if (semicolonAvg >= 2) {
    return { delimiter: ';', method: 'semicolon' };
  } else if (tabAvg >= 2) {
    return { delimiter: '\t', method: 'tab' };
  }
  
  return { delimiter: ';', method: 'fallback' };
}

function calculateStdDev(values: number[]): number {
  if (values.length === 0) return Infinity;
  const avg = values.reduce((a, b) => a + b, 0) / values.length;
  const variance = values.reduce((sum, val) => sum + Math.pow(val - avg, 2), 0) / values.length;
  return Math.sqrt(variance);
}

/**
 * Finds the MPN column index from headers (case-insensitive)
 * Uses priority order from MPN_COLUMN_ALIASES
 */
export function findMPNColumn(headers: string[], delimiter: string): MPNColumnDetection {
  const lowerHeaders = headers.map(h => h.toLowerCase().trim().replace(/"/g, ''));
  
  // Search in priority order
  for (const alias of MPN_COLUMN_ALIASES) {
    const index = lowerHeaders.indexOf(alias);
    if (index >= 0) {
      return {
        found: true,
        columnIndex: index,
        columnName: headers[index].replace(/"/g, ''),
        allHeaders: headers,
        delimiter,
        delimiterMethod: delimiter === '\t' ? 'tab' : 'semicolon'
      };
    }
  }
  
  return {
    found: false,
    columnIndex: -1,
    columnName: '',
    allHeaders: headers,
    delimiter,
    delimiterMethod: delimiter === '\t' ? 'tab' : 'semicolon',
    error: `MPN column not found. Searched for: ${MPN_COLUMN_ALIASES.join(', ')}. Headers found: ${headers.join(', ')}`
  };
}

/**
 * Example entry for diagnostic reports
 */
export interface MPNDiagnosticExample {
  rowIndex: number;
  mpnValue: string;
  rawLineSnippet: string;
}

/**
 * Raw scan results for MPN column
 */
export interface RawMPNScanResult {
  totalRows: number;
  rawSciInMpn: number;
  rawEPlusInMpn: number;
  sciExamples: MPNDiagnosticExample[];
  ePlusExamples: MPNDiagnosticExample[];
  columnDetection: MPNColumnDetection;
}

/**
 * Post-parse scan results for MPN column
 */
export interface PostParseMPNScanResult {
  totalRows: number;
  postSciInMpn: number;
  postEPlusInMpn: number;
  sciExamples: MPNDiagnosticExample[];
  ePlusExamples: MPNDiagnosticExample[];
  typeErrors: number; // MPN values that are not strings
  typeErrorExamples: Array<{ rowIndex: number; value: any; type: string }>;
}

/**
 * Combined diagnostics result with WARNING/INFO classification
 */
export interface MPNDiagnosticsResult {
  fileType: 'material' | 'mapping';
  filename: string;
  raw: RawMPNScanResult;
  post: PostParseMPNScanResult;
  // Derived warning indicators
  coercionSciInMpn: number; // rawSci=0 && postSci>0 → parser introduced
  divergenceSciInMpn: number; // rawSci>0 && postSci!=rawSci → parser altered
  hasWarning: boolean; // coercion>0 OR divergence>0
  warningMessages: string[];
  infoMessages: string[];
}

/**
 * Performs raw scan on MPN column BEFORE parsing
 * @param rawContent Raw file content as string
 * @param explicitDelimiter Optional explicit delimiter (auto-detected if not provided)
 * @returns Raw scan results
 */
export function performRawMPNScan(
  rawContent: string,
  explicitDelimiter?: string
): RawMPNScanResult {
  // Detect delimiter if not provided
  const delimiterResult = explicitDelimiter 
    ? { delimiter: explicitDelimiter, method: explicitDelimiter === '\t' ? 'tab' as const : 'semicolon' as const }
    : detectDelimiter(rawContent);
  const delimiter = delimiterResult.delimiter;
  
  const lines = rawContent.split('\n');
  
  // Get headers from first line
  const headerLine = lines[0] || '';
  const headers = headerLine.split(delimiter).map(h => h.trim().replace(/"/g, ''));
  
  const columnDetection = findMPNColumn(headers, delimiter);
  
  if (!columnDetection.found) {
    return {
      totalRows: 0,
      rawSciInMpn: 0,
      rawEPlusInMpn: 0,
      sciExamples: [],
      ePlusExamples: [],
      columnDetection
    };
  }
  
  let rawSciInMpn = 0;
  let rawEPlusInMpn = 0;
  const sciExamples: MPNDiagnosticExample[] = [];
  const ePlusExamples: MPNDiagnosticExample[] = [];
  let dataRowCount = 0;
  
  // Scan data rows (skip header)
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line || line.trim() === '') continue;
    
    dataRowCount++;
    const cells = line.split(delimiter);
    const mpnValue = (cells[columnDetection.columnIndex] || '').trim().replace(/"/g, '');
    
    if (!mpnValue) continue;
    
    const isSci = isScientificNotation(mpnValue);
    const hasEPlus = containsEPlusSubstring(mpnValue);
    
    if (isSci) {
      rawSciInMpn++;
      if (sciExamples.length < 20) {
        sciExamples.push({
          rowIndex: i + 1, // 1-indexed
          mpnValue,
          rawLineSnippet: line.substring(0, 200)
        });
      }
    } else if (hasEPlus) {
      rawEPlusInMpn++;
      if (ePlusExamples.length < 20) {
        ePlusExamples.push({
          rowIndex: i + 1,
          mpnValue,
          rawLineSnippet: line.substring(0, 200)
        });
      }
    }
  }
  
  return {
    totalRows: dataRowCount,
    rawSciInMpn,
    rawEPlusInMpn,
    sciExamples,
    ePlusExamples,
    columnDetection
  };
}

/**
 * Performs post-parse scan on MPN column AFTER parsing
 * @param parsedData Array of parsed records
 * @param mpnFieldName Name of the MPN field in records (e.g., 'ManufPartNr', 'mpn')
 * @returns Post-parse scan results
 */
export function performPostParseMPNScan(
  parsedData: any[],
  mpnFieldName: string
): PostParseMPNScanResult {
  let postSciInMpn = 0;
  let postEPlusInMpn = 0;
  let typeErrors = 0;
  const sciExamples: MPNDiagnosticExample[] = [];
  const ePlusExamples: MPNDiagnosticExample[] = [];
  const typeErrorExamples: Array<{ rowIndex: number; value: any; type: string }> = [];
  
  parsedData.forEach((row, index) => {
    const rawValue = row[mpnFieldName];
    const mpnValue = String(rawValue ?? '').trim();
    
    // Check if value was coerced to number
    if (rawValue !== null && rawValue !== undefined && typeof rawValue === 'number') {
      typeErrors++;
      if (typeErrorExamples.length < 20) {
        typeErrorExamples.push({
          rowIndex: index + 1,
          value: rawValue,
          type: typeof rawValue
        });
      }
    }
    
    if (!mpnValue) return;
    
    const isSci = isScientificNotation(mpnValue);
    const hasEPlus = containsEPlusSubstring(mpnValue);
    
    if (isSci) {
      postSciInMpn++;
      if (sciExamples.length < 20) {
        sciExamples.push({
          rowIndex: index + 1,
          mpnValue,
          rawLineSnippet: `Row ${index + 1}: ${mpnValue}`
        });
      }
    } else if (hasEPlus) {
      postEPlusInMpn++;
      if (ePlusExamples.length < 20) {
        ePlusExamples.push({
          rowIndex: index + 1,
          mpnValue,
          rawLineSnippet: `Row ${index + 1}: ${mpnValue}`
        });
      }
    }
  });
  
  return {
    totalRows: parsedData.length,
    postSciInMpn,
    postEPlusInMpn,
    sciExamples,
    ePlusExamples,
    typeErrors,
    typeErrorExamples
  };
}

/**
 * Performs full MPN diagnostics comparing raw vs post-parse
 * with proper WARNING/INFO classification
 */
export function performMPNDiagnostics(
  rawContent: string,
  parsedData: any[],
  fileType: 'material' | 'mapping',
  filename: string,
  explicitDelimiter?: string,
  mpnFieldName?: string
): MPNDiagnosticsResult {
  // Raw scan
  const raw = performRawMPNScan(rawContent, explicitDelimiter);
  
  // Determine MPN field name for parsed data
  const fieldName = mpnFieldName || (raw.columnDetection.found ? raw.columnDetection.columnName : 'ManufPartNr');
  
  // Post-parse scan
  const post = performPostParseMPNScan(parsedData, fieldName);
  
  // Calculate warning indicators
  const coercionSciInMpn = (raw.rawSciInMpn === 0 && post.postSciInMpn > 0) ? post.postSciInMpn : 0;
  const divergenceSciInMpn = (raw.rawSciInMpn > 0 && post.postSciInMpn !== raw.rawSciInMpn) 
    ? Math.abs(post.postSciInMpn - raw.rawSciInMpn) 
    : 0;
  
  const hasWarning = coercionSciInMpn > 0 || divergenceSciInMpn > 0;
  
  // Build warning messages
  const warningMessages: string[] = [];
  if (coercionSciInMpn > 0) {
    warningMessages.push(`⚠️ COERCIZIONE: Il parser ha introdotto ${coercionSciInMpn} MPN in notazione scientifica (RAW: 0, POST: ${post.postSciInMpn})`);
  }
  if (divergenceSciInMpn > 0) {
    warningMessages.push(`⚠️ DIVERGENZA: Il parser ha alterato la distribuzione (RAW: ${raw.rawSciInMpn}, POST: ${post.postSciInMpn}, diff: ${divergenceSciInMpn})`);
  }
  if (post.typeErrors > 0) {
    warningMessages.push(`⚠️ ${post.typeErrors} MPN convertiti a numero dal parser (type coercion)`);
  }
  
  // Build info messages
  const infoMessages: string[] = [];
  if (raw.rawSciInMpn > 0 && divergenceSciInMpn === 0) {
    infoMessages.push(`ℹ️ ${raw.rawSciInMpn} MPN in formato scientifico presenti nel file sorgente (non è un errore del tool)`);
  }
  if (raw.rawEPlusInMpn > 0) {
    infoMessages.push(`ℹ️ ${raw.rawEPlusInMpn} SKU validi con "E+" nel file sorgente`);
  }
  
  return {
    fileType,
    filename,
    raw,
    post,
    coercionSciInMpn,
    divergenceSciInMpn,
    hasWarning,
    warningMessages,
    infoMessages
  };
}

/**
 * Logs diagnostics to console with formatting
 */
export function logMPNDiagnostics(diagnostics: MPNDiagnosticsResult): void {
  const prefix = `[MPN-Diagnostics:${diagnostics.fileType}]`;
  
  console.group(`%c${prefix} ${diagnostics.filename}`, 'color: #E91E63; font-weight: bold;');
  
  console.log('Column detection:', {
    found: diagnostics.raw.columnDetection.found,
    columnName: diagnostics.raw.columnDetection.columnName,
    columnIndex: diagnostics.raw.columnDetection.columnIndex,
    delimiter: diagnostics.raw.columnDetection.delimiterMethod
  });
  
  console.log('%cRaw Scan (MPN column only):', 'color: #2196F3; font-weight: bold;', {
    totalRows: diagnostics.raw.totalRows,
    rawSciInMpn: diagnostics.raw.rawSciInMpn,
    rawEPlusInMpn: diagnostics.raw.rawEPlusInMpn
  });
  
  if (diagnostics.raw.sciExamples.length > 0) {
    console.log('Raw SCI examples:', diagnostics.raw.sciExamples);
  }
  if (diagnostics.raw.ePlusExamples.length > 0) {
    console.log('Raw E+ examples (first 5):', diagnostics.raw.ePlusExamples.slice(0, 5));
  }
  
  console.log('%cPost-Parse Scan:', 'color: #4CAF50; font-weight: bold;', {
    totalRows: diagnostics.post.totalRows,
    postSciInMpn: diagnostics.post.postSciInMpn,
    postEPlusInMpn: diagnostics.post.postEPlusInMpn,
    typeErrors: diagnostics.post.typeErrors
  });
  
  if (diagnostics.post.sciExamples.length > 0) {
    console.log('Post SCI examples:', diagnostics.post.sciExamples);
  }
  if (diagnostics.post.typeErrorExamples.length > 0) {
    console.log('Type error examples:', diagnostics.post.typeErrorExamples);
  }
  
  // WARNING/INFO classification
  console.log('%cClassification:', 'color: #FF9800; font-weight: bold;', {
    coercionSciInMpn: diagnostics.coercionSciInMpn,
    divergenceSciInMpn: diagnostics.divergenceSciInMpn,
    hasWarning: diagnostics.hasWarning
  });
  
  if (diagnostics.warningMessages.length > 0) {
    console.warn('%cWARNINGS:', 'color: #F44336; font-weight: bold;');
    diagnostics.warningMessages.forEach(msg => console.warn(msg));
  }
  
  if (diagnostics.infoMessages.length > 0) {
    console.info('%cINFO:', 'color: #2196F3; font-weight: bold;');
    diagnostics.infoMessages.forEach(msg => console.info(msg));
  }
  
  if (!diagnostics.hasWarning && diagnostics.raw.rawSciInMpn === 0 && diagnostics.post.postSciInMpn === 0) {
    console.log('%c✅ No coercion: Raw and Post both have 0 scientific notation', 'color: #4CAF50; font-weight: bold;');
  }
  
  console.groupEnd();
}

/**
 * Formats diagnostics for UI display with proper WARNING/INFO classification
 */
export function formatDiagnosticsForUI(diagnostics: MPNDiagnosticsResult): {
  counters: Record<string, number>;
  warnings: string[];
  info: string[];
} {
  const counters: Record<string, number> = {
    'RAW scientifico': diagnostics.raw.rawSciInMpn,
    'RAW con E+ (validi)': diagnostics.raw.rawEPlusInMpn,
    'POST scientifico': diagnostics.post.postSciInMpn,
    'POST con E+ (validi)': diagnostics.post.postEPlusInMpn,
    'Coercizione (RAW=0→POST>0)': diagnostics.coercionSciInMpn,
    'Divergenza (RAW≠POST)': diagnostics.divergenceSciInMpn
  };
  
  return { 
    counters, 
    warnings: diagnostics.warningMessages,
    info: diagnostics.infoMessages
  };
}

/**
 * Print examples to console for debugging
 */
export function printMPNExamplesToConsole(diagnostics: MPNDiagnosticsResult): void {
  const prefix = `[MPN-Examples:${diagnostics.fileType}]`;
  
  console.group(`%c${prefix} ${diagnostics.filename} - EXAMPLES`, 'color: #9C27B0; font-weight: bold; font-size: 14px;');
  
  if (diagnostics.raw.sciExamples.length > 0) {
    console.log('%cRAW Scientific Notation Examples (20 max):', 'color: #F44336; font-weight: bold;');
    console.table(diagnostics.raw.sciExamples);
  } else {
    console.log('%cRAW Scientific Notation: 0 examples', 'color: #4CAF50;');
  }
  
  if (diagnostics.raw.ePlusExamples.length > 0) {
    console.log('%cRAW E+ Substring Examples (20 max):', 'color: #2196F3; font-weight: bold;');
    console.table(diagnostics.raw.ePlusExamples);
  } else {
    console.log('%cRAW E+ Substring: 0 examples', 'color: #4CAF50;');
  }
  
  if (diagnostics.post.sciExamples.length > 0) {
    console.log('%cPOST Scientific Notation Examples (20 max):', 'color: #F44336; font-weight: bold;');
    console.table(diagnostics.post.sciExamples);
  } else {
    console.log('%cPOST Scientific Notation: 0 examples', 'color: #4CAF50;');
  }
  
  if (diagnostics.post.ePlusExamples.length > 0) {
    console.log('%cPOST E+ Substring Examples (20 max):', 'color: #2196F3; font-weight: bold;');
    console.table(diagnostics.post.ePlusExamples);
  } else {
    console.log('%cPOST E+ Substring: 0 examples', 'color: #4CAF50;');
  }
  
  if (diagnostics.post.typeErrorExamples.length > 0) {
    console.log('%cType Error Examples (MPN coerced to number):', 'color: #FF5722; font-weight: bold;');
    console.table(diagnostics.post.typeErrorExamples);
  }
  
  console.groupEnd();
}
