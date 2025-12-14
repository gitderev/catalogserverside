/**
 * MPN Diagnostics - Raw vs Post-Parse Analysis
 * 
 * This module provides diagnostic utilities to detect if scientific notation
 * in MPN/SKU fields is present in the raw file or introduced by parsing/import.
 */

// Build version for cache busting workers
export const MPN_DIAGNOSTICS_VERSION = '2025-12-14-v1';

// Known MPN/SKU column names (case-insensitive)
const MPN_COLUMN_ALIASES = [
  'mpn', 'manufpartnr', 'manufacturerpartno', 'manufpartno', 
  'sku', 'partno', 'partnumber', 'articlenumber'
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
 * This is for valid SKUs like "ABC1234E+XYZ"
 */
export function containsEPlusSubstring(value: string): boolean {
  if (!value || typeof value !== 'string') return false;
  return /e\+/i.test(value);
}

/**
 * Result of MPN column detection
 */
export interface MPNColumnDetection {
  found: boolean;
  columnIndex: number;
  columnName: string;
  allHeaders: string[];
  error?: string;
}

/**
 * Finds the MPN column index from headers (case-insensitive)
 */
export function findMPNColumn(headers: string[]): MPNColumnDetection {
  const lowerHeaders = headers.map(h => h.toLowerCase().trim());
  
  for (let i = 0; i < lowerHeaders.length; i++) {
    if (MPN_COLUMN_ALIASES.includes(lowerHeaders[i])) {
      return {
        found: true,
        columnIndex: i,
        columnName: headers[i],
        allHeaders: headers
      };
    }
  }
  
  return {
    found: false,
    columnIndex: -1,
    columnName: '',
    allHeaders: headers,
    error: `MPN column not found. Headers: ${headers.join(', ')}`
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
 * Combined diagnostics result
 */
export interface MPNDiagnosticsResult {
  fileType: 'material' | 'mapping';
  filename: string;
  raw: RawMPNScanResult;
  post: PostParseMPNScanResult;
  coercionDetected: boolean; // true if rawSci=0 but postSci>0
  coercionMessage?: string;
}

/**
 * Performs raw scan on MPN column BEFORE parsing
 * @param rawContent Raw file content as string
 * @param delimiter Field delimiter (';' for CSV, '\t' for TSV)
 * @returns Raw scan results
 */
export function performRawMPNScan(
  rawContent: string,
  delimiter: string = ';'
): RawMPNScanResult {
  const lines = rawContent.split('\n');
  
  // Get headers from first line
  const headerLine = lines[0] || '';
  const headers = headerLine.split(delimiter).map(h => h.trim().replace(/"/g, ''));
  
  const columnDetection = findMPNColumn(headers);
  
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
    const hasEPlus = containsEPlusSubstring(mpnValue) && !isSci;
    
    if (isSci) {
      rawSciInMpn++;
      if (sciExamples.length < 20) {
        sciExamples.push({
          rowIndex: i + 1, // 1-indexed
          mpnValue,
          rawLineSnippet: line.substring(0, 100)
        });
      }
    } else if (hasEPlus) {
      rawEPlusInMpn++;
      if (ePlusExamples.length < 20) {
        ePlusExamples.push({
          rowIndex: i + 1,
          mpnValue,
          rawLineSnippet: line.substring(0, 100)
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
    const hasEPlus = containsEPlusSubstring(mpnValue) && !isSci;
    
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
 */
export function performMPNDiagnostics(
  rawContent: string,
  parsedData: any[],
  fileType: 'material' | 'mapping',
  filename: string,
  delimiter: string = ';',
  mpnFieldName?: string
): MPNDiagnosticsResult {
  // Raw scan
  const raw = performRawMPNScan(rawContent, delimiter);
  
  // Determine MPN field name for parsed data
  const fieldName = mpnFieldName || (raw.columnDetection.found ? raw.columnDetection.columnName : 'ManufPartNr');
  
  // Post-parse scan
  const post = performPostParseMPNScan(parsedData, fieldName);
  
  // Detect coercion
  const coercionDetected = raw.rawSciInMpn === 0 && post.postSciInMpn > 0;
  
  let coercionMessage: string | undefined;
  if (coercionDetected) {
    coercionMessage = `⚠️ COERCION DETECTED: Raw file has 0 scientific notation in MPN, but post-parse has ${post.postSciInMpn}. Parser is converting MPN to numbers!`;
  }
  
  return {
    fileType,
    filename,
    raw,
    post,
    coercionDetected,
    coercionMessage
  };
}

/**
 * Logs diagnostics to console with formatting
 */
export function logMPNDiagnostics(diagnostics: MPNDiagnosticsResult): void {
  const prefix = `[MPN-Diagnostics:${diagnostics.fileType}]`;
  
  console.group(`%c${prefix} ${diagnostics.filename}`, 'color: #E91E63; font-weight: bold;');
  
  console.log('Column detection:', diagnostics.raw.columnDetection);
  
  console.log('%cRaw Scan:', 'color: #2196F3; font-weight: bold;', {
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
  
  if (diagnostics.coercionDetected) {
    console.error('%c' + diagnostics.coercionMessage, 'color: #F44336; font-weight: bold; font-size: 14px;');
  } else if (diagnostics.raw.rawSciInMpn === 0 && diagnostics.post.postSciInMpn === 0) {
    console.log('%c✅ No coercion: Raw and Post both have 0 scientific notation', 'color: #4CAF50; font-weight: bold;');
  }
  
  console.groupEnd();
}

/**
 * Formats diagnostics for UI display
 */
export function formatDiagnosticsForUI(diagnostics: MPNDiagnosticsResult): {
  counters: Record<string, number>;
  warnings: string[];
  info: string[];
} {
  const counters: Record<string, number> = {
    'Raw: Righe totali': diagnostics.raw.totalRows,
    'Raw: MPN formato scientifico': diagnostics.raw.rawSciInMpn,
    'Raw: MPN con E+ (SKU validi)': diagnostics.raw.rawEPlusInMpn,
    'Post: Righe totali': diagnostics.post.totalRows,
    'Post: MPN formato scientifico': diagnostics.post.postSciInMpn,
    'Post: MPN con E+ (SKU validi)': diagnostics.post.postEPlusInMpn,
    'Post: Errori tipo (non stringa)': diagnostics.post.typeErrors
  };
  
  const warnings: string[] = [];
  const info: string[] = [];
  
  if (diagnostics.coercionDetected) {
    warnings.push(diagnostics.coercionMessage || 'Coercizione numerica rilevata');
  }
  
  if (diagnostics.post.typeErrors > 0) {
    warnings.push(`${diagnostics.post.typeErrors} MPN convertiti a numero dal parser`);
  }
  
  if (!diagnostics.raw.columnDetection.found) {
    warnings.push(`Colonna MPN non trovata. Headers: ${diagnostics.raw.columnDetection.allHeaders.join(', ')}`);
  }
  
  if (diagnostics.raw.rawEPlusInMpn > 0) {
    info.push(`${diagnostics.raw.rawEPlusInMpn} SKU validi con "E+" nel raw`);
  }
  
  return { counters, warnings, info };
}
