/**
 * EAN Prefill Utilities
 * 
 * This module provides utilities for the EAN prefill step, including:
 * - MPN scientific notation detection (TRUE format like 1.23E+05)
 * - MPN "E+" string detection (valid SKUs like ABC1234E+XYZ)
 * - EAN normalization (reuses same algorithm as EAN pipeline)
 * - Conflict classification with proper precedence rules
 */

// Re-export normalizeEAN from ean.ts for consistency
export { normalizeEAN, type EANResult } from './ean';

/**
 * Detects if an MPN value is in TRUE scientific notation format
 * This means the ENTIRE string is a number in scientific notation (e.g., 1.23E+05, 5e-3)
 * 
 * This indicates the MPN was incorrectly parsed/coerced from a number during import.
 * 
 * Valid SKUs like "ABC1234E+XYZ" will NOT be flagged - they contain "E+" as a substring
 * but are NOT in scientific notation format.
 * 
 * @returns true only if the entire string matches scientific notation pattern
 */
export function isScientificNotation(value: string): boolean {
  if (!value || typeof value !== 'string') return false;
  // Pattern: entire string must be a scientific notation number
  // Examples that match: "1.23E+05", "5e-3", "-2.5e+10", "123e5"
  // Examples that DON'T match: "ABC1234E+XYZ", "SKU-E+123", "1234E+ABC"
  return /^[+-]?\d+(?:[.,]\d+)?[eE][+-]?\d+$/.test(value.trim());
}

/**
 * Legacy function - now redirects to isScientificNotation
 * @deprecated Use isScientificNotation instead
 */
export function detectScientificNotation(value: string): boolean {
  return isScientificNotation(value);
}

/**
 * Counts occurrences of "E+" substring in a string (case-insensitive)
 * This is used to count valid SKUs that contain "E+" as part of their name
 */
export function countEPlusSubstring(value: string): boolean {
  if (!value || typeof value !== 'string') return false;
  return /e\+/i.test(value);
}

/**
 * Sanitizes MPN to ensure it's treated as a string
 * Never converts to number, only trims and removes non-printable characters
 */
export function sanitizeMPN(value: unknown): string {
  if (value === null || value === undefined) return '';
  // Convert to string and trim
  let str = String(value).trim();
  // Remove non-printable characters except space
  str = str.replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/g, '');
  return str;
}

/**
 * Normalizes an EAN for comparison purposes
 * Uses the same algorithm as the EAN pipeline:
 * - 12 digits → pad with leading "0" to get 13
 * - 13 digits → keep as-is
 * - 14 digits starting with "0" → trim first digit to get 13
 * - 14 digits not starting with "0" → keep as-is (valid GTIN-14)
 * 
 * Returns null if EAN is empty, non-numeric, or invalid length
 */
export function normalizeEANForComparison(raw: unknown): { normalized: string | null; isValid: boolean; reason: string } {
  const original = (raw ?? '').toString().trim();
  if (!original) {
    return { normalized: null, isValid: false, reason: 'empty' };
  }

  // Remove spaces and dashes
  const compact = original.replace(/[\s-]+/g, '');

  // Must be only numeric
  if (!/^\d+$/.test(compact)) {
    return { normalized: null, isValid: false, reason: 'non_numeric' };
  }

  if (compact.length === 12) {
    return { normalized: '0' + compact, isValid: true, reason: 'padded_12_to_13' };
  }
  if (compact.length === 13) {
    return { normalized: compact, isValid: true, reason: 'valid_13' };
  }
  if (compact.length === 14) {
    if (compact.startsWith('0')) {
      return { normalized: compact.substring(1), isValid: true, reason: 'trimmed_14_to_13' };
    } else {
      return { normalized: compact, isValid: true, reason: 'valid_14' };
    }
  }

  return { normalized: null, isValid: false, reason: `invalid_length_${compact.length}` };
}

/**
 * Extended counters for EAN prefill with detailed conflict classification
 */
export interface EANPrefillExtendedCounters {
  // Original counters
  already_populated: number;
  filled_now: number;
  skipped_due_to_conflict: number;  // Now only for truly ambiguous cases
  duplicate_mpn_rows: number;
  mpn_not_in_material: number;
  empty_ean_rows: number;
  missing_mapping_in_new_file: number;
  errori_formali: number;
  
  // MPN with "E+" substring (valid SKUs like ABC1234E+XYZ) - informative only, NOT a warning
  mpnWithEPlusSubstringMaterial: number;
  mpnWithEPlusSubstringMapping: number;
  
  // TRUE scientific notation (e.g., 1.23E+05) - indicates parser coercion - THIS IS A WARNING
  mpnScientificNotationFoundMaterial: number;
  mpnScientificNotationFoundMapping: number;
  
  // Conflict classification
  materialWinsDifferentEan: number;      // Case 2A: Material has EAN, mapping has different EAN → Material wins
  materialNormalizedMatchesMapping: number; // Case 2B/2C: EANs match after normalization
  ambiguousMapping: number;               // Case 3: Multiple different EANs for same MPN in mapping
}

/**
 * Extended reports for EAN prefill
 */
export interface EANPrefillExtendedReports {
  updated: Array<{ ManufPartNr: string; EAN_old: string; EAN_new: string; EAN_new_normalized: string }>;
  already_populated: Array<{ ManufPartNr: string; EAN_existing: string; EAN_existing_normalized: string }>;
  
  // Material wins different EAN (Case 2A)
  materialWinsDifferentEan: Array<{
    ManufPartNr: string;
    EAN_material_raw: string;
    EAN_material_norm: string;
    EAN_mapping_raw: string;
    EAN_mapping_norm: string;
  }>;
  
  // Normalized match (Case 2B/2C)
  materialNormalizedMatchesMapping: Array<{
    ManufPartNr: string;
    EAN_material_raw: string;
    EAN_mapping_raw: string;
    EAN_normalized: string;
  }>;
  
  // Ambiguous mapping (Case 3)
  ambiguousMapping: Array<{
    ManufPartNr: string;
    candidates_raw: string[];
    candidates_normalized: string[];
  }>;
  
  // TRUE scientific notation (parser coercion) - WARNING
  mpnScientificNotationMaterial: Array<{ ManufPartNr: string; row_index: number }>;
  mpnScientificNotationMapping: Array<{ mpn: string; ean: string; row_index: number }>;
  
  // MPN with "E+" substring (valid SKUs) - informative only
  mpnWithEPlusSubstringMaterial: Array<{ ManufPartNr: string; row_index: number }>;
  mpnWithEPlusSubstringMapping: Array<{ mpn: string; ean: string; row_index: number }>;
  
  // Kept from original
  duplicate_mpn_rows: Array<{ mpn: string; ean_seen_first: string; ean_conflicting: string; row_index: number }>;
  mpn_not_in_material: Array<{ mpn: string; ean: string; row_index: number }>;
  empty_ean_rows: Array<{ mpn: string; row_index: number }>;
  missing_mapping_in_new_file: Array<{ ManufPartNr: string }>;
  errori_formali: Array<{ raw_line: string; reason: string; row_index: number }>;
  
  // Legacy: kept for backward compatibility but should be empty now
  skipped_due_to_conflict: Array<{ ManufPartNr: string; EAN_material: string; EAN_mapping_first: string }>;
}

/**
 * Initializes empty extended counters
 */
export function createEmptyExtendedCounters(): EANPrefillExtendedCounters {
  return {
    already_populated: 0,
    filled_now: 0,
    skipped_due_to_conflict: 0,
    duplicate_mpn_rows: 0,
    mpn_not_in_material: 0,
    empty_ean_rows: 0,
    missing_mapping_in_new_file: 0,
    errori_formali: 0,
    mpnWithEPlusSubstringMaterial: 0,
    mpnWithEPlusSubstringMapping: 0,
    mpnScientificNotationFoundMaterial: 0,
    mpnScientificNotationFoundMapping: 0,
    materialWinsDifferentEan: 0,
    materialNormalizedMatchesMapping: 0,
    ambiguousMapping: 0
  };
}

/**
 * Initializes empty extended reports
 */
export function createEmptyExtendedReports(): EANPrefillExtendedReports {
  return {
    updated: [],
    already_populated: [],
    materialWinsDifferentEan: [],
    materialNormalizedMatchesMapping: [],
    ambiguousMapping: [],
    mpnScientificNotationMaterial: [],
    mpnScientificNotationMapping: [],
    mpnWithEPlusSubstringMaterial: [],
    mpnWithEPlusSubstringMapping: [],
    duplicate_mpn_rows: [],
    mpn_not_in_material: [],
    empty_ean_rows: [],
    missing_mapping_in_new_file: [],
    errori_formali: [],
    skipped_due_to_conflict: []
  };
}

/**
 * Mapping entry with raw and normalized EAN
 */
interface MappingEntry {
  ean_raw: string;
  ean_normalized: string | null;
  row_index: number;
}

/**
 * Processes the EAN prefill mapping with enhanced conflict resolution
 * 
 * @param mappingLines Lines from the mapping file (including header)
 * @param materialData Material data array
 * @returns Updated material data, counters, and reports
 */
export function processEANPrefillWithNormalization(
  mappingLines: string[],
  materialData: any[]
): {
  updatedMaterial: any[];
  counters: EANPrefillExtendedCounters;
  reports: EANPrefillExtendedReports;
} {
  const counters = createEmptyExtendedCounters();
  const reports = createEmptyExtendedReports();
  
  // Build mapping index: MPN → list of EAN candidates
  // Use list to detect ambiguous mappings
  const mappingIndex = new Map<string, MappingEntry[]>();
  
  // Parse mapping file (skip header at index 0)
  for (let i = 1; i < mappingLines.length; i++) {
    const line = mappingLines[i]?.trim();
    if (!line) continue;
    
    const parts = line.split(';');
    if (parts.length < 2) {
      counters.errori_formali++;
      reports.errori_formali.push({
        raw_line: line,
        reason: 'formato_errato',
        row_index: i + 1
      });
      continue;
    }
    
    // CRITICAL: Never convert MPN to number, always treat as string
    const mpn = sanitizeMPN(parts[0]);
    const ean_raw = (parts[1] ?? '').trim();
    
    // Detect TRUE scientific notation (parser coercion) - this is a WARNING
    if (isScientificNotation(mpn)) {
      counters.mpnScientificNotationFoundMapping++;
      if (reports.mpnScientificNotationMapping.length < 20) {
        reports.mpnScientificNotationMapping.push({
          mpn,
          ean: ean_raw,
          row_index: i + 1
        });
      }
    }
    // Detect "E+" substring (valid SKUs) - this is informative only
    else if (countEPlusSubstring(mpn)) {
      counters.mpnWithEPlusSubstringMapping++;
      if (reports.mpnWithEPlusSubstringMapping.length < 10) {
        reports.mpnWithEPlusSubstringMapping.push({
          mpn,
          ean: ean_raw,
          row_index: i + 1
        });
      }
    }
    
    if (!ean_raw) {
      counters.empty_ean_rows++;
      reports.empty_ean_rows.push({
        mpn,
        row_index: i + 1
      });
      continue;
    }
    
    // Normalize EAN
    const normResult = normalizeEANForComparison(ean_raw);
    
    const entry: MappingEntry = {
      ean_raw,
      ean_normalized: normResult.normalized,
      row_index: i + 1
    };
    
    // Add to mapping index
    if (!mappingIndex.has(mpn)) {
      mappingIndex.set(mpn, [entry]);
    } else {
      const existing = mappingIndex.get(mpn)!;
      
      // Check if this EAN (normalized) is different from existing ones
      const isDuplicate = existing.some(e => 
        e.ean_normalized === entry.ean_normalized || 
        (e.ean_raw === ean_raw)
      );
      
      if (!isDuplicate) {
        existing.push(entry);
      } else {
        // Log as duplicate but don't add to index
        counters.duplicate_mpn_rows++;
        const firstEntry = existing[0];
        reports.duplicate_mpn_rows.push({
          mpn,
          ean_seen_first: firstEntry.ean_raw,
          ean_conflicting: ean_raw,
          row_index: i + 1
        });
      }
    }
  }
  
  // Create set of MPNs in material
  const materialMPNs = new Set<string>();
  
  // First pass: collect MPNs and detect scientific notation
  materialData.forEach((row, index) => {
    const mpn = sanitizeMPN(row.ManufPartNr);
    if (mpn) {
      materialMPNs.add(mpn);
      
      // Detect TRUE scientific notation (parser coercion) - this is a WARNING
      if (isScientificNotation(mpn)) {
        counters.mpnScientificNotationFoundMaterial++;
        if (reports.mpnScientificNotationMaterial.length < 20) {
          reports.mpnScientificNotationMaterial.push({
            ManufPartNr: mpn,
            row_index: index + 1
          });
        }
      }
      // Detect "E+" substring (valid SKUs) - this is informative only
      else if (countEPlusSubstring(mpn)) {
        counters.mpnWithEPlusSubstringMaterial++;
        if (reports.mpnWithEPlusSubstringMaterial.length < 10) {
          reports.mpnWithEPlusSubstringMaterial.push({
            ManufPartNr: mpn,
            row_index: index + 1
          });
        }
      }
    }
  });
  
  // Check for MPNs in mapping that don't exist in material
  for (const [mpn, entries] of mappingIndex.entries()) {
    if (!materialMPNs.has(mpn)) {
      counters.mpn_not_in_material++;
      reports.mpn_not_in_material.push({
        mpn,
        ean: entries[0].ean_raw,
        row_index: entries[0].row_index
      });
    }
  }
  
  // Process material rows
  const updatedMaterial = materialData.map((row, index) => {
    const newRow = { ...row };
    const mpn = sanitizeMPN(row.ManufPartNr);
    const currentEAN_raw = (row.EAN ?? '').toString().trim();
    
    // Normalize current EAN from material
    const currentEAN_norm = normalizeEANForComparison(currentEAN_raw);
    
    // Check if material already has a valid EAN
    const materialHasValidEAN = currentEAN_raw !== '' && currentEAN_norm.isValid;
    
    // Get mapping entries for this MPN
    const mappingEntries = mpn ? mappingIndex.get(mpn) : undefined;
    const hasMapping = mappingEntries && mappingEntries.length > 0;
    
    if (materialHasValidEAN) {
      // Material already has an EAN
      counters.already_populated++;
      reports.already_populated.push({
        ManufPartNr: mpn,
        EAN_existing: currentEAN_raw,
        EAN_existing_normalized: currentEAN_norm.normalized || currentEAN_raw
      });
      
      if (hasMapping) {
        // Compare with mapping
        const mappingEntry = mappingEntries[0]; // Use first entry for comparison
        const mappingEAN_norm = mappingEntry.ean_normalized;
        
        if (mappingEAN_norm && currentEAN_norm.normalized === mappingEAN_norm) {
          // Case 2B/2C: EANs match after normalization → resolved automatically
          counters.materialNormalizedMatchesMapping++;
          reports.materialNormalizedMatchesMapping.push({
            ManufPartNr: mpn,
            EAN_material_raw: currentEAN_raw,
            EAN_mapping_raw: mappingEntry.ean_raw,
            EAN_normalized: currentEAN_norm.normalized
          });
        } else {
          // Case 2A: EANs differ after normalization → Material wins
          counters.materialWinsDifferentEan++;
          reports.materialWinsDifferentEan.push({
            ManufPartNr: mpn,
            EAN_material_raw: currentEAN_raw,
            EAN_material_norm: currentEAN_norm.normalized || currentEAN_raw,
            EAN_mapping_raw: mappingEntry.ean_raw,
            EAN_mapping_norm: mappingEAN_norm || mappingEntry.ean_raw
          });
        }
      }
      
      // Material EAN is preserved (no change to newRow.EAN)
      
    } else if (hasMapping) {
      // Material has no valid EAN, but mapping exists
      
      // Check for ambiguous mapping (multiple different normalized EANs)
      const uniqueNormalizedEANs = new Set(
        mappingEntries
          .map(e => e.ean_normalized)
          .filter((n): n is string => n !== null)
      );
      
      if (uniqueNormalizedEANs.size > 1) {
        // Case 3: Ambiguous mapping → don't prefill
        counters.ambiguousMapping++;
        counters.skipped_due_to_conflict++; // Increment for backward compatibility
        reports.ambiguousMapping.push({
          ManufPartNr: mpn,
          candidates_raw: mappingEntries.map(e => e.ean_raw),
          candidates_normalized: mappingEntries.map(e => e.ean_normalized || e.ean_raw)
        });
        // Don't prefill - leave EAN empty
        
      } else {
        // Single unique normalized EAN → prefill
        const mappingEntry = mappingEntries[0];
        const ean_to_use = mappingEntry.ean_normalized || mappingEntry.ean_raw;
        
        // Prefill with normalized EAN
        newRow.EAN = ean_to_use;
        counters.filled_now++;
        reports.updated.push({
          ManufPartNr: mpn,
          EAN_old: currentEAN_raw,
          EAN_new: mappingEntry.ean_raw,
          EAN_new_normalized: ean_to_use
        });
      }
      
    } else {
      // No mapping found for this MPN
      counters.missing_mapping_in_new_file++;
      reports.missing_mapping_in_new_file.push({
        ManufPartNr: mpn || ''
      });
    }
    
    return newRow;
  });
  
  return { updatedMaterial, counters, reports };
}

/**
 * Generates a summary message for the EAN prefill step
 */
export function generatePrefillSummary(counters: EANPrefillExtendedCounters): string {
  const parts: string[] = [];
  
  // Use ?? 0 to prevent NaN when counters are undefined
  const filledNow = counters.filled_now ?? 0;
  const materialWins = counters.materialWinsDifferentEan ?? 0;
  const normalizedMatches = counters.materialNormalizedMatchesMapping ?? 0;
  const ambiguous = counters.ambiguousMapping ?? 0;
  
  if (filledNow > 0) {
    parts.push(`${filledNow} EAN riempiti`);
  }
  
  if (materialWins > 0) {
    parts.push(`${materialWins} vince Material`);
  }
  
  if (normalizedMatches > 0) {
    parts.push(`${normalizedMatches} match normalizzati`);
  }
  
  if (ambiguous > 0) {
    parts.push(`${ambiguous} ambigui`);
  }
  
  return parts.length > 0 ? parts.join(', ') : 'Prefill completato';
}

/**
 * Checks if there are TRUE scientific notation warnings (parser coercion)
 * This does NOT flag "E+" substrings in valid SKUs
 */
export function hasScientificNotationWarnings(counters: EANPrefillExtendedCounters): boolean {
  // Use ?? 0 to prevent undefined issues
  const materialSci = counters.mpnScientificNotationFoundMaterial ?? 0;
  const mappingSci = counters.mpnScientificNotationFoundMapping ?? 0;
  return materialSci > 0 || mappingSci > 0;
}

/**
 * Generates a warning message for TRUE scientific notation (parser coercion)
 * Only shown when actual scientific notation format is detected (e.g., 1.23E+05)
 */
export function generateScientificNotationWarning(counters: EANPrefillExtendedCounters): string | null {
  if (!hasScientificNotationWarnings(counters)) return null;
  
  const materialSci = counters.mpnScientificNotationFoundMaterial ?? 0;
  const mappingSci = counters.mpnScientificNotationFoundMapping ?? 0;
  
  return `⚠️ MPN in formato scientifico (es: 1.23E+05) rilevati. Probabile coercizione numerica durante import/parsing. (Material: ${materialSci}, Mapping: ${mappingSci})`;
}

/**
 * Generates info message for "E+" substring (valid SKUs) - NOT a warning
 */
export function generateEPlusSubstringInfo(counters: EANPrefillExtendedCounters): string | null {
  // Use ?? 0 to prevent NaN when counters are undefined
  const materialCount = counters.mpnWithEPlusSubstringMaterial ?? 0;
  const mappingCount = counters.mpnWithEPlusSubstringMapping ?? 0;
  const total = materialCount + mappingCount;
  if (total === 0 || isNaN(total)) return null;
  
  return `MPN con stringa "E+" (SKU validi): ${total}`;
}
