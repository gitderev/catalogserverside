/**
 * EAN Prefill Utilities
 * 
 * This module provides utilities for the EAN prefill step, including:
 * - MPN scientific notation detection
 * - EAN normalization (reuses same algorithm as EAN pipeline)
 * - Conflict classification with proper precedence rules
 */

// Re-export normalizeEAN from ean.ts for consistency
export { normalizeEAN, type EANResult } from './ean';

/**
 * Detects if an MPN value contains scientific notation (E+ or E-)
 * This indicates the MPN was incorrectly parsed as a number
 */
export function detectScientificNotation(value: string): boolean {
  if (!value || typeof value !== 'string') return false;
  // Pattern: digits followed by e+/e- and more digits (case insensitive)
  return /[0-9]\.?[0-9]*e[+-]?[0-9]+/i.test(value);
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
  
  // NEW: Scientific notation detection
  mpnScientificNotationFoundMaterial: number;
  mpnScientificNotationFoundMapping: number;
  
  // NEW: Conflict classification
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
  
  // NEW: Material wins different EAN (Case 2A)
  materialWinsDifferentEan: Array<{
    ManufPartNr: string;
    EAN_material_raw: string;
    EAN_material_norm: string;
    EAN_mapping_raw: string;
    EAN_mapping_norm: string;
  }>;
  
  // NEW: Normalized match (Case 2B/2C)
  materialNormalizedMatchesMapping: Array<{
    ManufPartNr: string;
    EAN_material_raw: string;
    EAN_mapping_raw: string;
    EAN_normalized: string;
  }>;
  
  // NEW: Ambiguous mapping (Case 3)
  ambiguousMapping: Array<{
    ManufPartNr: string;
    candidates_raw: string[];
    candidates_normalized: string[];
  }>;
  
  // NEW: Scientific notation warnings
  mpnScientificNotationMaterial: Array<{ ManufPartNr: string; row_index: number }>;
  mpnScientificNotationMapping: Array<{ mpn: string; ean: string; row_index: number }>;
  
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
    
    // Detect scientific notation in MPN from mapping
    if (detectScientificNotation(mpn)) {
      counters.mpnScientificNotationFoundMapping++;
      reports.mpnScientificNotationMapping.push({
        mpn,
        ean: ean_raw,
        row_index: i + 1
      });
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
      
      // Detect scientific notation in MPN from material
      if (detectScientificNotation(mpn)) {
        counters.mpnScientificNotationFoundMaterial++;
        reports.mpnScientificNotationMaterial.push({
          ManufPartNr: mpn,
          row_index: index + 1
        });
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
  
  if (counters.filled_now > 0) {
    parts.push(`${counters.filled_now} EAN riempiti`);
  }
  
  if (counters.materialWinsDifferentEan > 0) {
    parts.push(`${counters.materialWinsDifferentEan} vince Material`);
  }
  
  if (counters.materialNormalizedMatchesMapping > 0) {
    parts.push(`${counters.materialNormalizedMatchesMapping} match normalizzati`);
  }
  
  if (counters.ambiguousMapping > 0) {
    parts.push(`${counters.ambiguousMapping} ambigui`);
  }
  
  return parts.length > 0 ? parts.join(', ') : 'Prefill completato';
}

/**
 * Checks if there are scientific notation warnings
 */
export function hasScientificNotationWarnings(counters: EANPrefillExtendedCounters): boolean {
  return counters.mpnScientificNotationFoundMaterial > 0 || 
         counters.mpnScientificNotationFoundMapping > 0;
}

/**
 * Generates a warning message for scientific notation
 */
export function generateScientificNotationWarning(counters: EANPrefillExtendedCounters): string | null {
  if (!hasScientificNotationWarnings(counters)) return null;
  
  return `⚠️ Rilevati MPN in notazione scientifica (E+). Probabile parsing numerico errato. I conflitti EAN potrebbero essere falsi. (Material: ${counters.mpnScientificNotationFoundMaterial}, Mapping: ${counters.mpnScientificNotationFoundMapping})`;
}
