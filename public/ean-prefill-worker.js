// Web Worker for EAN Pre-fill processing
// Enhanced with: scientific notation detection, EAN normalization, proper conflict classification
// BUILD_VERSION for cache busting - INCREMENT THIS WHEN CHANGING WORKER CODE
const BUILD_VERSION = '2025-12-14-v4';
console.log('[ean-prefill-worker] Started, BUILD_VERSION:', BUILD_VERSION);

/**
 * Detects if an MPN value is in TRUE scientific notation format
 * This means the ENTIRE string is a number in scientific notation (e.g., 1.23E+05, 5e-3)
 * 
 * @returns true only if the entire string matches scientific notation pattern
 */
function isScientificNotation(value) {
  if (!value || typeof value !== 'string') return false;
  return /^[+-]?\d+(?:[.,]\d+)?[eE][+-]?\d+$/.test(value.trim());
}

/**
 * Checks if MPN contains "E+" as a substring (case-insensitive)
 * AND is NOT in scientific notation format (valid SKUs like "ABC1234E+XYZ")
 * @returns true if contains E+ substring but NOT scientific notation
 */
function containsEPlusSubstring(value) {
  if (!value || typeof value !== 'string') return false;
  return /e\+/i.test(value) && !isScientificNotation(value);
}

/**
 * Sanitizes MPN to ensure it's treated as a string
 * Never converts to number, only trims and removes non-printable characters
 */
function sanitizeMPN(value) {
  if (value === null || value === undefined) return '';
  // CRITICAL: Always convert to string first to prevent number coercion
  let str = String(value).trim();
  // Remove non-printable characters except space
  str = str.replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/g, '');
  return str;
}

/**
 * Normalizes an EAN for comparison purposes
 */
function normalizeEANForComparison(raw) {
  const original = (raw ?? '').toString().trim();
  if (!original) {
    return { normalized: null, isValid: false, reason: 'empty' };
  }

  const compact = original.replace(/[\s-]+/g, '');

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

  return { normalized: null, isValid: false, reason: 'invalid_length_' + compact.length };
}

self.onmessage = function(e) {
  const { mappingText, materialData, counters: initialCounters } = e.data;
  
  try {
    // Parse mapping file
    const lines = mappingText.split('\n');
    
    // Validate header
    const header = lines[0]?.trim().toLowerCase();
    if (header !== 'mpn;ean') {
      self.postMessage({
        error: true,
        message: 'Header richiesto: mpn;ean'
      });
      return;
    }
    
    // Initialize ALL counters to 0 explicitly to avoid NaN
    const counters = {
      already_populated: 0,
      filled_now: 0,
      skipped_due_to_conflict: 0,
      duplicate_mpn_rows: 0,
      mpn_not_in_material: 0,
      empty_ean_rows: 0,
      missing_mapping_in_new_file: 0,
      errori_formali: 0,
      // RAW E+ substring (valid SKUs) - informative only, based on mapping file parse
      mpnWithEPlusSubstringMaterial: 0,
      mpnWithEPlusSubstringMapping: 0,
      // TRUE scientific notation (potential coercion) - will be compared with raw later
      mpnScientificNotationFoundMaterial: 0,
      mpnScientificNotationFoundMapping: 0,
      // Conflict classification
      materialWinsDifferentEan: 0,
      materialNormalizedMatchesMapping: 0,
      ambiguousMapping: 0
    };
    
    // Initialize reports with all arrays
    const reports = {
      duplicate_mpn_rows: [],
      empty_ean_rows: [],
      errori_formali: [],
      updated: [],
      already_populated: [],
      skipped_due_to_conflict: [],
      mpn_not_in_material: [],
      missing_mapping_in_new_file: [],
      // Conflict classification reports
      materialWinsDifferentEan: [],
      materialNormalizedMatchesMapping: [],
      ambiguousMapping: [],
      // Scientific notation reports
      mpnScientificNotationMaterial: [],
      mpnScientificNotationMapping: [],
      // E+ substring reports (informative only)
      mpnWithEPlusSubstringMaterial: [],
      mpnWithEPlusSubstringMapping: []
    };
    
    // Build mapping index: MPN → list of EAN candidates
    const mappingIndex = new Map();
    
    for (let i = 1; i < lines.length; i++) {
      const line = lines[i]?.trim();
      if (!line) continue;
      
      const parts = line.split(';');
      if (parts.length < 2) {
        counters.errori_formali = (counters.errori_formali || 0) + 1;
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
      
      // Detect TRUE scientific notation in MPN from mapping (post-parse)
      if (isScientificNotation(mpn)) {
        counters.mpnScientificNotationFoundMapping = (counters.mpnScientificNotationFoundMapping || 0) + 1;
        if (reports.mpnScientificNotationMapping.length < 20) {
          reports.mpnScientificNotationMapping.push({
            mpn,
            ean: ean_raw,
            row_index: i + 1
          });
        }
      }
      // Detect "E+" substring (valid SKUs - informative only)
      else if (containsEPlusSubstring(mpn)) {
        counters.mpnWithEPlusSubstringMapping = (counters.mpnWithEPlusSubstringMapping || 0) + 1;
        if (reports.mpnWithEPlusSubstringMapping.length < 20) {
          reports.mpnWithEPlusSubstringMapping.push({
            mpn,
            ean: ean_raw,
            row_index: i + 1
          });
        }
      }
      
      if (!ean_raw) {
        counters.empty_ean_rows = (counters.empty_ean_rows || 0) + 1;
        reports.empty_ean_rows.push({
          mpn,
          row_index: i + 1
        });
        continue;
      }
      
      // Normalize EAN
      const normResult = normalizeEANForComparison(ean_raw);
      
      const entry = {
        ean_raw,
        ean_normalized: normResult.normalized,
        row_index: i + 1
      };
      
      // Add to mapping index
      if (!mappingIndex.has(mpn)) {
        mappingIndex.set(mpn, [entry]);
      } else {
        const existing = mappingIndex.get(mpn);
        
        const isDuplicate = existing.some(e => 
          e.ean_normalized === entry.ean_normalized || 
          e.ean_raw === ean_raw
        );
        
        if (!isDuplicate) {
          existing.push(entry);
        } else {
          counters.duplicate_mpn_rows = (counters.duplicate_mpn_rows || 0) + 1;
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
    
    // Create set of MPNs in material and detect scientific notation
    const materialMPNs = new Set();
    
    materialData.forEach((row, index) => {
      const mpn = sanitizeMPN(row.ManufPartNr);
      if (mpn) {
        materialMPNs.add(mpn);
        
        // Detect TRUE scientific notation in MPN from material (post-parse)
        if (isScientificNotation(mpn)) {
          counters.mpnScientificNotationFoundMaterial = (counters.mpnScientificNotationFoundMaterial || 0) + 1;
          if (reports.mpnScientificNotationMaterial.length < 20) {
            reports.mpnScientificNotationMaterial.push({
              ManufPartNr: mpn,
              row_index: index + 1
            });
          }
        }
        // Detect "E+" substring (valid SKUs - informative only)
        else if (containsEPlusSubstring(mpn)) {
          counters.mpnWithEPlusSubstringMaterial = (counters.mpnWithEPlusSubstringMaterial || 0) + 1;
          if (reports.mpnWithEPlusSubstringMaterial.length < 20) {
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
        counters.mpn_not_in_material = (counters.mpn_not_in_material || 0) + 1;
        reports.mpn_not_in_material.push({
          mpn,
          ean: entries[0].ean_raw,
          row_index: entries[0].row_index
        });
      }
    }
    
    // Process material rows
    const updatedMaterial = [];
    
    for (const row of materialData) {
      const newRow = { ...row };
      const mpn = sanitizeMPN(row.ManufPartNr);
      const currentEAN_raw = (row.EAN ?? '').toString().trim();
      
      const currentEAN_norm = normalizeEANForComparison(currentEAN_raw);
      
      const materialHasValidEAN = currentEAN_raw !== '' && currentEAN_norm.isValid;
      
      const mappingEntries = mpn ? mappingIndex.get(mpn) : undefined;
      const hasMapping = mappingEntries && mappingEntries.length > 0;
      
      if (materialHasValidEAN) {
        counters.already_populated = (counters.already_populated || 0) + 1;
        reports.already_populated.push({
          ManufPartNr: mpn,
          EAN_existing: currentEAN_raw
        });
        
        if (hasMapping) {
          const mappingEntry = mappingEntries[0];
          const mappingEAN_norm = mappingEntry.ean_normalized;
          
          if (mappingEAN_norm && currentEAN_norm.normalized === mappingEAN_norm) {
            counters.materialNormalizedMatchesMapping = (counters.materialNormalizedMatchesMapping || 0) + 1;
            if (reports.materialNormalizedMatchesMapping.length < 20) {
              reports.materialNormalizedMatchesMapping.push({
                ManufPartNr: mpn,
                EAN_material_raw: currentEAN_raw,
                EAN_mapping_raw: mappingEntry.ean_raw,
                EAN_normalized: currentEAN_norm.normalized
              });
            }
          } else {
            counters.materialWinsDifferentEan = (counters.materialWinsDifferentEan || 0) + 1;
            if (reports.materialWinsDifferentEan.length < 20) {
              reports.materialWinsDifferentEan.push({
                ManufPartNr: mpn,
                EAN_material_raw: currentEAN_raw,
                EAN_material_norm: currentEAN_norm.normalized || currentEAN_raw,
                EAN_mapping_raw: mappingEntry.ean_raw,
                EAN_mapping_norm: mappingEAN_norm || mappingEntry.ean_raw
              });
            }
          }
        }
        
      } else if (hasMapping) {
        const uniqueNormalizedEANs = new Set(
          mappingEntries
            .map(e => e.ean_normalized)
            .filter(n => n !== null)
        );
        
        if (uniqueNormalizedEANs.size > 1) {
          counters.ambiguousMapping = (counters.ambiguousMapping || 0) + 1;
          counters.skipped_due_to_conflict = (counters.skipped_due_to_conflict || 0) + 1;
          if (reports.ambiguousMapping.length < 20) {
            reports.ambiguousMapping.push({
              ManufPartNr: mpn,
              candidates_raw: mappingEntries.map(e => e.ean_raw),
              candidates_normalized: mappingEntries.map(e => e.ean_normalized || e.ean_raw)
            });
          }
          
        } else {
          const mappingEntry = mappingEntries[0];
          const ean_to_use = mappingEntry.ean_normalized || mappingEntry.ean_raw;
          
          newRow.EAN = ean_to_use;
          counters.filled_now = (counters.filled_now || 0) + 1;
          reports.updated.push({
            ManufPartNr: mpn,
            EAN_old: currentEAN_raw || '',
            EAN_new: mappingEntry.ean_raw
          });
        }
        
      } else {
        counters.missing_mapping_in_new_file = (counters.missing_mapping_in_new_file || 0) + 1;
        reports.missing_mapping_in_new_file.push({
          ManufPartNr: mpn || ''
        });
      }
      
      updatedMaterial.push(newRow);
    }
    
    // Ensure all counters are numbers (not undefined/NaN)
    Object.keys(counters).forEach(key => {
      if (typeof counters[key] !== 'number' || isNaN(counters[key])) {
        counters[key] = 0;
      }
    });
    
    // Send results back
    self.postMessage({
      success: true,
      updatedMaterial: updatedMaterial,
      counters: counters,
      reports: reports
    });
    
  } catch (error) {
    self.postMessage({
      error: true,
      message: error.message || 'Errore sconosciuto durante l\'elaborazione'
    });
  }
};
