// Web Worker for EAN Pre-fill processing
// Enhanced with: scientific notation detection, EAN normalization, proper conflict classification

/**
 * Detects if an MPN value contains scientific notation (E+ or E-)
 * This indicates the MPN was incorrectly parsed as a number
 */
function detectScientificNotation(value) {
  if (!value || typeof value !== 'string') return false;
  // Pattern: digits followed by e+/e- and more digits (case insensitive)
  return /[0-9]\.?[0-9]*e[+-]?[0-9]+/i.test(value);
}

/**
 * Sanitizes MPN to ensure it's treated as a string
 * Never converts to number, only trims and removes non-printable characters
 */
function sanitizeMPN(value) {
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
 * Returns object with normalized value, validity, and reason
 */
function normalizeEANForComparison(raw) {
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
    
    // Initialize counters
    const counters = {
      already_populated: 0,
      filled_now: 0,
      skipped_due_to_conflict: 0,
      duplicate_mpn_rows: 0,
      mpn_not_in_material: 0,
      empty_ean_rows: 0,
      missing_mapping_in_new_file: 0,
      errori_formali: 0,
      // NEW counters
      mpnScientificNotationFoundMaterial: 0,
      mpnScientificNotationFoundMapping: 0,
      materialWinsDifferentEan: 0,
      materialNormalizedMatchesMapping: 0,
      ambiguousMapping: 0
    };
    
    // Initialize reports
    const reports = {
      duplicate_mpn_rows: [],
      empty_ean_rows: [],
      errori_formali: [],
      updated: [],
      already_populated: [],
      skipped_due_to_conflict: [],
      mpn_not_in_material: [],
      missing_mapping_in_new_file: [],
      // NEW reports
      materialWinsDifferentEan: [],
      materialNormalizedMatchesMapping: [],
      ambiguousMapping: [],
      mpnScientificNotationMaterial: [],
      mpnScientificNotationMapping: []
    };
    
    // Build mapping index: MPN → list of EAN candidates
    // Use list to detect ambiguous mappings
    const mappingIndex = new Map();
    
    for (let i = 1; i < lines.length; i++) {
      const line = lines[i]?.trim();
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
        if (reports.mpnScientificNotationMapping.length < 10) {
          reports.mpnScientificNotationMapping.push({
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
        
        // Check if this EAN (normalized) is different from existing ones
        const isDuplicate = existing.some(e => 
          e.ean_normalized === entry.ean_normalized || 
          e.ean_raw === ean_raw
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
    
    // Create set of MPNs in material and detect scientific notation
    const materialMPNs = new Set();
    
    materialData.forEach((row, index) => {
      const mpn = sanitizeMPN(row.ManufPartNr);
      if (mpn) {
        materialMPNs.add(mpn);
        
        // Detect scientific notation in MPN from material
        if (detectScientificNotation(mpn)) {
          counters.mpnScientificNotationFoundMaterial++;
          if (reports.mpnScientificNotationMaterial.length < 10) {
            reports.mpnScientificNotationMaterial.push({
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
    const updatedMaterial = [];
    
    for (const row of materialData) {
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
          EAN_existing: currentEAN_raw
        });
        
        if (hasMapping) {
          // Compare with mapping
          const mappingEntry = mappingEntries[0]; // Use first entry for comparison
          const mappingEAN_norm = mappingEntry.ean_normalized;
          
          if (mappingEAN_norm && currentEAN_norm.normalized === mappingEAN_norm) {
            // Case 2B/2C: EANs match after normalization → resolved automatically
            counters.materialNormalizedMatchesMapping++;
            if (reports.materialNormalizedMatchesMapping.length < 20) {
              reports.materialNormalizedMatchesMapping.push({
                ManufPartNr: mpn,
                EAN_material_raw: currentEAN_raw,
                EAN_mapping_raw: mappingEntry.ean_raw,
                EAN_normalized: currentEAN_norm.normalized
              });
            }
          } else {
            // Case 2A: EANs differ after normalization → Material wins
            counters.materialWinsDifferentEan++;
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
        
        // Material EAN is preserved (no change to newRow.EAN)
        
      } else if (hasMapping) {
        // Material has no valid EAN, but mapping exists
        
        // Check for ambiguous mapping (multiple different normalized EANs)
        const uniqueNormalizedEANs = new Set(
          mappingEntries
            .map(e => e.ean_normalized)
            .filter(n => n !== null)
        );
        
        if (uniqueNormalizedEANs.size > 1) {
          // Case 3: Ambiguous mapping → don't prefill
          counters.ambiguousMapping++;
          counters.skipped_due_to_conflict++; // Increment for backward compatibility
          if (reports.ambiguousMapping.length < 20) {
            reports.ambiguousMapping.push({
              ManufPartNr: mpn,
              candidates_raw: mappingEntries.map(e => e.ean_raw),
              candidates_normalized: mappingEntries.map(e => e.ean_normalized || e.ean_raw)
            });
          }
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
            EAN_old: currentEAN_raw || '',
            EAN_new: mappingEntry.ean_raw
          });
        }
        
      } else {
        // No mapping found for this MPN
        counters.missing_mapping_in_new_file++;
        reports.missing_mapping_in_new_file.push({
          ManufPartNr: mpn || ''
        });
      }
      
      updatedMaterial.push(newRow);
    }
    
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
