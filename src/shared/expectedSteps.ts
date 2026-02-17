/**
 * Single source of truth for the 13 canonical pipeline steps.
 * Used by SyncScheduler, SyncHealthPanel, SyncCronHistory, and modal details.
 */

export const EXPECTED_STEPS = [
  'import_ftp',
  'parse_merge',
  'ean_mapping',
  'pricing',
  'override_products',
  'export_ean',
  'export_ean_xlsx',
  'export_amazon',
  'export_mediaworld',
  'export_eprice',
  'upload_sftp',
  'versioning',
  'notification',
] as const;

export type CanonicalStep = typeof EXPECTED_STEPS[number];

export const STEP_LABELS: Record<string, string> = {
  import_ftp: 'Import FTP',
  parse_merge: 'Parsing e Merge',
  ean_mapping: 'Mapping EAN',
  pricing: 'Calcolo Prezzi',
  override_products: 'Override Prodotti',
  export_ean: 'Export Catalogo EAN',
  export_ean_xlsx: 'Export Catalogo EAN (XLSX)',
  export_amazon: 'Export Amazon',
  export_mediaworld: 'Export Mediaworld',
  export_eprice: 'Export ePrice',
  upload_sftp: 'Upload SFTP',
  versioning: 'Versioning',
  notification: 'Notifica',
};

/**
 * Runtime assert: log error if steps config is inconsistent.
 * Returns true if valid.
 */
export function assertExpectedSteps(): boolean {
  const keys = Object.keys(STEP_LABELS);
  const valid =
    EXPECTED_STEPS.length === 13 &&
    keys.length === 13 &&
    EXPECTED_STEPS.every((s) => s in STEP_LABELS);
  if (!valid) {
    console.error(
      '[expectedSteps] Configurazione step incoerente:',
      `EXPECTED_STEPS.length=${EXPECTED_STEPS.length}`,
      `STEP_LABELS keys=${keys.length}`,
    );
  }
  return valid;
}
