/**
 * _shared/expectedSteps.ts - Single source of truth for pipeline expected steps.
 *
 * All 13 canonical steps, identical for manual and cron triggers.
 * Used by run-full-sync (completeness check) and send-sync-notification (missing steps report).
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
  'notification'
] as const;

export type CanonicalStep = typeof EXPECTED_STEPS[number];

/**
 * Returns the expected steps list. Trigger type is accepted for API compatibility
 * but the list is always the same (no manual/cron divergence).
 */
export function getExpectedSteps(_trigger?: string): string[] {
  return [...EXPECTED_STEPS];
}
