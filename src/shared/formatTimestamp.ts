/**
 * Universal timestamp formatter.
 * Shows Europe/Rome with real offset + UTC in parentheses.
 * Handles DST automatically (no hardcoded offset).
 */
export function formatTimestamp(isoString: string | null | undefined): string {
  if (!isoString) return '-';
  const d = new Date(isoString);
  if (isNaN(d.getTime())) return isoString;

  const rome = d.toLocaleString('it-IT', {
    timeZone: 'Europe/Rome',
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });

  const utc = d.toLocaleString('it-IT', {
    timeZone: 'UTC',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });

  return `${rome} (UTC ${utc})`;
}

/**
 * Short format: date + time in Europe/Rome, no UTC.
 */
export function formatTimestampShort(isoString: string | null | undefined): string {
  if (!isoString) return '-';
  const d = new Date(isoString);
  if (isNaN(d.getTime())) return isoString;

  return d.toLocaleString('it-IT', {
    timeZone: 'Europe/Rome',
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}
