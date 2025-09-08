/**
 * Pricing utilities for catalog generation
 */

/**
 * Apply ending ,99 using integer arithmetic in cents to avoid floating point errors
 * @param v - Input value (post fees)
 * @returns Value ending in .99 or NaN if invalid input
 */
export function toComma99Cents(v: number): number {
  if (!Number.isFinite(v) || v <= 0) return NaN;
  
  // Convert to cents using integer arithmetic to avoid floating point errors
  const cents = Math.floor(v * 100 + 0.5); // +0.5 neutralizes micro binary errors
  const euros = Math.floor(cents / 100);
  let resultCents = euros * 100 + 99;
  
  // If original value in cents is higher than euros.99, move to next euro + .99
  if (cents > resultCents) {
    resultCents = (euros + 1) * 100 + 99;
  }
  
  // Log sample for debugging (once per session)
  if (typeof (globalThis as any).eanEndingFunctionLogged === 'undefined') {
    console.warn('ean:ending:function=int-cents');
    (globalThis as any).eanEndingFunctionLogged = true;
    (globalThis as any).eanEndingSampleCount = 0;
  }
  
  if ((globalThis as any).eanEndingSampleCount < 3) {
    console.warn('ean:ending:sample', {
      preFee: Number(v.toFixed(4)),
      finalEan: resultCents / 100
    });
    (globalThis as any).eanEndingSampleCount++;
  }
  
  return resultCents / 100;
}

/**
 * Validate that a value ends with .99 using integer cents arithmetic
 * @param value - Value to validate
 * @returns true if value ends with .99
 */
export function validateEnding99(value: number): boolean {
  if (!Number.isFinite(value)) return false;
  const cents = Math.floor(value * 100 + 0.5);
  return (cents % 100) === 99;
}