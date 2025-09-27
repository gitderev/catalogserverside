/**
 * Pricing utilities for catalog generation - Pure integer cents arithmetic
 */

// One-time log to confirm integer-cents implementation
if (typeof (globalThis as any).eanEndingInitLogged === 'undefined') {
  console.warn('ean:ending:function=int-cents');
  (globalThis as any).eanEndingInitLogged = true;
}

/**
 * Convert a number or string to integer cents (avoiding floating point issues)
 */
export function toCents(x: number | string): number {
  if (typeof x === 'string') {
    const parsed = parseFloat(x.replace(',', '.'));
    return Math.floor(parsed * 100 + 0.5);
  }
  if (!Number.isFinite(x)) return 0;
  return Math.floor(x * 100 + 0.5);
}

/**
 * Format cents as Italian decimal string with 2 decimals
 */
export function formatCents(cents: number): string {
  const euros = Math.floor(cents / 100);
  const centsPart = cents % 100;
  return `${euros},${centsPart.toString().padStart(2, '0')}`;
}

/**
 * Apply ending ,99 on integer cents - returns minimum value >= cents that ends with 99
 */
export function toComma99Cents(cents: number): number {
  if (cents % 100 === 99) return cents;
  
  const euros = Math.floor(cents / 100);
  let target = euros * 100 + 99;
  
  if (target < cents) {
    target = (euros + 1) * 100 + 99;
  }
  
  return target;
}

/**
 * Validate that a cents value ends with 99
 */
export function validateEnding99Cents(cents: number): boolean {
  return (cents % 100) === 99;
}

/**
 * Legacy functions for backward compatibility
 */
export function roundToCents(n: number): number {
  if (!Number.isFinite(n)) return NaN;
  return Math.floor(n * 100 + 0.5) / 100;
}

export function validateEnding99(value: number): boolean {
  if (!Number.isFinite(value)) return false;
  const cents = Math.floor(value * 100 + 0.5);
  return (cents % 100) === 99;
}

/**
 * ListPrice with Fee computation - integer ceiling after full pipeline
 */
export function computeFromListPrice(
  listPrice: number,
  fees: { feeDeRev: number; feeMarketplace: number },
  shipping: number = 6,
  ivaPerc: number = 22
): { finalInt: number; finalDisplayInt: string } {
  
  // Convert to cents for all calculations
  const baseCents = toCents(listPrice);
  const shippingCents = toCents(shipping);
  
  // Step by step calculation in cents
  const afterShippingCents = baseCents + shippingCents;
  const afterIvaCents = Math.floor((afterShippingCents * (100 + ivaPerc)) / 100);
  const afterFeeDeRevCents = Math.floor(afterIvaCents * fees.feeDeRev);
  const afterFeesCents = Math.floor(afterFeeDeRevCents * fees.feeMarketplace);
  
  // Ceiling to next integer (multiple of 100 cents)
  const finalInt = Math.ceil(afterFeesCents / 100) * 100;
  const finalDisplayInt = Math.floor(finalInt / 100).toString();
  
  return { finalInt, finalDisplayInt };
}

/**
 * Unified EAN price computation in integer cents
 */
export function computeFinalEan(
  input: { listPrice: number; custBestPrice?: number },
  fees: { feeDeRev: number; feeMarketplace: number },
  shipping: number = 6,
  ivaPerc: number = 22
): { 
  subtotalCents: number; 
  subtotalDisplay: string; 
  finalCents: number; 
  finalDisplay: string; 
  route: string; 
  debug: any 
} {
  
  // Select base price source
  const usesCbp = input.custBestPrice && input.custBestPrice > 0 && Number.isFinite(input.custBestPrice);
  const route = usesCbp ? 'cbp' : 'listprice';
  const basePrice = usesCbp ? input.custBestPrice! : Math.ceil(input.listPrice);
  
  // Convert to cents for all calculations
  const baseCents = toCents(basePrice);
  const shippingCents = toCents(shipping);
  
  // Step by step calculation in cents
  const afterShippingCents = baseCents + shippingCents;
  const afterIvaCents = Math.floor((afterShippingCents * (100 + ivaPerc)) / 100);
  const afterFeeDeRevCents = Math.floor(afterIvaCents * fees.feeDeRev);
  const afterFeesCents = Math.floor(afterFeeDeRevCents * fees.feeMarketplace);
  
  // Subtotal after fees (before ,99 ceiling)
  const subtotalCents = afterFeesCents;
  const subtotalDisplay = formatCents(subtotalCents);
  
  // Apply ,99 ending
  const finalCents = toComma99Cents(afterFeesCents);
  const finalDisplay = formatCents(finalCents);
  
  // Debug info
  const debug = {
    route,
    baseCents,
    afterShippingCents,
    afterIvaCents,
    afterFeeDeRevCents,
    afterFeesCents,
    subtotalCents,
    finalCents
  };
  
  // Sample logging for first few calculations
  if (typeof (globalThis as any).eanSampleCount === 'undefined') {
    (globalThis as any).eanSampleCount = 0;
  }
  if ((globalThis as any).eanSampleCount < 6) {
    const sampleType = route === 'cbp' ? 'ean:sample:cbp' : 'ean:sample:listprice';
    console.warn(sampleType, {
      base: basePrice,
      route,
      baseCents,
      subtotalCents,
      finalCents,
      subtotalDisplay,
      finalDisplay
    });
    (globalThis as any).eanSampleCount++;
  }
  
  return { subtotalCents, subtotalDisplay, finalCents, finalDisplay, route, debug };
}
