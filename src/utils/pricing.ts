/**
 * Pricing utilities for catalog generation - Pure integer cents arithmetic with robust IT locale parsing
 */

// Typed globalThis extension for one-time logging flags
interface PricingGlobals {
  eanEndingInitLogged?: boolean;
  eanSampleCount?: number;
}
const _pricingGlobals = globalThis as unknown as PricingGlobals;

// One-time log to confirm integer-cents implementation
if (typeof _pricingGlobals.eanEndingInitLogged === 'undefined') {
  console.warn('ean:ending:function=int-cents');
  _pricingGlobals.eanEndingInitLogged = true;
}

/**
 * Robust IT locale parsing for any input (handles commas, thousand separators, percentages, dirty inputs)
 */
export function parseEuroLike(input: unknown): number {
  if (typeof input === 'number' && isFinite(input)) return input;
  let s = String(input ?? '').trim();
  s = s.replace(/[^\d.,\s%\-]/g, '').trim();
  s = s.split(/\s+/)[0] ?? '';
  s = s.replace(/%/g, '').trim();
  if (!s) return NaN;

  if (s.includes('.') && s.includes(',')) s = s.replace(/\./g, '').replace(',', '.');
  else s = s.replace(',', '.');

  const n = parseFloat(s);
  return Number.isFinite(n) ? n : NaN;
}

/**
 * Convert a number or string to integer cents (avoiding floating point issues)
 */
export function toCents(x: unknown, fallback = 0): number {
  const n = parseEuroLike(x);
  return Number.isFinite(n) ? Math.round(n * 100) : Math.round(fallback * 100);
}

/**
 * Parse percentage string to rate (e.g. "22%" -> 1.22)
 */
export function parsePercentToRate(v: unknown, fallbackPercent = 22): number {
  const n = parseEuroLike(v);
  const p = Number.isFinite(n) ? n : fallbackPercent;
  return 1 + p / 100; // 22 -> 1.22
}

/**
 * Parse rate value with fallback
 */
export function parseRate(v: unknown, fallback = 1): number {
  const n = parseEuroLike(v);
  return Number.isFinite(n) && n > 0 ? n : fallback;
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
 * Additional utility functions for cents-based calculations
 */
export function roundCents(cents: number): number {
  return Math.round(cents);
}

export function applyRateCents(cents: number, rate: number): number {
  return roundCents(cents * rate);
}

// Legacy alias for backward compatibility
export function applyRate(cents: number, rate: number): number {
  return applyRateCents(cents, rate);
}

export function ceilToComma99(cents: number): number {
  return toComma99Cents(cents);
}

export function ceilToIntegerEuros(cents: number): number {
  return Math.ceil(cents / 100);
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
  
  // Step by step calculation in cents - apply both fees as multipliers
  const afterShippingCents = baseCents + shippingCents;
  const afterIvaCents = applyRate(afterShippingCents, (100 + ivaPerc) / 100);
  const afterFeeDeRevCents = applyRate(afterIvaCents, fees.feeDeRev / 100);
  const afterFeesCents = applyRate(afterFeeDeRevCents, fees.feeMarketplace / 100);
  
  // Ceiling to next integer (multiple of 100 cents)
  const finalInt = Math.ceil(afterFeesCents / 100) * 100;
  const finalDisplayInt = Math.floor(finalInt / 100).toString();
  
  return { finalInt, finalDisplayInt };
}

/**
 * @deprecated This function contains LP/CBP routing logic and should not be used.
 * For new code, calculate basePriceCents directly using Math.round() and apply fees in cents,
 * then use toComma99Cents() directly for the ,99 ending.
 * 
 * Unified EAN price computation in integer cents (LEGACY - DO NOT USE)
 */
export function computeFinalEan(
  input: { listPrice: number; custBestPrice?: number; surcharge?: number },
  fees: { feeDeRev: number; feeMarketplace: number },
  shipping: number = 6,
  ivaPerc: number = 22
): { 
  subtotalCents: number; 
  subtotalDisplay: string; 
  finalCents: number; 
  finalDisplay: string; 
  route: string; 
  debug: Record<string, unknown> 
} {
  
  // Ensure surcharge is valid and non-negative
  const validSurcharge = (input.surcharge && Number.isFinite(input.surcharge) && input.surcharge >= 0) ? input.surcharge : 0;
  
  // Select base price source
  const usesCbp = input.custBestPrice && input.custBestPrice > 0 && Number.isFinite(input.custBestPrice);
  const route = usesCbp ? 'cbp' : 'listprice';
  
  // Calculate basePrice according to route
  let basePrice: number;
  if (usesCbp) {
    // CBP ROUTE: ALWAYS use CustBestPrice + Surcharge
    basePrice = input.custBestPrice! + validSurcharge;
  } else {
    // LP ROUTE: use ListPrice only (ceiled), NO Surcharge
    basePrice = Math.ceil(input.listPrice);
  }
  
  // Convert to cents for all calculations
  const baseCents = toCents(basePrice);
  const shippingCents = toCents(shipping);
  
  // Step by step calculation in cents - apply both fees as multipliers
  const afterShippingCents = baseCents + shippingCents;
  const afterIvaCents = applyRate(afterShippingCents, (100 + ivaPerc) / 100);
  const afterFeeDeRevCents = applyRate(afterIvaCents, fees.feeDeRev / 100);
  const afterFeesCents = applyRate(afterFeeDeRevCents, fees.feeMarketplace / 100);
  
  // Subtotal after fees (before ,99 ceiling)
  const subtotalCents = afterFeesCents;
  const subtotalDisplay = formatCents(subtotalCents);
  
  // Apply ,99 ending
  const finalCents = toComma99Cents(afterFeesCents);
  const finalDisplay = formatCents(finalCents);
  
  // Debug info
  const debug: Record<string, unknown> = {
    route,
    custBestPrice: input.custBestPrice,
    surcharge: validSurcharge,
    basePrice,
    baseCents,
    afterShippingCents,
    afterIvaCents,
    afterFeeDeRevCents,
    afterFeesCents,
    subtotalCents,
    finalCents
  };
  
  // Sample logging for first few calculations
  if (typeof _pricingGlobals.eanSampleCount === 'undefined') {
    _pricingGlobals.eanSampleCount = 0;
  }
  if (_pricingGlobals.eanSampleCount! < 6) {
    const sampleType = route === 'cbp' ? 'ean:sample:cbp:pricing' : 'ean:sample:listprice:pricing';
    console.warn(sampleType, {
      base: basePrice,
      route,
      ...(route === 'cbp' && { 
        custBestPrice: input.custBestPrice, 
        surcharge: validSurcharge,
        sumCbpSurcharge: input.custBestPrice! + validSurcharge
      }),
      baseCents,
      subtotalCents,
      finalCents,
      subtotalDisplay,
      finalDisplay
    });
    _pricingGlobals.eanSampleCount!++;
  }
  
  return { subtotalCents, subtotalDisplay, finalCents, finalDisplay, route, debug };
}
