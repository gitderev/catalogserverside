/**
 * Pricing utilities for catalog generation - Pure integer cents arithmetic with robust IT locale parsing
 */

// One-time log to confirm integer-cents implementation
if (typeof (globalThis as any).eanEndingInitLogged === 'undefined') {
  console.warn('ean:ending:function=int-cents');
  (globalThis as any).eanEndingInitLogged = true;
}

/**
 * Robust IT locale parsing for any input (handles commas, thousand separators, percentages, dirty inputs)
 */
export function parseEuroLike(input: unknown): number {
  if (typeof input === 'number' && isFinite(input)) return input;
  let s = String(input ?? '').trim();
  // Remove common non-numeric symbols (except . , space, - and %)
  s = s.replace(/[^\d.,\s%\-]/g, '').trim();
  // Take first token (e.g. "1,07 0,00" -> "1,07")
  s = s.split(/\s+/)[0] ?? '';
  s = s.replace(/%/g, '').trim();
  if (!s) return NaN;

  // If both . and , present, assume IT format (1.234,56)
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
 * SKU-specific fee calculation types and functions
 */
export type Fee = { kind: 'percent' | 'absolute'; value: number }; // percent in decimal: 0.05 = 5%
export type SkuCfg = { fees: Fee[]; shippingEuro?: number; vatRate?: number };

function applyFeesSeqCents(cents: number, fees: Fee[]): number {
  let v = cents;
  for (const f of fees ?? []) {
    if (f.kind === 'percent') {
      v = Math.round(v * (1 + f.value));
    } else {
      v += toCents(f.value);
    }
  }
  return v;
}

export function computeFinalSkuCents(baseEuro: number, cfg: SkuCfg): number {
  if (!isFinite(baseEuro) || baseEuro <= 0) throw new Error('Prezzo base non valido');
  const ship = toCents(cfg.shippingEuro ?? 6);
  const vat = cfg.vatRate ?? 0.22;

  let v = toCents(baseEuro) + ship;   // + spedizione
  v = Math.round(v * (1 + vat));      // + IVA
  v = applyFeesSeqCents(v, cfg.fees); // + fee sequenziali
  v = Math.ceil(v / 100) * 100;       // arrotondamento all'euro superiore
  return v;                           // requisito: v % 100 === 0
}

// Audit: parte SEMPRE da ListPrice
export function computeListPriceWithFeeCents(listPriceEuro: number, cfg: SkuCfg): number | null {
  if (!isFinite(listPriceEuro) || listPriceEuro <= 0) return null;
  const ship = toCents(cfg.shippingEuro ?? 6);
  const vat = cfg.vatRate ?? 0.22;
  let v = toCents(listPriceEuro) + ship;
  v = Math.round(v * (1 + vat));
  v = applyFeesSeqCents(v, cfg.fees);
  return Math.ceil(v / 100) * 100; // intero
}

function pickSkuBaseEuro(row: any): number | null {
  const cbp = Number(row.CustBestPrice ?? NaN);
  const lp = Number(row.ListPrice ?? NaN);
  if (isFinite(cbp) && cbp > 0) return cbp;
  if (isFinite(lp) && lp > 0) return lp;
  return null;
}

export function buildSkuCatalog(rows: any[], cfg: SkuCfg): any[] {
  const out = [];
  const rejects: {idx: number; reason: string}[] = [];

  rows.forEach((r, idx) => {
    const stock = Number(r.ExistingStock ?? NaN);
    if (!isFinite(stock) || stock <= 1) { 
      rejects.push({idx, reason: 'stock'}); 
      return; 
    }

    const mpn = String(r.ManufPartNr ?? '').trim();
    if (!mpn) { 
      rejects.push({idx, reason: 'mpn_vuoto'}); 
      return; 
    }

    const base = pickSkuBaseEuro(r);
    if (base == null) { 
      rejects.push({idx, reason: 'prezzo_base'}); 
      return; 
    }

    const finalCents = computeFinalSkuCents(base, cfg);
    const lpAuditCents = computeListPriceWithFeeCents(Number(r.ListPrice ?? NaN), cfg);

    // Calculate pre-fee price for display
    const ship = toCents(cfg.shippingEuro ?? 6);
    const vat = cfg.vatRate ?? 0.22;
    const preFeeC = Math.round((toCents(base) + ship) * (1 + vat));
    const preFeeEuro = preFeeC / 100;

    // Calculate individual fees for display (sequential application)
    const baseWithShipVat = preFeeC;
    let feeDeRevEuro = 0;
    let feeMktEuro = 0;
    let runningTotal = baseWithShipVat;
    
    // Apply FeeDeRev (first fee)
    if (cfg.fees.length >= 1 && cfg.fees[0].kind === 'percent') {
      feeDeRevEuro = (runningTotal * cfg.fees[0].value) / 100;
      runningTotal = runningTotal + Math.round(feeDeRevEuro * 100);
    } else if (cfg.fees.length >= 1 && cfg.fees[0].kind === 'absolute') {
      feeDeRevEuro = cfg.fees[0].value;
      runningTotal = runningTotal + toCents(cfg.fees[0].value);
    }
    
    // Apply Fee Marketplace (second fee, sequential)
    if (cfg.fees.length >= 2 && cfg.fees[1].kind === 'percent') {
      feeMktEuro = (runningTotal * cfg.fees[1].value) / 100;
    } else if (cfg.fees.length >= 2 && cfg.fees[1].kind === 'absolute') {
      feeMktEuro = cfg.fees[1].value;
    }

    const subtotalePostFee = preFeeEuro + feeDeRevEuro + feeMktEuro;

    out.push({
      ...r,
      ManufPartNr: mpn,
      Percorso: 'SKU',
      PrezzoBasePercorso: base,
      'Costo di Spedizione': 6.00,
      IVA: 0.22,
      'Prezzo con spedizione e IVA': preFeeEuro,
      FeeDeRev: feeDeRevEuro,
      'Fee Marketplace': feeMktEuro,
      'Subtotale post-fee': subtotalePostFee,
      'Prezzo Finale': finalCents / 100,
      ...(lpAuditCents != null ? {'ListPrice con Fee': lpAuditCents / 100} : {}),
    });
  });

  // Validazioni dure
  out.forEach(o => {
    const c = toCents(Number(o['Prezzo Finale']));
    if (c % 100 !== 0) throw new Error('SKU: PrezzoFinale deve essere intero in euro');
    if ('ListPrice con Fee' in o) {
      const a = toCents(Number(o['ListPrice con Fee']));
      if (a % 100 !== 0) throw new Error('SKU: ListPrice con Fee deve essere intero');
    }
  });

  // Log rejects for debugging
  console.debug('SKU rejects', rejects);
  return out;
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
