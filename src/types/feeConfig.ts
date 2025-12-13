/**
 * Extended Fee Config Types
 * 
 * These types extend the auto-generated Supabase types to include
 * new columns added for the Stock Location IT/EU feature and per-export pricing.
 */

import type { Tables } from '@/integrations/supabase/types';

// Base type from auto-generated types
type FeeConfigRow = Tables<'fee_config'>;

// Singleton ID for fee_config table
export const FEE_CONFIG_SINGLETON_ID = '00000000-0000-0000-0000-000000000001';

/**
 * Extended fee_config row with IT/EU stock location fields and per-export pricing.
 * Use this type when querying fee_config with the new columns.
 */
export interface FeeConfigExtendedRow extends FeeConfigRow {
  mediaworld_include_eu: boolean;
  mediaworld_it_preparation_days: number;
  mediaworld_eu_preparation_days: number;
  eprice_include_eu: boolean;
  eprice_it_preparation_days: number;
  eprice_eu_preparation_days: number;
  // Per-export pricing columns (NULL = use global)
  ean_fee_drev: number | null;
  ean_fee_mkt: number | null;
  ean_shipping_cost: number | null;
  mediaworld_fee_drev: number | null;
  mediaworld_fee_mkt: number | null;
  mediaworld_shipping_cost: number | null;
  eprice_fee_drev: number | null;
  eprice_fee_mkt: number | null;
  eprice_shipping_cost: number | null;
}

/**
 * Per-export pricing configuration
 */
export interface ExportPricingConfig {
  feeDrev: number;
  feeMkt: number;
  shippingCost: number;
}

/**
 * Client-side fee config state with all IT/EU fields and per-export pricing.
 */
export interface FeeConfigState {
  // Global defaults
  feeDrev: number;
  feeMkt: number;
  shippingCost: number;
  // Legacy fields (kept for backward compatibility)
  mediaworldPreparationDays: number;
  epricePreparationDays: number;
  // IT/EU fields
  mediaworldIncludeEu: boolean;
  mediaworldItPreparationDays: number;
  mediaworldEuPreparationDays: number;
  epriceIncludeEu: boolean;
  epriceItPreparationDays: number;
  epriceEuPreparationDays: number;
  // Per-export pricing (null = use global)
  eanFeeDrev: number | null;
  eanFeeMkt: number | null;
  eanShippingCost: number | null;
  mediaworldFeeDrev: number | null;
  mediaworldFeeMkt: number | null;
  mediaworldShippingCost: number | null;
  epriceFeeDrev: number | null;
  epriceFeeMkt: number | null;
  epriceShippingCost: number | null;
}

/**
 * Default fee config values
 * 
 * BACKWARD COMPATIBILITY: includeEu defaults to TRUE for both marketplaces
 * because the previous behavior used ExistingStock (total), which is equivalent
 * to "includeEU always ON". This preserves existing behavior until user changes it.
 * 
 * Per-export pricing defaults to NULL (use global values).
 */
export const DEFAULT_FEE_CONFIG: FeeConfigState = {
  feeDrev: 1.05,
  feeMkt: 1.08,
  shippingCost: 6.00,
  mediaworldPreparationDays: 3,
  epricePreparationDays: 1,
  // Backward compatibility: default to TRUE (previously used total stock)
  mediaworldIncludeEu: true,
  mediaworldItPreparationDays: 3,
  mediaworldEuPreparationDays: 5,
  epriceIncludeEu: true,
  epriceItPreparationDays: 1,
  epriceEuPreparationDays: 3,
  // Per-export pricing: null = use global
  eanFeeDrev: null,
  eanFeeMkt: null,
  eanShippingCost: null,
  mediaworldFeeDrev: null,
  mediaworldFeeMkt: null,
  mediaworldShippingCost: null,
  epriceFeeDrev: null,
  epriceFeeMkt: null,
  epriceShippingCost: null
};

/**
 * Get effective pricing for an export type (with fallback to global)
 */
export function getEffectivePricing(
  config: FeeConfigState,
  exportType: 'ean' | 'mediaworld' | 'eprice'
): ExportPricingConfig {
  switch (exportType) {
    case 'ean':
      return {
        feeDrev: config.eanFeeDrev ?? config.feeDrev,
        feeMkt: config.eanFeeMkt ?? config.feeMkt,
        shippingCost: config.eanShippingCost ?? config.shippingCost
      };
    case 'mediaworld':
      return {
        feeDrev: config.mediaworldFeeDrev ?? config.feeDrev,
        feeMkt: config.mediaworldFeeMkt ?? config.feeMkt,
        shippingCost: config.mediaworldShippingCost ?? config.shippingCost
      };
    case 'eprice':
      return {
        feeDrev: config.epriceFeeDrev ?? config.feeDrev,
        feeMkt: config.epriceFeeMkt ?? config.feeMkt,
        shippingCost: config.epriceShippingCost ?? config.shippingCost
      };
  }
}

/**
 * Maps database row to client state
 * 
 * BACKWARD COMPATIBILITY: If includeEu is null/undefined, default to TRUE
 * because previous behavior used ExistingStock (total) which is equivalent
 * to "includeEU always ON". DO NOT use Boolean() as it converts null to false.
 */
export function mapFeeConfigRowToState(row: FeeConfigExtendedRow): FeeConfigState {
  return {
    feeDrev: Number(row.fee_drev),
    feeMkt: Number(row.fee_mkt),
    shippingCost: Number(row.shipping_cost),
    mediaworldPreparationDays: Number(row.mediaworld_preparation_days),
    epricePreparationDays: Number(row.eprice_preparation_days),
    // CRITICAL: null/undefined → true (backward compat), explicit false → false
    mediaworldIncludeEu: row.mediaworld_include_eu == null ? true : !!row.mediaworld_include_eu,
    mediaworldItPreparationDays: Number(row.mediaworld_it_preparation_days),
    mediaworldEuPreparationDays: Number(row.mediaworld_eu_preparation_days),
    // CRITICAL: null/undefined → true (backward compat), explicit false → false
    epriceIncludeEu: row.eprice_include_eu == null ? true : !!row.eprice_include_eu,
    epriceItPreparationDays: Number(row.eprice_it_preparation_days),
    epriceEuPreparationDays: Number(row.eprice_eu_preparation_days),
    // Per-export pricing (null preserved = use global)
    eanFeeDrev: row.ean_fee_drev != null ? Number(row.ean_fee_drev) : null,
    eanFeeMkt: row.ean_fee_mkt != null ? Number(row.ean_fee_mkt) : null,
    eanShippingCost: row.ean_shipping_cost != null ? Number(row.ean_shipping_cost) : null,
    mediaworldFeeDrev: row.mediaworld_fee_drev != null ? Number(row.mediaworld_fee_drev) : null,
    mediaworldFeeMkt: row.mediaworld_fee_mkt != null ? Number(row.mediaworld_fee_mkt) : null,
    mediaworldShippingCost: row.mediaworld_shipping_cost != null ? Number(row.mediaworld_shipping_cost) : null,
    epriceFeeDrev: row.eprice_fee_drev != null ? Number(row.eprice_fee_drev) : null,
    epriceFeeMkt: row.eprice_fee_mkt != null ? Number(row.eprice_fee_mkt) : null,
    epriceShippingCost: row.eprice_shipping_cost != null ? Number(row.eprice_shipping_cost) : null
  };
}

/**
 * Stock location warnings tracking
 */
export interface StockLocationWarningsState {
  missing_location_file: number;
  invalid_location_parse: number;
  missing_location_data: number;
  split_mismatch: number;
  multi_mpn_per_matnr: number;
  orphan_4255: number;
  decode_fallback_used: number;
  invalid_stock_value: number;
}

export const EMPTY_WARNINGS: StockLocationWarningsState = {
  missing_location_file: 0,
  invalid_location_parse: 0,
  missing_location_data: 0,
  split_mismatch: 0,
  multi_mpn_per_matnr: 0,
  orphan_4255: 0,
  decode_fallback_used: 0,
  invalid_stock_value: 0
};
