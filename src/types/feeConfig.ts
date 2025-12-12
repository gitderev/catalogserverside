/**
 * Extended Fee Config Types
 * 
 * These types extend the auto-generated Supabase types to include
 * new columns added for the Stock Location IT/EU feature.
 */

import type { Tables } from '@/integrations/supabase/types';

// Base type from auto-generated types
type FeeConfigRow = Tables<'fee_config'>;

/**
 * Extended fee_config row with IT/EU stock location fields.
 * Use this type when querying fee_config with the new columns.
 */
export interface FeeConfigExtendedRow extends FeeConfigRow {
  mediaworld_include_eu: boolean;
  mediaworld_it_preparation_days: number;
  mediaworld_eu_preparation_days: number;
  eprice_include_eu: boolean;
  eprice_it_preparation_days: number;
  eprice_eu_preparation_days: number;
}

/**
 * Client-side fee config state with all IT/EU fields.
 */
export interface FeeConfigState {
  feeDrev: number;
  feeMkt: number;
  shippingCost: number;
  // Legacy fields (kept for backward compatibility)
  mediaworldPreparationDays: number;
  epricePreparationDays: number;
  // New IT/EU fields
  mediaworldIncludeEu: boolean;
  mediaworldItPreparationDays: number;
  mediaworldEuPreparationDays: number;
  epriceIncludeEu: boolean;
  epriceItPreparationDays: number;
  epriceEuPreparationDays: number;
}

/**
 * Default fee config values
 * 
 * BACKWARD COMPATIBILITY: includeEu defaults to TRUE for both marketplaces
 * because the previous behavior used ExistingStock (total), which is equivalent
 * to "includeEU always ON". This preserves existing behavior until user changes it.
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
  epriceEuPreparationDays: 3
};

/**
 * Maps database row to client state
 */
export function mapFeeConfigRowToState(row: FeeConfigExtendedRow): FeeConfigState {
  return {
    feeDrev: Number(row.fee_drev),
    feeMkt: Number(row.fee_mkt),
    shippingCost: Number(row.shipping_cost),
    mediaworldPreparationDays: Number(row.mediaworld_preparation_days),
    epricePreparationDays: Number(row.eprice_preparation_days),
    mediaworldIncludeEu: Boolean(row.mediaworld_include_eu),
    mediaworldItPreparationDays: Number(row.mediaworld_it_preparation_days),
    mediaworldEuPreparationDays: Number(row.mediaworld_eu_preparation_days),
    epriceIncludeEu: Boolean(row.eprice_include_eu),
    epriceItPreparationDays: Number(row.eprice_it_preparation_days),
    epriceEuPreparationDays: Number(row.eprice_eu_preparation_days)
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
