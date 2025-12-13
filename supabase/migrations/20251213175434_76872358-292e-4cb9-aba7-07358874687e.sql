-- =====================================================================
-- Migration: Add per-export pricing configuration columns
-- =====================================================================
-- This adds dedicated fee/shipping columns for each export type:
-- EAN, Mediaworld, ePrice (Amazon to be added later)
-- 
-- NULL values mean "use global fee_drev/fee_mkt/shipping_cost"
-- =====================================================================

-- Add EAN export-specific columns
ALTER TABLE public.fee_config 
ADD COLUMN IF NOT EXISTS ean_fee_drev numeric(6,4) DEFAULT NULL,
ADD COLUMN IF NOT EXISTS ean_fee_mkt numeric(6,4) DEFAULT NULL,
ADD COLUMN IF NOT EXISTS ean_shipping_cost numeric(10,2) DEFAULT NULL;

-- Add Mediaworld export-specific columns
ALTER TABLE public.fee_config 
ADD COLUMN IF NOT EXISTS mediaworld_fee_drev numeric(6,4) DEFAULT NULL,
ADD COLUMN IF NOT EXISTS mediaworld_fee_mkt numeric(6,4) DEFAULT NULL,
ADD COLUMN IF NOT EXISTS mediaworld_shipping_cost numeric(10,2) DEFAULT NULL;

-- Add ePrice export-specific columns
ALTER TABLE public.fee_config 
ADD COLUMN IF NOT EXISTS eprice_fee_drev numeric(6,4) DEFAULT NULL,
ADD COLUMN IF NOT EXISTS eprice_fee_mkt numeric(6,4) DEFAULT NULL,
ADD COLUMN IF NOT EXISTS eprice_shipping_cost numeric(10,2) DEFAULT NULL;

-- Create singleton row with fixed ID if it doesn't exist
INSERT INTO public.fee_config (
  id, 
  fee_drev, 
  fee_mkt, 
  shipping_cost,
  updated_at
)
VALUES (
  '00000000-0000-0000-0000-000000000001'::uuid,
  1.05,
  1.08,
  6.00,
  now()
)
ON CONFLICT (id) DO NOTHING;

-- Add comment to document singleton pattern
COMMENT ON TABLE public.fee_config IS 'Fee configuration singleton. Always use id=00000000-0000-0000-0000-000000000001. Per-export columns (ean_*, mediaworld_*, eprice_*) override globals when not NULL.';