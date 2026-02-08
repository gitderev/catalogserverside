
-- Add Amazon export columns to fee_config following existing per-export pattern
ALTER TABLE public.fee_config
  ADD COLUMN IF NOT EXISTS amazon_include_eu boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS amazon_it_preparation_days integer NOT NULL DEFAULT 3,
  ADD COLUMN IF NOT EXISTS amazon_eu_preparation_days integer NOT NULL DEFAULT 5,
  ADD COLUMN IF NOT EXISTS amazon_fee_drev numeric DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS amazon_fee_mkt numeric DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS amazon_shipping_cost numeric DEFAULT NULL;
