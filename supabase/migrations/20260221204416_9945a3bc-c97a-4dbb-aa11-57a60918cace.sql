
-- Ensure fee_config singleton row exists (idempotent)
-- If missing, insert with defaults. If already present, do nothing.
INSERT INTO public.fee_config (
  id, fee_drev, fee_mkt, shipping_cost,
  mediaworld_preparation_days, eprice_preparation_days,
  mediaworld_include_eu, mediaworld_it_preparation_days, mediaworld_eu_preparation_days,
  eprice_include_eu, eprice_it_preparation_days, eprice_eu_preparation_days,
  amazon_include_eu, amazon_it_preparation_days, amazon_eu_preparation_days
)
VALUES (
  '00000000-0000-0000-0000-000000000001',
  1.05, 1.08, 6.00,
  3, 1,
  true, 3, 5,
  true, 1, 3,
  true, 3, 5
)
ON CONFLICT (id) DO NOTHING;
