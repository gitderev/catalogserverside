-- ============================================================
-- STOCK LOCATION SPLIT IT/EU: Database Migration
-- ============================================================

-- Step 1: Add new columns to fee_config for IT/EU configuration
ALTER TABLE public.fee_config
ADD COLUMN IF NOT EXISTS mediaworld_include_eu boolean NOT NULL DEFAULT false,
ADD COLUMN IF NOT EXISTS mediaworld_it_preparation_days integer NOT NULL DEFAULT 3,
ADD COLUMN IF NOT EXISTS mediaworld_eu_preparation_days integer NOT NULL DEFAULT 5,
ADD COLUMN IF NOT EXISTS eprice_include_eu boolean NOT NULL DEFAULT false,
ADD COLUMN IF NOT EXISTS eprice_it_preparation_days integer NOT NULL DEFAULT 1,
ADD COLUMN IF NOT EXISTS eprice_eu_preparation_days integer NOT NULL DEFAULT 3;

-- Step 2: Backfill IT preparation days from existing columns
UPDATE public.fee_config
SET mediaworld_it_preparation_days = mediaworld_preparation_days,
    eprice_it_preparation_days = eprice_preparation_days
WHERE mediaworld_it_preparation_days = 3 AND eprice_it_preparation_days = 1;

-- Step 3: Add metrics column to sync_runs if not exists
ALTER TABLE public.sync_runs
ADD COLUMN IF NOT EXISTS location_warnings jsonb NOT NULL DEFAULT '{}'::jsonb;

-- Step 4: Ensure singleton fee_config with id='global'
-- First, identify the "most recent" row to keep
DO $$
DECLARE
    keep_id uuid;
    row_count integer;
BEGIN
    -- Count how many rows exist
    SELECT COUNT(*) INTO row_count FROM public.fee_config;
    
    IF row_count = 0 THEN
        -- No rows, insert a default one with id='global' (as text converted to uuid won't work, use a fixed uuid)
        INSERT INTO public.fee_config (
            id, fee_drev, fee_mkt, shipping_cost, 
            mediaworld_preparation_days, eprice_preparation_days,
            mediaworld_include_eu, mediaworld_it_preparation_days, mediaworld_eu_preparation_days,
            eprice_include_eu, eprice_it_preparation_days, eprice_eu_preparation_days
        ) VALUES (
            '00000000-0000-0000-0000-000000000001'::uuid,
            1.05, 1.08, 6.00, 3, 1,
            false, 3, 5,
            false, 1, 3
        );
    ELSIF row_count = 1 THEN
        -- Exactly one row, no cleanup needed
        NULL;
    ELSE
        -- Multiple rows, keep the most recent one based on updated_at (or created_at, or MIN id)
        SELECT id INTO keep_id
        FROM public.fee_config
        ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST, id ASC
        LIMIT 1;
        
        -- Delete all other rows
        DELETE FROM public.fee_config WHERE id != keep_id;
    END IF;
END $$;