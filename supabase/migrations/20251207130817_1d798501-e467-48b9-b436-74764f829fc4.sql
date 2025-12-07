-- Aggiungi le colonne per i giorni di preparazione alla tabella fee_config esistente
ALTER TABLE public.fee_config 
ADD COLUMN IF NOT EXISTS mediaworld_preparation_days integer NOT NULL DEFAULT 3,
ADD COLUMN IF NOT EXISTS eprice_preparation_days integer NOT NULL DEFAULT 1;

-- Aggiungi vincoli per garantire valori >= 1
ALTER TABLE public.fee_config 
ADD CONSTRAINT mediaworld_preparation_days_min CHECK (mediaworld_preparation_days >= 1),
ADD CONSTRAINT eprice_preparation_days_min CHECK (eprice_preparation_days >= 1);