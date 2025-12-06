-- Enable uuid-ossp extension if not already enabled
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create fee_config table
CREATE TABLE public.fee_config (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  fee_drev numeric(6,4) NOT NULL,
  fee_mkt numeric(6,4) NOT NULL,
  shipping_cost numeric(10,2) NOT NULL,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

-- Enable Row Level Security
ALTER TABLE public.fee_config ENABLE ROW LEVEL SECURITY;

-- Create RLS policies for authenticated users
CREATE POLICY "Authenticated users can select fee_config"
  ON public.fee_config
  FOR SELECT
  TO authenticated
  USING (true);

CREATE POLICY "Authenticated users can insert fee_config"
  ON public.fee_config
  FOR INSERT
  TO authenticated
  WITH CHECK (true);

CREATE POLICY "Authenticated users can update fee_config"
  ON public.fee_config
  FOR UPDATE
  TO authenticated
  USING (true)
  WITH CHECK (true);