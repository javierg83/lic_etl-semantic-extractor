ALTER TABLE public.homologaciones_productos
ADD COLUMN IF NOT EXISTS razonamiento_general TEXT;
