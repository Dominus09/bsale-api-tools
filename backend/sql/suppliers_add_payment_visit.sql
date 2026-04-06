-- Agrega columnas opcionales a bsale.suppliers (sin borrar datos; NULL permitido).
-- Validación de negocio (CHECK, enums, etc.) se puede agregar en un script futuro.

ALTER TABLE bsale.suppliers
  ADD COLUMN IF NOT EXISTS payment_method TEXT,
  ADD COLUMN IF NOT EXISTS visit_day TEXT;
