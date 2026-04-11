-- Notas operativas por fila rutero (vista listado / distribuidora).
ALTER TABLE bsale.rutero
    ADD COLUMN IF NOT EXISTS observaciones TEXT;
