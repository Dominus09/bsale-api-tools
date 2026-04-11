-- Orden de visita definido manualmente (independiente de orden_ruta / ORS).
-- Ejecutar una vez en bases existentes. Instalaciones nuevas: ver rutero_schema.sql.

ALTER TABLE bsale.rutero
    ADD COLUMN IF NOT EXISTS orden_manual INTEGER;

CREATE INDEX IF NOT EXISTS idx_rutero_orden_manual
    ON bsale.rutero (vendedor, dia_atencion, orden_manual);
