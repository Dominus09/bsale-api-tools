-- Restricciones básicas en bsale.rutero (idempotente en lo posible).
-- No modifica bsale.clients.
-- Si SET NOT NULL falla, corregir filas con company_id o bsale_id nulos antes de reintentar.

ALTER TABLE bsale.rutero
    ALTER COLUMN company_id SET NOT NULL;

ALTER TABLE bsale.rutero
    ALTER COLUMN bsale_id SET NOT NULL;

DO $migration$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'chk_tipo_atencion'
          AND conrelid = 'bsale.rutero'::regclass
    ) THEN
        ALTER TABLE bsale.rutero
            ADD CONSTRAINT chk_tipo_atencion
            CHECK (tipo_atencion IN ('terreno', 'telefonico'));
    END IF;
END
$migration$;

ALTER TABLE bsale.rutero
    ALTER COLUMN activo SET DEFAULT TRUE;
