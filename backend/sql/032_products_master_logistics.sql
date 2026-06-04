-- Maestro logístico: columnas Bsale + cubicación ERP (sin borrar datos manuales en sync).

ALTER TABLE bsale.variants
    ADD COLUMN IF NOT EXISTS units_per_box INTEGER;
-- +go

ALTER TABLE bsale.products_master
    ADD COLUMN IF NOT EXISTS product_id BIGINT;
-- +go

ALTER TABLE bsale.products_master
    ADD COLUMN IF NOT EXISTS variant_id BIGINT;
-- +go

ALTER TABLE bsale.products_master
    ADD COLUMN IF NOT EXISTS units_per_box INTEGER;
-- +go

ALTER TABLE bsale.products_master
    ADD COLUMN IF NOT EXISTS peso_caja_kg NUMERIC(12, 4);
-- +go

ALTER TABLE bsale.products_master
    ADD COLUMN IF NOT EXISTS alto_caja_cm NUMERIC(12, 2);
-- +go

ALTER TABLE bsale.products_master
    ADD COLUMN IF NOT EXISTS ancho_caja_cm NUMERIC(12, 2);
-- +go

ALTER TABLE bsale.products_master
    ADD COLUMN IF NOT EXISTS largo_caja_cm NUMERIC(12, 2);
-- +go

ALTER TABLE bsale.products_master
    ADD COLUMN IF NOT EXISTS logistics_completed BOOLEAN NOT NULL DEFAULT FALSE;
-- +go

ALTER TABLE bsale.products_master
    ADD COLUMN IF NOT EXISTS last_bsale_sync_at TIMESTAMPTZ;
-- +go

CREATE INDEX IF NOT EXISTS idx_products_master_variant_id
    ON bsale.products_master (variant_id);
-- +go

CREATE INDEX IF NOT EXISTS idx_products_master_logistics_completed
    ON bsale.products_master (logistics_completed);
-- +go

COMMENT ON COLUMN bsale.products_master.peso_caja_kg IS
    'Peso bruto por caja (kg). Peso unitario = peso_caja_kg / NULLIF(units_per_box,0).';
-- +go

COMMENT ON COLUMN bsale.products_master.last_bsale_sync_at IS
    'Última vez que refresh_products_master actualizó datos desde Bsale.';
-- +go
