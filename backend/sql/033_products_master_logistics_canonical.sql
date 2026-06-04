-- Maestro logístico canónico: nombres en inglés + vista para App Choferes / carga camiones.
-- Idempotente: renombra columnas legacy (español) si existen.

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'bsale' AND table_name = 'products_master' AND column_name = 'peso_caja_kg'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'bsale' AND table_name = 'products_master' AND column_name = 'weight_box_kg'
    ) THEN
        ALTER TABLE bsale.products_master RENAME COLUMN peso_caja_kg TO weight_box_kg;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'bsale' AND table_name = 'products_master' AND column_name = 'alto_caja_cm'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'bsale' AND table_name = 'products_master' AND column_name = 'height_cm'
    ) THEN
        ALTER TABLE bsale.products_master RENAME COLUMN alto_caja_cm TO height_cm;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'bsale' AND table_name = 'products_master' AND column_name = 'ancho_caja_cm'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'bsale' AND table_name = 'products_master' AND column_name = 'width_cm'
    ) THEN
        ALTER TABLE bsale.products_master RENAME COLUMN ancho_caja_cm TO width_cm;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'bsale' AND table_name = 'products_master' AND column_name = 'largo_caja_cm'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'bsale' AND table_name = 'products_master' AND column_name = 'length_cm'
    ) THEN
        ALTER TABLE bsale.products_master RENAME COLUMN largo_caja_cm TO length_cm;
    END IF;
END $$;
-- +go

ALTER TABLE bsale.products_master
    ADD COLUMN IF NOT EXISTS weight_box_kg NUMERIC(12, 4);
-- +go

ALTER TABLE bsale.products_master
    ADD COLUMN IF NOT EXISTS height_cm NUMERIC(12, 2);
-- +go

ALTER TABLE bsale.products_master
    ADD COLUMN IF NOT EXISTS width_cm NUMERIC(12, 2);
-- +go

ALTER TABLE bsale.products_master
    ADD COLUMN IF NOT EXISTS length_cm NUMERIC(12, 2);
-- +go

COMMENT ON COLUMN bsale.products_master.weight_box_kg IS
    'Peso bruto por caja (kg). weight_unit_kg se calcula en API/vista.';
-- +go

CREATE INDEX IF NOT EXISTS idx_products_master_weight_box
    ON bsale.products_master (weight_box_kg)
    WHERE weight_box_kg IS NOT NULL;
-- +go

DROP VIEW IF EXISTS bsale.v_product_logistics;
-- +go

CREATE VIEW bsale.v_product_logistics AS
SELECT
    pm.id AS products_master_id,
    pm.product_id,
    pm.variant_id,
    pm.barcode,
    pm.product_name,
    pm.variant_name,
    pm.units_per_box,
    pm.weight_box_kg,
    CASE
        WHEN pm.units_per_box IS NOT NULL
         AND pm.units_per_box > 0
         AND pm.weight_box_kg IS NOT NULL
         AND pm.weight_box_kg > 0
        THEN ROUND((pm.weight_box_kg / pm.units_per_box::numeric), 6)
        ELSE NULL
    END AS weight_unit_kg,
    pm.height_cm,
    pm.width_cm,
    pm.length_cm,
    CASE
        WHEN pm.height_cm IS NOT NULL AND pm.height_cm > 0
         AND pm.width_cm IS NOT NULL AND pm.width_cm > 0
         AND pm.length_cm IS NOT NULL AND pm.length_cm > 0
        THEN ROUND(
            (pm.height_cm * pm.width_cm * pm.length_cm) / 1000000.0,
            6
        )
        ELSE NULL
    END AS volume_m3,
    pm.supplier_id,
    pm.logistics_completed,
    pm.last_bsale_sync_at,
    pm.is_active,
    pm.updated_at
FROM bsale.products_master pm;
-- +go

COMMENT ON VIEW bsale.v_product_logistics IS
    'Fuente logística para App Choferes, carga de camiones y optimización (peso/volumen).';
-- +go
