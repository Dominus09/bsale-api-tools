-- =============================================================================
-- bsale.product_master_variants — migración autoritativa (idempotente)
--
-- Identidad producto canónico ERP (products_master.id) ↔ variante Bsale por empresa.
-- La tabla se creó manualmente en producción; este archivo permite reconstruirla desde cero
-- y completar una instalación existente SIN recrearla, SIN DROP y SIN tocar filas.
--
-- Aplicar MANUALMENTE. Re-ejecutable.
-- Sin FK hacia bsale.variants (se audita aparte).
--
-- Constraints agregadas sobre una tabla ya existente usan NOT VALID: se exigen para escrituras
-- nuevas sin revalidar filas actuales. Validar manualmente cuando corresponda con
--   ALTER TABLE bsale.product_master_variants VALIDATE CONSTRAINT <nombre>;
-- =============================================================================

-- 1) Instalación nueva: esquema completo ----------------------------------------
CREATE TABLE IF NOT EXISTS bsale.product_master_variants (
    id BIGSERIAL PRIMARY KEY,
    product_master_id INTEGER NOT NULL,
    company_id BIGINT NOT NULL,
    product_id BIGINT,
    variant_id BIGINT,
    barcode TEXT NOT NULL,
    mapping_status TEXT NOT NULL,
    mapping_source TEXT NOT NULL,
    match_count INTEGER NOT NULL DEFAULT 0,
    candidate_variant_ids BIGINT[] NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_verified_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_product_master_variants_master
        FOREIGN KEY (product_master_id) REFERENCES bsale.products_master (id) ON DELETE CASCADE,
    CONSTRAINT fk_product_master_variants_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id),
    CONSTRAINT chk_product_master_variants_status
        CHECK (mapping_status IN ('AUTO_EXACT', 'MISSING', 'AMBIGUOUS', 'MANUAL')),
    CONSTRAINT chk_product_master_variants_source
        CHECK (mapping_source IN ('BARCODE', 'LEGACY_COMPANIES', 'MANUAL')),
    CONSTRAINT chk_product_master_variants_match_count
        CHECK (match_count >= 0),
    CONSTRAINT chk_product_master_variants_mapping
        CHECK (
            (mapping_status IN ('AUTO_EXACT', 'MANUAL') AND variant_id IS NOT NULL AND product_id IS NOT NULL)
            OR (mapping_status IN ('MISSING', 'AMBIGUOUS') AND variant_id IS NULL)
        )
);

-- 2) Instalación existente: columnas faltantes ----------------------------------
-- Columnas con DEFAULT: se agregan NOT NULL (el default cubre filas existentes).
-- Columnas obligatorias sin default posible (barcode, mapping_status, mapping_source):
-- se agregan NULLables para no fallar ni inventar valores; imponer NOT NULL manualmente
-- tras completar datos. En producción ya existen y estas sentencias no hacen nada.
ALTER TABLE bsale.product_master_variants
    ADD COLUMN IF NOT EXISTS product_id BIGINT,
    ADD COLUMN IF NOT EXISTS variant_id BIGINT,
    ADD COLUMN IF NOT EXISTS barcode TEXT,
    ADD COLUMN IF NOT EXISTS mapping_status TEXT,
    ADD COLUMN IF NOT EXISTS mapping_source TEXT,
    ADD COLUMN IF NOT EXISTS match_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS candidate_variant_ids BIGINT[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS last_verified_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

-- 3) Constraints faltantes (detección por definición, no por nombre: en producción
--    los nombres los asignó PostgreSQL al crear la tabla manualmente) ---------------
DO $pmv_constraints$
DECLARE
    rel oid := 'bsale.product_master_variants'::regclass;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint c
        WHERE c.conrelid = rel AND c.contype = 'f'
          AND c.confrelid = 'bsale.products_master'::regclass
    ) THEN
        ALTER TABLE bsale.product_master_variants
            ADD CONSTRAINT fk_product_master_variants_master
            FOREIGN KEY (product_master_id) REFERENCES bsale.products_master (id)
            ON DELETE CASCADE NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint c
        WHERE c.conrelid = rel AND c.contype = 'f'
          AND c.confrelid = 'bsale.companies'::regclass
    ) THEN
        ALTER TABLE bsale.product_master_variants
            ADD CONSTRAINT fk_product_master_variants_company
            FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint c
        WHERE c.conrelid = rel AND c.contype = 'c'
          AND pg_get_constraintdef(c.oid) ILIKE '%mapping_status%'
          AND pg_get_constraintdef(c.oid) ILIKE '%AUTO_EXACT%'
          AND pg_get_constraintdef(c.oid) NOT ILIKE '%variant_id%'
    ) THEN
        ALTER TABLE bsale.product_master_variants
            ADD CONSTRAINT chk_product_master_variants_status
            CHECK (mapping_status IN ('AUTO_EXACT', 'MISSING', 'AMBIGUOUS', 'MANUAL')) NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint c
        WHERE c.conrelid = rel AND c.contype = 'c'
          AND pg_get_constraintdef(c.oid) ILIKE '%mapping_source%'
    ) THEN
        ALTER TABLE bsale.product_master_variants
            ADD CONSTRAINT chk_product_master_variants_source
            CHECK (mapping_source IN ('BARCODE', 'LEGACY_COMPANIES', 'MANUAL')) NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint c
        WHERE c.conrelid = rel AND c.contype = 'c'
          AND pg_get_constraintdef(c.oid) ILIKE '%match_count%'
    ) THEN
        ALTER TABLE bsale.product_master_variants
            ADD CONSTRAINT chk_product_master_variants_match_count
            CHECK (match_count >= 0) NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint c
        WHERE c.conrelid = rel AND c.contype = 'c'
          AND pg_get_constraintdef(c.oid) ILIKE '%mapping_status%'
          AND pg_get_constraintdef(c.oid) ILIKE '%variant_id%'
    ) THEN
        ALTER TABLE bsale.product_master_variants
            ADD CONSTRAINT chk_product_master_variants_mapping
            CHECK (
                (mapping_status IN ('AUTO_EXACT', 'MANUAL') AND variant_id IS NOT NULL AND product_id IS NOT NULL)
                OR (mapping_status IN ('MISSING', 'AMBIGUOUS') AND variant_id IS NULL)
            ) NOT VALID;
    END IF;
END
$pmv_constraints$;

-- 4) UNIQUE e índices (nombres reales de producción) ----------------------------
-- UNIQUE (product_master_id, company_id): árbitro del ON CONFLICT del sync.
DO $pmv_unique$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_index x
        WHERE x.indrelid = 'bsale.product_master_variants'::regclass
          AND x.indisunique
          AND x.indpred IS NULL
          AND (
              SELECT array_agg(a.attname::text ORDER BY a.attname)
              FROM unnest(x.indkey::int2[]) k
              JOIN pg_attribute a ON a.attrelid = x.indrelid AND a.attnum = k
          ) = ARRAY['company_id', 'product_master_id']
    ) THEN
        CREATE UNIQUE INDEX product_master_variants_master_company_uq
            ON bsale.product_master_variants (product_master_id, company_id);
    END IF;
END
$pmv_unique$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_product_master_variants_company_variant
    ON bsale.product_master_variants (company_id, variant_id)
    WHERE variant_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_product_master_variants_barcode
    ON bsale.product_master_variants (barcode);

CREATE INDEX IF NOT EXISTS idx_product_master_variants_company
    ON bsale.product_master_variants (company_id);

CREATE INDEX IF NOT EXISTS idx_product_master_variants_master
    ON bsale.product_master_variants (product_master_id);

CREATE INDEX IF NOT EXISTS idx_product_master_variants_status
    ON bsale.product_master_variants (mapping_status);
