-- Costos V2: tabla derivada versionada (propuesta — NO ejecutar automáticamente).
--
-- Propósito:
--   Separar valores almacenados en analytics.cost_reception_history
--   (mezcla de payload Bsale + sintéticos del sync V1) de un cálculo
--   tributario corregido y versionado.
--
-- Convención calculation_version: 'cost-v2.0.0' (motor puro Etapa C).
--
-- Vista latest: última ejecución por calculated_at DESC, id DESC.
--   "latest" ≠ mayor versión semver.
--
-- Importante:
--   - No modifica analytics.cost_reception_history.
--   - No modifica bsale.variant_cost.
--   - No incluye DML / backfill / triggers.
--   - history.id es la PK confirmada en 038_cost_analytics_receptions.sql.

CREATE SCHEMA IF NOT EXISTS analytics;

-- ---------------------------------------------------------------------------
-- 1) Tabla derivada versionada
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.cost_reception_calculated (
    id                              BIGSERIAL PRIMARY KEY,

    history_id                      BIGINT NOT NULL,
    calculation_version             TEXT NOT NULL DEFAULT 'cost-v2.0.0',
    calculation_batch_id            UUID NOT NULL,

    -- Ámbito denormalizado (office_id BIGINT alineado con history.office_id)
    company_id                      INTEGER,
    office_id                       BIGINT,
    variant_id                      BIGINT,
    admission_date                  DATE,

    -- Valores almacenados previamente en history (NO afirmar origen Bsale bruto)
    stored_cost_net                 NUMERIC(18, 4),
    stored_quantity                 NUMERIC(18, 4),
    stored_iva_amount               NUMERIC(18, 4),
    stored_other_taxes              NUMERIC(18, 4),
    stored_gross_cost               NUMERIC(18, 4),

    -- Contexto tributario
    reception_tax_ids_json          JSONB,
    catalog_tax_ids_json            JSONB,
    resolved_tax_ids_json           JSONB,
    iva_tax_id                      INTEGER,
    iva_rate                        NUMERIC(9, 4),
    calculated_iva_amount           NUMERIC(18, 4),
    additional_taxes_json           JSONB,
    additional_tax_rate_total       NUMERIC(9, 4),
    additional_tax_amount_total     NUMERIC(18, 4),
    total_tax_rate                  NUMERIC(9, 4),

    -- Resultado
    corrected_gross_cost            NUMERIC(18, 4),
    gross_difference_amount         NUMERIC(18, 4),
    tax_rate_on_net_pct             NUMERIC(9, 4),
    gross_understatement_vs_corrected_pct NUMERIC(9, 4),

    -- Resolución (NOT NULL: sin contexto seguro → 'unresolved')
    -- tax_context_source: LEGACY/DEPRECATED — preferir tax_ids_source + tax_rates_source.
    tax_context_source              TEXT NOT NULL DEFAULT 'unresolved',
    tax_ids_source                  TEXT NOT NULL DEFAULT 'unresolved',
    tax_rates_source                TEXT NOT NULL DEFAULT 'unresolved',
    tax_context_as_of               TIMESTAMPTZ,
    tax_context_is_historical       BOOLEAN,
    tax_context_fingerprint         TEXT,
    tax_resolution_quality          TEXT NOT NULL DEFAULT 'unresolved',

    -- Calidad (estado principal; outliers van en warnings_json)
    effective_quality_status        TEXT NOT NULL,
    warnings_json                   JSONB NOT NULL DEFAULT '[]'::jsonb,

    -- Trazabilidad
    source_history_created_at       TIMESTAMPTZ,
    source_history_fingerprint      TEXT,
    calculation_result_fingerprint  TEXT NOT NULL,
    calculated_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_cost_reception_calculated_history_version
        UNIQUE (history_id, calculation_version),

    CONSTRAINT fk_cost_reception_calculated_history
        FOREIGN KEY (history_id)
        REFERENCES analytics.cost_reception_history (id)
        ON DELETE RESTRICT
        ON UPDATE NO ACTION,

    CONSTRAINT ck_cost_reception_calculated_effective_quality
        CHECK (effective_quality_status IN (
            'missing_cost',
            'gross_component_mismatch',
            'duplicated_taxes_in_gross',
            'missing_taxes_in_gross',
            'incomplete_tax_context',
            'valid_gross'
        )),

    CONSTRAINT ck_cost_reception_calculated_tax_resolution_quality
        CHECK (tax_resolution_quality IN (
            'direct_reception',
            'historical_catalog',
            'current_catalog',
            'canonical_fallback',
            'unresolved'
        )),

    CONSTRAINT ck_cost_reception_calculated_tax_context_source
        CHECK (tax_context_source IN (
            'reception_payload',
            'historical_product_tax',
            'current_product_tax',
            'bsale_taxes',
            'canonical_fallback',
            'unresolved'
        )),

    CONSTRAINT ck_cost_reception_calculated_tax_ids_source
        CHECK (tax_ids_source IN (
            'reception_payload',
            'historical_product_tax',
            'current_product_tax',
            'unresolved'
        )),

    CONSTRAINT ck_cost_reception_calculated_tax_rates_source
        CHECK (tax_rates_source IN (
            'reception_payload',
            'bsale_taxes',
            'canonical_fallback',
            'unresolved'
        ))
);

-- ---------------------------------------------------------------------------
-- 2) Índices
--    UNIQUE (history_id, calculation_version) ya cubre búsquedas por history_id.
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_crc_company_office_admission
    ON analytics.cost_reception_calculated (company_id, office_id, admission_date DESC);

CREATE INDEX IF NOT EXISTS idx_crc_variant_admission
    ON analytics.cost_reception_calculated (variant_id, admission_date DESC);

CREATE INDEX IF NOT EXISTS idx_crc_effective_quality
    ON analytics.cost_reception_calculated (effective_quality_status);

CREATE INDEX IF NOT EXISTS idx_crc_calculation_version
    ON analytics.cost_reception_calculated (calculation_version);

CREATE INDEX IF NOT EXISTS idx_crc_calculation_batch_id
    ON analytics.cost_reception_calculated (calculation_batch_id);

-- ---------------------------------------------------------------------------
-- 3) Vista latest: última ejecución por history_id
--    "latest" = calculated_at más reciente (luego id), NO mayor semver.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.v_cost_reception_calculated_latest AS
SELECT
    c.id,
    c.history_id,
    c.calculation_version,
    c.calculation_batch_id,
    c.company_id,
    c.office_id,
    c.variant_id,
    c.admission_date,
    c.stored_cost_net,
    c.stored_quantity,
    c.stored_iva_amount,
    c.stored_other_taxes,
    c.stored_gross_cost,
    c.reception_tax_ids_json,
    c.catalog_tax_ids_json,
    c.resolved_tax_ids_json,
    c.iva_tax_id,
    c.iva_rate,
    c.calculated_iva_amount,
    c.additional_taxes_json,
    c.additional_tax_rate_total,
    c.additional_tax_amount_total,
    c.total_tax_rate,
    c.corrected_gross_cost,
    c.gross_difference_amount,
    c.tax_rate_on_net_pct,
    c.gross_understatement_vs_corrected_pct,
    c.tax_context_source,
    c.tax_ids_source,
    c.tax_rates_source,
    c.tax_context_as_of,
    c.tax_context_is_historical,
    c.tax_context_fingerprint,
    c.tax_resolution_quality,
    c.effective_quality_status,
    c.warnings_json,
    c.source_history_created_at,
    c.source_history_fingerprint,
    c.calculation_result_fingerprint,
    c.calculated_at,
    h.unique_key AS history_unique_key,
    h.reception_id,
    h.reception_detail_id,
    h.document,
    h.document_number,
    h.reception_type,
    h.barcode AS history_barcode,
    h.product_id AS history_product_id,
    h.product_name AS history_product_name,
    h.variant_name AS history_variant_name,
    h.created_at AS history_created_at
FROM (
    SELECT DISTINCT ON (history_id)
        *
    FROM analytics.cost_reception_calculated
    ORDER BY history_id, calculated_at DESC, id DESC
) c
INNER JOIN analytics.cost_reception_history h
    ON h.id = c.history_id;

-- ---------------------------------------------------------------------------
-- 4) Comentarios
-- ---------------------------------------------------------------------------
COMMENT ON TABLE analytics.cost_reception_calculated IS
    'Costos V2 derivado versionado. history conserva valores almacenados (payload + sintéticos V1); '
    'esta tabla guarda el cálculo tributario corregido por calculation_version sin sobrescribir raw.';

COMMENT ON COLUMN analytics.cost_reception_calculated.history_id IS
    'FK a analytics.cost_reception_history.id (PK BIGSERIAL confirmada en 038). ON DELETE RESTRICT.';

COMMENT ON COLUMN analytics.cost_reception_calculated.calculation_version IS
    'Convención: cost-v2.0.0. UNIQUE con history_id permite UPSERT idempotente y historial de versiones.';

COMMENT ON COLUMN analytics.cost_reception_calculated.calculation_batch_id IS
    'UUID de la corrida/backfill que produjo la fila; lo asigna el job, no el motor puro.';

COMMENT ON COLUMN analytics.cost_reception_calculated.stored_cost_net IS
    'Copia de history.cost_net (proveniente de line.cost Bsale vía sync). No afirmar unidad sin validación.';

COMMENT ON COLUMN analytics.cost_reception_calculated.stored_quantity IS
    'Copia de history.quantity. NO usar todavía para totales ponderados (semántica unidad/caja/kg no confirmada).';

COMMENT ON COLUMN analytics.cost_reception_calculated.stored_iva_amount IS
    'Copia de history.iva_amount (sintético del sync V1 vía split_erp_cost). No es impuesto de documento Bsale.';

COMMENT ON COLUMN analytics.cost_reception_calculated.stored_other_taxes IS
    'Copia de history.other_taxes (sintético del sync V1).';

COMMENT ON COLUMN analytics.cost_reception_calculated.stored_gross_cost IS
    'Copia de history.cost_bruto_erp. Puede ser sintético e incorrecto (p.ej. tax_factor=1 → bruto=neto). '
    'No interpretarlo como bruto original del payload Bsale.';

COMMENT ON COLUMN analytics.cost_reception_calculated.corrected_gross_cost IS
    'Bruto derivado: net + IVA calculado + impuestos adicionales (aditivos). '
    'NULL = cálculo no seguro (perfil unresolved / incomplete).';

COMMENT ON COLUMN analytics.cost_reception_calculated.tax_context_source IS
    'LEGACY/DEPRECATED. Preferir tax_ids_source + tax_rates_source. Conservado por compatibilidad. '
    'NOT NULL DEFAULT unresolved. current_product_tax NO implica vigencia histórica en admission_date.';

COMMENT ON COLUMN analytics.cost_reception_calculated.tax_ids_source IS
    'Origen de los tax_ids: reception_payload | historical_product_tax | current_product_tax | unresolved. '
    'Ortogonal a tax_rates_source (Etapa D: products.tax_ids_json → current_product_tax).';

COMMENT ON COLUMN analytics.cost_reception_calculated.tax_rates_source IS
    'Origen de las tasas: reception_payload | bsale_taxes | canonical_fallback | unresolved. '
    'Ortogonal a tax_ids_source (Etapa D: bsale.taxes → bsale_taxes).';

COMMENT ON COLUMN analytics.cost_reception_calculated.tax_context_fingerprint IS
    'SHA-256 del contexto tributario (ids ordenados + tasas + tax_ids_source + tax_rates_source + fuentes). '
    'Independiente del orden de tax_ids.';

COMMENT ON COLUMN analytics.cost_reception_calculated.calculation_result_fingerprint IS
    'SHA-256 del resultado calculado (montos, estado, warnings, fuentes, fingerprints de origen). '
    'Permite distinguir un rerun sin cambios de uno cuyo estado, warning o monto cambió. '
    'NULL ≠ cero; orden de warnings/taxes/ids no altera el hash.';

COMMENT ON COLUMN analytics.cost_reception_calculated.tax_resolution_quality IS
    'NOT NULL DEFAULT unresolved.';

COMMENT ON COLUMN analytics.cost_reception_calculated.tax_context_is_historical IS
    'TRUE solo si el contexto tributario corresponde a una fuente histórica de la admisión; '
    'FALSE cuando se usó catálogo vigente al momento del cálculo.';

COMMENT ON COLUMN analytics.cost_reception_calculated.effective_quality_status IS
    'Estado principal. Outliers y señales secundarias van en warnings_json, no como estado principal.';

COMMENT ON COLUMN analytics.cost_reception_calculated.warnings_json IS
    'Warnings secundarios (p.ej. suspicious_outlier, tax_ids_not_consumed). No reemplazan effective_quality_status.';

COMMENT ON VIEW analytics.v_cost_reception_calculated_latest IS
    'Última ejecución por history_id (calculated_at DESC, id DESC). '
    'latest = última corrida temporal, NO mayor calculation_version semver. '
    'Incluye identificadores desde cost_reception_history vía JOIN.';
