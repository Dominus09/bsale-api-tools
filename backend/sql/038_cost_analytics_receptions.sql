-- Analítica → Costos: historial oficial desde recepciones Bsale (append-only).

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.cost_sync_state (
    company_id INTEGER PRIMARY KEY,
    last_admission_ts BIGINT,
    last_run_at TIMESTAMPTZ,
    last_status TEXT,
    last_message TEXT,
    receptions_inserted INTEGER NOT NULL DEFAULT 0,
    lines_inserted INTEGER NOT NULL DEFAULT 0,
    total_lines_processed BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS analytics.cost_reception_history (
    id BIGSERIAL PRIMARY KEY,
    unique_key TEXT NOT NULL,
    company_id INTEGER NOT NULL,
    company_name TEXT,
    office_id BIGINT,
    office_name TEXT,
    variant_id BIGINT NOT NULL,
    product_id BIGINT,
    barcode TEXT,
    product_name TEXT,
    variant_name TEXT,
    reception_id BIGINT NOT NULL,
    reception_detail_id BIGINT NOT NULL,
    document TEXT,
    document_number BIGINT,
    admission_date TIMESTAMPTZ NOT NULL,
    quantity NUMERIC(18, 4) NOT NULL DEFAULT 0,
    cost_net NUMERIC(18, 4) NOT NULL DEFAULT 0,
    iva_amount NUMERIC(18, 4),
    other_taxes NUMERIC(18, 4),
    cost_bruto_erp NUMERIC(18, 4),
    average_cost NUMERIC(18, 4),
    variation_pct NUMERIC(10, 2),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_cost_reception_history_unique_key UNIQUE (unique_key),
    CONSTRAINT uq_cost_reception_history_company_detail UNIQUE (company_id, reception_detail_id)
);

CREATE INDEX IF NOT EXISTS idx_cost_reception_history_company_admission
    ON analytics.cost_reception_history (company_id, admission_date DESC);

CREATE INDEX IF NOT EXISTS idx_cost_reception_history_company_office_admission
    ON analytics.cost_reception_history (company_id, office_id, admission_date DESC);

CREATE INDEX IF NOT EXISTS idx_cost_reception_history_variant_date
    ON analytics.cost_reception_history (company_id, variant_id, admission_date DESC);

CREATE INDEX IF NOT EXISTS idx_cost_reception_history_reception
    ON analytics.cost_reception_history (company_id, reception_id);

CREATE INDEX IF NOT EXISTS idx_cost_reception_history_search
    ON analytics.cost_reception_history (company_id, barcode, product_name, variant_name);

-- Preparación márgenes / ERP (sin alterar consumidores actuales de variant_cost).
ALTER TABLE bsale.variant_cost
    ADD COLUMN IF NOT EXISTS average_cost_gross NUMERIC(18, 4),
    ADD COLUMN IF NOT EXISTS tax_factor NUMERIC(12, 6),
    ADD COLUMN IF NOT EXISTS iva_rate NUMERIC(8, 4),
    ADD COLUMN IF NOT EXISTS specific_taxes JSONB,
    ADD COLUMN IF NOT EXISTS cost_source TEXT;

COMMENT ON TABLE analytics.cost_reception_history IS
    'Historial de costos por recepción Bsale: empresa → sucursal → recepción → variante.';
COMMENT ON COLUMN analytics.cost_reception_history.unique_key IS
    'Clave idempotente: {company_id}_{reception_detail_id}';
