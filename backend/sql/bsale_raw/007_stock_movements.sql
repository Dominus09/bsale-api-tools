-- =============================================================================
-- bsale_raw 007 — recepciones y consumos de stock (+ detalles)
--
-- Sin full scan global: incremental por día (admissiondate / consumptiondate) con solape y
-- reconcile por ventana de 7 días. Disparan refresh de costos y stock de sus variantes.
-- Sin FK hijo → padre. PK de detalles = (company_id, <padre>_id, bsale_id): el hijo queda
-- ligado explícitamente a su recepción / consumo.
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS bsale_raw.stock_receptions (
    company_id BIGINT NOT NULL,
    bsale_id BIGINT NOT NULL,
    office_id BIGINT,
    admission_date DATE,
    document_number TEXT,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    api_fetched_at TIMESTAMPTZ NOT NULL,
    missing_since TIMESTAMPTZ,
    last_source TEXT NOT NULL,
    sync_run_id BIGINT,
    CONSTRAINT pk_raw_stock_receptions PRIMARY KEY (company_id, bsale_id),
    CONSTRAINT fk_raw_stock_receptions_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_raw_stock_receptions_last_source
        CHECK (last_source IN ('WEBHOOK', 'POINT', 'INCREMENTAL', 'FULL_RECONCILE', 'SCANNER', 'WINDOW_RECONCILE'))
);
CREATE INDEX IF NOT EXISTS ix_raw_stock_receptions_office_date
    ON bsale_raw.stock_receptions (company_id, office_id, admission_date);

CREATE TABLE IF NOT EXISTS bsale_raw.stock_reception_details (
    company_id BIGINT NOT NULL,
    bsale_id BIGINT NOT NULL,
    reception_id BIGINT NOT NULL,
    variant_id BIGINT,
    quantity NUMERIC,
    cost NUMERIC,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    api_fetched_at TIMESTAMPTZ NOT NULL,
    last_source TEXT NOT NULL,
    sync_run_id BIGINT,
    CONSTRAINT pk_raw_stock_reception_details PRIMARY KEY (company_id, reception_id, bsale_id),
    CONSTRAINT fk_raw_stock_reception_details_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_raw_stock_reception_details_last_source
        CHECK (last_source IN ('WEBHOOK', 'POINT', 'INCREMENTAL', 'FULL_RECONCILE', 'SCANNER', 'WINDOW_RECONCILE'))
);
CREATE INDEX IF NOT EXISTS ix_raw_stock_reception_details_bsale_id
    ON bsale_raw.stock_reception_details (company_id, bsale_id);
CREATE INDEX IF NOT EXISTS ix_raw_stock_reception_details_variant
    ON bsale_raw.stock_reception_details (company_id, variant_id);

CREATE TABLE IF NOT EXISTS bsale_raw.stock_consumptions (
    company_id BIGINT NOT NULL,
    bsale_id BIGINT NOT NULL,
    office_id BIGINT,
    consumption_date DATE,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    api_fetched_at TIMESTAMPTZ NOT NULL,
    missing_since TIMESTAMPTZ,
    last_source TEXT NOT NULL,
    sync_run_id BIGINT,
    CONSTRAINT pk_raw_stock_consumptions PRIMARY KEY (company_id, bsale_id),
    CONSTRAINT fk_raw_stock_consumptions_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_raw_stock_consumptions_last_source
        CHECK (last_source IN ('WEBHOOK', 'POINT', 'INCREMENTAL', 'FULL_RECONCILE', 'SCANNER', 'WINDOW_RECONCILE'))
);
CREATE INDEX IF NOT EXISTS ix_raw_stock_consumptions_office_date
    ON bsale_raw.stock_consumptions (company_id, office_id, consumption_date);

CREATE TABLE IF NOT EXISTS bsale_raw.stock_consumption_details (
    company_id BIGINT NOT NULL,
    bsale_id BIGINT NOT NULL,
    consumption_id BIGINT NOT NULL,
    variant_id BIGINT,
    quantity NUMERIC,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    api_fetched_at TIMESTAMPTZ NOT NULL,
    last_source TEXT NOT NULL,
    sync_run_id BIGINT,
    CONSTRAINT pk_raw_stock_consumption_details PRIMARY KEY (company_id, consumption_id, bsale_id),
    CONSTRAINT fk_raw_stock_consumption_details_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_raw_stock_consumption_details_last_source
        CHECK (last_source IN ('WEBHOOK', 'POINT', 'INCREMENTAL', 'FULL_RECONCILE', 'SCANNER', 'WINDOW_RECONCILE'))
);
CREATE INDEX IF NOT EXISTS ix_raw_stock_consumption_details_bsale_id
    ON bsale_raw.stock_consumption_details (company_id, bsale_id);
CREATE INDEX IF NOT EXISTS ix_raw_stock_consumption_details_variant
    ON bsale_raw.stock_consumption_details (company_id, variant_id);

COMMIT;
