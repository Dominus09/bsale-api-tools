-- =============================================================================
-- bsale_raw 003 — configuración (offices, taxes, document_types, product_types, price_lists)
--
-- Columnas comunes de entidad RAW:
--   PK (company_id, bsale_id); payload JSONB completo + payload_hash;
--   api_fetched_at = instante de la respuesta HTTP que originó la fila.
-- Regla de frescura (para los UPSERT futuros, ver docs/BSALE_RAW_PHASE3_SQL_PROPOSAL.md §5):
--   ON CONFLICT ... DO UPDATE ... WHERE target.api_fetched_at <= EXCLUDED.api_fetched_at
-- state: valor Bsale tal cual (0 activo / 1 inactivo según docs), sin CHECK.
-- last_source: valores de SyncMode (core/models.py).
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS bsale_raw.offices (
    company_id BIGINT NOT NULL,
    bsale_id BIGINT NOT NULL,
    state SMALLINT,
    name TEXT,
    is_virtual SMALLINT,
    cost_center TEXT,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    api_fetched_at TIMESTAMPTZ NOT NULL,
    missing_since TIMESTAMPTZ,
    last_source TEXT NOT NULL,
    sync_run_id BIGINT,
    CONSTRAINT pk_raw_offices PRIMARY KEY (company_id, bsale_id),
    CONSTRAINT fk_raw_offices_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_raw_offices_last_source
        CHECK (last_source IN ('WEBHOOK', 'POINT', 'INCREMENTAL', 'FULL_RECONCILE', 'SCANNER', 'WINDOW_RECONCILE'))
);

CREATE TABLE IF NOT EXISTS bsale_raw.taxes (
    company_id BIGINT NOT NULL,
    bsale_id BIGINT NOT NULL,
    state SMALLINT,
    name TEXT,
    code TEXT,
    percentage NUMERIC,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    api_fetched_at TIMESTAMPTZ NOT NULL,
    missing_since TIMESTAMPTZ,
    last_source TEXT NOT NULL,
    sync_run_id BIGINT,
    CONSTRAINT pk_raw_taxes PRIMARY KEY (company_id, bsale_id),
    CONSTRAINT fk_raw_taxes_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_raw_taxes_last_source
        CHECK (last_source IN ('WEBHOOK', 'POINT', 'INCREMENTAL', 'FULL_RECONCILE', 'SCANNER', 'WINDOW_RECONCILE'))
);

CREATE TABLE IF NOT EXISTS bsale_raw.document_types (
    company_id BIGINT NOT NULL,
    bsale_id BIGINT NOT NULL,
    state SMALLINT,
    name TEXT,
    code_sii TEXT,
    is_electronic SMALLINT,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    api_fetched_at TIMESTAMPTZ NOT NULL,
    missing_since TIMESTAMPTZ,
    last_source TEXT NOT NULL,
    sync_run_id BIGINT,
    CONSTRAINT pk_raw_document_types PRIMARY KEY (company_id, bsale_id),
    CONSTRAINT fk_raw_document_types_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_raw_document_types_last_source
        CHECK (last_source IN ('WEBHOOK', 'POINT', 'INCREMENTAL', 'FULL_RECONCILE', 'SCANNER', 'WINDOW_RECONCILE'))
);
CREATE INDEX IF NOT EXISTS ix_raw_document_types_code_sii
    ON bsale_raw.document_types (company_id, code_sii);

CREATE TABLE IF NOT EXISTS bsale_raw.product_types (
    company_id BIGINT NOT NULL,
    bsale_id BIGINT NOT NULL,
    state SMALLINT,
    name TEXT,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    api_fetched_at TIMESTAMPTZ NOT NULL,
    missing_since TIMESTAMPTZ,
    last_source TEXT NOT NULL,
    sync_run_id BIGINT,
    CONSTRAINT pk_raw_product_types PRIMARY KEY (company_id, bsale_id),
    CONSTRAINT fk_raw_product_types_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_raw_product_types_last_source
        CHECK (last_source IN ('WEBHOOK', 'POINT', 'INCREMENTAL', 'FULL_RECONCILE', 'SCANNER', 'WINDOW_RECONCILE'))
);

CREATE TABLE IF NOT EXISTS bsale_raw.price_lists (
    company_id BIGINT NOT NULL,
    bsale_id BIGINT NOT NULL,
    state SMALLINT,
    name TEXT,
    coin_id BIGINT,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    api_fetched_at TIMESTAMPTZ NOT NULL,
    missing_since TIMESTAMPTZ,
    last_source TEXT NOT NULL,
    sync_run_id BIGINT,
    CONSTRAINT pk_raw_price_lists PRIMARY KEY (company_id, bsale_id),
    CONSTRAINT fk_raw_price_lists_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_raw_price_lists_last_source
        CHECK (last_source IN ('WEBHOOK', 'POINT', 'INCREMENTAL', 'FULL_RECONCILE', 'SCANNER', 'WINDOW_RECONCILE'))
);

COMMIT;
