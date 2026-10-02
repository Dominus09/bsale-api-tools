-- =============================================================================
-- bsale_raw 004 — catálogo (products, variants, clients)
--
-- Full scan = 1 barrido SIN `state` (observado: devuelve activos + inactivos); se guarda el
-- state de cada ítem. SKU / barcode / RUT son columnas de búsqueda, nunca únicas.
-- clients.payload contiene PII (nombres, email, dirección, teléfono, RUT): acceso restringido.
-- Índices (company_id, api_fetched_at): marcar missing_since en el full reconcile sólo sobre
-- filas con api_fetched_at <= snapshot_started_at.
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS bsale_raw.products (
    company_id BIGINT NOT NULL,
    bsale_id BIGINT NOT NULL,
    state SMALLINT,
    name TEXT,
    product_type_id BIGINT,
    classification SMALLINT,
    stock_control SMALLINT,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    api_fetched_at TIMESTAMPTZ NOT NULL,
    missing_since TIMESTAMPTZ,
    last_source TEXT NOT NULL,
    sync_run_id BIGINT,
    CONSTRAINT pk_raw_products PRIMARY KEY (company_id, bsale_id),
    CONSTRAINT fk_raw_products_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_raw_products_last_source
        CHECK (last_source IN ('WEBHOOK', 'POINT', 'INCREMENTAL', 'FULL_RECONCILE', 'SCANNER', 'WINDOW_RECONCILE'))
);
CREATE INDEX IF NOT EXISTS ix_raw_products_type
    ON bsale_raw.products (company_id, product_type_id);
CREATE INDEX IF NOT EXISTS ix_raw_products_fetched
    ON bsale_raw.products (company_id, api_fetched_at);

CREATE TABLE IF NOT EXISTS bsale_raw.variants (
    company_id BIGINT NOT NULL,
    bsale_id BIGINT NOT NULL,
    state SMALLINT,
    product_id BIGINT,
    code TEXT,
    bar_code TEXT,
    description TEXT,
    unlimited_stock SMALLINT,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    api_fetched_at TIMESTAMPTZ NOT NULL,
    missing_since TIMESTAMPTZ,
    last_source TEXT NOT NULL,
    sync_run_id BIGINT,
    CONSTRAINT pk_raw_variants PRIMARY KEY (company_id, bsale_id),
    CONSTRAINT fk_raw_variants_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_raw_variants_last_source
        CHECK (last_source IN ('WEBHOOK', 'POINT', 'INCREMENTAL', 'FULL_RECONCILE', 'SCANNER', 'WINDOW_RECONCILE'))
);
CREATE INDEX IF NOT EXISTS ix_raw_variants_product
    ON bsale_raw.variants (company_id, product_id);
CREATE INDEX IF NOT EXISTS ix_raw_variants_code
    ON bsale_raw.variants (company_id, code);
CREATE INDEX IF NOT EXISTS ix_raw_variants_bar_code
    ON bsale_raw.variants (company_id, bar_code);
CREATE INDEX IF NOT EXISTS ix_raw_variants_fetched
    ON bsale_raw.variants (company_id, api_fetched_at);

CREATE TABLE IF NOT EXISTS bsale_raw.clients (
    company_id BIGINT NOT NULL,
    bsale_id BIGINT NOT NULL,
    state SMALLINT,
    code TEXT,
    first_name TEXT,
    last_name TEXT,
    company_name TEXT,
    email TEXT,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    api_fetched_at TIMESTAMPTZ NOT NULL,
    missing_since TIMESTAMPTZ,
    last_source TEXT NOT NULL,
    sync_run_id BIGINT,
    CONSTRAINT pk_raw_clients PRIMARY KEY (company_id, bsale_id),
    CONSTRAINT fk_raw_clients_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_raw_clients_last_source
        CHECK (last_source IN ('WEBHOOK', 'POINT', 'INCREMENTAL', 'FULL_RECONCILE', 'SCANNER', 'WINDOW_RECONCILE'))
);
CREATE INDEX IF NOT EXISTS ix_raw_clients_code
    ON bsale_raw.clients (company_id, code);
CREATE INDEX IF NOT EXISTS ix_raw_clients_fetched
    ON bsale_raw.clients (company_id, api_fetched_at);

COMMENT ON COLUMN bsale_raw.clients.payload IS
    'Respuesta Bsale completa, contiene PII. No exponer en endpoints genéricos ni imprimir en logs.';

COMMIT;
