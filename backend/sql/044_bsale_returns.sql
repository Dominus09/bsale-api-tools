-- Devoluciones Bsale (GET /v1/returns.json) — Análisis NC independiente del CRM Comercial.
-- Company 3 / Office 1. Ejecutar una vez en PG.

CREATE SCHEMA IF NOT EXISTS bsale;
-- +go

CREATE TABLE IF NOT EXISTS bsale.returns_sync_state (
    company_id     INTEGER NOT NULL,
    office_id      INTEGER NOT NULL DEFAULT 1,
    last_return_ts BIGINT,
    last_sync_at   TIMESTAMPTZ,
    records_total  BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (company_id, office_id)
);
-- +go

CREATE TABLE IF NOT EXISTS bsale.returns (
    company_id              INTEGER NOT NULL,
    office_id               INTEGER NOT NULL DEFAULT 1,
    bsale_id                BIGINT NOT NULL,
    code                    TEXT,
    return_date             TIMESTAMPTZ,
    motive                  TEXT,
    return_type             INTEGER,
    amount                  NUMERIC(18, 4) NOT NULL DEFAULT 0,
    price_adjustment        NUMERIC(18, 4) NOT NULL DEFAULT 0,
    edit_texts              INTEGER NOT NULL DEFAULT 0,
    reference_document_id   BIGINT,
    reference_document_number BIGINT,
    reference_document_type_id INTEGER,
    credit_note_id          BIGINT,
    credit_note_number      BIGINT,
    client_id               BIGINT,
    client_name             TEXT,
    seller_id               INTEGER,
    seller_name             TEXT,
    municipality            TEXT,
    credit_note_emission    TIMESTAMPTZ,
    reference_emission      TIMESTAMPTZ,
    raw_data                JSONB NOT NULL DEFAULT '{}'::jsonb,
    synced_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT returns_company_bsale_unique UNIQUE (company_id, bsale_id)
);
-- +go

CREATE INDEX IF NOT EXISTS idx_bsale_returns_company_office_date
    ON bsale.returns (company_id, office_id, return_date DESC);
-- +go

CREATE INDEX IF NOT EXISTS idx_bsale_returns_motive
    ON bsale.returns (company_id, motive);
-- +go

CREATE INDEX IF NOT EXISTS idx_bsale_returns_seller
    ON bsale.returns (company_id, seller_id);
-- +go

CREATE INDEX IF NOT EXISTS idx_bsale_returns_client
    ON bsale.returns (company_id, client_id);
-- +go

CREATE TABLE IF NOT EXISTS bsale.return_details (
    company_id           INTEGER NOT NULL,
    return_id            BIGINT NOT NULL,
    bsale_detail_id      BIGINT NOT NULL,
    document_detail_id   BIGINT,
    variant_id           BIGINT,
    product_name         TEXT,
    variant_description  TEXT,
    quantity             NUMERIC(18, 4) NOT NULL DEFAULT 0,
    unit_value           NUMERIC(18, 4) NOT NULL DEFAULT 0,
    total_amount         NUMERIC(18, 4) NOT NULL DEFAULT 0,
    raw_data             JSONB NOT NULL DEFAULT '{}'::jsonb,
    synced_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (company_id, return_id, bsale_detail_id)
);
-- +go

CREATE INDEX IF NOT EXISTS idx_bsale_return_details_variant
    ON bsale.return_details (company_id, variant_id);
-- +go

CREATE TABLE IF NOT EXISTS bsale.returns_sync (
    id                  SERIAL PRIMARY KEY,
    company_id          INTEGER NOT NULL,
    office_id           INTEGER NOT NULL DEFAULT 1,
    sync_type           TEXT NOT NULL CHECK (sync_type IN ('history', 'incremental')),
    date_from           DATE,
    date_to             DATE,
    last_return_date    TIMESTAMPTZ,
    last_return_id      BIGINT,
    pages_processed     INTEGER NOT NULL DEFAULT 0,
    records_processed   INTEGER NOT NULL DEFAULT 0,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at         TIMESTAMPTZ,
    duration_ms         BIGINT,
    status              TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'completed', 'failed', 'no_data')),
    error_message       TEXT
);
-- +go

CREATE INDEX IF NOT EXISTS idx_bsale_returns_sync_lookup
    ON bsale.returns_sync (company_id, office_id, sync_type, status, started_at DESC);
-- +go

-- Constraints e índices idempotentes: backend/repositories/returns_analytics_repo.ensure_returns_schema()
