-- Tabla de documentos Bsale (sync_documents.py).
-- Requiere UNIQUE (company_id, bsale_id) para ON CONFLICT del sync.

CREATE SCHEMA IF NOT EXISTS bsale;

CREATE TABLE IF NOT EXISTS bsale.documents (
    company_id         INTEGER NOT NULL,
    bsale_id           BIGINT NOT NULL,
    number             TEXT,
    emission_date      TIMESTAMPTZ,
    document_type_id   INTEGER,
    client_id          INTEGER,
    office_id          INTEGER,
    user_id            INTEGER,
    total_amount       NUMERIC(18, 4),
    state              INTEGER,
    url_pdf            TEXT,
    token              TEXT,
    CONSTRAINT documents_company_bsale_unique UNIQUE (company_id, bsale_id)
);

CREATE INDEX IF NOT EXISTS idx_documents_company_emission
    ON bsale.documents (company_id, emission_date);
