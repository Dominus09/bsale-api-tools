-- Esquema y tablas de PRUEBA para descarga de documentos Bsale (La Quillotana SPA, office_id = 1).
-- NO modifica tablas productivas existentes.
-- Ejecutar manualmente una vez antes del script: psql ... -f backend/sql/documents_bc_test_schema.sql

CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE IF NOT EXISTS app.documents_bc_test (
    id                  SERIAL PRIMARY KEY,
    bsale_id            BIGINT NOT NULL UNIQUE,
    number              BIGINT,
    document_type_id    BIGINT,
    office_id           BIGINT NOT NULL,
    client_id           BIGINT,
    client_name         TEXT,
    client_rut          TEXT,
    emission_date       DATE,
    generation_date     TIMESTAMPTZ,
    total_amount        NUMERIC,
    net_amount          NUMERIC,
    tax_amount          NUMERIC,
    state               BIGINT,
    url_pdf             TEXT,
    raw_json            JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS app.document_details_bc_test (
    id                  SERIAL PRIMARY KEY,
    document_bsale_id   BIGINT NOT NULL,
    detail_bsale_id     BIGINT NOT NULL UNIQUE,
    variant_id          BIGINT,
    product_name        TEXT,
    variant_name        TEXT,
    quantity            NUMERIC,
    net_unit_value      NUMERIC,
    total_unit_value    NUMERIC,
    net_amount          NUMERIC,
    total_amount        NUMERIC,
    raw_json            JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_documents_bc_test_office_id
    ON app.documents_bc_test (office_id);

CREATE INDEX IF NOT EXISTS idx_documents_bc_test_emission_date
    ON app.documents_bc_test (emission_date);

CREATE INDEX IF NOT EXISTS idx_documents_bc_test_document_type_id
    ON app.documents_bc_test (document_type_id);

CREATE INDEX IF NOT EXISTS idx_document_details_bc_test_document_bsale_id
    ON app.document_details_bc_test (document_bsale_id);

CREATE INDEX IF NOT EXISTS idx_document_details_bc_test_variant_id
    ON app.document_details_bc_test (variant_id);

COMMENT ON SCHEMA app IS 'Área de pruebas; tablas *_bc_test no sustituyen bsale.* ni distribuidora.*';
COMMENT ON TABLE app.documents_bc_test IS 'Documentos Bsale descargados en prueba (office 1); raw_json auditoría.';
COMMENT ON TABLE app.document_details_bc_test IS 'Detalles por documento Bsale; upsert por detail_bsale_id.';
