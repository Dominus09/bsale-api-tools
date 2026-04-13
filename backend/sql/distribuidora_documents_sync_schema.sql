-- Esquema distribuidora: documentos Bsale (empresa 3, office 1) + estado de sync incremental.
-- Ejecutar una vez en PostgreSQL (o dejar que el job intente crear si no existe).

CREATE SCHEMA IF NOT EXISTS distribuidora;

CREATE TABLE IF NOT EXISTS distribuidora.sync_state (
    id         SERIAL PRIMARY KEY,
    last_sync  TIMESTAMPTZ NOT NULL DEFAULT '2000-01-01 00:00:00+00'::timestamptz
);

INSERT INTO distribuidora.sync_state (last_sync)
SELECT '2000-01-01 00:00:00+00'::timestamptz
WHERE NOT EXISTS (SELECT 1 FROM distribuidora.sync_state);

CREATE TABLE IF NOT EXISTS distribuidora.documents (
    document_id        BIGINT PRIMARY KEY,
    emission_date      TIMESTAMPTZ,
    document_type_id   INTEGER,
    client_id          INTEGER,
    vendedor_id        INTEGER,
    total_amount       NUMERIC(18, 4),
    state              INTEGER,
    url_pdf            TEXT,
    token              TEXT,
    office_id          INTEGER NOT NULL,
    company_id         INTEGER NOT NULL DEFAULT 3
);

CREATE INDEX IF NOT EXISTS idx_distribuidora_documents_emission
    ON distribuidora.documents (emission_date);

CREATE INDEX IF NOT EXISTS idx_distribuidora_documents_company_office
    ON distribuidora.documents (company_id, office_id);

CREATE TABLE IF NOT EXISTS distribuidora.document_details (
    detail_id            BIGINT PRIMARY KEY,
    document_id          BIGINT NOT NULL,
    company_id           INTEGER NOT NULL DEFAULT 3,
    line_number          INTEGER,
    variant_id           BIGINT,
    quantity             NUMERIC(18, 4),
    net_unit_value       NUMERIC(18, 4),
    total_unit_value     NUMERIC(18, 4),
    net_amount           NUMERIC(18, 4),
    tax_amount           NUMERIC(18, 4),
    total_amount         NUMERIC(18, 4),
    net_discount         NUMERIC(18, 4),
    discount_percentage  NUMERIC(10, 4)
);

CREATE INDEX IF NOT EXISTS idx_distribuidora_details_document
    ON distribuidora.document_details (document_id);

CREATE INDEX IF NOT EXISTS idx_distribuidora_details_company
    ON distribuidora.document_details (company_id);

-- Nota: el job puede insertar una fila centinela con detail_id = -document_id cuando Bsale
-- devuelve 0 líneas, para no reprocesar el mismo documento en bucle.
