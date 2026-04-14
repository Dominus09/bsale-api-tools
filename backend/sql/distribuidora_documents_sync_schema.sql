-- DEPRECADO: el esquema vive en ``backend/sql/distribuidora/`` (001_schema, 002_indexes, 003_views).
-- El job aplica esos archivos vía ``sync_repo.ensure_distribuidora_schema``.
-- Se conserva este archivo solo como referencia histórica; no ejecutar en paralelo con el nuevo esquema.

-- Esquema distribuidora: documentos Bsale (empresa 3, office 1) + estado de sync incremental.
-- Ejecutar una vez en PostgreSQL (o dejar que el job intente crear si no existe).

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
    company_id         INTEGER NOT NULL DEFAULT 3,
    number             BIGINT,
    document_type_name TEXT,
    reference          TEXT,
    expiration_date    TIMESTAMPTZ,
    raw_data           JSONB,
    is_invoiced        BOOLEAN NOT NULL DEFAULT FALSE,
    attributes_data    JSONB,
    delivery_day       TEXT,
    document_to_generate TEXT,
    payment_method     TEXT,
    client_name        TEXT
);

ALTER TABLE distribuidora.documents
    ADD COLUMN IF NOT EXISTS is_invoiced BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE distribuidora.documents
    ADD COLUMN IF NOT EXISTS number BIGINT;

ALTER TABLE distribuidora.documents
    ADD COLUMN IF NOT EXISTS document_type_name TEXT;

ALTER TABLE distribuidora.documents
    ADD COLUMN IF NOT EXISTS reference TEXT;

ALTER TABLE distribuidora.documents
    ADD COLUMN IF NOT EXISTS expiration_date TIMESTAMPTZ;

ALTER TABLE distribuidora.documents
    ADD COLUMN IF NOT EXISTS raw_data JSONB;

ALTER TABLE distribuidora.documents
    ADD COLUMN IF NOT EXISTS attributes_data JSONB;

ALTER TABLE distribuidora.documents
    ADD COLUMN IF NOT EXISTS delivery_day TEXT;

ALTER TABLE distribuidora.documents
    ADD COLUMN IF NOT EXISTS document_to_generate TEXT;

ALTER TABLE distribuidora.documents
    ADD COLUMN IF NOT EXISTS payment_method TEXT;

ALTER TABLE distribuidora.documents
    ADD COLUMN IF NOT EXISTS client_name TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS uq_distribuidora_documents_logical
    ON distribuidora.documents (company_id, office_id, document_type_id, number)
    WHERE document_type_id IS NOT NULL AND number IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_distribuidora_documents_emission
    ON distribuidora.documents (emission_date);

CREATE INDEX IF NOT EXISTS idx_distribuidora_documents_company_office
    ON distribuidora.documents (company_id, office_id);

CREATE INDEX IF NOT EXISTS idx_distribuidora_documents_oc_invoiced
    ON distribuidora.documents (document_type_id, is_invoiced)
    WHERE document_type_id = 33;

CREATE INDEX IF NOT EXISTS idx_distribuidora_documents_oc_delivery_day
    ON distribuidora.documents (document_type_id, delivery_day)
    WHERE document_type_id = 33;

CREATE TABLE IF NOT EXISTS distribuidora.document_details (
    detail_id            BIGINT PRIMARY KEY,
    document_id          BIGINT NOT NULL,
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

-- Nota: el job puede insertar una fila centinela con detail_id = -document_id cuando Bsale
-- devuelve 0 líneas, para no reprocesar el mismo documento en bucle.
