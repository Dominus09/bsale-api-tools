-- Tabla de detalles de documentos Bsale (sync_document_details.py).
-- Diseñada para inserción incremental con:
-- ON CONFLICT (company_id, bsale_detail_id) DO NOTHING

CREATE SCHEMA IF NOT EXISTS bsale;

CREATE TABLE IF NOT EXISTS bsale.document_details (
    company_id           INTEGER NOT NULL,
    bsale_detail_id      BIGINT NOT NULL,
    document_id          BIGINT NOT NULL, -- corresponde a documents.bsale_id
    line_number          INTEGER,
    variant_id           BIGINT,
    quantity             NUMERIC(18, 4),
    net_unit_value       NUMERIC(18, 4),
    total_unit_value     NUMERIC(18, 4),
    net_amount           NUMERIC(18, 4),
    tax_amount           NUMERIC(18, 4),
    total_amount         NUMERIC(18, 4),
    net_discount         NUMERIC(18, 4),
    discount_percentage  NUMERIC(10, 4),
    CONSTRAINT document_details_company_bsale_detail_unique UNIQUE (company_id, bsale_detail_id)
);

-- Consultas típicas por empresa y documento.
CREATE INDEX IF NOT EXISTS idx_document_details_company_document
    ON bsale.document_details (company_id, document_id);

-- Ayuda para búsquedas analíticas por variante.
CREATE INDEX IF NOT EXISTS idx_document_details_company_variant
    ON bsale.document_details (company_id, variant_id);
