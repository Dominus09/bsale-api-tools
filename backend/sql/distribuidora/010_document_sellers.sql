-- Vendedores por documento (Bsale ``sellers``); una o más filas por ``document_id``.
-- Ejecutar antes de ``011_v_sales_document_sellers.sql``.

CREATE TABLE IF NOT EXISTS distribuidora.document_sellers (
    id BIGSERIAL PRIMARY KEY,
    document_id BIGINT NOT NULL REFERENCES distribuidora.documents (document_id) ON DELETE CASCADE,
    seller_id INTEGER,
    seller_name TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_document_sellers_document_id
    ON distribuidora.document_sellers (document_id);
-- +go

COMMENT ON TABLE distribuidora.document_sellers IS
    'Vendedores asociados a un documento Bsale (sync desde ``sellers`` en JSON o sellers.json).';
-- +go

COMMENT ON COLUMN distribuidora.document_sellers.document_id IS
    'FK a ``distribuidora.documents.document_id``; al borrar documento se eliminan filas hijas.';
-- +go
