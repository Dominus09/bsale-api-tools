-- Extensión: relación OC↔factura vía API relateddetailid, trazabilidad de sync, vistas v_orders / v_sales / v_sync_status.
-- No altera ``documents`` ni el flujo base de sync. Ejecutar después de 003_views (``ensure_distribuidora_schema``).

CREATE TABLE IF NOT EXISTS distribuidora.document_related (
    id SERIAL PRIMARY KEY,
    detail_id BIGINT NOT NULL,
    related_document_id BIGINT NOT NULL,
    related_document_type INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_distribuidora_document_related_detail_doc UNIQUE (detail_id, related_document_id)
);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_document_related_detail
    ON distribuidora.document_related (detail_id);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_document_related_related_doc
    ON distribuidora.document_related (related_document_id);
-- +go

DO $fk_dr$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_distribuidora_document_related_detail'
    ) THEN
        ALTER TABLE distribuidora.document_related
            ADD CONSTRAINT fk_distribuidora_document_related_detail
            FOREIGN KEY (detail_id)
            REFERENCES distribuidora.document_details (detail_id)
            ON DELETE CASCADE;
    END IF;
END
$fk_dr$;
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.sync_status (
    id SERIAL PRIMARY KEY,
    sync_type TEXT NOT NULL,
    last_run TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    records_processed INT NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_sync_status_type_time
    ON distribuidora.sync_status (sync_type, last_run DESC);
-- +go

DROP VIEW IF EXISTS distribuidora.v_sync_status CASCADE;
-- +go
DROP VIEW IF EXISTS distribuidora.v_sales CASCADE;
-- +go
DROP VIEW IF EXISTS distribuidora.v_orders CASCADE;
-- +go

-- Órdenes de compra (tipo 33): atributos de entrega/pago + facturación vía ``document_related``.
CREATE OR REPLACE VIEW distribuidora.v_orders AS
SELECT
    d.document_id,
    d.number,
    d.emission_date,
    d.client_id,
    d.municipality,
    d.total_amount,
    d.seller_name,
    attr.delivery_day,
    attr.payment_method,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM distribuidora.document_related dr
            INNER JOIN distribuidora.document_details dd
                ON dd.detail_id = dr.detail_id
            INNER JOIN distribuidora.v_documents_latest inv
                ON inv.document_id = dr.related_document_id
               AND inv.document_type_id IN (1, 6)
               AND inv.company_id = d.company_id
               AND inv.office_id = d.office_id
            WHERE dd.document_id = d.document_id
        ) THEN TRUE
        ELSE FALSE
    END AS is_invoiced
FROM distribuidora.v_documents_latest d
LEFT JOIN (
    SELECT
        da.document_id,
        COALESCE(
            MAX(da.attribute_value) FILTER (
                WHERE upper(btrim(da.attribute_name)) = upper(btrim('DÍA DE ENTREGA'))
            ),
            MAX(da.attribute_value) FILTER (
                WHERE upper(btrim(da.attribute_name)) = upper(btrim('DIA DE ENTREGA'))
            ),
            MAX(da.attribute_value) FILTER (
                WHERE upper(btrim(da.attribute_name)) = upper(btrim('FECHA DE ENTREGA'))
            )
        ) AS delivery_day,
        MAX(da.attribute_value) FILTER (
            WHERE upper(btrim(da.attribute_name)) = upper(btrim('FORMA DE PAGO'))
        ) AS payment_method
    FROM distribuidora.document_attributes da
    GROUP BY da.document_id
) attr ON attr.document_id = d.document_id
WHERE d.company_id = 3
  AND d.office_id = 1
  AND d.document_type_id = 33;
-- +go

-- Ventas: boleta / factura (tipos 1 y 6).
CREATE OR REPLACE VIEW distribuidora.v_sales AS
SELECT
    d.document_id,
    d.number,
    d.emission_date,
    d.client_id,
    d.municipality,
    d.total_amount,
    d.seller_name
FROM distribuidora.v_documents_latest d
WHERE d.company_id = 3
  AND d.office_id = 1
  AND d.document_type_id IN (1, 6);
-- +go

-- Una fila: últimas corridas por dominio (según ``sync_status.sync_type``).
CREATE OR REPLACE VIEW distribuidora.v_sync_status AS
SELECT
    MAX(ss.last_run) FILTER (WHERE ss.sync_type = 'sales') AS last_sales_sync,
    MAX(ss.last_run) FILTER (WHERE ss.sync_type = 'orders') AS last_orders_sync,
    MAX(ss.last_run) FILTER (WHERE ss.sync_type = 'related') AS last_related_sync,
    MAX(ss.last_run) FILTER (WHERE ss.sync_type = 'documents') AS last_documents_sync
FROM distribuidora.sync_status ss;
-- +go
