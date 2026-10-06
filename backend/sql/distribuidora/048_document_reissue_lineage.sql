-- Reemisiones Bsale: líneas no destructivas + relaciones resueltas al documento local estable.
--
-- Corre después de 001 (document_details, document_related), 003 (v_documents_latest,
-- v_orders_purchase_status), 007 (v_orders) y 026 (v_dispatch_plan_invoiced_documents).
-- Idempotente: el runner lo reaplica en cada ejecución. No borra datos.
--
-- 1. ``document_detail_history``: líneas que dejaron de ser vigentes.
-- 2. Sin FK ``document_related.detail_id`` → ``document_details`` (su CASCADE borraba relaciones).
-- 3. Índice lógico por folio solo para ``number > 0``.
-- 4. ``v_document_detail_lineage`` / ``v_document_related_resolved``.
-- 5. Vistas comerciales recreadas sobre la relación resuelta. 003/007/026 vuelven a crear
--    la versión previa en cada corrida; las de este archivo son las definitivas y deben
--    conservar exactamente las mismas columnas (test_schema_bootstrap lo verifica).

CREATE TABLE IF NOT EXISTS distribuidora.document_detail_history (
    detail_id BIGINT PRIMARY KEY,
    document_id BIGINT NOT NULL,
    line_number INT,
    variant_id BIGINT,
    variant_description TEXT,
    variant_code TEXT,
    quantity NUMERIC(18, 4),
    net_unit_value NUMERIC(18, 4),
    total_unit_value NUMERIC(18, 4),
    net_amount NUMERIC(18, 4),
    tax_amount NUMERIC(18, 4),
    total_amount NUMERIC(18, 4),
    net_discount NUMERIC(18, 4),
    total_discount NUMERIC(18, 4),
    discount_percentage NUMERIC(10, 4),
    related_detail_id BIGINT,
    note TEXT,
    raw_data JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    superseded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    superseded_by_source_document_id BIGINT,
    CONSTRAINT fk_distribuidora_document_detail_history_document
        FOREIGN KEY (document_id)
        REFERENCES distribuidora.documents (document_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_detail_history_document
    ON distribuidora.document_detail_history (document_id);
-- +go

COMMENT ON TABLE distribuidora.document_detail_history IS
    'Líneas Bsale que dejaron de ser vigentes (reemisión/edición). document_details = solo revisión vigente.';
-- +go
COMMENT ON COLUMN distribuidora.document_detail_history.superseded_by_source_document_id IS
    'Revisión Bsale cuyo replace archivó la línea (NULL si el caller no la informó).';
-- +go

ALTER TABLE distribuidora.document_related
    DROP CONSTRAINT IF EXISTS fk_distribuidora_document_related_detail;
-- +go

-- ``number <= 0`` (revisión técnica reemplazada) no es folio comercial. Reemplaza la
-- versión ``WHERE number IS NOT NULL`` de 002 una sola vez; luego es no-op.
DO $uq_folio$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = 'distribuidora'
          AND indexname = 'uq_distribuidora_documents_logical'
          AND indexdef LIKE '%number > 0%'
    ) THEN
        DROP INDEX IF EXISTS distribuidora.uq_distribuidora_documents_logical;
        CREATE UNIQUE INDEX uq_distribuidora_documents_logical
            ON distribuidora.documents (company_id, office_id, document_type_id, number)
            WHERE document_type_id IS NOT NULL AND number > 0;
    END IF;
END
$uq_folio$;
-- +go

-- Identidad de líneas (vigentes + históricas) para resolver relaciones.
-- Solo expone ``detail_id → document_id``: no tiene cantidades ni montos, así que
-- una línea histórica no puede colarse en peso/picking/totales (usar ``document_details``).
-- Un ``detail_id`` vigente nunca aparece también como histórico.
CREATE OR REPLACE VIEW distribuidora.v_document_detail_lineage AS
SELECT
    dd.detail_id,
    dd.document_id,
    TRUE AS detail_is_current
FROM distribuidora.document_details dd
UNION ALL
SELECT
    h.detail_id,
    h.document_id,
    FALSE AS detail_is_current
FROM distribuidora.document_detail_history h
WHERE NOT EXISTS (
    SELECT 1
    FROM distribuidora.document_details cur
    WHERE cur.detail_id = h.detail_id
);
-- +go

-- Relación → documento de origen local estable, con línea vigente o histórica.
-- Fuente canónica para OC→factura/boleta/guía/NC; no usar para cantidades.
-- ``origin_document_id`` NULL = detalle sin línea conocida (huérfano).
CREATE OR REPLACE VIEW distribuidora.v_document_related_resolved AS
SELECT
    dr.id,
    dr.detail_id,
    dr.related_document_id,
    dr.related_document_type,
    dr.created_at,
    l.document_id AS origin_document_id,
    COALESCE(l.detail_is_current, FALSE) AS detail_is_current,
    (l.detail_is_current IS FALSE) AS detail_is_historical
FROM distribuidora.document_related dr
LEFT JOIN distribuidora.v_document_detail_lineage l ON l.detail_id = dr.detail_id;
-- +go

-- Mismas columnas que 003; solo cambia el origen de la relación.
CREATE OR REPLACE VIEW distribuidora.v_orders_purchase_status AS
SELECT
    oc.document_id,
    oc.number,
    (inv.document_id IS NOT NULL) AS is_invoiced,
    inv.document_id AS invoicing_document_id,
    inv.document_type_id AS invoicing_document_type_id,
    inv.number AS invoicing_number,
    inv.emission_date AS invoicing_emission_date
FROM distribuidora.v_documents_latest oc
LEFT JOIN LATERAL (
    SELECT d.document_id, d.document_type_id, d.number, d.emission_date
    FROM distribuidora.v_document_related_resolved dr
    INNER JOIN distribuidora.v_documents_latest d
        ON d.document_id = dr.related_document_id
       AND d.document_type_id IN (1, 6)
       AND d.company_id = oc.company_id
       AND d.office_id = oc.office_id
    WHERE dr.origin_document_id = oc.document_id
    ORDER BY d.emission_date DESC NULLS LAST, d.document_id DESC
    LIMIT 1
) inv ON TRUE
WHERE oc.company_id = 3
  AND oc.office_id = 1
  AND oc.document_type_id = 33;
-- +go

-- Mismas columnas que 007; solo cambia el origen de la relación.
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
            FROM distribuidora.v_document_related_resolved dr
            INNER JOIN distribuidora.v_documents_latest inv
                ON inv.document_id = dr.related_document_id
               AND inv.document_type_id IN (1, 6)
               AND inv.company_id = d.company_id
               AND inv.office_id = d.office_id
            WHERE dr.origin_document_id = d.document_id
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

-- Mismas columnas que 026; solo cambia el origen de la relación.
CREATE OR REPLACE VIEW distribuidora.v_dispatch_plan_invoiced_documents AS
SELECT
    dpo.dispatch_plan_id,
    dpo.oc_document_id,
    dpo.oc_number,
    dpo.route_order,
    (
        COALESCE(st.is_invoiced, FALSE)
        OR (ps.score IS NOT NULL AND ps.score >= 75)
    ) AS is_invoiced_confirmed,
    COALESCE(
        st.invoicing_document_id,
        CASE WHEN ps.score >= 75 THEN ps.candidate_document_id END
    ) AS related_document_id,
    COALESCE(
        st.invoicing_number,
        CASE WHEN ps.score >= 75 THEN ps.candidate_number END
    ) AS related_document_number,
    COALESCE(
        st.invoicing_document_type_id,
        CASE WHEN ps.score >= 75 THEN ps.candidate_document_type END
    ) AS related_document_type_id,
    COALESCE(
        CASE st.invoicing_document_type_id
            WHEN 1 THEN 'Boleta'
            WHEN 6 THEN 'Factura'
            ELSE NULL
        END,
        CASE WHEN ps.score >= 75 THEN ps.candidate_document_type_label END
    ) AS related_document_type_label,
    ps.candidate_document_id AS probable_document_id,
    ps.candidate_number AS probable_document_number,
    ps.candidate_document_type AS probable_document_type_id,
    ps.candidate_document_type_label AS probable_document_type_label,
    ps.score AS probable_score,
    CASE
        WHEN COALESCE(st.is_invoiced, FALSE) THEN 'confirmed'
        WHEN ps.score >= 75 THEN 'confirmed'
        WHEN ps.score >= 60 THEN 'probable'
        ELSE 'missing'
    END AS status,
    CASE
        WHEN COALESCE(st.is_invoiced, FALSE) THEN 'relateddetailid'
        WHEN ps.score >= 75 THEN 'auto_match'
        WHEN ps.score >= 60 THEN 'probable_match'
        ELSE NULL
    END AS relation_source
FROM distribuidora.dispatch_plan_orders dpo
LEFT JOIN LATERAL (
    SELECT
        (d.document_id IS NOT NULL) AS is_invoiced,
        d.document_id AS invoicing_document_id,
        d.document_type_id AS invoicing_document_type_id,
        d.number AS invoicing_number
    FROM distribuidora.v_document_related_resolved dr
    INNER JOIN distribuidora.documents d
        ON d.document_id = dr.related_document_id
       AND d.document_type_id IN (1, 6)
       AND d.company_id = 3
       AND d.office_id = 1
    WHERE dr.origin_document_id = dpo.oc_document_id
    ORDER BY d.emission_date DESC NULLS LAST, d.document_id DESC
    LIMIT 1
) st ON TRUE
LEFT JOIN LATERAL (
    SELECT
        pm.candidate_document_id,
        d.number AS candidate_number,
        d.document_type_id AS candidate_document_type,
        CASE d.document_type_id
            WHEN 1 THEN 'Boleta'
            WHEN 6 THEN 'Factura'
            ELSE 'Tipo ' || d.document_type_id::text
        END AS candidate_document_type_label,
        pm.score
    FROM distribuidora.document_probable_matches pm
    INNER JOIN distribuidora.documents d
        ON d.document_id = pm.candidate_document_id
       AND d.document_type_id IN (1, 6)
       AND d.company_id = 3
       AND d.office_id = 1
    WHERE pm.oc_document_id = dpo.oc_document_id
      AND pm.score >= 60
    ORDER BY pm.score DESC, d.emission_date DESC NULLS LAST, d.document_id DESC
    LIMIT 1
) ps ON COALESCE(st.is_invoiced, FALSE) = FALSE;
-- +go
