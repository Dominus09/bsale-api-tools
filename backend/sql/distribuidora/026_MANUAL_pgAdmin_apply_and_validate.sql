-- =============================================================================
-- Migración 026 — MANUAL pgAdmin (producción)
-- Reemplaza v_dispatch_plan_invoiced_documents por definición plan-first.
-- Sin dependencia de v_orders_purchase_status ni v_purchase_document_status_full.
--
-- Columnas (orden = backend dispatch_plan_repo.list_invoiced_documents):
--   dispatch_plan_id, oc_document_id, oc_number, route_order,
--   is_invoiced_confirmed, related_document_id, related_document_number,
--   related_document_type_id, related_document_type_label,
--   probable_document_id, probable_document_number, probable_document_type_id,
--   probable_document_type_label, probable_score, status, relation_source
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1) Vista optimizada
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS distribuidora.v_dispatch_plan_invoiced_documents;

CREATE VIEW distribuidora.v_dispatch_plan_invoiced_documents AS
SELECT
    dpo.dispatch_plan_id,
    dpo.oc_document_id,
    dpo.oc_number,
    dpo.route_order,
    COALESCE(st.is_invoiced, FALSE) AS is_invoiced_confirmed,
    st.invoicing_document_id AS related_document_id,
    st.invoicing_number AS related_document_number,
    st.invoicing_document_type_id AS related_document_type_id,
    CASE st.invoicing_document_type_id
        WHEN 1 THEN 'Boleta'
        WHEN 6 THEN 'Factura'
        ELSE NULL
    END AS related_document_type_label,
    ps.candidate_document_id AS probable_document_id,
    ps.candidate_number AS probable_document_number,
    ps.candidate_document_type AS probable_document_type_id,
    ps.candidate_document_type_label AS probable_document_type_label,
    ps.score AS probable_score,
    CASE
        WHEN COALESCE(st.is_invoiced, FALSE) THEN 'confirmed'
        WHEN ps.score >= 60 THEN 'probable'
        ELSE 'missing'
    END AS status,
    CASE
        WHEN COALESCE(st.is_invoiced, FALSE) THEN 'relateddetailid'
        WHEN ps.score >= 60 THEN 'probable_match'
        ELSE NULL
    END AS relation_source
FROM distribuidora.dispatch_plan_orders dpo
LEFT JOIN LATERAL (
    -- Misma semántica que v_orders_purchase_status (document_related → boleta/factura)
    SELECT
        (d.document_id IS NOT NULL) AS is_invoiced,
        d.document_id AS invoicing_document_id,
        d.document_type_id AS invoicing_document_type_id,
        d.number AS invoicing_number
    FROM distribuidora.document_details dd
    INNER JOIN distribuidora.document_related dr
        ON dr.detail_id = dd.detail_id
    INNER JOIN distribuidora.documents d
        ON d.document_id = dr.related_document_id
       AND d.document_type_id IN (1, 6)
       AND d.company_id = 3
       AND d.office_id = 1
    WHERE dd.document_id = dpo.oc_document_id
    ORDER BY d.emission_date DESC NULLS LAST, d.document_id DESC
    LIMIT 1
) st ON TRUE
LEFT JOIN LATERAL (
    -- Misma semántica que v_purchase_document_status_full (solo si no hay confirmada)
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

COMMENT ON VIEW distribuidora.v_dispatch_plan_invoiced_documents IS
    'OCs del plan + factura confirmada (document_related) o probable (document_probable_matches). Plan-first; migración 026.';

-- -----------------------------------------------------------------------------
-- 2) Índices de soporte (idempotentes)
-- -----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_dispatch_plan_orders_oc
    ON distribuidora.dispatch_plan_orders (oc_document_id);

CREATE INDEX IF NOT EXISTS idx_distribuidora_documents_co_office_type
    ON distribuidora.documents (company_id, office_id, document_type_id, document_id)
    WHERE company_id = 3 AND office_id = 1;

COMMIT;

-- -----------------------------------------------------------------------------
-- 3) Comprobación rápida (firma 026)
-- -----------------------------------------------------------------------------
SELECT
    position('v_orders_purchase_status' IN lower(def)) = 0 AS sin_v_orders_purchase_status,
    position('v_purchase_document_status_full' IN lower(def)) = 0 AS sin_v_purchase_full,
    position('join lateral' IN lower(def)) > 0 AS tiene_lateral,
    position('dispatch_plan_orders' IN lower(def)) > 0 AS tiene_dispatch_plan_orders
FROM (
    SELECT pg_get_viewdef('distribuidora.v_dispatch_plan_invoiced_documents'::regclass, true) AS def
) x;

-- -----------------------------------------------------------------------------
-- 4) Validación rendimiento — ejecutar DESPUÉS del COMMIT
--    (comparar Execution Time con ~106641 ms antes)
-- -----------------------------------------------------------------------------
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT *
FROM distribuidora.v_dispatch_plan_invoiced_documents
WHERE dispatch_plan_id = 3;

-- Opcional: mismas columnas que el backend
-- SELECT
--     dispatch_plan_id, oc_document_id, oc_number, route_order,
--     is_invoiced_confirmed, related_document_id, related_document_number,
--     related_document_type_id, related_document_type_label,
--     probable_document_id, probable_document_number, probable_document_type_id,
--     probable_document_type_label, probable_score, status, relation_source
-- FROM distribuidora.v_dispatch_plan_invoiced_documents
-- WHERE dispatch_plan_id = 3
-- ORDER BY route_order ASC, oc_document_id ASC;
