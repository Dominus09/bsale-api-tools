-- Rendimiento: v_dispatch_plan_invoiced_documents
-- Problema: JOIN a v_orders_purchase_status / v_purchase_document_status_full materializa
-- TODAS las OCs (v_documents_latest + probables) antes de aplicar dispatch_plan_id.
-- Solución: LATERAL por oc_document_id del plan (misma semántica, plan-first).

CREATE OR REPLACE VIEW distribuidora.v_dispatch_plan_invoiced_documents AS
SELECT
    dpo.dispatch_plan_id,
    dpo.oc_document_id,
    dpo.oc_number,
    dpo.route_order,
    (st.invoicing_document_id IS NOT NULL) AS is_invoiced_confirmed,
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
        WHEN st.invoicing_document_id IS NOT NULL THEN 'confirmed'
        WHEN ps.score >= 60 THEN 'probable'
        ELSE 'missing'
    END AS status,
    CASE
        WHEN st.invoicing_document_id IS NOT NULL THEN 'relateddetailid'
        WHEN ps.score >= 60 THEN 'probable_match'
        ELSE NULL
    END AS relation_source
FROM distribuidora.dispatch_plan_orders dpo
LEFT JOIN LATERAL (
    SELECT
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
) ps ON st.invoicing_document_id IS NULL;
-- +go

-- Lookup inverso OC → plan (listados agregados).
CREATE INDEX IF NOT EXISTS idx_dispatch_plan_orders_oc
    ON distribuidora.dispatch_plan_orders (oc_document_id);
-- +go

-- Filtro empresa/sucursal/tipo en documents (evita seq scan en LATERAL).
CREATE INDEX IF NOT EXISTS idx_distribuidora_documents_co_office_type
    ON distribuidora.documents (company_id, office_id, document_type_id, document_id)
    WHERE company_id = 3 AND office_id = 1;
-- +go
