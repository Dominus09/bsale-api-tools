-- Migración 026: vista plan-first (sin v_orders_purchase_status ni v_purchase_document_status_full).
-- Ejecutar en producción vía ensure_distribuidora_schema o manual en pgAdmin.

DROP VIEW IF EXISTS distribuidora.v_dispatch_plan_invoiced_documents;
-- +go

CREATE VIEW distribuidora.v_dispatch_plan_invoiced_documents AS
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
) ps ON COALESCE(st.is_invoiced, FALSE) = FALSE;
-- +go

COMMENT ON VIEW distribuidora.v_dispatch_plan_invoiced_documents IS
    'OCs del plan + factura confirmada (document_related) o probable (document_probable_matches). Plan-first; migración 026.';
-- +go

CREATE INDEX IF NOT EXISTS idx_dispatch_plan_orders_oc
    ON distribuidora.dispatch_plan_orders (oc_document_id);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_documents_co_office_type
    ON distribuidora.documents (company_id, office_id, document_type_id, document_id)
    WHERE company_id = 3 AND office_id = 1;
-- +go
