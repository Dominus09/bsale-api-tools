-- Documentos facturados reales (o probables) vinculados a OCs de un dispatch_plan.

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
LEFT JOIN distribuidora.v_orders_purchase_status st
    ON st.document_id = dpo.oc_document_id
LEFT JOIN distribuidora.v_purchase_document_status_full ps
    ON ps.oc_document_id = dpo.oc_document_id
   AND COALESCE(st.is_invoiced, FALSE) = FALSE;
-- +go
