-- Vista operacional legible (folios + cliente + match probable). No reemplaza tablas base.

DROP VIEW IF EXISTS distribuidora.v_orders_purchase_enriched CASCADE;
-- +go
DROP VIEW IF EXISTS distribuidora.v_purchase_document_status CASCADE;
-- +go
DROP VIEW IF EXISTS distribuidora.v_purchase_document_status_full CASCADE;
-- +go

CREATE OR REPLACE VIEW distribuidora.v_purchase_document_status_full AS
SELECT
    p.document_id AS oc_document_id,
    p.number AS oc_number,
    p.emission_date AS oc_emission_date,
    p.total_amount AS oc_total_amount,
    p.client_id AS oc_client_id,
    COALESCE(
        NULLIF(BTRIM(p.nombre_fantasia), ''),
        NULLIF(BTRIM(c.nombre_fantasia), ''),
        NULLIF(BTRIM(c.company), ''),
        NULLIF(
            BTRIM(
                COALESCE(c.first_name, '')
                || ' '
                || COALESCE(c.last_name, '')
            ),
            ''
        )
    ) AS oc_client_name,
    COALESCE(conf.is_invoiced, FALSE) AS is_invoiced_confirmed,
    conf.invoicing_document_id,
    conf.invoicing_document_type_id,
    conf.invoicing_number,
    conf.invoicing_emission_date,
    prob.candidate_document_id,
    prob.candidate_number,
    prob.candidate_document_type,
    prob.candidate_document_type_label,
    prob.candidate_emission_date,
    prob.candidate_total_amount,
    prob.score,
    prob.match_products_pct,
    prob.same_client,
    prob.same_seller,
    prob.same_day,
    prob.same_amount,
    prob.tracking_match,
    CASE
        WHEN COALESCE(conf.is_invoiced, FALSE) THEN 'FACTURADA_CONFIRMADA'
        WHEN prob.score >= 90 THEN 'PROBABLE_FACTURADA_HIGH'
        WHEN prob.score >= 75 THEN 'PROBABLE_FACTURADA_MEDIUM'
        WHEN prob.score >= 60 THEN 'PROBABLE_FACTURADA_LOW'
        ELSE 'PENDIENTE'
    END AS status,
    CASE
        WHEN COALESCE(conf.is_invoiced, FALSE) THEN 'Facturada'
        WHEN prob.score >= 60 THEN 'Probable facturada'
        ELSE 'Pendiente'
    END AS estado_real,
    CASE
        WHEN COALESCE(conf.is_invoiced, FALSE) THEN
            CASE conf.invoicing_document_type_id
                WHEN 1 THEN 'Boleta'
                WHEN 6 THEN 'Factura'
                ELSE 'Documento'
            END
            || ' '
            || COALESCE(conf.invoicing_number::text, conf.invoicing_document_id::text)
        WHEN prob.score >= 60 THEN
            prob.candidate_document_type_label
            || ' '
            || COALESCE(prob.candidate_number::text, prob.candidate_document_id::text)
        ELSE NULL
    END AS associated_document_label,
    CASE
        WHEN COALESCE(conf.is_invoiced, FALSE) THEN 100::numeric
        WHEN prob.score >= 60 THEN prob.score
        ELSE NULL
    END AS display_score
FROM distribuidora.v_orders_purchase p
LEFT JOIN bsale.clients c
    ON c.company_id = 3
   AND c.bsale_id = p.client_id
LEFT JOIN distribuidora.v_orders_purchase_status conf
    ON conf.document_id = p.document_id
LEFT JOIN LATERAL (
    SELECT
        d.document_id AS candidate_document_id,
        d.number AS candidate_number,
        d.document_type_id AS candidate_document_type,
        CASE d.document_type_id
            WHEN 1 THEN 'Boleta'
            WHEN 6 THEN 'Factura'
            ELSE 'Tipo ' || d.document_type_id::text
        END AS candidate_document_type_label,
        d.emission_date AS candidate_emission_date,
        d.total_amount AS candidate_total_amount,
        pm.score,
        pm.match_products_pct,
        pm.same_client,
        pm.same_seller,
        pm.same_day,
        pm.same_amount,
        pm.tracking_match
    FROM distribuidora.document_probable_matches pm
    INNER JOIN distribuidora.v_documents_latest d
        ON d.document_id = pm.candidate_document_id
       AND d.document_type_id IN (1, 6)
       AND d.company_id = 3
       AND d.office_id = 1
    WHERE pm.oc_document_id = p.document_id
      AND pm.score >= 60
      AND COALESCE(conf.is_invoiced, FALSE) = FALSE
    ORDER BY pm.score DESC, d.emission_date DESC NULLS LAST, d.document_id DESC
    LIMIT 1
) prob ON TRUE;
-- +go

-- Compatibilidad endpoints existentes (columnas históricas + status granular).
CREATE OR REPLACE VIEW distribuidora.v_purchase_document_status AS
SELECT
    f.oc_document_id AS document_id,
    f.oc_number AS number,
    f.status,
    f.is_invoiced_confirmed AS is_invoiced_confirmed,
    f.invoicing_document_id,
    f.invoicing_document_type_id,
    f.invoicing_number,
    f.invoicing_emission_date,
    f.candidate_document_id AS probable_document_id,
    f.candidate_document_type AS probable_document_type_id,
    f.candidate_number AS probable_number,
    f.candidate_emission_date AS probable_emission_date,
    f.score AS probable_score,
    CASE
        WHEN f.status = 'PROBABLE_FACTURADA_HIGH' THEN 'PROBABLE_FACTURADA_HIGH'
        WHEN f.status = 'PROBABLE_FACTURADA_MEDIUM' THEN 'PROBABLE_FACTURADA_MEDIUM'
        WHEN f.status = 'PROBABLE_FACTURADA_LOW' THEN 'PROBABLE_FACTURADA_LOW'
        ELSE NULL
    END AS probable_tier,
    f.estado_real,
    f.associated_document_label,
    f.display_score
FROM distribuidora.v_purchase_document_status_full f;
-- +go

CREATE OR REPLACE VIEW distribuidora.v_orders_purchase_enriched AS
SELECT
    p.document_id,
    p.number,
    p.client_id,
    p.user_id,
    p.emission_date,
    p.total_amount,
    p.municipality,
    p.city,
    p.address,
    p.tipo_documento_a_generar,
    p.nombre_fantasia,
    p.forma_pago,
    p.observaciones,
    f.status AS purchase_status,
    f.is_invoiced_confirmed AS is_invoiced,
    f.estado_real,
    f.oc_document_id,
    f.oc_number,
    f.oc_emission_date,
    f.oc_total_amount,
    f.oc_client_id,
    f.oc_client_name,
    f.invoicing_document_id,
    f.invoicing_document_type_id,
    f.invoicing_number,
    f.invoicing_emission_date,
    f.candidate_document_id,
    f.candidate_number,
    f.candidate_document_type,
    f.candidate_document_type_label,
    f.candidate_emission_date,
    f.candidate_total_amount,
    f.score,
    f.match_products_pct,
    f.same_client,
    f.same_seller,
    f.same_day,
    f.same_amount,
    f.tracking_match,
    f.associated_document_label,
    f.display_score,
    f.candidate_document_id AS probable_document_id,
    f.candidate_document_type AS probable_document_type_id,
    f.candidate_number AS probable_number,
    f.candidate_emission_date AS probable_emission_date,
    f.score AS probable_score,
    CASE
        WHEN f.status = 'PROBABLE_FACTURADA_HIGH' THEN 'PROBABLE_FACTURADA_HIGH'
        WHEN f.status = 'PROBABLE_FACTURADA_MEDIUM' THEN 'PROBABLE_FACTURADA_MEDIUM'
        WHEN f.status = 'PROBABLE_FACTURADA_LOW' THEN 'PROBABLE_FACTURADA_LOW'
        ELSE NULL
    END AS probable_tier,
    p.seller_id,
    p.seller_name
FROM distribuidora.v_orders_purchase p
LEFT JOIN distribuidora.v_purchase_document_status_full f
    ON f.oc_document_id = p.document_id;
-- +go
