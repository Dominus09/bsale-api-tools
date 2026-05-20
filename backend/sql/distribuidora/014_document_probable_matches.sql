-- Capa analítica: coincidencias operacionales OC → boleta/factura sin ``document_related``.
-- No altera relateddetailid ni document_related.

CREATE TABLE IF NOT EXISTS distribuidora.document_probable_matches (
    id BIGSERIAL PRIMARY KEY,
    oc_document_id BIGINT NOT NULL,
    candidate_document_id BIGINT NOT NULL,
    candidate_document_type INT NOT NULL,
    score NUMERIC(5, 2) NOT NULL,
    match_products_pct NUMERIC(5, 2) NOT NULL DEFAULT 0,
    same_client BOOLEAN NOT NULL DEFAULT FALSE,
    same_seller BOOLEAN NOT NULL DEFAULT FALSE,
    same_day BOOLEAN NOT NULL DEFAULT FALSE,
    same_amount BOOLEAN NOT NULL DEFAULT FALSE,
    tracking_match BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_document_probable_matches_oc_candidate
        UNIQUE (oc_document_id, candidate_document_id),
    CONSTRAINT fk_probable_matches_oc
        FOREIGN KEY (oc_document_id)
        REFERENCES distribuidora.documents (document_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_probable_matches_candidate
        FOREIGN KEY (candidate_document_id)
        REFERENCES distribuidora.documents (document_id)
        ON DELETE CASCADE
);
-- +go

CREATE INDEX IF NOT EXISTS idx_document_probable_matches_oc_score
    ON distribuidora.document_probable_matches (oc_document_id, score DESC);
-- +go

CREATE INDEX IF NOT EXISTS idx_document_probable_matches_candidate
    ON distribuidora.document_probable_matches (candidate_document_id);
-- +go

-- Estado unificado OC: confirmada (document_related) > probable > pendiente.
CREATE OR REPLACE VIEW distribuidora.v_purchase_document_status AS
SELECT
    oc.document_id,
    oc.number,
    CASE
        WHEN COALESCE(conf.is_invoiced, FALSE) THEN 'FACTURADA_CONFIRMADA'
        WHEN prob.probable_document_id IS NOT NULL THEN 'PROBABLE_FACTURADA'
        ELSE 'PENDIENTE'
    END AS status,
    COALESCE(conf.is_invoiced, FALSE) AS is_invoiced_confirmed,
    conf.invoicing_document_id,
    conf.invoicing_document_type_id,
    conf.invoicing_number,
    conf.invoicing_emission_date,
    prob.probable_document_id,
    prob.probable_document_type_id,
    prob.probable_number,
    prob.probable_emission_date,
    prob.probable_score,
    prob.probable_tier,
    CASE
        WHEN COALESCE(conf.is_invoiced, FALSE) THEN 'Facturada'
        WHEN prob.probable_document_id IS NOT NULL THEN 'Probable facturada'
        ELSE 'Pendiente'
    END AS estado_real
FROM distribuidora.v_orders_purchase oc
LEFT JOIN distribuidora.v_orders_purchase_status conf ON conf.document_id = oc.document_id
LEFT JOIN LATERAL (
    SELECT
        d.document_id AS probable_document_id,
        d.document_type_id AS probable_document_type_id,
        d.number AS probable_number,
        d.emission_date AS probable_emission_date,
        pm.score AS probable_score,
        CASE
            WHEN pm.score >= 90 THEN 'PROBABLE_FACTURADA_HIGH'
            WHEN pm.score >= 75 THEN 'PROBABLE_FACTURADA_MEDIUM'
            WHEN pm.score >= 60 THEN 'PROBABLE_FACTURADA_LOW'
            ELSE NULL
        END AS probable_tier
    FROM distribuidora.document_probable_matches pm
    INNER JOIN distribuidora.v_documents_latest d
        ON d.document_id = pm.candidate_document_id
       AND d.document_type_id IN (1, 6)
       AND d.company_id = 3
       AND d.office_id = 1
    WHERE pm.oc_document_id = oc.document_id
      AND pm.score >= 60
      AND COALESCE(conf.is_invoiced, FALSE) = FALSE
    ORDER BY pm.score DESC, d.emission_date DESC NULLS LAST, d.document_id DESC
    LIMIT 1
) prob ON TRUE;
-- +go

-- Vista enriquecida: prioridad related real > probable > ninguno.
DROP VIEW IF EXISTS distribuidora.v_orders_purchase_enriched CASCADE;
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
    ps.status AS purchase_status,
    ps.is_invoiced_confirmed AS is_invoiced,
    ps.estado_real,
    ps.invoicing_document_id,
    ps.invoicing_document_type_id,
    ps.invoicing_number,
    ps.invoicing_emission_date,
    ps.probable_document_id,
    ps.probable_document_type_id,
    ps.probable_number,
    ps.probable_emission_date,
    ps.probable_score,
    ps.probable_tier,
    p.seller_id,
    p.seller_name
FROM distribuidora.v_orders_purchase p
LEFT JOIN distribuidora.v_purchase_document_status ps ON ps.document_id = p.document_id;
-- +go
