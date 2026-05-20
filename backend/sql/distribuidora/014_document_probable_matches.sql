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
-- Vistas de estado: ver ``015_v_purchase_document_status_full.sql`` (migración 015).
