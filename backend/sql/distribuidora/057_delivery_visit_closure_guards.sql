-- P4 complemento: columnas de cierre/evidencia/conformidad.
-- Aditivo sobre 056. No muta Bsale/picking/ORS.
-- No aplicar automáticamente desde syncs/HTTP.

ALTER TABLE distribuidora.delivery_visits
    ADD COLUMN IF NOT EXISTS closure_review_status TEXT;
-- +go

ALTER TABLE distribuidora.delivery_visits
    DROP CONSTRAINT IF EXISTS ck_delivery_visits_closure_review;
-- +go

ALTER TABLE distribuidora.delivery_visits
    ADD CONSTRAINT ck_delivery_visits_closure_review
    CHECK (
        closure_review_status IS NULL
        OR closure_review_status IN ('verified', 'pending_review')
    );
-- +go

ALTER TABLE distribuidora.delivery_evidence
    ADD COLUMN IF NOT EXISTS visit_event_id BIGINT
        REFERENCES distribuidora.delivery_visit_events (id) ON DELETE SET NULL;
-- +go

ALTER TABLE distribuidora.delivery_conformity
    ADD COLUMN IF NOT EXISTS summary_payload JSONB NOT NULL DEFAULT '{}'::jsonb;
-- +go

ALTER TABLE distribuidora.delivery_conformity
    ADD COLUMN IF NOT EXISTS visit_version_at_sign INTEGER;
-- +go

ALTER TABLE distribuidora.delivery_conformity
    ADD COLUMN IF NOT EXISTS invalidated_at TIMESTAMPTZ;
-- +go

ALTER TABLE distribuidora.delivery_conformity
    ADD COLUMN IF NOT EXISTS invalidate_reason TEXT;
-- +go

COMMENT ON COLUMN distribuidora.delivery_visits.closure_review_status IS
    'verified = cierre plenamente verificado; pending_review = excepción supervisada';
-- +go
