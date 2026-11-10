-- =============================================================================
-- 058 — Anulación / clasificación administrativa de visitas de entrega
-- ESTADO: PROPUESTA — NO APLICAR SIN AUTORIZACIÓN EXPLÍCITA
-- =============================================================================
-- Aditivo sobre 056/057. No muta Bsale / picking / ORS / R2 bytes.
-- No DELETE de visitas ni de resultados hijos.
--
-- Motivo: visitas de prueba (p. ej. Romeo Alfa #1 y #3) deben excluirse del
-- estado operativo y del bloqueo de nueva visita, conservando trazabilidad.
-- =============================================================================

ALTER TABLE distribuidora.delivery_visits
    ADD COLUMN IF NOT EXISTS admin_classification TEXT NOT NULL DEFAULT 'operational';
-- +go

ALTER TABLE distribuidora.delivery_visits
    DROP CONSTRAINT IF EXISTS ck_delivery_visits_admin_classification;
-- +go

ALTER TABLE distribuidora.delivery_visits
    ADD CONSTRAINT ck_delivery_visits_admin_classification
    CHECK (
        admin_classification IN (
            'operational',
            'pilot_test',
            'duplicate_pending_review',
            'voided_admin'
        )
    );
-- +go

ALTER TABLE distribuidora.delivery_visits
    ADD COLUMN IF NOT EXISTS voided_at TIMESTAMPTZ;
-- +go

ALTER TABLE distribuidora.delivery_visits
    ADD COLUMN IF NOT EXISTS voided_by_user_id INTEGER;
-- +go

ALTER TABLE distribuidora.delivery_visits
    ADD COLUMN IF NOT EXISTS void_reason TEXT;
-- +go

ALTER TABLE distribuidora.delivery_visits
    ADD COLUMN IF NOT EXISTS void_ticket_ref TEXT;
-- +go

CREATE INDEX IF NOT EXISTS idx_delivery_visits_admin_class
    ON distribuidora.delivery_visits (dispatch_plan_id, admin_classification, status);
-- +go

COMMENT ON COLUMN distribuidora.delivery_visits.admin_classification IS
    'operational | pilot_test | duplicate_pending_review | voided_admin. Solo voided_admin + voided_at excluye del bloqueo/estado operativo. pilot_test sin void NO salta restricciones.';
-- +go

COMMENT ON COLUMN distribuidora.delivery_visits.voided_at IS
    'Timestamp de anulación administrativa auditada (obligatorio junto a voided_admin). No implica DELETE.';
-- +go
