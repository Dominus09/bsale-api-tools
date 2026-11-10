-- =============================================================================
-- 058 — Anulación / clasificación administrativa de visitas de entrega
-- ESTADO: LISTA PARA REVISIÓN — NO APLICAR EN PRODUCCIÓN SIN AUTORIZACIÓN
-- =============================================================================
-- Aditivo sobre 056/057. No muta Bsale / picking / ORS / R2 bytes.
-- No DELETE de visitas ni de resultados hijos.
--
-- Clasificación (pilot_test / duplicate_pending_review): SOLO informativa.
-- Exclusión operativa / desbloqueo: ÚNICAMENTE voided_admin + auditoría completa
-- (voided_at, voided_by_user_id > 0, void_reason no vacío).
--
-- Atomicidad del runner: cada chunk `-- +go` se ejecuta en el mismo cursor;
-- el job apply_distribuidora_schema hace COMMIT al final del lote. Si falla un
-- chunk intermedio, hacer ROLLBACK de la conexión antes de reintentar.
-- Preferible aplicar 058 en una sesión controlada (BEGIN…COMMIT) en pgAdmin
-- cuando se autorice, o vía runner explícito sobre DB de prueba primero.
-- =============================================================================

ALTER TABLE distribuidora.delivery_visits
    ADD COLUMN IF NOT EXISTS admin_classification TEXT NOT NULL DEFAULT 'operational';
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

-- Dominio de clasificación (informativa + voided_admin)
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

-- Integridad void: voided_admin exige auditoría completa;
-- no-anuladas no pueden tener campos de void parciales/inconsistentes.
ALTER TABLE distribuidora.delivery_visits
    DROP CONSTRAINT IF EXISTS ck_delivery_visits_admin_void_integrity;
-- +go

ALTER TABLE distribuidora.delivery_visits
    ADD CONSTRAINT ck_delivery_visits_admin_void_integrity
    CHECK (
        (
            admin_classification = 'voided_admin'
            AND voided_at IS NOT NULL
            AND voided_by_user_id IS NOT NULL
            AND voided_by_user_id > 0
            AND void_reason IS NOT NULL
            AND length(btrim(void_reason)) > 0
        )
        OR
        (
            admin_classification <> 'voided_admin'
            AND voided_at IS NULL
            AND voided_by_user_id IS NULL
            AND (void_reason IS NULL OR length(btrim(void_reason)) = 0)
            AND void_ticket_ref IS NULL
        )
    );
-- +go

CREATE INDEX IF NOT EXISTS idx_delivery_visits_admin_class
    ON distribuidora.delivery_visits (dispatch_plan_id, admin_classification, status);
-- +go

COMMENT ON COLUMN distribuidora.delivery_visits.admin_classification IS
    'operational | pilot_test | duplicate_pending_review | voided_admin. pilot_test/duplicate_pending_review son informativos y NO liberan bloqueo. Solo voided_admin + auditoría completa excluye del estado operativo.';
-- +go

COMMENT ON COLUMN distribuidora.delivery_visits.voided_at IS
    'Timestamp de anulación administrativa auditada. Obligatorio con voided_admin. No implica DELETE.';
-- +go

COMMENT ON COLUMN distribuidora.delivery_visits.voided_by_user_id IS
    'Actor administrador de la anulación (>0). Obligatorio con voided_admin.';
-- +go

COMMENT ON COLUMN distribuidora.delivery_visits.void_reason IS
    'Motivo no vacío de anulación. Obligatorio con voided_admin.';
-- +go
