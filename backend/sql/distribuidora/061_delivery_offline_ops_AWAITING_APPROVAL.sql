-- 061 — Offline V2 Fase 2A.1: cola de sincronización idempotente (delivery_offline_ops)
-- ESTADO: LISTO PARA ADMINISTRADOR — aplicar SOLO manualmente tras respaldo.
-- NO ejecutar automáticamente desde Cursor / CI.
-- Rollback operativo = kill switch (writes OFF). NO usar DROP TABLE con datos reales.
-- Ver: docs/offline-v2-production-pilot-runbook.md
--
-- Objetivo:
--   Registrar operaciones offline (entrega completa / local cerrado) con
--   UNIQUE(driver_user_id, idempotency_key), request_hash inmutable,
--   respuesta persistida y etapas recuperables (R2 fuera de TX SQL larga).
--
-- Política de retención (cliente + servidor):
--   - Una operación no confirmada NUNCA se elimina automáticamente.
--   - 401 / logout conservan la outbox aislada por userId (lado PWA en 2A.2).
--   - Login de otro usuario NO borra pendientes ajenos; solo aislamiento.
--   - Borrado solo con procedimiento autorizado y auditable.
--
-- No toca Bsale, picking histórico, stock ni cuadratura.
-- Idempotente. Aditivo.

BEGIN;

CREATE TABLE IF NOT EXISTS distribuidora.delivery_offline_ops (
    id BIGSERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL,
    driver_user_id INTEGER NOT NULL,
    dispatch_plan_id BIGINT NOT NULL
        REFERENCES distribuidora.dispatch_plan (id) ON DELETE RESTRICT,
    customer_key TEXT NOT NULL,
    client_id BIGINT,
    op_type TEXT NOT NULL
        CHECK (op_type IN ('complete_delivery', 'closed_store')),
    idempotency_key TEXT NOT NULL,
    client_mutation_id TEXT,
    request_hash TEXT NOT NULL,
    request_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'received'
        CHECK (status IN (
            'received',
            'processing',
            'awaiting_evidence',
            'evidence_ready',
            'completed',
            'conflict',
            'failed'
        )),
    processing_stage TEXT NOT NULL DEFAULT 'claimed'
        CHECK (processing_stage IN (
            'claimed',
            'validated',
            'evidence_upload',
            'evidence_confirmed',
            'visit_applied',
            'completed',
            'conflict',
            'failed'
        )),
    visit_id BIGINT
        REFERENCES distribuidora.delivery_visits (id) ON DELETE SET NULL,
    picking_id BIGINT,
    picking_version TEXT,
    response_json JSONB,
    error_code TEXT,
    error_message TEXT,
    evidence_object_key TEXT,
    evidence_sha256 TEXT,
    evidence_upload_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
    gps_capture_mode TEXT
        CHECK (gps_capture_mode IS NULL OR gps_capture_mode IN (
            'online',
            'deferred_offline',
            'exception'
        )),
    captured_at TIMESTAMPTZ,
    device_id TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    locked_at TIMESTAMPTZ,
    locked_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    created_by_user_id INTEGER NOT NULL,
    CONSTRAINT uq_delivery_offline_ops_driver_idem
        UNIQUE (driver_user_id, idempotency_key)
);

COMMENT ON TABLE distribuidora.delivery_offline_ops IS
    'Offline V2: operaciones de sync idempotentes. No borrar automáticamente pendientes.';

COMMENT ON COLUMN distribuidora.delivery_offline_ops.request_hash IS
    'Hash SHA-256 del payload lógico inmutable (sin bytes de foto).';

COMMENT ON COLUMN distribuidora.delivery_offline_ops.response_json IS
    'Respuesta confirmada para replay tras COMMIT (pérdida de HTTP).';

COMMENT ON COLUMN distribuidora.delivery_offline_ops.evidence_object_key IS
    'Clave R2 estable; upload fuera de TX SQL larga; confirmar luego en DB.';

CREATE INDEX IF NOT EXISTS idx_delivery_offline_ops_plan_status
    ON distribuidora.delivery_offline_ops (dispatch_plan_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_delivery_offline_ops_visit
    ON distribuidora.delivery_offline_ops (visit_id)
    WHERE visit_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_delivery_offline_ops_driver_pending
    ON distribuidora.delivery_offline_ops (driver_user_id, status, created_at DESC)
    WHERE status IN (
        'received',
        'processing',
        'awaiting_evidence',
        'evidence_ready',
        'conflict',
        'failed'
    );

COMMIT;
