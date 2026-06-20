-- Cuadratura v2: por documento, historial y estados operacionales.

ALTER TABLE distribuidora.dispatch_plan_cuadratura
    ADD COLUMN IF NOT EXISTS schema_version INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'pending',
    ADD COLUMN IF NOT EXISTS documents JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS credit_notes_v2 JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS not_loaded_v2 JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS picking_id BIGINT,
    ADD COLUMN IF NOT EXISTS picking_version INTEGER,
    ADD COLUMN IF NOT EXISTS closed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS closed_by TEXT,
    ADD COLUMN IF NOT EXISTS resultado_cache JSONB;

COMMENT ON COLUMN distribuidora.dispatch_plan_cuadratura.schema_version IS
    '1=medios globales legacy, 2=cuadratura documental.';
COMMENT ON COLUMN distribuidora.dispatch_plan_cuadratura.status IS
    'pending|draft|in_review|difference|squared';
COMMENT ON COLUMN distribuidora.dispatch_plan_cuadratura.documents IS
    'Filas documentales con medio de pago por venta despachada.';
COMMENT ON COLUMN distribuidora.dispatch_plan_cuadratura.resultado_cache IS
    'Snapshot calculado para listados y dashboard futuro.';

CREATE TABLE IF NOT EXISTS distribuidora.dispatch_plan_cuadratura_history (
    id                 BIGSERIAL PRIMARY KEY,
    dispatch_plan_id   BIGINT NOT NULL
        REFERENCES distribuidora.dispatch_plan (id) ON DELETE CASCADE,
    version            INTEGER NOT NULL DEFAULT 1,
    schema_version     INTEGER NOT NULL DEFAULT 2,
    status             TEXT NOT NULL,
    snapshot           JSONB NOT NULL,
    closed_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    closed_by          TEXT,
    observacion        TEXT,
    diferencia_clp     INTEGER,
    diferencia_status  TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dispatch_plan_cuadratura_status
    ON distribuidora.dispatch_plan_cuadratura (status);

CREATE INDEX IF NOT EXISTS idx_dispatch_plan_cuadratura_history_plan
    ON distribuidora.dispatch_plan_cuadratura_history (dispatch_plan_id, closed_at DESC);

COMMENT ON TABLE distribuidora.dispatch_plan_cuadratura_history IS
    'Historial de cierres de cuadratura por plan (trazabilidad operacional).';
