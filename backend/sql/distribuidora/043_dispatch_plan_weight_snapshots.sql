-- Snapshot de peso logístico al confirmar planificación (congelado como picking).

ALTER TABLE distribuidora.dispatch_plan
    ADD COLUMN IF NOT EXISTS weight_total_kg NUMERIC(14, 3),
    ADD COLUMN IF NOT EXISTS truck_max_weight_kg INTEGER,
    ADD COLUMN IF NOT EXISTS weight_utilization_pct NUMERIC(5, 2),
    ADD COLUMN IF NOT EXISTS weight_calculated_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS weight_calc_version TEXT,
    ADD COLUMN IF NOT EXISTS weight_orders_count INTEGER,
    ADD COLUMN IF NOT EXISTS weight_productos_totales INTEGER,
    ADD COLUMN IF NOT EXISTS weight_unidades_totales NUMERIC(18, 4),
    ADD COLUMN IF NOT EXISTS weight_cobertura_pct NUMERIC(5, 1);
-- +go

ALTER TABLE distribuidora.dispatch_plan_orders
    ADD COLUMN IF NOT EXISTS peso_total_kg NUMERIC(14, 3),
    ADD COLUMN IF NOT EXISTS cantidad_productos INTEGER,
    ADD COLUMN IF NOT EXISTS cantidad_unidades NUMERIC(18, 4),
    ADD COLUMN IF NOT EXISTS cantidad_cajas NUMERIC(18, 4),
    ADD COLUMN IF NOT EXISTS productos_sin_peso INTEGER,
    ADD COLUMN IF NOT EXISTS cobertura_logistica NUMERIC(5, 1),
    ADD COLUMN IF NOT EXISTS peso_calculated_at TIMESTAMPTZ;
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.dispatch_plan_stop_snapshots (
    id BIGSERIAL PRIMARY KEY,
    dispatch_plan_id BIGINT NOT NULL
        REFERENCES distribuidora.dispatch_plan (id) ON DELETE CASCADE,
    stop_order INTEGER NOT NULL DEFAULT 0,
    client_id BIGINT,
    client_name TEXT,
    address TEXT,
    city TEXT,
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION,
    peso_total_kg NUMERIC(14, 3) NOT NULL DEFAULT 0,
    cantidad_cajas NUMERIC(18, 4) NOT NULL DEFAULT 0,
    cantidad_unidades NUMERIC(18, 4) NOT NULL DEFAULT 0,
    cantidad_productos INTEGER NOT NULL DEFAULT 0,
    monto_total NUMERIC(18, 2) NOT NULL DEFAULT 0,
    oc_document_ids BIGINT[] NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +go

CREATE INDEX IF NOT EXISTS idx_dispatch_plan_stop_snapshots_plan
    ON distribuidora.dispatch_plan_stop_snapshots (dispatch_plan_id, stop_order);
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.dispatch_plan_weight_audit (
    id BIGSERIAL PRIMARY KEY,
    dispatch_plan_id BIGINT REFERENCES distribuidora.dispatch_plan (id) ON DELETE SET NULL,
    plan_session_id TEXT,
    user_email TEXT,
    peso_anterior_kg NUMERIC(14, 3),
    peso_nuevo_kg NUMERIC(14, 3),
    motivo TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +go

CREATE INDEX IF NOT EXISTS idx_dispatch_plan_weight_audit_plan
    ON distribuidora.dispatch_plan_weight_audit (dispatch_plan_id, created_at DESC);
-- +go

COMMENT ON COLUMN distribuidora.dispatch_plan.weight_total_kg IS
    'Peso total congelado al confirmar el plan (kg).';
-- +go

COMMENT ON TABLE distribuidora.dispatch_plan_stop_snapshots IS
    'Paradas agrupadas por cliente/dirección con peso congelado al confirmar.';
-- +go
