-- Costos operacionales editables por sesión de planificación y camión (ORS 2.0 Fase 1).

CREATE TABLE IF NOT EXISTS distribuidora.route_operational_costs (
    id                   BIGSERIAL PRIMARY KEY,
    plan_session_id      TEXT NOT NULL,
    truck_id             INTEGER NOT NULL REFERENCES distribuidora.trucks (id),
    ferry_clp            INTEGER NOT NULL DEFAULT 0,
    per_diem_clp         INTEGER NOT NULL DEFAULT 0,
    other_clp            INTEGER NOT NULL DEFAULT 0,
    diesel_clp_per_liter NUMERIC(12, 2),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_route_operational_costs_session_truck
        UNIQUE (plan_session_id, truck_id)
);

CREATE INDEX IF NOT EXISTS idx_route_operational_costs_session
    ON distribuidora.route_operational_costs (plan_session_id);

COMMENT ON TABLE distribuidora.route_operational_costs IS
    'Ferry, viáticos y otros gastos por sesión ORS antes de confirmar dispatch_plan.';
