-- Costos operacionales chofer/peoneta por vuelta + persistencia por sesión de planificación ORS.

UPDATE distribuidora.system_config
SET value_json = value_json || jsonb_build_object(
    'driver_cost_clp_per_trip', 50895,
    'assistant_cost_clp_per_trip', 38102,
    'bonus_clp_per_route', 0,
    'per_diem_clp_per_day', 0,
    'lodging_clp_per_night', 0,
    'enabled_modules', '["fuel", "crew"]'::jsonb
)
WHERE key = 'logistics_cost_settings';
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.ors_plan_route_crew (
    plan_session_id TEXT NOT NULL,
    camion TEXT NOT NULL,
    truck_id INTEGER,
    driver_count INTEGER NOT NULL DEFAULT 1 CHECK (driver_count >= 0 AND driver_count <= 10),
    assistant_count INTEGER NOT NULL DEFAULT 0 CHECK (assistant_count >= 0 AND assistant_count <= 10),
    driver_cost_clp INTEGER NOT NULL CHECK (driver_cost_clp >= 0),
    assistant_cost_clp INTEGER NOT NULL CHECK (assistant_cost_clp >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (plan_session_id, camion)
);
-- +go

CREATE INDEX IF NOT EXISTS idx_ors_plan_route_crew_session
    ON distribuidora.ors_plan_route_crew (plan_session_id);
-- +go

COMMENT ON TABLE distribuidora.ors_plan_route_crew IS
    'Cantidades y tarifas de personal (chofer/peoneta) por camión en una sesión de planificación ORS.';
