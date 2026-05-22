-- Parámetros de costo logístico extensible (combustible hoy; ferry/peajes/chofer preparados).

INSERT INTO distribuidora.system_config (key, value_json)
VALUES (
    'logistics_cost_settings',
    '{
        "consumption_tolerance_pct": 0,
        "ferry_cost_clp": 0,
        "toll_cost_clp_per_km": 0,
        "driver_cost_clp_per_hour": 0,
        "enabled_modules": ["fuel"]
    }'::jsonb
)
ON CONFLICT (key) DO NOTHING;
-- +go

COMMENT ON TABLE distribuidora.system_config IS
    'Parámetros operativos: diesel_price_per_liter, logistics_cost_settings (ferry, peajes, chofer, tolerancia).';
