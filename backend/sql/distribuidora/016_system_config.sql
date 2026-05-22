-- Configuración operativa editable (precios combustible, etc.).

CREATE TABLE IF NOT EXISTS distribuidora.system_config (
    key TEXT PRIMARY KEY,
    value_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +go

INSERT INTO distribuidora.system_config (key, value_json)
VALUES (
    'diesel_price_per_liter',
    '{"clp": 1200}'::jsonb
)
ON CONFLICT (key) DO NOTHING;
-- +go

COMMENT ON TABLE distribuidora.system_config IS
    'Parámetros de negocio editables (ej. valor diesel CLP/L).';
