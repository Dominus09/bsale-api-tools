-- Flota de camiones para pre-planificación / rutas (carga futura por peso).

CREATE TABLE IF NOT EXISTS distribuidora.trucks (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    plate TEXT NOT NULL UNIQUE,
    max_weight_kg INTEGER NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_trucks_active_name
    ON distribuidora.trucks (active, name);
-- +go

INSERT INTO distribuidora.trucks (name, plate, max_weight_kg)
VALUES
    ('Hino 3', 'RYPJ-94', 5600),
    ('Hino 2', 'PZPD-64', 5600),
    ('Hino 4', 'TDDP-64', 7600),
    ('Hyundai', 'SRJK-79', 1500)
ON CONFLICT (plate) DO NOTHING;
-- +go

COMMENT ON TABLE distribuidora.trucks IS
    'Camiones de despacho; capacidad en kg para futura optimización de carga.';
