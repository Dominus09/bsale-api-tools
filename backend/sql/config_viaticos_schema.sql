-- Parámetros globales para cálculo de viáticos (combustible y rendimiento por vehículo).
-- Ejecutar una vez en la base (o reaplicar: INSERT es idempotente).

CREATE SCHEMA IF NOT EXISTS bsale;

CREATE TABLE IF NOT EXISTS bsale.config_viaticos (
    id                  SERIAL PRIMARY KEY,
    valor_combustible   NUMERIC,
    rendimiento_v1      NUMERIC,
    rendimiento_v2      NUMERIC,
    rendimiento_v3      NUMERIC,
    rendimiento_v4      NUMERIC
);

INSERT INTO bsale.config_viaticos (
    id,
    valor_combustible,
    rendimiento_v1,
    rendimiento_v2,
    rendimiento_v3,
    rendimiento_v4
)
VALUES (1, 1000, 10, 10, 10, 10)
ON CONFLICT (id) DO NOTHING;

-- Mantener la secuencia alineada si se insertó id = 1 explícitamente.
SELECT setval(
    pg_get_serial_sequence('bsale.config_viaticos', 'id'),
    COALESCE((SELECT MAX(id) FROM bsale.config_viaticos), 1)
);
