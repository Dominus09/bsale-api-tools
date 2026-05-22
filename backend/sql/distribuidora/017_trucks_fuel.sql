-- Consumo y tipo de combustible por camión (costo ruta ORS).

ALTER TABLE distribuidora.trucks
    ADD COLUMN IF NOT EXISTS km_per_liter NUMERIC(6, 2),
    ADD COLUMN IF NOT EXISTS fuel_type TEXT NOT NULL DEFAULT 'diesel';
-- +go

-- Valores provisionales si 018 aún no corrió (sobrescritos por 018_trucks_real_consumption.sql).
UPDATE distribuidora.trucks
SET
    km_per_liter = CASE
        WHEN TRIM(name) ILIKE 'Hyundai' THEN 7.0
        WHEN TRIM(name) ILIKE 'Hino 2' THEN 4.2
        WHEN TRIM(name) ILIKE 'Hino 3' THEN 4.2
        WHEN TRIM(name) ILIKE 'Hino 4' THEN 3.5
        WHEN TRIM(name) ILIKE 'Hino 5' THEN 2.8
        ELSE 8.0
    END,
    fuel_type = 'diesel'
WHERE km_per_liter IS NULL;
-- +go

ALTER TABLE distribuidora.trucks
    ALTER COLUMN km_per_liter SET DEFAULT 8.0;
-- +go

COMMENT ON COLUMN distribuidora.trucks.km_per_liter IS
    'Rendimiento km/L para estimar combustible en rutas ORS.';
COMMENT ON COLUMN distribuidora.trucks.fuel_type IS
    'Tipo combustible (diesel, gasoline); diesel usa diesel_price_per_liter.';
