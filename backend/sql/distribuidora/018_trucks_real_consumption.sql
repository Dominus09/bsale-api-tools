-- Rendimientos reales km/L por unidad (operación Quillotana).

UPDATE distribuidora.trucks
SET km_per_liter = 7.0, fuel_type = 'diesel'
WHERE TRIM(name) ILIKE 'Hyundai';
-- +go

UPDATE distribuidora.trucks
SET km_per_liter = 4.2, fuel_type = 'diesel'
WHERE TRIM(name) ILIKE 'Hino 2';
-- +go

UPDATE distribuidora.trucks
SET km_per_liter = 4.2, fuel_type = 'diesel'
WHERE TRIM(name) ILIKE 'Hino 3';
-- +go

UPDATE distribuidora.trucks
SET km_per_liter = 3.5, fuel_type = 'diesel'
WHERE TRIM(name) ILIKE 'Hino 4';
-- +go

UPDATE distribuidora.trucks
SET km_per_liter = 2.8, fuel_type = 'diesel'
WHERE TRIM(name) ILIKE 'Hino 5';
-- +go

-- Alta opcional si aún no existe en flota activa.
INSERT INTO distribuidora.trucks (name, plate, max_weight_kg, km_per_liter, fuel_type, active)
VALUES ('Hino 5', 'HINO5-PL', 7600, 2.8, 'diesel', TRUE)
ON CONFLICT (plate) DO UPDATE
SET
    name = EXCLUDED.name,
    km_per_liter = EXCLUDED.km_per_liter,
    fuel_type = EXCLUDED.fuel_type,
    max_weight_kg = EXCLUDED.max_weight_kg;
-- +go
