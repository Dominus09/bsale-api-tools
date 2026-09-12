-- Aplica coordenadas manuales confirmadas a la fuente maestra de clientes.
-- bsale.clients.lat / lon  (+ espejo operacional en rutero si existe la fila)
-- Idempotente. No toca otros campos del cliente.
--
-- IDs PLAN-00044:
--   3411 Casa Esquila
--   3851 Supermercado la michelada

BEGIN;

UPDATE bsale.clients
SET
    lat = -42.378674115374466,
    lon = -73.65026453890569,
    updated = CURRENT_TIMESTAMP
WHERE company_id = 3
  AND bsale_id = 3411;

UPDATE bsale.clients
SET
    lat = -42.32447079164018,
    lon = -73.56903424065618,
    updated = CURRENT_TIMESTAMP
WHERE company_id = 3
  AND bsale_id = 3851;

-- Casa Esquila está en rutero (id 336). Michelada no tiene fila rutero.
UPDATE bsale.rutero
SET
    lat = -42.378674115374466,
    lon = -73.65026453890569,
    lat_operacional = -42.378674115374466,
    lon_operacional = -73.65026453890569,
    georef_estado = CASE
        WHEN georef_estado IS NULL OR georef_estado = 'pendiente' THEN 'capturada'
        ELSE georef_estado
    END,
    georef_actualizada_at = clock_timestamp(),
    georef_actualizada_por = 'manual_coords_sql'
WHERE company_id = 3
  AND bsale_id = 3411;

COMMIT;

-- Validación rápida
SELECT bsale_id, nombre_fantasia, lat, lon
FROM bsale.clients
WHERE company_id = 3 AND bsale_id IN (3411, 3851)
ORDER BY bsale_id;
