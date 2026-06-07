-- OPCIONAL: el backend deriva price_list_id (13/14/16) desde app.orders.price_list
-- en lectura/respuesta API; esta columna no es requerida para GET/POST /orders.
-- Ejecutar solo si se desea persistir el ID en BD además del slug.

ALTER TABLE app.orders ADD COLUMN IF NOT EXISTS price_list_id INTEGER;
