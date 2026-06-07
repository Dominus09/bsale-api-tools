-- Vendedor asignado y ciudad del cliente en pedidos web (app.orders).
-- Ejecutar una vez en PostgreSQL. No modifica bsale ni distribuidora.

ALTER TABLE app.orders ADD COLUMN IF NOT EXISTS seller_id INTEGER;
ALTER TABLE app.orders ADD COLUMN IF NOT EXISTS seller_name TEXT;
ALTER TABLE app.orders ADD COLUMN IF NOT EXISTS client_city TEXT;
