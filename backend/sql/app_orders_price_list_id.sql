-- ID numérico Bsale de la lista de precios usada en el pedido web (13/14/16).
-- Ejecutar una vez en PostgreSQL. No modifica bsale ni distribuidora.

ALTER TABLE app.orders ADD COLUMN IF NOT EXISTS price_list_id INTEGER;
