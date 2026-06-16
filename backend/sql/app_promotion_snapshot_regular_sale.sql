-- Snapshot congelado: regular_price (ANTES) y sale_price (AHORA).
-- Idempotente: agrega columnas y rellena desde precio_normal / precio_oferta si existían.

ALTER TABLE app.promotion_price_snapshot
    ADD COLUMN IF NOT EXISTS regular_price NUMERIC(12, 2),
    ADD COLUMN IF NOT EXISTS sale_price NUMERIC(12, 2);

UPDATE app.promotion_price_snapshot
SET
    regular_price = COALESCE(regular_price, precio_normal),
    sale_price = COALESCE(sale_price, precio_oferta)
WHERE regular_price IS NULL
   OR sale_price IS NULL;

COMMENT ON COLUMN app.promotion_price_snapshot.regular_price IS
    'Precio lista congelado al crear la promoción (ANTES). No se recalcula.';
COMMENT ON COLUMN app.promotion_price_snapshot.sale_price IS
    'Precio promocional congelado (AHORA). Editable sin tocar regular_price.';
