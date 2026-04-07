-- Marca de última escritura por fila de stock (sync_stock.py).
-- Ejecutar una vez antes de usar GET /purchase-data-freshness.

ALTER TABLE bsale.stocks
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();

UPDATE bsale.stocks SET updated_at = NOW() WHERE updated_at IS NULL;
