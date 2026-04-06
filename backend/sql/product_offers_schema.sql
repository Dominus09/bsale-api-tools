-- Tabla de ofertas/remates por código de barras.
-- El historial se conserva: se permiten múltiples filas por barcode.

CREATE TABLE IF NOT EXISTS bsale.product_offers (
    id         SERIAL PRIMARY KEY,
    barcode    TEXT NOT NULL,
    offer_type TEXT NOT NULL,
    status     TEXT NOT NULL,
    start_date DATE,
    end_date   DATE,
    reason     TEXT,
    notes      TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_product_offers_barcode
    ON bsale.product_offers (barcode);

CREATE INDEX IF NOT EXISTS idx_product_offers_status
    ON bsale.product_offers (status);

CREATE INDEX IF NOT EXISTS idx_product_offers_offer_type
    ON bsale.product_offers (offer_type);

-- Vista de ofertas activas en ventana vigente.
CREATE OR REPLACE VIEW bsale.active_offers_view AS
SELECT
    po.id,
    po.barcode,
    po.offer_type,
    po.status,
    po.start_date,
    po.end_date,
    po.reason,
    po.notes,
    po.created_at,
    po.updated_at
FROM bsale.product_offers po
WHERE po.status = 'activa'
  AND CURRENT_DATE BETWEEN po.start_date AND po.end_date;
