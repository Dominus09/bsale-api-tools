-- Carga masiva de proveedor por barcode en bsale.products_master
-- Formato esperado en staging: barcode | supplier_id
-- No inserta productos nuevos; solo actualiza coincidencias existentes.

BEGIN;

CREATE TEMP TABLE temp_supplier_upload (
    barcode TEXT,
    supplier_id INTEGER
) ON COMMIT DROP;

-- Cargar datos aquí (ejemplo):
-- INSERT INTO temp_supplier_upload (barcode, supplier_id) VALUES
-- ('7801234567890', 10),
-- ('7800000000001', 12);

UPDATE bsale.products_master pm
SET supplier_id = t.supplier_id,
    updated_at = NOW()
FROM temp_supplier_upload t
WHERE pm.barcode = t.barcode;

COMMIT;
