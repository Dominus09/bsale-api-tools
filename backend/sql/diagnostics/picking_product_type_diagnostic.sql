-- Diagnóstico categorías «Sin tipo» en picking producto
-- Reemplazar :plan_id por el id del dispatch plan

\echo '=== Resumen por tipo en snapshot ==='
SELECT
    COALESCE(NULLIF(BTRIM(tipo_producto), ''), 'VACÍO') AS tipo_snapshot,
    COUNT(*)::bigint AS lineas,
    COUNT(DISTINCT codigo_barras)::bigint AS skus
FROM distribuidora.dispatch_plan_picking_products
WHERE picking_id = (
    SELECT id FROM distribuidora.dispatch_plan_pickings
    WHERE plan_id = :plan_id
    ORDER BY version DESC LIMIT 1
)
GROUP BY 1
ORDER BY lineas DESC;

\echo '=== Líneas Sin tipo / vacío con causa en PM y Bsale ==='
SELECT
    pp.codigo_barras,
    LEFT(pp.producto, 40) AS producto,
    pp.tipo_producto AS tipo_snapshot,
    pm.product_type AS tipo_products_master,
    pt.name AS tipo_bsale_product_types
FROM distribuidora.dispatch_plan_picking_products pp
JOIN distribuidora.dispatch_plan_pickings pk ON pk.id = pp.picking_id
LEFT JOIN bsale.products_master pm ON pm.barcode = BTRIM(pp.codigo_barras)
LEFT JOIN bsale.variants v
    ON v.company_id = 3 AND BTRIM(v.bar_code) = BTRIM(pp.codigo_barras)
LEFT JOIN bsale.products p
    ON p.company_id = v.company_id AND p.bsale_id = v.product_id
LEFT JOIN bsale.product_types pt
    ON pt.company_id = p.company_id AND pt.bsale_id = p.product_type_id
WHERE pk.plan_id = :plan_id
  AND (
      pp.tipo_producto IS NULL
      OR BTRIM(pp.tipo_producto) = ''
      OR pp.tipo_producto = 'Sin tipo'
  )
ORDER BY pp.codigo_barras
LIMIT 100;
