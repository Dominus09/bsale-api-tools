-- Trazabilidad picking: barcode EAN 7802100501196 (HEINEKEN / CERVEZAS)
-- Ejecutar en PG para auditar categoría, nombre y código.

\echo '=== variants (company 3) ==='
SELECT
    v.bsale_id,
    v.product_id,
    v.code AS sku_interno,
    v.bar_code AS ean,
    v.description AS variante,
    v.units_per_box,
    p.name AS producto_bsale,
    pt.name AS tipo_bsale
FROM bsale.variants v
LEFT JOIN bsale.products p
    ON p.company_id = v.company_id AND p.bsale_id = v.product_id
LEFT JOIN bsale.product_types pt
    ON pt.company_id = p.company_id AND pt.bsale_id = p.product_type_id
WHERE v.company_id = 3
  AND (
      BTRIM(v.bar_code) = '7802100501196'
      OR BTRIM(v.code) = '7802100501196'
  );

\echo '=== products_master ==='
SELECT barcode, sku, product_name, variant_name, product_type, variant_id
FROM bsale.products_master
WHERE barcode = '7802100501196';

\echo '=== document_details (muestra SKU vs EAN) ==='
SELECT
    dd.variant_id,
    dd.variant_code AS guardado_como_variant_code,
    dd.variant_description,
    v.bar_code AS ean_real,
    v.code AS sku_real
FROM distribuidora.document_details dd
LEFT JOIN bsale.variants v
    ON v.company_id = 3 AND v.bsale_id = dd.variant_id
WHERE BTRIM(dd.variant_code) = '7802100501196'
   OR v.bar_code = '7802100501196'
LIMIT 20;

\echo '=== snapshot picking (último por barcode) ==='
SELECT
    pp.tipo_producto,
    pp.producto,
    pp.variante,
    pp.codigo_barras,
    pp.variant_id,
    pp.unidades,
    pp.cajas,
    pp.units_per_box
FROM distribuidora.dispatch_plan_picking_products pp
WHERE BTRIM(pp.codigo_barras) = '7802100501196'
   OR pp.variant_id IN (
       SELECT bsale_id FROM bsale.variants
       WHERE company_id = 3 AND BTRIM(bar_code) = '7802100501196'
   )
ORDER BY pp.id DESC
LIMIT 5;
