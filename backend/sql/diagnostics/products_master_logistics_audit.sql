-- Auditoría maestro logístico (products_master, variants, products).
-- Ejecutar: psql "$DATABASE_URL" -f backend/sql/diagnostics/products_master_logistics_audit.sql

\echo '=== products_master ==='
SELECT COUNT(*)::bigint AS total_products_master
FROM bsale.products_master;

SELECT COUNT(*)::bigint AS con_barcode
FROM bsale.products_master
WHERE NULLIF(BTRIM(barcode), '') IS NOT NULL;

SELECT COUNT(*)::bigint AS con_units_per_box
FROM bsale.products_master
WHERE units_per_box IS NOT NULL AND units_per_box > 0;

SELECT COUNT(*)::bigint AS con_proveedor
FROM bsale.products_master
WHERE supplier_id IS NOT NULL;

SELECT COUNT(*)::bigint AS con_peso
FROM bsale.products_master
WHERE weight_box_kg IS NOT NULL AND weight_box_kg > 0;

SELECT COUNT(*)::bigint AS con_dimensiones_completas
FROM bsale.products_master
WHERE height_cm IS NOT NULL AND height_cm > 0
  AND width_cm IS NOT NULL AND width_cm > 0
  AND length_cm IS NOT NULL AND length_cm > 0;

SELECT COUNT(*)::bigint AS logistics_completed_true
FROM bsale.products_master
WHERE logistics_completed = TRUE;

\echo '=== variants ==='
SELECT COUNT(*)::bigint AS total_variants FROM bsale.variants;

SELECT COUNT(*)::bigint AS variants_con_barcode
FROM bsale.variants
WHERE NULLIF(BTRIM(bar_code), '') IS NOT NULL;

SELECT COUNT(*)::bigint AS variants_con_units_per_box
FROM bsale.variants
WHERE units_per_box IS NOT NULL AND units_per_box > 0;

\echo '=== products ==='
SELECT COUNT(*)::bigint AS total_products FROM bsale.products;

\echo '=== gaps PM vs variants (barcode) ==='
SELECT COUNT(DISTINCT BTRIM(v.bar_code))::bigint AS variants_sin_pm
FROM bsale.variants v
WHERE NULLIF(BTRIM(v.bar_code), '') IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM bsale.products_master pm
      WHERE pm.barcode = BTRIM(v.bar_code)
  );

SELECT COUNT(*)::bigint AS pm_sin_variant
FROM bsale.products_master pm
WHERE NULLIF(BTRIM(pm.barcode), '') IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM bsale.variants v
      WHERE BTRIM(v.bar_code) = pm.barcode
  );
