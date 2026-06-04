-- =============================================================================
-- Diagnóstico flujo productos Bsale (auditoría 2026-06)
-- Ejecutar en PostgreSQL producción. Ajustar company_id (default 3 = Quillotana).
-- =============================================================================

\set company_id 3

-- -----------------------------------------------------------------------------
-- B. Conteos por tabla
-- -----------------------------------------------------------------------------
SELECT 'products' AS tabla, COUNT(*) AS n FROM bsale.products
UNION ALL SELECT 'variants', COUNT(*) FROM bsale.variants
UNION ALL SELECT 'products_master', COUNT(*) FROM bsale.products_master
UNION ALL SELECT 'variants_con_barcode', COUNT(*) FROM bsale.variants
  WHERE NULLIF(BTRIM(bar_code), '') IS NOT NULL;

SELECT
    :company_id AS company_id,
    (SELECT COUNT(*) FROM bsale.products WHERE company_id = :company_id) AS products,
    (SELECT COUNT(*) FROM bsale.variants WHERE company_id = :company_id) AS variants,
    (SELECT COUNT(*) FROM bsale.variants WHERE company_id = :company_id
       AND NULLIF(BTRIM(bar_code), '') IS NOT NULL) AS variants_barcode;

-- Frescura products_master
SELECT
    COUNT(*) AS filas,
    MIN(updated_at) AS min_updated_at,
    MAX(updated_at) AS max_updated_at
FROM bsale.products_master;

SELECT DATE_TRUNC('month', updated_at) AS mes, COUNT(*) AS filas
FROM bsale.products_master
GROUP BY 1 ORDER BY 1 DESC;

-- Indirectos sync (si existen columnas)
SELECT MAX(last_update) AS ultimo_variant_cost FROM bsale.variant_cost;
-- SELECT MAX(updated_at) FROM bsale.stocks;  -- descomentar si existe columna

-- Catálogo (fallará si la vista no existe en el entorno)
-- SELECT COUNT(*) AS catalog_view_rows FROM bsale.catalog_view;

-- -----------------------------------------------------------------------------
-- C.1 Variantes sincronizadas sin fila en products_master
-- -----------------------------------------------------------------------------
SELECT
    v.company_id,
    v.bsale_id AS variant_id,
    BTRIM(v.bar_code) AS barcode,
    p.name AS product_name,
    v.description AS variant_name
FROM bsale.variants v
INNER JOIN bsale.products p
    ON p.company_id = v.company_id AND p.bsale_id = v.product_id
WHERE v.company_id = :company_id
  AND NULLIF(BTRIM(v.bar_code), '') IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM bsale.products_master pm WHERE pm.barcode = BTRIM(v.bar_code)
  )
ORDER BY v.bsale_id DESC
LIMIT 200;

-- -----------------------------------------------------------------------------
-- C.2 products_master huérfanos (sin variant con mismo barcode)
-- -----------------------------------------------------------------------------
SELECT pm.barcode, pm.product_name, pm.variant_name, pm.updated_at
FROM bsale.products_master pm
WHERE NOT EXISTS (
    SELECT 1 FROM bsale.variants v
    WHERE v.company_id = :company_id AND BTRIM(v.bar_code) = pm.barcode
);

-- -----------------------------------------------------------------------------
-- C.3 Catálogo web sin products_master
-- -----------------------------------------------------------------------------
-- SELECT cv.variant_id, cv.barcode, cv.product, cv.variant
-- FROM bsale.catalog_view cv
-- WHERE NULLIF(BTRIM(cv.barcode), '') IS NOT NULL
--   AND NOT EXISTS (SELECT 1 FROM bsale.products_master pm WHERE pm.barcode = BTRIM(cv.barcode))
-- ORDER BY cv.product LIMIT 200;

-- -----------------------------------------------------------------------------
-- C.5 Nombres desactualizados en PM vs variants/products
-- -----------------------------------------------------------------------------
SELECT
    pm.barcode,
    pm.product_name AS pm_producto,
    p.name AS actual_producto,
    pm.variant_name AS pm_variante,
    v.description AS actual_variante,
    pm.updated_at
FROM bsale.products_master pm
JOIN bsale.variants v ON v.company_id = :company_id AND BTRIM(v.bar_code) = pm.barcode
JOIN bsale.products p ON p.company_id = v.company_id AND p.bsale_id = v.product_id
WHERE pm.product_name IS DISTINCT FROM p.name
   OR pm.variant_name IS DISTINCT FROM v.description
ORDER BY pm.updated_at ASC NULLS FIRST
LIMIT 100;
