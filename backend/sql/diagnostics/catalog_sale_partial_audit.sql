-- =============================================================================
-- Auditoría: configuración comercial PARCIAL y barcodes duplicados en PM.
-- Solo lectura. No modifica datos.
--
-- Ejecutar TODO el archivo en pgAdmin (F5).
-- Cada bloque es independiente (puede ejecutarse uno por uno si hace falta).
-- =============================================================================

-- Base reutilizable (macro CTE inline en cada consulta)
-- units_per_box_eff = SEC resuelto (columna o texto)

-- -----------------------------------------------------------------------------
-- 1) PARCIAL con quantity_step “extraño”
--    (no múltiplo de 5  O  quantity_step > 10)
-- -----------------------------------------------------------------------------
WITH pm_enriched AS (
    SELECT
        BTRIM(pm.barcode) AS barcode_trim,
        TRIM(COALESCE(pm.product_name, '') || ' ' || COALESCE(pm.variant_name, '')) AS name,
        pm.product_type,
        pm.sale_type,
        pm.quantity_step,
        COALESCE(
            NULLIF(pm.units_per_box, 0),
            NULLIF(v.units_per_box, 0),
            (
                regexp_match(
                    UPPER(COALESCE(v.description, pm.variant_name, '')),
                    'SEC[[:space:]]*([0-9]+)'
                )
            )[1]::integer
        ) AS units_per_box_eff
    FROM bsale.products_master pm
    LEFT JOIN bsale.variants v
        ON v.company_id = 3
       AND (
            pm.variant_id = v.bsale_id
            OR (
                NULLIF(BTRIM(pm.barcode), '') IS NOT NULL
                AND BTRIM(v.bar_code) = BTRIM(pm.barcode)
            )
       )
)
SELECT
    'partial_suspicious_step' AS reporte,
    barcode_trim AS barcode,
    name,
    units_per_box_eff AS units_per_box,
    sale_type,
    quantity_step,
    CASE
        WHEN quantity_step IS NULL THEN 'sin_step'
        WHEN quantity_step > 10 AND quantity_step % 5 <> 0 THEN 'gt10_y_no_multiplo_5'
        WHEN quantity_step > 10 THEN 'gt10'
        WHEN quantity_step % 5 <> 0 THEN 'no_multiplo_5'
    END AS motivo
FROM pm_enriched
WHERE sale_type = 'PARCIAL'
  AND (
        quantity_step IS NULL
        OR quantity_step % 5 <> 0
        OR quantity_step > 10
  )
ORDER BY units_per_box_eff DESC NULLS LAST, quantity_step DESC NULLS LAST, name;

-- -----------------------------------------------------------------------------
-- 2) Caso explícito SEC >= 41 (ej. SEC 50 → step 13 por ROUND(sec/4))
-- -----------------------------------------------------------------------------
WITH pm_enriched AS (
    SELECT
        BTRIM(pm.barcode) AS barcode_trim,
        TRIM(COALESCE(pm.product_name, '') || ' ' || COALESCE(pm.variant_name, '')) AS name,
        pm.sale_type,
        pm.quantity_step,
        COALESCE(
            NULLIF(pm.units_per_box, 0),
            NULLIF(v.units_per_box, 0),
            (
                regexp_match(
                    UPPER(COALESCE(v.description, pm.variant_name, '')),
                    'SEC[[:space:]]*([0-9]+)'
                )
            )[1]::integer
        ) AS units_per_box_eff
    FROM bsale.products_master pm
    LEFT JOIN bsale.variants v
        ON v.company_id = 3
       AND (
            pm.variant_id = v.bsale_id
            OR (
                NULLIF(BTRIM(pm.barcode), '') IS NOT NULL
                AND BTRIM(v.bar_code) = BTRIM(pm.barcode)
            )
       )
)
SELECT
    'partial_sec_ge_41' AS reporte,
    barcode_trim AS barcode,
    name,
    units_per_box_eff AS units_per_box,
    sale_type,
    quantity_step,
    ROUND(units_per_box_eff / 4.0)::integer AS step_formula_else_seed
FROM pm_enriched
WHERE sale_type = 'PARCIAL'
  AND units_per_box_eff >= 41
ORDER BY units_per_box_eff DESC, name;

-- -----------------------------------------------------------------------------
-- 3) Todos los PARCIAL (referencia completa)
-- -----------------------------------------------------------------------------
WITH pm_enriched AS (
    SELECT
        BTRIM(pm.barcode) AS barcode_trim,
        TRIM(COALESCE(pm.product_name, '') || ' ' || COALESCE(pm.variant_name, '')) AS name,
        pm.product_type,
        pm.sale_type,
        pm.quantity_step,
        COALESCE(
            NULLIF(pm.units_per_box, 0),
            NULLIF(v.units_per_box, 0),
            (
                regexp_match(
                    UPPER(COALESCE(v.description, pm.variant_name, '')),
                    'SEC[[:space:]]*([0-9]+)'
                )
            )[1]::integer
        ) AS units_per_box_eff
    FROM bsale.products_master pm
    LEFT JOIN bsale.variants v
        ON v.company_id = 3
       AND (
            pm.variant_id = v.bsale_id
            OR (
                NULLIF(BTRIM(pm.barcode), '') IS NOT NULL
                AND BTRIM(v.bar_code) = BTRIM(pm.barcode)
            )
       )
)
SELECT
    'partial_all' AS reporte,
    barcode_trim AS barcode,
    name,
    units_per_box_eff AS units_per_box,
    sale_type,
    quantity_step
FROM pm_enriched
WHERE sale_type = 'PARCIAL'
ORDER BY product_type, units_per_box_eff, name;

-- -----------------------------------------------------------------------------
-- 4) Barcodes duplicados en products_master
-- -----------------------------------------------------------------------------
SELECT
    'duplicate_barcodes' AS reporte,
    BTRIM(barcode) AS barcode,
    COUNT(*)::bigint AS cantidad_registros,
    STRING_AGG(
        DISTINCT TRIM(COALESCE(product_name, '') || ' ' || COALESCE(variant_name, '')),
        ' | '
    ) AS productos_involucrados
FROM bsale.products_master
WHERE NULLIF(BTRIM(barcode), '') IS NOT NULL
GROUP BY BTRIM(barcode)
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC, BTRIM(barcode);
