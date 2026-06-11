-- =============================================================================
-- Auditoría: duplicados comerciales en catálogo (mismo barcode, distinta regla).
-- Replica la query de GET /api/catalog (backend/routers/catalog.py).
-- Solo lectura.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 0) Definición de la query API (referencia)
-- FROM bsale.catalog_view cv
-- LEFT JOIN bsale.variants v ON v.company_id = 3 AND v.bsale_id = cv.variant_id
-- LEFT JOIN bsale.products_master pm
--   ON pm.variant_id = cv.variant_id
--   OR (BTRIM(cv.bar_code) = pm.barcode)   <-- OR puede multiplicar filas
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- 1) Barcodes duplicados en catalog_view (múltiples variant_id)
-- -----------------------------------------------------------------------------
SELECT
    '1_catalog_view_dup_barcode' AS reporte,
    BTRIM(bar_code) AS barcode,
    COUNT(*)::bigint AS filas_catalog_view,
    COUNT(DISTINCT variant_id)::bigint AS variant_ids_distintos,
    STRING_AGG(DISTINCT variant_id::text, ', ' ORDER BY variant_id::text) AS variant_ids,
    STRING_AGG(DISTINCT TRIM(product || ' ' || COALESCE(variant, '')), ' | ') AS nombres
FROM bsale.catalog_view
WHERE NULLIF(BTRIM(bar_code), '') IS NOT NULL
GROUP BY 2
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC, 2;

-- -----------------------------------------------------------------------------
-- 2) Barcodes duplicados en products_master (violación lógica UNIQUE)
-- -----------------------------------------------------------------------------
SELECT
    '2_products_master_dup_barcode' AS reporte,
    BTRIM(barcode) AS barcode,
    COUNT(*)::bigint AS cantidad_registros,
    STRING_AGG(id::text, ', ' ORDER BY id) AS pm_ids,
    STRING_AGG(COALESCE(variant_id::text, 'null'), ', ') AS variant_ids,
    STRING_AGG(COALESCE(sale_type, 'null'), ', ') AS sale_types,
    STRING_AGG(COALESCE(quantity_step::text, 'null'), ', ') AS quantity_steps
FROM bsale.products_master
WHERE NULLIF(BTRIM(barcode), '') IS NOT NULL
GROUP BY 2
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC, 2;

-- -----------------------------------------------------------------------------
-- 3) bar_code duplicado en variants (empresa 3)
-- -----------------------------------------------------------------------------
SELECT
    '3_variants_dup_barcode' AS reporte,
    BTRIM(bar_code) AS barcode,
    COUNT(*)::bigint AS filas_variants,
    COUNT(DISTINCT bsale_id)::bigint AS variant_ids_distintos,
    STRING_AGG(DISTINCT bsale_id::text, ', ' ORDER BY bsale_id::text) AS variant_ids,
    STRING_AGG(DISTINCT LEFT(COALESCE(description, ''), 80), ' | ') AS descriptions
FROM bsale.variants
WHERE company_id = 3
  AND NULLIF(BTRIM(bar_code), '') IS NOT NULL
GROUP BY 2
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC, 2;

-- -----------------------------------------------------------------------------
-- 4) Simulación exacta GET /api/catalog — filas API por barcode
--    (misma query que catalog.py; detecta multiplicación por JOIN OR)
-- -----------------------------------------------------------------------------
WITH api_rows AS (
    SELECT
        cv.variant_id AS id,
        BTRIM(cv.bar_code) AS barcode,
        TRIM(cv.product || ' ' || COALESCE(cv.variant, '')) AS name,
        cv.product_type AS type,
        pm.id AS pm_id,
        pm.variant_id AS pm_variant_id,
        pm.sale_type AS pm_sale_type,
        pm.quantity_step AS pm_quantity_step,
        COALESCE(
            NULLIF(v.units_per_box, 0),
            NULLIF(pm.units_per_box, 0),
            (regexp_match(UPPER(COALESCE(v.description, '')),
                          'SEC[[:space:]]*([0-9]+)'))[1]::integer
        ) AS units_per_box,
        CASE
            WHEN pm.variant_id = cv.variant_id
             AND BTRIM(pm.barcode) = BTRIM(cv.bar_code)
                THEN 'join:variant_id_y_barcode'
            WHEN pm.variant_id = cv.variant_id
                THEN 'join:solo_variant_id'
            WHEN BTRIM(pm.barcode) = BTRIM(cv.bar_code)
                THEN 'join:solo_barcode'
            ELSE 'join:otro'
        END AS pm_match_tipo
    FROM bsale.catalog_view cv
    LEFT JOIN bsale.variants v
        ON v.company_id = 3
       AND v.bsale_id = cv.variant_id
    LEFT JOIN bsale.products_master pm
        ON pm.variant_id = cv.variant_id
        OR (
            NULLIF(BTRIM(cv.bar_code), '') IS NOT NULL
            AND pm.barcode = BTRIM(cv.bar_code)
        )
)
SELECT
    '4_api_rows_por_barcode' AS reporte,
    barcode,
    COUNT(*)::bigint AS filas_api_simuladas,
    COUNT(DISTINCT id)::bigint AS catalog_variant_ids,
    COUNT(DISTINCT pm_id)::bigint AS pm_ids_distintos,
    STRING_AGG(DISTINCT id::text, ', ') AS variant_ids,
    STRING_AGG(DISTINCT COALESCE(pm_sale_type, 'null') || '/' || COALESCE(pm_quantity_step::text, 'null'), ' | ') AS reglas_comerciales,
    STRING_AGG(DISTINCT pm_match_tipo, ' | ') AS tipos_join
FROM api_rows
WHERE NULLIF(barcode, '') IS NOT NULL
GROUP BY barcode
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC, barcode;

-- -----------------------------------------------------------------------------
-- 5) Detalle barcodes problemáticos (log temporal equivalente)
--    Incluye ejemplos 7809562401330 y 7809562401293 si existen
-- -----------------------------------------------------------------------------
WITH api_rows AS (
    SELECT
        cv.variant_id AS id,
        BTRIM(cv.bar_code) AS barcode,
        TRIM(cv.product || ' ' || COALESCE(cv.variant, '')) AS name,
        pm.id AS pm_id,
        pm.sale_type,
        pm.quantity_step,
        COALESCE(
            NULLIF(v.units_per_box, 0),
            NULLIF(pm.units_per_box, 0),
            (regexp_match(UPPER(COALESCE(v.description, '')),
                          'SEC[[:space:]]*([0-9]+)'))[1]::integer
        ) AS units_per_box,
        CASE
            WHEN pm.variant_id = cv.variant_id THEN TRUE
            ELSE FALSE
        END AS match_por_variant_id,
        CASE
            WHEN BTRIM(pm.barcode) = BTRIM(cv.bar_code) THEN TRUE
            ELSE FALSE
        END AS match_por_barcode
    FROM bsale.catalog_view cv
    LEFT JOIN bsale.variants v
        ON v.company_id = 3
       AND v.bsale_id = cv.variant_id
    LEFT JOIN bsale.products_master pm
        ON pm.variant_id = cv.variant_id
        OR (
            NULLIF(BTRIM(cv.bar_code), '') IS NOT NULL
            AND pm.barcode = BTRIM(cv.bar_code)
        )
)
SELECT
    '5_detalle_duplicados' AS reporte,
    barcode,
    id AS variant_id,
    name,
    pm_id,
    sale_type,
    quantity_step,
    units_per_box,
    match_por_variant_id,
    match_por_barcode,
    CASE
        WHEN sale_type = 'PARCIAL' AND quantity_step IS NOT NULL
            THEN 'UI: Mínimo ' || quantity_step::text || ' unidades'
        WHEN sale_type = 'ENTERA' AND units_per_box IS NOT NULL
            THEN 'UI: Caja x ' || units_per_box::text || ' unidades'
        WHEN sale_type = 'UNITARIO'
            THEN 'UI: Unidad libre'
        ELSE 'UI: sin etiqueta clara'
    END AS etiqueta_ui_esperada
FROM api_rows
WHERE barcode IN (
    SELECT barcode
    FROM api_rows
    WHERE NULLIF(barcode, '') IS NOT NULL
    GROUP BY barcode
    HAVING COUNT(*) > 1
)
   OR barcode IN ('7809562401330', '7809562401293')
ORDER BY barcode, id, pm_id;

-- -----------------------------------------------------------------------------
-- 6) JOIN OR: filas donde variant_id y barcode apuntan a PM distintos
-- -----------------------------------------------------------------------------
WITH api_rows AS (
    SELECT
        cv.variant_id,
        BTRIM(cv.bar_code) AS barcode,
        pm.id AS pm_id,
        pm.variant_id AS pm_variant_id,
        pm.barcode AS pm_barcode,
        pm.sale_type,
        pm.quantity_step
    FROM bsale.catalog_view cv
    LEFT JOIN bsale.variants v
        ON v.company_id = 3
       AND v.bsale_id = cv.variant_id
    LEFT JOIN bsale.products_master pm
        ON pm.variant_id = cv.variant_id
        OR (
            NULLIF(BTRIM(cv.bar_code), '') IS NOT NULL
            AND pm.barcode = BTRIM(cv.bar_code)
        )
)
SELECT
    '6_join_or_pm_distintos' AS reporte,
    variant_id,
    barcode,
    COUNT(*)::bigint AS filas_generadas,
    COUNT(DISTINCT pm_id)::bigint AS pm_distintos,
    STRING_AGG(DISTINCT pm_id::text || ' (v=' || COALESCE(pm_variant_id::text, '?') || ',b=' || COALESCE(pm_barcode, '?') || ',st=' || COALESCE(sale_type, '?') || ',step=' || COALESCE(quantity_step::text, '?') || ')', ' | ') AS pm_matches
FROM api_rows
WHERE pm_id IS NOT NULL
GROUP BY variant_id, barcode
HAVING COUNT(DISTINCT pm_id) > 1
ORDER BY COUNT(*) DESC, barcode;

-- -----------------------------------------------------------------------------
-- 7) Resumen: cantidad exacta de barcodes duplicados en salida API simulada
-- -----------------------------------------------------------------------------
WITH api_rows AS (
    SELECT BTRIM(cv.bar_code) AS barcode
    FROM bsale.catalog_view cv
    LEFT JOIN bsale.variants v
        ON v.company_id = 3
       AND v.bsale_id = cv.variant_id
    LEFT JOIN bsale.products_master pm
        ON pm.variant_id = cv.variant_id
        OR (
            NULLIF(BTRIM(cv.bar_code), '') IS NOT NULL
            AND pm.barcode = BTRIM(cv.bar_code)
        )
    WHERE NULLIF(BTRIM(cv.bar_code), '') IS NOT NULL
),
dup AS (
    SELECT barcode
    FROM api_rows
    GROUP BY barcode
    HAVING COUNT(*) > 1
)
SELECT
    '7_resumen' AS reporte,
    (SELECT COUNT(DISTINCT barcode) FROM dup) AS barcodes_duplicados_en_api,
    (SELECT COUNT(*) FROM api_rows ar INNER JOIN dup d ON d.barcode = ar.barcode) AS filas_api_extra_totales,
    (SELECT COUNT(DISTINCT barcode) FROM bsale.catalog_view WHERE NULLIF(BTRIM(bar_code), '') IS NOT NULL) AS barcodes_unicos_catalog_view,
    (SELECT COUNT(*) FROM bsale.catalog_view) AS filas_catalog_view_total;

-- =============================================================================
-- BLOQUE B: traza company_id + barcode puntual (ej. 7809562401293)
-- Replica exacta GET /api/catalog con columnas de diagnóstico.
-- Cambiar :barcode si se audita otro código.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 8) Query EXACTA API para un barcode — filas que construyen Product en JSON
--     company_id en variants: filtro EN el ON (antes de evaluar columnas de v).
--     products_master: SIN filtro company_id.
-- -----------------------------------------------------------------------------
WITH target AS (
    SELECT '7809562401293'::text AS barcode
)
SELECT
    '8_api_exacta_por_barcode' AS reporte,
    cv.variant_id AS api_id,
    BTRIM(cv.bar_code) AS barcode,
    TRIM(cv.product || ' ' || COALESCE(cv.variant, '')) AS api_name,
    v.company_id AS v_company_id,
    v.bsale_id AS v_bsale_id,
    pm.id AS pm_id,
    pm.variant_id AS pm_variant_id,
    BTRIM(pm.barcode) AS pm_barcode,
    pm.sale_type,
    pm.quantity_step,
    COALESCE(
        NULLIF(v.units_per_box, 0),
        NULLIF(pm.units_per_box, 0),
        (regexp_match(UPPER(COALESCE(v.description, '')),
                      'SEC[[:space:]]*([0-9]+)'))[1]::integer
    ) AS units_per_box,
    CASE
        WHEN pm.variant_id = cv.variant_id
         AND BTRIM(pm.barcode) = BTRIM(cv.bar_code)
            THEN 'join:variant_id_y_barcode'
        WHEN pm.variant_id = cv.variant_id
            THEN 'join:solo_variant_id'
        WHEN BTRIM(pm.barcode) = BTRIM(cv.bar_code)
            THEN 'join:solo_barcode'
        ELSE 'join:ninguno'
    END AS pm_match_tipo,
    CASE
        WHEN pm.sale_type = 'PARCIAL' AND pm.quantity_step IS NOT NULL
            THEN 'UI: Mínimo ' || pm.quantity_step::text || ' unidades'
        WHEN COALESCE(pm.sale_type, 'ENTERA') = 'ENTERA'
         AND COALESCE(
                NULLIF(v.units_per_box, 0),
                NULLIF(pm.units_per_box, 0),
                (regexp_match(UPPER(COALESCE(v.description, '')),
                              'SEC[[:space:]]*([0-9]+)'))[1]::integer
            ) IS NOT NULL
            THEN 'UI: Caja x ' || COALESCE(
                NULLIF(v.units_per_box, 0),
                NULLIF(pm.units_per_box, 0),
                (regexp_match(UPPER(COALESCE(v.description, '')),
                              'SEC[[:space:]]*([0-9]+)'))[1]::integer
            )::text || ' unidades'
        ELSE 'UI: otra'
    END AS etiqueta_ui_esperada
FROM bsale.catalog_view cv
CROSS JOIN target t
LEFT JOIN bsale.variants v
    ON v.company_id = 3
   AND v.bsale_id = cv.variant_id
LEFT JOIN bsale.products_master pm
    ON pm.variant_id = cv.variant_id
    OR (
        NULLIF(BTRIM(cv.bar_code), '') IS NOT NULL
        AND pm.barcode = BTRIM(cv.bar_code)
    )
WHERE BTRIM(cv.bar_code) = t.barcode
ORDER BY cv.variant_id, pm.id;

-- -----------------------------------------------------------------------------
-- 9) Conteo filas API para el barcode (respuesta directa a "cuántas filas")
-- -----------------------------------------------------------------------------
WITH target AS (
    SELECT '7809562401293'::text AS barcode
),
api_rows AS (
    SELECT
        cv.variant_id,
        BTRIM(cv.bar_code) AS barcode,
        v.company_id AS v_company_id,
        pm.id AS pm_id,
        pm.sale_type,
        pm.quantity_step
    FROM bsale.catalog_view cv
    CROSS JOIN target t
    LEFT JOIN bsale.variants v
        ON v.company_id = 3
       AND v.bsale_id = cv.variant_id
    LEFT JOIN bsale.products_master pm
        ON pm.variant_id = cv.variant_id
        OR (
            NULLIF(BTRIM(cv.bar_code), '') IS NOT NULL
            AND pm.barcode = BTRIM(cv.bar_code)
        )
    WHERE BTRIM(cv.bar_code) = t.barcode
)
SELECT
    '9_conteo_api_barcode' AS reporte,
    barcode,
    COUNT(*)::bigint AS filas_api_total,
    COUNT(DISTINCT variant_id)::bigint AS catalog_variant_ids,
    COUNT(DISTINCT pm_id)::bigint AS pm_ids_distintos,
    STRING_AGG(DISTINCT COALESCE(v_company_id::text, 'null'), ', ') AS v_company_ids_vistos,
    STRING_AGG(DISTINCT COALESCE(sale_type, 'null') || '/' || COALESCE(quantity_step::text, 'null'), ' | ') AS reglas_pm
FROM api_rows
GROUP BY barcode;

-- -----------------------------------------------------------------------------
-- 10) variants: todas las empresas con ese barcode (1 y 3 esperadas)
--      La API solo usa company_id=3 en el ON del JOIN.
-- -----------------------------------------------------------------------------
SELECT
    '10_variants_todas_empresas' AS reporte,
    v.company_id,
    v.bsale_id AS variant_id,
    BTRIM(v.bar_code) AS barcode,
    v.units_per_box,
    LEFT(COALESCE(v.description, ''), 100) AS description
FROM bsale.variants v
WHERE BTRIM(v.bar_code) = '7809562401293'
ORDER BY v.company_id, v.bsale_id;

-- -----------------------------------------------------------------------------
-- 11) products_master: filas que tocan ese barcode o variant_id del catálogo
-- -----------------------------------------------------------------------------
WITH cv_ids AS (
    SELECT DISTINCT variant_id
    FROM bsale.catalog_view
    WHERE BTRIM(bar_code) = '7809562401293'
)
SELECT
    '11_products_master_relacionados' AS reporte,
    pm.id AS pm_id,
    pm.variant_id AS pm_variant_id,
    BTRIM(pm.barcode) AS pm_barcode,
    pm.sale_type,
    pm.quantity_step,
    pm.units_per_box,
    pm.product_name,
    CASE
        WHEN pm.variant_id IN (SELECT variant_id FROM cv_ids) THEN TRUE
        ELSE FALSE
    END AS pm_variant_en_catalog_view,
    CASE
        WHEN BTRIM(pm.barcode) = '7809562401293' THEN TRUE
        ELSE FALSE
    END AS pm_barcode_match
FROM bsale.products_master pm
WHERE BTRIM(pm.barcode) = '7809562401293'
   OR pm.variant_id IN (SELECT variant_id FROM cv_ids)
ORDER BY pm.id;

-- -----------------------------------------------------------------------------
-- 12) catalog_view: filas base antes de cualquier JOIN
-- -----------------------------------------------------------------------------
SELECT
    '12_catalog_view_filas' AS reporte,
    variant_id,
    BTRIM(bar_code) AS barcode,
    TRIM(product || ' ' || COALESCE(variant, '')) AS name,
    product_type,
    COALESCE(stock, 0) AS stock
FROM bsale.catalog_view
WHERE BTRIM(bar_code) = '7809562401293'
ORDER BY variant_id;

-- -----------------------------------------------------------------------------
-- 13) Log temporal: barcode + company_id + sale_type + quantity_step
--     (una fila por fila API simulada)
-- -----------------------------------------------------------------------------
WITH target AS (
    SELECT '7809562401293'::text AS barcode
)
SELECT
    '13_log_temporal' AS reporte,
    BTRIM(cv.bar_code) AS barcode,
    v.company_id,
    pm.sale_type,
    pm.quantity_step,
    cv.variant_id AS api_id,
    pm.id AS pm_id,
    pm_match.pm_match_tipo
FROM bsale.catalog_view cv
CROSS JOIN target t
LEFT JOIN bsale.variants v
    ON v.company_id = 3
   AND v.bsale_id = cv.variant_id
LEFT JOIN bsale.products_master pm
    ON pm.variant_id = cv.variant_id
    OR (
        NULLIF(BTRIM(cv.bar_code), '') IS NOT NULL
        AND pm.barcode = BTRIM(cv.bar_code)
    )
CROSS JOIN LATERAL (
    SELECT CASE
        WHEN pm.variant_id = cv.variant_id
         AND BTRIM(pm.barcode) = BTRIM(cv.bar_code)
            THEN 'variant_id_y_barcode'
        WHEN pm.variant_id = cv.variant_id
            THEN 'solo_variant_id'
        WHEN BTRIM(pm.barcode) = BTRIM(cv.bar_code)
            THEN 'solo_barcode'
        ELSE 'ninguno'
    END AS pm_match_tipo
) pm_match
WHERE BTRIM(cv.bar_code) = t.barcode
ORDER BY cv.variant_id, pm.id;

-- -----------------------------------------------------------------------------
-- 14) Punto exacto de multiplicación: 1 fila → 2 productos
--     Compara conteos en cada capa del pipeline GET /api/catalog.
-- -----------------------------------------------------------------------------
WITH target AS (
    SELECT '7809562401293'::text AS barcode
),
capa_cv AS (
    SELECT COUNT(*)::bigint AS n
    FROM bsale.catalog_view cv
    CROSS JOIN target t
    WHERE BTRIM(cv.bar_code) = t.barcode
),
capa_join AS (
    SELECT COUNT(*)::bigint AS n
    FROM bsale.catalog_view cv
    CROSS JOIN target t
    LEFT JOIN bsale.variants v
        ON v.company_id = 3
       AND v.bsale_id = cv.variant_id
    LEFT JOIN bsale.products_master pm
        ON pm.variant_id = cv.variant_id
        OR (
            NULLIF(BTRIM(cv.bar_code), '') IS NOT NULL
            AND pm.barcode = BTRIM(cv.bar_code)
        )
    WHERE BTRIM(cv.bar_code) = t.barcode
),
pm_por_barcode AS (
    SELECT COUNT(*)::bigint AS n
    FROM bsale.products_master pm
    CROSS JOIN target t
    WHERE BTRIM(pm.barcode) = t.barcode
),
pm_involucrados AS (
    SELECT COUNT(DISTINCT pm.id)::bigint AS n
    FROM bsale.catalog_view cv
    CROSS JOIN target t
    LEFT JOIN bsale.products_master pm
        ON pm.variant_id = cv.variant_id
        OR (
            NULLIF(BTRIM(cv.bar_code), '') IS NOT NULL
            AND pm.barcode = BTRIM(cv.bar_code)
        )
    WHERE BTRIM(cv.bar_code) = t.barcode
      AND pm.id IS NOT NULL
)
SELECT
    '14_punto_multiplicacion' AS reporte,
    (SELECT n FROM capa_cv) AS filas_catalog_view,
    (SELECT n FROM capa_join) AS filas_despues_join_sql,
    (SELECT n FROM pm_por_barcode) AS pm_con_ese_barcode,
    (SELECT n FROM pm_involucrados) AS pm_distintos_en_join,
    CASE
        WHEN (SELECT n FROM capa_cv) > 1
            THEN 'MULTIPLICA EN catalog_view (varias filas mismo barcode)'
        WHEN (SELECT n FROM capa_join) > (SELECT n FROM capa_cv)
            THEN 'MULTIPLICA EN JOIN products_master OR (1 cv → N pm)'
        ELSE 'MISMO CONTEO cv y join: revisar mapeo Python'
    END AS donde_se_duplica;

-- -----------------------------------------------------------------------------
-- 15) Log temporal: barcode, company_id, sale_type, quantity_step, units_per_box
--     sale_type_api / quantity_step_api ≈ build_commercial_rules() en Python.
-- -----------------------------------------------------------------------------
WITH target AS (
    SELECT '7809562401293'::text AS barcode
),
sql_rows AS (
    SELECT
        cv.variant_id AS id,
        BTRIM(cv.bar_code) AS barcode,
        v.company_id,
        pm.sale_type AS pm_sale_type,
        pm.quantity_step AS pm_quantity_step,
        COALESCE(
            NULLIF(v.units_per_box, 0),
            NULLIF(pm.units_per_box, 0),
            (regexp_match(UPPER(COALESCE(v.description, '')),
                          'SEC[[:space:]]*([0-9]+)'))[1]::integer
        ) AS units_per_box_raw
    FROM bsale.catalog_view cv
    CROSS JOIN target t
    LEFT JOIN bsale.variants v
        ON v.company_id = 3
       AND v.bsale_id = cv.variant_id
    LEFT JOIN bsale.products_master pm
        ON pm.variant_id = cv.variant_id
        OR (
            NULLIF(BTRIM(cv.bar_code), '') IS NOT NULL
            AND pm.barcode = BTRIM(cv.bar_code)
        )
    WHERE BTRIM(cv.bar_code) = t.barcode
)
SELECT
    '15_log_api_completo' AS reporte,
    barcode,
    company_id,
    CASE
        WHEN units_per_box_raw IS NULL OR units_per_box_raw <= 0 THEN 'UNITARIO'
        WHEN UPPER(BTRIM(pm_sale_type)) IN ('ENTERA', 'PARCIAL', 'UNITARIO')
            THEN UPPER(BTRIM(pm_sale_type))
        ELSE 'ENTERA'
    END AS sale_type,
    CASE
        WHEN units_per_box_raw IS NULL OR units_per_box_raw <= 0 THEN 1
        WHEN UPPER(BTRIM(pm_sale_type)) = 'UNITARIO' THEN 1
        WHEN pm_quantity_step IS NOT NULL AND pm_quantity_step > 0 THEN pm_quantity_step
        WHEN UPPER(BTRIM(pm_sale_type)) = 'ENTERA'
          OR pm_sale_type IS NULL
          OR BTRIM(pm_sale_type) = ''
            THEN units_per_box_raw
        ELSE units_per_box_raw
    END AS quantity_step,
    units_per_box_raw AS units_per_box,
    id AS api_id,
    pm_sale_type,
    pm_quantity_step
FROM sql_rows
ORDER BY id;
