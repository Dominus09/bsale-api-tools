-- =============================================================================
-- Recalcular quantity_step para productos PARCIAL (reglas comerciales definitivas)
--
-- NO ejecutar a ciegas. Orden recomendado en pgAdmin:
--   1) Ejecutar bloque PREVIEW y revisar filas.
--   2) Si está OK, ejecutar bloque UPDATE (solo PARCIAL).
--   3) Ejecutar bloques RESUMEN y REVISIÓN MANUAL.
--
-- No modifica sale_type.
-- No toca ENTERA ni UNITARIO.
-- No usa ROUND(sec / 4).
--
-- SEC resuelto (units_per_box efectivo):
--   1) bsale.products_master.units_per_box
--   2) bsale.variants.units_per_box (company_id = 3, por variant_id o barcode)
--   3) regexp en variants.description / products_master.variant_name
-- =============================================================================


-- =============================================================================
-- BLOQUE A — PREVIEW (solo lectura)
-- Muestra step actual vs sugerido y la regla que aplicaría el UPDATE.
-- =============================================================================
WITH sec_step_map AS (
    SELECT * FROM (VALUES
        (6,   3),
        (8,   4),
        (10,  5),
        (12,  6),
        (18,  6),
        (20,  5),
        (24,  6),
        (30,  10),
        (40,  10),
        (50,  10),
        (60,  12),
        (72,  12),
        (80,  10),
        (85,  10),
        (100, 10),
        (140, 10),
        (150, 10),
        (300, 10),
        (360, 10)
    ) AS t(sec, quantity_step)
),
pm_sec AS (
    SELECT
        pm.id,
        BTRIM(pm.barcode) AS barcode,
        pm.product_name,
        pm.variant_name,
        pm.sale_type,
        pm.quantity_step AS quantity_step_actual,
        COALESCE(
            NULLIF(pm.units_per_box, 0),
            NULLIF(v.units_per_box, 0),
            (
                regexp_match(
                    UPPER(COALESCE(v.description, pm.variant_name, '')),
                    'SEC[[:space:]]*([0-9]+)'
                )
            )[1]::integer
        ) AS units_per_box
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
    WHERE pm.sale_type = 'PARCIAL'
),
calc AS (
    SELECT
        p.*,
        m.quantity_step AS step_mapa,
        CASE
            WHEN p.units_per_box IS NULL OR p.units_per_box <= 0
                THEN NULL
            WHEN m.quantity_step IS NOT NULL
                THEN m.quantity_step
            WHEN p.units_per_box % 10 = 0
                THEN 10
            WHEN p.units_per_box % 6 = 0
                THEN 6
            WHEN p.units_per_box <= 12
                THEN (p.units_per_box / 2)   -- entero: FLOOR para positivos
            ELSE NULL
        END AS quantity_step_sugerido,
        CASE
            WHEN p.units_per_box IS NULL OR p.units_per_box <= 0
                THEN 'revision_manual:sin_sec'
            WHEN m.quantity_step IS NOT NULL
                THEN 'mapa:SEC' || p.units_per_box::text
            WHEN p.units_per_box % 10 = 0
                THEN 'fallback:multiplo_10'
            WHEN p.units_per_box % 6 = 0
                THEN 'fallback:multiplo_6'
            WHEN p.units_per_box <= 12
                THEN 'fallback:sec_le_12_floor_div2'
            ELSE 'revision_manual:sin_regla'
        END AS regla_aplicada
    FROM pm_sec p
    LEFT JOIN sec_step_map m
        ON m.sec = p.units_per_box
)
SELECT
    barcode,
    product_name,
    variant_name,
    units_per_box,
    sale_type,
    quantity_step_actual,
    quantity_step_sugerido,
    regla_aplicada,
    CASE
        WHEN quantity_step_sugerido IS NULL
            THEN 'NO SE ACTUALIZARÁ'
        WHEN quantity_step_actual IS DISTINCT FROM quantity_step_sugerido
            THEN 'CAMBIARÁ'
        ELSE 'SIN CAMBIO'
    END AS accion_update
FROM calc
ORDER BY
    CASE WHEN quantity_step_sugerido IS NULL THEN 0 ELSE 1 END,
    units_per_box DESC NULLS LAST,
    product_name,
    variant_name;


-- =============================================================================
-- BLOQUE B — UPDATE (ejecutar solo tras aprobar PREVIEW)
-- Actualiza únicamente PARCIAL con step sugerido calculado (no revision_manual).
-- =============================================================================
/*
BEGIN;

WITH sec_step_map AS (
    SELECT * FROM (VALUES
        (6,   3),
        (8,   4),
        (10,  5),
        (12,  6),
        (18,  6),
        (20,  5),
        (24,  6),
        (30,  10),
        (40,  10),
        (50,  10),
        (60,  12),
        (72,  12),
        (80,  10),
        (85,  10),
        (100, 10),
        (140, 10),
        (150, 10),
        (300, 10),
        (360, 10)
    ) AS t(sec, quantity_step)
),
pm_sec AS (
    SELECT
        pm.id,
        pm.quantity_step AS quantity_step_actual,
        COALESCE(
            NULLIF(pm.units_per_box, 0),
            NULLIF(v.units_per_box, 0),
            (
                regexp_match(
                    UPPER(COALESCE(v.description, pm.variant_name, '')),
                    'SEC[[:space:]]*([0-9]+)'
                )
            )[1]::integer
        ) AS units_per_box
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
    WHERE pm.sale_type = 'PARCIAL'
),
calc AS (
    SELECT
        p.id,
        CASE
            WHEN p.units_per_box IS NULL OR p.units_per_box <= 0
                THEN NULL
            WHEN m.quantity_step IS NOT NULL
                THEN m.quantity_step
            WHEN p.units_per_box % 10 = 0
                THEN 10
            WHEN p.units_per_box % 6 = 0
                THEN 6
            WHEN p.units_per_box <= 12
                THEN (p.units_per_box / 2)
            ELSE NULL
        END AS quantity_step_sugerido
    FROM pm_sec p
    LEFT JOIN sec_step_map m
        ON m.sec = p.units_per_box
)
UPDATE bsale.products_master pm
SET
    quantity_step = c.quantity_step_sugerido,
    updated_at = NOW()
FROM calc c
WHERE pm.id = c.id
  AND pm.sale_type = 'PARCIAL'
  AND c.quantity_step_sugerido IS NOT NULL
  AND c.quantity_step_sugerido > 0
  AND pm.quantity_step IS DISTINCT FROM c.quantity_step_sugerido;

-- Ver filas afectadas antes de confirmar:
-- SELECT COUNT(*) FROM bsale.products_master WHERE sale_type = 'PARCIAL';

COMMIT;
*/


-- =============================================================================
-- BLOQUE C — RESUMEN POST-UPDATE (ejecutar después del UPDATE)
-- Agrupa PARCIAL por SEC y step configurado.
-- =============================================================================
WITH pm_sec AS (
    SELECT
        pm.id,
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
        ) AS units_per_box
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
    WHERE pm.sale_type = 'PARCIAL'
)
SELECT
    units_per_box,
    quantity_step,
    COUNT(*)::bigint AS cantidad_productos
FROM pm_sec
GROUP BY units_per_box, quantity_step
ORDER BY units_per_box NULLS LAST, quantity_step NULLS LAST;


-- =============================================================================
-- BLOQUE D — REVISIÓN MANUAL
-- Productos PARCIAL que requieren intervención humana o validación extra.
-- =============================================================================
WITH sec_step_map AS (
    SELECT * FROM (VALUES
        (6,   3),
        (8,   4),
        (10,  5),
        (12,  6),
        (18,  6),
        (20,  5),
        (24,  6),
        (30,  10),
        (40,  10),
        (50,  10),
        (60,  12),
        (72,  12),
        (80,  10),
        (85,  10),
        (100, 10),
        (140, 10),
        (150, 10),
        (300, 10),
        (360, 10)
    ) AS t(sec, quantity_step)
),
pm_sec AS (
    SELECT
        pm.id,
        BTRIM(pm.barcode) AS barcode,
        pm.product_name,
        pm.variant_name,
        pm.quantity_step AS quantity_step_actual,
        COALESCE(
            NULLIF(pm.units_per_box, 0),
            NULLIF(v.units_per_box, 0),
            (
                regexp_match(
                    UPPER(COALESCE(v.description, pm.variant_name, '')),
                    'SEC[[:space:]]*([0-9]+)'
                )
            )[1]::integer
        ) AS units_per_box
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
    WHERE pm.sale_type = 'PARCIAL'
),
calc AS (
    SELECT
        p.*,
        m.quantity_step AS step_mapa,
        CASE
            WHEN p.units_per_box IS NULL OR p.units_per_box <= 0
                THEN NULL
            WHEN m.quantity_step IS NOT NULL
                THEN m.quantity_step
            WHEN p.units_per_box % 10 = 0
                THEN 10
            WHEN p.units_per_box % 6 = 0
                THEN 6
            WHEN p.units_per_box <= 12
                THEN (p.units_per_box / 2)
            ELSE NULL
        END AS quantity_step_sugerido,
        CASE
            WHEN p.units_per_box IS NULL OR p.units_per_box <= 0
                THEN 'revision_manual:sin_sec'
            WHEN m.quantity_step IS NOT NULL
                THEN 'mapa:SEC' || p.units_per_box::text
            WHEN p.units_per_box % 10 = 0
                THEN 'fallback:multiplo_10'
            WHEN p.units_per_box % 6 = 0
                THEN 'fallback:multiplo_6'
            WHEN p.units_per_box <= 12
                THEN 'fallback:sec_le_12_floor_div2'
            ELSE 'revision_manual:sin_regla'
        END AS regla_aplicada
    FROM pm_sec p
    LEFT JOIN sec_step_map m
        ON m.sec = p.units_per_box
)
SELECT
    barcode,
    product_name,
    variant_name,
    units_per_box,
    'PARCIAL' AS sale_type,
    quantity_step_actual,
    quantity_step_sugerido,
    regla_aplicada,
    CASE
        WHEN quantity_step_actual IS NULL OR quantity_step_actual <= 0
            THEN 'step_invalido_o_null'
        WHEN regla_aplicada LIKE 'revision_manual%'
            THEN 'sin_regla_automatica'
        WHEN quantity_step_actual > 12
            THEN 'step_mayor_12'
        WHEN quantity_step_sugerido IS NOT NULL
         AND quantity_step_actual IS DISTINCT FROM quantity_step_sugerido
            THEN 'step_no_coincide_con_regla'
        ELSE 'revisar'
    END AS motivo_revision
FROM calc
WHERE
    quantity_step_actual IS NULL
    OR quantity_step_actual <= 0
    OR regla_aplicada LIKE 'revision_manual%'
    OR quantity_step_actual > 12
    OR (
        quantity_step_sugerido IS NOT NULL
        AND quantity_step_actual IS DISTINCT FROM quantity_step_sugerido
    )
ORDER BY units_per_box DESC NULLS LAST, product_name, variant_name;
