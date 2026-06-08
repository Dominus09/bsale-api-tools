-- =============================================================================
-- Población masiva: sale_type + quantity_step en bsale.products_master
-- por categoría (product_type) y SEC resuelto desde columna o texto (SEC N).
--
-- Ejecutar después de: backend/sql/app_catalog_sale_rules.sql
-- Idempotente: puede re-ejecutarse (sobrescribe sale_type/quantity_step por reglas).
-- =============================================================================

BEGIN;

WITH pm_sec AS (
    SELECT
        pm.id,
        UPPER(BTRIM(pm.product_type)) AS pt_norm,
        COALESCE(
            NULLIF(pm.units_per_box, 0),
            NULLIF(v.units_per_box, 0),
            (
                regexp_match(
                    UPPER(COALESCE(v.description, pm.variant_name, '')),
                    'SEC[[:space:]]*([0-9]+)'
                )
            )[1]::integer
        ) AS sec
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
),
classified AS (
    SELECT
        id,
        pt_norm,
        sec,
        CASE
            WHEN pt_norm IN (
                'AGUA MINERAL',
                'AGUAS C/SABORES',
                'BEBIDAS',
                'CERVEZAS',
                'ENERGIZANTES',
                'ISOTONIC',
                'NECTAR/JUGOS LIQ'
            ) THEN 'ENTERA'
            WHEN pt_norm IN (
                'ABARROTES',
                'ASEO/HOGAR',
                'CONFITERIA',
                'GALLETAS',
                'PERFUM/BELLEZA/FARMACIA'
            ) THEN 'PARCIAL'
            WHEN pt_norm IN (
                'VINO',
                'LICOR',
                'WHISKY',
                'RON',
                'PISCO',
                'VODKA',
                'TEQUILA',
                'COGNAC',
                'ESPUMANTES'
            ) THEN 'UNITARIO'
        END AS sale_type,
        CASE
            WHEN pt_norm IN (
                'VINO',
                'LICOR',
                'WHISKY',
                'RON',
                'PISCO',
                'VODKA',
                'TEQUILA',
                'COGNAC',
                'ESPUMANTES'
            ) THEN 1
            WHEN pt_norm IN (
                'AGUA MINERAL',
                'AGUAS C/SABORES',
                'BEBIDAS',
                'CERVEZAS',
                'ENERGIZANTES',
                'ISOTONIC',
                'NECTAR/JUGOS LIQ'
            ) THEN sec
            WHEN pt_norm IN (
                'ABARROTES',
                'ASEO/HOGAR',
                'CONFITERIA',
                'GALLETAS',
                'PERFUM/BELLEZA/FARMACIA'
            ) THEN
                CASE
                    WHEN sec IS NULL OR sec <= 0 THEN NULL
                    WHEN sec <= 12 THEN sec / 2
                    WHEN sec BETWEEN 18 AND 24 THEN ROUND(sec / 4.0)::integer
                    WHEN sec BETWEEN 30 AND 40 THEN ROUND(sec / 4.0)::integer
                    -- SEC 13-17: no definido en negocio; aproximar con mitad de caja
                    WHEN sec BETWEEN 13 AND 17 THEN ROUND(sec / 2.0)::integer
                    ELSE ROUND(sec / 4.0)::integer
                END
        END AS quantity_step
    FROM pm_sec
)
UPDATE bsale.products_master pm
SET
    sale_type = c.sale_type,
    quantity_step = c.quantity_step,
    updated_at = NOW()
FROM classified c
WHERE pm.id = c.id
  AND c.sale_type IS NOT NULL
  AND (
        c.sale_type = 'UNITARIO'
        OR (c.quantity_step IS NOT NULL AND c.quantity_step > 0)
  );

COMMIT;

-- Verificación rápida (opcional, comentar si no se desea)
-- SELECT product_type, sale_type, quantity_step, units_per_box, COUNT(*)
-- FROM bsale.products_master
-- WHERE sale_type IS NOT NULL
-- GROUP BY 1, 2, 3, 4
-- ORDER BY 1, 2;
