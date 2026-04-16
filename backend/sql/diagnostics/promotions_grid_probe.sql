-- Probe manual: misma forma que GET /promotions/grid (WHERE 1=1, sin parámetros).
-- Ejecutar en psql antes de depurar FastAPI, p. ej.:
--   psql "host=... dbname=... user=..." -f backend/sql/diagnostics/promotions_grid_probe.sql
--
-- Validación rápida de columnas (ajustar si tu \d difiere):
--   \d bsale.products_master   -> barcode, product_name, variant_name, product_type, ...
--   \d bsale.variants          -> company_id, bsale_id, bar_code, description, ...
--   \d bsale.product_types     -> company_id, bsale_id, name, state

SELECT
    ps.promotion_id AS promotion_id,
    p.activa AS activa,
    COALESCE(pm.product_type, '') AS tipo_producto,
    COALESCE(pm.product_name, '') AS producto,
    COALESCE(NULLIF(pm.variant_name, ''), vv.description, '') AS variante,
    ps.barcode AS codigo_barras,
    ROUND(
        CASE
            WHEN ps.precio_normal > 0
            THEN ((ps.precio_normal - ps.precio_oferta) / ps.precio_normal) * 100
            ELSE 0
        END,
        2
    ) AS descuento_porcentaje,
    CASE
        WHEN pi.tipo_descuento = 'porcentaje' THEN CONCAT(pi.valor::text, '%')
        ELSE 'precio fijo'
    END AS descuento_texto,
    p.fecha_inicio AS fecha_inicio,
    p.fecha_fin AS fecha_fin,
    p.tipo AS tipo,
    pi.observacion AS observacion,
    p.canal AS canal,
    CASE
        WHEN NOT p.activa THEN 'Inactiva'
        WHEN CURRENT_DATE < p.fecha_inicio THEN 'Programada'
        WHEN CURRENT_DATE > p.fecha_fin THEN 'Vencida'
        ELSE 'Activa'
    END AS estado,
    ps.company_id AS company_id,
    COALESCE(ps.price_list, pc.price_list) AS price_list,
    ps.precio_normal AS precio_normal,
    ps.precio_oferta AS precio_oferta
FROM app.promotion_price_snapshot ps
INNER JOIN app.promotions p
    ON p.id = ps.promotion_id
INNER JOIN app.promotion_items pi
    ON pi.promotion_id = p.id
   AND pi.barcode = ps.barcode
INNER JOIN app.promotion_companies pc
    ON pc.promotion_id = p.id
   AND pc.company_id = ps.company_id
LEFT JOIN bsale.products_master pm
    ON pm.barcode = ps.barcode
LEFT JOIN (
    SELECT DISTINCT ON (v.company_id, v.bar_code)
        v.company_id,
        v.bar_code,
        v.description
    FROM bsale.variants v
    WHERE v.bar_code IS NOT NULL
    ORDER BY v.company_id, v.bar_code, v.bsale_id
) vv
    ON vv.company_id = ps.company_id
   AND vv.bar_code = ps.barcode
WHERE 1 = 1
ORDER BY p.fecha_inicio DESC,
         COALESCE(pm.product_name, '') ASC
LIMIT 50;
