-- =============================================================================
-- Analítica V2 — Motor tributario: validación del COSTO BRUTO COMERCIAL (company 3).
-- SÓLO LECTURA (SELECT / EXPLAIN). Ejecutar cada bloque por separado en pgAdmin.
-- No lee bsale.companies, note, user ni payloads de documentos; de taxes.payload sólo lee
-- banderas tributarias (forRetention, overtax, controlTax).
--
-- Fórmula: costo_bruto = costo_neto × (1 + (IVA + ILA) / 100)   (nunca 1,19 × 1,205)
-- La clasificación replica backend/services/analytics_v2/tax_profile.py:
--   * por código SII de Bsale (14 IVA; 17/18/19 IVA anticipado; 24/25/26/27/271 ILA);
--   * nunca por nombre, categoría ni tipo de producto: sin código SII el impuesto queda
--     'desconocido' y el producto requiere revisión tributaria;
--   * tasa distinta a la del código, tasa nula, impuesto inactivo o retirado => inconsistente;
--   * producto sin impuestos => sin_impuestos (NO se asume exento).
-- Los redondeos con round() son sólo de presentación.
-- =============================================================================


-- 1. Inventario real de impuestos RAW de la empresa con su clasificación.
WITH sii(code, role, legal_rate) AS (
    VALUES ('14', 'iva', 19.0), ('17', 'iva_anticipado', 5.0), ('18', 'iva_anticipado', 5.0),
           ('19', 'iva_anticipado', 12.0), ('24', 'ila', 31.5), ('25', 'ila', 20.5),
           ('26', 'ila', 20.5), ('27', 'ila', 10.0), ('271', 'ila', 18.0)
)
SELECT t.bsale_id AS tax_id,
       t.name,
       t.code,
       t.percentage,
       t.state,
       t.missing_since,
       t.payload ->> 'forRetention' AS for_retention,
       t.payload ->> 'overtax' AS overtax,
       t.payload ->> 'controlTax' AS control_tax,
       s.role AS role_by_sii_code,
       s.legal_rate,
       CASE
         WHEN nullif(btrim(t.code), '') IS NULL THEN 'codigo_sii_ausente'
         WHEN s.code IS NULL THEN 'codigo_sii_no_reconocido'
         WHEN t.percentage IS NULL THEN 'tasa_nula'
         WHEN t.percentage <> s.legal_rate THEN 'tasa_distinta_a_codigo_sii'
         WHEN coalesce(t.state, 0) <> 0 THEN 'impuesto_inactivo'
         WHEN t.missing_since IS NOT NULL THEN 'impuesto_retirado_de_bsale'
         ELSE 'ok'
       END AS check_result
FROM bsale_raw.taxes t
LEFT JOIN sii s ON s.code = btrim(t.code)
WHERE t.company_id = 3
ORDER BY t.bsale_id;


-- 2. Combinaciones de impuestos por producto (todas las combinaciones existentes).
WITH sii(code, role, legal_rate) AS (
    VALUES ('14', 'iva', 19.0), ('17', 'iva_anticipado', 5.0), ('18', 'iva_anticipado', 5.0),
           ('19', 'iva_anticipado', 12.0), ('24', 'ila', 31.5), ('25', 'ila', 20.5),
           ('26', 'ila', 20.5), ('27', 'ila', 10.0), ('271', 'ila', 18.0)
),
tx AS (
    SELECT t.bsale_id AS tax_id, t.name, t.percentage,
           CASE
             WHEN nullif(btrim(t.code), '') IS NOT NULL THEN coalesce(s.role, 'desconocido')
             ELSE 'desconocido'
           END AS role,
           (t.percentage IS NULL OR coalesce(t.state, 0) <> 0 OR t.missing_since IS NOT NULL
            OR (s.code IS NOT NULL AND t.percentage <> s.legal_rate)) AS blocking
    FROM bsale_raw.taxes t
    LEFT JOIN sii s ON s.code = btrim(t.code)
    WHERE t.company_id = 3
),
prod AS (
    SELECT p.bsale_id AS product_id, p.name AS product_name, ptx.tax_ids,
           (ptx.missing_since IS NOT NULL) AS stale,
           (SELECT array_agg(DISTINCT x ORDER BY x) FROM unnest(ptx.tax_ids) AS x) AS combo
    FROM bsale_raw.products p
    LEFT JOIN bsale_raw.product_taxes ptx ON ptx.company_id = p.company_id AND ptx.product_id = p.bsale_id
    WHERE p.company_id = 3
),
prof AS (
    SELECT pr.product_id, pr.product_name, pr.tax_ids, pr.combo, pr.stale,
           count(u.tax_id) FILTER (WHERE tx.tax_id IS NULL) AS n_missing,
           count(*) FILTER (WHERE tx.role = 'desconocido') AS n_unknown,
           count(*) FILTER (WHERE tx.role = 'iva') AS n_iva,
           count(*) FILTER (WHERE tx.role = 'ila') AS n_ila,
           count(*) FILTER (WHERE tx.role = 'iva_anticipado') AS n_anticipado,
           coalesce(bool_or(tx.blocking), false) AS any_blocking,
           sum(tx.percentage) AS rate_pct,
           string_agg(tx.name, ' + ' ORDER BY u.tax_id) AS tax_names
    FROM prod pr
    LEFT JOIN LATERAL unnest(pr.combo) AS u(tax_id) ON true
    LEFT JOIN tx ON tx.tax_id = u.tax_id
    GROUP BY pr.product_id, pr.product_name, pr.tax_ids, pr.combo, pr.stale
),
classified AS (
    SELECT prof.*,
           CASE
             WHEN tax_ids IS NULL THEN 'no_consultado'
             WHEN stale THEN 'configuracion_inconsistente'
             WHEN cardinality(tax_ids) = 0 THEN 'sin_impuestos'
             WHEN n_missing > 0 OR n_unknown > 0 THEN 'impuesto_no_reconocido'
             WHEN any_blocking THEN 'configuracion_inconsistente'
             WHEN n_anticipado > 0 THEN 'iva_anticipado'
             WHEN n_ila >= 2 THEN 'ila_combinado'
             WHEN n_iva <> 1 THEN 'configuracion_inconsistente'
             WHEN n_ila = 1 THEN 'iva_ila'
             ELSE 'iva'
           END AS tax_status
    FROM prof
)
SELECT combo AS tax_ids,
       tax_names,
       tax_status,
       CASE WHEN tax_status IN ('iva', 'iva_ila') THEN rate_pct END AS total_rate_pct,
       count(*) AS products
FROM classified
GROUP BY combo, tax_names, tax_status, CASE WHEN tax_status IN ('iva', 'iva_ila') THEN rate_pct END
ORDER BY products DESC, tax_ids;


-- 3. SKUs activos por combinación (misma clasificación).
WITH sii(code, role, legal_rate) AS (
    VALUES ('14', 'iva', 19.0), ('17', 'iva_anticipado', 5.0), ('18', 'iva_anticipado', 5.0),
           ('19', 'iva_anticipado', 12.0), ('24', 'ila', 31.5), ('25', 'ila', 20.5),
           ('26', 'ila', 20.5), ('27', 'ila', 10.0), ('271', 'ila', 18.0)
),
tx AS (
    SELECT t.bsale_id AS tax_id, t.name, t.percentage,
           CASE
             WHEN nullif(btrim(t.code), '') IS NOT NULL THEN coalesce(s.role, 'desconocido')
             ELSE 'desconocido'
           END AS role,
           (t.percentage IS NULL OR coalesce(t.state, 0) <> 0 OR t.missing_since IS NOT NULL
            OR (s.code IS NOT NULL AND t.percentage <> s.legal_rate)) AS blocking
    FROM bsale_raw.taxes t
    LEFT JOIN sii s ON s.code = btrim(t.code)
    WHERE t.company_id = 3
),
prod AS (
    SELECT p.bsale_id AS product_id, p.name AS product_name, ptx.tax_ids,
           (ptx.missing_since IS NOT NULL) AS stale,
           (SELECT array_agg(DISTINCT x ORDER BY x) FROM unnest(ptx.tax_ids) AS x) AS combo
    FROM bsale_raw.products p
    LEFT JOIN bsale_raw.product_taxes ptx ON ptx.company_id = p.company_id AND ptx.product_id = p.bsale_id
    WHERE p.company_id = 3
),
prof AS (
    SELECT pr.product_id, pr.product_name, pr.tax_ids, pr.combo, pr.stale,
           count(u.tax_id) FILTER (WHERE tx.tax_id IS NULL) AS n_missing,
           count(*) FILTER (WHERE tx.role = 'desconocido') AS n_unknown,
           count(*) FILTER (WHERE tx.role = 'iva') AS n_iva,
           count(*) FILTER (WHERE tx.role = 'ila') AS n_ila,
           count(*) FILTER (WHERE tx.role = 'iva_anticipado') AS n_anticipado,
           coalesce(bool_or(tx.blocking), false) AS any_blocking,
           sum(tx.percentage) AS rate_pct,
           string_agg(tx.name, ' + ' ORDER BY u.tax_id) AS tax_names
    FROM prod pr
    LEFT JOIN LATERAL unnest(pr.combo) AS u(tax_id) ON true
    LEFT JOIN tx ON tx.tax_id = u.tax_id
    GROUP BY pr.product_id, pr.product_name, pr.tax_ids, pr.combo, pr.stale
),
classified AS (
    SELECT prof.*,
           CASE
             WHEN tax_ids IS NULL THEN 'no_consultado'
             WHEN stale THEN 'configuracion_inconsistente'
             WHEN cardinality(tax_ids) = 0 THEN 'sin_impuestos'
             WHEN n_missing > 0 OR n_unknown > 0 THEN 'impuesto_no_reconocido'
             WHEN any_blocking THEN 'configuracion_inconsistente'
             WHEN n_anticipado > 0 THEN 'iva_anticipado'
             WHEN n_ila >= 2 THEN 'ila_combinado'
             WHEN n_iva <> 1 THEN 'configuracion_inconsistente'
             WHEN n_ila = 1 THEN 'iva_ila'
             ELSE 'iva'
           END AS tax_status
    FROM prof
)
SELECT c.combo AS tax_ids,
       c.tax_names,
       coalesce(c.tax_status, 'no_consultado') AS tax_status,
       count(*) AS skus_active,
       count(DISTINCT v.product_id) AS products,
       count(*) FILTER (WHERE vc.average_cost > 0) AS skus_with_average_cost
FROM bsale_raw.variants v
LEFT JOIN classified c ON c.product_id = v.product_id
LEFT JOIN bsale_raw.variant_costs vc ON vc.company_id = v.company_id AND vc.variant_id = v.bsale_id
WHERE v.company_id = 3 AND v.missing_since IS NULL AND v.state = 0
GROUP BY c.combo, c.tax_names, coalesce(c.tax_status, 'no_consultado')
ORDER BY skus_active DESC, tax_ids;


-- 4. Productos con IVA o ILA duplicados, más de un ILA o ids repetidos en product_taxes.
WITH sii(code, role, legal_rate) AS (
    VALUES ('14', 'iva', 19.0), ('17', 'iva_anticipado', 5.0), ('18', 'iva_anticipado', 5.0),
           ('19', 'iva_anticipado', 12.0), ('24', 'ila', 31.5), ('25', 'ila', 20.5),
           ('26', 'ila', 20.5), ('27', 'ila', 10.0), ('271', 'ila', 18.0)
),
tx AS (
    SELECT t.bsale_id AS tax_id, t.name,
           CASE
             WHEN nullif(btrim(t.code), '') IS NOT NULL THEN coalesce(s.role, 'desconocido')
             ELSE 'desconocido'
           END AS role
    FROM bsale_raw.taxes t
    LEFT JOIN sii s ON s.code = btrim(t.code)
    WHERE t.company_id = 3
)
SELECT p.bsale_id AS product_id,
       p.name AS product_name,
       ptx.tax_ids,
       string_agg(tx.name, ' + ' ORDER BY u.tax_id) AS tax_names,
       count(*) FILTER (WHERE tx.role = 'iva') AS iva_count,
       count(*) FILTER (WHERE tx.role = 'ila') AS ila_count,
       (cardinality(ptx.tax_ids) > (SELECT count(DISTINCT x) FROM unnest(ptx.tax_ids) AS x)) AS repeated_ids,
       (SELECT count(*) FROM bsale_raw.variants v
         WHERE v.company_id = 3 AND v.product_id = p.bsale_id AND v.missing_since IS NULL AND v.state = 0) AS skus_active
FROM bsale_raw.products p
JOIN bsale_raw.product_taxes ptx ON ptx.company_id = p.company_id AND ptx.product_id = p.bsale_id
CROSS JOIN LATERAL unnest(ptx.tax_ids) AS u(tax_id)
LEFT JOIN tx ON tx.tax_id = u.tax_id
WHERE p.company_id = 3
GROUP BY p.bsale_id, p.name, ptx.tax_ids
HAVING count(*) FILTER (WHERE tx.role = 'iva') > 1
    OR count(*) FILTER (WHERE tx.role = 'ila') > 1
    OR cardinality(ptx.tax_ids) > (SELECT count(DISTINCT x) FROM unnest(ptx.tax_ids) AS x)
ORDER BY skus_active DESC, product_id;


-- 5. Productos con IVA anticipado (harina / carne): requieren definición contable.
SELECT p.bsale_id AS product_id,
       p.name AS product_name,
       ptx.tax_ids,
       string_agg(t.name || ' [' || coalesce(t.code, 'sin código') || ']', ' + ' ORDER BY t.bsale_id) AS tax_names,
       (SELECT count(*) FROM bsale_raw.variants v
         WHERE v.company_id = 3 AND v.product_id = p.bsale_id AND v.missing_since IS NULL AND v.state = 0) AS skus_active
FROM bsale_raw.products p
JOIN bsale_raw.product_taxes ptx ON ptx.company_id = p.company_id AND ptx.product_id = p.bsale_id
CROSS JOIN LATERAL unnest(ptx.tax_ids) AS u(tax_id)
JOIN bsale_raw.taxes t ON t.company_id = p.company_id AND t.bsale_id = u.tax_id
WHERE p.company_id = 3
GROUP BY p.bsale_id, p.name, ptx.tax_ids
HAVING bool_or(btrim(t.code) IN ('17', '18', '19'))
ORDER BY skus_active DESC, product_id;


-- 6. SKUs activos sin configuración tributaria utilizable (sin fila, tax_ids NULL, vacío o retirada).
SELECT v.bsale_id AS variant_id,
       v.code AS sku,
       p.bsale_id AS product_id,
       p.name AS product_name,
       CASE
         WHEN ptx.product_id IS NULL THEN 'sin_fila_product_taxes'
         WHEN ptx.tax_ids IS NULL THEN 'tax_ids_nulo'
         WHEN ptx.missing_since IS NOT NULL THEN 'configuracion_retirada'
         ELSE 'sin_impuestos'
       END AS problem,
       count(*) OVER () AS total_skus
FROM bsale_raw.variants v
LEFT JOIN bsale_raw.products p ON p.company_id = v.company_id AND p.bsale_id = v.product_id
LEFT JOIN bsale_raw.product_taxes ptx ON ptx.company_id = p.company_id AND ptx.product_id = p.bsale_id
WHERE v.company_id = 3 AND v.missing_since IS NULL AND v.state = 0
  AND (ptx.product_id IS NULL OR ptx.tax_ids IS NULL OR cardinality(ptx.tax_ids) = 0 OR ptx.missing_since IS NOT NULL)
ORDER BY problem, product_name
LIMIT 200;


-- 7. 30 ejemplos neto -> bruto (hasta 5 por combinación calculable). Se muestra también el valor
--    multiplicativo incorrecto (1,19 × (1 + ILA)) sólo para contrastar.
WITH sii(code, role, legal_rate) AS (
    VALUES ('14', 'iva', 19.0), ('17', 'iva_anticipado', 5.0), ('18', 'iva_anticipado', 5.0),
           ('19', 'iva_anticipado', 12.0), ('24', 'ila', 31.5), ('25', 'ila', 20.5),
           ('26', 'ila', 20.5), ('27', 'ila', 10.0), ('271', 'ila', 18.0)
),
tx AS (
    SELECT t.bsale_id AS tax_id, t.name, t.percentage,
           CASE
             WHEN nullif(btrim(t.code), '') IS NOT NULL THEN coalesce(s.role, 'desconocido')
             ELSE 'desconocido'
           END AS role,
           (t.percentage IS NULL OR coalesce(t.state, 0) <> 0 OR t.missing_since IS NOT NULL
            OR (s.code IS NOT NULL AND t.percentage <> s.legal_rate)) AS blocking
    FROM bsale_raw.taxes t
    LEFT JOIN sii s ON s.code = btrim(t.code)
    WHERE t.company_id = 3
),
prod AS (
    SELECT p.bsale_id AS product_id, p.name AS product_name, ptx.tax_ids,
           (ptx.missing_since IS NOT NULL) AS stale,
           (SELECT array_agg(DISTINCT x ORDER BY x) FROM unnest(ptx.tax_ids) AS x) AS combo
    FROM bsale_raw.products p
    LEFT JOIN bsale_raw.product_taxes ptx ON ptx.company_id = p.company_id AND ptx.product_id = p.bsale_id
    WHERE p.company_id = 3
),
prof AS (
    SELECT pr.product_id, pr.product_name, pr.tax_ids, pr.combo, pr.stale,
           count(u.tax_id) FILTER (WHERE tx.tax_id IS NULL) AS n_missing,
           count(*) FILTER (WHERE tx.role = 'desconocido') AS n_unknown,
           count(*) FILTER (WHERE tx.role = 'iva') AS n_iva,
           count(*) FILTER (WHERE tx.role = 'ila') AS n_ila,
           count(*) FILTER (WHERE tx.role = 'iva_anticipado') AS n_anticipado,
           coalesce(bool_or(tx.blocking), false) AS any_blocking,
           sum(tx.percentage) FILTER (WHERE tx.role = 'iva') AS iva_pct,
           coalesce(sum(tx.percentage) FILTER (WHERE tx.role = 'ila'), 0) AS ila_pct,
           string_agg(tx.name, ' + ' ORDER BY u.tax_id) AS tax_names
    FROM prod pr
    LEFT JOIN LATERAL unnest(pr.combo) AS u(tax_id) ON true
    LEFT JOIN tx ON tx.tax_id = u.tax_id
    GROUP BY pr.product_id, pr.product_name, pr.tax_ids, pr.combo, pr.stale
),
calculable AS (
    SELECT * FROM prof
    WHERE tax_ids IS NOT NULL AND NOT stale AND cardinality(tax_ids) > 0
      AND n_missing = 0 AND n_unknown = 0 AND NOT any_blocking AND n_anticipado = 0
      AND n_ila <= 1 AND n_iva = 1
),
examples AS (
    SELECT v.bsale_id AS variant_id, v.code AS sku, c.product_name, c.combo, c.tax_names,
           c.iva_pct, c.ila_pct, vc.average_cost,
           row_number() OVER (PARTITION BY c.combo ORDER BY v.bsale_id) AS rn
    FROM bsale_raw.variants v
    JOIN calculable c ON c.product_id = v.product_id
    JOIN bsale_raw.variant_costs vc ON vc.company_id = v.company_id AND vc.variant_id = v.bsale_id
    WHERE v.company_id = 3 AND v.missing_since IS NULL AND v.state = 0 AND vc.average_cost > 0
)
SELECT variant_id, sku, product_name, combo AS tax_ids, tax_names,
       average_cost AS costo_neto_promedio,
       iva_pct, ila_pct,
       1 + (iva_pct + ila_pct) / 100 AS factor,
       average_cost * (1 + (iva_pct + ila_pct) / 100) AS costo_bruto_comercial,
       round(average_cost * (1 + iva_pct / 100) * (1 + ila_pct / 100), 2) AS bruto_multiplicativo_incorrecto
FROM examples
WHERE rn <= 5
ORDER BY tax_ids, variant_id
LIMIT 30;


-- 8a. Precio bruto Bsale vs costo bruto comercial calculado, listas 12, 13 y 16 (resumen).
--     recargo = (precio bruto / costo bruto - 1) × 100
WITH sii(code, role, legal_rate) AS (
    VALUES ('14', 'iva', 19.0), ('17', 'iva_anticipado', 5.0), ('18', 'iva_anticipado', 5.0),
           ('19', 'iva_anticipado', 12.0), ('24', 'ila', 31.5), ('25', 'ila', 20.5),
           ('26', 'ila', 20.5), ('27', 'ila', 10.0), ('271', 'ila', 18.0)
),
tx AS (
    SELECT t.bsale_id AS tax_id, t.name, t.percentage,
           CASE
             WHEN nullif(btrim(t.code), '') IS NOT NULL THEN coalesce(s.role, 'desconocido')
             ELSE 'desconocido'
           END AS role,
           (t.percentage IS NULL OR coalesce(t.state, 0) <> 0 OR t.missing_since IS NOT NULL
            OR (s.code IS NOT NULL AND t.percentage <> s.legal_rate)) AS blocking
    FROM bsale_raw.taxes t
    LEFT JOIN sii s ON s.code = btrim(t.code)
    WHERE t.company_id = 3
),
prod AS (
    SELECT p.bsale_id AS product_id, ptx.tax_ids, (ptx.missing_since IS NOT NULL) AS stale,
           (SELECT array_agg(DISTINCT x ORDER BY x) FROM unnest(ptx.tax_ids) AS x) AS combo
    FROM bsale_raw.products p
    LEFT JOIN bsale_raw.product_taxes ptx ON ptx.company_id = p.company_id AND ptx.product_id = p.bsale_id
    WHERE p.company_id = 3
),
prof AS (
    SELECT pr.product_id, pr.tax_ids, pr.stale,
           count(u.tax_id) FILTER (WHERE tx.tax_id IS NULL) AS n_missing,
           count(*) FILTER (WHERE tx.role = 'desconocido') AS n_unknown,
           count(*) FILTER (WHERE tx.role = 'iva') AS n_iva,
           count(*) FILTER (WHERE tx.role = 'ila') AS n_ila,
           count(*) FILTER (WHERE tx.role = 'iva_anticipado') AS n_anticipado,
           coalesce(bool_or(tx.blocking), false) AS any_blocking,
           sum(tx.percentage) AS rate_pct
    FROM prod pr
    LEFT JOIN LATERAL unnest(pr.combo) AS u(tax_id) ON true
    LEFT JOIN tx ON tx.tax_id = u.tax_id
    GROUP BY pr.product_id, pr.tax_ids, pr.stale
),
gross AS (
    SELECT v.bsale_id AS variant_id,
           vc.average_cost,
           CASE WHEN vc.average_cost > 0
                 AND c.tax_ids IS NOT NULL AND NOT c.stale AND cardinality(c.tax_ids) > 0
                 AND c.n_missing = 0 AND c.n_unknown = 0 AND NOT c.any_blocking AND c.n_anticipado = 0
                 AND c.n_ila <= 1 AND c.n_iva = 1
                THEN vc.average_cost * (1 + c.rate_pct / 100)
           END AS gross_cost
    FROM bsale_raw.variants v
    LEFT JOIN prof c ON c.product_id = v.product_id
    LEFT JOIN bsale_raw.variant_costs vc ON vc.company_id = v.company_id AND vc.variant_id = v.bsale_id
    WHERE v.company_id = 3 AND v.missing_since IS NULL AND v.state = 0
),
priced AS (
    SELECT vp.price_list_id, g.variant_id, vp.variant_value_with_taxes AS price_gross, g.gross_cost,
           CASE WHEN g.gross_cost > 0 AND vp.variant_value_with_taxes > 0
                THEN (vp.variant_value_with_taxes / g.gross_cost - 1) * 100 END AS recargo_pct
    FROM gross g
    JOIN bsale_raw.variant_prices vp
      ON vp.company_id = 3 AND vp.variant_id = g.variant_id AND vp.missing_since IS NULL
    WHERE vp.price_list_id IN (12, 13, 16)
)
SELECT pr.price_list_id,
       pl.name AS price_list_name,
       count(*) AS skus_with_price_row,
       count(*) FILTER (WHERE pr.price_gross > 0) AS skus_with_price,
       count(*) FILTER (WHERE pr.recargo_pct IS NOT NULL) AS skus_with_recargo,
       count(*) FILTER (WHERE pr.recargo_pct < 0) AS skus_price_below_gross_cost,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY pr.recargo_pct)::numeric, 2) AS recargo_median_pct,
       round(min(pr.recargo_pct), 2) AS recargo_min_pct,
       round(max(pr.recargo_pct), 2) AS recargo_max_pct
FROM priced pr
LEFT JOIN bsale_raw.price_lists pl ON pl.company_id = 3 AND pl.bsale_id = pr.price_list_id
GROUP BY pr.price_list_id, pl.name
ORDER BY pr.price_list_id;


-- 8b. Detalle: 50 combinaciones SKU × lista con menor recargo (posibles precios bajo costo).
WITH sii(code, role, legal_rate) AS (
    VALUES ('14', 'iva', 19.0), ('17', 'iva_anticipado', 5.0), ('18', 'iva_anticipado', 5.0),
           ('19', 'iva_anticipado', 12.0), ('24', 'ila', 31.5), ('25', 'ila', 20.5),
           ('26', 'ila', 20.5), ('27', 'ila', 10.0), ('271', 'ila', 18.0)
),
tx AS (
    SELECT t.bsale_id AS tax_id, t.name, t.percentage,
           CASE
             WHEN nullif(btrim(t.code), '') IS NOT NULL THEN coalesce(s.role, 'desconocido')
             ELSE 'desconocido'
           END AS role,
           (t.percentage IS NULL OR coalesce(t.state, 0) <> 0 OR t.missing_since IS NOT NULL
            OR (s.code IS NOT NULL AND t.percentage <> s.legal_rate)) AS blocking
    FROM bsale_raw.taxes t
    LEFT JOIN sii s ON s.code = btrim(t.code)
    WHERE t.company_id = 3
),
prod AS (
    SELECT p.bsale_id AS product_id, p.name AS product_name, ptx.tax_ids, (ptx.missing_since IS NOT NULL) AS stale,
           (SELECT array_agg(DISTINCT x ORDER BY x) FROM unnest(ptx.tax_ids) AS x) AS combo
    FROM bsale_raw.products p
    LEFT JOIN bsale_raw.product_taxes ptx ON ptx.company_id = p.company_id AND ptx.product_id = p.bsale_id
    WHERE p.company_id = 3
),
prof AS (
    SELECT pr.product_id, pr.product_name, pr.tax_ids, pr.combo, pr.stale,
           count(u.tax_id) FILTER (WHERE tx.tax_id IS NULL) AS n_missing,
           count(*) FILTER (WHERE tx.role = 'desconocido') AS n_unknown,
           count(*) FILTER (WHERE tx.role = 'iva') AS n_iva,
           count(*) FILTER (WHERE tx.role = 'ila') AS n_ila,
           count(*) FILTER (WHERE tx.role = 'iva_anticipado') AS n_anticipado,
           coalesce(bool_or(tx.blocking), false) AS any_blocking,
           sum(tx.percentage) AS rate_pct
    FROM prod pr
    LEFT JOIN LATERAL unnest(pr.combo) AS u(tax_id) ON true
    LEFT JOIN tx ON tx.tax_id = u.tax_id
    GROUP BY pr.product_id, pr.product_name, pr.tax_ids, pr.combo, pr.stale
)
SELECT vp.price_list_id,
       v.bsale_id AS variant_id,
       v.code AS sku,
       c.product_name,
       c.combo AS tax_ids,
       c.rate_pct AS tax_rate_pct,
       vc.average_cost AS costo_neto_promedio,
       vc.average_cost * (1 + c.rate_pct / 100) AS costo_bruto_comercial,
       vp.variant_value_with_taxes AS precio_bruto_bsale,
       round((vp.variant_value_with_taxes / (vc.average_cost * (1 + c.rate_pct / 100)) - 1) * 100, 2) AS recargo_pct
FROM bsale_raw.variants v
JOIN prof c ON c.product_id = v.product_id
JOIN bsale_raw.variant_costs vc ON vc.company_id = v.company_id AND vc.variant_id = v.bsale_id
JOIN bsale_raw.variant_prices vp
  ON vp.company_id = v.company_id AND vp.variant_id = v.bsale_id AND vp.missing_since IS NULL
WHERE v.company_id = 3 AND v.missing_since IS NULL AND v.state = 0
  AND vp.price_list_id IN (12, 13, 16) AND vp.variant_value_with_taxes > 0
  AND vc.average_cost > 0
  AND c.tax_ids IS NOT NULL AND NOT c.stale AND cardinality(c.tax_ids) > 0
  AND c.n_missing = 0 AND c.n_unknown = 0 AND NOT c.any_blocking AND c.n_anticipado = 0
  AND c.n_ila <= 1 AND c.n_iva = 1
ORDER BY recargo_pct, vp.price_list_id, v.bsale_id
LIMIT 50;


-- 9. SKUs activos: bruto calculable (provisional) vs revisión tributaria vs sin configuración vs sin costo.
WITH sii(code, role, legal_rate) AS (
    VALUES ('14', 'iva', 19.0), ('17', 'iva_anticipado', 5.0), ('18', 'iva_anticipado', 5.0),
           ('19', 'iva_anticipado', 12.0), ('24', 'ila', 31.5), ('25', 'ila', 20.5),
           ('26', 'ila', 20.5), ('27', 'ila', 10.0), ('271', 'ila', 18.0)
),
tx AS (
    SELECT t.bsale_id AS tax_id, t.percentage,
           CASE
             WHEN nullif(btrim(t.code), '') IS NOT NULL THEN coalesce(s.role, 'desconocido')
             ELSE 'desconocido'
           END AS role,
           (t.percentage IS NULL OR coalesce(t.state, 0) <> 0 OR t.missing_since IS NOT NULL
            OR (s.code IS NOT NULL AND t.percentage <> s.legal_rate)) AS blocking
    FROM bsale_raw.taxes t
    LEFT JOIN sii s ON s.code = btrim(t.code)
    WHERE t.company_id = 3
),
prod AS (
    SELECT p.bsale_id AS product_id, ptx.tax_ids, (ptx.missing_since IS NOT NULL) AS stale,
           (SELECT array_agg(DISTINCT x ORDER BY x) FROM unnest(ptx.tax_ids) AS x) AS combo
    FROM bsale_raw.products p
    LEFT JOIN bsale_raw.product_taxes ptx ON ptx.company_id = p.company_id AND ptx.product_id = p.bsale_id
    WHERE p.company_id = 3
),
prof AS (
    SELECT pr.product_id,
           CASE
             WHEN pr.tax_ids IS NULL OR cardinality(pr.tax_ids) = 0 THEN 'sin_configuracion'
             WHEN pr.stale
               OR count(u.tax_id) FILTER (WHERE tx.tax_id IS NULL) > 0
               OR count(*) FILTER (WHERE tx.role = 'desconocido') > 0
               OR coalesce(bool_or(tx.blocking), false)
               OR count(*) FILTER (WHERE tx.role = 'iva_anticipado') > 0
               OR count(*) FILTER (WHERE tx.role = 'ila') > 1
               OR count(*) FILTER (WHERE tx.role = 'iva') <> 1 THEN 'requiere_revision_tributaria'
             ELSE 'calculable'
           END AS validation
    FROM prod pr
    LEFT JOIN LATERAL unnest(pr.combo) AS u(tax_id) ON true
    LEFT JOIN tx ON tx.tax_id = u.tax_id
    GROUP BY pr.product_id, pr.tax_ids, pr.stale
)
SELECT coalesce(c.validation, 'sin_configuracion') AS tax_validation,
       count(*) AS skus_active,
       count(*) FILTER (WHERE vc.average_cost > 0) AS skus_with_cost,
       count(*) FILTER (WHERE vc.average_cost IS NULL OR vc.average_cost <= 0) AS skus_without_cost,
       count(*) FILTER (WHERE c.validation = 'calculable' AND vc.average_cost > 0) AS skus_gross_provisional,
       0 AS skus_gross_validated  -- TAX_CONFIG_VERIFIED = False: ningún bruto se presenta como validado
FROM bsale_raw.variants v
LEFT JOIN prof c ON c.product_id = v.product_id
LEFT JOIN bsale_raw.variant_costs vc ON vc.company_id = v.company_id AND vc.variant_id = v.bsale_id
WHERE v.company_id = 3 AND v.missing_since IS NULL AND v.state = 0
GROUP BY ROLLUP (coalesce(c.validation, 'sin_configuracion'))
ORDER BY 1 NULLS LAST;


-- 10. Rendimiento: plan real de la consulta 9 (EXPLAIN ANALYZE ejecuta el SELECT; no escribe).
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
WITH sii(code, role, legal_rate) AS (
    VALUES ('14', 'iva', 19.0), ('17', 'iva_anticipado', 5.0), ('18', 'iva_anticipado', 5.0),
           ('19', 'iva_anticipado', 12.0), ('24', 'ila', 31.5), ('25', 'ila', 20.5),
           ('26', 'ila', 20.5), ('27', 'ila', 10.0), ('271', 'ila', 18.0)
),
tx AS (
    SELECT t.bsale_id AS tax_id, t.percentage,
           CASE
             WHEN nullif(btrim(t.code), '') IS NOT NULL THEN coalesce(s.role, 'desconocido')
             ELSE 'desconocido'
           END AS role,
           (t.percentage IS NULL OR coalesce(t.state, 0) <> 0 OR t.missing_since IS NOT NULL
            OR (s.code IS NOT NULL AND t.percentage <> s.legal_rate)) AS blocking
    FROM bsale_raw.taxes t
    LEFT JOIN sii s ON s.code = btrim(t.code)
    WHERE t.company_id = 3
),
prod AS (
    SELECT p.bsale_id AS product_id, ptx.tax_ids, (ptx.missing_since IS NOT NULL) AS stale,
           (SELECT array_agg(DISTINCT x ORDER BY x) FROM unnest(ptx.tax_ids) AS x) AS combo
    FROM bsale_raw.products p
    LEFT JOIN bsale_raw.product_taxes ptx ON ptx.company_id = p.company_id AND ptx.product_id = p.bsale_id
    WHERE p.company_id = 3
),
prof AS (
    SELECT pr.product_id,
           CASE
             WHEN pr.tax_ids IS NULL OR cardinality(pr.tax_ids) = 0 THEN 'sin_configuracion'
             WHEN pr.stale
               OR count(u.tax_id) FILTER (WHERE tx.tax_id IS NULL) > 0
               OR count(*) FILTER (WHERE tx.role = 'desconocido') > 0
               OR coalesce(bool_or(tx.blocking), false)
               OR count(*) FILTER (WHERE tx.role = 'iva_anticipado') > 0
               OR count(*) FILTER (WHERE tx.role = 'ila') > 1
               OR count(*) FILTER (WHERE tx.role = 'iva') <> 1 THEN 'requiere_revision_tributaria'
             ELSE 'calculable'
           END AS validation
    FROM prod pr
    LEFT JOIN LATERAL unnest(pr.combo) AS u(tax_id) ON true
    LEFT JOIN tx ON tx.tax_id = u.tax_id
    GROUP BY pr.product_id, pr.tax_ids, pr.stale
)
SELECT coalesce(c.validation, 'sin_configuracion') AS tax_validation,
       count(*) AS skus_active,
       count(*) FILTER (WHERE c.validation = 'calculable' AND vc.average_cost > 0) AS skus_gross_provisional
FROM bsale_raw.variants v
LEFT JOIN prof c ON c.product_id = v.product_id
LEFT JOIN bsale_raw.variant_costs vc ON vc.company_id = v.company_id AND vc.variant_id = v.bsale_id
WHERE v.company_id = 3 AND v.missing_since IS NULL AND v.state = 0
GROUP BY 1;
