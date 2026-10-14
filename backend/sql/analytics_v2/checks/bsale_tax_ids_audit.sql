-- =============================================================================
-- Analítica V2 — Auditoría de impuestos Bsale por ID (company 3), previa al cálculo definitivo.
-- SÓLO LECTURA. Ejecutar cada bloque por separado en pgAdmin.
--
-- Los impuestos de un producto se determinan EXCLUSIVAMENTE por los ids de
-- bsale_raw.product_taxes.tax_ids resueltos contra bsale_raw.taxes de la misma empresa.
-- El tipo (IVA / ILA / IVA anticipado) sale del código SII del impuesto; nunca del nombre del
-- impuesto ni de la categoría, tipo o nombre del producto. Sin código SII => 'desconocido'.
-- tax_id es un identificador: la tasa es siempre taxes.percentage.
-- De taxes.payload sólo se leen banderas tributarias (forRetention, overtax, controlTax).
-- =============================================================================


-- A. Impuestos reales de Bsale por ID, con su tasa, metadatos y uso.
WITH sii(code, role, legal_rate) AS (
    VALUES ('14', 'iva', 19.0), ('17', 'iva_anticipado', 5.0), ('18', 'iva_anticipado', 5.0),
           ('19', 'iva_anticipado', 12.0), ('24', 'ila', 31.5), ('25', 'ila', 20.5),
           ('26', 'ila', 20.5), ('27', 'ila', 10.0), ('271', 'ila', 18.0)
),
active_skus AS (
    SELECT v.product_id, count(*) AS skus
    FROM bsale_raw.variants v
    WHERE v.company_id = 3 AND v.missing_since IS NULL AND v.state = 0
    GROUP BY v.product_id
),
usage AS (
    SELECT u.tax_id, count(DISTINCT ptx.product_id) AS products, coalesce(sum(s.skus), 0) AS active_skus
    FROM bsale_raw.product_taxes ptx
    CROSS JOIN LATERAL (SELECT DISTINCT x AS tax_id FROM unnest(ptx.tax_ids) AS x) AS u
    LEFT JOIN active_skus s ON s.product_id = ptx.product_id
    WHERE ptx.company_id = 3 AND ptx.missing_since IS NULL
    GROUP BY u.tax_id
)
SELECT t.bsale_id AS tax_id,
       t.name AS bsale_name_display_only,
       nullif(btrim(t.code), '') AS sii_code,
       t.percentage AS rate_pct,
       t.state,
       t.missing_since,
       t.payload ->> 'forRetention' AS for_retention,
       t.payload ->> 'overtax' AS overtax,
       t.payload ->> 'controlTax' AS control_tax,
       coalesce(s.role, 'desconocido') AS tax_type,
       s.legal_rate,
       array_remove(ARRAY[
           CASE WHEN nullif(btrim(t.code), '') IS NULL THEN 'codigo_sii_ausente' END,
           CASE WHEN nullif(btrim(t.code), '') IS NOT NULL AND s.code IS NULL THEN 'codigo_sii_no_reconocido' END,
           CASE WHEN t.percentage IS NULL THEN 'tasa_nula' END,
           CASE WHEN s.code IS NOT NULL AND t.percentage <> s.legal_rate THEN 'tasa_distinta_a_codigo_sii' END,
           CASE WHEN coalesce(t.state, 0) <> 0 THEN 'impuesto_inactivo' END,
           CASE WHEN t.missing_since IS NOT NULL THEN 'impuesto_retirado_de_bsale' END
       ], NULL) AS tax_issues,
       coalesce(us.products, 0) AS products_using,
       coalesce(us.active_skus, 0) AS active_skus_using
FROM bsale_raw.taxes t
LEFT JOIN sii s ON s.code = btrim(t.code)
LEFT JOIN usage us ON us.tax_id = t.bsale_id
WHERE t.company_id = 3
ORDER BY t.bsale_id;


-- B. Asociaciones por producto: ids de impuestos, código SII y tasa de cada uno, factor resultante
--    y motivo de ambigüedad. tax_detail = id:código:tasa en el orden de Bsale.
WITH sii(code, role, legal_rate) AS (
    VALUES ('14', 'iva', 19.0), ('17', 'iva_anticipado', 5.0), ('18', 'iva_anticipado', 5.0),
           ('19', 'iva_anticipado', 12.0), ('24', 'ila', 31.5), ('25', 'ila', 20.5),
           ('26', 'ila', 20.5), ('27', 'ila', 10.0), ('271', 'ila', 18.0)
),
tx AS (
    SELECT t.bsale_id AS tax_id, nullif(btrim(t.code), '') AS code, t.percentage,
           coalesce(s.role, 'desconocido') AS role,
           (nullif(btrim(t.code), '') IS NULL OR s.code IS NULL OR t.percentage IS NULL
            OR t.percentage <> s.legal_rate OR coalesce(t.state, 0) <> 0 OR t.missing_since IS NOT NULL) AS has_issue
    FROM bsale_raw.taxes t
    LEFT JOIN sii s ON s.code = btrim(t.code)
    WHERE t.company_id = 3
),
active_skus AS (
    SELECT v.product_id, count(*) AS skus
    FROM bsale_raw.variants v
    WHERE v.company_id = 3 AND v.missing_since IS NULL AND v.state = 0
    GROUP BY v.product_id
),
assoc AS (
    SELECT p.bsale_id AS product_id, p.name AS product_name, pt.name AS product_type_name,
           (ptx.product_id IS NOT NULL) AS has_row, ptx.tax_ids, ptx.missing_since AS config_missing_since
    FROM bsale_raw.products p
    LEFT JOIN bsale_raw.product_types pt ON pt.company_id = p.company_id AND pt.bsale_id = p.product_type_id
    LEFT JOIN bsale_raw.product_taxes ptx ON ptx.company_id = p.company_id AND ptx.product_id = p.bsale_id
    WHERE p.company_id = 3
),
detail AS (
    SELECT a.product_id,
           string_agg(u.tax_id || ':' || coalesce(tx.code, '?') || ':' || coalesce(tx.percentage::text, '?'),
                      ' + ' ORDER BY u.ord) AS tax_detail,
           count(u.tax_id) AS n_ids,
           count(DISTINCT u.tax_id) AS n_distinct,
           count(u.tax_id) FILTER (WHERE tx.tax_id IS NULL) AS n_missing,
           count(DISTINCT u.tax_id) FILTER (WHERE tx.role = 'iva') AS n_iva,
           count(DISTINCT u.tax_id) FILTER (WHERE tx.role = 'ila') AS n_ila,
           count(DISTINCT u.tax_id) FILTER (WHERE tx.role = 'iva_anticipado') AS n_anticipado,
           count(DISTINCT u.tax_id) FILTER (WHERE tx.role = 'desconocido') AS n_unknown,
           count(DISTINCT u.tax_id) FILTER (WHERE tx.has_issue) AS n_with_issue,
           (SELECT sum(t2.percentage) FROM tx t2 WHERE t2.tax_id = ANY(a.tax_ids) AND t2.role IN ('iva', 'ila')) AS commercial_rate
    FROM assoc a
    LEFT JOIN LATERAL unnest(a.tax_ids) WITH ORDINALITY AS u(tax_id, ord) ON true
    LEFT JOIN tx ON tx.tax_id = u.tax_id
    GROUP BY a.product_id, a.tax_ids
),
evaluated AS (
    SELECT a.*, d.tax_detail, d.n_iva, d.n_ila, d.commercial_rate,
           array_remove(ARRAY[
               CASE WHEN NOT a.has_row THEN 'sin_fila_product_taxes' END,
               CASE WHEN a.has_row AND a.tax_ids IS NULL THEN 'tax_ids_nulo' END,
               CASE WHEN cardinality(a.tax_ids) = 0 THEN 'sin_impuestos_no_se_asume_exento' END,
               CASE WHEN a.config_missing_since IS NOT NULL THEN 'configuracion_retirada' END,
               CASE WHEN d.n_missing > 0 THEN 'impuesto_fuera_de_catalogo' END,
               CASE WHEN d.n_unknown > 0 THEN 'impuesto_sin_codigo_sii_reconocido' END,
               CASE WHEN d.n_with_issue > 0 THEN 'impuesto_con_tasa_estado_o_codigo_inconsistente' END,
               CASE WHEN d.n_anticipado > 0 THEN 'iva_anticipado' END,
               CASE WHEN d.n_ila > 1 THEN 'mas_de_un_ila' END,
               CASE WHEN cardinality(a.tax_ids) > 0 AND d.n_iva = 0 THEN 'sin_iva' END,
               CASE WHEN d.n_iva > 1 THEN 'iva_duplicado' END
           ], NULL) AS ambiguity,
           (d.n_ids > d.n_distinct) AS repeated_ids
    FROM assoc a
    JOIN detail d ON d.product_id = a.product_id
)
SELECT e.product_id,
       e.product_name,
       e.product_type_name,
       e.tax_ids,
       e.tax_detail,
       CASE WHEN cardinality(e.ambiguity) = 0 THEN e.commercial_rate END AS iva_plus_ila_pct,
       CASE WHEN cardinality(e.ambiguity) = 0 THEN 1 + e.commercial_rate / 100 END AS gross_factor,
       CASE WHEN cardinality(e.ambiguity) = 0 THEN 'calculable' ELSE 'requiere_revision_tributaria' END AS result,
       e.ambiguity,
       e.repeated_ids,
       coalesce(s.skus, 0) AS active_skus
FROM evaluated e
LEFT JOIN active_skus s ON s.product_id = e.product_id
ORDER BY result DESC, e.product_type_name NULLS LAST, e.product_name;


-- C. Misma categoría, distintos impuestos: combinaciones de ids por tipo de producto.
--    Sirve para comprobar que la categoría NO determina el ILA (p. ej. Bebidas con 10 % y 18 %).
WITH tx AS (
    SELECT t.bsale_id AS tax_id, nullif(btrim(t.code), '') AS code, t.percentage
    FROM bsale_raw.taxes t
    WHERE t.company_id = 3
),
combos AS (
    SELECT p.product_type_id,
           ptx.product_id,
           (SELECT string_agg(x || ':' || coalesce(tx.code, '?') || ':' || coalesce(tx.percentage::text, '?'), ' + ' ORDER BY x)
              FROM (SELECT DISTINCT y AS x FROM unnest(ptx.tax_ids) AS y) AS ids
              LEFT JOIN tx ON tx.tax_id = ids.x) AS tax_detail
    FROM bsale_raw.products p
    JOIN bsale_raw.product_taxes ptx ON ptx.company_id = p.company_id AND ptx.product_id = p.bsale_id
    WHERE p.company_id = 3 AND ptx.missing_since IS NULL
)
SELECT pt.name AS product_type_name,
       coalesce(c.tax_detail, '(sin impuestos)') AS tax_detail,
       count(*) AS products,
       count(*) OVER (PARTITION BY c.product_type_id) AS combinations_in_type
FROM combos c
LEFT JOIN bsale_raw.product_types pt ON pt.company_id = 3 AND pt.bsale_id = c.product_type_id
GROUP BY c.product_type_id, pt.name, c.tax_detail
ORDER BY combinations_in_type DESC, product_type_name, products DESC;


-- D. Sólo configuraciones ambiguas (requieren revisión tributaria), con SKUs activos afectados.
WITH sii(code, role, legal_rate) AS (
    VALUES ('14', 'iva', 19.0), ('17', 'iva_anticipado', 5.0), ('18', 'iva_anticipado', 5.0),
           ('19', 'iva_anticipado', 12.0), ('24', 'ila', 31.5), ('25', 'ila', 20.5),
           ('26', 'ila', 20.5), ('27', 'ila', 10.0), ('271', 'ila', 18.0)
),
tx AS (
    SELECT t.bsale_id AS tax_id, nullif(btrim(t.code), '') AS code, t.percentage,
           coalesce(s.role, 'desconocido') AS role,
           (nullif(btrim(t.code), '') IS NULL OR s.code IS NULL OR t.percentage IS NULL
            OR t.percentage <> s.legal_rate OR coalesce(t.state, 0) <> 0 OR t.missing_since IS NOT NULL) AS has_issue
    FROM bsale_raw.taxes t
    LEFT JOIN sii s ON s.code = btrim(t.code)
    WHERE t.company_id = 3
),
active_skus AS (
    SELECT v.product_id, count(*) AS skus
    FROM bsale_raw.variants v
    WHERE v.company_id = 3 AND v.missing_since IS NULL AND v.state = 0
    GROUP BY v.product_id
),
assoc AS (
    SELECT p.bsale_id AS product_id, p.name AS product_name,
           (ptx.product_id IS NOT NULL) AS has_row, ptx.tax_ids, ptx.missing_since AS config_missing_since
    FROM bsale_raw.products p
    LEFT JOIN bsale_raw.product_taxes ptx ON ptx.company_id = p.company_id AND ptx.product_id = p.bsale_id
    WHERE p.company_id = 3
),
detail AS (
    SELECT a.product_id,
           string_agg(u.tax_id || ':' || coalesce(tx.code, '?') || ':' || coalesce(tx.percentage::text, '?'),
                      ' + ' ORDER BY u.ord) AS tax_detail,
           count(u.tax_id) FILTER (WHERE tx.tax_id IS NULL) AS n_missing,
           count(DISTINCT u.tax_id) FILTER (WHERE tx.role = 'iva') AS n_iva,
           count(DISTINCT u.tax_id) FILTER (WHERE tx.role = 'ila') AS n_ila,
           count(DISTINCT u.tax_id) FILTER (WHERE tx.role = 'iva_anticipado') AS n_anticipado,
           count(DISTINCT u.tax_id) FILTER (WHERE tx.role = 'desconocido') AS n_unknown,
           count(DISTINCT u.tax_id) FILTER (WHERE tx.has_issue) AS n_with_issue
    FROM assoc a
    LEFT JOIN LATERAL unnest(a.tax_ids) WITH ORDINALITY AS u(tax_id, ord) ON true
    LEFT JOIN tx ON tx.tax_id = u.tax_id
    GROUP BY a.product_id
),
evaluated AS (
    SELECT a.product_id, a.product_name, a.tax_ids, d.tax_detail,
           array_remove(ARRAY[
               CASE WHEN NOT a.has_row THEN 'sin_fila_product_taxes' END,
               CASE WHEN a.has_row AND a.tax_ids IS NULL THEN 'tax_ids_nulo' END,
               CASE WHEN cardinality(a.tax_ids) = 0 THEN 'sin_impuestos_no_se_asume_exento' END,
               CASE WHEN a.config_missing_since IS NOT NULL THEN 'configuracion_retirada' END,
               CASE WHEN d.n_missing > 0 THEN 'impuesto_fuera_de_catalogo' END,
               CASE WHEN d.n_unknown > 0 THEN 'impuesto_sin_codigo_sii_reconocido' END,
               CASE WHEN d.n_with_issue > 0 THEN 'impuesto_con_tasa_estado_o_codigo_inconsistente' END,
               CASE WHEN d.n_anticipado > 0 THEN 'iva_anticipado' END,
               CASE WHEN d.n_ila > 1 THEN 'mas_de_un_ila' END,
               CASE WHEN cardinality(a.tax_ids) > 0 AND d.n_iva = 0 THEN 'sin_iva' END,
               CASE WHEN d.n_iva > 1 THEN 'iva_duplicado' END
           ], NULL) AS ambiguity
    FROM assoc a
    JOIN detail d ON d.product_id = a.product_id
)
SELECT reason,
       count(*) AS products,
       sum(coalesce(s.skus, 0)) AS active_skus,
       (array_agg(e.product_id || ' ' || coalesce(e.product_name, '') || ' [' || coalesce(e.tax_detail, '-') || ']'
                  ORDER BY coalesce(s.skus, 0) DESC, e.product_id))[1:10] AS examples
FROM evaluated e
CROSS JOIN LATERAL unnest(e.ambiguity) AS reason
LEFT JOIN active_skus s ON s.product_id = e.product_id
GROUP BY reason
ORDER BY active_skus DESC, reason;
