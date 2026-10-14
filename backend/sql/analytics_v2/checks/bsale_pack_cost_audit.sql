-- =============================================================================
-- Analítica V2 — Auditoría de packs Bsale y su costo (company 3).
-- SÓLO LECTURA. Ejecutar cada bloque por separado en pgAdmin.
--
-- Pack = bsale_raw.products.classification = 3 (documentación oficial Bsale: 0 producto,
-- 1 servicio, 3 pack o promoción). Nunca se deduce por el nombre del producto.
-- "Más de un ILA" usa la misma definición que el bloque D de bsale_tax_ids_audit.sql:
-- ids distintos de product_taxes.tax_ids cuyo código SII en bsale_raw.taxes es de ILA.
-- No se asume que average_cost del SKU pack sea la suma de sus componentes.
-- =============================================================================


-- P0. Contexto: clasificación Bsale de todos los productos de la empresa.
SELECT p.classification,
       CASE p.classification WHEN 0 THEN 'producto' WHEN 1 THEN 'servicio' WHEN 3 THEN 'pack_o_promocion'
            ELSE 'sin_dato_o_desconocido' END AS classification_label,
       count(*) AS products,
       count(*) FILTER (WHERE p.state = 0 AND p.missing_since IS NULL) AS active_products
FROM bsale_raw.products p
WHERE p.company_id = 3
GROUP BY p.classification
ORDER BY p.classification NULLS LAST;


-- P1. Cobertura: de los productos con más de un ILA, cuántos son pack (classification = 3).
WITH sii_ila(code) AS (VALUES ('24'), ('25'), ('26'), ('27'), ('271')),
ila_ids AS (
    SELECT t.bsale_id FROM bsale_raw.taxes t JOIN sii_ila s ON s.code = btrim(t.code) WHERE t.company_id = 3
),
multi_ila AS (
    SELECT ptx.product_id, count(DISTINCT x) AS n_ila
    FROM bsale_raw.product_taxes ptx
    CROSS JOIN LATERAL unnest(ptx.tax_ids) AS x
    WHERE ptx.company_id = 3 AND x IN (SELECT bsale_id FROM ila_ids)
    GROUP BY ptx.product_id
    HAVING count(DISTINCT x) > 1
)
SELECT CASE p.classification WHEN 3 THEN 'pack (classification = 3)'
            WHEN 0 THEN 'producto (classification = 0)'
            WHEN 1 THEN 'servicio (classification = 1)'
            ELSE 'sin producto RAW o classification nula' END AS bsale_class,
       count(*) AS products,
       count(*) FILTER (WHERE p.state = 0 AND p.missing_since IS NULL) AS active_products,
       count(*) FILTER (WHERE p.stock_control = 1) AS with_stock_control,
       count(*) FILTER (WHERE p.payload ->> 'printDetailPack' = '1') AS print_detail_pack
FROM multi_ila m
LEFT JOIN bsale_raw.products p ON p.company_id = 3 AND p.bsale_id = m.product_id
GROUP BY 1
ORDER BY 1;


-- P2. ¿El payload de products guarda la composición? Claves presentes por clase (sólo nombres de clave).
SELECT CASE WHEN p.classification = 3 THEN 'pack' ELSE 'no_pack' END AS bsale_class,
       k AS payload_key,
       count(*) AS products,
       (k ~* 'pack|detail|component|promo') AS could_hold_composition
FROM bsale_raw.products p
CROSS JOIN LATERAL jsonb_object_keys(p.payload) AS k
WHERE p.company_id = 3
GROUP BY 1, k
ORDER BY 1, k;


-- P3. ¿Algún otro RAW trae componentes? Claves de payload en líneas de venta, consumos y
--     recepciones de variantes pack (sólo nombres de clave; sin valores).
WITH pack_variants AS (
    SELECT v.bsale_id AS variant_id
    FROM bsale_raw.variants v
    JOIN bsale_raw.products p ON p.company_id = v.company_id AND p.bsale_id = v.product_id
    WHERE v.company_id = 3 AND p.classification = 3
)
SELECT 'document_details' AS raw_table, k AS payload_key, count(*) AS lines
FROM bsale_raw.document_details d
CROSS JOIN LATERAL jsonb_object_keys(d.payload) AS k
WHERE d.company_id = 3 AND d.variant_id IN (SELECT variant_id FROM pack_variants)
GROUP BY k
UNION ALL
SELECT 'stock_consumption_details', k, count(*)
FROM bsale_raw.stock_consumption_details c
CROSS JOIN LATERAL jsonb_object_keys(c.payload) AS k
WHERE c.company_id = 3 AND c.variant_id IN (SELECT variant_id FROM pack_variants)
GROUP BY k
UNION ALL
SELECT 'stock_reception_details', k, count(*)
FROM bsale_raw.stock_reception_details r
CROSS JOIN LATERAL jsonb_object_keys(r.payload) AS k
WHERE r.company_id = 3 AND r.variant_id IN (SELECT variant_id FROM pack_variants)
GROUP BY k
ORDER BY 1, 2;


-- P4. Costo promedio Bsale de los SKUs con más de un ILA, por clase: sin fila, nulo, cero, positivo.
WITH sii_ila(code) AS (VALUES ('24'), ('25'), ('26'), ('27'), ('271')),
ila_ids AS (
    SELECT t.bsale_id FROM bsale_raw.taxes t JOIN sii_ila s ON s.code = btrim(t.code) WHERE t.company_id = 3
),
multi_ila AS (
    SELECT ptx.product_id
    FROM bsale_raw.product_taxes ptx
    CROSS JOIN LATERAL unnest(ptx.tax_ids) AS x
    WHERE ptx.company_id = 3 AND x IN (SELECT bsale_id FROM ila_ids)
    GROUP BY ptx.product_id
    HAVING count(DISTINCT x) > 1
)
SELECT CASE WHEN p.classification = 3 THEN 'pack' ELSE 'no_pack' END AS bsale_class,
       CASE WHEN vc.variant_id IS NULL THEN 'sin_fila_variant_costs'
            WHEN vc.average_cost IS NULL THEN 'average_cost_nulo'
            WHEN vc.average_cost = 0 THEN 'average_cost_cero'
            WHEN vc.average_cost < 0 THEN 'average_cost_negativo'
            ELSE 'average_cost_positivo' END AS cost_bucket,
       count(*) AS skus,
       count(*) FILTER (WHERE v.state = 0 AND v.missing_since IS NULL) AS active_skus,
       count(*) FILTER (WHERE vc.history_count > 0) AS with_cost_history,
       max(vc.last_admission_date) AS last_admission_date
FROM multi_ila m
JOIN bsale_raw.products p ON p.company_id = 3 AND p.bsale_id = m.product_id
JOIN bsale_raw.variants v ON v.company_id = 3 AND v.product_id = m.product_id
LEFT JOIN bsale_raw.variant_costs vc ON vc.company_id = 3 AND vc.variant_id = v.bsale_id
GROUP BY 1, 2
ORDER BY 1, 2;


-- P5. Detalle por SKU de los productos con más de un ILA (evidencia, no clasificación).
--     Recepciones propias y stock propio indican que el SKU se compra/almacena como unidad;
--     no reemplazan a classification ni revelan componentes.
WITH sii(code, role) AS (
    VALUES ('14', 'iva'), ('17', 'iva_anticipado'), ('18', 'iva_anticipado'), ('19', 'iva_anticipado'),
           ('24', 'ila'), ('25', 'ila'), ('26', 'ila'), ('27', 'ila'), ('271', 'ila')
),
tx AS (
    SELECT t.bsale_id AS tax_id, nullif(btrim(t.code), '') AS code, t.percentage,
           coalesce(s.role, 'desconocido') AS role
    FROM bsale_raw.taxes t LEFT JOIN sii s ON s.code = btrim(t.code)
    WHERE t.company_id = 3
),
multi_ila AS (
    SELECT ptx.product_id, ptx.tax_ids,
           count(DISTINCT x) FILTER (WHERE tx.role = 'ila') AS n_ila,
           string_agg(DISTINCT x || ':' || coalesce(tx.code, '?') || ':' || coalesce(tx.percentage::text, '?'), ' + ')
               AS tax_detail
    FROM bsale_raw.product_taxes ptx
    CROSS JOIN LATERAL unnest(ptx.tax_ids) AS x
    LEFT JOIN tx ON tx.tax_id = x
    WHERE ptx.company_id = 3
    GROUP BY ptx.product_id, ptx.tax_ids
    HAVING count(DISTINCT x) FILTER (WHERE tx.role = 'ila') > 1
),
rec AS (
    SELECT d.variant_id, count(*) AS reception_lines, max(r.admission_date) AS last_admission,
           (array_agg(d.cost ORDER BY r.admission_date DESC NULLS LAST, d.bsale_id DESC))[1] AS last_reception_cost
    FROM bsale_raw.stock_reception_details d
    JOIN bsale_raw.stock_receptions r ON r.company_id = d.company_id AND r.bsale_id = d.reception_id
    WHERE d.company_id = 3 AND r.missing_since IS NULL
    GROUP BY d.variant_id
),
stk AS (
    SELECT s.variant_id, sum(s.quantity) AS stock_qty
    FROM bsale_raw.stocks s
    WHERE s.company_id = 3
    GROUP BY s.variant_id
)
SELECT p.bsale_id AS product_id,
       p.name AS product_name,
       p.classification,
       (p.classification = 3) AS is_bsale_pack,
       p.stock_control,
       p.payload ->> 'printDetailPack' AS print_detail_pack,
       p.state AS product_state,
       m.tax_ids,
       m.tax_detail,
       m.n_ila,
       v.bsale_id AS variant_id,
       v.code AS sku,
       v.state AS variant_state,
       vc.average_cost,
       vc.history_count,
       vc.last_admission_date,
       coalesce(rec.reception_lines, 0) AS reception_lines,
       rec.last_admission,
       rec.last_reception_cost,
       stk.stock_qty,
       (p.bsale_id IN (5646, 5710, 8099, 8100)) AS user_example
FROM multi_ila m
JOIN bsale_raw.products p ON p.company_id = 3 AND p.bsale_id = m.product_id
JOIN bsale_raw.variants v ON v.company_id = 3 AND v.product_id = p.bsale_id
LEFT JOIN bsale_raw.variant_costs vc ON vc.company_id = 3 AND vc.variant_id = v.bsale_id
LEFT JOIN rec ON rec.variant_id = v.bsale_id
LEFT JOIN stk ON stk.variant_id = v.bsale_id
ORDER BY user_example DESC, is_bsale_pack DESC, stk.stock_qty DESC NULLS LAST, p.bsale_id;


-- P6. Ejemplos reportados, por id de producto: 5646 Mistral + Sprite, 5710 Mistral + Coca-Cola,
--     8099 Alto del Carmen + Coca-Cola, 8100 Alto del Carmen + Sprite.
--     Payload completo del producto (catálogo, sin PII): classification y banderas de pack.
SELECT p.bsale_id AS product_id, p.classification, p.stock_control, p.state, p.missing_since,
       p.payload
FROM bsale_raw.products p
WHERE p.company_id = 3 AND p.bsale_id IN (5646, 5710, 8099, 8100);


-- P7. Packs Bsale (classification = 3) con su SKU, estado y precio bruto vigente en listas 12, 13 y 16.
--     El costo promedio del SKU pack se muestra sólo como evidencia: no es el costo de sus componentes.
SELECT p.bsale_id AS product_id,
       p.name AS product_name,
       (p.state = 0 AND p.missing_since IS NULL) AS product_active,
       v.bsale_id AS variant_id,
       v.code AS sku,
       (v.state = 0 AND v.missing_since IS NULL) AS variant_active,
       max(vp.variant_value_with_taxes) FILTER (WHERE vp.price_list_id = 12) AS gross_price_list_12,
       max(vp.variant_value_with_taxes) FILTER (WHERE vp.price_list_id = 13) AS gross_price_list_13,
       max(vp.variant_value_with_taxes) FILTER (WHERE vp.price_list_id = 16) AS gross_price_list_16,
       max(vp.api_fetched_at) AS prices_fetched_at,
       max(vc.average_cost) AS pack_sku_average_cost_evidence_only
FROM bsale_raw.products p
JOIN bsale_raw.variants v ON v.company_id = p.company_id AND v.product_id = p.bsale_id
LEFT JOIN bsale_raw.variant_prices vp
       ON vp.company_id = v.company_id AND vp.variant_id = v.bsale_id AND vp.price_list_id IN (12, 13, 16)
LEFT JOIN bsale_raw.variant_costs vc ON vc.company_id = v.company_id AND vc.variant_id = v.bsale_id
WHERE p.company_id = 3 AND p.classification = 3
GROUP BY p.bsale_id, p.name, p.state, p.missing_since, v.bsale_id, v.code, v.state, v.missing_since
ORDER BY product_active DESC, variant_active DESC, p.bsale_id;


-- P8. Ventas de packs presentes en BSALE RAW por tipo de documento (RAW sólo sincroniza OC 33;
--     boletas y facturas no están en RAW). Evidencia para la rentabilidad histórica.
SELECT d.company_id,
       doc.document_type_id,
       count(*) AS lines,
       count(DISTINCT d.variant_id) AS pack_variants,
       sum(d.quantity) AS qty,
       min(doc.emission_date) AS first_date,
       max(doc.emission_date) AS last_date
FROM bsale_raw.document_details d
JOIN bsale_raw.documents doc ON doc.company_id = d.company_id AND doc.bsale_id = d.document_id
JOIN bsale_raw.variants v ON v.company_id = d.company_id AND v.bsale_id = d.variant_id
JOIN bsale_raw.products p ON p.company_id = v.company_id AND p.bsale_id = v.product_id
WHERE d.company_id = 3 AND p.classification = 3
GROUP BY d.company_id, doc.document_type_id
ORDER BY lines DESC;
