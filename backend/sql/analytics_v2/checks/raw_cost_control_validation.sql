-- =============================================================================
-- Analítica V2 — Etapa 0: validación de BSALE RAW para Control de Costos (company 3).
-- SÓLO LECTURA. Ejecutar en pgAdmin con el mismo rol que usa la API.
-- No imprime note, user ni payload de recepciones.
-- =============================================================================

-- 1. Acceso: el rol de la API debe poder leer todas las tablas que usa Analítica V2.
SELECT t.table_name,
       has_table_privilege(current_user, 'bsale_raw.' || t.table_name, 'SELECT') AS can_select
FROM (VALUES ('variants'), ('products'), ('product_types'), ('product_taxes'), ('taxes'),
             ('variant_costs'), ('variant_prices'), ('price_lists'), ('offices'), ('stocks'),
             ('stock_receptions'), ('stock_reception_details'), ('stock_reception_change_log'),
             ('sync_state')) AS t(table_name)
ORDER BY 1;

-- 2. Etiquetas de documento y despacho interno (base de la clasificación de movimientos).
SELECT coalesce(document, '<NULL>') AS document,
       (coalesce(internal_dispatch_id, 0) <> 0) AS has_internal_dispatch,
       count(*) AS receptions,
       min(admission_date) AS first_date,
       max(admission_date) AS last_date
FROM bsale_raw.stock_receptions
WHERE company_id = 3 AND missing_since IS NULL AND admission_date >= DATE '2025-01-01'
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 100;

-- 3. Guías de despacho SIN internalDispatchId: rango de folios por oficina. Folios correlativos
--    propios (p. ej. 25803–26793) sugieren despachos entre sucursales y no compras a proveedor.
SELECT office_id,
       count(*) AS receptions,
       min(document_number) AS min_number,
       max(document_number) AS max_number
FROM bsale_raw.stock_receptions
WHERE company_id = 3 AND missing_since IS NULL AND admission_date >= DATE '2025-01-01'
  AND document ILIKE 'GU%' AND coalesce(internal_dispatch_id, 0) = 0
GROUP BY office_id
ORDER BY office_id;

-- 4. Catálogo de impuestos y combinaciones reales por producto.
SELECT bsale_id, name, code, percentage, state
FROM bsale_raw.taxes
WHERE company_id = 3
ORDER BY bsale_id;

SELECT tax_ids, count(*) AS products
FROM bsale_raw.product_taxes
WHERE company_id = 3 AND missing_since IS NULL
GROUP BY tax_ids
ORDER BY 2 DESC;

-- 5. Productos sin fila de product_taxes (impuestos aún no consultados).
SELECT count(*) AS products_without_tax_row
FROM bsale_raw.products p
LEFT JOIN bsale_raw.product_taxes t ON t.company_id = p.company_id AND t.product_id = p.bsale_id
WHERE p.company_id = 3 AND p.missing_since IS NULL AND t.product_id IS NULL;

-- 6. Cobertura de costos por variante activa: promedio Bsale y recepciones desde 2025.
WITH lines AS (
    SELECT d.variant_id, d.cost, d.quantity, r.document, r.internal_dispatch_id, r.details_complete
    FROM bsale_raw.stock_receptions r
    JOIN bsale_raw.stock_reception_details d ON d.company_id = r.company_id AND d.reception_id = r.bsale_id
    WHERE r.company_id = 3 AND r.missing_since IS NULL AND d.missing_since IS NULL
      AND r.admission_date >= DATE '2025-01-01'
)
SELECT count(*) AS active_variants,
       count(*) FILTER (WHERE vc.average_cost > 0) AS with_average_cost,
       count(*) FILTER (WHERE EXISTS (
           SELECT 1 FROM lines l
           WHERE l.variant_id = v.bsale_id AND l.cost > 0 AND l.quantity > 0
             AND l.details_complete IS TRUE AND coalesce(l.internal_dispatch_id, 0) = 0
             AND (l.document ILIKE '%factura%' OR (l.document ILIKE 'gu%' AND l.document NOT ILIKE '%anulado%'))
       )) AS with_purchase_reception
FROM bsale_raw.variants v
LEFT JOIN bsale_raw.variant_costs vc ON vc.company_id = v.company_id AND vc.variant_id = v.bsale_id
WHERE v.company_id = 3 AND v.missing_since IS NULL AND v.state = 0;

-- 7. Carácter neto del costo (pendiente): ver backend/sql/bsale_raw/checks/receptions_validation.sql
--    consulta 14 (muestra de facturas para contrastar cost × quantity con el neto de la factura).

-- 8. Plan de ejecución de la consulta principal: generar el SQL con
--      python -c "from backend.services.analytics_v2.cost_control_repo import VARIANT_FACTS_SQL; print(VARIANT_FACTS_SQL)"
--    reemplazar los %s por (3, DATE '2025-01-01', 3, 3) y ejecutar EXPLAIN (ANALYZE, BUFFERS).

-- 9. Vigencia real de listas de precios (se espera activas 12, 13 y 16 en company_id = 3;
--    14 = COMODITI ALCOHOLES debe figurar NO vigente).
SELECT bsale_id AS price_list_id, name, state, missing_since,
       (state = 0 AND missing_since IS NULL) AS vigente
FROM bsale_raw.price_lists
WHERE company_id = 3
ORDER BY bsale_id;

-- 10. Reglas de margen sobre listas no vigentes (se conservan; quedan fuera del alcance comercial).
SELECT mr.price_list_id, pl.name, pl.state, pl.missing_since IS NOT NULL AS missing,
       count(*) AS rules, count(*) FILTER (WHERE mr.active) AS active_rules
FROM bsale.margin_rules mr
LEFT JOIN bsale_raw.price_lists pl ON pl.company_id = mr.company_id AND pl.bsale_id = mr.price_list_id
WHERE mr.company_id = 3
  AND (pl.bsale_id IS NULL OR pl.state IS DISTINCT FROM 0 OR pl.missing_since IS NOT NULL)
GROUP BY mr.price_list_id, pl.name, pl.state, pl.missing_since
ORDER BY mr.price_list_id;

-- =============================================================================
-- Márgenes V2 (Analítica V2.2) — validaciones manuales en pgAdmin (solo lectura)
-- =============================================================================

-- 11. Cobertura y calidad de precios por lista evaluada (12, 13, 16).
SELECT vp.price_list_id,
       count(*) AS price_rows,
       count(*) FILTER (WHERE vp.missing_since IS NOT NULL) AS retired_in_bsale,
       count(*) FILTER (WHERE vp.variant_value_with_taxes IS NULL) AS null_gross,
       count(*) FILTER (WHERE vp.variant_value_with_taxes <= 0) AS zero_or_negative_gross,
       count(*) FILTER (WHERE vp.variant_value_with_taxes > 0 AND vp.variant_value_with_taxes < 10) AS symbolic_gross,
       count(*) FILTER (WHERE v.state = 0 AND v.missing_since IS NULL) AS on_active_variants,
       max(vp.api_fetched_at) AS last_fetched
FROM bsale_raw.variant_prices vp
LEFT JOIN bsale_raw.variants v ON v.company_id = vp.company_id AND v.bsale_id = vp.variant_id
WHERE vp.company_id = 3 AND vp.price_list_id IN (12, 13, 16)
GROUP BY vp.price_list_id
ORDER BY vp.price_list_id;

-- 12. Coherencia precio bruto vs neto (informativo; V2 usa el bruto original sin recalcular).
--     Distribución del factor bruto/neto por combinación de impuestos del producto.
SELECT pt.tax_ids,
       round(vp.variant_value_with_taxes / NULLIF(vp.variant_value, 0), 3) AS gross_net_factor,
       count(*) AS rows
FROM bsale_raw.variant_prices vp
JOIN bsale_raw.variants v ON v.company_id = vp.company_id AND v.bsale_id = vp.variant_id
LEFT JOIN bsale_raw.product_taxes pt ON pt.company_id = v.company_id AND pt.product_id = v.product_id
WHERE vp.company_id = 3 AND vp.price_list_id = 12 AND vp.missing_since IS NULL AND vp.variant_value > 0
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 40;

-- 13. Tipos de producto con SKUs activos con precio y sin regla, por lista evaluada.
SELECT vp.price_list_id, p.product_type_id, count(DISTINCT v.bsale_id) AS priced_active_skus
FROM bsale_raw.variant_prices vp
JOIN bsale_raw.variants v ON v.company_id = vp.company_id AND v.bsale_id = vp.variant_id
JOIN bsale_raw.products p ON p.company_id = v.company_id AND p.bsale_id = v.product_id
LEFT JOIN bsale.margin_rules mr
       ON mr.company_id = vp.company_id AND mr.price_list_id = vp.price_list_id
      AND mr.product_type_id IS NOT DISTINCT FROM p.product_type_id
WHERE vp.company_id = 3 AND vp.price_list_id IN (12, 13, 16)
  AND vp.missing_since IS NULL AND vp.variant_value_with_taxes >= 10
  AND v.state = 0 AND v.missing_since IS NULL
  AND mr.id IS NULL
GROUP BY 1, 2
ORDER BY 1, 2;

-- 14. Plan de la consulta de precios de Márgenes V2 (usa la PK company_id, price_list_id, variant_id).
EXPLAIN (ANALYZE, BUFFERS)
SELECT vp.price_list_id, vp.variant_id, vp.variant_value, vp.variant_value_with_taxes,
       vp.missing_since, vp.api_fetched_at
FROM bsale_raw.variant_prices vp
WHERE vp.company_id = 3 AND vp.price_list_id = ANY(ARRAY[12, 13, 16]::bigint[]);
