-- =============================================================================
-- Validación de recepciones RAW (company 3). SÓLO LECTURA: ejecutar en pgAdmin después de
-- sync-receptions. No corrige nada: las diferencias se investigan, no se ajustan.
-- No imprime note ni user (sólo ids, fechas, conteos, cantidades y costos).
-- =============================================================================

-- 1. Día 31-07-2026: Bsale informó 26 recepciones.
SELECT count(*) AS receptions,
       count(*) FILTER (WHERE details_complete) AS complete,
       count(*) FILTER (WHERE details_complete IS NOT TRUE) AS incomplete,
       count(*) FILTER (WHERE missing_since IS NOT NULL) AS missing
FROM bsale_raw.stock_receptions
WHERE company_id = 3 AND admission_date = DATE '2026-07-31';

-- 2. Marzo–julio 2026: legacy tiene 3.827 recepciones y 24.845 líneas.
SELECT count(DISTINCT r.bsale_id) AS raw_receptions,
       count(d.bsale_id) FILTER (WHERE d.missing_since IS NULL) AS raw_lines
FROM bsale_raw.stock_receptions r
LEFT JOIN bsale_raw.stock_reception_details d ON d.company_id = r.company_id AND d.reception_id = r.bsale_id
WHERE r.company_id = 3 AND r.missing_since IS NULL
  AND r.admission_date BETWEEN DATE '2026-03-01' AND DATE '2026-07-31';

SELECT count(DISTINCT reception_id) AS legacy_receptions, count(*) AS legacy_lines
FROM analytics.cost_reception_history
WHERE company_id = 3 AND admission_date >= DATE '2026-03-01' AND admission_date < DATE '2026-08-01';

-- 3. Identidad Bsale: recepciones del legacy que no están en RAW y viceversa (con su fecha).
WITH legacy AS (
    SELECT DISTINCT reception_id, min(admission_date) AS legacy_date
    FROM analytics.cost_reception_history
    WHERE company_id = 3 AND admission_date >= DATE '2026-03-01' AND admission_date < DATE '2026-08-01'
    GROUP BY reception_id
), raw AS (
    SELECT bsale_id AS reception_id, admission_date, missing_since, details_complete
    FROM bsale_raw.stock_receptions
    WHERE company_id = 3 AND admission_date BETWEEN DATE '2026-03-01' AND DATE '2026-07-31'
)
SELECT 'solo_legacy' AS side, l.reception_id, l.legacy_date::text AS fecha, NULL::boolean AS complete
FROM legacy l LEFT JOIN raw r USING (reception_id) WHERE r.reception_id IS NULL
UNION ALL
SELECT 'solo_raw', r.reception_id, r.admission_date::text, r.details_complete
FROM raw r LEFT JOIN legacy l USING (reception_id) WHERE l.reception_id IS NULL
ORDER BY 1, 2
LIMIT 500;

-- 4. Líneas por (reception_id, reception_detail_id): faltantes y diferencias de quantity / cost.
--    legacy.cost_net vs RAW cost tal cual (RAW no calcula nada; una diferencia se investiga).
WITH legacy AS (
    SELECT reception_id, reception_detail_id, variant_id, quantity, cost_net
    FROM analytics.cost_reception_history
    WHERE company_id = 3 AND admission_date >= DATE '2026-03-01' AND admission_date < DATE '2026-08-01'
), raw AS (
    SELECT d.reception_id, d.bsale_id AS reception_detail_id, d.variant_id, d.quantity, d.cost, d.missing_since
    FROM bsale_raw.stock_reception_details d
    JOIN bsale_raw.stock_receptions r ON r.company_id = d.company_id AND r.bsale_id = d.reception_id
    WHERE d.company_id = 3 AND r.admission_date BETWEEN DATE '2026-03-01' AND DATE '2026-07-31'
)
SELECT CASE
           WHEN r.reception_detail_id IS NULL THEN 'linea_solo_legacy'
           WHEN l.reception_detail_id IS NULL THEN 'linea_solo_raw'
           WHEN r.missing_since IS NOT NULL THEN 'linea_ausente_en_raw'
           WHEN l.variant_id IS DISTINCT FROM r.variant_id THEN 'variante_distinta'
           WHEN l.quantity IS DISTINCT FROM r.quantity THEN 'cantidad_distinta'
           WHEN l.cost_net IS DISTINCT FROM r.cost THEN 'costo_distinto'
           ELSE 'igual'
       END AS result,
       count(*) AS lines
FROM legacy l
FULL JOIN raw r USING (reception_id, reception_detail_id)
GROUP BY 1
ORDER BY 1;

-- 4b. Muestra de diferencias de cantidad / costo (para investigar caso a caso).
SELECT l.reception_id, l.reception_detail_id, l.variant_id, l.quantity AS legacy_quantity, d.quantity AS raw_quantity,
       l.cost_net AS legacy_cost, d.cost AS raw_cost
FROM analytics.cost_reception_history l
JOIN bsale_raw.stock_reception_details d
  ON d.company_id = l.company_id AND d.reception_id = l.reception_id AND d.bsale_id = l.reception_detail_id
WHERE l.company_id = 3 AND l.admission_date >= DATE '2026-03-01' AND l.admission_date < DATE '2026-08-01'
  AND (l.quantity IS DISTINCT FROM d.quantity OR l.cost_net IS DISTINCT FROM d.cost)
ORDER BY l.reception_id, l.reception_detail_id
LIMIT 200;

-- 5. Oficinas: recepciones con office_id inexistente en bsale_raw.offices.
SELECT r.office_id, count(*) AS receptions
FROM bsale_raw.stock_receptions r
LEFT JOIN bsale_raw.offices o ON o.company_id = r.company_id AND o.bsale_id = r.office_id
WHERE r.company_id = 3 AND (r.office_id IS NULL OR o.bsale_id IS NULL)
GROUP BY r.office_id ORDER BY 2 DESC;

-- 6. Documentos: etiquetas originales (sin clasificar) y recepciones sin número.
SELECT coalesce(document, '<NULL>') AS document, count(*) AS receptions,
       count(*) FILTER (WHERE coalesce(document_number, '') = '') AS without_number
FROM bsale_raw.stock_receptions
WHERE company_id = 3 AND missing_since IS NULL
GROUP BY 1 ORDER BY 2 DESC LIMIT 100;

-- 7. Detalles completos: incompletas, y details_count distinto de las líneas vigentes.
SELECT r.bsale_id, r.admission_date, r.details_complete, r.details_count,
       count(d.bsale_id) FILTER (WHERE d.missing_since IS NULL) AS live_lines
FROM bsale_raw.stock_receptions r
LEFT JOIN bsale_raw.stock_reception_details d ON d.company_id = r.company_id AND d.reception_id = r.bsale_id
WHERE r.company_id = 3 AND r.missing_since IS NULL
GROUP BY r.bsale_id, r.admission_date, r.details_complete, r.details_count
HAVING r.details_complete IS NOT TRUE
    OR r.details_count IS DISTINCT FROM count(d.bsale_id) FILTER (WHERE d.missing_since IS NULL)
ORDER BY r.bsale_id LIMIT 200;

-- 8. Costos cero / nulos y cantidades no positivas (sólo conteo; RAW los guarda tal cual).
SELECT count(*) FILTER (WHERE cost = 0) AS zero_cost,
       count(*) FILTER (WHERE cost IS NULL) AS null_cost,
       count(*) FILTER (WHERE quantity IS NULL OR quantity <= 0) AS non_positive_quantity,
       count(*) FILTER (WHERE quantity <> trunc(quantity)) AS fractional_quantity
FROM bsale_raw.stock_reception_details
WHERE company_id = 3 AND missing_since IS NULL;

-- 9. Variantes inexistentes en bsale_raw.variants.
SELECT d.variant_id, count(*) AS lines
FROM bsale_raw.stock_reception_details d
LEFT JOIN bsale_raw.variants v ON v.company_id = d.company_id AND v.bsale_id = d.variant_id
WHERE d.company_id = 3 AND d.missing_since IS NULL AND v.bsale_id IS NULL
GROUP BY d.variant_id ORDER BY 2 DESC LIMIT 200;

-- 10. Duplicados: un id de línea en más de una recepción; variante repetida dentro de una recepción.
SELECT bsale_id AS reception_detail_id, array_agg(reception_id ORDER BY reception_id) AS receptions
FROM bsale_raw.stock_reception_details
WHERE company_id = 3
GROUP BY bsale_id HAVING count(DISTINCT reception_id) > 1
LIMIT 200;

SELECT reception_id, variant_id, count(*) AS lines
FROM bsale_raw.stock_reception_details
WHERE company_id = 3 AND missing_since IS NULL
GROUP BY reception_id, variant_id HAVING count(*) > 1
ORDER BY 3 DESC LIMIT 200;

-- 11. Referencias internas: internalDispatchId distinto de 0 (recepción originada por despacho interno).
SELECT count(*) FILTER (WHERE internal_dispatch_id IS NOT NULL AND internal_dispatch_id <> 0) AS from_internal_dispatch,
       count(*) FILTER (WHERE internal_dispatch_id IS NULL) AS without_value,
       count(*) FILTER (WHERE user_id IS NULL) AS without_user
FROM bsale_raw.stock_receptions
WHERE company_id = 3 AND missing_since IS NULL;

-- 12. Vínculo con variant_costs.history[].reception_detail.id (NOT_IN_HISTORY no es error confirmado:
--     el historial de costos de Bsale puede no listar todas las recepciones).
WITH history AS (
    SELECT vc.variant_id, (h -> 'reception_detail' ->> 'id')::bigint AS reception_detail_id, h ->> 'cost' AS history_cost
    FROM bsale_raw.variant_costs vc
    CROSS JOIN LATERAL jsonb_array_elements(coalesce(vc.payload -> 'history', '[]'::jsonb)) AS h
    WHERE vc.company_id = 3 AND (h -> 'reception_detail' ->> 'id') ~ '^[0-9]+$'
)
SELECT count(*) AS raw_lines,
       count(h.reception_detail_id) AS in_history,
       count(*) FILTER (WHERE h.reception_detail_id IS NOT NULL AND h.history_cost::numeric IS DISTINCT FROM d.cost)
           AS cost_differs
FROM bsale_raw.stock_reception_details d
JOIN bsale_raw.stock_receptions r ON r.company_id = d.company_id AND r.bsale_id = d.reception_id
LEFT JOIN history h ON h.variant_id = d.variant_id AND h.reception_detail_id = d.bsale_id
WHERE d.company_id = 3 AND d.missing_since IS NULL
  AND r.admission_date BETWEEN DATE '2026-03-01' AND DATE '2026-07-31';

-- 13. Bitácora: eventos por tipo y por corrida.
SELECT event_kind, detected_by, count(*) AS events,
       count(*) FILTER (WHERE cardinality(cost_changed_detail_ids) > 0) AS with_cost_change,
       count(*) FILTER (WHERE cardinality(quantity_changed_detail_ids) > 0) AS with_quantity_change,
       count(*) FILTER (WHERE cardinality(removed_detail_ids) > 0) AS with_removed_lines
FROM bsale_raw.stock_reception_change_log
WHERE company_id = 3
GROUP BY 1, 2 ORDER BY 1, 2;

-- 14. Carácter neto de cost (pendiente): muestra de recepciones con factura para contrastar a mano
--     cost × quantity contra el neto de la factura del proveedor (no se calcula IVA aquí).
SELECT r.bsale_id, r.admission_date, r.document, r.document_number, count(d.bsale_id) AS lines
FROM bsale_raw.stock_receptions r
JOIN bsale_raw.stock_reception_details d ON d.company_id = r.company_id AND d.reception_id = r.bsale_id
WHERE r.company_id = 3 AND r.document ILIKE 'FACTURA%' AND coalesce(r.document_number, '') <> ''
  AND r.admission_date BETWEEN DATE '2026-07-01' AND DATE '2026-07-31'
GROUP BY r.bsale_id, r.admission_date, r.document, r.document_number
ORDER BY r.admission_date DESC LIMIT 10;

-- 15. Cobertura del alcance (desde 2025-01-01): recepciones, líneas e incompletas por mes. Un mes con 0
--     o muy por debajo de sus vecinos = lote no cargado o cortado (retomar con --from = resume_from).
SELECT date_trunc('month', r.admission_date)::date AS month,
       count(*) AS receptions,
       count(*) FILTER (WHERE r.details_complete IS NOT TRUE) AS incomplete,
       count(*) FILTER (WHERE r.missing_since IS NOT NULL) AS missing,
       sum(coalesce(r.details_count, 0)) AS lines_declared
FROM bsale_raw.stock_receptions r
WHERE r.company_id = 3 AND r.admission_date >= DATE '2025-01-01'
GROUP BY 1 ORDER BY 1;

-- 16. MODIFIED por bitácora en los últimos 7 días: sin cambios reales en Bsale debería ser ~0
--     (variantStock está excluido del content_hash; un número alto indica otro campo volátil).
SELECT count(*) AS modified_7d,
       count(*) FILTER (WHERE header_changed) AS header_changed,
       count(*) FILTER (WHERE details_changed) AS details_changed
FROM bsale_raw.stock_reception_change_log
WHERE company_id = 3 AND event_kind = 'MODIFIED' AND detected_at >= now() - interval '7 days';

-- 17. Filas anteriores al alcance (informativo: no se cargan ni se marcan ausentes; nunca se borran).
SELECT count(*) AS receptions_before_2025
FROM bsale_raw.stock_receptions
WHERE company_id = 3 AND admission_date < DATE '2025-01-01';
