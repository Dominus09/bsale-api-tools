-- =============================================================================
-- Analítica V2 — Validación de composiciones de packs Bsale (company 3).
-- SÓLO LECTURA. Ejecutar cada bloque por separado en pgAdmin.
--
-- Pack = bsale_raw.products.classification = 3. Composición = products.payload -> 'pack_details'.
-- La extracción acepta las formas que usa Bsale (lista o {items: [...]}; variant.id / variantId /
-- variant_id; product.id / productId / product_id). Ningún bloque deduce componentes por nombre.
-- Mismo criterio que el motor (backend/services/analytics_v2/pack_engine.py): un pack es calculable
-- solo si es cerrado, todos sus componentes existen, tienen costo y no son packs no resolubles.
-- =============================================================================


-- C0. Estructura: tipo JSON de pack_details y conjuntos de claves de sus elementos.
WITH packs AS (
    SELECT p.bsale_id AS product_id, p.payload -> 'pack_details' AS pd
    FROM bsale_raw.products p
    WHERE p.company_id = 3 AND p.classification = 3 AND p.missing_since IS NULL
),
elems AS (
    SELECT k.product_id, e.elem
    FROM packs k
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE jsonb_typeof(k.pd)
            WHEN 'array' THEN k.pd
            WHEN 'object' THEN CASE WHEN jsonb_typeof(k.pd -> 'items') = 'array' THEN k.pd -> 'items' ELSE '[]'::jsonb END
            ELSE '[]'::jsonb
        END
    ) AS e(elem)
)
SELECT 'tipo_pack_details' AS bloque, coalesce(jsonb_typeof(pd), 'ausente') AS valor, count(*) AS n
FROM packs GROUP BY 2
UNION ALL
SELECT 'claves_elemento', (SELECT string_agg(k, ',' ORDER BY k) FROM jsonb_object_keys(elem) AS k), count(*)
FROM elems WHERE jsonb_typeof(elem) = 'object' GROUP BY 2
UNION ALL
SELECT 'claves_variant', (SELECT string_agg(k, ',' ORDER BY k) FROM jsonb_object_keys(elem -> 'variant') AS k), count(*)
FROM elems WHERE jsonb_typeof(elem -> 'variant') = 'object' GROUP BY 2
ORDER BY 1, 3 DESC;


-- C1. Cobertura: packs con pack_details no vacío y distribución de componentes por pack.
WITH packs AS (
    SELECT p.bsale_id AS product_id, p.state, p.payload -> 'pack_details' AS pd
    FROM bsale_raw.products p
    WHERE p.company_id = 3 AND p.classification = 3 AND p.missing_since IS NULL
),
sized AS (
    SELECT product_id, state,
           CASE jsonb_typeof(pd)
               WHEN 'array' THEN jsonb_array_length(pd)
               WHEN 'object' THEN CASE WHEN jsonb_typeof(pd -> 'items') = 'array' THEN jsonb_array_length(pd -> 'items') END
           END AS n_components
    FROM packs
)
SELECT coalesce(n_components::text, 'sin_lista') AS componentes_por_pack,
       count(*) AS packs,
       count(*) FILTER (WHERE state = 0) AS packs_activos
FROM sized
GROUP BY n_components
ORDER BY n_components NULLS FIRST;


-- C2. Validación de cada detalle: ids, cantidades, existencia, estado, pack anidado, costo e impuestos.
WITH packs AS (
    SELECT p.bsale_id AS product_id, p.state, p.payload -> 'pack_details' AS pd
    FROM bsale_raw.products p
    WHERE p.company_id = 3 AND p.classification = 3 AND p.missing_since IS NULL
),
elems AS (
    SELECT k.product_id, k.state AS pack_state, e.elem
    FROM packs k
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE jsonb_typeof(k.pd)
            WHEN 'array' THEN k.pd
            WHEN 'object' THEN CASE WHEN jsonb_typeof(k.pd -> 'items') = 'array' THEN k.pd -> 'items' ELSE '[]'::jsonb END
            ELSE '[]'::jsonb
        END
    ) AS e(elem)
),
comp AS (
    SELECT product_id, pack_state, elem,
           coalesce(elem -> 'variant' ->> 'id', elem ->> 'variantId', elem ->> 'variant_id', elem ->> 'variantPromoId') AS variant_txt,
           elem ->> 'quantity' AS quantity_txt,
           coalesce(elem ->> 'multipleVariant', elem ->> 'multiple_variant') AS multiple_txt,
           elem ->> 'state' AS detail_state
    FROM elems
),
typed AS (
    SELECT c.*,
           CASE WHEN c.variant_txt ~ '^[0-9]+$' THEN nullif(c.variant_txt::bigint, 0) END AS variant_id,
           CASE WHEN c.quantity_txt ~ '^[0-9]+(\.[0-9]+)?$' THEN c.quantity_txt::numeric END AS quantity
    FROM comp c
)
SELECT count(*) AS detalles,
       count(*) FILTER (WHERE t.detail_state IS NOT NULL AND t.detail_state <> '0') AS detalle_inactivo,
       count(*) FILTER (WHERE t.variant_id IS NULL) AS sin_variante_pack_abierto,
       count(*) FILTER (WHERE t.multiple_txt = '1') AS multiple_variant,
       count(*) FILTER (WHERE t.quantity IS NULL OR t.quantity <= 0) AS cantidad_invalida,
       count(*) FILTER (WHERE t.quantity IS NOT NULL AND t.quantity <> 1) AS cantidad_distinta_de_1,
       count(*) FILTER (WHERE t.variant_id IS NOT NULL AND v.bsale_id IS NULL) AS variante_inexistente_o_retirada,
       count(*) FILTER (WHERE v.bsale_id IS NOT NULL AND (v.state <> 0 OR cp.state <> 0)) AS componente_inactivo,
       count(*) FILTER (WHERE cp.classification = 3) AS componente_es_pack,
       count(*) FILTER (WHERE v.bsale_id IS NOT NULL AND cp.classification IS DISTINCT FROM 3
                          AND (vc.average_cost IS NULL OR vc.average_cost <= 0)) AS componente_sin_costo,
       count(*) FILTER (WHERE v.bsale_id IS NOT NULL AND cp.classification IS DISTINCT FROM 3
                          AND (ptx.product_id IS NULL OR cardinality(ptx.tax_ids) = 0)) AS componente_sin_impuestos_configurados
FROM typed t
LEFT JOIN bsale_raw.variants v
  ON v.company_id = 3 AND v.bsale_id = t.variant_id AND v.missing_since IS NULL
LEFT JOIN bsale_raw.products cp
  ON cp.company_id = 3 AND cp.bsale_id = v.product_id
LEFT JOIN bsale_raw.variant_costs vc
  ON vc.company_id = 3 AND vc.variant_id = v.bsale_id
LEFT JOIN bsale_raw.product_taxes ptx
  ON ptx.company_id = 3 AND ptx.product_id = cp.bsale_id;


-- C3. Estado estimado por pack (misma precedencia que el motor) y cobertura de packs calculables.
WITH packs AS (
    SELECT p.bsale_id AS product_id, p.state, p.payload -> 'pack_details' AS pd
    FROM bsale_raw.products p
    WHERE p.company_id = 3 AND p.classification = 3 AND p.missing_since IS NULL
),
elems AS (
    SELECT k.product_id, e.elem
    FROM packs k
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE jsonb_typeof(k.pd)
            WHEN 'array' THEN k.pd
            WHEN 'object' THEN CASE WHEN jsonb_typeof(k.pd -> 'items') = 'array' THEN k.pd -> 'items' ELSE '[]'::jsonb END
            ELSE '[]'::jsonb
        END
    ) AS e(elem)
    WHERE coalesce(e.elem ->> 'state', '0') = '0'
),
typed AS (
    SELECT product_id,
           CASE WHEN x.vt ~ '^[0-9]+$' THEN nullif(x.vt::bigint, 0) END AS variant_id,
           CASE WHEN x.qt ~ '^[0-9]+(\.[0-9]+)?$' THEN x.qt::numeric END AS quantity,
           coalesce(elem ->> 'multipleVariant', elem ->> 'multiple_variant') = '1' AS multiple
    FROM elems
    CROSS JOIN LATERAL (
        SELECT coalesce(elem -> 'variant' ->> 'id', elem ->> 'variantId', elem ->> 'variant_id', elem ->> 'variantPromoId') AS vt,
               elem ->> 'quantity' AS qt
    ) x
),
per_pack AS (
    SELECT k.product_id, k.state,
           count(t.product_id) AS n,
           count(*) FILTER (WHERE t.quantity IS NULL OR t.quantity <= 0) AS bad_qty,
           count(*) FILTER (WHERE t.variant_id IS NULL OR t.multiple) AS open_details,
           count(*) FILTER (WHERE t.variant_id IS NOT NULL AND v.bsale_id IS NULL) AS missing,
           count(*) FILTER (WHERE cp.classification = 3) AS nested,
           count(*) FILTER (WHERE v.bsale_id IS NOT NULL AND cp.classification IS DISTINCT FROM 3
                              AND (vc.average_cost IS NULL OR vc.average_cost <= 0)) AS no_cost
    FROM packs k
    LEFT JOIN typed t ON t.product_id = k.product_id
    LEFT JOIN bsale_raw.variants v ON v.company_id = 3 AND v.bsale_id = t.variant_id AND v.missing_since IS NULL
    LEFT JOIN bsale_raw.products cp ON cp.company_id = 3 AND cp.bsale_id = v.product_id
    LEFT JOIN bsale_raw.variant_costs vc ON vc.company_id = 3 AND vc.variant_id = v.bsale_id
    GROUP BY k.product_id, k.state
)
SELECT CASE
           WHEN n = 0 THEN 'sin_composicion'
           WHEN bad_qty > 0 THEN 'composicion_no_interpretable'
           WHEN open_details > 0 THEN 'pack_abierto'
           WHEN missing > 0 THEN 'componente_inexistente'
           WHEN nested > 0 THEN 'pack_anidado_revisar_en_motor'
           WHEN no_cost > 0 THEN 'componente_sin_costo'
           ELSE 'calculable_si_impuestos_resueltos'
       END AS estado_estimado,
       count(*) AS packs,
       count(*) FILTER (WHERE state = 0) AS packs_activos
FROM per_pack
GROUP BY 1
ORDER BY 2 DESC;


-- C4. Packs anidados y ciclos: recorrido pack → componente pack (máx. 6 niveles).
WITH RECURSIVE edges AS (
    SELECT DISTINCT pv.bsale_id AS pack_variant_id, x.variant_id AS component_variant_id
    FROM bsale_raw.products p
    JOIN bsale_raw.variants pv ON pv.company_id = 3 AND pv.product_id = p.bsale_id AND pv.missing_since IS NULL
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE jsonb_typeof(p.payload -> 'pack_details')
            WHEN 'array' THEN p.payload -> 'pack_details'
            WHEN 'object' THEN CASE WHEN jsonb_typeof(p.payload -> 'pack_details' -> 'items') = 'array'
                                    THEN p.payload -> 'pack_details' -> 'items' ELSE '[]'::jsonb END
            ELSE '[]'::jsonb
        END
    ) AS e(elem)
    CROSS JOIN LATERAL (
        SELECT CASE WHEN vt ~ '^[0-9]+$' THEN vt::bigint END AS variant_id
        FROM (SELECT coalesce(e.elem -> 'variant' ->> 'id', e.elem ->> 'variantId', e.elem ->> 'variant_id', e.elem ->> 'variantPromoId') AS vt) s
    ) x
    JOIN bsale_raw.variants cv ON cv.company_id = 3 AND cv.bsale_id = x.variant_id AND cv.missing_since IS NULL
    JOIN bsale_raw.products cp ON cp.company_id = 3 AND cp.bsale_id = cv.product_id AND cp.classification = 3
    WHERE p.company_id = 3 AND p.classification = 3 AND p.missing_since IS NULL
),
walk AS (
    SELECT pack_variant_id AS root, component_variant_id AS node, ARRAY[pack_variant_id, component_variant_id] AS path,
           pack_variant_id = component_variant_id AS cycle, 1 AS depth
    FROM edges
    UNION ALL
    SELECT w.root, e.component_variant_id, w.path || e.component_variant_id,
           e.component_variant_id = ANY (w.path), w.depth + 1
    FROM walk w
    JOIN edges e ON e.pack_variant_id = w.node
    WHERE NOT w.cycle AND w.depth < 6
)
SELECT root AS pack_variant_id, max(depth) AS profundidad_max, bool_or(cycle) AS ciclo,
       (array_agg(path ORDER BY cycle DESC, depth DESC))[1] AS ejemplo_ruta
FROM walk
GROUP BY root
ORDER BY bool_or(cycle) DESC, max(depth) DESC, root;


-- C5. Detalle de packs no calculables (hasta 200) con el primer motivo y los componentes afectados.
WITH packs AS (
    SELECT p.bsale_id AS product_id, p.name, p.state, p.payload -> 'pack_details' AS pd
    FROM bsale_raw.products p
    WHERE p.company_id = 3 AND p.classification = 3 AND p.missing_since IS NULL
),
typed AS (
    SELECT k.product_id,
           CASE WHEN x.vt ~ '^[0-9]+$' THEN nullif(x.vt::bigint, 0) END AS variant_id,
           CASE WHEN x.qt ~ '^[0-9]+(\.[0-9]+)?$' THEN x.qt::numeric END AS quantity,
           coalesce(e.elem ->> 'multipleVariant', e.elem ->> 'multiple_variant') = '1' AS multiple
    FROM packs k
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE jsonb_typeof(k.pd)
            WHEN 'array' THEN k.pd
            WHEN 'object' THEN CASE WHEN jsonb_typeof(k.pd -> 'items') = 'array' THEN k.pd -> 'items' ELSE '[]'::jsonb END
            ELSE '[]'::jsonb
        END
    ) AS e(elem)
    CROSS JOIN LATERAL (
        SELECT coalesce(e.elem -> 'variant' ->> 'id', e.elem ->> 'variantId', e.elem ->> 'variant_id', e.elem ->> 'variantPromoId') AS vt,
               e.elem ->> 'quantity' AS qt
    ) x
    WHERE coalesce(e.elem ->> 'state', '0') = '0'
)
SELECT k.product_id, k.name AS pack, k.state AS estado_pack,
       array_agg(t.variant_id ORDER BY t.variant_id) FILTER (WHERE t.variant_id IS NULL OR t.multiple) AS abiertos,
       array_agg(t.variant_id ORDER BY t.variant_id) FILTER (WHERE t.variant_id IS NOT NULL AND v.bsale_id IS NULL) AS inexistentes,
       array_agg(t.variant_id ORDER BY t.variant_id) FILTER (WHERE cp.classification = 3) AS packs_anidados,
       array_agg(t.variant_id ORDER BY t.variant_id) FILTER (WHERE v.bsale_id IS NOT NULL AND cp.classification IS DISTINCT FROM 3
                                                          AND (vc.average_cost IS NULL OR vc.average_cost <= 0)) AS sin_costo,
       array_agg(t.variant_id ORDER BY t.variant_id) FILTER (WHERE t.quantity IS NULL OR t.quantity <= 0) AS cantidad_invalida
FROM packs k
LEFT JOIN typed t ON t.product_id = k.product_id
LEFT JOIN bsale_raw.variants v ON v.company_id = 3 AND v.bsale_id = t.variant_id AND v.missing_since IS NULL
LEFT JOIN bsale_raw.products cp ON cp.company_id = 3 AND cp.bsale_id = v.product_id
LEFT JOIN bsale_raw.variant_costs vc ON vc.company_id = 3 AND vc.variant_id = v.bsale_id
GROUP BY k.product_id, k.name, k.state
HAVING count(t.product_id) = 0
    OR count(*) FILTER (WHERE t.variant_id IS NULL OR t.multiple OR t.quantity IS NULL OR t.quantity <= 0) > 0
    OR count(*) FILTER (WHERE t.variant_id IS NOT NULL AND v.bsale_id IS NULL) > 0
    OR count(*) FILTER (WHERE cp.classification = 3) > 0
    OR count(*) FILTER (WHERE v.bsale_id IS NOT NULL AND cp.classification IS DISTINCT FROM 3
                         AND (vc.average_cost IS NULL OR vc.average_cost <= 0)) > 0
ORDER BY k.state, k.product_id
LIMIT 200;


-- C6. Frescura: composición (products), costos de componentes y estado de sincronización RAW.
SELECT 'products_pack' AS fuente, min(p.api_fetched_at) AS min_fetched, max(p.api_fetched_at) AS max_fetched, count(*) AS filas
FROM bsale_raw.products p
WHERE p.company_id = 3 AND p.classification = 3 AND p.missing_since IS NULL
UNION ALL
SELECT 'variant_costs', min(vc.api_fetched_at), max(vc.api_fetched_at), count(*)
FROM bsale_raw.variant_costs vc
WHERE vc.company_id = 3
UNION ALL
SELECT 'sync_state:' || s.resource || ':' || s.scope || ':' || coalesce(s.status, '?'), s.last_success_at, s.last_attempt_at, NULL
FROM bsale_raw.sync_state s
WHERE s.company_id = 3 AND s.resource IN ('products', 'variant_costs', 'variant_prices', 'product_taxes', 'variants')
ORDER BY 1;


-- C7. Ejemplo pack 5646 (Mistral 35° + Sprite + hielo): componentes, cantidad, costo e impuestos por id.
WITH pack AS (
    SELECT p.bsale_id AS product_id, p.payload -> 'pack_details' AS pd
    FROM bsale_raw.products p
    WHERE p.company_id = 3 AND p.bsale_id = 5646
),
typed AS (
    SELECT CASE WHEN x.vt ~ '^[0-9]+$' THEN x.vt::bigint END AS variant_id,
           e.elem ->> 'quantity' AS quantity,
           coalesce(e.elem ->> 'multipleVariant', e.elem ->> 'multiple_variant') AS multiple_variant
    FROM pack k
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE jsonb_typeof(k.pd)
            WHEN 'array' THEN k.pd
            WHEN 'object' THEN CASE WHEN jsonb_typeof(k.pd -> 'items') = 'array' THEN k.pd -> 'items' ELSE '[]'::jsonb END
            ELSE '[]'::jsonb
        END
    ) AS e(elem)
    CROSS JOIN LATERAL (
        SELECT coalesce(e.elem -> 'variant' ->> 'id', e.elem ->> 'variantId', e.elem ->> 'variant_id', e.elem ->> 'variantPromoId') AS vt
    ) x
)
SELECT t.variant_id, t.quantity, t.multiple_variant,
       v.description AS variante, cp.bsale_id AS product_id, cp.name AS producto, cp.classification,
       vc.average_cost AS costo_promedio_neto, vc.api_fetched_at AS costo_fetched_at,
       ptx.tax_ids,
       (SELECT string_agg(tx.bsale_id || ':' || coalesce(btrim(tx.code), '-') || ':' || tx.percentage, ' + ' ORDER BY tx.bsale_id)
          FROM bsale_raw.taxes tx
         WHERE tx.company_id = 3 AND tx.bsale_id = ANY (ptx.tax_ids)) AS impuestos_id_codigo_tasa
FROM typed t
LEFT JOIN bsale_raw.variants v ON v.company_id = 3 AND v.bsale_id = t.variant_id
LEFT JOIN bsale_raw.products cp ON cp.company_id = 3 AND cp.bsale_id = v.product_id
LEFT JOIN bsale_raw.variant_costs vc ON vc.company_id = 3 AND vc.variant_id = t.variant_id
LEFT JOIN bsale_raw.product_taxes ptx ON ptx.company_id = 3 AND ptx.product_id = cp.bsale_id
ORDER BY t.variant_id;


-- C8. Caso 8171 (Royal Guard Pacific IPA x6): clasificación y precios RAW en listas 12, 13 y 16.
SELECT p.bsale_id AS product_id, p.name, p.classification, p.state,
       (p.payload -> 'pack_details') IS NOT NULL AS tiene_pack_details,
       v.bsale_id AS variant_id, v.description AS variante,
       vp.price_list_id, vp.variant_value AS precio_neto, vp.variant_value_with_taxes AS precio_bruto,
       vp.missing_since, vp.api_fetched_at
FROM bsale_raw.products p
JOIN bsale_raw.variants v ON v.company_id = 3 AND v.product_id = p.bsale_id
LEFT JOIN bsale_raw.variant_prices vp
  ON vp.company_id = 3 AND vp.variant_id = v.bsale_id AND vp.price_list_id IN (12, 13, 16)
WHERE p.company_id = 3 AND p.bsale_id = 8171
ORDER BY v.bsale_id, vp.price_list_id;
