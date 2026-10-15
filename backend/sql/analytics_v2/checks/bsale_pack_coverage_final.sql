-- =============================================================================
-- Analítica V2 — Cobertura final de packs Bsale (company_id = 3). SÓLO LECTURA.
--
-- Un único SELECT: ejecutar el archivo completo en pgAdmin. Devuelve una sola tabla con secciones:
--   0. Base                      universo y forma de pack_details
--   1. Estado final del motor    un estado por pack activo (excluyentes; suman el total de activos)
--   2. Potencialmente calculables (grupo "calculable_si_impuestos_resueltos" de C3)
--   3. Bloqueo tributario        motivo principal por pack + componentes que lo causan
--   4. Costo neto y bruto        packs con costo por componentes (provisional)
--   5. Componentes               los que explican los packs C3 "sin costo" e "inexistentes"
--   6. C3 vs motor               comparación pack a pack del estado estimado (C3) y el del motor
--
-- Replica en SQL las reglas de:
--   backend/services/analytics_v2/pack_engine.py  (parse_pack_details, PackCostEngine, _LINE_TO_PACK)
--   backend/services/analytics_v2/tax_profile.py  (classify_tax, resolve_tax_profile, commercial_gross_cost)
-- con la configuración vigente: sin roles tributarios confirmados por id, sin exentos verificados,
-- base de costo = costo promedio Bsale (variant_costs.average_cost), máx. 4 niveles de anidamiento.
-- Impuestos: solo por id Bsale + código SII; nunca por nombre. Componentes: solo por id de variante.
-- No modifica RAW, costos, impuestos, precios ni crons.
-- =============================================================================
WITH RECURSIVE
cfg AS (SELECT 3::bigint AS company_id),

-- ---------------------------------------------------------------- motor tributario
sii (code, role, legal_rate) AS (VALUES
    ('14', 'iva', 19::numeric),
    ('17', 'iva_anticipado', 5), ('18', 'iva_anticipado', 5), ('19', 'iva_anticipado', 12),
    ('24', 'ila', 31.5), ('25', 'ila', 20.5), ('26', 'ila', 20.5), ('27', 'ila', 10), ('271', 'ila', 18)
),
tax_cat AS (
    SELECT t.bsale_id AS tax_id,
           nullif(btrim(t.code, E' \t\r\n'), '') AS code,
           t.percentage,
           coalesce(s.role, 'desconocido') AS role,
           (s.code IS NOT NULL AND t.percentage IS NOT NULL AND t.percentage <> s.legal_rate) AS rate_mismatch,
           (t.percentage IS NULL) AS null_rate,
           (t.state IS NOT NULL AND t.state <> 0) AS inactive,
           (t.missing_since IS NOT NULL) AS retired
    FROM bsale_raw.taxes t
    JOIN cfg ON t.company_id = cfg.company_id
    LEFT JOIN sii s ON s.code = nullif(btrim(t.code, E' \t\r\n'), '')
),
tax_rank (tax_status, rnk, descripcion) AS (VALUES
    ('no_consultado', 1, 'Producto componente sin fila en bsale_raw.product_taxes'),
    ('sin_impuestos', 2, 'Bsale no informa impuestos del componente (no se asume exento)'),
    ('impuesto_no_reconocido', 3, 'Impuesto fuera del catálogo RAW o sin código SII reconocido'),
    ('configuracion_inconsistente', 4, 'Tasa distinta al código SII, impuesto inactivo/retirado, IVA duplicado, ILA sin IVA o configuración retirada'),
    ('ila_combinado', 5, 'Más de un ILA en el mismo componente'),
    ('iva_anticipado', 6, 'IVA anticipado (harina/carne): tratamiento contable pendiente')
),

-- ---------------------------------------------------------------- packs y pack_details
packs AS (
    SELECT p.bsale_id AS pack_product_id, p.name AS pack_name, p.state AS pack_state,
           p.payload -> 'pack_details' AS pd
    FROM bsale_raw.products p
    JOIN cfg ON p.company_id = cfg.company_id
    WHERE p.classification = 3 AND p.missing_since IS NULL
),
shape AS (
    SELECT k.*,
           CASE
               WHEN k.pd IS NULL OR jsonb_typeof(k.pd) = 'null' THEN 'ausente'
               WHEN jsonb_typeof(k.pd) = 'array' THEN 'lista'
               WHEN jsonb_typeof(k.pd) = 'object' AND k.pd ? 'items' THEN
                    CASE WHEN jsonb_typeof(k.pd -> 'items') = 'array' THEN 'objeto_items' ELSE 'no_interpretable' END
               WHEN jsonb_typeof(k.pd) = 'object' AND k.pd ? 'href' THEN 'no_expandido'
               WHEN jsonb_typeof(k.pd) = 'string' THEN 'texto_json'
               ELSE 'no_interpretable'
           END AS shape,
           CASE jsonb_typeof(k.pd)
               WHEN 'array' THEN k.pd
               WHEN 'object' THEN CASE WHEN jsonb_typeof(k.pd -> 'items') = 'array' THEN k.pd -> 'items' END
           END AS arr
    FROM packs k
),
elems AS (
    SELECT s.pack_product_id, e.ord, e.elem
    FROM shape s
    CROSS JOIN LATERAL jsonb_array_elements(coalesce(s.arr, '[]'::jsonb)) WITH ORDINALITY AS e(elem, ord)
),
fields AS (
    SELECT el.pack_product_id, el.ord, el.elem,
           (jsonb_typeof(el.elem) = 'object') AS is_object,
           CASE
               WHEN jsonb_typeof(el.elem) <> 'object' THEN NULL
               WHEN jsonb_typeof(el.elem -> 'state') IS NULL OR jsonb_typeof(el.elem -> 'state') = 'null' THEN true
               WHEN jsonb_typeof(el.elem -> 'state') = 'number' THEN (el.elem ->> 'state')::numeric = 0
               WHEN jsonb_typeof(el.elem -> 'state') = 'string' THEN el.elem ->> 'state' = '0'
               WHEN jsonb_typeof(el.elem -> 'state') = 'boolean' THEN el.elem ->> 'state' = 'false'
               ELSE false
           END AS detail_active,
           CASE WHEN jsonb_typeof(el.elem -> 'quantity') IN ('number', 'string')
                THEN el.elem -> 'quantity' #>> '{}' END AS qty_txt,
           CASE WHEN jsonb_typeof(el.elem) = 'object' THEN
                CASE WHEN el.elem ? 'multipleVariant' THEN el.elem -> 'multipleVariant' ELSE el.elem -> 'multiple_variant' END
           END AS mv
    FROM elems el
),
keyvals AS (
    SELECT f.pack_product_id, f.ord, kv.kind, kv.pos,
           CASE WHEN jsonb_typeof(kv.raw) = 'object' THEN kv.raw -> 'id' ELSE kv.raw END AS val
    FROM fields f
    CROSS JOIN LATERAL (VALUES
        ('variant', 1, f.elem -> 'variant'), ('variant', 2, f.elem -> 'variantId'),
        ('variant', 3, f.elem -> 'variant_id'), ('variant', 4, f.elem -> 'variantPromoId'),
        ('product', 1, f.elem -> 'product'), ('product', 2, f.elem -> 'productId'),
        ('product', 3, f.elem -> 'product_id'), ('product', 4, f.elem -> 'productPromoId')
    ) AS kv(kind, pos, raw)
    WHERE f.is_object AND kv.raw IS NOT NULL
),
ids AS (
    SELECT pack_product_id, ord, kind, pos,
           CASE WHEN jsonb_typeof(val) IN ('number', 'string')
                 AND btrim(val #>> '{}', E' \t\r\n') ~ '^\+?[0-9]{1,18}$'
                THEN nullif(btrim(val #>> '{}', E' \t\r\n')::numeric, 0)::bigint END AS id
    FROM keyvals
),
detail_class AS (
    SELECT f.pack_product_id, f.ord,
           (SELECT i.id FROM ids i WHERE i.pack_product_id = f.pack_product_id AND i.ord = f.ord
               AND i.kind = 'variant' AND i.id IS NOT NULL ORDER BY i.pos LIMIT 1) AS variant_id,
           (SELECT i.id FROM ids i WHERE i.pack_product_id = f.pack_product_id AND i.ord = f.ord
               AND i.kind = 'product' AND i.id IS NOT NULL ORDER BY i.pos LIMIT 1) AS product_ref,
           CASE WHEN f.qty_txt ~ '^\s*[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][+-]?[0-9]{1,3})?\s*$'
                THEN CASE WHEN f.qty_txt::numeric > 0 THEN f.qty_txt::numeric END END AS quantity,
           CASE jsonb_typeof(f.mv)
               WHEN 'boolean' THEN f.mv = 'true'::jsonb
               WHEN 'number' THEN trunc((f.mv #>> '{}')::numeric) = 1
               WHEN 'string' THEN btrim(f.mv #>> '{}', E' \t\r\n') ~ '^\+?0*1$'
               ELSE false
           END AS multiple,
           f.is_object, f.detail_active
    FROM fields f
),
detail_parsed AS (
    SELECT d.*,
           CASE
               WHEN NOT d.is_object THEN 'detalle_no_interpretable'
               WHEN NOT d.detail_active THEN 'detalle_inactivo_excluido'
               WHEN d.variant_id IS NULL AND d.product_ref IS NULL THEN 'detalle_no_interpretable'
               WHEN d.quantity IS NULL THEN 'cantidad_invalida'
               ELSE 'componente'
           END AS parse_class
    FROM detail_class d
),
comp_block AS (
    SELECT s.pack_product_id, s.pack_name, s.pack_state, s.shape,
           CASE
               WHEN s.shape = 'texto_json' THEN 'no_verificable_en_sql'
               WHEN s.shape IN ('ausente', 'no_expandido') THEN 'sin_composicion'
               WHEN s.shape = 'no_interpretable' THEN 'composicion_no_interpretable'
               WHEN count(*) FILTER (WHERE dp.parse_class IN ('detalle_no_interpretable', 'cantidad_invalida')) > 0
                   THEN 'composicion_no_interpretable'
               WHEN count(*) FILTER (WHERE dp.parse_class = 'componente') = 0 THEN 'sin_composicion'
           END AS block_status
    FROM shape s
    LEFT JOIN detail_parsed dp ON dp.pack_product_id = s.pack_product_id
    GROUP BY s.pack_product_id, s.pack_name, s.pack_state, s.shape
),

-- ---------------------------------------------------------------- hechos de los componentes
ref_variants AS (
    SELECT DISTINCT variant_id FROM detail_parsed WHERE parse_class = 'componente' AND variant_id IS NOT NULL
),
vfacts AS (
    SELECT v.bsale_id AS variant_id, v.product_id, cp.name AS product_name, cp.classification,
           vc.average_cost, (vc.variant_id IS NOT NULL) AS has_cost_row, vc.history_count,
           ptx.tax_ids, (ptx.product_id IS NOT NULL) AS has_tax_row, (ptx.missing_since IS NOT NULL) AS tax_stale
    FROM bsale_raw.variants v
    JOIN cfg ON v.company_id = cfg.company_id
    JOIN ref_variants r ON r.variant_id = v.bsale_id
    LEFT JOIN bsale_raw.products cp ON cp.company_id = v.company_id AND cp.bsale_id = v.product_id
    LEFT JOIN bsale_raw.variant_costs vc ON vc.company_id = v.company_id AND vc.variant_id = v.bsale_id
    LEFT JOIN bsale_raw.product_taxes ptx ON ptx.company_id = cp.company_id AND ptx.product_id = cp.bsale_id
    WHERE v.missing_since IS NULL
),
vtax AS (
    SELECT f.*, x.taxes_txt, x.iva_rate, x.ila_rate,
           CASE
               WHEN NOT f.has_tax_row OR f.tax_ids IS NULL THEN 'no_consultado'
               WHEN f.tax_stale THEN 'configuracion_inconsistente'
               WHEN x.n_ids = 0 THEN 'sin_impuestos'
               WHEN x.n_missing > 0 OR x.n_unknown > 0 THEN 'impuesto_no_reconocido'
               WHEN x.n_blocking > 0 THEN 'configuracion_inconsistente'
               WHEN x.n_anticipated > 0 THEN 'iva_anticipado'
               WHEN x.n_ila >= 2 THEN 'ila_combinado'
               WHEN x.n_iva <> 1 THEN 'configuracion_inconsistente'
               WHEN x.n_ila = 1 THEN 'iva_ila'
               ELSE 'iva'
           END AS tax_status
    FROM vfacts f
    LEFT JOIN LATERAL (
        SELECT count(*) AS n_ids,
               count(*) FILTER (WHERE c.tax_id IS NULL) AS n_missing,
               count(*) FILTER (WHERE c.role = 'desconocido') AS n_unknown,
               count(*) FILTER (WHERE c.rate_mismatch OR c.null_rate OR c.inactive OR c.retired) AS n_blocking,
               count(*) FILTER (WHERE c.role = 'iva_anticipado') AS n_anticipated,
               count(*) FILTER (WHERE c.role = 'ila') AS n_ila,
               count(*) FILTER (WHERE c.role = 'iva') AS n_iva,
               max(c.percentage) FILTER (WHERE c.role = 'iva') AS iva_rate,
               max(c.percentage) FILTER (WHERE c.role = 'ila') AS ila_rate,
               string_agg(u.tax_id::text || ' [SII ' || coalesce(c.code, 'sin código') || ', '
                          || coalesce(c.percentage::text, 'sin tasa') || '%]', ' + ' ORDER BY u.tax_id) AS taxes_txt
        FROM (SELECT DISTINCT t.tax_id FROM unnest(f.tax_ids) AS t(tax_id)) u
        LEFT JOIN tax_cat c ON c.tax_id = u.tax_id
    ) x ON true
),
vline AS (
    SELECT t.*,
           CASE WHEN t.tax_status IN ('iva', 'iva_ila')
                THEN 1 + (t.iva_rate + coalesce(t.ila_rate, 0)) / 100 END AS factor
    FROM vtax t
),

-- ---------------------------------------------------------------- líneas del motor
lines0 AS (
    SELECT dp.pack_product_id, dp.ord, dp.variant_id, dp.quantity,
           f.product_id AS component_product_id, f.product_name AS component_name,
           f.average_cost, f.has_cost_row, f.history_count, f.tax_status, f.taxes_txt, f.factor,
           (cbs.pack_product_id IS NOT NULL) AS sub_in_packs,
           CASE
               WHEN dp.multiple OR dp.variant_id IS NULL THEN 'variante_no_determinada'
               WHEN f.variant_id IS NULL THEN 'inexistente'
               WHEN f.classification = 3 THEN 'anidado'
               WHEN f.average_cost IS NULL OR f.average_cost <= 0 THEN 'sin_costo'
               WHEN f.tax_status NOT IN ('iva', 'iva_ila') THEN 'requiere_revision_tributaria'
               ELSE 'calculado'
           END AS base_status
    FROM detail_parsed dp
    JOIN comp_block cb ON cb.pack_product_id = dp.pack_product_id AND cb.block_status IS NULL
    LEFT JOIN vline f ON f.variant_id = dp.variant_id
    LEFT JOIN comp_block cbs ON cbs.pack_product_id = f.product_id
    WHERE dp.parse_class = 'componente'
),
lines AS (
    SELECT l.*,
           CASE WHEN l.base_status = 'calculado' THEN l.average_cost * l.quantity END AS net,
           CASE WHEN l.base_status = 'calculado' THEN l.average_cost * l.factor * l.quantity END AS gross,
           count(*) OVER (PARTITION BY l.pack_product_id) AS n_lines
    FROM lines0 l
),
line_rank (line_status, rnk, pack_status) AS (VALUES
    ('ciclo', 1, 'ciclo_detectado'),
    ('variante_no_determinada', 2, 'pack_abierto'),
    ('inexistente', 3, 'componente_inexistente'),
    ('anidamiento_excesivo', 4, 'componente_pack_no_calculable'),
    ('pack_no_calculable', 5, 'componente_pack_no_calculable'),
    ('requiere_revision_tributaria', 6, 'componente_requiere_revision_tributaria'),
    ('sin_costo', 7, 'componente_sin_costo'),
    ('calculado', 8, 'calculado')
),

-- Resolución por niveles: nivel 1 = packs sin packs anidados; nivel k = packs cuyos packs anidados
-- se resolvieron en k-1. Cuatro niveles cubren todo lo que el motor resuelve sin tocar el límite.
st0 AS (
    SELECT pack_product_id, block_status AS status, NULL::numeric AS net, NULL::numeric AS gross, 0 AS lvl
    FROM comp_block WHERE block_status IS NOT NULL
),
r1 AS (
    SELECT l.pack_product_id, l.n_lines,
           CASE WHEN l.base_status <> 'anidado' THEN l.base_status
                WHEN NOT l.sub_in_packs THEN 'pack_no_calculable'
                WHEN s.pack_product_id IS NULL THEN NULL
                WHEN s.status = 'calculado' THEN 'calculado'
                WHEN s.status = 'ciclo_detectado' THEN 'ciclo'
                ELSE 'pack_no_calculable' END AS ls,
           CASE WHEN l.base_status <> 'anidado' THEN l.net ELSE s.net * l.quantity END AS net,
           CASE WHEN l.base_status <> 'anidado' THEN l.gross ELSE s.gross * l.quantity END AS gross
    FROM lines l LEFT JOIN st0 s ON s.pack_product_id = l.component_product_id
),
st1 AS (
    SELECT * FROM st0
    UNION ALL
    SELECT r.pack_product_id, (array_agg(lr.pack_status ORDER BY lr.rnk))[1],
           CASE WHEN bool_and(r.ls = 'calculado') THEN sum(r.net) END,
           CASE WHEN bool_and(r.ls = 'calculado') THEN sum(r.gross) END, 1
    FROM r1 r JOIN line_rank lr ON lr.line_status = r.ls
    WHERE NOT EXISTS (SELECT 1 FROM st0 p WHERE p.pack_product_id = r.pack_product_id)
    GROUP BY r.pack_product_id
    HAVING count(*) = max(r.n_lines)
),
r2 AS (
    SELECT l.pack_product_id, l.n_lines,
           CASE WHEN l.base_status <> 'anidado' THEN l.base_status
                WHEN NOT l.sub_in_packs THEN 'pack_no_calculable'
                WHEN s.pack_product_id IS NULL THEN NULL
                WHEN s.status = 'calculado' THEN 'calculado'
                WHEN s.status = 'ciclo_detectado' THEN 'ciclo'
                ELSE 'pack_no_calculable' END AS ls,
           CASE WHEN l.base_status <> 'anidado' THEN l.net ELSE s.net * l.quantity END AS net,
           CASE WHEN l.base_status <> 'anidado' THEN l.gross ELSE s.gross * l.quantity END AS gross
    FROM lines l LEFT JOIN st1 s ON s.pack_product_id = l.component_product_id
),
st2 AS (
    SELECT * FROM st1
    UNION ALL
    SELECT r.pack_product_id, (array_agg(lr.pack_status ORDER BY lr.rnk))[1],
           CASE WHEN bool_and(r.ls = 'calculado') THEN sum(r.net) END,
           CASE WHEN bool_and(r.ls = 'calculado') THEN sum(r.gross) END, 2
    FROM r2 r JOIN line_rank lr ON lr.line_status = r.ls
    WHERE NOT EXISTS (SELECT 1 FROM st1 p WHERE p.pack_product_id = r.pack_product_id)
    GROUP BY r.pack_product_id
    HAVING count(*) = max(r.n_lines)
),
r3 AS (
    SELECT l.pack_product_id, l.n_lines,
           CASE WHEN l.base_status <> 'anidado' THEN l.base_status
                WHEN NOT l.sub_in_packs THEN 'pack_no_calculable'
                WHEN s.pack_product_id IS NULL THEN NULL
                WHEN s.status = 'calculado' THEN 'calculado'
                WHEN s.status = 'ciclo_detectado' THEN 'ciclo'
                ELSE 'pack_no_calculable' END AS ls,
           CASE WHEN l.base_status <> 'anidado' THEN l.net ELSE s.net * l.quantity END AS net,
           CASE WHEN l.base_status <> 'anidado' THEN l.gross ELSE s.gross * l.quantity END AS gross
    FROM lines l LEFT JOIN st2 s ON s.pack_product_id = l.component_product_id
),
st3 AS (
    SELECT * FROM st2
    UNION ALL
    SELECT r.pack_product_id, (array_agg(lr.pack_status ORDER BY lr.rnk))[1],
           CASE WHEN bool_and(r.ls = 'calculado') THEN sum(r.net) END,
           CASE WHEN bool_and(r.ls = 'calculado') THEN sum(r.gross) END, 3
    FROM r3 r JOIN line_rank lr ON lr.line_status = r.ls
    WHERE NOT EXISTS (SELECT 1 FROM st2 p WHERE p.pack_product_id = r.pack_product_id)
    GROUP BY r.pack_product_id
    HAVING count(*) = max(r.n_lines)
),
r4 AS (
    SELECT l.pack_product_id, l.n_lines,
           CASE WHEN l.base_status <> 'anidado' THEN l.base_status
                WHEN NOT l.sub_in_packs THEN 'pack_no_calculable'
                WHEN s.pack_product_id IS NULL THEN NULL
                WHEN s.status = 'calculado' THEN 'calculado'
                WHEN s.status = 'ciclo_detectado' THEN 'ciclo'
                ELSE 'pack_no_calculable' END AS ls,
           CASE WHEN l.base_status <> 'anidado' THEN l.net ELSE s.net * l.quantity END AS net,
           CASE WHEN l.base_status <> 'anidado' THEN l.gross ELSE s.gross * l.quantity END AS gross
    FROM lines l LEFT JOIN st3 s ON s.pack_product_id = l.component_product_id
),
st4 AS (
    SELECT * FROM st3
    UNION ALL
    SELECT r.pack_product_id, (array_agg(lr.pack_status ORDER BY lr.rnk))[1],
           CASE WHEN bool_and(r.ls = 'calculado') THEN sum(r.net) END,
           CASE WHEN bool_and(r.ls = 'calculado') THEN sum(r.gross) END, 4
    FROM r4 r JOIN line_rank lr ON lr.line_status = r.ls
    WHERE NOT EXISTS (SELECT 1 FROM st3 p WHERE p.pack_product_id = r.pack_product_id)
    GROUP BY r.pack_product_id
    HAVING count(*) = max(r.n_lines)
),
-- Lo que no se resolvió en 4 niveles: ciclo (si se alcanza dentro del límite) o anidamiento excesivo.
edges AS (
    SELECT DISTINCT pack_product_id AS src, component_product_id AS dst
    FROM lines WHERE base_status = 'anidado' AND sub_in_packs
),
walk AS (
    SELECT e.src AS root, e.dst AS node, ARRAY[e.src, e.dst] AS path, (e.src = e.dst) AS cyc, 1 AS depth
    FROM edges e
    UNION ALL
    SELECT w.root, e.dst, w.path || e.dst, e.dst = ANY (w.path), w.depth + 1
    FROM walk w JOIN edges e ON e.src = w.node
    WHERE NOT w.cyc AND w.depth < 4
),
r5 AS (
    SELECT l.pack_product_id,
           CASE WHEN l.base_status <> 'anidado' THEN l.base_status
                WHEN NOT l.sub_in_packs THEN 'pack_no_calculable'
                WHEN s.pack_product_id IS NULL THEN 'pack_no_calculable'
                WHEN s.status = 'calculado' THEN 'calculado'
                WHEN s.status = 'ciclo_detectado' THEN 'ciclo'
                ELSE 'pack_no_calculable' END AS ls
    FROM lines l LEFT JOIN st4 s ON s.pack_product_id = l.component_product_id
    WHERE NOT EXISTS (SELECT 1 FROM st4 p WHERE p.pack_product_id = l.pack_product_id)
),
fallback AS (
    SELECT r.pack_product_id,
           CASE WHEN EXISTS (SELECT 1 FROM walk w WHERE w.root = r.pack_product_id AND w.cyc) THEN 'ciclo_detectado'
                ELSE (array_agg(lr.pack_status ORDER BY lr.rnk))[1] END AS status
    FROM r5 r JOIN line_rank lr ON lr.line_status = r.ls
    GROUP BY r.pack_product_id
),
final_status AS (
    SELECT pack_product_id, status, net, gross, false AS approx FROM st4
    UNION ALL
    SELECT pack_product_id, status, NULL, NULL, true FROM fallback
),

-- ---------------------------------------------------------------- C3 original (estimación previa)
c3_typed AS (
    SELECT k.pack_product_id,
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
),
c3 AS (
    SELECT k.pack_product_id,
           CASE
               WHEN count(t.pack_product_id) = 0 THEN 'sin_composicion'
               WHEN count(*) FILTER (WHERE t.quantity IS NULL OR t.quantity <= 0) > 0 THEN 'composicion_no_interpretable'
               WHEN count(*) FILTER (WHERE t.variant_id IS NULL OR t.multiple) > 0 THEN 'pack_abierto'
               WHEN count(*) FILTER (WHERE t.variant_id IS NOT NULL AND v.bsale_id IS NULL) > 0 THEN 'componente_inexistente'
               WHEN count(*) FILTER (WHERE cp.classification = 3) > 0 THEN 'pack_anidado_revisar_en_motor'
               WHEN count(*) FILTER (WHERE v.bsale_id IS NOT NULL AND cp.classification IS DISTINCT FROM 3
                                       AND (vc.average_cost IS NULL OR vc.average_cost <= 0)) > 0 THEN 'componente_sin_costo'
               ELSE 'calculable_si_impuestos_resueltos'
           END AS c3_status
    FROM packs k
    LEFT JOIN c3_typed t ON t.pack_product_id = k.pack_product_id
    LEFT JOIN bsale_raw.variants v ON v.company_id = 3 AND v.bsale_id = t.variant_id AND v.missing_since IS NULL
    LEFT JOIN bsale_raw.products cp ON cp.company_id = 3 AND cp.bsale_id = v.product_id
    LEFT JOIN bsale_raw.variant_costs vc ON vc.company_id = 3 AND vc.variant_id = v.bsale_id
    GROUP BY k.pack_product_id
),

-- ---------------------------------------------------------------- consolidado por pack
pack_variants AS (
    SELECT v.product_id, count(*) AS n_variants, count(*) FILTER (WHERE v.state = 0) AS n_active_variants
    FROM bsale_raw.variants v
    JOIN cfg ON v.company_id = cfg.company_id
    WHERE v.missing_since IS NULL AND v.product_id IN (SELECT pack_product_id FROM packs)
    GROUP BY v.product_id
),
flags AS (
    SELECT l.pack_product_id,
           bool_and(l.base_status IN ('calculado', 'requiere_revision_tributaria')) AS all_cost_ok,
           bool_and(coalesce(l.base_status IN ('calculado', 'requiere_revision_tributaria', 'sin_costo')
                             AND l.tax_status IN ('iva', 'iva_ila'), false)) AS all_tax_ok,
           bool_or(l.base_status IN ('requiere_revision_tributaria', 'sin_costo')
                   AND l.tax_status NOT IN ('iva', 'iva_ila')) AS any_tax_issue,
           (array_agg(l.tax_status ORDER BY tr.rnk)
               FILTER (WHERE l.base_status = 'requiere_revision_tributaria'))[1] AS tax_reason,
           count(*) FILTER (WHERE l.base_status = 'anidado') AS n_nested
    FROM lines l
    LEFT JOIN tax_rank tr ON tr.tax_status = l.tax_status
    GROUP BY l.pack_product_id
),
pack_eval AS (
    SELECT cb.pack_product_id, cb.pack_name, cb.pack_state, cb.shape,
           coalesce(pv.n_variants, 0) AS n_variants,
           coalesce(pv.n_active_variants, 0) AS n_active_variants,
           CASE WHEN coalesce(pv.n_variants, 0) = 0 THEN 'no_evaluado_sin_variante_vigente' ELSE fs.status END AS engine_status,
           fs.net, fs.gross, coalesce(fs.approx, false) AS approx,
           c3.c3_status,
           coalesce(fl.all_cost_ok, false) AS all_cost_ok,
           coalesce(fl.all_tax_ok, false) AS all_tax_ok,
           coalesce(fl.any_tax_issue, false) AS any_tax_issue,
           fl.tax_reason,
           coalesce(fl.n_nested, 0) AS n_nested
    FROM comp_block cb
    LEFT JOIN pack_variants pv ON pv.product_id = cb.pack_product_id
    LEFT JOIN final_status fs ON fs.pack_product_id = cb.pack_product_id
    LEFT JOIN c3 ON c3.pack_product_id = cb.pack_product_id
    LEFT JOIN flags fl ON fl.pack_product_id = cb.pack_product_id
),
act AS (SELECT * FROM pack_eval WHERE pack_state = 0),
tot AS (
    SELECT count(*) AS n_act,
           count(*) FILTER (WHERE c3_status = 'calculable_si_impuestos_resueltos') AS n_c3,
           count(*) FILTER (WHERE c3_status = 'componente_sin_costo') AS n_c3_cost,
           count(*) FILTER (WHERE c3_status = 'componente_inexistente') AS n_c3_missing,
           count(*) FILTER (WHERE engine_status = 'componente_requiere_revision_tributaria') AS n_tax
    FROM act
),
status_desc (status, ord, descripcion) AS (VALUES
    ('calculado', 1, 'Costo neto y bruto comercial por componentes. PROVISIONAL: configuración tributaria y costo neto sin contrastar contablemente'),
    ('componente_requiere_revision_tributaria', 2, 'Algún componente con costo no tiene IVA/ILA calculable por id Bsale y código SII'),
    ('componente_sin_costo', 3, 'Algún componente sin costo promedio Bsale > 0'),
    ('componente_inexistente', 4, 'Algún componente referencia una variante inexistente o retirada en RAW'),
    ('componente_pack_no_calculable', 5, 'Contiene un pack no calculable o supera 4 niveles de anidamiento'),
    ('pack_abierto', 6, 'Detalle con multipleVariant o sin variante determinada'),
    ('ciclo_detectado', 7, 'El pack se contiene a sí mismo directa o indirectamente'),
    ('composicion_no_interpretable', 8, 'Detalle o cantidad inválidos en pack_details'),
    ('sin_composicion', 9, 'pack_details ausente, vacío, no expandido o con todos sus detalles inactivos'),
    ('no_verificable_en_sql', 10, 'pack_details guardado como texto JSON: verificar en el motor'),
    ('no_evaluado_sin_variante_vigente', 11, 'Pack sin variantes vigentes en RAW: el motor no lo evalúa')
),
ghost AS (
    SELECT r.variant_id, gv.bsale_id IS NOT NULL AS in_raw, gv.missing_since, gv.product_id, gp.name AS product_name
    FROM (SELECT DISTINCT variant_id FROM lines0 WHERE base_status = 'inexistente') r
    CROSS JOIN cfg
    LEFT JOIN bsale_raw.variants gv ON gv.company_id = cfg.company_id AND gv.bsale_id = r.variant_id
    LEFT JOIN bsale_raw.products gp ON gp.company_id = cfg.company_id AND gp.bsale_id = gv.product_id
),
report AS (
    -- 0. Base -----------------------------------------------------------------------------------
    SELECT '0. Base' AS seccion, 1 AS orden, 'Packs classification = 3 vigentes en RAW' AS indicador,
           count(*)::bigint AS packs, NULL::numeric AS pct_activos, NULL::numeric AS pct_grupo,
           'products.missing_since IS NULL (activos e inactivos)' AS detalle
    FROM pack_eval
    UNION ALL
    SELECT '0. Base', 2, 'Packs activos (products.state = 0)', n_act, 100.0, NULL,
           'Universo de los porcentajes pct_activos' FROM tot
    UNION ALL
    SELECT '0. Base', 3, 'Activos con al menos una variante vigente (evaluados por el motor)',
           count(*) FILTER (WHERE n_variants > 0),
           round(100.0 * count(*) FILTER (WHERE n_variants > 0) / nullif((SELECT n_act FROM tot), 0), 1), NULL,
           'El motor evalúa cada variante vigente del pack; todas comparten la misma composición' FROM act
    UNION ALL
    SELECT '0. Base', 4, 'Activos con al menos una variante activa (estado "activo" en la vista de packs)',
           count(*) FILTER (WHERE n_active_variants > 0),
           round(100.0 * count(*) FILTER (WHERE n_active_variants > 0) / nullif((SELECT n_act FROM tot), 0), 1), NULL,
           'La vista Rentabilidad de packs muestra por defecto variante activa + producto activo' FROM act
    UNION ALL
    SELECT '0. Base', 5, 'Forma de pack_details: ' || shape, count(*),
           round(100.0 * count(*) / nullif((SELECT n_act FROM tot), 0), 1), NULL,
           'lista / objeto_items: interpretables; ausente / no_expandido / no_interpretable / texto_json: no' FROM act
    GROUP BY shape
    UNION ALL
    SELECT '0. Base', 6, 'Activos que contienen packs anidados', count(*) FILTER (WHERE n_nested > 0),
           round(100.0 * count(*) FILTER (WHERE n_nested > 0) / nullif((SELECT n_act FROM tot), 0), 1), NULL,
           'Resueltos por niveles, igual que el motor' FROM act
    UNION ALL
    SELECT '0. Base', 7, 'Control: activos resueltos por la vía de ciclo / anidamiento > 4 niveles', count(*) FILTER (WHERE approx),
           NULL, NULL,
           'Si es 0, el SQL reproduce el motor sin aproximaciones' FROM act

    -- 1. Estado final del motor (excluyentes) ---------------------------------------------------
    UNION ALL
    SELECT '1. Estado final del motor (excluyentes)', coalesce(d.ord, 99), a.engine_status, count(*),
           round(100.0 * count(*) / nullif((SELECT n_act FROM tot), 0), 1), NULL, d.descripcion
    FROM act a LEFT JOIN status_desc d ON d.status = a.engine_status
    GROUP BY a.engine_status, d.ord, d.descripcion
    UNION ALL
    SELECT '1. Estado final del motor (excluyentes)', 100, 'TOTAL (debe igualar los packs activos)', count(*), 100.0, NULL,
           'Cada pack aparece en un único estado' FROM act

    -- 2. Potencialmente calculables (C3) ---------------------------------------------------------
    UNION ALL
    SELECT '2. Potencialmente calculables (C3)', 1, 'Potencialmente calculables según C3', n_c3,
           round(100.0 * n_c3 / nullif(n_act, 0), 1), 100.0,
           'C3 = cerrado, componentes existentes, sin packs anidados y con costo > 0 (sin evaluar impuestos)' FROM tot
    UNION ALL
    SELECT '2. Potencialmente calculables (C3)', 2, 'Con todos sus componentes con costo promedio Bsale > 0',
           count(*) FILTER (WHERE all_cost_ok),
           round(100.0 * count(*) FILTER (WHERE all_cost_ok) / nullif((SELECT n_act FROM tot), 0), 1),
           round(100.0 * count(*) FILTER (WHERE all_cost_ok) / nullif((SELECT n_c3 FROM tot), 0), 1),
           'Costo válido = variant_costs.average_cost > 0 de cada variante componente'
    FROM act WHERE c3_status = 'calculable_si_impuestos_resueltos'
    UNION ALL
    SELECT '2. Potencialmente calculables (C3)', 3, 'Con IVA e ILA identificados por id Bsale y código SII en todos sus componentes',
           count(*) FILTER (WHERE all_tax_ok),
           round(100.0 * count(*) FILTER (WHERE all_tax_ok) / nullif((SELECT n_act FROM tot), 0), 1),
           round(100.0 * count(*) FILTER (WHERE all_tax_ok) / nullif((SELECT n_c3 FROM tot), 0), 1),
           'Perfil IVA (SII 14) o IVA + un ILA (SII 24/25/26/27/271) con tasa legal, activo y vigente'
    FROM act WHERE c3_status = 'calculable_si_impuestos_resueltos'
    UNION ALL
    SELECT '2. Potencialmente calculables (C3)', 4, 'Bloqueados por inconsistencias tributarias',
           count(*) FILTER (WHERE engine_status = 'componente_requiere_revision_tributaria'),
           round(100.0 * count(*) FILTER (WHERE engine_status = 'componente_requiere_revision_tributaria') / nullif((SELECT n_act FROM tot), 0), 1),
           round(100.0 * count(*) FILTER (WHERE engine_status = 'componente_requiere_revision_tributaria') / nullif((SELECT n_c3 FROM tot), 0), 1),
           'Motivos en la sección 3'
    FROM act WHERE c3_status = 'calculable_si_impuestos_resueltos'
    UNION ALL
    SELECT '2. Potencialmente calculables (C3)', 5, 'Con costo neto y costo bruto comercial (calculado, provisional)',
           count(*) FILTER (WHERE engine_status = 'calculado'),
           round(100.0 * count(*) FILTER (WHERE engine_status = 'calculado') / nullif((SELECT n_act FROM tot), 0), 1),
           round(100.0 * count(*) FILTER (WHERE engine_status = 'calculado') / nullif((SELECT n_c3 FROM tot), 0), 1),
           'Validación contable pendiente: TAX_CONFIG_VERIFIED = false y costo neto no contrastado con facturas'
    FROM act WHERE c3_status = 'calculable_si_impuestos_resueltos'
    UNION ALL
    SELECT '2. Potencialmente calculables (C3)', 6, 'Con otro estado en el motor',
           count(*) FILTER (WHERE engine_status NOT IN ('calculado', 'componente_requiere_revision_tributaria')),
           NULL,
           round(100.0 * count(*) FILTER (WHERE engine_status NOT IN ('calculado', 'componente_requiere_revision_tributaria'))
                 / nullif((SELECT n_c3 FROM tot), 0), 1),
           coalesce(string_agg(DISTINCT engine_status, ', ')
                        FILTER (WHERE engine_status NOT IN ('calculado', 'componente_requiere_revision_tributaria')), 'ninguno')
    FROM act WHERE c3_status = 'calculable_si_impuestos_resueltos'

    -- 3. Bloqueo tributario ------------------------------------------------------------------------
    UNION ALL
    SELECT '3. Bloqueo tributario', 1, 'Motivo principal: ' || coalesce(a.tax_reason, 'sin_motivo'), count(*),
           round(100.0 * count(*) / nullif((SELECT n_act FROM tot), 0), 1),
           round(100.0 * count(*) / nullif((SELECT n_tax FROM tot), 0), 1),
           tr.descripcion
    FROM act a LEFT JOIN tax_rank tr ON tr.tax_status = a.tax_reason
    WHERE a.engine_status = 'componente_requiere_revision_tributaria'
    GROUP BY a.tax_reason, tr.descripcion
    UNION ALL
    SELECT '3. Bloqueo tributario', 2, 'Activos con algún componente con problema tributario (cualquier estado final)',
           count(*) FILTER (WHERE any_tax_issue),
           round(100.0 * count(*) FILTER (WHERE any_tax_issue) / nullif((SELECT n_act FROM tot), 0), 1), NULL,
           'Incluye packs cuyo estado final es otro por precedencia (inexistente, abierto, etc.)' FROM act
    UNION ALL
    SELECT '3. Bloqueo tributario', 3, 'Activos con IVA e ILA identificados por id y código SII en todos sus componentes',
           count(*) FILTER (WHERE all_tax_ok),
           round(100.0 * count(*) FILTER (WHERE all_tax_ok) / nullif((SELECT n_act FROM tot), 0), 1), NULL,
           'Sobre todos los activos (no solo C3)' FROM act
    UNION ALL
    SELECT '3b. Componentes con bloqueo tributario', 1,
           'Variante ' || l.variant_id || ' · ' || coalesce(l.component_name, 'sin producto'),
           count(DISTINCT a.pack_product_id), NULL, NULL,
           concat_ws(' · ', l.tax_status, coalesce(l.taxes_txt, 'sin impuestos informados'),
                     'packs: ' || array_to_string((array_agg(DISTINCT a.pack_product_id ORDER BY a.pack_product_id))[1:12], ', '))
    FROM lines l JOIN act a ON a.pack_product_id = l.pack_product_id
    WHERE l.base_status = 'requiere_revision_tributaria'
    GROUP BY l.variant_id, l.component_name, l.tax_status, l.taxes_txt

    -- 4. Costo neto y bruto ------------------------------------------------------------------------
    UNION ALL
    SELECT '4. Costo neto y bruto comercial', 1, 'Activos con costo neto y bruto comercial por componentes',
           count(*) FILTER (WHERE engine_status = 'calculado'),
           round(100.0 * count(*) FILTER (WHERE engine_status = 'calculado') / nullif((SELECT n_act FROM tot), 0), 1), NULL,
           'Estado provisional (preliminar_no_verificado): no usar como cifra contable definitiva' FROM act
    UNION ALL
    SELECT '4. Costo neto y bruto comercial', 2, 'Activos sin costo calculable', count(*) FILTER (WHERE engine_status IS DISTINCT FROM 'calculado'),
           round(100.0 * count(*) FILTER (WHERE engine_status IS DISTINCT FROM 'calculado') / nullif((SELECT n_act FROM tot), 0), 1), NULL,
           'Sin margen definitivo; motivo en la sección 1' FROM act
    UNION ALL
    SELECT '4. Costo neto y bruto comercial', 3, 'Rango de costo bruto comercial de los calculados',
           count(*) FILTER (WHERE engine_status = 'calculado'), NULL, NULL,
           'neto ' || coalesce(round(min(net), 2)::text, '-') || ' a ' || coalesce(round(max(net), 2)::text, '-')
           || ' · bruto ' || coalesce(round(min(gross), 2)::text, '-') || ' a ' || coalesce(round(max(gross), 2)::text, '-')
    FROM act WHERE engine_status = 'calculado'

    -- 5. Componentes de los packs C3 sin costo / inexistentes --------------------------------------
    UNION ALL
    SELECT '5a. Packs C3 sin costo: componentes', 0, 'Packs activos C3 componente_sin_costo', n_c3_cost,
           round(100.0 * n_c3_cost / nullif(n_act, 0), 1), 100.0, 'Detalle por componente en las filas siguientes' FROM tot
    UNION ALL
    SELECT '5a. Packs C3 sin costo: componentes', 1,
           'Variante ' || l.variant_id || ' · ' || coalesce(l.component_name, 'sin producto'),
           count(DISTINCT a.pack_product_id), NULL,
           round(100.0 * count(DISTINCT a.pack_product_id) / nullif((SELECT n_c3_cost FROM tot), 0), 1),
           concat_ws(' · ',
               CASE WHEN NOT l.has_cost_row THEN 'sin registro en bsale_raw.variant_costs'
                    WHEN l.average_cost = 0 THEN 'costo promedio Bsale = 0'
                         || CASE WHEN coalesce(l.history_count, 0) = 0 THEN ' (sin historial de costos)' ELSE '' END
                    ELSE 'costo promedio Bsale negativo' END,
               'impuestos: ' || coalesce(l.tax_status, '-'),
               'packs: ' || array_to_string((array_agg(DISTINCT a.pack_product_id ORDER BY a.pack_product_id))[1:12], ', '))
    FROM lines l JOIN act a ON a.pack_product_id = l.pack_product_id
    WHERE a.c3_status = 'componente_sin_costo' AND l.base_status = 'sin_costo'
    GROUP BY l.variant_id, l.component_name, l.has_cost_row, l.average_cost, l.history_count, l.tax_status
    UNION ALL
    SELECT '5b. Packs C3 inexistentes: componentes', 0, 'Packs activos C3 componente_inexistente', n_c3_missing,
           round(100.0 * n_c3_missing / nullif(n_act, 0), 1), 100.0, 'Detalle por componente en las filas siguientes' FROM tot
    UNION ALL
    SELECT '5b. Packs C3 inexistentes: componentes', 1,
           'Variante ' || l.variant_id || ' · ' || coalesce(g.product_name, 'producto desconocido'),
           count(DISTINCT a.pack_product_id), NULL,
           round(100.0 * count(DISTINCT a.pack_product_id) / nullif((SELECT n_c3_missing FROM tot), 0), 1),
           concat_ws(' · ',
               CASE WHEN g.in_raw THEN 'variante retirada de Bsale desde ' || g.missing_since::date
                    ELSE 'la variante no existe en RAW de la empresa' END,
               'producto ' || coalesce(g.product_id::text, '-'),
               'packs: ' || array_to_string((array_agg(DISTINCT a.pack_product_id ORDER BY a.pack_product_id))[1:12], ', '))
    FROM lines l
    JOIN act a ON a.pack_product_id = l.pack_product_id
    LEFT JOIN ghost g ON g.variant_id = l.variant_id
    WHERE a.c3_status = 'componente_inexistente' AND l.base_status = 'inexistente'
    GROUP BY l.variant_id, g.product_name, g.in_raw, g.missing_since, g.product_id

    -- 6. C3 vs motor ---------------------------------------------------------------------------------
    UNION ALL
    SELECT '6. C3 vs motor', 1, coalesce(c3_status, '-') || ' -> ' || coalesce(engine_status, '-'), count(*),
           round(100.0 * count(*) / nullif((SELECT n_act FROM tot), 0), 1), NULL,
           CASE
               WHEN c3_status = engine_status THEN 'coincide'
               WHEN c3_status = 'calculable_si_impuestos_resueltos' AND engine_status = 'calculado' THEN 'coincide'
               WHEN c3_status = 'calculable_si_impuestos_resueltos' AND engine_status = 'componente_requiere_revision_tributaria'
                   THEN 'coincide en costo; el motor además exige impuestos válidos'
               WHEN c3_status = 'componente_sin_costo' AND engine_status = 'componente_requiere_revision_tributaria'
                   THEN 'difiere por precedencia: el motor informa primero la revisión tributaria'
               WHEN c3_status = 'pack_anidado_revisar_en_motor' THEN 'C3 no resolvía packs anidados'
               WHEN engine_status = 'ciclo_detectado' THEN 'C3 no detectaba ciclos'
               WHEN engine_status = 'no_evaluado_sin_variante_vigente' THEN 'el pack no tiene variantes vigentes'
               ELSE 'DIFIERE: revisar la extracción de pack_details'
           END
    FROM act
    GROUP BY c3_status, engine_status
)
SELECT seccion, orden, indicador, packs, pct_activos, pct_grupo, detalle
FROM report
ORDER BY seccion, orden, packs DESC NULLS LAST, indicador;
