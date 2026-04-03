-- =============================================================================
-- FASE 1 — INSPECCIÓN (referencia repo sync_catalog / sync_prices_costs)
-- =============================================================================
-- bsale.variants:     company_id, bsale_id, product_id (id producto Bsale),
--                     code (SKU), bar_code, description (nombre variante)
-- bsale.products:     company_id, bsale_id, name, product_type_id, tax_factor, ...
-- Join variante→producto: p.company_id = v.company_id AND p.bsale_id = v.product_id
--
-- bsale.variant_prices: company_id, variant_id (id variante Bsale = v.bsale_id),
--                       price_list_id, price_net, price_gross
--                       UNIQUE (company_id, variant_id, price_list_id)
--
-- bsale.variant_cost: company_id, variant_id (= v.bsale_id), average_cost_net, last_update
--                     UNIQUE (company_id, variant_id)
--
-- bsale.margin_rules: company_id, product_type_id, price_list_id, min_margin (% sobre costo),
--                     active (ver margin_rules_schema.sql si hay que crearla)
-- Join reglas: company_id + product_type_id + price_list_id (IS NOT DISTINCT FROM para NULLs)
--
-- Precio analizado: price_gross (lista). Costo en bruto aproximado: average_cost_net * tax_factor
-- (misma convención que en paneles con costo “gross”).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- FASE 2 — Vista
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW bsale.margin_analysis_view AS
WITH joined AS (
    SELECT
        vp.company_id,
        p.product_type_id,
        p.name AS product_name,
        v.bsale_id AS variant_id,
        v.description AS variant_name,
        v.bar_code AS barcode,
        v.code AS sku,
        vp.price_list_id,
        vp.price_gross AS price,
        CASE
            WHEN vc.average_cost_net IS NULL THEN NULL
            ELSE ROUND(
                (vc.average_cost_net * COALESCE(NULLIF(p.tax_factor, 0), 1))::numeric,
                6
            )
        END AS cost,
        mr.min_margin AS min_margin_percent,
        mr.company_id AS margin_rule_company_id
    FROM bsale.variant_prices vp
    INNER JOIN bsale.variants v
        ON v.company_id = vp.company_id
       AND v.bsale_id = vp.variant_id
    INNER JOIN bsale.products p
        ON p.company_id = v.company_id
       AND p.bsale_id = v.product_id
    LEFT JOIN bsale.variant_cost vc
        ON vc.company_id = v.company_id
       AND vc.variant_id = v.bsale_id
    LEFT JOIN bsale.margin_rules mr
        ON mr.company_id = vp.company_id
       AND mr.price_list_id = vp.price_list_id
       AND mr.product_type_id IS NOT DISTINCT FROM p.product_type_id
       AND COALESCE(mr.active, TRUE) IS TRUE
),
calc AS (
    SELECT
        company_id,
        product_type_id,
        product_name,
        variant_id,
        variant_name,
        barcode,
        sku,
        price_list_id,
        price,
        cost,
        min_margin_percent,
        margin_rule_company_id,
        CASE
            WHEN cost IS NOT NULL AND price IS NOT NULL THEN ROUND((price - cost)::numeric, 6)
        END AS margin_value,
        CASE
            WHEN cost IS NOT NULL AND cost > 0 AND price IS NOT NULL THEN
                ROUND((((price - cost) / cost) * 100)::numeric, 4)
        END AS margin_percent
    FROM joined
)
SELECT
    company_id,
    product_type_id,
    product_name,
    variant_id,
    variant_name,
    barcode,
    sku,
    price_list_id,
    price,
    cost,
    margin_value,
    margin_percent,
    min_margin_percent,
    CASE
        WHEN margin_percent IS NOT NULL AND min_margin_percent IS NOT NULL THEN
            ROUND((margin_percent - min_margin_percent)::numeric, 4)
    END AS margin_diff,
    CASE
        WHEN cost IS NULL OR cost <= 0 THEN 'NO_COST'
        WHEN margin_rule_company_id IS NULL OR min_margin_percent IS NULL THEN 'NO_RULE'
        WHEN margin_percent IS NULL THEN 'NO_RULE'
        WHEN margin_percent < min_margin_percent THEN 'LOW'
        ELSE 'OK'
    END AS status
FROM calc;

-- =============================================================================
-- FASE 3 — Consultas clave (parámetros: :company_id, :price_list_id opcional)
-- =============================================================================

-- 1) Bajo margen
-- SELECT * FROM bsale.margin_analysis_view
-- WHERE company_id = :company_id
--   AND (:price_list_id IS NULL OR price_list_id = :price_list_id)
--   AND status = 'LOW';

-- 2) Sin costo
-- SELECT * FROM bsale.margin_analysis_view
-- WHERE company_id = :company_id
--   AND (:price_list_id IS NULL OR price_list_id = :price_list_id)
--   AND status = 'NO_COST';

-- 3) Sin regla
-- SELECT * FROM bsale.margin_analysis_view
-- WHERE company_id = :company_id
--   AND (:price_list_id IS NULL OR price_list_id = :price_list_id)
--   AND status = 'NO_RULE';

-- 4) Ranking peor margen (mayor brecha bajo el mínimo)
-- SELECT * FROM bsale.margin_analysis_view
-- WHERE company_id = :company_id
--   AND (:price_list_id IS NULL OR price_list_id = :price_list_id)
-- ORDER BY margin_diff ASC NULLS LAST, margin_percent ASC NULLS LAST;

-- 5) Conteo por estado
-- SELECT status, COUNT(*) AS n
-- FROM bsale.margin_analysis_view
-- WHERE company_id = :company_id
--   AND (:price_list_id IS NULL OR price_list_id = :price_list_id)
-- GROUP BY status
-- ORDER BY status;

-- =============================================================================
-- FASE 4 — Exportación Excel (solo LOW; suggested según fórmula pedida)
-- =============================================================================
-- SELECT
--     company_id,
--     price_list_id,
--     product_name,
--     variant_name,
--     sku,
--     barcode,
--     cost,
--     price AS current_price,
--     ROUND((cost * (1 + min_margin_percent / 100))::numeric, 4) AS suggested_price,
--     margin_percent AS margin_actual,
--     min_margin_percent AS margin_target,
--     margin_diff AS diferencia
-- FROM bsale.margin_analysis_view
-- WHERE company_id = :company_id
--   AND (:price_list_id IS NULL OR price_list_id = :price_list_id)
--   AND status = 'LOW';

-- =============================================================================
-- FASE 5 — Índices sugeridos (ajustar nombres si ya existen equivalentes)
-- =============================================================================
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_variant_prices_company_list
--     ON bsale.variant_prices (company_id, price_list_id);
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_variant_prices_company_variant
--     ON bsale.variant_prices (company_id, variant_id);
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_variants_company_product
--     ON bsale.variants (company_id, product_id);
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_products_company_bsale
--     ON bsale.products (company_id, bsale_id);
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_products_company_type
--     ON bsale.products (company_id, product_type_id);
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_variant_cost_company_variant
--     ON bsale.variant_cost (company_id, variant_id);
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_margin_rules_company_type_list
--     ON bsale.margin_rules (company_id, product_type_id, price_list_id);
