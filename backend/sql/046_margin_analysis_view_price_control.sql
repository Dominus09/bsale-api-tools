-- =============================================================================
-- 046 — margin_analysis_view: semántica de control de precios (PROPUESTA)
-- =============================================================================
-- NO aplicar desde Cursor en producción.
--
-- Contexto:
--   La vista actual calcula cost = average_cost_net * tax_factor y llama
--   "margin_percent" al recargo % sobre ese costo. /margins ahora obtiene
--   datos canónicos vía GET /price-list-control (Python + Decimal), usando
--   el costo bruto máximo válido de analytics.cost_reception_history.
--
-- PostgreSQL: CREATE OR REPLACE VIEW NO puede renombrar ni reordenar columnas.
--   Solo se pueden AGREGAR columnas al FINAL. Por eso gross_margin_pct y
--   max_margin_percent van después de status.
--
-- Si hiciera falta reordenar, usar:
--   DROP VIEW IF EXISTS bsale.margin_analysis_view;
--   CREATE VIEW ...
-- =============================================================================

CREATE OR REPLACE VIEW bsale.margin_analysis_view AS
WITH joined AS (
    SELECT
        vp.company_id,
        p.product_type_id,
        pt.name AS product_type_name,
        p.name AS product_name,
        v.bsale_id AS variant_id,
        v.description AS variant_name,
        v.bar_code AS barcode,
        v.code AS sku,
        vp.price_list_id,
        pl.name AS price_list_name,
        stq.stock_quantity,
        vp.price_gross AS price,
        -- Fallback de costo bruto actual (NO es el máximo histórico válido).
        -- Preferir average_cost_gross; si falta, net * tax_factor.
        CASE
            WHEN vc.average_cost_gross IS NOT NULL AND vc.average_cost_gross > 0
                THEN ROUND(vc.average_cost_gross::numeric, 6)
            WHEN vc.average_cost_net IS NULL THEN NULL
            ELSE ROUND(
                (vc.average_cost_net * COALESCE(NULLIF(p.tax_factor, 0), 1))::numeric,
                6
            )
        END AS cost,
        mr.min_margin AS min_margin_percent,
        mr.max_margin AS max_margin_percent,
        mr.company_id AS margin_rule_company_id
    FROM bsale.variant_prices vp
    INNER JOIN bsale.variants v
        ON v.company_id = vp.company_id
       AND v.bsale_id = vp.variant_id
    INNER JOIN bsale.products p
        ON p.company_id = v.company_id
       AND p.bsale_id = v.product_id
    LEFT JOIN bsale.price_lists pl
        ON pl.company_id = vp.company_id
       AND pl.bsale_id = vp.price_list_id
    LEFT JOIN (
        SELECT
            company_id,
            variant_id,
            SUM(quantity_available)::numeric AS stock_quantity
        FROM bsale.stocks
        GROUP BY company_id, variant_id
    ) stq
        ON stq.company_id = vp.company_id
       AND stq.variant_id = vp.variant_id
    LEFT JOIN bsale.product_types pt
        ON pt.company_id = p.company_id
       AND p.product_type_id IS NOT NULL
       AND pt.bsale_id = p.product_type_id
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
        product_type_name,
        product_name,
        variant_id,
        variant_name,
        barcode,
        sku,
        price_list_id,
        price_list_name,
        stock_quantity,
        price,
        cost,
        min_margin_percent,
        max_margin_percent,
        margin_rule_company_id,
        CASE
            WHEN cost IS NOT NULL AND price IS NOT NULL THEN ROUND((price - cost)::numeric, 6)
        END AS margin_value,
        -- Recargo % sobre costo (markup). Nombre histórico margin_percent.
        CASE
            WHEN cost IS NOT NULL AND cost > 0 AND price IS NOT NULL THEN
                ROUND((((price - cost) / cost) * 100)::numeric, 4)
        END AS margin_percent,
        CASE
            WHEN price IS NOT NULL AND price > 0 AND cost IS NOT NULL THEN
                ROUND((((price - cost) / price) * 100)::numeric, 4)
        END AS gross_margin_pct
    FROM joined
)
SELECT
    -- Columnas originales (mismo orden/nombres que la vista existente)
    company_id,
    product_type_id,
    product_type_name,
    product_name,
    variant_id,
    variant_name,
    barcode,
    sku,
    price_list_id,
    price_list_name,
    stock_quantity,
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
        WHEN price IS NULL OR price <= 0 THEN 'missing_price'
        WHEN cost IS NULL OR cost <= 0 THEN 'missing_cost'
        WHEN margin_rule_company_id IS NULL OR min_margin_percent IS NULL THEN 'missing_rule'
        WHEN margin_percent < min_margin_percent THEN 'below_minimum'
        WHEN max_margin_percent IS NOT NULL
             AND max_margin_percent > 0
             AND margin_percent > max_margin_percent THEN 'above_maximum'
        ELSE 'within_policy'
    END AS status,
    -- Columnas nuevas solo al final (permitido por CREATE OR REPLACE VIEW)
    gross_margin_pct,
    max_margin_percent
FROM calc;

COMMENT ON VIEW bsale.margin_analysis_view IS
    'Control de precios por lista (fallback costo actual). Fuente canónica /margins: GET /price-list-control (máx. bruto recepción). margin_percent = recargo % sobre costo.';
