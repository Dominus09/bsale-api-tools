-- =============================================================================
-- bsale.products_master — consolidación por código de barras (variants.bar_code)
-- Origen: variants + products + product_types + variant_prices
-- =============================================================================

CREATE TABLE IF NOT EXISTS bsale.products_master (
    id           SERIAL PRIMARY KEY,
    barcode      TEXT NOT NULL,
    sku          TEXT,
    product_name TEXT,
    variant_name TEXT,
    product_type TEXT,
    companies    JSONB NOT NULL DEFAULT '[]'::jsonb,
    supplier_id  INTEGER,
    is_active    BOOLEAN NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT products_master_barcode_unique UNIQUE (barcode)
);

CREATE INDEX IF NOT EXISTS idx_products_master_supplier_id
    ON bsale.products_master (supplier_id);

CREATE INDEX IF NOT EXISTS idx_products_master_product_type
    ON bsale.products_master (product_type);

-- -----------------------------------------------------------------------------
-- Carga / refresco: agrupa por barcode (trim); companies desde variant_prices
-- -----------------------------------------------------------------------------
INSERT INTO bsale.products_master (
    barcode,
    sku,
    product_name,
    variant_name,
    product_type,
    companies,
    is_active,
    updated_at
)
SELECT
    btrim(v.bar_code) AS barcode,
    (array_agg(v.code ORDER BY v.company_id, v.bsale_id)
        FILTER (WHERE v.code IS NOT NULL))[1] AS sku,
    (array_agg(p.name ORDER BY v.company_id, v.bsale_id)
        FILTER (WHERE p.name IS NOT NULL))[1] AS product_name,
    (array_agg(v.description ORDER BY v.company_id, v.bsale_id)
        FILTER (WHERE v.description IS NOT NULL))[1] AS variant_name,
    (array_agg(pt.name ORDER BY v.company_id, v.bsale_id)
        FILTER (WHERE pt.name IS NOT NULL))[1] AS product_type,
    COALESCE(
        to_jsonb(
            array_agg(DISTINCT vp.company_id ORDER BY vp.company_id)
                FILTER (WHERE vp.company_id IS NOT NULL)
        ),
        '[]'::jsonb
    ) AS companies,
    TRUE AS is_active,
    NOW() AS updated_at
FROM bsale.variants v
LEFT JOIN bsale.products p
    ON p.company_id = v.company_id
   AND p.bsale_id = v.product_id
LEFT JOIN bsale.product_types pt
    ON pt.company_id = p.company_id
   AND pt.bsale_id = p.product_type_id
LEFT JOIN bsale.variant_prices vp
    ON vp.company_id = v.company_id
   AND vp.variant_id = v.bsale_id
WHERE v.bar_code IS NOT NULL
  AND btrim(v.bar_code) <> ''
GROUP BY btrim(v.bar_code)
-- En conflicto: no se actualizan supplier_id ni is_active (edición manual / negocio)
ON CONFLICT (barcode) DO UPDATE SET
    sku          = EXCLUDED.sku,
    product_name = EXCLUDED.product_name,
    variant_name = EXCLUDED.variant_name,
    product_type = EXCLUDED.product_type,
    companies    = EXCLUDED.companies,
    updated_at   = EXCLUDED.updated_at;
