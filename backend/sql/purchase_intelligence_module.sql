-- =============================================================================
-- MÓDULO COMPRAS INTELIGENTE (ventas, rotación, stock, costos)
-- Validado contra: estructura_postgres_bsale.md, estructura_postgres_full.md
-- y esquemas reales del repo (sync_catalog, sync_prices_costs, sync_stock).
--
-- Guía por etapas: ver purchase_intelligence_EXECUTION_GUIDE.md en esta carpeta.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS bsale;

-- =============================================================================
-- ETAPA A — variants.units_per_box + backfill (formato Bsale: "(SEC 6)", "(SEC 12)")
-- Patrón: literal SEC + espacios opcionales + dígitos (primer grupo capturado).
-- No sobrescribe filas con units_per_box ya informado.
-- =============================================================================
ALTER TABLE bsale.variants
    ADD COLUMN IF NOT EXISTS units_per_box INTEGER;

UPDATE bsale.variants v
SET units_per_box = (regexp_match(
    UPPER(COALESCE(v.description, '')),
    E'SEC\s*([0-9]+)'
))[1]::integer
WHERE v.units_per_box IS NULL
  AND UPPER(COALESCE(v.description, '')) ~ E'SEC\s*[0-9]+';

-- =============================================================================
-- ETAPA B — Vistas de ventas y auxiliares
-- =============================================================================
CREATE OR REPLACE VIEW bsale.vw_sales_base AS
SELECT
    d.company_id,
    d.office_id,
    dd.variant_id,
    pt.name AS product_type_name,
    p.name AS product_name,
    v.description AS variant_name,
    v.bar_code AS barcode,
    SUM(COALESCE(dd.quantity, 0))::numeric AS cantidad_vendida,
    SUM(COALESCE(dd.total_amount, 0))::numeric AS total_venta
FROM bsale.document_details dd
INNER JOIN bsale.documents d
    ON d.company_id = dd.company_id
   AND d.bsale_id = dd.document_id
INNER JOIN bsale.variants v
    ON v.company_id = dd.company_id
   AND v.bsale_id = dd.variant_id
INNER JOIN bsale.products p
    ON p.company_id = v.company_id
   AND p.bsale_id = v.product_id
LEFT JOIN bsale.product_types pt
    ON pt.company_id = p.company_id
   AND pt.bsale_id = p.product_type_id
WHERE d.document_type_id IN (1, 6)
GROUP BY
    d.company_id,
    d.office_id,
    dd.variant_id,
    pt.name,
    p.name,
    v.description,
    v.bar_code;

CREATE OR REPLACE VIEW bsale.vw_sales_7d AS
SELECT
    d.company_id,
    d.office_id,
    dd.variant_id,
    pt.name AS product_type_name,
    p.name AS product_name,
    v.description AS variant_name,
    v.bar_code AS barcode,
    SUM(COALESCE(dd.quantity, 0))::numeric AS cantidad_vendida,
    SUM(COALESCE(dd.total_amount, 0))::numeric AS total_venta
FROM bsale.document_details dd
INNER JOIN bsale.documents d
    ON d.company_id = dd.company_id
   AND d.bsale_id = dd.document_id
INNER JOIN bsale.variants v
    ON v.company_id = dd.company_id
   AND v.bsale_id = dd.variant_id
INNER JOIN bsale.products p
    ON p.company_id = v.company_id
   AND p.bsale_id = v.product_id
LEFT JOIN bsale.product_types pt
    ON pt.company_id = p.company_id
   AND pt.bsale_id = p.product_type_id
WHERE d.document_type_id IN (1, 6)
  AND d.emission_date >= (CURRENT_TIMESTAMP - INTERVAL '7 days')
GROUP BY
    d.company_id,
    d.office_id,
    dd.variant_id,
    pt.name,
    p.name,
    v.description,
    v.bar_code;

CREATE OR REPLACE VIEW bsale.vw_sales_30d AS
SELECT
    d.company_id,
    d.office_id,
    dd.variant_id,
    pt.name AS product_type_name,
    p.name AS product_name,
    v.description AS variant_name,
    v.bar_code AS barcode,
    SUM(COALESCE(dd.quantity, 0))::numeric AS cantidad_vendida,
    SUM(COALESCE(dd.total_amount, 0))::numeric AS total_venta
FROM bsale.document_details dd
INNER JOIN bsale.documents d
    ON d.company_id = dd.company_id
   AND d.bsale_id = dd.document_id
INNER JOIN bsale.variants v
    ON v.company_id = dd.company_id
   AND v.bsale_id = dd.variant_id
INNER JOIN bsale.products p
    ON p.company_id = v.company_id
   AND p.bsale_id = v.product_id
LEFT JOIN bsale.product_types pt
    ON pt.company_id = p.company_id
   AND pt.bsale_id = p.product_type_id
WHERE d.document_type_id IN (1, 6)
  AND d.emission_date >= (CURRENT_TIMESTAMP - INTERVAL '30 days')
GROUP BY
    d.company_id,
    d.office_id,
    dd.variant_id,
    pt.name,
    p.name,
    v.description,
    v.bar_code;

CREATE OR REPLACE VIEW bsale.vw_rotation AS
SELECT
    company_id,
    office_id,
    variant_id,
    COALESCE(cantidad_vendida, 0)::numeric AS ventas_30_dias,
    (COALESCE(cantidad_vendida, 0)::numeric / 30.0) AS promedio_diario
FROM bsale.vw_sales_30d;

CREATE OR REPLACE VIEW bsale.vw_costs AS
SELECT
    v.company_id,
    v.bsale_id AS variant_id,
    COALESCE(vc.average_cost_net, 0)::numeric AS costo_neto,
    (
        COALESCE(tax_sum.pct / 100.0, GREATEST(COALESCE(p.tax_factor, 1) - 1, 0))
    )::numeric AS tasa_impuesto,
    (
        COALESCE(vc.average_cost_net, 0)::numeric
        * CASE
            WHEN tax_sum.pct IS NOT NULL THEN (1::numeric + tax_sum.pct / 100.0)
            ELSE COALESCE(NULLIF(p.tax_factor, 0), 1)::numeric
          END
    )::numeric AS costo_bruto
FROM bsale.variants v
INNER JOIN bsale.products p
    ON p.company_id = v.company_id
   AND p.bsale_id = v.product_id
LEFT JOIN bsale.variant_cost vc
    ON vc.company_id = v.company_id
   AND vc.variant_id = v.bsale_id
LEFT JOIN LATERAL (
    SELECT SUM(tx.percentage)::numeric AS pct
    FROM jsonb_array_elements_text(
        COALESCE(p.tax_ids_json::text, '[]')::jsonb
    ) AS elem(tid)
    INNER JOIN bsale.taxes tx
        ON tx.company_id = p.company_id
       AND tx.bsale_id = elem.tid::integer
) tax_sum ON TRUE;

CREATE OR REPLACE VIEW bsale.vw_stock AS
SELECT
    company_id,
    office_id,
    variant_id,
    COALESCE(quantity_available, 0)::numeric AS stock_actual
FROM bsale.stocks;

-- =============================================================================
-- ETAPA C — vw_purchase_analysis
-- status: orden estricto de evaluación CASE (primera condición que cumple gana)
-- units_per_box = valor real en variants; units_per_box_eff = COALESCE(NULLIF(units_per_box,0),1)
-- =============================================================================
CREATE OR REPLACE VIEW bsale.vw_purchase_analysis AS
WITH office_variants AS (
    SELECT company_id, office_id, variant_id
    FROM bsale.stocks
    UNION
    SELECT company_id, office_id, variant_id
    FROM bsale.vw_sales_30d
),
enriched AS (
    SELECT
        ov.company_id,
        ov.office_id,
        ov.variant_id,
        COALESCE(s7.cantidad_vendida, 0)::numeric AS ventas_7_dias,
        COALESCE(s30.cantidad_vendida, 0)::numeric AS ventas_30_dias,
        COALESCE(r.promedio_diario, 0)::numeric AS promedio_diario,
        COALESCE(st.stock_actual, 0)::numeric AS stock_actual,
        COALESCE(c.costo_bruto, 0)::numeric AS costo_bruto,
        v.units_per_box,
        COALESCE(NULLIF(v.units_per_box, 0), 1) AS units_per_box_eff,
        pt.name AS product_type_name,
        p.name AS product_name,
        v.description AS variant_name,
        v.bar_code AS barcode
    FROM office_variants ov
    INNER JOIN bsale.variants v
        ON v.company_id = ov.company_id
       AND v.bsale_id = ov.variant_id
    INNER JOIN bsale.products p
        ON p.company_id = v.company_id
       AND p.bsale_id = v.product_id
    LEFT JOIN bsale.product_types pt
        ON pt.company_id = p.company_id
       AND pt.bsale_id = p.product_type_id
    LEFT JOIN bsale.vw_sales_7d s7
        ON s7.company_id = ov.company_id
       AND s7.office_id = ov.office_id
       AND s7.variant_id = ov.variant_id
    LEFT JOIN bsale.vw_sales_30d s30
        ON s30.company_id = ov.company_id
       AND s30.office_id = ov.office_id
       AND s30.variant_id = ov.variant_id
    LEFT JOIN bsale.vw_rotation r
        ON r.company_id = ov.company_id
       AND r.office_id = ov.office_id
       AND r.variant_id = ov.variant_id
    LEFT JOIN bsale.vw_stock st
        ON st.company_id = ov.company_id
       AND st.office_id = ov.office_id
       AND st.variant_id = ov.variant_id
    LEFT JOIN bsale.vw_costs c
        ON c.company_id = ov.company_id
       AND c.variant_id = ov.variant_id
),
calc AS (
    SELECT
        e.*,
        7::integer AS dias_cobertura,
        (e.promedio_diario * 7)::numeric AS demanda_proyectada,
        GREATEST(
            (e.promedio_diario * 7)::numeric - e.stock_actual,
            0::numeric
        ) AS unidades_a_comprar
    FROM enriched e
)
SELECT
    c.company_id,
    c.office_id,
    c.variant_id,
    c.product_type_name,
    c.product_name,
    c.variant_name,
    c.barcode,
    c.ventas_7_dias,
    c.ventas_30_dias,
    c.promedio_diario,
    c.stock_actual,
    c.costo_bruto,
    c.dias_cobertura,
    c.demanda_proyectada,
    c.unidades_a_comprar,
    c.units_per_box,
    c.units_per_box_eff,
    (c.unidades_a_comprar / c.units_per_box_eff::numeric) AS cajas_sugeridas,
    CASE
        WHEN c.ventas_30_dias = 0 AND c.stock_actual > 0 THEN 'NO_COMPRAR'
        WHEN c.unidades_a_comprar <= 0 THEN 'NO_COMPRAR'
        WHEN c.unidades_a_comprar > 0
             AND c.unidades_a_comprar < c.units_per_box_eff THEN 'REVISAR'
        WHEN c.unidades_a_comprar >= c.units_per_box_eff THEN 'COMPRAR'
        ELSE 'REVISAR'
    END AS status,
    (c.unidades_a_comprar * c.costo_bruto)::numeric AS costo_total_compra
FROM calc c;

-- =============================================================================
-- ETAPA D — Tablas OC primero (purchase_manual_items referencia oc_document)
-- =============================================================================
CREATE TABLE IF NOT EXISTS bsale.oc_document (
    oc_id          BIGSERIAL PRIMARY KEY,
    company_id     INTEGER NOT NULL,
    office_id      INTEGER NOT NULL,
    supplier_id    INTEGER NOT NULL REFERENCES bsale.suppliers (id),
    fecha_emision  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    fecha_entrega  DATE,
    total_oc       NUMERIC(18, 4) NOT NULL DEFAULT 0,
    forma_pago     TEXT,
    responsable    TEXT,
    observacion    TEXT,
    status         TEXT NOT NULL DEFAULT 'BORRADOR',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bsale.oc_details (
    oc_detail_id      BIGSERIAL PRIMARY KEY,
    oc_id             BIGINT NOT NULL REFERENCES bsale.oc_document (oc_id) ON DELETE CASCADE,
    company_id        INTEGER NOT NULL,
    office_id         INTEGER NOT NULL,
    variant_id        BIGINT,
    product_type_name TEXT,
    product_name      TEXT,
    variant_name      TEXT,
    barcode           TEXT,
    cantidad          NUMERIC(18, 4) NOT NULL,
    units_per_box     INTEGER,
    cajas             NUMERIC(18, 6),
    costo_unitario    NUMERIC(18, 4) NOT NULL,
    costo_total       NUMERIC(18, 4) NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_oc_details_oc_id
    ON bsale.oc_details (oc_id);

CREATE TABLE IF NOT EXISTS bsale.purchase_manual_items (
    id                 BIGSERIAL PRIMARY KEY,
    company_id         INTEGER NOT NULL,
    office_id          INTEGER NOT NULL,
    supplier_id        INTEGER NOT NULL REFERENCES bsale.suppliers (id),
    product_type_name  TEXT,
    product_name       TEXT,
    variant_name       TEXT,
    barcode            TEXT,
    units_per_box      INTEGER,
    costo_bruto        NUMERIC(18, 4),
    cantidad           NUMERIC(18, 4) NOT NULL,
    oc_id              BIGINT REFERENCES bsale.oc_document (oc_id) ON DELETE SET NULL,
    consumed_at        TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_purchase_manual_items_company_office
    ON bsale.purchase_manual_items (company_id, office_id);

CREATE INDEX IF NOT EXISTS idx_purchase_manual_items_oc_id
    ON bsale.purchase_manual_items (oc_id)
    WHERE oc_id IS NOT NULL;

-- Si la tabla ya existía de una versión anterior sin oc_id/consumed_at:
ALTER TABLE bsale.purchase_manual_items
    ADD COLUMN IF NOT EXISTS oc_id BIGINT REFERENCES bsale.oc_document (oc_id) ON DELETE SET NULL;
ALTER TABLE bsale.purchase_manual_items
    ADD COLUMN IF NOT EXISTS consumed_at TIMESTAMPTZ;

-- =============================================================================
-- ETAPA E — generate_purchase_order
-- Parámetros: company_id, office_id, supplier_id, fecha_emision (opcional),
--             fecha_entrega, forma_pago, responsable, observacion,
--             p_manual_ids (opcional): IDs de purchase_manual_items a incluir y marcar consumidos
-- =============================================================================
CREATE OR REPLACE FUNCTION bsale.generate_purchase_order(
    p_company_id integer,
    p_office_id integer,
    p_supplier_id integer,
    p_fecha_emision timestamptz DEFAULT NULL,
    p_fecha_entrega date DEFAULT NULL,
    p_forma_pago text DEFAULT NULL,
    p_responsable text DEFAULT NULL,
    p_observacion text DEFAULT NULL,
    p_manual_ids bigint[] DEFAULT NULL
)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
    v_oc_id bigint;
    v_sum numeric(18, 4);
    v_emit timestamptz;
BEGIN
    v_emit := COALESCE(p_fecha_emision, CURRENT_TIMESTAMP);

    INSERT INTO bsale.oc_document (
        company_id,
        office_id,
        supplier_id,
        fecha_emision,
        fecha_entrega,
        forma_pago,
        responsable,
        observacion,
        status,
        total_oc
    )
    VALUES (
        p_company_id,
        p_office_id,
        p_supplier_id,
        v_emit,
        p_fecha_entrega,
        p_forma_pago,
        p_responsable,
        p_observacion,
        'GENERADA',
        0
    )
    RETURNING oc_id INTO v_oc_id;

    INSERT INTO bsale.oc_details (
        oc_id,
        company_id,
        office_id,
        variant_id,
        product_type_name,
        product_name,
        variant_name,
        barcode,
        cantidad,
        units_per_box,
        cajas,
        costo_unitario,
        costo_total
    )
    SELECT
        v_oc_id,
        pa.company_id,
        pa.office_id,
        pa.variant_id,
        pa.product_type_name,
        pa.product_name,
        pa.variant_name,
        pa.barcode,
        pa.unidades_a_comprar,
        pa.units_per_box,
        pa.cajas_sugeridas,
        pa.costo_bruto,
        pa.costo_total_compra
    FROM bsale.vw_purchase_analysis pa
    WHERE pa.company_id = p_company_id
      AND pa.office_id = p_office_id
      AND pa.status = 'COMPRAR';

    IF p_manual_ids IS NOT NULL AND COALESCE(cardinality(p_manual_ids), 0) > 0 THEN
        INSERT INTO bsale.oc_details (
            oc_id,
            company_id,
            office_id,
            variant_id,
            product_type_name,
            product_name,
            variant_name,
            barcode,
            cantidad,
            units_per_box,
            cajas,
            costo_unitario,
            costo_total
        )
        SELECT
            v_oc_id,
            m.company_id,
            m.office_id,
            NULL::bigint,
            m.product_type_name,
            m.product_name,
            m.variant_name,
            m.barcode,
            m.cantidad,
            m.units_per_box,
            (m.cantidad / NULLIF(COALESCE(NULLIF(m.units_per_box, 0), 1), 0)::numeric),
            COALESCE(m.costo_bruto, 0),
            (m.cantidad * COALESCE(m.costo_bruto, 0))::numeric
        FROM bsale.purchase_manual_items m
        WHERE m.id = ANY (p_manual_ids)
          AND m.company_id = p_company_id
          AND m.office_id = p_office_id
          AND m.supplier_id = p_supplier_id
          AND m.oc_id IS NULL;

        UPDATE bsale.purchase_manual_items m
        SET
            oc_id = v_oc_id,
            consumed_at = CURRENT_TIMESTAMP
        WHERE m.id = ANY (p_manual_ids)
          AND m.company_id = p_company_id
          AND m.office_id = p_office_id
          AND m.supplier_id = p_supplier_id
          AND m.oc_id IS NULL;
    END IF;

    SELECT COALESCE(SUM(costo_total), 0)
    INTO v_sum
    FROM bsale.oc_details
    WHERE oc_id = v_oc_id;

    UPDATE bsale.oc_document
    SET total_oc = v_sum
    WHERE oc_id = v_oc_id;

    RETURN v_oc_id;
END;
$$;

COMMENT ON VIEW bsale.vw_purchase_analysis IS
    'status: 1) ventas_30=0 y stock>0 → NO_COMPRAR; 2) unidades<=0 → NO_COMPRAR; '
    '3) 0<unidades<units_per_box_eff → REVISAR; 4) unidades>=units_per_box_eff → COMPRAR; ELSE REVISAR (nunca NULL).';

COMMENT ON FUNCTION bsale.generate_purchase_order IS
    'OC desde vw_purchase_analysis (COMPRAR) + opcional líneas manuales (p_manual_ids); marca oc_id/consumed_at.';
