-- =============================================================================
-- bsale_raw 011 — product_taxes: impuestos de cada producto (NO destructiva)
--
-- Fuente: GET /v1/products/{id}/product_taxes.json (un request por producto; el listado de
-- productos sólo trae product_taxes{href}). Una fila por producto consultado con éxito:
--   items_count = 0  -> Bsale confirmó que el producto NO tiene impuestos;
--   items_count > 0  -> tax_ids en el orden en que Bsale entrega los ítems;
--   sin fila         -> producto todavía no consultado (o su consulta nunca tuvo éxito);
--   consulta fallida -> bsale_raw.sync_cursors (resource 'product_taxes', cursor 'failures');
--                       un 404 o un error NUNCA se guarda como lista vacía.
-- Aquí no se calcula IVA, ILA, factor tributario ni montos brutos: eso es de la ERP.
-- Producto ausente de bsale_raw.products -> missing_since (nunca se borra); vuelve a NULL si
-- el producto reaparece y se vuelve a consultar.
--
-- Explícita (sin IF NOT EXISTS): aplicarla dos veces falla en vez de ocultar drift.
-- Después de aplicarla, correr verify_bsale_raw.sql.
-- =============================================================================

BEGIN;

CREATE TABLE bsale_raw.product_taxes (
    company_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    tax_ids BIGINT[] NOT NULL,
    items_count INTEGER NOT NULL,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    api_fetched_at TIMESTAMPTZ NOT NULL,
    missing_since TIMESTAMPTZ,
    last_source TEXT NOT NULL,
    sync_run_id BIGINT,
    CONSTRAINT pk_raw_product_taxes PRIMARY KEY (company_id, product_id),
    CONSTRAINT fk_raw_product_taxes_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_raw_product_taxes_last_source
        CHECK (last_source IN ('WEBHOOK', 'POINT', 'INCREMENTAL', 'FULL_RECONCILE', 'SCANNER', 'WINDOW_RECONCILE'))
);
CREATE INDEX ix_raw_product_taxes_fetched
    ON bsale_raw.product_taxes (company_id, api_fetched_at);

COMMENT ON TABLE bsale_raw.product_taxes IS
    'Relación producto-impuesto tal como la entrega Bsale (products/{id}/product_taxes.json). Sin cálculos tributarios.';
COMMENT ON COLUMN bsale_raw.product_taxes.tax_ids IS
    'tax.id de cada ítem, en el orden de Bsale (sin deduplicar ni ordenar). Vacío = Bsale confirmó cero impuestos.';
COMMENT ON COLUMN bsale_raw.product_taxes.items_count IS
    'Ítems recibidos (= count de Bsale, snapshot estricto).';
COMMENT ON COLUMN bsale_raw.product_taxes.payload IS
    'Array JSON con las páginas de respuesta tal cual llegaron (normalmente una).';
COMMENT ON COLUMN bsale_raw.product_taxes.missing_since IS
    'El producto dejó de estar vigente en bsale_raw.products; NULL = vigente. Nunca se borra.';

COMMIT;
