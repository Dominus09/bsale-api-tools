-- =============================================================================
-- bsale_raw 005 — estado actual de stock, precios y costos
--
-- Mecanismos concurrentes sobre la misma fila: webhook targeted, targeted manual/sistema,
-- scanner y full reconcile. Regla: un snapshot viejo NUNCA sobrescribe una fila obtenida
-- después. UPSERT futuro:
--   ON CONFLICT (pk) DO UPDATE SET ... WHERE target.api_fetched_at <= EXCLUDED.api_fetched_at
-- DELETE stale futuro (sólo reconcile destructivo por company + office / company + price_list):
--   fila ausente de staging AND mismo company/resource/scope
--   AND target.api_fetched_at <= snapshot_started_at (capturado antes del primer GET).
--
-- PK operacional; el id propio de Bsale se guarda aparte (bsale_stock_id, bsale_detail_id)
-- SIN UNIQUE: su unicidad dentro de la instancia no está demostrada.
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS bsale_raw.stocks (
    company_id BIGINT NOT NULL,
    variant_id BIGINT NOT NULL,
    office_id BIGINT NOT NULL,
    bsale_stock_id BIGINT,
    quantity NUMERIC,
    quantity_reserved NUMERIC,
    quantity_available NUMERIC,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    api_fetched_at TIMESTAMPTZ NOT NULL,
    last_source TEXT NOT NULL,
    sync_run_id BIGINT,
    CONSTRAINT pk_raw_stocks PRIMARY KEY (company_id, variant_id, office_id),
    CONSTRAINT fk_raw_stocks_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_raw_stocks_last_source
        CHECK (last_source IN ('WEBHOOK', 'POINT', 'INCREMENTAL', 'FULL_RECONCILE', 'SCANNER', 'WINDOW_RECONCILE'))
);
CREATE INDEX IF NOT EXISTS ix_raw_stocks_office_fetched
    ON bsale_raw.stocks (company_id, office_id, api_fetched_at);
CREATE INDEX IF NOT EXISTS ix_raw_stocks_bsale_stock_id
    ON bsale_raw.stocks (company_id, bsale_stock_id);

CREATE TABLE IF NOT EXISTS bsale_raw.variant_prices (
    company_id BIGINT NOT NULL,
    price_list_id BIGINT NOT NULL,
    variant_id BIGINT NOT NULL,
    bsale_detail_id BIGINT,
    variant_value NUMERIC,
    variant_value_with_taxes NUMERIC,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    api_fetched_at TIMESTAMPTZ NOT NULL,
    last_source TEXT NOT NULL,
    sync_run_id BIGINT,
    CONSTRAINT pk_raw_variant_prices PRIMARY KEY (company_id, price_list_id, variant_id),
    CONSTRAINT fk_raw_variant_prices_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_raw_variant_prices_last_source
        CHECK (last_source IN ('WEBHOOK', 'POINT', 'INCREMENTAL', 'FULL_RECONCILE', 'SCANNER', 'WINDOW_RECONCILE'))
);
CREATE INDEX IF NOT EXISTS ix_raw_variant_prices_list_fetched
    ON bsale_raw.variant_prices (company_id, price_list_id, api_fetched_at);
CREATE INDEX IF NOT EXISTS ix_raw_variant_prices_variant
    ON bsale_raw.variant_prices (company_id, variant_id);
CREATE INDEX IF NOT EXISTS ix_raw_variant_prices_bsale_detail_id
    ON bsale_raw.variant_prices (company_id, bsale_detail_id);

CREATE TABLE IF NOT EXISTS bsale_raw.variant_costs (
    company_id BIGINT NOT NULL,
    variant_id BIGINT NOT NULL,
    average_cost NUMERIC,
    total_cost NUMERIC,
    history_count INTEGER,
    last_admission_date DATE,
    history_complete BOOLEAN NOT NULL DEFAULT false,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    api_fetched_at TIMESTAMPTZ NOT NULL,
    last_source TEXT NOT NULL,
    sync_run_id BIGINT,
    CONSTRAINT pk_raw_variant_costs PRIMARY KEY (company_id, variant_id),
    CONSTRAINT fk_raw_variant_costs_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_raw_variant_costs_last_source
        CHECK (last_source IN ('WEBHOOK', 'POINT', 'INCREMENTAL', 'FULL_RECONCILE', 'SCANNER', 'WINDOW_RECONCILE'))
);
CREATE INDEX IF NOT EXISTS ix_raw_variant_costs_fetched
    ON bsale_raw.variant_costs (company_id, api_fetched_at);

COMMENT ON COLUMN bsale_raw.stocks.api_fetched_at IS
    'Instante de la respuesta HTTP. Ningún UPSERT ni DELETE con datos más antiguos puede afectar la fila.';
COMMENT ON COLUMN bsale_raw.stocks.bsale_stock_id IS
    'id propio del registro de stock en Bsale. Sin UNIQUE (unicidad no demostrada).';
COMMENT ON COLUMN bsale_raw.variant_prices.bsale_detail_id IS
    'id del detalle de lista de precios en Bsale. Sin UNIQUE (unicidad no demostrada).';
COMMENT ON COLUMN bsale_raw.variant_costs.history_complete IS
    'Siempre false mientras Bsale no entregue metadata de paginación de history.';

COMMIT;
