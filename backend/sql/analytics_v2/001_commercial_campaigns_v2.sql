-- =============================================================================
-- Analítica V2 — Promociones y Remates V2 (campañas comerciales).
--
-- NO ejecutar en producción sin autorización. Migración versionada, idempotente y aditiva:
--   * crea tablas nuevas en el esquema app;
--   * NO modifica app.promotions, app.promotion_items, app.promotion_companies ni
--     app.promotion_price_snapshot (se conservan intactas y se leen en modo compatibilidad);
--   * NO toca bsale_raw ni precios de Bsale.
--
-- Reglas de integridad:
--   * cada ítem pertenece a la misma empresa que su campaña (FK compuesta);
--   * un SKU no puede repetirse activo en la misma lista dentro de una campaña;
--   * el historial es append-only (trigger rechaza UPDATE/DELETE);
--   * campañas e ítems no se borran (trigger rechaza DELETE): se cancelan o retiran.
-- =============================================================================

BEGIN;

CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE IF NOT EXISTS app.commercial_campaigns_v2 (
    id BIGSERIAL,
    company_id INTEGER NOT NULL,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    starts_on DATE NOT NULL,
    ends_on DATE NOT NULL,
    approval_status TEXT NOT NULL DEFAULT 'borrador',
    responsible TEXT,
    notes TEXT,
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by TEXT,
    updated_at TIMESTAMPTZ,
    submitted_by TEXT,
    submitted_at TIMESTAMPTZ,
    approved_by TEXT,
    approved_at TIMESTAMPTZ,
    rejected_by TEXT,
    rejected_at TIMESTAMPTZ,
    cancelled_by TEXT,
    cancelled_at TIMESTAMPTZ,
    status_reason TEXT,
    legacy_promotion_id INTEGER,
    version INTEGER NOT NULL DEFAULT 1,
    CONSTRAINT pk_commercial_campaigns_v2 PRIMARY KEY (id),
    CONSTRAINT uq_commercial_campaigns_v2_id_company UNIQUE (id, company_id),
    CONSTRAINT fk_commercial_campaigns_v2_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_commercial_campaigns_v2_kind CHECK (kind IN ('oferta', 'remate')),
    CONSTRAINT ck_commercial_campaigns_v2_approval
        CHECK (approval_status IN ('borrador', 'pendiente_aprobacion', 'aprobada', 'rechazada', 'cancelada')),
    CONSTRAINT ck_commercial_campaigns_v2_dates CHECK (ends_on >= starts_on),
    CONSTRAINT ck_commercial_campaigns_v2_name CHECK (length(btrim(name)) > 0),
    CONSTRAINT ck_commercial_campaigns_v2_remate_responsible
        CHECK (kind <> 'remate' OR length(btrim(coalesce(responsible, ''))) > 0),
    CONSTRAINT ck_commercial_campaigns_v2_approved
        CHECK (approval_status <> 'aprobada' OR (approved_by IS NOT NULL AND approved_at IS NOT NULL)),
    CONSTRAINT ck_commercial_campaigns_v2_cancelled
        CHECK (approval_status <> 'cancelada' OR (cancelled_by IS NOT NULL AND cancelled_at IS NOT NULL)),
    CONSTRAINT ck_commercial_campaigns_v2_version CHECK (version >= 1)
);

-- Una promoción legacy solo puede importarse una vez por empresa.
CREATE UNIQUE INDEX IF NOT EXISTS uq_commercial_campaigns_v2_legacy
    ON app.commercial_campaigns_v2 (company_id, legacy_promotion_id)
    WHERE legacy_promotion_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_commercial_campaigns_v2_company_dates
    ON app.commercial_campaigns_v2 (company_id, starts_on, ends_on);
CREATE INDEX IF NOT EXISTS ix_commercial_campaigns_v2_company_approval
    ON app.commercial_campaigns_v2 (company_id, approval_status, ends_on);

CREATE TABLE IF NOT EXISTS app.commercial_campaign_items_v2 (
    id BIGSERIAL,
    campaign_id BIGINT NOT NULL,
    company_id INTEGER NOT NULL,
    variant_id BIGINT NOT NULL,
    product_id BIGINT,
    price_list_id BIGINT NOT NULL,
    sku_code TEXT,
    barcode TEXT,
    product_name TEXT,
    variant_name TEXT,
    normal_gross_price NUMERIC(14, 2),
    normal_price_fetched_at TIMESTAMPTZ,
    promo_gross_price NUMERIC(14, 2) NOT NULL,
    clearance_reason TEXT,
    reference_stock NUMERIC(14, 3),
    reference_stock_fetched_at TIMESTAMPTZ,
    notes TEXT,
    item_status TEXT NOT NULL DEFAULT 'activo',
    legacy_snapshot_id INTEGER,
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by TEXT,
    updated_at TIMESTAMPTZ,
    removed_by TEXT,
    removed_at TIMESTAMPTZ,
    version INTEGER NOT NULL DEFAULT 1,
    CONSTRAINT pk_commercial_campaign_items_v2 PRIMARY KEY (id),
    CONSTRAINT fk_commercial_campaign_items_v2_campaign
        FOREIGN KEY (campaign_id, company_id)
        REFERENCES app.commercial_campaigns_v2 (id, company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_commercial_campaign_items_v2_promo_price CHECK (promo_gross_price > 0),
    CONSTRAINT ck_commercial_campaign_items_v2_normal_price
        CHECK (normal_gross_price IS NULL OR normal_gross_price >= 0),
    CONSTRAINT ck_commercial_campaign_items_v2_status CHECK (item_status IN ('activo', 'retirado')),
    CONSTRAINT ck_commercial_campaign_items_v2_clearance
        CHECK (clearance_reason IS NULL
               OR clearance_reason IN ('vencimiento', 'descontinuado', 'dano', 'sobrestock', 'otro')),
    CONSTRAINT ck_commercial_campaign_items_v2_removed
        CHECK (item_status <> 'retirado' OR (removed_by IS NOT NULL AND removed_at IS NOT NULL)),
    CONSTRAINT ck_commercial_campaign_items_v2_version CHECK (version >= 1)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_commercial_campaign_items_v2_active
    ON app.commercial_campaign_items_v2 (campaign_id, variant_id, price_list_id)
    WHERE item_status = 'activo';
CREATE INDEX IF NOT EXISTS ix_commercial_campaign_items_v2_lookup
    ON app.commercial_campaign_items_v2 (company_id, variant_id, price_list_id);
CREATE INDEX IF NOT EXISTS ix_commercial_campaign_items_v2_campaign
    ON app.commercial_campaign_items_v2 (campaign_id);

CREATE TABLE IF NOT EXISTS app.commercial_campaign_history_v2 (
    id BIGSERIAL,
    campaign_id BIGINT NOT NULL,
    item_id BIGINT,
    company_id INTEGER NOT NULL,
    event TEXT NOT NULL,
    previous_status TEXT,
    new_status TEXT,
    previous_values JSONB,
    new_values JSONB,
    reason TEXT,
    actor TEXT NOT NULL,
    actor_role TEXT,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_commercial_campaign_history_v2 PRIMARY KEY (id),
    CONSTRAINT fk_commercial_campaign_history_v2_campaign
        FOREIGN KEY (campaign_id, company_id)
        REFERENCES app.commercial_campaigns_v2 (id, company_id) ON DELETE RESTRICT,
    CONSTRAINT fk_commercial_campaign_history_v2_item
        FOREIGN KEY (item_id) REFERENCES app.commercial_campaign_items_v2 (id) ON DELETE RESTRICT,
    CONSTRAINT ck_commercial_campaign_history_v2_event
        CHECK (event IN (
            'campania_creada', 'campania_editada', 'enviada_aprobacion', 'aprobada', 'rechazada',
            'cancelada', 'aprobacion_revocada_por_cambio', 'item_agregado', 'item_editado',
            'item_precio_cambiado', 'item_retirado', 'importada_legacy'
        ))
);

CREATE INDEX IF NOT EXISTS ix_commercial_campaign_history_v2_campaign
    ON app.commercial_campaign_history_v2 (campaign_id, occurred_at);
CREATE INDEX IF NOT EXISTS ix_commercial_campaign_history_v2_company_time
    ON app.commercial_campaign_history_v2 (company_id, occurred_at DESC);

CREATE OR REPLACE FUNCTION app.commercial_v2_reject_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% no permitido en %: historial comercial V2 inmutable', TG_OP, TG_TABLE_NAME;
END;
$$;

DROP TRIGGER IF EXISTS trg_commercial_campaign_history_v2_append_only ON app.commercial_campaign_history_v2;
CREATE TRIGGER trg_commercial_campaign_history_v2_append_only
    BEFORE UPDATE OR DELETE ON app.commercial_campaign_history_v2
    FOR EACH ROW EXECUTE FUNCTION app.commercial_v2_reject_mutation();

DROP TRIGGER IF EXISTS trg_commercial_campaigns_v2_no_delete ON app.commercial_campaigns_v2;
CREATE TRIGGER trg_commercial_campaigns_v2_no_delete
    BEFORE DELETE ON app.commercial_campaigns_v2
    FOR EACH ROW EXECUTE FUNCTION app.commercial_v2_reject_mutation();

DROP TRIGGER IF EXISTS trg_commercial_campaign_items_v2_no_delete ON app.commercial_campaign_items_v2;
CREATE TRIGGER trg_commercial_campaign_items_v2_no_delete
    BEFORE DELETE ON app.commercial_campaign_items_v2
    FOR EACH ROW EXECUTE FUNCTION app.commercial_v2_reject_mutation();

COMMENT ON TABLE app.commercial_campaigns_v2 IS
    'Promociones V2: campañas de oferta o remate por empresa. Registro comercial; no aplica precios en Bsale.';
COMMENT ON COLUMN app.commercial_campaigns_v2.approval_status IS
    'Estado de aprobación. El estado temporal (programada/vigente/finalizada) se deriva de las fechas al consultar.';
COMMENT ON COLUMN app.commercial_campaign_items_v2.normal_gross_price IS
    'Precio bruto Bsale (bsale_raw.variant_prices.variant_value_with_taxes) leído en backend al registrar el ítem.';
COMMENT ON COLUMN app.commercial_campaign_items_v2.reference_stock IS
    'Stock disponible total (bsale_raw.stocks) leído en backend al registrar el ítem. Solo referencia.';
COMMENT ON TABLE app.commercial_campaign_history_v2 IS
    'Historial append-only de campañas e ítems: estados, precios, motivos y usuario responsable.';

COMMIT;
