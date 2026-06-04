-- Migración 030: cache operacional de estado OC (Pendiente / Probable / Facturada).
-- Calcular en sync o job "Actualizar facturación"; lectura O(1) por oc_document_id en UI/API.
--
-- Semántica alineada con:
--   015_v_purchase_document_status_full.sql
--   026_dispatch_plan_invoiced_view_perf.sql
--   backend/utils/invoicing_auto_confirm.py (umbrales 60 / 75)
--
-- Refresh (fase aplicación Python, no en este archivo):
--   refresh_purchase_document_status_cache(oc_document_ids bigint[])
--   Tras: sync orders, sync related, live_sync_probable_matches, botón actualizar facturación.

CREATE TABLE IF NOT EXISTS distribuidora.purchase_document_status_cache (
    oc_document_id BIGINT PRIMARY KEY
        REFERENCES distribuidora.documents (document_id) ON DELETE CASCADE,

    company_id INT NOT NULL DEFAULT 3,
    office_id INT NOT NULL DEFAULT 1,
    oc_number BIGINT,
    emission_date DATE,

    -- Confirmada vía document_related (Bsale relateddetailid)
    is_invoiced_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
    invoicing_document_id BIGINT,
    invoicing_document_type_id INT,
    invoicing_number BIGINT,
    invoicing_emission_date TIMESTAMPTZ,

    -- Mejor candidato probable (tabla document_probable_matches, score >= 60)
    probable_document_id BIGINT,
    probable_document_type_id INT,
    probable_number BIGINT,
    probable_score NUMERIC(5, 2),
    probable_tier TEXT,
    match_products_pct NUMERIC(5, 2),

    -- Estado unificado (API / Pre-despacho / badges)
    purchase_status TEXT NOT NULL DEFAULT 'PENDIENTE',
    estado_real TEXT NOT NULL DEFAULT 'Pendiente',
    associated_document_label TEXT,
    display_score NUMERIC(5, 2),

    -- Dashboard plan / picking / app choferes
    operational_status TEXT NOT NULL DEFAULT 'missing',
    relation_source TEXT,
    is_auto_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
    related_document_id BIGINT,
    related_document_number BIGINT,
    related_document_type_id INT,
    related_document_type_label TEXT,

    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    compute_source TEXT NOT NULL DEFAULT 'unknown',
    compute_version INT NOT NULL DEFAULT 1,

    CONSTRAINT chk_purchase_status_cache_purchase_status
        CHECK (
            purchase_status IN (
                'FACTURADA_CONFIRMADA',
                'PROBABLE_FACTURADA_HIGH',
                'PROBABLE_FACTURADA_MEDIUM',
                'PROBABLE_FACTURADA_LOW',
                'PENDIENTE'
            )
        ),
    CONSTRAINT chk_purchase_status_cache_operational_status
        CHECK (operational_status IN ('confirmed', 'probable', 'missing')),
    CONSTRAINT chk_purchase_status_cache_relation_source
        CHECK (
            relation_source IS NULL
            OR relation_source IN ('relateddetailid', 'auto_match', 'probable_match')
        ),
    CONSTRAINT chk_purchase_status_cache_probable_tier
        CHECK (
            probable_tier IS NULL
            OR probable_tier IN (
                'PROBABLE_FACTURADA_HIGH',
                'PROBABLE_FACTURADA_MEDIUM',
                'PROBABLE_FACTURADA_LOW'
            )
        )
);
-- +go

COMMENT ON TABLE distribuidora.purchase_document_status_cache IS
    'Estado operacional OC pre-calculado (confirmada Bsale + probable + pendiente). '
    'Refresh en sync/actualizar facturación; lectura en pre-despacho, plan, picking, app.';
-- +go

CREATE INDEX IF NOT EXISTS idx_purchase_status_cache_emission
    ON distribuidora.purchase_document_status_cache (company_id, office_id, emission_date DESC);
-- +go

CREATE INDEX IF NOT EXISTS idx_purchase_status_cache_purchase_status
    ON distribuidora.purchase_document_status_cache (purchase_status)
    WHERE company_id = 3 AND office_id = 1;
-- +go

CREATE INDEX IF NOT EXISTS idx_purchase_status_cache_operational
    ON distribuidora.purchase_document_status_cache (operational_status)
    WHERE company_id = 3 AND office_id = 1;
-- +go

CREATE INDEX IF NOT EXISTS idx_purchase_status_cache_not_invoiced
    ON distribuidora.purchase_document_status_cache (emission_date DESC, oc_document_id DESC)
    WHERE company_id = 3
      AND office_id = 1
      AND is_invoiced_confirmed = FALSE
      AND operational_status <> 'confirmed';
-- +go

CREATE INDEX IF NOT EXISTS idx_purchase_status_cache_computed_at
    ON distribuidora.purchase_document_status_cache (computed_at DESC);
-- +go

-- Vista de lectura para reemplazar joins pesados (consumidores migran aquí).
CREATE OR REPLACE VIEW distribuidora.v_purchase_document_status_cached AS
SELECT
    c.oc_document_id AS document_id,
    c.oc_document_id,
    c.oc_number AS number,
    c.emission_date,
    c.is_invoiced_confirmed AS is_invoiced,
    c.invoicing_document_id,
    c.invoicing_document_type_id,
    c.invoicing_number,
    c.invoicing_emission_date,
    c.probable_document_id,
    c.probable_document_type_id,
    c.probable_number AS probable_number,
    c.probable_score,
    c.probable_tier,
    c.match_products_pct,
    c.purchase_status,
    c.estado_real,
    c.associated_document_label,
    c.display_score,
    c.operational_status AS status,
    c.relation_source,
    c.is_auto_confirmed,
    c.related_document_id,
    c.related_document_number,
    c.related_document_type_id,
    c.related_document_type_label,
    c.computed_at,
    c.compute_source
FROM distribuidora.purchase_document_status_cache c
WHERE c.company_id = 3
  AND c.office_id = 1;
-- +go

COMMENT ON VIEW distribuidora.v_purchase_document_status_cached IS
    'Lectura rápida de estado OC desde purchase_document_status_cache (sin LATERAL por request).';
-- +go

-- Log opcional de corridas de refresh (auditoría / diagnóstico).
CREATE TABLE IF NOT EXISTS distribuidora.purchase_document_status_cache_runs (
    id BIGSERIAL PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    compute_source TEXT NOT NULL,
    date_from DATE,
    date_to DATE,
    ocs_requested INT NOT NULL DEFAULT 0,
    ocs_upserted INT NOT NULL DEFAULT 0,
    elapsed_ms INT,
    error_message TEXT
);
-- +go

CREATE INDEX IF NOT EXISTS idx_purchase_status_cache_runs_started
    ON distribuidora.purchase_document_status_cache_runs (started_at DESC);
-- +go

-- Plantilla de refresh (ejecutar desde aplicación; referencia de lógica CASE).
-- INSERT INTO distribuidora.purchase_document_status_cache (...)
-- SELECT
--     oc.document_id,
--     oc.company_id,
--     oc.office_id,
--     oc.number,
--     oc.emission_date::date,
--     (inv.document_id IS NOT NULL),
--     inv.document_id,
--     ...
-- FROM distribuidora.documents oc
-- LEFT JOIN LATERAL ( ... document_related ... ) inv ON TRUE
-- LEFT JOIN LATERAL ( ... document_probable_matches score>=60 ... ) prob ON NOT inv
-- ON CONFLICT (oc_document_id) DO UPDATE SET ...;
