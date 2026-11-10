-- P4: visitas de entrega, resultados por documento/línea, cobranza declarada,
-- evidencias/firmas (R2) y auditoría. Aditivo. No muta Bsale/picking/ORS.
-- No aplicar automáticamente desde syncs/HTTP.

CREATE TABLE IF NOT EXISTS distribuidora.delivery_visits (
    id BIGSERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL,
    dispatch_plan_id BIGINT NOT NULL
        REFERENCES distribuidora.dispatch_plan (id) ON DELETE CASCADE,
    customer_key TEXT NOT NULL,
    client_id BIGINT,
    driver_user_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'in_progress'
        CHECK (status IN (
            'in_progress',
            'completed',
            'partial',
            'not_delivered',
            'cancelled'
        )),
    result_reason TEXT,
    result_note TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at TIMESTAMPTZ,
    start_lat DOUBLE PRECISION,
    start_lng DOUBLE PRECISION,
    start_accuracy_m REAL,
    start_gps_captured_at TIMESTAMPTZ,
    start_gps_source TEXT,
    end_lat DOUBLE PRECISION,
    end_lng DOUBLE PRECISION,
    end_accuracy_m REAL,
    end_gps_captured_at TIMESTAMPTZ,
    end_gps_source TEXT,
    gps_exception_code TEXT,
    gps_exception_note TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    idempotency_key TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +go

CREATE UNIQUE INDEX IF NOT EXISTS uq_delivery_visit_active
    ON distribuidora.delivery_visits (dispatch_plan_id, customer_key)
    WHERE status = 'in_progress';
-- +go

CREATE UNIQUE INDEX IF NOT EXISTS uq_delivery_visit_idem
    ON distribuidora.delivery_visits (driver_user_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
-- +go

CREATE INDEX IF NOT EXISTS idx_delivery_visits_plan_status
    ON distribuidora.delivery_visits (dispatch_plan_id, status, started_at DESC);
-- +go

CREATE INDEX IF NOT EXISTS idx_delivery_visits_company
    ON distribuidora.delivery_visits (company_id, started_at DESC);
-- +go

CREATE INDEX IF NOT EXISTS idx_delivery_visits_driver
    ON distribuidora.delivery_visits (driver_user_id, started_at DESC);
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.delivery_visit_events (
    id BIGSERIAL PRIMARY KEY,
    visit_id BIGINT NOT NULL
        REFERENCES distribuidora.delivery_visits (id) ON DELETE CASCADE,
    action TEXT NOT NULL,
    actor_user_id INTEGER,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +go

CREATE INDEX IF NOT EXISTS idx_delivery_visit_events_visit
    ON distribuidora.delivery_visit_events (visit_id, created_at DESC);
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.delivery_document_results (
    id BIGSERIAL PRIMARY KEY,
    visit_id BIGINT NOT NULL
        REFERENCES distribuidora.delivery_visits (id) ON DELETE CASCADE,
    related_document_id BIGINT,
    oc_document_id BIGINT,
    document_number BIGINT,
    document_type TEXT,
    document_total NUMERIC,
    result_status TEXT NOT NULL DEFAULT 'pending'
        CHECK (result_status IN (
            'pending', 'full', 'partial', 'none', 'skipped'
        )),
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +go

CREATE UNIQUE INDEX IF NOT EXISTS uq_delivery_doc_result_visit_doc
    ON distribuidora.delivery_document_results (visit_id, related_document_id)
    WHERE related_document_id IS NOT NULL;
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.delivery_line_results (
    id BIGSERIAL PRIMARY KEY,
    visit_id BIGINT NOT NULL
        REFERENCES distribuidora.delivery_visits (id) ON DELETE CASCADE,
    document_result_id BIGINT NOT NULL
        REFERENCES distribuidora.delivery_document_results (id) ON DELETE CASCADE,
    variant_id BIGINT,
    sku_code TEXT,
    product_name TEXT,
    planned_units NUMERIC NOT NULL DEFAULT 0,
    delivered_units NUMERIC NOT NULL DEFAULT 0,
    undelivered_units NUMERIC NOT NULL DEFAULT 0,
    undelivered_reason TEXT,
    undelivered_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_delivery_line_units_nonneg CHECK (
        planned_units >= 0 AND delivered_units >= 0 AND undelivered_units >= 0
    ),
    CONSTRAINT ck_delivery_line_delivered_le_planned CHECK (
        delivered_units <= planned_units
    )
);
-- +go

CREATE INDEX IF NOT EXISTS idx_delivery_line_results_visit
    ON distribuidora.delivery_line_results (visit_id);
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.delivery_collections (
    id BIGSERIAL PRIMARY KEY,
    visit_id BIGINT NOT NULL
        REFERENCES distribuidora.delivery_visits (id) ON DELETE CASCADE,
    document_result_id BIGINT
        REFERENCES distribuidora.delivery_document_results (id) ON DELETE CASCADE,
    method TEXT NOT NULL
        CHECK (method IN (
            'cash',
            'transfer_declared',
            'transfer_verified',
            'credit',
            'partial',
            'pending',
            'not_applicable'
        )),
    document_amount NUMERIC,
    declared_amount NUMERIC,
    verified_amount NUMERIC,
    pending_balance NUMERIC,
    note TEXT,
    created_by_user_id INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +go

CREATE INDEX IF NOT EXISTS idx_delivery_collections_visit
    ON distribuidora.delivery_collections (visit_id);
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.delivery_evidence (
    id BIGSERIAL PRIMARY KEY,
    visit_id BIGINT NOT NULL
        REFERENCES distribuidora.delivery_visits (id) ON DELETE CASCADE,
    company_id INTEGER NOT NULL,
    evidence_kind TEXT NOT NULL
        CHECK (evidence_kind IN (
            'closed_store',
            'not_found',
            'rejected_order',
            'absent_customer',
            'signature',
            'other'
        )),
    object_key TEXT NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'image/jpeg',
    size_bytes INTEGER,
    sha256 TEXT,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    captured_by_user_id INTEGER NOT NULL,
    capture_lat DOUBLE PRECISION,
    capture_lng DOUBLE PRECISION,
    capture_accuracy_m REAL,
    upload_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
    validation_status TEXT NOT NULL DEFAULT 'pending'
        CHECK (validation_status IN (
            'pending', 'approved', 'rejected', 'upload_pending', 'exception'
        )),
    exception_note TEXT,
    idempotency_key TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_delivery_evidence_object_key UNIQUE (object_key)
);
-- +go

CREATE UNIQUE INDEX IF NOT EXISTS uq_delivery_evidence_idem
    ON distribuidora.delivery_evidence (captured_by_user_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
-- +go

CREATE INDEX IF NOT EXISTS idx_delivery_evidence_visit
    ON distribuidora.delivery_evidence (visit_id, created_at DESC);
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.delivery_conformity (
    id BIGSERIAL PRIMARY KEY,
    visit_id BIGINT NOT NULL UNIQUE
        REFERENCES distribuidora.delivery_visits (id) ON DELETE CASCADE,
    receptor_name TEXT,
    signature_object_key TEXT,
    signature_content_type TEXT,
    accepted_summary_sha256 TEXT,
    signed_at TIMESTAMPTZ,
    refused BOOLEAN NOT NULL DEFAULT FALSE,
    refuse_reason TEXT,
    gps_lat DOUBLE PRECISION,
    gps_lng DOUBLE PRECISION,
    gps_accuracy_m REAL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.delivery_report_snapshots (
    id BIGSERIAL PRIMARY KEY,
    visit_id BIGINT NOT NULL
        REFERENCES distribuidora.delivery_visits (id) ON DELETE CASCADE,
    report_text TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    generated_by_user_id INTEGER NOT NULL
);
-- +go

CREATE INDEX IF NOT EXISTS idx_delivery_report_snapshots_visit
    ON distribuidora.delivery_report_snapshots (visit_id, generated_at DESC);
-- +go

COMMENT ON TABLE distribuidora.delivery_visits IS
    'Visitas de entrega PWA (chofer). No muta Bsale ni picking.';
-- +go

COMMENT ON TABLE distribuidora.delivery_evidence IS
    'Metadatos de evidencias; bytes en R2 privado (object_key).';
-- +go
