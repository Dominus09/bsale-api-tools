-- Módulo Cargas: certificación física de mercadería en camión.
-- Independiente de dispatch_plan_load_batches. Solo CREATE (sin ALTER de tablas existentes).

CREATE TABLE IF NOT EXISTS distribuidora.loads (
    id                      BIGSERIAL PRIMARY KEY,
    picking_number          TEXT NOT NULL,
    picking_date            DATE,
    destination             TEXT,
    truck                   TEXT,
    seal                    TEXT,
    status                  TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN (
            'draft', 'pending', 'in_progress', 'completed', 'certified', 'cancelled'
        )),
    original_filename       TEXT,
    source_type             TEXT NOT NULL
        CHECK (source_type IN ('excel', 'pdf')),
    total_requested_units   NUMERIC(14, 3) NOT NULL DEFAULT 0,
    total_items             INTEGER NOT NULL DEFAULT 0,
    total_value             NUMERIC(14, 2),
    document_units_total    NUMERIC(14, 3),
    document_value_total    NUMERIC(14, 2),
    created_by              TEXT NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    loading_started_at      TIMESTAMPTZ,
    loading_finished_at     TIMESTAMPTZ,
    certified_by            TEXT,
    certified_at            TIMESTAMPTZ,
    reopened_by             TEXT,
    reopened_at             TIMESTAMPTZ,
    notes                   TEXT
);
-- +go

CREATE UNIQUE INDEX IF NOT EXISTS uq_distribuidora_loads_picking_active
    ON distribuidora.loads (picking_number)
    WHERE status <> 'cancelled';
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_loads_status_created
    ON distribuidora.loads (status, created_at DESC);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_loads_picking_number
    ON distribuidora.loads (picking_number);
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.load_items (
    id                      BIGSERIAL PRIMARY KEY,
    load_id                 BIGINT NOT NULL
        REFERENCES distribuidora.loads (id) ON DELETE CASCADE,
    line_number             INTEGER,
    branch                  TEXT,
    product_type            TEXT,
    product_name            TEXT NOT NULL,
    normalized_product_name TEXT NOT NULL,
    barcode                 TEXT,
    sec                     INTEGER,
    requested_units         NUMERIC(14, 3) NOT NULL CHECK (requested_units > 0),
    source_boxes_value      NUMERIC(14, 4),
    certified_units         NUMERIC(14, 3) NOT NULL DEFAULT 0
        CHECK (certified_units >= 0),
    total_value             NUMERIC(14, 2),
    status                  TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'partial', 'complete', 'excess', 'issue')),
    last_event_at           TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_load_items_load_id
    ON distribuidora.load_items (load_id);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_load_items_barcode
    ON distribuidora.load_items (load_id, barcode);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_load_items_status
    ON distribuidora.load_items (load_id, status);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_load_items_normalized_name
    ON distribuidora.load_items (load_id, normalized_product_name);
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.load_item_events (
    id              BIGSERIAL PRIMARY KEY,
    load_id         BIGINT NOT NULL
        REFERENCES distribuidora.loads (id) ON DELETE CASCADE,
    load_item_id    BIGINT NOT NULL
        REFERENCES distribuidora.load_items (id) ON DELETE CASCADE,
    user_email      TEXT NOT NULL,
    action          TEXT NOT NULL
        CHECK (action IN (
            'add', 'subtract', 'complete', 'correction', 'issue', 'resolve_issue', 'reopen'
        )),
    boxes           NUMERIC(14, 4),
    loose_units     NUMERIC(14, 3),
    units_delta     NUMERIC(14, 3) NOT NULL,
    units_after     NUMERIC(14, 3) NOT NULL,
    notes           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_load_item_events_load
    ON distribuidora.load_item_events (load_id, created_at DESC);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_load_item_events_item
    ON distribuidora.load_item_events (load_item_id, created_at DESC);
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.load_issues (
    id              BIGSERIAL PRIMARY KEY,
    load_id         BIGINT NOT NULL
        REFERENCES distribuidora.loads (id) ON DELETE CASCADE,
    load_item_id    BIGINT NOT NULL
        REFERENCES distribuidora.load_items (id) ON DELETE CASCADE,
    issue_type      TEXT NOT NULL
        CHECK (issue_type IN (
            'not_found', 'insufficient_stock', 'wrong_product', 'damaged',
            'excess', 'picking_error', 'other'
        )),
    description     TEXT,
    expected_units  NUMERIC(14, 3),
    actual_units    NUMERIC(14, 3),
    status          TEXT NOT NULL DEFAULT 'open'
        CHECK (status IN ('open', 'resolved', 'dismissed')),
    created_by      TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_by     TEXT,
    resolved_at     TIMESTAMPTZ
);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_load_issues_load_status
    ON distribuidora.load_issues (load_id, status);
-- +go

COMMENT ON TABLE distribuidora.loads IS
    'Cargas/pickings importados (Excel/PDF) para certificación física en camión.';
-- +go

COMMENT ON COLUMN distribuidora.load_items.requested_units IS
    'Cantidad oficial solicitada en unidades (columna CANTIDAD del picking).';
-- +go

COMMENT ON COLUMN distribuidora.load_items.source_boxes_value IS
    'Valor informativo de Cajas x cargar del documento; no es fuente oficial.';
