-- Pickings múltiples por planificación + asignación documental + auditoría operacional.

CREATE TABLE IF NOT EXISTS distribuidora.dispatch_plan_load_batches (
    id BIGSERIAL PRIMARY KEY,
    dispatch_plan_id BIGINT NOT NULL
        REFERENCES distribuidora.dispatch_plan (id) ON DELETE CASCADE,
    sort_order INTEGER NOT NULL DEFAULT 0,
    name TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +go

CREATE INDEX IF NOT EXISTS idx_dispatch_plan_load_batches_plan
    ON distribuidora.dispatch_plan_load_batches (dispatch_plan_id, sort_order);
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.dispatch_plan_document_load_assignments (
    id BIGSERIAL PRIMARY KEY,
    dispatch_plan_id BIGINT NOT NULL
        REFERENCES distribuidora.dispatch_plan (id) ON DELETE CASCADE,
    load_batch_id BIGINT
        REFERENCES distribuidora.dispatch_plan_load_batches (id) ON DELETE SET NULL,
    related_document_id BIGINT NOT NULL,
    oc_document_id BIGINT,
    document_number BIGINT,
    client_name TEXT,
    document_total NUMERIC(18, 2),
    assigned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dispatch_plan_doc_load_assignment
        UNIQUE (dispatch_plan_id, related_document_id)
);
-- +go

CREATE INDEX IF NOT EXISTS idx_dispatch_plan_doc_load_assignments_batch
    ON distribuidora.dispatch_plan_document_load_assignments (load_batch_id);
-- +go

CREATE INDEX IF NOT EXISTS idx_dispatch_plan_doc_load_assignments_plan
    ON distribuidora.dispatch_plan_document_load_assignments (dispatch_plan_id);
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.dispatch_plan_order_events (
    id BIGSERIAL PRIMARY KEY,
    dispatch_plan_id BIGINT NOT NULL
        REFERENCES distribuidora.dispatch_plan (id) ON DELETE CASCADE,
    action TEXT NOT NULL,
    user_name TEXT,
    reason TEXT,
    oc_document_id BIGINT,
    oc_number BIGINT,
    picking_id BIGINT,
    picking_version INTEGER,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +go

CREATE INDEX IF NOT EXISTS idx_dispatch_plan_order_events_plan_created
    ON distribuidora.dispatch_plan_order_events (dispatch_plan_id, created_at DESC);
-- +go

ALTER TABLE distribuidora.dispatch_plan_pickings
    ADD COLUMN IF NOT EXISTS regenerated_by TEXT,
    ADD COLUMN IF NOT EXISTS regeneration_reason TEXT;
-- +go

ALTER TABLE distribuidora.dispatch_plan
    DROP CONSTRAINT IF EXISTS dispatch_plan_status_check;
-- +go

ALTER TABLE distribuidora.dispatch_plan
    ADD CONSTRAINT dispatch_plan_status_check
    CHECK (status IN (
        'draft',
        'planned',
        'invoicing',
        'ready_for_picking',
        'picking_generated',
        'closed',
        'dispatched',
        'delivered',
        'squared'
    ));
-- +go

COMMENT ON TABLE distribuidora.dispatch_plan_load_batches IS
    'Grupos configurables de carga física (Picking 1, 2, N) por planificación.';
-- +go

COMMENT ON TABLE distribuidora.dispatch_plan_document_load_assignments IS
    'Asignación de documento facturado a un grupo de picking operacional.';
-- +go

COMMENT ON TABLE distribuidora.dispatch_plan_order_events IS
    'Auditoría: órdenes agregadas y regeneraciones de picking por plan.';
-- +go
