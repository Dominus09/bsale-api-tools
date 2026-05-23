-- Snapshots de picking (cliente / producto) para auditoría; sin UI final aún.

CREATE TABLE IF NOT EXISTS distribuidora.dispatch_plan_picking_snapshots (
    id BIGSERIAL PRIMARY KEY,
    dispatch_plan_id BIGINT NOT NULL
        REFERENCES distribuidora.dispatch_plan (id) ON DELETE CASCADE,
    picking_type TEXT NOT NULL CHECK (picking_type IN ('client', 'product')),
    payload JSONB NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +go

CREATE INDEX IF NOT EXISTS idx_dispatch_plan_picking_plan_type
    ON distribuidora.dispatch_plan_picking_snapshots (dispatch_plan_id, picking_type, generated_at DESC);
-- +go
