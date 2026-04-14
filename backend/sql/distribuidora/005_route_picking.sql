-- Picking por cliente (líneas por OC planificada). Índice único: una fila por fila de route_planning.
-- +go es separador para ensure_distribuidora_schema.

CREATE TABLE IF NOT EXISTS distribuidora.route_picking (
    id BIGSERIAL PRIMARY KEY,
    planning_id BIGINT NOT NULL REFERENCES distribuidora.route_planning (id) ON DELETE CASCADE,
    document_id BIGINT NOT NULL,
    oc_number BIGINT,
    client_name TEXT,
    address TEXT,
    city TEXT,
    phone TEXT,
    document_number TEXT,
    payment_method TEXT,
    observations TEXT,
    seller TEXT,
    total_amount NUMERIC(18, 4),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +go

CREATE UNIQUE INDEX IF NOT EXISTS uq_distribuidora_route_picking_planning_id
    ON distribuidora.route_picking (planning_id);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_route_picking_document
    ON distribuidora.route_picking (document_id);
-- +go
