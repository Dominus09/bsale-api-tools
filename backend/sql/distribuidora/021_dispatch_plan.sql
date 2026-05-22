-- Planificación operacional por camión: snapshot, facturación y picking desde documentos reales.

CREATE TABLE IF NOT EXISTS distribuidora.dispatch_plan (
    id BIGSERIAL PRIMARY KEY,
    plan_session_id TEXT,
    planning_date DATE NOT NULL DEFAULT CURRENT_DATE,
    truck_id INTEGER REFERENCES distribuidora.trucks (id),
    route_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft'
        CHECK (status IN (
            'draft',
            'planned',
            'invoicing',
            'ready_for_picking',
            'picking_generated',
            'dispatched'
        )),
    driver_count INTEGER NOT NULL DEFAULT 1 CHECK (driver_count >= 0 AND driver_count <= 10),
    assistant_count INTEGER NOT NULL DEFAULT 0 CHECK (assistant_count >= 0 AND assistant_count <= 10),
    driver_cost_clp INTEGER NOT NULL DEFAULT 0,
    assistant_cost_clp INTEGER NOT NULL DEFAULT 0,
    diesel_price_per_liter NUMERIC(12, 2),
    km_total NUMERIC(12, 3),
    duration_min NUMERIC(12, 2),
    liters_estimated NUMERIC(12, 3),
    fuel_cost_clp INTEGER NOT NULL DEFAULT 0,
    ferry_cost_clp INTEGER NOT NULL DEFAULT 0,
    toll_cost_clp INTEGER NOT NULL DEFAULT 0,
    extras_cost_clp INTEGER NOT NULL DEFAULT 0,
    crew_cost_clp INTEGER NOT NULL DEFAULT 0,
    total_route_cost_clp INTEGER NOT NULL DEFAULT 0,
    route_geometry JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    confirmed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +go

CREATE INDEX IF NOT EXISTS idx_dispatch_plan_session
    ON distribuidora.dispatch_plan (plan_session_id);
-- +go

CREATE INDEX IF NOT EXISTS idx_dispatch_plan_date_truck
    ON distribuidora.dispatch_plan (planning_date, truck_id);
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.dispatch_plan_orders (
    id BIGSERIAL PRIMARY KEY,
    dispatch_plan_id BIGINT NOT NULL
        REFERENCES distribuidora.dispatch_plan (id) ON DELETE CASCADE,
    oc_document_id BIGINT NOT NULL,
    oc_number BIGINT,
    route_order INTEGER NOT NULL DEFAULT 0,
    client_id BIGINT,
    client_name TEXT,
    address TEXT,
    city TEXT,
    seller_name TEXT,
    payment_method TEXT,
    document_type_to_generate TEXT,
    oc_total_amount NUMERIC(18, 2),
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dispatch_plan_orders_plan_doc UNIQUE (dispatch_plan_id, oc_document_id)
);
-- +go

CREATE INDEX IF NOT EXISTS idx_dispatch_plan_orders_plan_order
    ON distribuidora.dispatch_plan_orders (dispatch_plan_id, route_order);
-- +go

COMMENT ON TABLE distribuidora.dispatch_plan IS
    'Snapshot operacional por camión/ruta ORS (congelado al confirmar).';
-- +go

COMMENT ON TABLE distribuidora.dispatch_plan_orders IS
    'OCs incluidas en el plan; base para Excel facturación y enlace post-factura.';
-- +go
