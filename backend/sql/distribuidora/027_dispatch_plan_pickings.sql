-- Picking persistido por plan (versionado). Fuente oficial para bodega, choferes y exportaciones.

CREATE TABLE IF NOT EXISTS distribuidora.dispatch_plan_pickings (
    id BIGSERIAL PRIMARY KEY,
    dispatch_plan_id BIGINT NOT NULL
        REFERENCES distribuidora.dispatch_plan (id) ON DELETE CASCADE,
    version INTEGER NOT NULL CHECK (version > 0),
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    include_probable BOOLEAN NOT NULL DEFAULT FALSE,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    superseded_at TIMESTAMPTZ,
    header JSONB NOT NULL DEFAULT '{}'::jsonb,
    warnings JSONB NOT NULL DEFAULT '[]'::jsonb,
    stops_count INTEGER NOT NULL DEFAULT 0,
    product_lines_count INTEGER NOT NULL DEFAULT 0,
    document_total_clp NUMERIC(18, 2) NOT NULL DEFAULT 0,
    product_total_monto_clp NUMERIC(18, 2) NOT NULL DEFAULT 0,
    CONSTRAINT uq_dispatch_plan_pickings_plan_version UNIQUE (dispatch_plan_id, version)
);
-- +go

CREATE UNIQUE INDEX IF NOT EXISTS uq_dispatch_plan_pickings_current
    ON distribuidora.dispatch_plan_pickings (dispatch_plan_id)
    WHERE is_current = TRUE;
-- +go

CREATE INDEX IF NOT EXISTS idx_dispatch_plan_pickings_plan_generated
    ON distribuidora.dispatch_plan_pickings (dispatch_plan_id, generated_at DESC);
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.dispatch_plan_picking_clients (
    id BIGSERIAL PRIMARY KEY,
    picking_id BIGINT NOT NULL
        REFERENCES distribuidora.dispatch_plan_pickings (id) ON DELETE CASCADE,
    dispatch_plan_id BIGINT NOT NULL
        REFERENCES distribuidora.dispatch_plan (id) ON DELETE CASCADE,
    route_order INTEGER NOT NULL DEFAULT 0,
    oc_document_id BIGINT,
    related_document_id BIGINT NOT NULL,
    client_id BIGINT,
    client_name TEXT,
    fantasy_name TEXT,
    address TEXT,
    city TEXT,
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION,
    phone TEXT,
    document_number BIGINT,
    document_type_label TEXT,
    payment_method TEXT,
    seller_name TEXT,
    observations TEXT,
    document_total NUMERIC(18, 2),
    relation_source TEXT,
    inclusion TEXT,
    is_probable_included BOOLEAN NOT NULL DEFAULT FALSE,
    probable_score NUMERIC(8, 2)
);
-- +go

CREATE INDEX IF NOT EXISTS idx_dispatch_plan_picking_clients_picking
    ON distribuidora.dispatch_plan_picking_clients (picking_id, route_order);
-- +go

CREATE INDEX IF NOT EXISTS idx_dispatch_plan_picking_clients_plan
    ON distribuidora.dispatch_plan_picking_clients (dispatch_plan_id, route_order);
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.dispatch_plan_picking_products (
    id BIGSERIAL PRIMARY KEY,
    picking_id BIGINT NOT NULL
        REFERENCES distribuidora.dispatch_plan_pickings (id) ON DELETE CASCADE,
    dispatch_plan_id BIGINT NOT NULL
        REFERENCES distribuidora.dispatch_plan (id) ON DELETE CASCADE,
    sort_order INTEGER NOT NULL DEFAULT 0,
    sucursal_bodega TEXT,
    tipo_producto TEXT,
    producto TEXT,
    variante TEXT,
    producto_variante TEXT,
    codigo_barras TEXT,
    unidades NUMERIC(18, 4) NOT NULL DEFAULT 0,
    cajas NUMERIC(18, 4),
    units_per_box NUMERIC(12, 4),
    sin_unidad_caja BOOLEAN NOT NULL DEFAULT FALSE,
    total_monto NUMERIC(18, 2) NOT NULL DEFAULT 0
);
-- +go

CREATE INDEX IF NOT EXISTS idx_dispatch_plan_picking_products_picking
    ON distribuidora.dispatch_plan_picking_products (picking_id, sort_order);
-- +go

CREATE INDEX IF NOT EXISTS idx_dispatch_plan_picking_products_plan
    ON distribuidora.dispatch_plan_picking_products (dispatch_plan_id);
-- +go

COMMENT ON TABLE distribuidora.dispatch_plan_pickings IS
    'Snapshot versionado de picking (cliente + producto) por plan de despacho.';
-- +go

COMMENT ON TABLE distribuidora.dispatch_plan_picking_clients IS
    'Paradas por documento facturado/auto-confirmado, orden de ruta y coordenadas.';
-- +go

COMMENT ON TABLE distribuidora.dispatch_plan_picking_products IS
    'Consolidado de productos del picking persistido para bodega.';
-- +go
