-- Peso oficial de órdenes de compra (fuente para planificación, camiones, ORS).

CREATE TABLE IF NOT EXISTS distribuidora.order_weight_snapshots (
    id BIGSERIAL PRIMARY KEY,
    document_id BIGINT NOT NULL REFERENCES distribuidora.documents(document_id) ON DELETE CASCADE,
    company_id INT NOT NULL,
    office_id INT NOT NULL,
    oc_number INT,
    peso_total_kg NUMERIC(14, 3) NOT NULL DEFAULT 0,
    productos_totales INT NOT NULL DEFAULT 0,
    productos_con_peso INT NOT NULL DEFAULT 0,
    productos_sin_peso INT NOT NULL DEFAULT 0,
    productos_manuales INT NOT NULL DEFAULT 0,
    productos_estimados INT NOT NULL DEFAULT 0,
    porcentaje_cobertura NUMERIC(5, 1) NOT NULL DEFAULT 0,
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    calculated_by TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +go

CREATE UNIQUE INDEX IF NOT EXISTS uq_order_weight_snapshots_document
    ON distribuidora.order_weight_snapshots (document_id);
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.order_weight_snapshot_lines (
    id BIGSERIAL PRIMARY KEY,
    snapshot_id BIGINT NOT NULL REFERENCES distribuidora.order_weight_snapshots(id) ON DELETE CASCADE,
    detail_id BIGINT NOT NULL,
    line_number INT,
    codigo TEXT,
    producto TEXT,
    variante TEXT,
    cantidad_unitaria NUMERIC(14, 4) NOT NULL DEFAULT 0,
    cantidad_cajas NUMERIC(14, 4),
    units_per_box INT,
    peso_unitario_kg NUMERIC(14, 6),
    peso_caja_kg NUMERIC(14, 4),
    peso_linea_kg NUMERIC(14, 3) NOT NULL DEFAULT 0,
    fuente_peso TEXT NOT NULL DEFAULT 'sin_datos',
    estado_linea TEXT NOT NULL DEFAULT 'sin_peso',
    products_master_id BIGINT,
    variant_id BIGINT,
    join_debug JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (snapshot_id, detail_id)
);
-- +go

CREATE INDEX IF NOT EXISTS idx_order_weight_snapshot_lines_snapshot
    ON distribuidora.order_weight_snapshot_lines (snapshot_id);
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.order_weight_history (
    id BIGSERIAL PRIMARY KEY,
    document_id BIGINT NOT NULL REFERENCES distribuidora.documents(document_id) ON DELETE CASCADE,
    user_email TEXT,
    peso_anterior_kg NUMERIC(14, 3),
    peso_nuevo_kg NUMERIC(14, 3),
    productos_modificados INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +go

CREATE INDEX IF NOT EXISTS idx_order_weight_history_document
    ON distribuidora.order_weight_history (document_id, created_at DESC);
-- +go

COMMENT ON TABLE distribuidora.order_weight_snapshots IS
    'Peso oficial calculado por OC; consumido por planificación y capacidad de camiones.';
-- +go
