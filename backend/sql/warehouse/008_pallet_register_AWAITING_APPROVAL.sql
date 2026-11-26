-- Fase 2A: levantamiento físico de pallets. NO ejecutar sin aprobación.
-- Aditivo; no cambia stock, Bsale, layout ni datos de PWA.
BEGIN;
SET LOCAL lock_timeout = '5s';
DO $guard$
DECLARE missing text;
BEGIN
    IF current_database() <> 'analytics' AND NOT (
        left(current_database(),length('warehouse_pallets_test')) = 'warehouse_pallets_test'
        AND COALESCE(current_setting('warehouse.pallets_lab', true),'off') = 'on'
    ) THEN
        RAISE EXCEPTION 'Destino no autorizado para warehouse 008: %', current_database();
    END IF;
    IF to_regclass('warehouse.physical_elements') IS NULL
       OR to_regclass('warehouse.layout_elements') IS NULL
       OR to_regclass('warehouse.layout_revisions') IS NULL THEN
        RAISE EXCEPTION 'Aplicar y verificar primero esquema warehouse layout 001-004';
    END IF;
    SELECT string_agg(req.t || '.' || req.c, ', ') INTO missing
    FROM (VALUES
        ('variants','company_id'),('variants','bsale_id'),('variants','product_id'),
        ('variants','description'),('variants','bar_code'),('variants','code'),
        ('variants','units_per_box'),('products','company_id'),('products','bsale_id'),
        ('products','name'),('products','product_type_id'),
        ('product_types','company_id'),('product_types','bsale_id'),('product_types','name')
    ) AS req(t,c)
    WHERE NOT EXISTS (
        SELECT 1 FROM information_schema.columns col
        WHERE col.table_schema='bsale' AND col.table_name=req.t AND col.column_name=req.c
    );
    IF missing IS NOT NULL THEN
        RAISE EXCEPTION 'Faltan columnas de catálogo Bsale: %', missing;
    END IF;
END
$guard$;

CREATE TABLE IF NOT EXISTS warehouse.pallets (
    id BIGSERIAL PRIMARY KEY,
    registration_key UUID NOT NULL UNIQUE,
    request_hash TEXT NOT NULL,
    company_id BIGINT NOT NULL,
    facility_id BIGINT NOT NULL REFERENCES warehouse.facilities(id) ON DELETE RESTRICT,
    floor_id BIGINT NOT NULL REFERENCES warehouse.floors(id) ON DELETE RESTRICT,
    pallet_kind TEXT NOT NULL CHECK (pallet_kind IN ('complete','partial','mixed')),
    received_on DATE,
    notes TEXT,
    position_id BIGINT REFERENCES warehouse.physical_elements(id) ON DELETE RESTRICT,
    position_code_snap TEXT,
    observed_level INTEGER,
    layout_revision_id BIGINT REFERENCES warehouse.layout_revisions(id) ON DELETE RESTRICT,
    layout_row_version INTEGER,
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','voided')),
    row_version INTEGER NOT NULL DEFAULT 1 CHECK (row_version > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by TEXT NOT NULL,
    CONSTRAINT ck_pallet_observed_location CHECK (
      (position_id IS NULL AND position_code_snap IS NULL AND observed_level IS NULL
       AND layout_revision_id IS NULL AND layout_row_version IS NULL)
      OR
      (position_id IS NOT NULL AND position_code_snap IS NOT NULL
       AND observed_level IS NOT NULL AND observed_level > 0
       AND layout_revision_id IS NOT NULL AND layout_row_version IS NOT NULL)
    )
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_warehouse_pallet_active_observed_slot
    ON warehouse.pallets(position_id, observed_level)
    WHERE status='active' AND position_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_warehouse_pallets_floor
    ON warehouse.pallets(floor_id, status, id DESC);
COMMENT ON TABLE warehouse.pallets IS
    'Identidad persistente PAL-<id>. Ubicacion y nivel son observaciones, no autorizacion de seguridad ni stock Bsale.';
COMMENT ON COLUMN warehouse.pallets.observed_level IS
    'Nivel fisico observado, 1=piso; no equivale a capacidad de apilamiento autorizada.';

CREATE TABLE IF NOT EXISTS warehouse.pallet_lines (
    id BIGSERIAL PRIMARY KEY,
    pallet_id BIGINT NOT NULL REFERENCES warehouse.pallets(id) ON DELETE RESTRICT,
    line_number INTEGER NOT NULL CHECK (line_number > 0),
    variant_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    product_name_snap TEXT NOT NULL,
    variant_name_snap TEXT,
    product_type_snap TEXT,
    barcode_snap TEXT,
    sku_snap TEXT,
    units_per_box_snap INTEGER CHECK (units_per_box_snap > 0),
    quantity_status TEXT NOT NULL CHECK (quantity_status IN ('verified','unknown')),
    boxes INTEGER CHECK (boxes >= 0),
    loose_units INTEGER CHECK (loose_units >= 0),
    total_units BIGINT CHECK (total_units > 0 AND total_units <= 9007199254740991),
    expiry_status TEXT NOT NULL CHECK (expiry_status IN ('dated','unknown','not_applicable')),
    expires_on DATE,
    lot TEXT,
    UNIQUE (pallet_id,line_number),
    CONSTRAINT ck_pallet_line_quantity CHECK (
      (quantity_status='unknown' AND boxes IS NULL AND loose_units IS NULL AND total_units IS NULL)
      OR
      (quantity_status='verified' AND boxes IS NOT NULL AND loose_units IS NOT NULL
       AND total_units IS NOT NULL AND (boxes=0 OR units_per_box_snap IS NOT NULL)
       AND total_units=boxes::bigint*COALESCE(units_per_box_snap,0)+loose_units)
    ),
    CONSTRAINT ck_pallet_line_expiry CHECK ((expiry_status='dated')=(expires_on IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS ix_warehouse_pallet_lines_variant
    ON warehouse.pallet_lines(variant_id);
CREATE INDEX IF NOT EXISTS ix_warehouse_pallet_lines_expiry
    ON warehouse.pallet_lines(expires_on) WHERE expires_on IS NOT NULL;

CREATE TABLE IF NOT EXISTS warehouse.pallet_events (
    id BIGSERIAL PRIMARY KEY,
    pallet_id BIGINT NOT NULL REFERENCES warehouse.pallets(id) ON DELETE RESTRICT,
    action TEXT NOT NULL CHECK (action IN ('registered','location_observed','voided')),
    actor_user_id BIGINT,
    actor_label TEXT NOT NULL,
    detail_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_warehouse_pallet_events
    ON warehouse.pallet_events(pallet_id,id DESC);
COMMIT;
