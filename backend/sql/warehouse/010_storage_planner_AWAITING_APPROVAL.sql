-- AWAITING_APPROVAL: only planning data, no stock/pallet/layout writes.
BEGIN;
SET LOCAL lock_timeout = '5s';
DO $guard$
BEGIN
    IF current_database() <> 'analytics' AND NOT (
        left(current_database(),length('warehouse_pallets_test'))='warehouse_pallets_test'
        AND COALESCE(current_setting('warehouse.pallets_lab',true),'off')='on'
    ) THEN
        RAISE EXCEPTION 'Destino no autorizado para planificación warehouse: %',current_database();
    END IF;
    IF to_regclass('warehouse.pallets') IS NULL OR to_regclass('warehouse.layout_elements') IS NULL THEN
        RAISE EXCEPTION 'Aplicar y verificar primero warehouse 001-009.';
    END IF;
END $guard$;
CREATE TABLE IF NOT EXISTS warehouse.storage_plans (
    id uuid PRIMARY KEY,
    company_id bigint NOT NULL,
    name text NOT NULL CHECK (length(name) BETWEEN 1 AND 120),
    row_version integer NOT NULL DEFAULT 1 CHECK (row_version > 0),
    document jsonb NOT NULL CHECK (jsonb_typeof(document) = 'object'),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    updated_by text NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_warehouse_storage_plans_company
    ON warehouse.storage_plans(company_id, updated_at DESC);
COMMIT;
