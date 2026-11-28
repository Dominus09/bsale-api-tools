-- AWAITING_APPROVAL: only planning data, no stock/pallet/layout writes.
BEGIN;
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
