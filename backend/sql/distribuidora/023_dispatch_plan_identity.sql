-- Identidad persistente y margen final por planificación.

CREATE SEQUENCE IF NOT EXISTS distribuidora.dispatch_plan_code_seq START 1;
-- +go

ALTER TABLE distribuidora.dispatch_plan
    ADD COLUMN IF NOT EXISTS planning_code TEXT,
    ADD COLUMN IF NOT EXISTS planning_name TEXT,
    ADD COLUMN IF NOT EXISTS truck_name TEXT,
    ADD COLUMN IF NOT EXISTS invoiced_sales_clp INTEGER,
    ADD COLUMN IF NOT EXISTS final_margin_clp INTEGER,
    ADD COLUMN IF NOT EXISTS margin_calculated_at TIMESTAMPTZ;
-- +go

UPDATE distribuidora.dispatch_plan
SET planning_code = 'PLAN-' || LPAD(id::text, 5, '0')
WHERE planning_code IS NULL OR BTRIM(planning_code) = '';
-- +go

UPDATE distribuidora.dispatch_plan dp
SET truck_name = COALESCE(
    dp.truck_name,
    t.name,
    dp.route_name
)
FROM distribuidora.trucks t
WHERE t.id = dp.truck_id
  AND (dp.truck_name IS NULL OR BTRIM(dp.truck_name) = '');
-- +go

UPDATE distribuidora.dispatch_plan
SET planning_name = COALESCE(NULLIF(BTRIM(planning_name), ''), route_name)
WHERE planning_name IS NULL OR BTRIM(planning_name) = '';
-- +go

CREATE UNIQUE INDEX IF NOT EXISTS uq_dispatch_plan_planning_code
    ON distribuidora.dispatch_plan (planning_code)
    WHERE planning_code IS NOT NULL;
-- +go

CREATE INDEX IF NOT EXISTS idx_dispatch_plan_created
    ON distribuidora.dispatch_plan (created_at DESC);
-- +go

COMMENT ON COLUMN distribuidora.dispatch_plan.planning_code IS
    'Código legible permanente, ej. PLAN-00014.';
-- +go
