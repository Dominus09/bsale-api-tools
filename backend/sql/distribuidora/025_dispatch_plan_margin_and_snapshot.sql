-- Margen comercial real + snapshot extendido + estado delivered.

ALTER TABLE distribuidora.dispatch_plan
    ADD COLUMN IF NOT EXISTS commercial_margin_clp INTEGER,
    ADD COLUMN IF NOT EXISTS net_operational_clp INTEGER,
    ADD COLUMN IF NOT EXISTS margin_computation_source TEXT,
    ADD COLUMN IF NOT EXISTS margin_lines_with_cost INTEGER,
    ADD COLUMN IF NOT EXISTS margin_lines_total INTEGER;
-- +go

ALTER TABLE distribuidora.dispatch_plan_orders
    ADD COLUMN IF NOT EXISTS fantasy_name TEXT;
-- +go

COMMENT ON COLUMN distribuidora.dispatch_plan.commercial_margin_clp IS
    'Suma margen comercial documentos confirmados (venta - costo variante Bsale). NULL si costos incompletos.';
-- +go

COMMENT ON COLUMN distribuidora.dispatch_plan.net_operational_clp IS
    'commercial_margin_clp - total_route_cost_clp (resultado operativo neto).';
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
        'dispatched',
        'delivered'
    ));
-- +go
