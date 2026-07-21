-- Invalidación no destructiva de planes cuando cambia una OC fuente.

ALTER TABLE distribuidora.dispatch_plan
    ADD COLUMN IF NOT EXISTS needs_recalculation BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS invalidated_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS invalidation_reason TEXT;
-- +go

COMMENT ON COLUMN distribuidora.dispatch_plan.needs_recalculation IS
    'TRUE cuando una OC cambió después de congelar pesos/paradas del plan.';
-- +go

CREATE INDEX IF NOT EXISTS idx_dispatch_plan_needs_recalculation
    ON distribuidora.dispatch_plan (needs_recalculation, planning_date)
    WHERE needs_recalculation = TRUE;
-- +go
