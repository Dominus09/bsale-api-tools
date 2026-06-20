-- Conteo de efectivo por denominación en cuadratura v2.

ALTER TABLE distribuidora.dispatch_plan_cuadratura
    ADD COLUMN IF NOT EXISTS cash_count JSONB NOT NULL DEFAULT '[]'::jsonb;

COMMENT ON COLUMN distribuidora.dispatch_plan_cuadratura.cash_count IS
    'Conteo físico de efectivo: [{denominacion_clp, cantidad, subtotal_clp}]';
