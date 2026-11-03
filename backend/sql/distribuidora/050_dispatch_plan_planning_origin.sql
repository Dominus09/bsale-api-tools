-- PROPUESTA — NO EJECUTAR automáticamente.
-- BD objetivo: analytics. Aditivo. No recalcula planes ORS existentes.
--
-- Distingue origen de planificación y métricas ORS realmente calculadas.
-- Costos INTEGER NOT NULL se mantienen en 0 por compatibilidad legacy;
-- ors_metrics_calculated=false indica que 0/NULL no son costos reales.

ALTER TABLE distribuidora.dispatch_plan
    ADD COLUMN IF NOT EXISTS planning_origin TEXT NOT NULL DEFAULT 'ors';

ALTER TABLE distribuidora.dispatch_plan
    ADD COLUMN IF NOT EXISTS ors_metrics_calculated BOOLEAN NOT NULL DEFAULT TRUE;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'dispatch_plan_planning_origin_check'
      AND conrelid = 'distribuidora.dispatch_plan'::regclass
  ) THEN
    ALTER TABLE distribuidora.dispatch_plan
      ADD CONSTRAINT dispatch_plan_planning_origin_check
      CHECK (planning_origin IN ('ors', 'simple_list'));
  END IF;
END $$;

COMMENT ON COLUMN distribuidora.dispatch_plan.planning_origin IS
  'ors = ruta ORS tradicional; simple_list = planificación simplificada por lista de OC.';

COMMENT ON COLUMN distribuidora.dispatch_plan.ors_metrics_calculated IS
  'true = km/duración/combustible/costos ORS fueron calculados; false = no calculados (no interpretar ceros legacy como costo real).';

-- Planes previos: origen ORS con métricas consideradas calculadas (default).
UPDATE distribuidora.dispatch_plan
SET planning_origin = COALESCE(NULLIF(BTRIM(planning_origin), ''), 'ors'),
    ors_metrics_calculated = COALESCE(ors_metrics_calculated, TRUE)
WHERE TRUE;
