-- Aditivo 3B.3.2 — aplicar SOLO vía apply_distribuidora_schema o psql manual.
-- Vincula despachos a usuarios reales (perfil PWA chofer) sin borrar driver_name histórico.

ALTER TABLE distribuidora.dispatch_plan
    ADD COLUMN IF NOT EXISTS driver_user_id INTEGER
        REFERENCES bsale.users (id);

CREATE INDEX IF NOT EXISTS idx_dispatch_plan_driver_user_date
    ON distribuidora.dispatch_plan (driver_user_id, planning_date)
    WHERE driver_user_id IS NOT NULL;

COMMENT ON COLUMN distribuidora.dispatch_plan.driver_user_id IS
  'Usuario bsale.users con perfil PWA chofer asignado al despacho. Independiente de confirm_dispatch_plan. driver_name se mantiene como etiqueta histórica/display.';
