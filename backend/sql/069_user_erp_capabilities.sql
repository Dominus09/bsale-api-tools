-- PROPUESTA — NO EJECUTAR automáticamente.
-- BD objetivo: analytics (única). Aditivo.
-- Capacidades ERP acotadas (p. ej. planificación simplificada) sin roles admin globales
-- y sin mezclar con perfiles operativos PWA (065).
--
-- Regla: tener ``simple_planning`` NO concede app ERP ni PWA.
-- El acceso a la pantalla sigue exigiendo JWT + app ERP (require_erp_app).

CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE IF NOT EXISTS app.user_erp_capabilities (
  user_id          integer NOT NULL REFERENCES bsale.users (id) ON DELETE CASCADE,
  capability_code  text NOT NULL CHECK (
    capability_code IN (
      'simple_planning'
    )
  ),
  active           boolean NOT NULL DEFAULT true,
  created_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, capability_code)
);

CREATE INDEX IF NOT EXISTS idx_user_erp_capabilities_code_active
  ON app.user_erp_capabilities (capability_code, active)
  WHERE active IS TRUE;

COMMENT ON TABLE app.user_erp_capabilities IS
  'Capacidades ERP acotadas (simple_planning, …). Independientes de perfiles PWA y de admin usuarios.';

COMMENT ON COLUMN app.user_erp_capabilities.capability_code IS
  'simple_planning: confirmar planificación simplificada por lista/Excel; no implica admin ni acceso PWA.';
