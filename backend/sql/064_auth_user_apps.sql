-- PROPUESTA — NO EJECUTAR automáticamente.
-- BD objetivo: analytics (única). Aditivo. Requiere schema app (061+).
-- Aplicaciones autorizadas por usuario: erp | pwa.
--
-- Transición (código, default seguro):
--   Sin filas en user_apps → acceso ERP legacy (no romper cuentas actuales).
--   Con filas → solo las apps con active=true.
--   PWA nunca se concede por defecto.
-- Runbook: backend/sql/RUNBOOK_auth_multiapp_064_066.md

CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE IF NOT EXISTS app.user_apps (
  user_id    integer NOT NULL REFERENCES bsale.users (id) ON DELETE CASCADE,
  app_code   text NOT NULL CHECK (app_code IN ('erp', 'pwa')),
  active     boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, app_code)
);

CREATE INDEX IF NOT EXISTS idx_user_apps_app_active
  ON app.user_apps (app_code, active)
  WHERE active IS TRUE;

COMMENT ON TABLE app.user_apps IS
  'Apps autorizadas (erp|pwa). Ausencia de filas = ERP legacy hasta cutover AUTH_REQUIRE_USER_APPS.';
