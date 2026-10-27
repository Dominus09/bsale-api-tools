-- PROPUESTA — NO EJECUTAR automáticamente.
-- BD objetivo: analytics (única).
-- Auditoría de autenticación y cambios administrativos (eventos futuros).
-- Runbook: backend/sql/RUNBOOK_auth_analytics_060_063.md
-- Activación código: AUTH_AUDIT_EVENTS=1
--
-- Retención recomendada (job aparte, no incluido aquí):
--   login_success / login_failure / logout: 180 días
--   cambios admin (rol, active, password, companies): 365 días
-- Lectura: solo ADMIN_ROLES vía futuro módulo Usuarios y accesos.
-- No almacenar passwords, hashes ni tokens en detail.

CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE IF NOT EXISTS app.auth_events (
  id            bigserial PRIMARY KEY,
  event_type    text NOT NULL,
  user_id       integer NULL REFERENCES bsale.users (id) ON DELETE SET NULL,
  email         text NULL,
  success       boolean NULL,
  ip            text NULL,
  user_agent    text NULL,
  actor_user_id integer NULL REFERENCES bsale.users (id) ON DELETE SET NULL,
  detail        jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_auth_events_created
  ON app.auth_events (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_auth_events_user_created
  ON app.auth_events (user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_auth_events_type_created
  ON app.auth_events (event_type, created_at DESC);

COMMENT ON TABLE app.auth_events IS
  'Eventos de login y administración de usuarios. Sin secretos en detail.';
