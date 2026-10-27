-- PROPUESTA — NO EJECUTAR automáticamente.
-- BD objetivo: analytics (única). Tabla vacía; no inventa relaciones.
-- Asociación usuario staff ↔ empresas autorizadas.
-- Runbook: backend/sql/RUNBOOK_auth_analytics_060_063.md
--
-- Política de transición (código):
--   AUTH_EMIT_COMPANY_IDS=0 (default): login NO emite company_ids → Analítica V2
--     sigue sin restringir por token (comportamiento actual).
--   AUTH_EMIT_COMPANY_IDS=1: si el usuario tiene filas → claim company_ids;
--     si no tiene filas → no restringir (salvo AUTH_REQUIRE_USER_COMPANIES=1).
--   AUTH_REQUIRE_USER_COMPANIES=1 (cutover): sin filas = login 403.
--
-- NO poblar masivamente sin inventario gerencial multi-empresa.

CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE IF NOT EXISTS app.user_companies (
  user_id    integer NOT NULL REFERENCES bsale.users (id) ON DELETE CASCADE,
  company_id integer NOT NULL REFERENCES bsale.companies (company_id) ON DELETE CASCADE,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, company_id)
);

CREATE INDEX IF NOT EXISTS idx_user_companies_company
  ON app.user_companies (company_id);

COMMENT ON TABLE app.user_companies IS
  'Empresas permitidas por usuario staff ERP. Ausencia de filas = sin restricción hasta cutover.';
