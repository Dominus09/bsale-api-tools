-- PROPUESTA — NO EJECUTAR automáticamente.
-- BD objetivo: analytics (única). Aditivo. Perfiles operativos PWA.
-- Un usuario puede tener varios perfiles activos.
-- Perfiles PWA son independientes de roles administrativos ERP (OWNER/GERENCIA/ADM).
-- Runbook: backend/sql/RUNBOOK_auth_multiapp_064_066.md

CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE IF NOT EXISTS app.user_operational_profiles (
  user_id       integer NOT NULL REFERENCES bsale.users (id) ON DELETE CASCADE,
  profile_code  text NOT NULL CHECK (
    profile_code IN (
      'vendedor',
      'chofer',
      'bodega',
      'cajero',
      'reponedor',
      'jefe_sucursal',
      'supervisor'
    )
  ),
  active        boolean NOT NULL DEFAULT true,
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, profile_code)
);

CREATE INDEX IF NOT EXISTS idx_user_operational_profiles_code_active
  ON app.user_operational_profiles (profile_code, active)
  WHERE active IS TRUE;

COMMENT ON TABLE app.user_operational_profiles IS
  'Perfiles PWA (vendedor, chofer, bodega, supervisor, …). Independientes del rol jerárquico ERP.';

-- Si la tabla ya existía sin 'supervisor', ampliar el CHECK de forma idempotente.
DO $$
BEGIN
  IF to_regclass('app.user_operational_profiles') IS NOT NULL THEN
    ALTER TABLE app.user_operational_profiles
      DROP CONSTRAINT IF EXISTS user_operational_profiles_profile_code_check;
    ALTER TABLE app.user_operational_profiles
      ADD CONSTRAINT user_operational_profiles_profile_code_check
      CHECK (
        profile_code IN (
          'vendedor',
          'chofer',
          'bodega',
          'cajero',
          'reponedor',
          'jefe_sucursal',
          'supervisor'
        )
      );
  END IF;
END $$;
