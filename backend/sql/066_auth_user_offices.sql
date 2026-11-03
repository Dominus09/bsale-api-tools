-- PROPUESTA — NO EJECUTAR automáticamente.
-- BD objetivo: analytics (única). Aditivo.
-- Oficinas autorizadas: siempre (user_id, company_id, office_id).
-- office_id = bsale.offices.bsale_id. Validación de pertenencia en servicio;
-- FK opcional a offices cuando la PK compuesta está disponible.
-- Runbook: backend/sql/RUNBOOK_auth_multiapp_064_066.md

CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE IF NOT EXISTS app.user_offices (
  user_id    integer NOT NULL REFERENCES bsale.users (id) ON DELETE CASCADE,
  company_id integer NOT NULL REFERENCES bsale.companies (company_id) ON DELETE CASCADE,
  office_id  bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, company_id, office_id)
);

CREATE INDEX IF NOT EXISTS idx_user_offices_company_office
  ON app.user_offices (company_id, office_id);

CREATE INDEX IF NOT EXISTS idx_user_offices_user
  ON app.user_offices (user_id);

COMMENT ON TABLE app.user_offices IS
  'Oficinas PWA/ERP por usuario. office_id = bsale.offices.bsale_id; company_id obligatorio.';

-- FK lógica offices: solo si existe PK/unique (company_id, bsale_id).
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = 'bsale'
      AND t.relname = 'offices'
      AND c.contype IN ('p', 'u')
      AND pg_get_constraintdef(c.oid) ILIKE '%company_id%'
      AND pg_get_constraintdef(c.oid) ILIKE '%bsale_id%'
  ) AND NOT EXISTS (
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = 'app'
      AND t.relname = 'user_offices'
      AND c.conname = 'fk_user_offices_office'
  ) THEN
    ALTER TABLE app.user_offices
      ADD CONSTRAINT fk_user_offices_office
      FOREIGN KEY (company_id, office_id)
      REFERENCES bsale.offices (company_id, bsale_id)
      ON DELETE CASCADE;
  END IF;
END $$;
