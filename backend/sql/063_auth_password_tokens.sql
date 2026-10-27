-- PROPUESTA — NO EJECUTAR automáticamente.
-- BD objetivo: analytics (única).
-- Tokens de invitación / restablecimiento de contraseña (un solo uso).
-- Runbook: backend/sql/RUNBOOK_auth_analytics_060_063.md
-- Se almacena SOLO el hash SHA-256 del token; el valor en claro se entrega
-- una vez al administrador o al flujo de correo (nunca en listados ni auditoría).

CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE IF NOT EXISTS app.auth_password_tokens (
  id           bigserial PRIMARY KEY,
  user_id      integer NOT NULL REFERENCES bsale.users (id) ON DELETE CASCADE,
  token_hash   text NOT NULL,
  purpose      text NOT NULL CHECK (purpose IN ('invite', 'reset')),
  expires_at   timestamptz NOT NULL,
  used_at      timestamptz NULL,
  created_by   integer NULL REFERENCES bsale.users (id) ON DELETE SET NULL,
  created_at   timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT auth_password_tokens_hash_unique UNIQUE (token_hash)
);

CREATE INDEX IF NOT EXISTS idx_auth_password_tokens_user
  ON app.auth_password_tokens (user_id, created_at DESC);

COMMENT ON TABLE app.auth_password_tokens IS
  'Invitaciones y resets; solo token_hash. No guardar el token en claro.';
