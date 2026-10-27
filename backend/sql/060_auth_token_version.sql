-- PROPUESTA — NO EJECUTAR automáticamente.
-- BD objetivo: analytics (única). Aditivo sobre bsale.users.
-- Etapa 1 autenticación: versión de token para invalidar sesiones tras
-- desactivar usuario o restablecer contraseña.
--
-- Código: claim JWT ``tv`` + AUTH_ENFORCE_TOKEN_VERSION=1
-- (sin esta columna el flag no puede revocar de forma fiable).
-- Runbook: backend/sql/RUNBOOK_auth_analytics_060_063.md

ALTER TABLE bsale.users
  ADD COLUMN IF NOT EXISTS token_version integer NOT NULL DEFAULT 0;

COMMENT ON COLUMN bsale.users.token_version IS
  'Incrementar al desactivar o resetear password; JWT claim tv debe coincidir.';
