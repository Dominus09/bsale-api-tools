-- PROPOSAL: apply manually to the actual ERP analytics database after approval/backup.
-- Additive, idempotent. Empty overrides preserve all existing environment defaults.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE IF NOT EXISTS app.system_feature_controls (
    feature_key text PRIMARY KEY CHECK (feature_key ~ '^[a-z][a-z0-9_]{1,79}$'),
    enabled_override boolean,
    row_version bigint NOT NULL DEFAULT 0 CHECK (row_version >= 0),
    updated_by bigint,
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS app.system_feature_control_audit (
    id bigserial PRIMARY KEY,
    feature_key text NOT NULL REFERENCES app.system_feature_controls(feature_key),
    actor_user_id bigint NOT NULL CHECK (actor_user_id > 0),
    before_override boolean,
    after_override boolean,
    before_configured boolean NOT NULL,
    after_configured boolean NOT NULL,
    row_version bigint NOT NULL CHECK (row_version > 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (feature_key, row_version)
);
COMMENT ON TABLE app.system_feature_controls IS
  'Explicit OWNER overrides for server-allowlisted operational features. NULL inherits existing environment default. Never stores credentials.';
COMMENT ON TABLE app.system_feature_control_audit IS
  'Append-only via feature control service. Records actor and previous/new configured values, not secrets.';
COMMIT;
