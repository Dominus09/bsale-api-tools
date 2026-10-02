-- =============================================================================
-- bsale_raw 002 — control de corridas, frescura y checkpoints
--
-- scope (TEXT, sin CHECK). Convención canónica, definida exclusivamente en
-- backend/services/bsale_raw/core/registry.py:
--   global                          recurso completo de la empresa
--   office:<office_id>              stock por sucursal
--   price_list:<price_list_id>      precios por lista
--   document_type:<document_type_id> documentos por tipo (p. ej. OC 33)
-- Nuevos scopes se agregan sólo en registry.py; no requieren migración.
-- El SLA de frescura vive en el registry (ResourceSpec.freshness_sla_seconds), no aquí.
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS bsale_raw.sync_runs (
    id BIGSERIAL NOT NULL,
    mode TEXT NOT NULL,
    trigger TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'RUNNING',
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    host TEXT,
    summary JSONB,
    error TEXT,
    CONSTRAINT pk_raw_sync_runs PRIMARY KEY (id),
    CONSTRAINT ck_raw_sync_runs_mode
        CHECK (mode IN ('WEBHOOK', 'POINT', 'INCREMENTAL', 'FULL_RECONCILE', 'SCANNER', 'WINDOW_RECONCILE')),
    CONSTRAINT ck_raw_sync_runs_status
        CHECK (status IN ('RUNNING', 'SUCCESS', 'PARTIAL', 'FAILED', 'SKIPPED'))
);
CREATE INDEX IF NOT EXISTS ix_raw_sync_runs_mode_started
    ON bsale_raw.sync_runs (mode, started_at DESC);
CREATE INDEX IF NOT EXISTS ix_raw_sync_runs_running
    ON bsale_raw.sync_runs (started_at) WHERE status = 'RUNNING';

CREATE TABLE IF NOT EXISTS bsale_raw.sync_entity_runs (
    id BIGSERIAL NOT NULL,
    sync_run_id BIGINT NOT NULL,
    company_id BIGINT NOT NULL,
    resource TEXT NOT NULL,
    scope TEXT NOT NULL DEFAULT 'global',
    status TEXT NOT NULL,
    snapshot_started_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    rows_received INTEGER NOT NULL DEFAULT 0,
    rows_inserted INTEGER NOT NULL DEFAULT 0,
    rows_updated INTEGER NOT NULL DEFAULT 0,
    rows_unchanged INTEGER NOT NULL DEFAULT 0,
    rows_skipped_newer INTEGER NOT NULL DEFAULT 0,
    rows_missing INTEGER NOT NULL DEFAULT 0,
    rows_deleted INTEGER NOT NULL DEFAULT 0,
    api_count INTEGER,
    requests INTEGER NOT NULL DEFAULT 0,
    http_429 INTEGER NOT NULL DEFAULT 0,
    http_5xx INTEGER NOT NULL DEFAULT 0,
    duration_ms INTEGER,
    fuse JSONB,
    error TEXT,
    CONSTRAINT pk_raw_sync_entity_runs PRIMARY KEY (id),
    CONSTRAINT fk_raw_sync_entity_runs_run
        FOREIGN KEY (sync_run_id) REFERENCES bsale_raw.sync_runs (id) ON DELETE CASCADE,
    CONSTRAINT fk_raw_sync_entity_runs_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_raw_sync_entity_runs_status
        CHECK (status IN ('RUNNING', 'SUCCESS', 'PARTIAL', 'FAILED', 'SKIPPED'))
);
CREATE INDEX IF NOT EXISTS ix_raw_sync_entity_runs_run
    ON bsale_raw.sync_entity_runs (sync_run_id);
CREATE INDEX IF NOT EXISTS ix_raw_sync_entity_runs_lookup
    ON bsale_raw.sync_entity_runs (company_id, resource, scope, started_at DESC);

COMMENT ON COLUMN bsale_raw.sync_entity_runs.snapshot_started_at IS
    'Capturado ANTES del primer GET del snapshot. Un DELETE stale sólo puede afectar filas con api_fetched_at <= este valor.';
COMMENT ON COLUMN bsale_raw.sync_entity_runs.rows_skipped_newer IS
    'Filas del snapshot no aplicadas porque la fila destino tenía api_fetched_at posterior (webhook/targeted).';

CREATE TABLE IF NOT EXISTS bsale_raw.sync_state (
    company_id BIGINT NOT NULL,
    resource TEXT NOT NULL,
    scope TEXT NOT NULL DEFAULT 'global',
    last_attempt_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    last_webhook_at TIMESTAMPTZ,
    last_incremental_at TIMESTAMPTZ,
    last_full_reconcile_at TIMESTAMPTZ,
    last_error_at TIMESTAMPTZ,
    last_error TEXT,
    rows_received INTEGER,
    duration_ms INTEGER,
    status TEXT,
    last_sync_run_id BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_raw_sync_state PRIMARY KEY (company_id, resource, scope),
    CONSTRAINT fk_raw_sync_state_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_raw_sync_state_status
        CHECK (status IN ('RUNNING', 'SUCCESS', 'PARTIAL', 'FAILED', 'SKIPPED'))
);
CREATE INDEX IF NOT EXISTS ix_raw_sync_state_resource
    ON bsale_raw.sync_state (resource, last_success_at);

CREATE TABLE IF NOT EXISTS bsale_raw.sync_cursors (
    company_id BIGINT NOT NULL,
    resource TEXT NOT NULL,
    scope TEXT NOT NULL DEFAULT 'global',
    cursor_name TEXT NOT NULL,
    cursor_value JSONB NOT NULL,
    cycle_started_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sync_run_id BIGINT,
    CONSTRAINT pk_raw_sync_cursors PRIMARY KEY (company_id, resource, scope, cursor_name),
    CONSTRAINT fk_raw_sync_cursors_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT
);

COMMENT ON COLUMN bsale_raw.sync_state.scope IS
    'global | office:<id> | price_list:<id> | document_type:<id>. Convención en registry.py; sin CHECK.';
COMMENT ON COLUMN bsale_raw.sync_cursors.scope IS
    'global | office:<id> | price_list:<id> | document_type:<id>. Convención en registry.py; sin CHECK.';

COMMIT;
