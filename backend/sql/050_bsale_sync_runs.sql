-- Historial por corrida de los sync Bsale (sync_bsale_catalog y scripts raíz).
-- Aplicar MANUALMENTE. Idempotente. No borra ni modifica datos existentes.

CREATE TABLE IF NOT EXISTS bsale.sync_runs (
    id BIGSERIAL PRIMARY KEY,
    job TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    error TEXT,
    duration_ms BIGINT,
    companies_expected BIGINT[] NOT NULL DEFAULT '{}',
    companies_processed BIGINT[] NOT NULL DEFAULT '{}',
    stats JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT chk_bsale_sync_runs_status
        CHECK (status IN ('running', 'success', 'failed', 'partial'))
);

CREATE INDEX IF NOT EXISTS idx_bsale_sync_runs_job_started
    ON bsale.sync_runs (job, started_at DESC);
