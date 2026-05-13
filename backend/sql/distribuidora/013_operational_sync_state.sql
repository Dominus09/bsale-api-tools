-- Estado operacional oficial (watermark, ventanas, overlap) para jobs incremental / backfill.
-- La tabla histórica por ``process_name`` (last_sync incremental documentos) pasa a ``sync_process_cursor``
-- para liberar el nombre ``sync_state`` sin tocar ``sync_status``, ``sync_logs`` ni ``v_sync_status``.

DO $rename_legacy$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'distribuidora' AND table_name = 'sync_state'
    ) AND EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'distribuidora' AND table_name = 'sync_state'
          AND column_name = 'process_name'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'distribuidora' AND table_name = 'sync_process_cursor'
    ) THEN
        ALTER TABLE distribuidora.sync_state RENAME TO sync_process_cursor;
    END IF;
END
$rename_legacy$;
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.sync_state (
    id BIGSERIAL PRIMARY KEY,
    sync_type TEXT NOT NULL,
    mode TEXT NOT NULL,
    office_id INT NOT NULL,
    last_success_at TIMESTAMPTZ,
    last_window_from TIMESTAMPTZ,
    last_window_to TIMESTAMPTZ,
    last_watermark TIMESTAMPTZ,
    overlap_seconds INT,
    overlap_days INT,
    status TEXT NOT NULL DEFAULT 'idle',
    items_processed BIGINT NOT NULL DEFAULT 0,
    error_summary TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_distribuidora_sync_state_mode CHECK (mode IN ('incremental', 'backfill')),
    CONSTRAINT uq_distribuidora_sync_state_op_key UNIQUE (sync_type, mode, office_id)
);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_sync_state_office_updated
    ON distribuidora.sync_state (office_id, updated_at DESC);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_sync_state_type_mode
    ON distribuidora.sync_state (sync_type, mode);
-- +go
