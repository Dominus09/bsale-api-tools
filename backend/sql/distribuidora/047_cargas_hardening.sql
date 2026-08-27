-- Hardening Cargas V1 (046 ya aplicado).
-- Solo ALTER / CREATE adicionales. NO DROP. NO recrear tablas.
-- NO ejecutar automáticamente desde Cursor.

-- 1) Hash SHA-256 del archivo original (preview ↔ import)
ALTER TABLE distribuidora.loads
    ADD COLUMN IF NOT EXISTS file_hash TEXT;
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_loads_file_hash
    ON distribuidora.loads (file_hash)
    WHERE file_hash IS NOT NULL;
-- +go

-- 2) Cancelación explícita
ALTER TABLE distribuidora.loads
    ADD COLUMN IF NOT EXISTS cancel_reason TEXT;
-- +go

ALTER TABLE distribuidora.loads
    ADD COLUMN IF NOT EXISTS cancelled_by TEXT;
-- +go

ALTER TABLE distribuidora.loads
    ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMPTZ;
-- +go

-- 3) Conservar certificación previa al reopen (no perder histórico)
ALTER TABLE distribuidora.loads
    ADD COLUMN IF NOT EXISTS last_certified_by TEXT;
-- +go

ALTER TABLE distribuidora.loads
    ADD COLUMN IF NOT EXISTS last_certified_at TIMESTAMPTZ;
-- +go

ALTER TABLE distribuidora.loads
    ADD COLUMN IF NOT EXISTS reopen_reason TEXT;
-- +go

-- 4) Auditoría de cambios de estado de la carga (certify / cancel / reopen)
CREATE TABLE IF NOT EXISTS distribuidora.load_status_events (
    id              BIGSERIAL PRIMARY KEY,
    load_id         BIGINT NOT NULL
        REFERENCES distribuidora.loads (id) ON DELETE CASCADE,
    from_status     TEXT,
    to_status       TEXT NOT NULL,
    user_email      TEXT NOT NULL,
    reason          TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_load_status_events_load
    ON distribuidora.load_status_events (load_id, created_at DESC);
-- +go

COMMENT ON COLUMN distribuidora.loads.file_hash IS
    'SHA-256 hex de los bytes originales del archivo importado.';
-- +go

COMMENT ON COLUMN distribuidora.loads.last_certified_by IS
    'Último certificador conocido; se conserva al reabrir (certified_by se limpia).';
-- +go

COMMENT ON TABLE distribuidora.load_status_events IS
    'Historial append-only de transiciones de estado de cargas.';
