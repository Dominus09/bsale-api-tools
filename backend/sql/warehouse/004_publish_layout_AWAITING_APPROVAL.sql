-- =============================================================================
-- warehouse / 004 — Publicación formal de layouts (slice 1.4)
-- AWAITING_APPROVAL — NO producción sin autorización.
--
-- - Como máximo una revisión published vigente por piso.
-- - Metadatos de publicación / origen / snapshot de facility+floor.
-- Idempotente (IF NOT EXISTS / ADD COLUMN seguro).
-- =============================================================================

BEGIN;

-- Una sola published activa por piso
CREATE UNIQUE INDEX IF NOT EXISTS uq_warehouse_one_published_per_floor
    ON warehouse.layout_revisions (floor_id)
    WHERE status = 'published';

-- Columnas de trazabilidad de publicación (compatibles con PG 12+)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'warehouse' AND table_name = 'layout_revisions'
          AND column_name = 'source_revision_id'
    ) THEN
        ALTER TABLE warehouse.layout_revisions
            ADD COLUMN source_revision_id BIGINT
                REFERENCES warehouse.layout_revisions (id) ON DELETE SET NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'warehouse' AND table_name = 'layout_revisions'
          AND column_name = 'archived_at'
    ) THEN
        ALTER TABLE warehouse.layout_revisions
            ADD COLUMN archived_at TIMESTAMPTZ;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'warehouse' AND table_name = 'layout_revisions'
          AND column_name = 'archived_by'
    ) THEN
        ALTER TABLE warehouse.layout_revisions
            ADD COLUMN archived_by TEXT;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'warehouse' AND table_name = 'layout_revisions'
          AND column_name = 'publish_meta_json'
    ) THEN
        ALTER TABLE warehouse.layout_revisions
            ADD COLUMN publish_meta_json JSONB NOT NULL DEFAULT '{}'::jsonb;
    END IF;
END $$;

COMMENT ON COLUMN warehouse.layout_revisions.source_revision_id IS
  'Revisión de origen al crear borrador desde published/archived, o draft origen al publicar.';
COMMENT ON COLUMN warehouse.layout_revisions.publish_meta_json IS
  'Snapshot facility/floor + conteos de validación al publicar. No es certificación de seguridad.';

COMMIT;
