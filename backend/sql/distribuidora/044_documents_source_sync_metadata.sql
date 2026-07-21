-- Identidad Bsale vigente y metadatos de reconciliación.
--
-- ``document_id`` sigue siendo la PK local estable. ``source_document_id`` es
-- el id externo vigente y puede cambiar si Bsale reemplaza/reemite un folio.

ALTER TABLE distribuidora.documents
    ADD COLUMN IF NOT EXISTS source_document_id BIGINT,
    ADD COLUMN IF NOT EXISTS source_hash TEXT,
    ADD COLUMN IF NOT EXISTS source_updated_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_synced_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_reconciliation_at TIMESTAMPTZ;
-- +go

COMMENT ON COLUMN distribuidora.documents.source_document_id IS
    'Id externo vigente en Bsale; puede diferir de document_id tras reemisión.';
COMMENT ON COLUMN distribuidora.documents.source_hash IS
    'SHA-256 del encabezado y detalles Bsale usados en la última persistencia.';
COMMENT ON COLUMN distribuidora.documents.source_updated_at IS
    'modificationDate o generationDate del source vigente en Bsale.';
COMMENT ON COLUMN distribuidora.documents.last_synced_at IS
    'Última revisión Bsale exitosa, haya o no cambios de contenido.';
COMMENT ON COLUMN distribuidora.documents.last_reconciliation_at IS
    'Cursor rotativo: último intento de reconciliación de la OC.';
-- +go

CREATE INDEX IF NOT EXISTS idx_documents_source_document_id
    ON distribuidora.documents (source_document_id)
    WHERE source_document_id IS NOT NULL;
-- +go

CREATE INDEX IF NOT EXISTS idx_documents_oc_reconciliation_cursor
    ON distribuidora.documents (
        last_reconciliation_at NULLS FIRST,
        document_id
    )
    WHERE company_id = 3
      AND office_id = 1
      AND document_type_id = 33
      AND state = 0;
-- +go

UPDATE distribuidora.documents d
SET
    source_document_id = CASE
        WHEN d.raw_data->>'id' ~ '^[0-9]+$'
        THEN (d.raw_data->>'id')::bigint
        ELSE d.document_id
    END,
    source_updated_at = COALESCE(
        CASE
            WHEN d.raw_data->>'modificationDate' ~ '^[0-9]+$'
            THEN to_timestamp((d.raw_data->>'modificationDate')::bigint)
            ELSE NULL
        END,
        d.generation_date
    ),
    last_synced_at = d.updated_at
WHERE d.source_document_id IS NULL
   OR d.source_updated_at IS NULL
   OR d.last_synced_at IS NULL;
-- +go
