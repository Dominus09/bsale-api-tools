-- Marca temporal Bsale (modificación) distinta de generation_date y updated_at ERP.

ALTER TABLE distribuidora.documents
    ADD COLUMN IF NOT EXISTS bsale_modified_at TIMESTAMPTZ;
-- +go

COMMENT ON COLUMN distribuidora.documents.bsale_modified_at IS
    'Última modificación en Bsale (modificationDate o generationDate del JSON).';
-- +go

UPDATE distribuidora.documents d
SET bsale_modified_at = COALESCE(
    CASE
        WHEN d.raw_data->>'modificationDate' ~ '^[0-9]+$'
        THEN to_timestamp((d.raw_data->>'modificationDate')::bigint)
        ELSE NULL
    END,
    d.generation_date
)
WHERE d.bsale_modified_at IS NULL;
-- +go
