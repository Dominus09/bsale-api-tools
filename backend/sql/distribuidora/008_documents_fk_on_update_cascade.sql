-- Permite actualizar ``documents.document_id`` (misma clave lógica folio, nuevo id Bsale)
-- sin borrar hijos: las FK repuntan con ON UPDATE CASCADE.

DO $c$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_document_details_document'
    ) THEN
        ALTER TABLE distribuidora.document_details
            DROP CONSTRAINT fk_document_details_document;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_document_details_document'
    ) THEN
        ALTER TABLE distribuidora.document_details
            ADD CONSTRAINT fk_document_details_document
            FOREIGN KEY (document_id)
            REFERENCES distribuidora.documents (document_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE;
    END IF;
END
$c$;
-- +go

DO $c$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_document_attributes_document'
    ) THEN
        ALTER TABLE distribuidora.document_attributes
            DROP CONSTRAINT fk_document_attributes_document;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_document_attributes_document'
    ) THEN
        ALTER TABLE distribuidora.document_attributes
            ADD CONSTRAINT fk_document_attributes_document
            FOREIGN KEY (document_id)
            REFERENCES distribuidora.documents (document_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE;
    END IF;
END
$c$;
-- +go

DO $c$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_document_references_source'
    ) THEN
        ALTER TABLE distribuidora.document_references
            DROP CONSTRAINT fk_document_references_source;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_document_references_source'
    ) THEN
        ALTER TABLE distribuidora.document_references
            ADD CONSTRAINT fk_document_references_source
            FOREIGN KEY (source_document_id)
            REFERENCES distribuidora.documents (document_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE;
    END IF;
END
$c$;
-- +go

DO $c$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT c.conname
        FROM pg_constraint c
        JOIN pg_class t ON c.conrelid = t.oid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'distribuidora'
          AND t.relname = 'route_planning'
          AND c.contype = 'f'
          AND c.confrelid = 'distribuidora.documents'::regclass
    LOOP
        EXECUTE format('ALTER TABLE distribuidora.route_planning DROP CONSTRAINT %I', r.conname);
    END LOOP;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'route_planning_document_id_fkey'
    ) THEN
        ALTER TABLE distribuidora.route_planning
            ADD CONSTRAINT route_planning_document_id_fkey
            FOREIGN KEY (document_id)
            REFERENCES distribuidora.documents (document_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE;
    END IF;
END
$c$;
-- +go
