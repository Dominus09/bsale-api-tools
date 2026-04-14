-- Índices Distribuidora (idempotentes). Cada sentencia termina en ";".
-- +go es solo separador para el job Python.

CREATE INDEX IF NOT EXISTS idx_distribuidora_documents_type
    ON distribuidora.documents (document_type_id);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_documents_emission
    ON distribuidora.documents (emission_date);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_documents_client
    ON distribuidora.documents (client_id);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_documents_office
    ON distribuidora.documents (office_id);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_documents_number
    ON distribuidora.documents (number);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_documents_type_emission
    ON distribuidora.documents (document_type_id, emission_date);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_documents_type_number
    ON distribuidora.documents (document_type_id, number);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_details_document
    ON distribuidora.document_details (document_id);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_details_variant
    ON distribuidora.document_details (variant_id);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_attributes_document
    ON distribuidora.document_attributes (document_id);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_attributes_name
    ON distribuidora.document_attributes (attribute_name);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_references_source
    ON distribuidora.document_references (source_document_id);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_references_number
    ON distribuidora.document_references (reference_number);
-- +go

-- Apoya la vista de facturación (number + tipo referenciado OC o NULL).
CREATE INDEX IF NOT EXISTS idx_distribuidora_references_number_ref_doc_type
    ON distribuidora.document_references (reference_number, reference_document_type_id)
    WHERE reference_number IS NOT NULL;
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_references_ref_type
    ON distribuidora.document_references (reference_document_type_id);
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_sync_logs_process_started
    ON distribuidora.sync_logs (process_name, started_at DESC);
-- +go
