-- Índices para GET dispatch-prep/planning-rows (filtro por fecha + tipo OC).

CREATE INDEX IF NOT EXISTS idx_distribuidora_documents_planning_rows
    ON distribuidora.documents (company_id, office_id, document_type_id, emission_date DESC)
    WHERE company_id = 3 AND office_id = 1 AND document_type_id = 33;
-- +go

CREATE INDEX IF NOT EXISTS idx_distribuidora_document_attributes_obs
    ON distribuidora.document_attributes (document_id)
    WHERE upper(btrim(attribute_name)) = 'OBSERVACIONES';
-- +go
