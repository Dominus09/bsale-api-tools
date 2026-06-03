-- Soporte ORDER BY number en planning-rows (fase 1) tras filtro por emission_date.

CREATE INDEX IF NOT EXISTS idx_distribuidora_documents_planning_rows_sort
    ON distribuidora.documents (
        company_id,
        office_id,
        document_type_id,
        emission_date,
        number DESC NULLS LAST,
        document_id DESC
    )
    WHERE company_id = 3 AND office_id = 1 AND document_type_id = 33;
-- +go
