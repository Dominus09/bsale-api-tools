-- =============================================================================
-- bsale_raw 006 — documentos + hijos (details, references, sellers)
--
-- Prohibido: full scan global (C3 = 3.880.542 docs) y generationdaterange (HTTP 403).
--
-- OPEN DOCUMENT WATCH (OC 33, company 3): el webhook documentado sólo garantiza creación.
-- Los documentos no terminales se seleccionan desde esta tabla y se refrescan POR ID:
--   WHERE company_id = 3 AND document_type_id = 33
--     AND emission_date >= <lookback>
--     AND <no terminal según registry: state / commercial_state>
--   ORDER BY api_fetched_at
-- La interpretación de estados terminales vive en Python (resources/documents.py); este SQL
-- no asigna significado a state ni commercial_state. Índice: ix_raw_documents_watch.
--
-- Una OC 33 es MUTABLE (líneas, cantidades, descuentos, montos, vendedor, cliente, atributos,
-- estado, facturación, anulación, nuevas references). Nunca append-only.
--
-- Refresh completo = header + details paginados + references + sellers + attributes en UNA
-- transacción, todos de la misma versión observada (version_hash):
--   BEGIN
--     leer variant_id actuales de document_details (previous_variants)
--     UPSERT documents   WHERE target.api_fetched_at <= EXCLUDED.api_fetched_at
--                        (si no se aplica: ROLLBACK, había una versión más nueva)
--     REPLACE document_details / document_references / document_sellers
--                        (UPSERT de los recibidos + eliminar los del mismo (company_id, document_id)
--                         que ya no vinieron; requiere details paginado completo)
--     INSERT document_change_log si cambió version_hash (affected_variants = previous ∪ current)
--   COMMIT  → recién entonces stock puntual de TODAS las affected_variants.
-- Cualquier fallo → ROLLBACK completo.
--
-- Invariante: todo hijo de un documento tiene document_version_hash = documents.version_hash.
-- Hijos con otro hash = mezcla de versiones (incidente) y se detectan por consulta.
-- Hijos identificados siempre por (company_id, document_id) + su id Bsale: la PK de details y
-- references es (company_id, document_id, bsale_id). Un id de hijo nunca puede "mudarse"
-- silenciosamente a otro documento por un UPSERT; el replace queda acotado al documento.
-- La OC facturada o anulada NO se elimina: queda su versión final con references.
--
-- Sin FK hijo → documento: un hijo puede llegar antes; la integridad se mide.
-- documents.payload puede contener token del documento, urlPdf, urlXml, urlPublicView y datos
-- del cliente: acceso restringido, no exponer al frontend ni imprimir en logs.
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS bsale_raw.documents (
    company_id BIGINT NOT NULL,
    bsale_id BIGINT NOT NULL,
    state SMALLINT,
    commercial_state TEXT,
    document_type_id BIGINT,
    office_id BIGINT,
    client_id BIGINT,
    user_id BIGINT,
    number BIGINT,
    emission_date DATE,
    generation_date TIMESTAMPTZ,
    total_amount NUMERIC,
    informed_sii SMALLINT,
    details_count INTEGER,
    details_complete BOOLEAN NOT NULL DEFAULT false,
    children_fetched_at TIMESTAMPTZ,
    attributes_payload JSONB,
    children_hash TEXT,
    version_hash TEXT,
    version_changed_at TIMESTAMPTZ,
    watch_terminal_seen_at TIMESTAMPTZ,
    watch_stable_reads INTEGER NOT NULL DEFAULT 0,
    watch_closed_at TIMESTAMPTZ,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    api_fetched_at TIMESTAMPTZ NOT NULL,
    missing_since TIMESTAMPTZ,
    last_source TEXT NOT NULL,
    sync_run_id BIGINT,
    CONSTRAINT pk_raw_documents PRIMARY KEY (company_id, bsale_id),
    CONSTRAINT fk_raw_documents_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_raw_documents_last_source
        CHECK (last_source IN ('WEBHOOK', 'POINT', 'INCREMENTAL', 'FULL_RECONCILE', 'SCANNER', 'WINDOW_RECONCILE'))
);
-- Open document watch: candidatos por empresa + tipo con watch abierto (incluye gracia
-- post-cierre), los menos recientemente confirmados primero. Sin semántica de estados.
CREATE INDEX IF NOT EXISTS ix_raw_documents_watch
    ON bsale_raw.documents (company_id, document_type_id, api_fetched_at)
    WHERE watch_closed_at IS NULL;
-- Incremental / reconcile por ventana y lookback del watch.
CREATE INDEX IF NOT EXISTS ix_raw_documents_type_emission
    ON bsale_raw.documents (company_id, document_type_id, emission_date);
CREATE INDEX IF NOT EXISTS ix_raw_documents_type_state
    ON bsale_raw.documents (company_id, document_type_id, state, commercial_state);
CREATE INDEX IF NOT EXISTS ix_raw_documents_office_emission
    ON bsale_raw.documents (company_id, office_id, emission_date);
CREATE INDEX IF NOT EXISTS ix_raw_documents_generation
    ON bsale_raw.documents (company_id, document_type_id, generation_date);
CREATE INDEX IF NOT EXISTS ix_raw_documents_client
    ON bsale_raw.documents (company_id, client_id);
CREATE INDEX IF NOT EXISTS ix_raw_documents_number
    ON bsale_raw.documents (company_id, document_type_id, number);

CREATE TABLE IF NOT EXISTS bsale_raw.document_details (
    company_id BIGINT NOT NULL,
    bsale_id BIGINT NOT NULL,
    document_id BIGINT NOT NULL,
    document_version_hash TEXT NOT NULL,
    variant_id BIGINT,
    line_number INTEGER,
    quantity NUMERIC,
    related_detail_id BIGINT,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    api_fetched_at TIMESTAMPTZ NOT NULL,
    last_source TEXT NOT NULL,
    sync_run_id BIGINT,
    CONSTRAINT pk_raw_document_details PRIMARY KEY (company_id, document_id, bsale_id),
    CONSTRAINT fk_raw_document_details_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_raw_document_details_last_source
        CHECK (last_source IN ('WEBHOOK', 'POINT', 'INCREMENTAL', 'FULL_RECONCILE', 'SCANNER', 'WINDOW_RECONCILE'))
);
-- La PK (company_id, document_id, bsale_id) cubre el replace por documento.
CREATE INDEX IF NOT EXISTS ix_raw_document_details_bsale_id
    ON bsale_raw.document_details (company_id, bsale_id);
CREATE INDEX IF NOT EXISTS ix_raw_document_details_variant
    ON bsale_raw.document_details (company_id, variant_id);

CREATE TABLE IF NOT EXISTS bsale_raw.document_references (
    company_id BIGINT NOT NULL,
    bsale_id BIGINT NOT NULL,
    document_id BIGINT NOT NULL,
    document_version_hash TEXT NOT NULL,
    number TEXT,
    dte_code_id BIGINT,
    reference_date DATE,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    api_fetched_at TIMESTAMPTZ NOT NULL,
    last_source TEXT NOT NULL,
    sync_run_id BIGINT,
    CONSTRAINT pk_raw_document_references PRIMARY KEY (company_id, document_id, bsale_id),
    CONSTRAINT fk_raw_document_references_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_raw_document_references_last_source
        CHECK (last_source IN ('WEBHOOK', 'POINT', 'INCREMENTAL', 'FULL_RECONCILE', 'SCANNER', 'WINDOW_RECONCILE'))
);
CREATE INDEX IF NOT EXISTS ix_raw_document_references_bsale_id
    ON bsale_raw.document_references (company_id, bsale_id);

CREATE TABLE IF NOT EXISTS bsale_raw.document_sellers (
    company_id BIGINT NOT NULL,
    document_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    document_version_hash TEXT NOT NULL,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    api_fetched_at TIMESTAMPTZ NOT NULL,
    last_source TEXT NOT NULL,
    sync_run_id BIGINT,
    CONSTRAINT pk_raw_document_sellers PRIMARY KEY (company_id, document_id, user_id),
    CONSTRAINT fk_raw_document_sellers_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_raw_document_sellers_last_source
        CHECK (last_source IN ('WEBHOOK', 'POINT', 'INCREMENTAL', 'FULL_RECONCILE', 'SCANNER', 'WINDOW_RECONCILE'))
);
CREATE INDEX IF NOT EXISTS ix_raw_document_sellers_user
    ON bsale_raw.document_sellers (company_id, user_id);

-- Auditoría de cambios de versión (no guarda payload; el estado ACTUAL vive en documents).
-- Una fila por refresh en que cambió version_hash (o por la primera observación). Permite
-- reconstruir: creada → modificaciones → versión vigente → facturada / anulada, y saber
-- cómo (detected_by, sync_run_id, webhook_event_id) y cuándo se detectó cada cambio.
CREATE TABLE IF NOT EXISTS bsale_raw.document_change_log (
    id BIGSERIAL NOT NULL,
    company_id BIGINT NOT NULL,
    document_id BIGINT NOT NULL,
    document_type_id BIGINT,
    change_kind TEXT NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    api_fetched_at TIMESTAMPTZ NOT NULL,
    detected_by TEXT NOT NULL,
    sync_run_id BIGINT,
    webhook_event_id BIGINT,
    previous_version_hash TEXT,
    version_hash TEXT NOT NULL,
    previous_payload_hash TEXT,
    payload_hash TEXT NOT NULL,
    previous_state SMALLINT,
    state SMALLINT,
    previous_commercial_state TEXT,
    commercial_state TEXT,
    header_changed BOOLEAN NOT NULL,
    details_changed BOOLEAN NOT NULL,
    references_changed BOOLEAN NOT NULL,
    sellers_changed BOOLEAN NOT NULL,
    attributes_changed BOOLEAN NOT NULL,
    previous_variant_ids BIGINT[] NOT NULL DEFAULT '{}',
    current_variant_ids BIGINT[] NOT NULL DEFAULT '{}',
    affected_variant_ids BIGINT[] NOT NULL DEFAULT '{}',
    stock_refresh_requested_at TIMESTAMPTZ,
    stock_refresh_done_at TIMESTAMPTZ,
    CONSTRAINT pk_raw_document_change_log PRIMARY KEY (id),
    CONSTRAINT fk_raw_document_change_log_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_raw_document_change_log_change_kind
        CHECK (change_kind IN ('CREATED', 'MODIFIED')),
    CONSTRAINT ck_raw_document_change_log_detected_by
        CHECK (detected_by IN ('WEBHOOK', 'POINT', 'INCREMENTAL', 'FULL_RECONCILE', 'SCANNER', 'WINDOW_RECONCILE'))
);
CREATE INDEX IF NOT EXISTS ix_raw_document_change_log_document
    ON bsale_raw.document_change_log (company_id, document_id, detected_at);
CREATE INDEX IF NOT EXISTS ix_raw_document_change_log_pending_stock
    ON bsale_raw.document_change_log (detected_at) WHERE stock_refresh_done_at IS NULL;

COMMENT ON COLUMN bsale_raw.documents.payload IS
    'Respuesta Bsale completa: puede incluir token del documento, URLs PDF/XML/public view y datos del cliente. Acceso restringido.';
COMMENT ON COLUMN bsale_raw.documents.commercial_state IS
    'Valor Bsale tal cual si existe en el JSON; su significado (terminal o no) se define en registry Python.';
COMMENT ON COLUMN bsale_raw.documents.details_complete IS
    'true sólo si /documents/{id}/details.json se paginó completo; condición para reemplazar hijos.';
COMMENT ON COLUMN bsale_raw.documents.version_hash IS
    'Hash de la versión observada completa (header + details + references + sellers + attributes). Todo hijo debe tener el mismo document_version_hash.';
COMMENT ON COLUMN bsale_raw.documents.last_changed_at IS
    'Sólo cambia cuando cambia payload_hash/version_hash. Una lectura sin cambios actualiza last_seen_at y api_fetched_at.';
COMMENT ON COLUMN bsale_raw.documents.watch_terminal_seen_at IS
    'Primera vez que Python/registry consideró terminal la versión (gracia post-cierre). SQL no interpreta estados.';
COMMENT ON COLUMN bsale_raw.documents.watch_closed_at IS
    'Cierre del open document watch tras la gracia y lecturas estables. NULL = sigue en watch.';
COMMENT ON TABLE bsale_raw.document_change_log IS
    'Auditoría de cambios de versión por documento y affected_variants (previous ∪ current) para el refresh de stock post-COMMIT.';

COMMIT;
