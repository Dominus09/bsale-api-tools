-- =============================================================================
-- bsale_raw 012 — recepciones: completitud, hashes, ausencias de líneas y bitácora (NO destructiva)
--
-- Fuente: GET /v1/stocks/receptions.json?expand=[details] (company 3, LIVE VALIDATED C3.1A:
-- listado ASC por id, admissiondate filtra por día, admissiondaterange se IGNORA, expand completo
-- en las muestras). Si el nodo details viene truncado o inconsistente:
-- /v1/stocks/receptions/{id}/details.json paginado. Motor: core/reception_engine.py.
--
-- Alcance operativo: recepciones con admission_date desde 2025-01-01 (modo por fechas). No hay barrido
-- global del histórico; filas anteriores que pudieran existir no se tocan ni se marcan ausentes.
--
-- payload = la recepción COMPLETA tal como la entrega Bsale (incluido el nodo "details" tal como
-- vino); payload_hash = hash de ese payload. Cada línea guarda además su payload original en
-- stock_reception_details. payload contiene note y user: acceso restringido, no exponer ni imprimir.
-- content_hash = hash de la parte comparable: cabecera sin "details"; línea sin "variantStock" (stock
-- de la variante, cambia sin que cambie la recepción). Los cambios (last_changed_at y bitácora) se
-- detectan SÓLO por content_hash / details_hash / version_hash, nunca por payload_hash.
-- Una recepción sólo queda details_complete = true si TODAS sus líneas llegaron y se validaron
-- (count == líneas recibidas, ids únicos, variant.id, quantity y cost numéricos). Si el detalle falla,
-- una recepción nueva se guarda con details_complete = false y sin líneas; una ya guardada no se toca.
-- RAW guarda cost y quantity tal cual: aquí no se calcula IVA, ILA, costo bruto, promedio ni total.
-- Nunca se borra: recepción ausente en un día reconciliado completo y línea ausente en un detalle
-- completo -> missing_since; vuelve a NULL si Bsale la vuelve a entregar.
--
-- Columnas nullable sin DEFAULT: cambio sólo de catálogo (sin reescritura de las tablas).
-- Explícita (sin IF NOT EXISTS): aplicarla dos veces falla en vez de ocultar drift.
-- Después de aplicarla, correr verify_bsale_raw.sql.
-- =============================================================================

BEGIN;

ALTER TABLE bsale_raw.stock_receptions ADD COLUMN document TEXT;
ALTER TABLE bsale_raw.stock_receptions ADD COLUMN user_id BIGINT;
ALTER TABLE bsale_raw.stock_receptions ADD COLUMN internal_dispatch_id BIGINT;
ALTER TABLE bsale_raw.stock_receptions ADD COLUMN admission_date_unix BIGINT;
ALTER TABLE bsale_raw.stock_receptions ADD COLUMN details_count INTEGER;
ALTER TABLE bsale_raw.stock_receptions ADD COLUMN details_complete BOOLEAN;
ALTER TABLE bsale_raw.stock_receptions ADD COLUMN details_hash TEXT;
ALTER TABLE bsale_raw.stock_receptions ADD COLUMN version_hash TEXT;
ALTER TABLE bsale_raw.stock_receptions ADD COLUMN details_source TEXT;
ALTER TABLE bsale_raw.stock_receptions ADD COLUMN details_fetched_at TIMESTAMPTZ;
ALTER TABLE bsale_raw.stock_receptions ADD COLUMN content_hash TEXT;
ALTER TABLE bsale_raw.stock_reception_details ADD COLUMN missing_since TIMESTAMPTZ;
ALTER TABLE bsale_raw.stock_reception_details ADD COLUMN content_hash TEXT;

CREATE INDEX ix_raw_stock_receptions_date
    ON bsale_raw.stock_receptions (company_id, admission_date);
CREATE INDEX ix_raw_stock_receptions_incomplete
    ON bsale_raw.stock_receptions (company_id, bsale_id) WHERE details_complete IS NOT TRUE;

-- Bitácora de cambios (sin payload: note y user no se copian). Una fila por recepción creada,
-- modificada (cabecera y/o líneas), que reaparece o que pasa a ausente en un día reconciliado.
CREATE TABLE bsale_raw.stock_reception_change_log (
    id BIGSERIAL NOT NULL,
    company_id BIGINT NOT NULL,
    reception_id BIGINT NOT NULL,
    event_kind TEXT NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    api_fetched_at TIMESTAMPTZ,
    detected_by TEXT NOT NULL,
    sync_run_id BIGINT,
    admission_date DATE,
    previous_content_hash TEXT,
    content_hash TEXT,
    previous_details_hash TEXT,
    details_hash TEXT,
    previous_version_hash TEXT,
    version_hash TEXT,
    header_changed BOOLEAN NOT NULL,
    details_changed BOOLEAN NOT NULL,
    added_detail_ids BIGINT[] NOT NULL DEFAULT '{}',
    removed_detail_ids BIGINT[] NOT NULL DEFAULT '{}',
    quantity_changed_detail_ids BIGINT[] NOT NULL DEFAULT '{}',
    cost_changed_detail_ids BIGINT[] NOT NULL DEFAULT '{}',
    variant_changed_detail_ids BIGINT[] NOT NULL DEFAULT '{}',
    CONSTRAINT pk_raw_stock_reception_change_log PRIMARY KEY (id),
    CONSTRAINT fk_raw_stock_reception_change_log_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_raw_stock_reception_change_log_event_kind
        CHECK (event_kind IN ('CREATED', 'MODIFIED', 'REAPPEARED', 'MISSING')),
    CONSTRAINT ck_raw_stock_reception_change_log_detected_by
        CHECK (detected_by IN ('WEBHOOK', 'POINT', 'INCREMENTAL', 'FULL_RECONCILE', 'SCANNER', 'WINDOW_RECONCILE'))
);
CREATE INDEX ix_raw_stock_reception_change_log_reception
    ON bsale_raw.stock_reception_change_log (company_id, reception_id, detected_at);

COMMENT ON COLUMN bsale_raw.stock_receptions.payload IS
    'Recepción completa tal como la entrega Bsale (incluido el nodo details). Incluye note y user: acceso restringido.';
COMMENT ON COLUMN bsale_raw.stock_receptions.content_hash IS
    'Hash de la cabecera comparable (payload sin details). Base de la detección de cambios de cabecera.';
COMMENT ON COLUMN bsale_raw.stock_reception_details.content_hash IS
    'Hash de la línea comparable (payload sin variantStock). Base de last_changed_at y de details_hash.';
COMMENT ON COLUMN bsale_raw.stock_receptions.document IS
    'Etiqueta document original de Bsale (texto libre, sin clasificar).';
COMMENT ON COLUMN bsale_raw.stock_receptions.user_id IS
    'user.id de Bsale si viene en la recepción.';
COMMENT ON COLUMN bsale_raw.stock_receptions.internal_dispatch_id IS
    'internalDispatchId original de Bsale (0 = sin despacho interno, tal cual).';
COMMENT ON COLUMN bsale_raw.stock_receptions.admission_date_unix IS
    'admissionDate original (entero unix). admission_date es su fecha UTC.';
COMMENT ON COLUMN bsale_raw.stock_receptions.details_count IS
    'count del detalle informado por Bsale en la última lectura completa.';
COMMENT ON COLUMN bsale_raw.stock_receptions.details_complete IS
    'true sólo si se recibieron y validaron todas las líneas; false = recepción conocida con detalle pendiente.';
COMMENT ON COLUMN bsale_raw.stock_receptions.details_hash IS
    'Hash del conjunto de líneas vigente (id + content_hash de cada línea).';
COMMENT ON COLUMN bsale_raw.stock_receptions.version_hash IS
    'Hash de la versión comparable (content_hash + details_hash).';
COMMENT ON COLUMN bsale_raw.stock_receptions.details_source IS
    'EXPAND (nodo del listado) o DETAILS_ENDPOINT (details.json paginado).';
COMMENT ON COLUMN bsale_raw.stock_reception_details.missing_since IS
    'Línea que un detalle completo de su recepción dejó de entregar; NULL = vigente. Nunca se borra.';
COMMENT ON TABLE bsale_raw.stock_reception_change_log IS
    'Auditoría de recepciones: creación, cambios de cabecera/cantidad/costo/variante, líneas nuevas o ausentes, reaparición y ausencia.';

COMMIT;
