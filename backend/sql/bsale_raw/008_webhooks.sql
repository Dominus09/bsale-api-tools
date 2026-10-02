-- =============================================================================
-- bsale_raw 008 — inbox de webhooks + evidencia del GET exacto
--
-- Bsale no documenta un event_id: no se asume.
--
-- Cada POST recibido se conserva como fila (evidencia RAW), incluso reenvíos idénticos.
--
-- dedupe_key  = company|topic|action|resourceId|officeId|priceListId|send
--               Identifica un REENVÍO exacto del mismo evento. Índice NO único: sólo diagnóstico.
-- refresh_key = company|topic|resourceId|officeId|priceListId
--               Identifica el TRABAJO de refresh (mismo recurso). Base del coalescing.
--
-- Unicidad SÓLO sobre eventos activos (índices parciales):
--   * a lo sumo 1 evento en cola (PENDING/RETRY) por refresh_key;
--   * a lo sumo 1 evento PROCESSING por refresh_key (sin trabajo simultáneo redundante).
-- Un evento que llega con otro ya en cola se inserta como COALESCED (coalesced_into_id = el
-- de la cola). Si llega mientras otro está PROCESSING, entra como PENDING: el refresh en curso
-- pudo leer Bsale antes del cambio. Una vez DONE / FAILED_FINAL / COALESCED, un evento idéntico
-- del mismo recurso vuelve a entrar como PENDING.
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS bsale_raw.webhook_events (
    id BIGSERIAL NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    company_id BIGINT,
    cpn_id BIGINT,
    topic TEXT,
    action TEXT,
    resource TEXT,
    resource_id BIGINT,
    office_id BIGINT,
    price_list_id BIGINT,
    sent_at BIGINT,
    payload JSONB NOT NULL,
    dedupe_key TEXT,
    refresh_key TEXT,
    status TEXT NOT NULL DEFAULT 'PENDING',
    coalesced_into_id BIGINT,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_at TIMESTAMPTZ,
    locked_by TEXT,
    validation_error TEXT,
    last_error TEXT,
    processed_at TIMESTAMPTZ,
    CONSTRAINT pk_raw_webhook_events PRIMARY KEY (id),
    CONSTRAINT fk_raw_webhook_events_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT fk_raw_webhook_events_coalesced
        FOREIGN KEY (coalesced_into_id) REFERENCES bsale_raw.webhook_events (id) ON DELETE SET NULL,
    CONSTRAINT ck_raw_webhook_events_status
        CHECK (status IN ('PENDING', 'PROCESSING', 'DONE', 'RETRY', 'FAILED_FINAL', 'COALESCED'))
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_webhook_events_queued_refresh
    ON bsale_raw.webhook_events (refresh_key) WHERE status IN ('PENDING', 'RETRY');
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_webhook_events_processing_refresh
    ON bsale_raw.webhook_events (refresh_key) WHERE status = 'PROCESSING';
CREATE INDEX IF NOT EXISTS ix_raw_webhook_events_queue
    ON bsale_raw.webhook_events (next_attempt_at) WHERE status IN ('PENDING', 'RETRY');
CREATE INDEX IF NOT EXISTS ix_raw_webhook_events_processing_lock
    ON bsale_raw.webhook_events (locked_at) WHERE status = 'PROCESSING';
CREATE INDEX IF NOT EXISTS ix_raw_webhook_events_dedupe
    ON bsale_raw.webhook_events (dedupe_key);
CREATE INDEX IF NOT EXISTS ix_raw_webhook_events_company_received
    ON bsale_raw.webhook_events (company_id, received_at);

COMMENT ON COLUMN bsale_raw.webhook_events.dedupe_key IS
    'Reenvío exacto (incluye send). NO único: todos los POST se conservan como evidencia.';
COMMENT ON COLUMN bsale_raw.webhook_events.refresh_key IS
    'Recurso a refrescar (sin action/send). Único sólo entre eventos activos: PENDING/RETRY y PROCESSING por separado.';

CREATE TABLE IF NOT EXISTS bsale_raw.webhook_resource_responses (
    id BIGSERIAL NOT NULL,
    webhook_event_id BIGINT NOT NULL,
    company_id BIGINT NOT NULL,
    requested_path TEXT NOT NULL,
    http_status INTEGER,
    envelope TEXT NOT NULL,
    response_code INTEGER,
    body JSONB,
    body_text TEXT,
    body_hash TEXT,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    duration_ms INTEGER,
    CONSTRAINT pk_raw_webhook_resource_responses PRIMARY KEY (id),
    CONSTRAINT fk_raw_webhook_resource_responses_event
        FOREIGN KEY (webhook_event_id) REFERENCES bsale_raw.webhook_events (id) ON DELETE CASCADE,
    CONSTRAINT fk_raw_webhook_resource_responses_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_raw_webhook_resource_responses_envelope
        CHECK (envelope IN ('V2_CODE_DATA', 'OTHER', 'NO_JSON', 'NETWORK_ERROR'))
);
CREATE INDEX IF NOT EXISTS ix_raw_webhook_resource_responses_event
    ON bsale_raw.webhook_resource_responses (webhook_event_id);
CREATE INDEX IF NOT EXISTS ix_raw_webhook_resource_responses_fetched
    ON bsale_raw.webhook_resource_responses (company_id, fetched_at);

COMMENT ON TABLE bsale_raw.webhook_resource_responses IS
    'Respuesta original del GET exacto de `resource` (V2 code+data observado). Evidencia; nunca alimenta tablas operativas. Sin headers de request.';

COMMIT;
