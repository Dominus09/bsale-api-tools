-- Esquema base Distribuidora / Bsale (ingesta).
-- Cada bloque termina en ";" para poder pegar todo el archivo en pgAdmin.
-- El job Python sigue usando el separador "-- +go" (línea propia) para partir sentencias.

CREATE SCHEMA IF NOT EXISTS distribuidora;
-- +go

-- Migra sync_state legado (id + last_sync) → modelo por process_name.
DO $mig_sync$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'distribuidora' AND table_name = 'sync_state'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'distribuidora' AND table_name = 'sync_state'
          AND column_name = 'process_name'
    ) THEN
        CREATE TABLE distribuidora.sync_state_new (
            id SERIAL PRIMARY KEY,
            process_name TEXT NOT NULL,
            last_sync TIMESTAMPTZ,
            last_status TEXT,
            last_message TEXT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT uq_distribuidora_sync_state_process UNIQUE (process_name)
        );
        INSERT INTO distribuidora.sync_state_new (process_name, last_sync)
        VALUES (
            'documents_incremental',
            (SELECT last_sync FROM distribuidora.sync_state ORDER BY id ASC LIMIT 1)
        );
        DROP TABLE distribuidora.sync_state;
        ALTER TABLE distribuidora.sync_state_new RENAME TO sync_state;
    END IF;
END
$mig_sync$;
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.sync_state (
    id SERIAL PRIMARY KEY,
    process_name TEXT NOT NULL,
    last_sync TIMESTAMPTZ,
    last_status TEXT,
    last_message TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_distribuidora_sync_state_process UNIQUE (process_name)
);
-- +go

INSERT INTO distribuidora.sync_state (process_name, last_sync, last_status)
VALUES ('documents_incremental', TIMESTAMPTZ '2000-01-01 00:00:00+00', NULL)
ON CONFLICT (process_name) DO NOTHING;
-- +go

INSERT INTO distribuidora.sync_state (process_name, last_sync, last_status)
VALUES ('documents_resync', NULL, NULL)
ON CONFLICT (process_name) DO NOTHING;
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.sync_logs (
    id BIGSERIAL PRIMARY KEY,
    process_name TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'running',
    documents_processed INT NOT NULL DEFAULT 0,
    documents_inserted INT NOT NULL DEFAULT 0,
    documents_updated INT NOT NULL DEFAULT 0,
    details_inserted INT NOT NULL DEFAULT 0,
    attributes_inserted INT NOT NULL DEFAULT 0,
    references_inserted INT NOT NULL DEFAULT 0,
    message TEXT
);
-- +go

-- Si ``sync_logs`` ya existía de un esquema viejo, CREATE IF NOT EXISTS no la toca:
-- añadir columnas faltantes (valores por defecto solo para filas ya existentes).
ALTER TABLE distribuidora.sync_logs ADD COLUMN IF NOT EXISTS process_name TEXT NOT NULL DEFAULT 'legacy';
-- +go
ALTER TABLE distribuidora.sync_logs ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
-- +go
ALTER TABLE distribuidora.sync_logs ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ;
-- +go
ALTER TABLE distribuidora.sync_logs ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'unknown';
-- +go
ALTER TABLE distribuidora.sync_logs ADD COLUMN IF NOT EXISTS documents_processed INT NOT NULL DEFAULT 0;
-- +go
ALTER TABLE distribuidora.sync_logs ADD COLUMN IF NOT EXISTS documents_inserted INT NOT NULL DEFAULT 0;
-- +go
ALTER TABLE distribuidora.sync_logs ADD COLUMN IF NOT EXISTS documents_updated INT NOT NULL DEFAULT 0;
-- +go
ALTER TABLE distribuidora.sync_logs ADD COLUMN IF NOT EXISTS details_inserted INT NOT NULL DEFAULT 0;
-- +go
ALTER TABLE distribuidora.sync_logs ADD COLUMN IF NOT EXISTS attributes_inserted INT NOT NULL DEFAULT 0;
-- +go
ALTER TABLE distribuidora.sync_logs ADD COLUMN IF NOT EXISTS references_inserted INT NOT NULL DEFAULT 0;
-- +go
ALTER TABLE distribuidora.sync_logs ADD COLUMN IF NOT EXISTS message TEXT;
-- +go
ALTER TABLE distribuidora.sync_logs ALTER COLUMN process_name DROP DEFAULT;
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.documents (
    document_id BIGINT PRIMARY KEY,
    number BIGINT,
    document_type_id INT,
    client_id BIGINT,
    office_id INT NOT NULL DEFAULT 1,
    company_id INT NOT NULL DEFAULT 3,
    user_id BIGINT,
    emission_date TIMESTAMPTZ,
    expiration_date TIMESTAMPTZ,
    generation_date TIMESTAMPTZ,
    total_amount NUMERIC(18, 4),
    net_amount NUMERIC(18, 4),
    tax_amount NUMERIC(18, 4),
    state INT,
    commercial_state INT,
    informed_sii INT,
    municipality TEXT,
    city TEXT,
    address TEXT,
    token TEXT,
    url_pdf TEXT,
    url_public_view TEXT,
    price_list_id BIGINT,
    tracking_number TEXT,
    seller_id INT,
    seller_name TEXT,
    raw_data JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +go

ALTER TABLE distribuidora.documents
    ADD COLUMN IF NOT EXISTS seller_id INT,
    ADD COLUMN IF NOT EXISTS seller_name TEXT;
-- +go

DO $mig_doc$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'distribuidora' AND table_name = 'documents'
          AND column_name = 'vendedor_id'
    ) THEN
        ALTER TABLE distribuidora.documents ADD COLUMN IF NOT EXISTS user_id BIGINT;
        UPDATE distribuidora.documents SET user_id = vendedor_id WHERE user_id IS NULL;
    END IF;
END
$mig_doc$;
-- +go

ALTER TABLE distribuidora.documents ADD COLUMN IF NOT EXISTS number BIGINT;
-- +go
ALTER TABLE distribuidora.documents ADD COLUMN IF NOT EXISTS document_type_id INT;
-- +go
ALTER TABLE distribuidora.documents ADD COLUMN IF NOT EXISTS client_id BIGINT;
-- +go
ALTER TABLE distribuidora.documents ADD COLUMN IF NOT EXISTS office_id INT NOT NULL DEFAULT 1;
-- +go
ALTER TABLE distribuidora.documents ADD COLUMN IF NOT EXISTS company_id INT NOT NULL DEFAULT 3;
-- +go
ALTER TABLE distribuidora.documents ADD COLUMN IF NOT EXISTS user_id BIGINT;
-- +go
ALTER TABLE distribuidora.documents ADD COLUMN IF NOT EXISTS emission_date TIMESTAMPTZ;
-- +go
ALTER TABLE distribuidora.documents ADD COLUMN IF NOT EXISTS expiration_date TIMESTAMPTZ;
-- +go
ALTER TABLE distribuidora.documents ADD COLUMN IF NOT EXISTS generation_date TIMESTAMPTZ;
-- +go
ALTER TABLE distribuidora.documents ADD COLUMN IF NOT EXISTS total_amount NUMERIC(18, 4);
-- +go
ALTER TABLE distribuidora.documents ADD COLUMN IF NOT EXISTS net_amount NUMERIC(18, 4);
-- +go
ALTER TABLE distribuidora.documents ADD COLUMN IF NOT EXISTS tax_amount NUMERIC(18, 4);
-- +go
ALTER TABLE distribuidora.documents ADD COLUMN IF NOT EXISTS state INT;
-- +go
ALTER TABLE distribuidora.documents ADD COLUMN IF NOT EXISTS commercial_state INT;
-- +go
ALTER TABLE distribuidora.documents ADD COLUMN IF NOT EXISTS informed_sii INT;
-- +go
ALTER TABLE distribuidora.documents ADD COLUMN IF NOT EXISTS municipality TEXT;
-- +go
ALTER TABLE distribuidora.documents ADD COLUMN IF NOT EXISTS city TEXT;
-- +go
ALTER TABLE distribuidora.documents ADD COLUMN IF NOT EXISTS address TEXT;
-- +go
ALTER TABLE distribuidora.documents ADD COLUMN IF NOT EXISTS token TEXT;
-- +go
ALTER TABLE distribuidora.documents ADD COLUMN IF NOT EXISTS url_pdf TEXT;
-- +go
ALTER TABLE distribuidora.documents ADD COLUMN IF NOT EXISTS url_public_view TEXT;
-- +go
ALTER TABLE distribuidora.documents ADD COLUMN IF NOT EXISTS price_list_id BIGINT;
-- +go
ALTER TABLE distribuidora.documents ADD COLUMN IF NOT EXISTS tracking_number TEXT;
-- +go
ALTER TABLE distribuidora.documents ADD COLUMN IF NOT EXISTS raw_data JSONB;
-- +go
ALTER TABLE distribuidora.documents ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
-- +go
ALTER TABLE distribuidora.documents ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
-- +go

UPDATE distribuidora.documents SET raw_data = '{}'::jsonb WHERE raw_data IS NULL;
-- +go

ALTER TABLE distribuidora.documents ALTER COLUMN raw_data SET DEFAULT '{}'::jsonb;
-- +go

ALTER TABLE distribuidora.documents ALTER COLUMN raw_data SET NOT NULL;
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.document_details (
    detail_id BIGINT PRIMARY KEY,
    document_id BIGINT NOT NULL,
    line_number INT,
    variant_id BIGINT,
    variant_description TEXT,
    variant_code TEXT,
    quantity NUMERIC(18, 4),
    net_unit_value NUMERIC(18, 4),
    total_unit_value NUMERIC(18, 4),
    net_amount NUMERIC(18, 4),
    tax_amount NUMERIC(18, 4),
    total_amount NUMERIC(18, 4),
    net_discount NUMERIC(18, 4),
    total_discount NUMERIC(18, 4),
    discount_percentage NUMERIC(10, 4),
    related_detail_id BIGINT,
    note TEXT,
    raw_data JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +go

ALTER TABLE distribuidora.document_details ADD COLUMN IF NOT EXISTS variant_description TEXT;
-- +go
ALTER TABLE distribuidora.document_details ADD COLUMN IF NOT EXISTS variant_code TEXT;
-- +go
ALTER TABLE distribuidora.document_details ADD COLUMN IF NOT EXISTS total_discount NUMERIC(18, 4);
-- +go
ALTER TABLE distribuidora.document_details ADD COLUMN IF NOT EXISTS related_detail_id BIGINT;
-- +go
ALTER TABLE distribuidora.document_details ADD COLUMN IF NOT EXISTS note TEXT;
-- +go
ALTER TABLE distribuidora.document_details ADD COLUMN IF NOT EXISTS raw_data JSONB;
-- +go
ALTER TABLE distribuidora.document_details ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
-- +go
ALTER TABLE distribuidora.document_details ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
-- +go

UPDATE distribuidora.document_details SET raw_data = '{}'::jsonb WHERE raw_data IS NULL;
-- +go

ALTER TABLE distribuidora.document_details ALTER COLUMN raw_data SET NOT NULL;
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.document_attributes (
    id BIGSERIAL PRIMARY KEY,
    document_id BIGINT NOT NULL,
    attribute_id BIGINT,
    attribute_name TEXT NOT NULL,
    attribute_value TEXT,
    raw_data JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.document_references (
    id BIGSERIAL PRIMARY KEY,
    source_document_id BIGINT NOT NULL,
    reference_number BIGINT,
    reference_document_type_id INT,
    reference_date TIMESTAMPTZ,
    reference_reason TEXT,
    raw_data JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +go

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_document_details_document'
    ) THEN
        ALTER TABLE distribuidora.document_details
            ADD CONSTRAINT fk_document_details_document
            FOREIGN KEY (document_id)
            REFERENCES distribuidora.documents (document_id)
            ON DELETE CASCADE;
    END IF;
END $$;
-- +go

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_document_attributes_document'
    ) THEN
        ALTER TABLE distribuidora.document_attributes
            ADD CONSTRAINT fk_document_attributes_document
            FOREIGN KEY (document_id)
            REFERENCES distribuidora.documents (document_id)
            ON DELETE CASCADE;
    END IF;
END $$;
-- +go

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_document_references_source'
    ) THEN
        ALTER TABLE distribuidora.document_references
            ADD CONSTRAINT fk_document_references_source
            FOREIGN KEY (source_document_id)
            REFERENCES distribuidora.documents (document_id)
            ON DELETE CASCADE;
    END IF;
END $$;
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.route_planning (
    id BIGSERIAL PRIMARY KEY,
    planning_date DATE NOT NULL,
    document_id BIGINT NOT NULL REFERENCES distribuidora.documents (document_id) ON DELETE CASCADE,
    oc_number BIGINT,
    client_id BIGINT,
    client_name TEXT,
    municipality TEXT,
    address TEXT,
    lat NUMERIC(12, 8),
    lon NUMERIC(12, 8),
    total_amount NUMERIC(18, 4),
    truck TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'planned',
    route_name TEXT,
    driver TEXT,
    assistant_1 TEXT,
    assistant_2 TEXT,
    departure_time TEXT,
    general_observation TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_distribuidora_route_planning_date_document UNIQUE (planning_date, document_id)
);
-- +go

ALTER TABLE distribuidora.route_planning
    ADD COLUMN IF NOT EXISTS route_name TEXT,
    ADD COLUMN IF NOT EXISTS driver TEXT,
    ADD COLUMN IF NOT EXISTS assistant_1 TEXT,
    ADD COLUMN IF NOT EXISTS assistant_2 TEXT,
    ADD COLUMN IF NOT EXISTS departure_time TEXT,
    ADD COLUMN IF NOT EXISTS general_observation TEXT;
-- +go
