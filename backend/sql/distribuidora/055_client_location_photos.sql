-- P3: propuestas GPS de chofer + fotos de fachada (R2) + ubicación verificada interna.
-- No escribe en bsale.clients. No aplicar automáticamente desde syncs/HTTP.

CREATE TABLE IF NOT EXISTS distribuidora.client_verified_locations (
    client_id BIGINT NOT NULL PRIMARY KEY,
    company_id INTEGER NOT NULL DEFAULT 3,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    accuracy_m REAL,
    source TEXT NOT NULL DEFAULT 'driver_approved',
    approved_proposal_id BIGINT,
    approved_by_user_id INTEGER,
    approved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_client_verified_lat CHECK (latitude BETWEEN -90 AND 90 AND latitude <> 0),
    CONSTRAINT ck_client_verified_lng CHECK (longitude BETWEEN -180 AND 180 AND longitude <> 0)
);
-- +go

COMMENT ON TABLE distribuidora.client_verified_locations IS
    'Ubicación interna verificada (aprobada). Prioridad sobre rutero/clients en mapa PWA.';
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.client_location_proposals (
    id BIGSERIAL PRIMARY KEY,
    client_id BIGINT NOT NULL,
    company_id INTEGER NOT NULL DEFAULT 3,
    dispatch_plan_id BIGINT NOT NULL
        REFERENCES distribuidora.dispatch_plan (id) ON DELETE CASCADE,
    customer_key TEXT NOT NULL,
    proposed_lat DOUBLE PRECISION NOT NULL,
    proposed_lng DOUBLE PRECISION NOT NULL,
    accuracy_m REAL,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    previous_lat DOUBLE PRECISION,
    previous_lng DOUBLE PRECISION,
    previous_source TEXT,
    distance_from_previous_m REAL,
    proposed_by_user_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'approved', 'rejected', 'superseded')),
    reviewed_by_user_id INTEGER,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    facade_photo_id BIGINT,
    idempotency_key TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_proposal_lat CHECK (proposed_lat BETWEEN -90 AND 90 AND proposed_lat <> 0),
    CONSTRAINT ck_proposal_lng CHECK (proposed_lng BETWEEN -180 AND 180 AND proposed_lng <> 0)
);
-- +go

CREATE UNIQUE INDEX IF NOT EXISTS uq_client_location_proposal_pending
    ON distribuidora.client_location_proposals (client_id)
    WHERE status = 'pending';
-- +go

CREATE UNIQUE INDEX IF NOT EXISTS uq_client_location_proposal_idem
    ON distribuidora.client_location_proposals (proposed_by_user_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
-- +go

CREATE INDEX IF NOT EXISTS idx_client_location_proposals_status
    ON distribuidora.client_location_proposals (status, created_at DESC);
-- +go

CREATE INDEX IF NOT EXISTS idx_client_location_proposals_plan
    ON distribuidora.client_location_proposals (dispatch_plan_id);
-- +go

CREATE INDEX IF NOT EXISTS idx_client_location_proposals_company_status
    ON distribuidora.client_location_proposals (company_id, status, created_at DESC);
-- +go

CREATE INDEX IF NOT EXISTS idx_client_location_proposals_client
    ON distribuidora.client_location_proposals (client_id, created_at DESC);
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.client_facade_photos (
    id BIGSERIAL PRIMARY KEY,
    client_id BIGINT NOT NULL,
    company_id INTEGER NOT NULL DEFAULT 3,
    dispatch_plan_id BIGINT NOT NULL
        REFERENCES distribuidora.dispatch_plan (id) ON DELETE CASCADE,
    customer_key TEXT NOT NULL,
    object_key TEXT NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'image/jpeg',
    size_bytes INTEGER,
    sha256 TEXT,
    width INTEGER,
    height INTEGER,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    captured_by_user_id INTEGER NOT NULL,
    validation_status TEXT NOT NULL DEFAULT 'pending'
        CHECK (validation_status IN ('pending', 'approved', 'rejected', 'replaced', 'upload_pending')),
    reviewed_by_user_id INTEGER,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    is_current BOOLEAN NOT NULL DEFAULT FALSE,
    upload_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
    idempotency_key TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_client_facade_object_key UNIQUE (object_key)
);
-- +go

CREATE UNIQUE INDEX IF NOT EXISTS uq_client_facade_current_approved
    ON distribuidora.client_facade_photos (client_id)
    WHERE is_current AND validation_status = 'approved';
-- +go

CREATE UNIQUE INDEX IF NOT EXISTS uq_client_facade_idem
    ON distribuidora.client_facade_photos (captured_by_user_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
-- +go

CREATE INDEX IF NOT EXISTS idx_client_facade_photos_client
    ON distribuidora.client_facade_photos (client_id, created_at DESC);
-- +go

ALTER TABLE distribuidora.client_location_proposals
    DROP CONSTRAINT IF EXISTS fk_proposal_facade_photo;
-- +go

ALTER TABLE distribuidora.client_location_proposals
    ADD CONSTRAINT fk_proposal_facade_photo
    FOREIGN KEY (facade_photo_id)
    REFERENCES distribuidora.client_facade_photos (id)
    ON DELETE SET NULL;
-- +go

ALTER TABLE distribuidora.client_verified_locations
    DROP CONSTRAINT IF EXISTS fk_verified_proposal;
-- +go

ALTER TABLE distribuidora.client_verified_locations
    ADD CONSTRAINT fk_verified_proposal
    FOREIGN KEY (approved_proposal_id)
    REFERENCES distribuidora.client_location_proposals (id)
    ON DELETE SET NULL;
-- +go

CREATE TABLE IF NOT EXISTS distribuidora.client_location_audit (
    id BIGSERIAL PRIMARY KEY,
    client_id BIGINT NOT NULL,
    entity_type TEXT NOT NULL
        CHECK (entity_type IN ('location_proposal', 'facade_photo', 'verified_location')),
    entity_id BIGINT,
    action TEXT NOT NULL,
    actor_user_id INTEGER,
    dispatch_plan_id BIGINT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +go

CREATE INDEX IF NOT EXISTS idx_client_location_audit_client
    ON distribuidora.client_location_audit (client_id, created_at DESC);
-- +go

CREATE INDEX IF NOT EXISTS idx_client_verified_locations_company
    ON distribuidora.client_verified_locations (company_id);
-- +go

CREATE INDEX IF NOT EXISTS idx_client_facade_photos_company
    ON distribuidora.client_facade_photos (company_id, created_at DESC);
-- +go

COMMENT ON TABLE distribuidora.client_location_proposals IS
    'Propuestas GPS del chofer; requieren aprobación ERP. No mutan bsale.clients.';
-- +go

COMMENT ON TABLE distribuidora.client_facade_photos IS
    'Metadatos de fachada; bytes en Cloudflare R2 privado (object_key).';
-- +go
