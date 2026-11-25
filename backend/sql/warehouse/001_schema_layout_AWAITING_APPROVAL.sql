-- =============================================================================
-- warehouse / 001 — Schema layout físico (Inteligencia de Bodega · slice 1.1)
--
-- AWAITING_APPROVAL — NO ejecutar en producción sin autorización expresa.
-- Aditivo e idempotente. Schema propio `warehouse`.
-- NO toca bsale, bsale_raw, distribuidora, stock comercial ni PWA.
--
-- Identidad permanente: warehouse.physical_elements
-- Geometría versionada: warehouse.layout_elements (por layout_revisions)
-- =============================================================================

BEGIN;

CREATE SCHEMA IF NOT EXISTS warehouse;

COMMENT ON SCHEMA warehouse IS
  'Inteligencia de Bodega: layout físico e identidades. Independiente de stock Bsale.';

-- ---------------------------------------------------------------------------
-- Convención de coordenadas (SoT métrico; el frontend debe usar la misma)
-- ---------------------------------------------------------------------------
-- Origen: esquina suroeste del límite de la instalación (plano 2D).
-- +X: a lo largo del largo del galpón (metros).
-- +Y: a lo largo del ancho del galpón (metros).
-- Rotación: grados en sentido antihorario; pivote = centro del rectángulo
--   (x_m + width_m/2, y_m + depth_m/2) salvo meta_json.pivot explícito.
-- Unidades: metros. No almacenar píxeles.
-- elevation_m: cota del piso de edificio (0 = losa del piso).
-- stack_levels_max: niveles de apilamiento de pallets (FUTURO) — independiente
--   de elevation_m; NULL = desconocido / no autorizado.
-- validation_status: draft | pending_validation | validated
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS warehouse.facilities (
    id              BIGSERIAL PRIMARY KEY,
    code            TEXT NOT NULL,
    name            TEXT NOT NULL,
    company_id      BIGINT NOT NULL DEFAULT 3,
    bsale_office_id BIGINT,
    status          TEXT NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'archived')),
    meta_json       JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by      TEXT,
    updated_by      TEXT,
    CONSTRAINT uq_warehouse_facilities_company_code UNIQUE (company_id, code)
);

CREATE TABLE IF NOT EXISTS warehouse.floors (
    id              BIGSERIAL PRIMARY KEY,
    facility_id     BIGINT NOT NULL
                    REFERENCES warehouse.facilities (id) ON DELETE RESTRICT,
    code            TEXT NOT NULL,
    name            TEXT NOT NULL,
    level_index     INTEGER NOT NULL DEFAULT 1,
    length_m        NUMERIC(12, 4),
    width_m         NUMERIC(12, 4),
    status          TEXT NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'archived')),
    meta_json       JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by      TEXT,
    updated_by      TEXT,
    CONSTRAINT uq_warehouse_floors_facility_code UNIQUE (facility_id, code),
    CONSTRAINT uq_warehouse_floors_facility_level UNIQUE (facility_id, level_index),
    CONSTRAINT ck_warehouse_floors_dims CHECK (
        (length_m IS NULL OR length_m > 0)
        AND (width_m IS NULL OR width_m > 0)
    )
);

-- Identidad permanente de un elemento físico (sobrevive a revisiones geométricas).
CREATE TABLE IF NOT EXISTS warehouse.physical_elements (
    id                  BIGSERIAL PRIMARY KEY,
    facility_id         BIGINT NOT NULL
                        REFERENCES warehouse.facilities (id) ON DELETE RESTRICT,
    floor_id            BIGINT NOT NULL
                        REFERENCES warehouse.floors (id) ON DELETE RESTRICT,
    parent_id           BIGINT
                        REFERENCES warehouse.physical_elements (id) ON DELETE RESTRICT,
    element_type        TEXT NOT NULL,
    code                TEXT NOT NULL,
    name                TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active', 'retired')),
    retired_at          TIMESTAMPTZ,
    retired_by          TEXT,
    retire_reason       TEXT,
    meta_json           JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by          TEXT,
    updated_by          TEXT,
    CONSTRAINT ck_warehouse_physical_type CHECK (
        element_type IN (
            'facility_boundary',
            'sector',
            'aisle',
            'position',
            'loose_storage',
            'wall',
            'pillar',
            'door',
            'gate',
            'stair',
            'office',
            'circulation',
            'obstacle',
            'unusable',
            'beam',
            'other'
        )
    ),
    CONSTRAINT ck_warehouse_physical_no_self_parent CHECK (
        parent_id IS NULL OR parent_id <> id
    ),
    CONSTRAINT ck_warehouse_physical_retire CHECK (
        (status = 'active' AND retired_at IS NULL)
        OR (status = 'retired' AND retired_at IS NOT NULL)
    )
);

-- Código visible único entre elementos activos del mismo piso.
CREATE UNIQUE INDEX IF NOT EXISTS uq_warehouse_physical_floor_code_active
    ON warehouse.physical_elements (floor_id, lower(code))
    WHERE status = 'active';

CREATE INDEX IF NOT EXISTS ix_warehouse_physical_floor_type
    ON warehouse.physical_elements (floor_id, element_type, status);

CREATE INDEX IF NOT EXISTS ix_warehouse_physical_parent
    ON warehouse.physical_elements (parent_id)
    WHERE parent_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS warehouse.layout_revisions (
    id              BIGSERIAL PRIMARY KEY,
    floor_id        BIGINT NOT NULL
                    REFERENCES warehouse.floors (id) ON DELETE RESTRICT,
    status          TEXT NOT NULL DEFAULT 'draft'
                    CHECK (status IN ('draft', 'published', 'archived')),
    label           TEXT,
    row_version     INTEGER NOT NULL DEFAULT 1,
    notes           TEXT,
    published_at    TIMESTAMPTZ,
    published_by    TEXT,
    archived_at     TIMESTAMPTZ,
    archived_by     TEXT,
    source_revision_id BIGINT,
    publish_meta_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by      TEXT,
    updated_by      TEXT
);

-- MVP: como máximo un borrador activo por piso.
CREATE UNIQUE INDEX IF NOT EXISTS uq_warehouse_one_draft_per_floor
    ON warehouse.layout_revisions (floor_id)
    WHERE status = 'draft';

-- Como máximo una revisión published vigente por piso.
CREATE UNIQUE INDEX IF NOT EXISTS uq_warehouse_one_published_per_floor
    ON warehouse.layout_revisions (floor_id)
    WHERE status = 'published';

CREATE INDEX IF NOT EXISTS ix_warehouse_revisions_floor_status
    ON warehouse.layout_revisions (floor_id, status, updated_at DESC);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_warehouse_revisions_source'
    ) THEN
        ALTER TABLE warehouse.layout_revisions
            ADD CONSTRAINT fk_warehouse_revisions_source
            FOREIGN KEY (source_revision_id)
            REFERENCES warehouse.layout_revisions (id) ON DELETE SET NULL;
    END IF;
END $$;

-- Representación geométrica + identidad congelada por revisión.
-- Los *_snap conservan code/name/type/parent tal como se vieron al guardar
-- esta revisión; editar physical_elements en un borrador NO altera published.
CREATE TABLE IF NOT EXISTS warehouse.layout_elements (
    id                      BIGSERIAL PRIMARY KEY,
    revision_id             BIGINT NOT NULL
                            REFERENCES warehouse.layout_revisions (id) ON DELETE CASCADE,
    physical_element_id     BIGINT NOT NULL
                            REFERENCES warehouse.physical_elements (id) ON DELETE RESTRICT,
    -- Identidad histórica de la revisión (aislamiento draft ↔ published)
    code_snap               TEXT NOT NULL,
    name_snap               TEXT NOT NULL,
    element_type_snap       TEXT NOT NULL,
    parent_physical_id_snap BIGINT,
    physical_meta_snap      JSONB NOT NULL DEFAULT '{}'::jsonb,
    x_m                     NUMERIC(12, 4) NOT NULL DEFAULT 0,
    y_m                     NUMERIC(12, 4) NOT NULL DEFAULT 0,
    elevation_m             NUMERIC(12, 4) NOT NULL DEFAULT 0,
    width_m                 NUMERIC(12, 4) NOT NULL,
    depth_m                 NUMERIC(12, 4) NOT NULL,
    height_clear_m          NUMERIC(12, 4),
    rotation_deg            NUMERIC(8, 3) NOT NULL DEFAULT 0,
    geom_type               TEXT NOT NULL DEFAULT 'rect'
                            CHECK (geom_type IN ('rect', 'polygon')),
    polygon_m               JSONB,
    stack_levels_max        INTEGER,
    validation_status       TEXT NOT NULL DEFAULT 'pending_validation'
                            CHECK (validation_status IN (
                                'draft', 'pending_validation', 'validated'
                            )),
    constraints_json        JSONB NOT NULL DEFAULT '{}'::jsonb,
    observations            TEXT,
    meta_json               JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_warehouse_layout_rev_physical UNIQUE (revision_id, physical_element_id),
    CONSTRAINT ck_warehouse_layout_dims CHECK (
        width_m > 0 AND depth_m > 0
        AND (height_clear_m IS NULL OR height_clear_m > 0)
        AND (stack_levels_max IS NULL OR stack_levels_max > 0)
    ),
    CONSTRAINT ck_warehouse_layout_type_snap CHECK (
        element_type_snap IN (
            'facility_boundary',
            'sector',
            'aisle',
            'position',
            'loose_storage',
            'wall',
            'pillar',
            'door',
            'gate',
            'stair',
            'office',
            'circulation',
            'obstacle',
            'unusable',
            'beam',
            'other'
        )
    )
);

CREATE INDEX IF NOT EXISTS ix_warehouse_layout_elements_revision
    ON warehouse.layout_elements (revision_id);

CREATE TABLE IF NOT EXISTS warehouse.layout_audit (
    id              BIGSERIAL PRIMARY KEY,
    revision_id     BIGINT
                    REFERENCES warehouse.layout_revisions (id) ON DELETE SET NULL,
    facility_id     BIGINT
                    REFERENCES warehouse.facilities (id) ON DELETE SET NULL,
    floor_id        BIGINT
                    REFERENCES warehouse.floors (id) ON DELETE SET NULL,
    physical_element_id BIGINT
                    REFERENCES warehouse.physical_elements (id) ON DELETE SET NULL,
    action          TEXT NOT NULL,
    actor_user_id   INTEGER,
    actor_label     TEXT,
    detail_json     JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_warehouse_layout_audit_revision
    ON warehouse.layout_audit (revision_id, created_at DESC);

COMMENT ON TABLE warehouse.physical_elements IS
  'Identidad permanente de ubicación/estructura. Futuros pallets/ocupación referencian este id.';
COMMENT ON TABLE warehouse.layout_elements IS
  'Geometría + snapshot de identidad (code/name/type/parent) por revisión. '
  'Editar o retirar en draft no altera la representación de revisiones published.';
COMMENT ON COLUMN warehouse.layout_elements.code_snap IS
  'Código visible congelado en esta revisión. Fuente de lectura histórica.';
COMMENT ON COLUMN warehouse.layout_elements.name_snap IS
  'Nombre congelado en esta revisión.';
COMMENT ON COLUMN warehouse.layout_elements.element_type_snap IS
  'Tipo congelado en esta revisión.';
COMMENT ON COLUMN warehouse.layout_elements.parent_physical_id_snap IS
  'Padre (physical_elements.id) congelado en esta revisión; NULL si raíz.';
COMMENT ON COLUMN warehouse.layout_elements.physical_meta_snap IS
  'meta_json de identidad congelado en esta revisión.';
COMMENT ON COLUMN warehouse.layout_elements.stack_levels_max IS
  'Capacidad de apilamiento autorizada (futuro). Independiente de height_clear_m; no inferir.';
COMMENT ON COLUMN warehouse.layout_elements.height_clear_m IS
  'Altura libre disponible (metros). No implica capacidad segura de apilamiento.';

COMMIT;
