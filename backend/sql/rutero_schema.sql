-- Rutero operativo: mismas columnas de negocio que bsale.clients (nombres y orden lógico
-- alineados al sync en sync_clients.py y columnas usadas en backend), más campos de operación.
-- Unicidad natural igual que clients: (company_id, bsale_id) — ver ON CONFLICT en sync_clients.py.
-- No modifica bsale.clients.
-- Tablas ya creadas: aplicar también rutero_constraints.sql (NOT NULL, CHECK, DEFAULT).

CREATE SCHEMA IF NOT EXISTS bsale;

CREATE TABLE IF NOT EXISTS bsale.rutero (
    id                 SERIAL PRIMARY KEY,
    company_id         BIGINT NOT NULL,
    bsale_id           BIGINT NOT NULL,
    first_name         TEXT,
    last_name          TEXT,
    code               TEXT,
    phone              TEXT,
    company            TEXT,
    facebook           TEXT,
    city               TEXT,
    municipality       TEXT,
    address            TEXT,
    created            TIMESTAMP,
    updated            TIMESTAMP,
    dia_atencion       TEXT,
    dia_extra          TEXT,
    nombre_fantasia    TEXT,
    vendedor           TEXT,
    rut_clean          VARCHAR,
    lat                DOUBLE PRECISION,
    lon                DOUBLE PRECISION,
    tipo_atencion      TEXT DEFAULT 'terreno',
    activo             BOOLEAN DEFAULT TRUE,
    orden_ruta         INTEGER,
    fecha_rutero       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT rutero_company_bsale_unique UNIQUE (company_id, bsale_id),
    CONSTRAINT chk_tipo_atencion CHECK (tipo_atencion IN ('terreno', 'telefonico'))
);

CREATE INDEX IF NOT EXISTS idx_rutero_company
    ON bsale.rutero (company_id);

CREATE INDEX IF NOT EXISTS idx_rutero_vendedor
    ON bsale.rutero (vendedor);

CREATE INDEX IF NOT EXISTS idx_rutero_dia
    ON bsale.rutero (dia_atencion);

CREATE INDEX IF NOT EXISTS idx_rutero_municipality
    ON bsale.rutero (municipality);

CREATE INDEX IF NOT EXISTS idx_rutero_activo
    ON bsale.rutero (activo);
