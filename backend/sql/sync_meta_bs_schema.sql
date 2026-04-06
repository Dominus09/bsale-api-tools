-- Metadatos Bsale sincronizados por sync_meta_bs.py (tipos de documento y usuarios API).
-- Ejecutar antes del primer sync si las tablas no existen.

CREATE SCHEMA IF NOT EXISTS bsale;

CREATE TABLE IF NOT EXISTS bsale.document_types (
    company_id  INTEGER NOT NULL,
    bsale_id    BIGINT NOT NULL,
    name        TEXT,
    code_sii    INTEGER,
    CONSTRAINT document_types_company_bsale_unique UNIQUE (company_id, bsale_id)
);

-- Usuarios devueltos por GET /users.json de Bsale (no confundir con login staff en bsale.users).
CREATE TABLE IF NOT EXISTS bsale.bsale_users (
    company_id  INTEGER NOT NULL,
    bsale_id    BIGINT NOT NULL,
    first_name  TEXT,
    last_name   TEXT,
    email       TEXT,
    state       TEXT,
    CONSTRAINT bsale_users_company_bsale_unique UNIQUE (company_id, bsale_id)
);
