-- =============================================================================
-- bsale_raw 001 — schema + sources (instalación inicial)
--
-- Aplicar MANUALMENTE en este orden: 001 → 008, luego verify_bsale_raw.sql (sólo lectura),
-- y recién después 009_seed_sources.sql. Runbook: docs/BSALE_RAW_PHASE3_APPLY_RUNBOOK.md.
-- IF NOT EXISTS sólo para el bootstrap: bsale_raw no existe todavía. Las migraciones
-- posteriores NO deben depender de IF NOT EXISTS para ocultar drift (usar verify_bsale_raw.sql).
-- Única referencia fuera de bsale_raw: FK a bsale.companies (company_id).
--
-- Seguridad: sources guarda sólo el NOMBRE de la variable de entorno del token. Los tokens
-- Bsale nunca se almacenan en SQL, payload ni configuración.
-- =============================================================================

BEGIN;

CREATE SCHEMA IF NOT EXISTS bsale_raw;

COMMENT ON SCHEMA bsale_raw IS
    'Espejo fiel de la API Bsale por empresa. Sin reglas de negocio. Sin secretos. payload puede contener PII y datos sensibles: acceso restringido, no exponer al frontend.';

CREATE TABLE IF NOT EXISTS bsale_raw.sources (
    company_id BIGINT NOT NULL,
    cpn_id BIGINT NOT NULL,
    name TEXT,
    token_env TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    verified_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_raw_sources PRIMARY KEY (company_id),
    CONSTRAINT fk_raw_sources_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT uq_raw_sources_cpn_id UNIQUE (cpn_id),
    CONSTRAINT ck_raw_sources_token_env CHECK (token_env ~ '^BSALE_TOKEN_[A-Za-z0-9_]+$')
);

COMMENT ON TABLE bsale_raw.sources IS
    'Empresa ↔ instancia Bsale (cpnId de webhooks). token_env = nombre de variable de entorno, nunca el token.';
COMMENT ON COLUMN bsale_raw.sources.cpn_id IS
    'id de instancia (credential.bsale.io/v1/instances/basic/{token}.json) = cpnId del webhook.';
COMMENT ON COLUMN bsale_raw.sources.token_env IS
    'Nombre de la variable de entorno (debe coincidir con bsale.companies.bsale_token). Nunca el valor.';

COMMIT;
