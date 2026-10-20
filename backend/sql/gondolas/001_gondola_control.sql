-- =============================================================================
-- Control de Góndolas — sesiones de exhibición, escaneos y reposición.
--
-- NO ejecutar en producción sin revisión y autorización expresa. Migración
-- versionada, idempotente y aditiva:
--   * crea tablas nuevas en el esquema app (separadas de bsale_raw);
--   * NO modifica bsale_raw, stock, documentos ni precios de Bsale;
--   * el stock y la última recepción se leen de BSALE RAW al consultar; aquí
--     solo se congela la referencia del inicio del recorrido y el resultado
--     operativo del escaneo en sala.
--
-- Universo del recorrido: SKU distintos (variantes) con stock positivo en la
-- oficina al iniciar. No se cuentan unidades físicas como SKU.
-- =============================================================================

BEGIN;

CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE IF NOT EXISTS app.gondola_sessions (
    id BIGSERIAL,
    company_id BIGINT NOT NULL,
    office_id BIGINT NOT NULL,
    product_type_id BIGINT NOT NULL,
    status TEXT NOT NULL DEFAULT 'en_curso',
    created_by TEXT NOT NULL,
    created_by_role TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    finished_by TEXT,
    cancelled_at TIMESTAMPTZ,
    cancelled_by TEXT,
    cancel_reason TEXT,
    stock_snapshot_at TIMESTAMPTZ NOT NULL,
    total_sku_snapshot INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT pk_gondola_sessions PRIMARY KEY (id),
    CONSTRAINT uq_gondola_sessions_id_company UNIQUE (id, company_id),
    CONSTRAINT fk_gondola_sessions_company
        FOREIGN KEY (company_id) REFERENCES bsale.companies (company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_gondola_sessions_status
        CHECK (status IN ('en_curso', 'cerrado', 'cancelado')),
    CONSTRAINT ck_gondola_sessions_total CHECK (total_sku_snapshot >= 0),
    CONSTRAINT ck_gondola_sessions_finished
        CHECK (status <> 'cerrado' OR (finished_at IS NOT NULL AND finished_by IS NOT NULL)),
    CONSTRAINT ck_gondola_sessions_cancelled
        CHECK (status <> 'cancelado' OR (cancelled_at IS NOT NULL AND cancelled_by IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS ix_gondola_sessions_company_scope
    ON app.gondola_sessions (company_id, office_id, product_type_id, started_at DESC);
CREATE INDEX IF NOT EXISTS ix_gondola_sessions_company_status
    ON app.gondola_sessions (company_id, status, started_at DESC);

-- Un solo recorrido en curso por empresa + oficina + tipo: evita universos
-- paralelos contradictorios sobre la misma góndola.
CREATE UNIQUE INDEX IF NOT EXISTS uq_gondola_sessions_active_scope
    ON app.gondola_sessions (company_id, office_id, product_type_id)
    WHERE status = 'en_curso';

CREATE TABLE IF NOT EXISTS app.gondola_session_items (
    id BIGSERIAL,
    session_id BIGINT NOT NULL,
    company_id BIGINT NOT NULL,
    variant_id BIGINT NOT NULL,
    product_id BIGINT,
    sku_code TEXT,
    bar_code TEXT,
    product_name TEXT NOT NULL,
    variant_name TEXT,
    stock_snapshot NUMERIC(14, 3) NOT NULL DEFAULT 0,
    stock_available_snapshot NUMERIC(14, 3),
    stock_at_close NUMERIC(14, 3),
    last_entry_at DATE,
    last_entry_movement TEXT,
    exhibition_status TEXT NOT NULL DEFAULT 'pendiente',
    found_at TIMESTAMPTZ,
    found_by TEXT,
    found_device TEXT,
    CONSTRAINT pk_gondola_session_items PRIMARY KEY (id),
    CONSTRAINT fk_gondola_session_items_session
        FOREIGN KEY (session_id, company_id)
        REFERENCES app.gondola_sessions (id, company_id) ON DELETE RESTRICT,
    CONSTRAINT uq_gondola_session_items_variant UNIQUE (session_id, variant_id),
    CONSTRAINT ck_gondola_session_items_status
        CHECK (exhibition_status IN ('pendiente', 'encontrado')),
    CONSTRAINT ck_gondola_session_items_found
        CHECK (exhibition_status <> 'encontrado' OR found_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS ix_gondola_session_items_session_status
    ON app.gondola_session_items (session_id, exhibition_status, product_name);
CREATE INDEX IF NOT EXISTS ix_gondola_session_items_company_variant
    ON app.gondola_session_items (company_id, variant_id);

CREATE TABLE IF NOT EXISTS app.gondola_scans (
    id BIGSERIAL,
    session_id BIGINT NOT NULL,
    company_id BIGINT NOT NULL,
    bar_code_raw TEXT NOT NULL,
    variant_id BIGINT,
    result TEXT NOT NULL,
    device TEXT,
    scanned_by TEXT NOT NULL,
    scanned_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_gondola_scans PRIMARY KEY (id),
    CONSTRAINT fk_gondola_scans_session
        FOREIGN KEY (session_id, company_id)
        REFERENCES app.gondola_sessions (id, company_id) ON DELETE RESTRICT,
    CONSTRAINT ck_gondola_scans_result
        CHECK (result IN (
            'encontrado', 'duplicado', 'ambiguo', 'sin_coincidencia',
            'fuera_de_tipo', 'stock_cero', 'fuera_de_universo'
        )),
    CONSTRAINT ck_gondola_scans_bar_code CHECK (length(btrim(bar_code_raw)) > 0)
);

CREATE INDEX IF NOT EXISTS ix_gondola_scans_session_time
    ON app.gondola_scans (session_id, scanned_at DESC);
CREATE INDEX IF NOT EXISTS ix_gondola_scans_company_bar_code
    ON app.gondola_scans (company_id, bar_code_raw, scanned_at DESC);

CREATE TABLE IF NOT EXISTS app.gondola_replenishment (
    id BIGSERIAL,
    session_id BIGINT NOT NULL,
    company_id BIGINT NOT NULL,
    variant_id BIGINT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pendiente_buscar',
    note TEXT,
    stock_snapshot NUMERIC(14, 3) NOT NULL DEFAULT 0,
    stock_at_close NUMERIC(14, 3),
    updated_by TEXT,
    updated_at TIMESTAMPTZ,
    CONSTRAINT pk_gondola_replenishment PRIMARY KEY (id),
    CONSTRAINT fk_gondola_replenishment_session
        FOREIGN KEY (session_id, company_id)
        REFERENCES app.gondola_sessions (id, company_id) ON DELETE RESTRICT,
    CONSTRAINT fk_gondola_replenishment_item
        FOREIGN KEY (session_id, variant_id)
        REFERENCES app.gondola_session_items (session_id, variant_id) ON DELETE RESTRICT,
    CONSTRAINT uq_gondola_replenishment_variant UNIQUE (session_id, variant_id),
    CONSTRAINT ck_gondola_replenishment_status
        CHECK (status IN (
            'pendiente_buscar', 'encontrado_bodega', 'repuesto_gondola', 'no_encontrado'
        ))
);

CREATE INDEX IF NOT EXISTS ix_gondola_replenishment_session_status
    ON app.gondola_replenishment (session_id, status);

COMMENT ON TABLE app.gondola_sessions IS
    'Control de Góndolas: recorrido de exhibición por empresa, oficina Bsale y tipo de producto. No modifica stock Bsale.';
COMMENT ON COLUMN app.gondola_sessions.stock_snapshot_at IS
    'Instante en que se congeló el universo de SKU con stock positivo y su stock RAW de referencia.';
COMMENT ON COLUMN app.gondola_session_items.last_entry_at IS
    'Última recepción/transferencia de entrada registrada en BSALE RAW para esa oficina (no global por variante).';
COMMENT ON TABLE app.gondola_scans IS
    'Bitácora de escaneos del recorrido: un SKU se marca encontrado una sola vez; los duplicados quedan como intentos.';
COMMENT ON TABLE app.gondola_replenishment IS
    'Lista operativa de SKU no detectados al cerrar el recorrido. No afirma que el producto esté en bodega.';
COMMENT ON CONSTRAINT fk_gondola_replenishment_item ON app.gondola_replenishment IS
    'Solo puede aparecer en reposición un SKU del universo congelado del recorrido (gondola_session_items).';

COMMIT;
