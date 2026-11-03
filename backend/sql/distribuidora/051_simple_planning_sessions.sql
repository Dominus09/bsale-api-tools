-- Aditivo 3B.3 — aplicar SOLO vía apply_distribuidora_schema o psql manual.
-- BD objetivo: analytics. Sesiones de confirmación simple_list.
-- Nota: 051 aquí ≠ backend/sql/051_product_master_variants_sync_columns.sql.

CREATE TABLE IF NOT EXISTS distribuidora.simple_planning_sessions (
    id UUID PRIMARY KEY,
    user_id INTEGER NOT NULL,
    company_id INTEGER NOT NULL,
    office_id INTEGER NOT NULL,
    planning_date DATE NOT NULL,
    truck_id INTEGER NOT NULL
        REFERENCES distribuidora.trucks (id),
    dispatch_name TEXT,
    driver_name TEXT,
    plan_session_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'open'
        CHECK (status IN ('open', 'confirming', 'confirmed', 'expired', 'failed')),
    expires_at TIMESTAMPTZ NOT NULL,
    confirmed_plan_id BIGINT
        REFERENCES distribuidora.dispatch_plan (id),
    idempotency_key TEXT,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_simple_planning_sessions_idempotency
        UNIQUE (user_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_simple_planning_sessions_user_status
    ON distribuidora.simple_planning_sessions (user_id, status, expires_at);

CREATE INDEX IF NOT EXISTS idx_simple_planning_sessions_plan_session
    ON distribuidora.simple_planning_sessions (plan_session_id);

COMMENT ON TABLE distribuidora.simple_planning_sessions IS
  'Sesión firmada en servidor para confirmar un camión de planificación simplificada. No reutilizable entre usuarios.';
