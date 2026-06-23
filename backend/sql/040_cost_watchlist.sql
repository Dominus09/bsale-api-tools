-- Fase 3: watchlist personal por usuario (staff).

CREATE TABLE IF NOT EXISTS analytics.cost_watchlist (
    id BIGSERIAL PRIMARY KEY,
    user_email TEXT NOT NULL,
    user_id INTEGER,
    company_id INTEGER NOT NULL,
    variant_id BIGINT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT cost_watchlist_user_company_variant UNIQUE (user_email, company_id, variant_id)
);

CREATE INDEX IF NOT EXISTS idx_cost_watchlist_user_company
    ON analytics.cost_watchlist (user_email, company_id)
    WHERE active = TRUE;

COMMENT ON TABLE analytics.cost_watchlist IS
    'Productos críticos seguidos por usuario staff (Analítica → Costos).';
