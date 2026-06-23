-- Costos 2.0: notas de recepción, tipo de recepción, índices analíticos.

ALTER TABLE analytics.cost_reception_history
    ADD COLUMN IF NOT EXISTS reception_note TEXT,
    ADD COLUMN IF NOT EXISTS reception_type TEXT;

COMMENT ON COLUMN analytics.cost_reception_history.reception_note IS
    'Nota Bsale de la recepción (note). Append-only; no recalcular históricos.';

COMMENT ON COLUMN analytics.cost_reception_history.reception_type IS
    'recepcion_normal | recepcion_ajuste | recepcion_devolucion | recepcion_nc';

CREATE INDEX IF NOT EXISTS idx_cost_history_company_reception_type
    ON analytics.cost_reception_history (company_id, reception_type);

CREATE INDEX IF NOT EXISTS idx_cost_history_company_variant_admission
    ON analytics.cost_reception_history (company_id, variant_id, admission_date DESC);
