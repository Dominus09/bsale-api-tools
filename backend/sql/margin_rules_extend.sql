-- Extensiones opcionales para bsale.margin_rules (administración Política de Márgenes).
-- Ejecutar en PostgreSQL si la tabla solo tiene min_margin (ver margin_rules_schema.sql).

ALTER TABLE bsale.margin_rules
    ADD COLUMN IF NOT EXISTS max_margin NUMERIC,
    ADD COLUMN IF NOT EXISTS notes TEXT;

COMMENT ON COLUMN bsale.margin_rules.max_margin IS
    'Margen máximo permitido (% sobre costo). Usado en erp_margin_dashboard y alertas.';
COMMENT ON COLUMN bsale.margin_rules.notes IS
    'Notas operativas de la regla (solo administración).';
