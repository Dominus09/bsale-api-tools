-- PROPUESTA de índices para lecturas Costos V2 — NO ejecutar automáticamente.
--
-- Beneficio esperado:
--   Keyset listado (company, office, admission_date DESC, id DESC) + filtro versión.
--   Filtros frecuentes: variant_id, barcode (history), effective_quality_status.
--
-- Costo:
--   Escritura adicional en backfill UPSERT; espacio en disco.
--   Evaluar con EXPLAIN en staging antes de aplicar.
--
-- No crear “por si acaso”: aplicar solo tras medir listado limit 50 / summary 7k.

-- Listado / summary scope + keyset
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_crc_v2_scope_admission
-- ON analytics.cost_reception_calculated (company_id, office_id, admission_date DESC, history_id DESC)
-- WHERE calculation_version = 'cost-v2.0.0';

-- Filtro por estado
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_crc_v2_quality
-- ON analytics.cost_reception_calculated (company_id, office_id, effective_quality_status)
-- WHERE calculation_version = 'cost-v2.0.0';

-- Warning containment (GIN sobre jsonb array)
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_crc_v2_warnings_gin
-- ON analytics.cost_reception_calculated USING GIN (warnings_json)
-- WHERE calculation_version = 'cost-v2.0.0';

-- History: búsqueda barcode / document en scope (ya puede existir PK id)
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_crh_company_office_admission
-- ON analytics.cost_reception_history (company_id, office_id, admission_date DESC, id DESC);
