# E.7.3 — EXPLAIN y propuestas de índice (NO aplicar automáticamente)

Fuente: `analytics.cost_reception_calculated` + `analytics.cost_reception_history`.
Versión pin: `cost-v2.0.0`.

## Consultas a explicar (preparar en staging)

1. Company summary (CTE productos + agregados)
2. Listado 50 productos (`sort=latest_reception`)
3. Mayor alza (`sort=pct_increase`)
4. Mayor baja (`sort=pct_decrease`)
5. Detalle de un `variant_id`
6. Historial de un producto (ASC por fecha)
7. Desglose por oficinas (último calculable por office)

Plantilla:

```sql
EXPLAIN (ANALYZE false, BUFFERS false, FORMAT TEXT)
/* pegar SQL del repo CostV2CompanyReadRepository */
;
```

## Índices actuales relevantes

- `(company_id, office_id, admission_date DESC)` en calculated (office-scoped)

## Propuestas (solo si EXPLAIN lo justifica)

```sql
-- NO EJECUTAR desde Cursor / no aplicar en prod sin aprobación
-- Proposal A: acceso empresa + versión + fecha vía history join
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_crc_company_version_history
--   ON analytics.cost_reception_calculated (company_id, calculation_version, history_id);

-- Proposal B: historial por variant
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_crc_company_variant_version
--   ON analytics.cost_reception_calculated (company_id, variant_id, calculation_version);

-- Proposal C: history admission para filtros de periodo
-- (si no existe ya un índice útil en cost_reception_history)
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_crh_company_admission
--   ON analytics.cost_reception_history (company_id, admission_date DESC, id DESC);
```

Documentar resultados de EXPLAIN en staging antes de crear índices.
