-- Auditoría READ-ONLY Costos V2 sync (Etapa E.6) — NO escribe.
-- Ejecutar en Coolify/psql; Cursor no debe correrlo contra producción.

-- 1) Columnas reales de history
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'analytics'
  AND table_name = 'cost_reception_history'
ORDER BY ordinal_position;

-- 2) Bounds + cobertura V2 (company 3 / office 3)
SELECT
    COUNT(*)::bigint AS total_rows,
    COUNT(*) FILTER (WHERE h.admission_date >= DATE '2026-06-23')::bigint AS rows_after_2026_06_22,
    MIN(h.admission_date) AS min_admission_date,
    MAX(h.admission_date) AS max_admission_date,
    MIN(h.id) AS min_history_id,
    MAX(h.id) AS max_history_id,
    COUNT(*) FILTER (WHERE c.history_id IS NULL)::bigint AS missing_cost_v2,
    COUNT(*) FILTER (WHERE c.history_id IS NOT NULL)::bigint AS with_cost_v2
FROM analytics.cost_reception_history h
LEFT JOIN analytics.cost_reception_calculated c
    ON c.history_id = h.id
   AND c.calculation_version = 'cost-v2.0.0'
WHERE h.company_id = 3
  AND h.office_id = 3;

-- 3) ¿history_id monotónico vs admission_date? (sample de inversiones)
SELECT COUNT(*)::bigint AS inversions
FROM analytics.cost_reception_history a
JOIN analytics.cost_reception_history b
  ON b.company_id = a.company_id
 AND b.office_id = a.office_id
 AND b.id > a.id
 AND b.admission_date < a.admission_date
WHERE a.company_id = 3
  AND a.office_id = 3
LIMIT 1;

-- Nota: no existe updated_at ni synced_at en 038.
-- Sync: ON CONFLICT (unique_key) DO NOTHING → append-only.
