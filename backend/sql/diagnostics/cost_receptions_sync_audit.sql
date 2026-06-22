-- Diagnóstico manual sync_cost_receptions (ejecutar en psql / Coolify).

-- 1) Estado sync por empresa
SELECT
    company_id,
    last_admission_ts,
    to_timestamp(last_admission_ts) AT TIME ZONE 'UTC' AS last_admission_utc,
    to_timestamp(last_admission_ts) AT TIME ZONE 'America/Santiago' AS last_admission_cl,
    last_run_at,
    last_status,
    last_message,
    receptions_inserted,
    lines_inserted,
    total_lines_processed
FROM analytics.cost_sync_state
ORDER BY company_id;

-- 2) Historial cargado
SELECT
    company_id,
    COUNT(*) AS lines,
    COUNT(DISTINCT reception_id) AS receptions,
    MIN(admission_date) AS min_admission,
    MAX(admission_date) AS max_admission
FROM analytics.cost_reception_history
GROUP BY company_id
ORDER BY company_id;

-- 3) ¿Tabla legacy anterior? (si existía migración previa)
SELECT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'bsale' AND table_name = 'cost_analytics_sync'
) AS has_legacy_bsale_cost_analytics_sync;

-- 5) Watermark sin historial (estado inconsistente)
SELECT
    s.company_id,
    s.last_admission_ts,
    to_timestamp(s.last_admission_ts) AT TIME ZONE 'America/Santiago' AS last_admission_cl,
    s.lines_inserted AS lines_last_run,
    s.total_lines_processed,
    COALESCE(h.lines, 0) AS history_lines
FROM analytics.cost_sync_state s
LEFT JOIN (
    SELECT company_id, COUNT(*)::int AS lines
    FROM analytics.cost_reception_history
    GROUP BY company_id
) h ON h.company_id = s.company_id
WHERE s.last_admission_ts IS NOT NULL
  AND COALESCE(h.lines, 0) = 0;

-- 6) Reset watermark para re-sync (solo si history vacío y watermark bloquea lookback)
-- UPDATE analytics.cost_sync_state
-- SET last_admission_ts = NULL, last_message = 'watermark reset manual'
-- WHERE company_id = ? AND NOT EXISTS (
--     SELECT 1 FROM analytics.cost_reception_history WHERE company_id = ?
-- );
