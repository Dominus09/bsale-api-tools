-- Diagnóstico READ-ONLY: frescura recepciones RAW vs analytics.cost_reception_history
-- company_id = 3 (La Quillotana SPA)
-- NO escribe. NO cambia privilegios.
--
-- ai_reader: puede ejecutar secciones 1–4 (sync_state / sync_runs / analytics).
-- Secciones 5–6 requieren SELECT en bsale_raw.stock_receptions(+_details);
-- si fallan por privilegio, NO implica ausencia de datos (ver sync_runs).

-- =============================================================================
-- 1) Watermark analytics (capa legacy sync_cost_receptions)
-- =============================================================================
SELECT
    company_id,
    last_admission_ts,
    to_timestamp(last_admission_ts) AT TIME ZONE 'UTC' AS last_admission_utc,
    last_run_at,
    last_status,
    last_message,
    receptions_inserted,
    lines_inserted
FROM analytics.cost_sync_state
WHERE company_id = 3;

-- =============================================================================
-- 2) Cobertura mensual analytics 2026
-- =============================================================================
SELECT
    date_trunc('month', admission_date)::date AS mes,
    count(*)::bigint AS lineas,
    count(DISTINCT reception_id)::bigint AS recepciones,
    min(admission_date) AS min_adm,
    max(admission_date) AS max_adm
FROM analytics.cost_reception_history
WHERE company_id = 3
  AND admission_date >= DATE '2026-01-01'
GROUP BY 1
ORDER BY 1;

-- =============================================================================
-- 3) Estado sync RAW recepciones (autorizado para ai_reader)
-- =============================================================================
SELECT
    company_id,
    resource,
    scope,
    status,
    last_success_at,
    last_attempt_at,
    rows_received,
    last_sync_run_id,
    updated_at
FROM bsale_raw.sync_state
WHERE company_id = 3
  AND resource = 'stock_receptions'
ORDER BY scope;

-- =============================================================================
-- 4) Evidencia de ventanas ago/sep/oct en sync_runs (sin SELECT a tablas de filas)
-- =============================================================================
SELECT
    id AS sync_run_id,
    mode,
    status,
    started_at,
    finished_at,
    summary->>'scope' AS scope,
    (summary->>'rows_received')::int AS rows_received,
    (summary->'point'->>'lines')::int AS lines,
    (summary->'point'->>'created')::int AS created,
    summary->'point'->'days_done' AS days_done
FROM bsale_raw.sync_runs
WHERE (summary->>'resource') = 'stock_receptions'
  AND (summary->>'company_id') = '3'
  AND started_at >= TIMESTAMPTZ '2026-10-13'
  AND mode = 'WINDOW_RECONCILE'
  AND (
        summary->>'scope' ILIKE '%2026-08%'
     OR summary->>'scope' ILIKE '%2026-09%'
     OR summary->>'scope' ILIKE '%2026-10%'
     OR summary->>'scope' ILIKE '%2026-06-01..2026-08-31%'
  )
ORDER BY started_at;

-- Criterio de veredicto (manual):
--   A) RAW last_success reciente + analytics.max_admission << hoy  → analytics desactualizada
--   B) RAW sync FAILED / sin SUCCESS reciente                    → sync RAW rota
--   C) Job sync_cost_receptions es MANUAL (docs/BSALE_SYNC_INDEX) → corte intencional post-piloto

-- =============================================================================
-- 5) OPCIONAL (rol con SELECT en stock_receptions) — conteo mensual RAW exacto
-- =============================================================================
-- SELECT
--     date_trunc('month', admission_date)::date AS mes,
--     count(*)::bigint AS recepciones,
--     min(admission_date) AS min_adm,
--     max(admission_date) AS max_adm
-- FROM bsale_raw.stock_receptions
-- WHERE company_id = 3
--   AND missing_since IS NULL
--   AND admission_date >= DATE '2026-01-01'
-- GROUP BY 1
-- ORDER BY 1;
--
-- SELECT count(*)::bigint AS detail_lines
-- FROM bsale_raw.stock_reception_details
-- WHERE company_id = 3 AND missing_since IS NULL;
--
-- -- Cristal 11053 últimos movimientos por oficina (RAW)
-- SELECT DISTINCT ON (office_id)
--     r.office_id,
--     r.admission_date,
--     r.bsale_id AS reception_id,
--     r.document,
--     r.document_number,
--     d.quantity,
--     d.cost
-- FROM bsale_raw.stock_receptions r
-- JOIN bsale_raw.stock_reception_details d
--   ON d.company_id = r.company_id AND d.reception_id = r.bsale_id
-- WHERE r.company_id = 3
--   AND r.missing_since IS NULL
--   AND d.missing_since IS NULL
--   AND d.variant_id = 11053
-- ORDER BY office_id, r.admission_date DESC, r.bsale_id DESC;

-- =============================================================================
-- 6) Procedimiento seguro de catch-up analytics (NO ejecutar desde Cursor)
-- =============================================================================
-- Objetivo: append-only en cost_reception_history (ON CONFLICT DO NOTHING).
-- No borrar históricos. No tocar Costos V2 / bsale_raw.
--
-- Dry-run (cuenta recepciones Bsale del rango; no escribe):
--   python -m backend.jobs.sync_cost_receptions \
--     --company-id 3 \
--     --date-from 2026-08-01 --date-to 2026-10-27 \
--     --history-only --dry-run
--
-- Apply controlado (obligatorio --confirm-reception-count = N del dry-run):
--   python -m backend.jobs.sync_cost_receptions \
--     --company-id 3 \
--     --date-from 2026-08-01 --date-to 2026-10-27 \
--     --history-only \
--     --confirm-reception-count N \
--     --apply
--
-- Alternativa incremental (sin fechas; escribe desde watermark-1d):
--   python -m backend.jobs.sync_cost_receptions --company-id 3 --history-only
--
-- Luego, si se usa capa calculated legacy:
--   python -m backend.jobs.sync_cost_reception_calculated_v2 --company-id 3
--
-- Nota: Comparativo por oficina de Costos V2 lee bsale_raw, NO esta tabla.
--       El catch-up de analytics es opcional para legacy /costos y calculated.
