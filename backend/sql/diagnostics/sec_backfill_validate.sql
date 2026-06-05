-- Validación antes/después del job: python -m backend.jobs.backfill_units_per_box
-- Patrón: (SEC N) case-insensitive

\echo '=== ANTES / ESTADO ACTUAL ==='

SELECT COUNT(*)::bigint AS variants_total
FROM bsale.variants;

SELECT COUNT(*)::bigint AS variants_con_patron_sec
FROM bsale.variants v
WHERE COALESCE(v.description, '') ~* '\(SEC\s*([0-9]+)';

SELECT COUNT(*)::bigint AS variants_sin_upb_con_sec
FROM bsale.variants v
WHERE (v.units_per_box IS NULL OR v.units_per_box = 0)
  AND COALESCE(v.description, '') ~* '\(SEC\s*([0-9]+)';

SELECT COUNT(*)::bigint AS variants_con_upb
FROM bsale.variants v
WHERE v.units_per_box IS NOT NULL AND v.units_per_box > 0;

SELECT COUNT(*)::bigint AS products_master_con_upb
FROM bsale.products_master pm
WHERE pm.units_per_box IS NOT NULL AND pm.units_per_box > 0;

\echo '=== MUESTRA (10) candidatos a backfill ==='

SELECT
    v.company_id,
    v.bsale_id AS variant_id,
    BTRIM(v.bar_code) AS barcode,
    v.units_per_box,
    (regexp_match(COALESCE(v.description, ''), '\(SEC\s*([0-9]+)', 'i'))[1]::integer AS sec_extraido,
    LEFT(v.description, 120) AS description_preview
FROM bsale.variants v
WHERE (v.units_per_box IS NULL OR v.units_per_box = 0)
  AND COALESCE(v.description, '') ~* '\(SEC\s*([0-9]+)'
ORDER BY v.company_id, v.bsale_id
LIMIT 10;

\echo '=== PM desalineados (variant tiene CxC, PM no) ==='

SELECT COUNT(*)::bigint AS pm_desalineados
FROM bsale.products_master pm
JOIN bsale.variants v ON BTRIM(v.bar_code) = pm.barcode
WHERE v.units_per_box IS NOT NULL AND v.units_per_box > 0
  AND (pm.units_per_box IS NULL OR pm.units_per_box IS DISTINCT FROM v.units_per_box);

\echo '=== DESPUÉS del job: repetir este script y comparar ==='
