-- =============================================================================
-- Diagnóstico y reparación: bsale.variants.units_per_box (CxC / cantidad por caja)
-- Origen en análisis de compra: columna variants.units_per_box, con respaldo
-- desde description con patrón Bsale "(SEC N)" o "SEC N".
--
-- Validación esperada: units_per_box = 24 y unidades = 120 → cajas = 5.
-- Si units_per_box es NULL o 0 y no hay SEC, la vista usa eff = 1 (cajas = unidades).
-- =============================================================================

-- --- DIAGNÓSTICO 1: Variantes sin CxC útil (NULL o 0) ---
SELECT
    v.company_id,
    v.bsale_id AS variant_id,
    v.units_per_box,
    LEFT(v.description, 120) AS description_sample,
    (regexp_match(
        UPPER(COALESCE(v.description, '')),
        E'SEC\s*([0-9]+)'
    ))[1] AS sec_extraido,
    CASE
        WHEN v.units_per_box IS NOT NULL AND v.units_per_box > 0 THEN 'OK columna'
        WHEN UPPER(COALESCE(v.description, '')) ~ E'SEC\s*[0-9]+' THEN 'Reparable por SEC'
        ELSE 'Sin columna ni SEC'
    END AS estado
FROM bsale.variants v
WHERE v.units_per_box IS NULL
   OR v.units_per_box = 0
ORDER BY v.company_id, v.bsale_id
LIMIT 500;

-- --- DIAGNÓSTICO 2: Conteo por empresa ---
SELECT
    v.company_id,
    COUNT(*) FILTER (WHERE v.units_per_box IS NOT NULL AND v.units_per_box > 0) AS con_cxc,
    COUNT(*) FILTER (WHERE v.units_per_box IS NULL OR v.units_per_box = 0) AS sin_cxc,
    COUNT(*) FILTER (
        WHERE (v.units_per_box IS NULL OR v.units_per_box = 0)
          AND UPPER(COALESCE(v.description, '')) ~ E'SEC\s*[0-9]+'
    ) AS reparables_sec
FROM bsale.variants v
GROUP BY v.company_id
ORDER BY v.company_id;

-- --- DIAGNÓSTICO 3: Muestra desde vw_purchase_analysis (tras redeploy de la vista) ---
-- cajas_sugeridas debe ser unidades_a_comprar / units_per_box_eff (no 1:1 salvo CxC=1).
SELECT
    company_id,
    office_id,
    variant_id,
    unidades_a_comprar,
    units_per_box,
    units_per_box_eff,
    cajas_sugeridas,
    ROUND(
        (unidades_a_comprar / NULLIF(units_per_box_eff, 0))::numeric,
        4
    ) AS cajas_check
FROM bsale.vw_purchase_analysis
WHERE unidades_a_comprar > 0
  AND units_per_box_eff > 1
  AND ABS(
      cajas_sugeridas
      - (unidades_a_comprar / NULLIF(units_per_box_eff, 0))
  ) > 0.0001
LIMIT 50;

-- --- REPARACIÓN: Poblar units_per_box desde description (SEC) ---
-- No sobrescribe filas con units_per_box > 0.
UPDATE bsale.variants v
SET units_per_box = (regexp_match(
    UPPER(COALESCE(v.description, '')),
    E'SEC\s*([0-9]+)'
))[1]::integer
WHERE (v.units_per_box IS NULL OR v.units_per_box = 0)
  AND UPPER(COALESCE(v.description, '')) ~ E'SEC\s*[0-9]+'
  AND (regexp_match(
        UPPER(COALESCE(v.description, '')),
        E'SEC\s*([0-9]+)'
    ))[1]::integer > 0;

-- Verificar filas afectadas con SELECT antes en transacción:
-- BEGIN; UPDATE ...; SELECT ...; ROLLBACK; o COMMIT;
