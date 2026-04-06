-- Carga masiva de ofertas (histórico): SOLO INSERT, sin UPDATE.
-- Formato de entrada:
-- barcode | offer_type | status | start_date | end_date | reason | notes
--
-- Reglas:
-- 1) Ignora barcodes que no existan en bsale.products_master.
-- 2) Valida fechas (formato YYYY-MM-DD y start_date <= end_date).
-- 3) Bonus: detecta conflicto si ya existe oferta activa del mismo barcode + offer_type.

BEGIN;

CREATE TEMP TABLE temp_offer_upload (
    barcode    TEXT,
    offer_type TEXT,
    status     TEXT,
    start_date TEXT,
    end_date   TEXT,
    reason     TEXT,
    notes      TEXT
) ON COMMIT DROP;

-- Cargar datos aquí (ejemplo):
-- INSERT INTO temp_offer_upload (barcode, offer_type, status, start_date, end_date, reason, notes) VALUES
-- ('7801234567890', 'oferta_mes', 'activa', '2026-04-01', '2026-04-30', 'Campana abril', 'Tope 500 unidades');

WITH normalized AS (
    SELECT
        btrim(t.barcode) AS barcode,
        NULLIF(btrim(t.offer_type), '') AS offer_type,
        NULLIF(btrim(t.status), '') AS status,
        CASE
            WHEN t.start_date ~ '^\d{4}-\d{2}-\d{2}$' THEN t.start_date::date
            ELSE NULL
        END AS start_date,
        CASE
            WHEN t.end_date ~ '^\d{4}-\d{2}-\d{2}$' THEN t.end_date::date
            ELSE NULL
        END AS end_date,
        NULLIF(btrim(t.reason), '') AS reason,
        NULLIF(btrim(t.notes), '') AS notes
    FROM temp_offer_upload t
),
valid_rows AS (
    SELECT n.*
    FROM normalized n
    INNER JOIN bsale.products_master pm
        ON pm.barcode = n.barcode
    WHERE n.barcode <> ''
      AND n.offer_type IS NOT NULL
      AND n.status IS NOT NULL
      AND n.start_date IS NOT NULL
      AND n.end_date IS NOT NULL
      AND n.start_date <= n.end_date
),
active_conflicts AS (
    SELECT DISTINCT
        v.barcode,
        v.offer_type
    FROM valid_rows v
    INNER JOIN bsale.product_offers po
        ON po.barcode = v.barcode
       AND po.offer_type = v.offer_type
       AND po.status = 'activa'
       AND CURRENT_DATE BETWEEN po.start_date AND po.end_date
)
INSERT INTO bsale.product_offers (
    barcode,
    offer_type,
    status,
    start_date,
    end_date,
    reason,
    notes,
    created_at,
    updated_at
)
SELECT
    v.barcode,
    v.offer_type,
    v.status,
    v.start_date,
    v.end_date,
    v.reason,
    v.notes,
    NOW(),
    NOW()
FROM valid_rows v
LEFT JOIN active_conflicts c
    ON c.barcode = v.barcode
   AND c.offer_type = v.offer_type
WHERE c.barcode IS NULL;

-- BONUS: revisar conflictos detectados (oferta activa existente mismo producto+tipo)
WITH normalized AS (
    SELECT
        btrim(t.barcode) AS barcode,
        NULLIF(btrim(t.offer_type), '') AS offer_type,
        CASE
            WHEN t.start_date ~ '^\d{4}-\d{2}-\d{2}$' THEN t.start_date::date
            ELSE NULL
        END AS start_date,
        CASE
            WHEN t.end_date ~ '^\d{4}-\d{2}-\d{2}$' THEN t.end_date::date
            ELSE NULL
        END AS end_date
    FROM temp_offer_upload t
),
valid_rows AS (
    SELECT n.*
    FROM normalized n
    INNER JOIN bsale.products_master pm
        ON pm.barcode = n.barcode
    WHERE n.barcode <> ''
      AND n.offer_type IS NOT NULL
      AND n.start_date IS NOT NULL
      AND n.end_date IS NOT NULL
      AND n.start_date <= n.end_date
)
SELECT DISTINCT
    v.barcode,
    v.offer_type,
    'conflicto_oferta_activa_existente' AS conflict_reason
FROM valid_rows v
INNER JOIN bsale.product_offers po
    ON po.barcode = v.barcode
   AND po.offer_type = v.offer_type
   AND po.status = 'activa'
   AND CURRENT_DATE BETWEEN po.start_date AND po.end_date
ORDER BY v.barcode, v.offer_type;

COMMIT;
