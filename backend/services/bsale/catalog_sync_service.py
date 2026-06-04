"""Sincronización catálogo Bsale → variants/products → products_master (UPSERT seguro)."""

from __future__ import annotations

import logging
from typing import Any

from backend.db import get_connection

logger = logging.getLogger(__name__)

LOG_PREFIX = "[CATALOG_SYNC]"

_SEC_BACKFILL_SQL = """
UPDATE bsale.variants v
SET units_per_box = (regexp_match(
    UPPER(COALESCE(v.description, '')),
    E'SEC[[:space:]]*([0-9]+)'
))[2]::integer
WHERE (v.units_per_box IS NULL OR v.units_per_box = 0)
  AND UPPER(COALESCE(v.description, '')) ~ E'SEC[[:space:]]*[0-9]+'
  AND (regexp_match(
        UPPER(COALESCE(v.description, '')),
        E'SEC[[:space:]]*([0-9]+)'
    ))[2]::integer > 0
"""

_REFRESH_PRODUCTS_MASTER_SQL = """
WITH source AS (
    SELECT
        BTRIM(v.bar_code) AS barcode,
        (
            array_agg(v.code ORDER BY v.company_id, v.bsale_id)
            FILTER (WHERE v.code IS NOT NULL)
        )[1] AS sku,
        (
            array_agg(p.bsale_id ORDER BY v.company_id, v.bsale_id)
            FILTER (WHERE p.bsale_id IS NOT NULL)
        )[1] AS product_id,
        (
            array_agg(v.bsale_id ORDER BY v.company_id, v.bsale_id)
            FILTER (WHERE v.bsale_id IS NOT NULL)
        )[1] AS variant_id,
        (
            array_agg(p.name ORDER BY v.company_id, v.bsale_id)
            FILTER (WHERE p.name IS NOT NULL)
        )[1] AS product_name,
        (
            array_agg(v.description ORDER BY v.company_id, v.bsale_id)
            FILTER (WHERE v.description IS NOT NULL)
        )[1] AS variant_name,
        (
            array_agg(pt.name ORDER BY v.company_id, v.bsale_id)
            FILTER (WHERE pt.name IS NOT NULL)
        )[1] AS product_type,
        COALESCE(
            to_jsonb(
                array_agg(DISTINCT vp.company_id ORDER BY vp.company_id)
                    FILTER (WHERE vp.company_id IS NOT NULL)
            ),
            '[]'::jsonb
        ) AS companies,
        (
            array_agg(
                NULLIF(v.units_per_box, 0)
                ORDER BY v.company_id, v.bsale_id
            )
            FILTER (WHERE v.units_per_box IS NOT NULL AND v.units_per_box > 0)
        )[1] AS units_per_box
    FROM bsale.variants v
    LEFT JOIN bsale.products p
        ON p.company_id = v.company_id
       AND p.bsale_id = v.product_id
    LEFT JOIN bsale.product_types pt
        ON pt.company_id = p.company_id
       AND pt.bsale_id = p.product_type_id
    LEFT JOIN bsale.variant_prices vp
        ON vp.company_id = v.company_id
       AND vp.variant_id = v.bsale_id
    WHERE v.bar_code IS NOT NULL
      AND BTRIM(v.bar_code) <> ''
    GROUP BY BTRIM(v.bar_code)
),
upserted AS (
    INSERT INTO bsale.products_master (
        barcode,
        sku,
        product_id,
        variant_id,
        product_name,
        variant_name,
        product_type,
        companies,
        units_per_box,
        is_active,
        created_at,
        updated_at,
        last_bsale_sync_at
    )
    SELECT
        s.barcode,
        s.sku,
        s.product_id,
        s.variant_id,
        s.product_name,
        s.variant_name,
        s.product_type,
        s.companies,
        s.units_per_box,
        TRUE,
        NOW(),
        NOW(),
        NOW()
    FROM source s
    ON CONFLICT (barcode) DO UPDATE SET
        sku = EXCLUDED.sku,
        product_id = EXCLUDED.product_id,
        variant_id = EXCLUDED.variant_id,
        product_name = EXCLUDED.product_name,
        variant_name = EXCLUDED.variant_name,
        product_type = EXCLUDED.product_type,
        companies = EXCLUDED.companies,
        units_per_box = EXCLUDED.units_per_box,
        updated_at = NOW(),
        last_bsale_sync_at = NOW()
    RETURNING (xmax = 0) AS inserted
)
SELECT
    COUNT(*) FILTER (WHERE inserted) AS products_master_insertados,
    COUNT(*) FILTER (WHERE NOT inserted) AS products_master_actualizados
FROM upserted
"""


def backfill_units_per_box_from_sec() -> dict[str, Any]:
    """Pobla variants.units_per_box desde patrón (SEC N) en description."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(_SEC_BACKFILL_SQL)
        updated = cur.rowcount
        conn.commit()
        cur.close()
        logger.info("%s units_per_box_actualizados=%s", LOG_PREFIX, updated)
        return {"ok": True, "units_per_box_actualizados": int(updated)}
    except Exception as exc:
        conn.rollback()
        logger.exception("%s backfill_units_per_box_from_sec failed", LOG_PREFIX)
        return {"ok": False, "units_per_box_actualizados": 0, "error": str(exc)}
    finally:
        conn.close()


def refresh_products_master() -> dict[str, Any]:
    """
    UPSERT incremental por barcode. No borra filas. No toca supplier ni cubicación manual.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(_REFRESH_PRODUCTS_MASTER_SQL)
        row = cur.fetchone()
        inserted = int(row[0] or 0) if row else 0
        updated = int(row[1] or 0) if row else 0
        conn.commit()
        cur.close()
        logger.info(
            "%s products_master_insertados=%s products_master_actualizados=%s",
            LOG_PREFIX,
            inserted,
            updated,
        )
        return {
            "ok": True,
            "products_master_insertados": inserted,
            "products_master_actualizados": updated,
        }
    except Exception as exc:
        conn.rollback()
        logger.exception("%s refresh_products_master failed", LOG_PREFIX)
        return {
            "ok": False,
            "products_master_insertados": 0,
            "products_master_actualizados": 0,
            "error": str(exc),
        }
    finally:
        conn.close()


def count_new_bsale_products_since_pm() -> int:
    """Variantes con barcode que aún no están en products_master (diagnóstico)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(DISTINCT BTRIM(v.bar_code))
            FROM bsale.variants v
            WHERE NULLIF(BTRIM(v.bar_code), '') IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM bsale.products_master pm
                  WHERE pm.barcode = BTRIM(v.bar_code)
              )
            """
        )
        n = int(cur.fetchone()[0] or 0)
        cur.close()
        return n
    finally:
        conn.close()
