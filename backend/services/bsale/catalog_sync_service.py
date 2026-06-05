"""Sincronización catálogo Bsale → variants/products → products_master (UPSERT seguro)."""

from __future__ import annotations

import logging
import time
from typing import Any

from backend.db import get_connection

logger = logging.getLogger(__name__)

LOG_PREFIX = "[CATALOG_SYNC]"
SEC_LOG_PREFIX = "[SEC_BACKFILL]"

# Patrón: (SEC 6), (SEC 12), (SEC 24), (SEC 48), etc.
_SEC_REGEX = r"\(SEC\s*([0-9]+)"

_SEC_BACKFILL_SQL = f"""
UPDATE bsale.variants v
SET units_per_box = (regexp_match(
    COALESCE(v.description, ''),
    '{_SEC_REGEX}',
    'i'
))[1]::integer
WHERE (v.units_per_box IS NULL OR v.units_per_box = 0)
  AND COALESCE(v.description, '') ~* '{_SEC_REGEX}'
  AND (regexp_match(
        COALESCE(v.description, ''),
        '{_SEC_REGEX}',
        'i'
    ))[1]::integer > 0
"""

_SEC_COUNT_SQL = f"""
SELECT
    COUNT(*)::bigint AS variants_total,
    COUNT(*) FILTER (
        WHERE COALESCE(v.description, '') ~* '{_SEC_REGEX}'
    )::bigint AS variants_con_sec,
    COUNT(*) FILTER (
        WHERE (v.units_per_box IS NULL OR v.units_per_box = 0)
          AND COALESCE(v.description, '') ~* '{_SEC_REGEX}'
          AND (regexp_match(
                COALESCE(v.description, ''),
                '{_SEC_REGEX}',
                'i'
          ))[1]::integer > 0
    )::bigint AS variants_actualizables
FROM bsale.variants v
"""

_SYNC_PM_UNITS_SQL = """
UPDATE bsale.products_master pm
SET units_per_box = src.units_per_box,
    updated_at = NOW()
FROM (
    SELECT
        BTRIM(v.bar_code) AS barcode,
        (
            array_agg(v.units_per_box ORDER BY v.company_id, v.bsale_id)
            FILTER (WHERE v.units_per_box IS NOT NULL AND v.units_per_box > 0)
        )[1] AS units_per_box
    FROM bsale.variants v
    WHERE NULLIF(BTRIM(v.bar_code), '') IS NOT NULL
      AND v.units_per_box IS NOT NULL
      AND v.units_per_box > 0
    GROUP BY BTRIM(v.bar_code)
) src
WHERE pm.barcode = src.barcode
  AND pm.units_per_box IS DISTINCT FROM src.units_per_box
"""

_SYNC_PM_UNITS_BY_VARIANT_SQL = """
UPDATE bsale.products_master pm
SET units_per_box = v.units_per_box,
    updated_at = NOW()
FROM bsale.variants v
WHERE pm.variant_id = v.bsale_id
  AND v.units_per_box IS NOT NULL
  AND v.units_per_box > 0
  AND (pm.units_per_box IS NULL OR pm.units_per_box = 0)
  AND pm.units_per_box IS DISTINCT FROM v.units_per_box
"""

_PM_UNITS_SYNCABLE_COUNT_SQL = """
SELECT COUNT(*)::bigint
FROM bsale.products_master pm
WHERE EXISTS (
    SELECT 1
    FROM bsale.variants v
    WHERE (
        (NULLIF(BTRIM(v.bar_code), '') IS NOT NULL AND pm.barcode = BTRIM(v.bar_code))
        OR (pm.variant_id IS NOT NULL AND pm.variant_id = v.bsale_id)
    )
    AND v.units_per_box IS NOT NULL
    AND v.units_per_box > 0
    AND pm.units_per_box IS DISTINCT FROM v.units_per_box
)
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
        product_id = EXCLUDED.product_id,
        variant_id = EXCLUDED.variant_id,
        product_name = EXCLUDED.product_name,
        variant_name = EXCLUDED.variant_name,
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


def _sec_backfill_counts(cur: Any) -> dict[str, int]:
    cur.execute(_SEC_COUNT_SQL)
    row = cur.fetchone()
    cols = [d[0] for d in cur.description]
    data = dict(zip(cols, row)) if row else {}
    return {
        "variants_total": int(data.get("variants_total") or 0),
        "variants_con_sec": int(data.get("variants_con_sec") or 0),
        "variants_actualizables": int(data.get("variants_actualizables") or 0),
    }


def run_sec_backfill(*, dry_run: bool = False) -> dict[str, Any]:
    """
    Backfill SEC → variants.units_per_box → products_master.units_per_box.
    dry_run=True: solo conteos, sin UPDATE.
    """
    t0 = time.perf_counter()
    conn = get_connection()
    try:
        cur = conn.cursor()
        counts = _sec_backfill_counts(cur)
        variants_updated = 0
        pm_updated = 0

        if dry_run:
            cur.execute(_PM_UNITS_SYNCABLE_COUNT_SQL)
            pm_row = cur.fetchone()
            pm_updated = int(pm_row[0] or 0) if pm_row else 0
            variants_updated = counts["variants_actualizables"]
        else:
            cur.execute(_SEC_BACKFILL_SQL)
            variants_updated = int(cur.rowcount)
            cur.execute(_SYNC_PM_UNITS_SQL)
            pm_updated = int(cur.rowcount)
            cur.execute(_SYNC_PM_UNITS_BY_VARIANT_SQL)
            pm_updated += int(cur.rowcount)
            conn.commit()

        cur.close()
        duration_ms = int((time.perf_counter() - t0) * 1000)
        result = {
            "ok": True,
            "dry_run": dry_run,
            "variants_total": counts["variants_total"],
            "variants_con_sec": counts["variants_con_sec"],
            "variants_actualizadas": variants_updated,
            "products_master_actualizados": pm_updated,
            "duration_ms": duration_ms,
        }
        logger.info(
            "%s dry_run=%s variants_total=%s variants_con_sec=%s "
            "variants_actualizadas=%s products_master_actualizados=%s duration_ms=%s",
            SEC_LOG_PREFIX,
            dry_run,
            result["variants_total"],
            result["variants_con_sec"],
            result["variants_actualizadas"],
            result["products_master_actualizados"],
            duration_ms,
        )
        return result
    except Exception as exc:
        conn.rollback()
        logger.exception("%s failed dry_run=%s", SEC_LOG_PREFIX, dry_run)
        return {
            "ok": False,
            "dry_run": dry_run,
            "error": str(exc),
            "duration_ms": int((time.perf_counter() - t0) * 1000),
        }
    finally:
        conn.close()


def backfill_units_per_box_from_sec() -> dict[str, Any]:
    """Compat: solo UPDATE en variants (usado por sync_bsale_catalog)."""
    r = run_sec_backfill(dry_run=False)
    if not r.get("ok"):
        return {
            "ok": False,
            "units_per_box_actualizados": 0,
            "error": r.get("error"),
        }
    return {
        "ok": True,
        "units_per_box_actualizados": r.get("variants_actualizadas", 0),
    }


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
