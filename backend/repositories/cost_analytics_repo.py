"""Persistencia Analítica → Costos (analytics.cost_reception_history)."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

import psycopg2
from psycopg2.extras import Json

logger = logging.getLogger(__name__)

HISTORY = "analytics.cost_reception_history"
SYNC_STATE = "analytics.cost_sync_state"

_product_column_cache: dict[str, bool] = {}


def reset_product_column_cache() -> None:
    _product_column_cache.clear()


def log_bsale_products_schema(cur) -> list[str]:
    """Introspección bsale.products — columnas reales en PG."""
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'bsale'
          AND table_name = 'products'
        ORDER BY ordinal_position
        """
    )
    columns = [row[0] for row in cur.fetchall()]
    tax_cols = [
        c
        for c in columns
        if "tax" in c.lower()
    ]
    logger.info(
        "[COST_SYNC_SCHEMA] bsale.products column_count=%s columns=%s",
        len(columns),
        columns,
    )
    logger.info(
        "[COST_SYNC_SCHEMA] tax_related_columns=%s "
        "has_taxes=%s has_tax=%s has_tax_id=%s has_taxes_json=%s",
        tax_cols,
        "taxes" in columns,
        "tax" in columns,
        "tax_id" in columns,
        "taxes_json" in columns,
    )
    return columns


def _cols(cur) -> list[str]:
    return [d[0] for d in cur.description]


def _row_dict(cur, row) -> dict[str, Any]:
    return dict(zip(_cols(cur), row))


def _filter_clause(
    *,
    company_id: int,
    office_id: int | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    prefix: str = "h",
) -> tuple[str, list[Any]]:
    clauses = [f"{prefix}.company_id = %s"]
    params: list[Any] = [company_id]
    if office_id is not None:
        clauses.append(f"{prefix}.office_id = %s")
        params.append(office_id)
    if date_from:
        clauses.append(f"{prefix}.admission_date >= %s")
        params.append(date_from)
    if date_to:
        clauses.append(f"{prefix}.admission_date < %s")
        params.append(date_to)
    return " AND ".join(clauses), params


def get_sync_state(cur, company_id: int) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT company_id, last_admission_ts, last_run_at, last_status,
               last_message, receptions_inserted, lines_inserted, total_lines_processed
        FROM {SYNC_STATE}
        WHERE company_id = %s
        """,
        (company_id,),
    )
    row = cur.fetchone()
    return _row_dict(cur, row) if row else None


def upsert_sync_state(
    cur,
    *,
    company_id: int,
    last_admission_ts: int | None,
    status: str,
    message: str | None,
    receptions_inserted: int,
    lines_inserted: int,
    total_lines_processed: int | None = None,
) -> None:
    cur.execute(
        f"""
        INSERT INTO {SYNC_STATE} (
            company_id, last_admission_ts, last_run_at, last_status,
            last_message, receptions_inserted, lines_inserted, total_lines_processed
        )
        VALUES (%s, %s, NOW(), %s, %s, %s, %s, COALESCE(%s, 0))
        ON CONFLICT (company_id) DO UPDATE
        SET last_admission_ts = COALESCE(EXCLUDED.last_admission_ts, {SYNC_STATE}.last_admission_ts),
            last_run_at = NOW(),
            last_status = EXCLUDED.last_status,
            last_message = EXCLUDED.last_message,
            receptions_inserted = EXCLUDED.receptions_inserted,
            lines_inserted = EXCLUDED.lines_inserted,
            total_lines_processed = COALESCE(EXCLUDED.total_lines_processed, {SYNC_STATE}.total_lines_processed)
                + EXCLUDED.lines_inserted
        """,
        (
            company_id,
            last_admission_ts,
            status,
            message,
            receptions_inserted,
            lines_inserted,
            total_lines_processed,
        ),
    )


def line_exists(cur, company_id: int, reception_detail_id: int) -> bool:
    cur.execute(
        f"""
        SELECT 1 FROM {HISTORY}
        WHERE company_id = %s AND reception_detail_id = %s
        LIMIT 1
        """,
        (company_id, reception_detail_id),
    )
    return cur.fetchone() is not None


def previous_cost_for_variant(
    cur,
    *,
    company_id: int,
    variant_id: int,
    office_id: int | None,
    before: datetime,
) -> float | None:
    if office_id is not None:
        cur.execute(
            f"""
            SELECT cost_net FROM {HISTORY}
            WHERE company_id = %s AND variant_id = %s AND office_id = %s
              AND admission_date < %s
            ORDER BY admission_date DESC, id DESC
            LIMIT 1
            """,
            (company_id, variant_id, office_id, before),
        )
    else:
        cur.execute(
            f"""
            SELECT cost_net FROM {HISTORY}
            WHERE company_id = %s AND variant_id = %s AND admission_date < %s
            ORDER BY admission_date DESC, id DESC
            LIMIT 1
            """,
            (company_id, variant_id, before),
        )
    row = cur.fetchone()
    return float(row[0]) if row and row[0] is not None else None


def insert_history_line(
    cur,
    *,
    unique_key: str,
    company_id: int,
    company_name: str | None,
    office_id: int | None,
    office_name: str | None,
    variant_id: int,
    product_id: int | None,
    barcode: str | None,
    product_name: str | None,
    variant_name: str | None,
    reception_id: int,
    reception_detail_id: int,
    document: str | None,
    document_number: int | None,
    admission_date: datetime,
    quantity: float,
    cost_net: float,
    iva_amount: float,
    other_taxes: float,
    cost_bruto_erp: float,
    average_cost: float | None,
    variation_pct: float | None,
) -> bool:
    cur.execute(
        f"""
        INSERT INTO {HISTORY} (
            unique_key, company_id, company_name, office_id, office_name,
            variant_id, product_id, barcode, product_name, variant_name,
            reception_id, reception_detail_id, document, document_number,
            admission_date, quantity, cost_net, iva_amount, other_taxes,
            cost_bruto_erp, average_cost, variation_pct
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (unique_key) DO NOTHING
        RETURNING id
        """,
        (
            unique_key,
            company_id,
            company_name,
            office_id,
            office_name,
            variant_id,
            product_id,
            barcode,
            product_name,
            variant_name,
            reception_id,
            reception_detail_id,
            document,
            document_number,
            admission_date,
            quantity,
            cost_net,
            iva_amount,
            other_taxes,
            cost_bruto_erp,
            average_cost,
            variation_pct,
        ),
    )
    return cur.fetchone() is not None


def _product_column_exists(cur, column: str) -> bool:
    key = f"bsale.products.{column}"
    if key in _product_column_cache:
        return _product_column_cache[key]
    try:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = 'bsale'
                  AND table_name = 'products'
                  AND column_name = %s
            )
            """,
            (column,),
        )
        exists = bool(cur.fetchone()[0])
    except Exception as exc:
        logger.warning(
            "[COST_SYNC_SCHEMA] column_exists check failed column=%s error=%s",
            column,
            exc,
        )
        try:
            cur.connection.rollback()
        except Exception:
            pass
        exists = False
    _product_column_cache[key] = exists
    return exists


def _default_tax_context(**extra: Any) -> dict[str, Any]:
    return {
        "product_id": extra.get("product_id"),
        "product_name": extra.get("product_name"),
        "variant_name": extra.get("variant_name"),
        "barcode": extra.get("barcode"),
        "tax_factor": 1.0,
        "iva_rate": None,
        "specific_taxes": None,
        "iva_amount": 0,
        "other_taxes": 0,
        "tax_context_available": False,
    }


def _bump_tax_audit(tax_audit: dict[str, int] | None, key: str, n: int = 1) -> None:
    if tax_audit is not None:
        tax_audit[key] = int(tax_audit.get(key, 0)) + n


def _log_tax_context_call(
    *,
    company_id: int,
    variant_id: int,
    tax_context_available: bool,
    has_taxes: bool,
    used_fallback: bool,
    error: str | None = None,
) -> None:
    logger.info(
        "[COST_SYNC_TAX] company_id=%s variant_id=%s tax_context_available=%s "
        "has_taxes_column=%s used_fallback=%s error=%s",
        company_id,
        variant_id,
        tax_context_available,
        has_taxes,
        used_fallback,
        error,
        extra={
            "company_id": company_id,
            "variant_id": variant_id,
            "tax_context_available": tax_context_available,
            "has_taxes_column": has_taxes,
            "used_fallback": used_fallback,
            "error": error,
        },
    )


def variant_tax_context(
    cur,
    company_id: int,
    variant_id: int,
    *,
    tax_audit: dict[str, int] | None = None,
    tax_log_sample_limit: int = 5,
) -> dict[str, Any]:
    """
    Contexto tributario por variant. Modo degradado absoluto: cualquier fallo SQL
    retorna defaults sin propagar excepción.
    """
    _bump_tax_audit(tax_audit, "calls")
    has_taxes = False
    used_fallback = False
    try:
        has_taxes = _product_column_exists(cur, "taxes")
        has_tax_factor = _product_column_exists(cur, "tax_factor")
        if has_taxes:
            _bump_tax_audit(tax_audit, "p_taxes_attempted")
        tax_factor_expr = (
            "COALESCE(NULLIF(p.tax_factor, 0), 1) AS tax_factor"
            if has_tax_factor
            else "1.0 AS tax_factor"
        )
        taxes_expr = "p.taxes" if has_taxes else "NULL::jsonb AS taxes"
        sql = f"""
            SELECT
                v.bsale_id AS variant_id,
                v.product_id,
                v.description AS variant_name,
                v.bar_code AS barcode,
                p.name AS product_name,
                {tax_factor_expr},
                {taxes_expr}
            FROM bsale.variants v
            LEFT JOIN bsale.products p
                ON p.company_id = v.company_id AND p.bsale_id = v.product_id
            WHERE v.company_id = %s AND v.bsale_id = %s
            LIMIT 1
            """
        cur.execute(sql, (company_id, variant_id))
        row = cur.fetchone()
        if not row:
            used_fallback = True
            _bump_tax_audit(tax_audit, "fallback")
            result = _default_tax_context()
            if int(tax_audit.get("_logged", 0) if tax_audit else 0) < tax_log_sample_limit:
                _log_tax_context_call(
                    company_id=company_id,
                    variant_id=variant_id,
                    tax_context_available=False,
                    has_taxes=has_taxes,
                    used_fallback=True,
                    error="no_row",
                )
                if tax_audit is not None:
                    tax_audit["_logged"] = int(tax_audit.get("_logged", 0)) + 1
            return result

        if not has_taxes:
            d = _row_dict(cur, row)
            used_fallback = True
            _bump_tax_audit(tax_audit, "fallback")
            result = _default_tax_context(
                product_id=d.get("product_id"),
                product_name=d.get("product_name"),
                variant_name=d.get("variant_name"),
                barcode=d.get("barcode"),
            )
            if int(tax_audit.get("_logged", 0) if tax_audit else 0) < tax_log_sample_limit:
                _log_tax_context_call(
                    company_id=company_id,
                    variant_id=variant_id,
                    tax_context_available=False,
                    has_taxes=False,
                    used_fallback=True,
                )
                if tax_audit is not None:
                    tax_audit["_logged"] = int(tax_audit.get("_logged", 0)) + 1
            return result

        d = _row_dict(cur, row)
        taxes = d.get("taxes")
        if isinstance(taxes, str):
            try:
                taxes = json.loads(taxes)
            except json.JSONDecodeError:
                taxes = None
        iva_rate = None
        if isinstance(taxes, list) and taxes:
            try:
                iva_rate = float(
                    taxes[0].get("percentage") or taxes[0].get("rate") or 0
                )
            except (TypeError, ValueError, AttributeError):
                iva_rate = None
        result = {
            "product_id": d.get("product_id"),
            "product_name": d.get("product_name"),
            "variant_name": d.get("variant_name"),
            "barcode": d.get("barcode"),
            "tax_factor": float(d.get("tax_factor") or 1),
            "iva_rate": iva_rate,
            "specific_taxes": taxes,
            "iva_amount": 0,
            "other_taxes": 0,
            "tax_context_available": True,
        }
        if int(tax_audit.get("_logged", 0) if tax_audit else 0) < tax_log_sample_limit:
            _log_tax_context_call(
                company_id=company_id,
                variant_id=variant_id,
                tax_context_available=True,
                has_taxes=True,
                used_fallback=False,
            )
            if tax_audit is not None:
                tax_audit["_logged"] = int(tax_audit.get("_logged", 0)) + 1
        return result
    except Exception as exc:
        _product_column_cache.clear()
        used_fallback = True
        _bump_tax_audit(tax_audit, "fallback")
        _bump_tax_audit(tax_audit, "errors")
        try:
            cur.connection.rollback()
        except Exception:
            pass
        err = f"{type(exc).__name__}: {exc}"
        logger.warning(
            "[COST_SYNC_TAX] degraded company_id=%s variant_id=%s error=%s",
            company_id,
            variant_id,
            err,
        )
        _log_tax_context_call(
            company_id=company_id,
            variant_id=variant_id,
            tax_context_available=False,
            has_taxes=has_taxes,
            used_fallback=True,
            error=err,
        )
        return _default_tax_context()


def upsert_variant_cost_snapshot(
    cur,
    *,
    company_id: int,
    variant_id: int,
    average_cost_net: float | None,
    average_cost_gross: float | None,
    tax_factor: float | None,
    iva_rate: float | None,
    specific_taxes: Any,
) -> None:
    cur.execute(
        """
        INSERT INTO bsale.variant_cost (
            company_id, variant_id, average_cost_net, last_update,
            average_cost_gross, tax_factor, iva_rate, specific_taxes, cost_source
        )
        VALUES (%s, %s, %s, NOW(), %s, %s, %s, %s, 'cost_receptions_sync')
        ON CONFLICT (company_id, variant_id) DO UPDATE
        SET average_cost_net = COALESCE(EXCLUDED.average_cost_net, bsale.variant_cost.average_cost_net),
            average_cost_gross = COALESCE(EXCLUDED.average_cost_gross, bsale.variant_cost.average_cost_gross),
            tax_factor = COALESCE(EXCLUDED.tax_factor, bsale.variant_cost.tax_factor),
            iva_rate = COALESCE(EXCLUDED.iva_rate, bsale.variant_cost.iva_rate),
            specific_taxes = COALESCE(EXCLUDED.specific_taxes, bsale.variant_cost.specific_taxes),
            cost_source = COALESCE(bsale.variant_cost.cost_source, EXCLUDED.cost_source),
            last_update = NOW()
        """,
        (
            company_id,
            variant_id,
            average_cost_net,
            average_cost_gross,
            tax_factor,
            iva_rate,
            Json(specific_taxes) if specific_taxes is not None else None,
        ),
    )


def list_offices(cur, company_id: int) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT DISTINCT office_id, office_name
        FROM {HISTORY}
        WHERE company_id = %s AND office_id IS NOT NULL
        ORDER BY office_name NULLS LAST
        """,
        (company_id,),
    )
    return [_row_dict(cur, r) for r in cur.fetchall()]


def dashboard_kpis(
    cur,
    company_id: int,
    *,
    office_id: int | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
) -> dict[str, Any]:
    filt, filt_params = _filter_clause(
        company_id=company_id,
        office_id=office_id,
        date_from=date_from,
        date_to=date_to,
        prefix="h",
    )
    cur.execute(
        f"""
        WITH scoped AS (
            SELECT h.*
            FROM {HISTORY} h
            WHERE {filt}
        )
        SELECT
            (SELECT COUNT(*)::int FROM bsale.variants v WHERE v.company_id = %s) AS variants_total,
            (SELECT COUNT(*)::int FROM bsale.variants v
             INNER JOIN bsale.variant_cost vc
                ON vc.company_id = v.company_id AND vc.variant_id = v.bsale_id
             WHERE v.company_id = %s
               AND vc.average_cost_net IS NOT NULL AND vc.average_cost_net > 0) AS with_cost,
            (SELECT COUNT(*)::int FROM bsale.variants v
             LEFT JOIN bsale.variant_cost vc
                ON vc.company_id = v.company_id AND vc.variant_id = v.bsale_id
             WHERE v.company_id = %s
               AND (vc.variant_id IS NULL OR vc.average_cost_net IS NULL)) AS without_cost,
            (SELECT COUNT(*)::int FROM bsale.variants v
             INNER JOIN bsale.variant_cost vc
                ON vc.company_id = v.company_id AND vc.variant_id = v.bsale_id
             WHERE v.company_id = %s AND vc.average_cost_net = 0) AS zero_cost,
            (SELECT COUNT(DISTINCT reception_id)::int FROM scoped
             WHERE admission_date >= NOW() - INTERVAL '24 hours') AS receptions_24h,
            (SELECT COUNT(*)::int FROM scoped
             WHERE variation_pct IS NOT NULL AND ABS(variation_pct) > 10) AS variation_gt_10,
            (SELECT COUNT(*)::int FROM scoped
             WHERE variation_pct IS NOT NULL AND ABS(variation_pct) > 20) AS variation_gt_20,
            (SELECT COUNT(DISTINCT reception_id)::int FROM scoped) AS receptions_processed,
            (SELECT COUNT(*)::int FROM scoped) AS lines_processed
        """,
        [*filt_params, company_id, company_id, company_id, company_id],
    )
    return _row_dict(cur, cur.fetchone())


def search_variants(
    cur,
    company_id: int,
    *,
    q: str,
    limit: int = 50,
) -> list[dict[str, Any]]:
    term = f"%{q.strip()}%"
    cur.execute(
        f"""
        SELECT DISTINCT ON (h.variant_id)
            h.variant_id,
            h.product_name,
            h.variant_name,
            h.barcode,
            h.company_name,
            h.average_cost,
            vc.average_cost_gross
        FROM {HISTORY} h
        LEFT JOIN bsale.variant_cost vc
            ON vc.company_id = h.company_id AND vc.variant_id = h.variant_id
        WHERE h.company_id = %s
          AND (
              h.barcode ILIKE %s
              OR COALESCE(h.product_name, '') ILIKE %s
              OR COALESCE(h.variant_name, '') ILIKE %s
          )
        ORDER BY h.variant_id, h.admission_date DESC
        LIMIT %s
        """,
        (company_id, term, term, term, limit),
    )
    return [_row_dict(cur, r) for r in cur.fetchall()]


def list_history_rows(
    cur,
    company_id: int,
    *,
    q: str | None = None,
    variant_id: int | None = None,
    office_id: int | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    limit: int = 200,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    where, params = _filter_clause(
        company_id=company_id,
        office_id=office_id,
        date_from=date_from,
        date_to=date_to,
    )
    extra = []
    if q and q.strip():
        term = f"%{q.strip()}%"
        extra.append(
            "(h.barcode ILIKE %s OR COALESCE(h.product_name,'') ILIKE %s OR COALESCE(h.variant_name,'') ILIKE %s)"
        )
        params.extend([term, term, term])
    if variant_id is not None:
        extra.append("h.variant_id = %s")
        params.append(variant_id)
    if extra:
        where = f"{where} AND " + " AND ".join(extra)

    cur.execute(f"SELECT COUNT(*) FROM {HISTORY} h WHERE {where}", params)
    total = int(cur.fetchone()[0])
    cur.execute(
        f"""
        SELECT
            h.company_id, h.company_name, h.office_id, h.office_name,
            h.variant_id, h.barcode, h.product_name, h.variant_name,
            h.reception_id, h.reception_detail_id, h.document, h.document_number,
            h.admission_date, h.quantity, h.cost_net, h.iva_amount, h.other_taxes,
            h.cost_bruto_erp, h.average_cost, h.variation_pct
        FROM {HISTORY} h
        WHERE {where}
        ORDER BY h.admission_date DESC, h.id DESC
        LIMIT %s OFFSET %s
        """,
        [*params, limit, offset],
    )
    return [_row_dict(cur, r) for r in cur.fetchall()], total


def list_receptions(
    cur,
    company_id: int,
    *,
    date_from: datetime | None,
    date_to: datetime | None,
    office_id: int | None,
    document_type: str | None,
    limit: int,
    offset: int,
) -> tuple[list[dict[str, Any]], int]:
    where, params = _filter_clause(
        company_id=company_id,
        office_id=office_id,
        date_from=date_from,
        date_to=date_to,
        prefix="h",
    )
    if document_type:
        where += " AND h.document ILIKE %s"
        params.append(document_type)

    cur.execute(
        f"""
        SELECT COUNT(*) FROM (
            SELECT h.reception_id
            FROM {HISTORY} h
            WHERE {where}
            GROUP BY h.reception_id
        ) t
        """,
        params,
    )
    total = int(cur.fetchone()[0])
    cur.execute(
        f"""
        SELECT
            h.reception_id,
            MIN(h.admission_date) AS admission_date,
            MIN(h.company_name) AS company_name,
            MIN(h.office_id) AS office_id,
            MIN(h.office_name) AS office_name,
            MIN(h.document) AS document,
            MIN(h.document_number) AS document_number,
            COUNT(DISTINCT h.variant_id)::int AS products_count,
            COALESCE(SUM(h.quantity), 0) AS total_quantity,
            COALESCE(SUM(h.cost_net * h.quantity), 0) AS total_cost_net,
            COALESCE(SUM(h.cost_bruto_erp * h.quantity), 0) AS total_cost_bruto
        FROM {HISTORY} h
        WHERE {where}
        GROUP BY h.reception_id
        ORDER BY MIN(h.admission_date) DESC, h.reception_id DESC
        LIMIT %s OFFSET %s
        """,
        [*params, limit, offset],
    )
    return [_row_dict(cur, r) for r in cur.fetchall()], total


def get_reception_detail(
    cur, company_id: int, reception_id: int
) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT
            reception_id,
            MIN(company_id) AS company_id,
            MIN(company_name) AS company_name,
            MIN(office_id) AS office_id,
            MIN(office_name) AS office_name,
            MIN(admission_date) AS admission_date,
            MIN(document) AS document,
            MIN(document_number) AS document_number,
            COUNT(DISTINCT variant_id)::int AS products_count,
            COALESCE(SUM(quantity), 0) AS total_quantity,
            COALESCE(SUM(cost_net * quantity), 0) AS total_cost_net,
            COALESCE(SUM(cost_bruto_erp * quantity), 0) AS total_cost_bruto
        FROM {HISTORY}
        WHERE company_id = %s AND reception_id = %s
        GROUP BY reception_id
        """,
        (company_id, reception_id),
    )
    header = cur.fetchone()
    if not header:
        return None
    h = _row_dict(cur, header)
    cur.execute(
        f"""
        SELECT
            reception_detail_id, variant_id, product_name, variant_name, barcode,
            quantity, cost_net, iva_amount, other_taxes, cost_bruto_erp,
            average_cost, variation_pct
        FROM {HISTORY}
        WHERE company_id = %s AND reception_id = %s
        ORDER BY id ASC
        """,
        (company_id, reception_id),
    )
    h["items"] = [_row_dict(cur, r) for r in cur.fetchall()]
    return h


def compare_offices_by_variant(
    cur, company_id: int, variant_id: int
) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT DISTINCT ON (h.office_id)
            h.office_id,
            h.office_name,
            h.cost_net,
            h.cost_bruto_erp,
            h.admission_date,
            h.reception_id
        FROM {HISTORY} h
        WHERE h.company_id = %s AND h.variant_id = %s AND h.office_id IS NOT NULL
        ORDER BY h.office_id, h.admission_date DESC, h.id DESC
        """,
        (company_id, variant_id),
    )
    offices = [_row_dict(cur, r) for r in cur.fetchall()]
    if not offices:
        return None
    cur.execute(
        f"""
        SELECT product_name, variant_name, barcode
        FROM {HISTORY}
        WHERE company_id = %s AND variant_id = %s
        ORDER BY admission_date DESC
        LIMIT 1
        """,
        (company_id, variant_id),
    )
    meta = cur.fetchone()
    product_name = variant_name = barcode = None
    if meta:
        d = _row_dict(cur, meta)
        product_name = d.get("product_name")
        variant_name = d.get("variant_name")
        barcode = d.get("barcode")
    costs = [float(o["cost_net"]) for o in offices if o.get("cost_net") is not None]
    min_c = min(costs) if costs else None
    max_c = max(costs) if costs else None
    return {
        "variant_id": variant_id,
        "product_name": product_name,
        "variant_name": variant_name,
        "barcode": barcode,
        "offices": offices,
        "min_cost_net": min_c,
        "max_cost_net": max_c,
    }


def search_compare_variants(
    cur, company_id: int, q: str, *, limit: int = 20
) -> list[dict[str, Any]]:
    term = f"%{q.strip()}%"
    cur.execute(
        f"""
        SELECT DISTINCT h.variant_id, h.product_name, h.variant_name, h.barcode
        FROM {HISTORY} h
        WHERE h.company_id = %s
          AND (
              h.barcode ILIKE %s
              OR COALESCE(h.product_name, '') ILIKE %s
              OR COALESCE(h.variant_name, '') ILIKE %s
          )
        ORDER BY h.product_name NULLS LAST, h.variant_name NULLS LAST
        LIMIT %s
        """,
        (company_id, term, term, term, limit),
    )
    return [_row_dict(cur, r) for r in cur.fetchall()]


def list_cost_alerts(
    cur,
    company_id: int,
    *,
    office_id: int | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    where_office = ""
    office_params: list[Any] = []
    if office_id is not None:
        where_office = " AND d.office_id = %s"
        office_params = [office_id]

    cur.execute(
        f"""
        WITH latest AS (
            SELECT DISTINCT ON (d.variant_id)
                d.variant_id,
                d.product_name,
                d.variant_name,
                d.barcode,
                d.office_id,
                d.office_name,
                d.cost_net,
                d.variation_pct,
                d.admission_date,
                d.reception_id
            FROM {HISTORY} d
            WHERE d.company_id = %s{where_office}
            ORDER BY d.variant_id, d.admission_date DESC
        ),
        branch_spread AS (
            SELECT variant_id,
                   MAX(cost_net) AS max_c,
                   MIN(cost_net) AS min_c
            FROM (
                SELECT DISTINCT ON (variant_id, office_id)
                    variant_id, office_id, cost_net
                FROM {HISTORY}
                WHERE company_id = %s AND office_id IS NOT NULL
                ORDER BY variant_id, office_id, admission_date DESC
            ) x
            GROUP BY variant_id
            HAVING COUNT(*) > 1
        ),
        suspicious AS (
            SELECT reception_id
            FROM {HISTORY}
            WHERE company_id = %s
            GROUP BY reception_id
            HAVING
                COUNT(*) FILTER (WHERE cost_net = 0) * 100.0 / NULLIF(COUNT(*), 0) >= 50
                OR AVG(ABS(COALESCE(variation_pct, 0))) >= 30
        )
        SELECT
            l.variant_id,
            l.product_name,
            l.variant_name,
            l.barcode,
            l.office_id,
            l.office_name,
            l.cost_net,
            l.variation_pct,
            l.admission_date,
            l.reception_id,
            vc.average_cost_net AS average_cost,
            CASE WHEN vc.variant_id IS NULL THEN TRUE ELSE FALSE END AS missing_cost,
            CASE WHEN l.reception_id IN (SELECT reception_id FROM suspicious) THEN TRUE ELSE FALSE END AS suspicious_reception,
            CASE
                WHEN bs.min_c > 0 THEN ROUND(((bs.max_c - bs.min_c) / bs.min_c) * 100.0, 2)
                ELSE NULL
            END AS cross_branch_spread
        FROM latest l
        LEFT JOIN bsale.variant_cost vc
            ON vc.company_id = %s AND vc.variant_id = l.variant_id
        LEFT JOIN branch_spread bs ON bs.variant_id = l.variant_id
        ORDER BY l.admission_date DESC NULLS LAST
        LIMIT %s
        """,
        [company_id, *office_params, company_id, company_id, company_id, limit],
    )
    return [_row_dict(cur, r) for r in cur.fetchall()]
