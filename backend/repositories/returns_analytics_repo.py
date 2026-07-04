"""Persistencia devoluciones Bsale — bsale.returns / return_details."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from psycopg2.extras import Json

logger = logging.getLogger(__name__)

RETURNS = "bsale.returns"
RETURN_DETAILS = "bsale.return_details"
SYNC_STATE = "bsale.returns_sync_state"
SYNC_RUNS = "bsale.returns_sync"


def ensure_returns_schema(cur) -> None:
    sql_path = Path(__file__).resolve().parents[1] / "sql" / "044_bsale_returns.sql"
    if not sql_path.is_file():
        logger.warning("returns schema file missing: %s", sql_path)
        return
    raw = sql_path.read_text(encoding="utf-8")
    for chunk in raw.split("-- +go"):
        stmt = chunk.strip()
        if stmt and not stmt.startswith("--"):
            cur.execute(stmt)


def get_sync_state(cur, *, company_id: int, office_id: int) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT company_id, office_id, last_return_ts, last_sync_at, records_total
        FROM {SYNC_STATE}
        WHERE company_id = %s AND office_id = %s
        """,
        (company_id, office_id),
    )
    row = cur.fetchone()
    if not row:
        return None
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


def upsert_sync_state(
    cur,
    *,
    company_id: int,
    office_id: int,
    last_return_ts: int | None,
    records_delta: int,
) -> None:
    cur.execute(
        f"""
        INSERT INTO {SYNC_STATE} (company_id, office_id, last_return_ts, last_sync_at, records_total)
        VALUES (%s, %s, %s, NOW(), %s)
        ON CONFLICT (company_id, office_id) DO UPDATE SET
            last_return_ts = GREATEST({SYNC_STATE}.last_return_ts, EXCLUDED.last_return_ts),
            last_sync_at = NOW(),
            records_total = {SYNC_STATE}.records_total + EXCLUDED.records_total
        """,
        (company_id, office_id, last_return_ts, records_delta),
    )


def _row_dict(cur, row) -> dict[str, Any]:
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


def create_sync_run(
    cur,
    *,
    company_id: int,
    office_id: int,
    sync_type: str,
    date_from,
    date_to,
) -> int:
    cur.execute(
        f"""
        INSERT INTO {SYNC_RUNS} (
            company_id, office_id, sync_type, date_from, date_to, status
        ) VALUES (%s, %s, %s, %s, %s, 'running')
        RETURNING id
        """,
        (company_id, office_id, sync_type, date_from, date_to),
    )
    return int(cur.fetchone()[0])


def update_sync_run_progress(
    cur,
    sync_id: int,
    *,
    pages_processed: int,
    records_processed: int,
    last_return_date: datetime | None,
    last_return_id: int | None,
) -> None:
    cur.execute(
        f"""
        UPDATE {SYNC_RUNS}
        SET pages_processed = %s,
            records_processed = %s,
            last_return_date = %s,
            last_return_id = %s
        WHERE id = %s
        """,
        (pages_processed, records_processed, last_return_date, last_return_id, sync_id),
    )


def finish_sync_run(
    cur,
    sync_id: int,
    *,
    status: str,
    duration_ms: int,
    error_message: str | None = None,
) -> None:
    cur.execute(
        f"""
        UPDATE {SYNC_RUNS}
        SET status = %s,
            finished_at = NOW(),
            duration_ms = %s,
            error_message = %s
        WHERE id = %s
        """,
        (status, duration_ms, error_message, sync_id),
    )


def get_completed_history_sync(
    cur,
    *,
    company_id: int,
    office_id: int,
    date_from,
    date_to,
) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT *
        FROM {SYNC_RUNS}
        WHERE company_id = %s AND office_id = %s
          AND sync_type = 'history'
          AND status = 'completed'
          AND date_from = %s AND date_to = %s
        ORDER BY finished_at DESC NULLS LAST
        LIMIT 1
        """,
        (company_id, office_id, date_from, date_to),
    )
    row = cur.fetchone()
    return _row_dict(cur, row) if row else None


def get_resumable_history_sync(
    cur,
    *,
    company_id: int,
    office_id: int,
    date_from,
    date_to,
) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT *
        FROM {SYNC_RUNS}
        WHERE company_id = %s AND office_id = %s
          AND sync_type = 'history'
          AND status IN ('running', 'failed')
          AND date_from = %s AND date_to = %s
        ORDER BY started_at DESC
        LIMIT 1
        """,
        (company_id, office_id, date_from, date_to),
    )
    row = cur.fetchone()
    return _row_dict(cur, row) if row else None


def list_sync_runs(
    cur,
    *,
    company_id: int,
    office_id: int,
    limit: int = 10,
) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT *
        FROM {SYNC_RUNS}
        WHERE company_id = %s AND office_id = %s
        ORDER BY started_at DESC
        LIMIT %s
        """,
        (company_id, office_id, limit),
    )
    return [_row_dict(cur, r) for r in cur.fetchall()]


def upsert_return(cur, row: dict[str, Any]) -> None:
    cur.execute(
        f"""
        INSERT INTO {RETURNS} (
            company_id, office_id, bsale_id, code, return_date, motive, return_type,
            amount, price_adjustment, edit_texts,
            reference_document_id, reference_document_number, reference_document_type_id,
            credit_note_id, credit_note_number,
            client_id, client_name, seller_id, seller_name, municipality,
            credit_note_emission, reference_emission, raw_data, synced_at, updated_at
        ) VALUES (
            %(company_id)s, %(office_id)s, %(bsale_id)s, %(code)s, %(return_date)s,
            %(motive)s, %(return_type)s, %(amount)s, %(price_adjustment)s, %(edit_texts)s,
            %(reference_document_id)s, %(reference_document_number)s, %(reference_document_type_id)s,
            %(credit_note_id)s, %(credit_note_number)s,
            %(client_id)s, %(client_name)s, %(seller_id)s, %(seller_name)s, %(municipality)s,
            %(credit_note_emission)s, %(reference_emission)s, %(raw_data)s, NOW(), NOW()
        )
        ON CONFLICT (company_id, bsale_id) DO UPDATE SET
            code = EXCLUDED.code,
            return_date = EXCLUDED.return_date,
            motive = EXCLUDED.motive,
            return_type = EXCLUDED.return_type,
            amount = EXCLUDED.amount,
            price_adjustment = EXCLUDED.price_adjustment,
            edit_texts = EXCLUDED.edit_texts,
            reference_document_id = EXCLUDED.reference_document_id,
            reference_document_number = EXCLUDED.reference_document_number,
            reference_document_type_id = EXCLUDED.reference_document_type_id,
            credit_note_id = EXCLUDED.credit_note_id,
            credit_note_number = EXCLUDED.credit_note_number,
            client_id = EXCLUDED.client_id,
            client_name = EXCLUDED.client_name,
            seller_id = EXCLUDED.seller_id,
            seller_name = EXCLUDED.seller_name,
            municipality = EXCLUDED.municipality,
            credit_note_emission = EXCLUDED.credit_note_emission,
            reference_emission = EXCLUDED.reference_emission,
            raw_data = EXCLUDED.raw_data,
            synced_at = NOW(),
            updated_at = NOW()
        """,
        {**row, "raw_data": Json(row.get("raw_data") or {})},
    )


def replace_return_details(
    cur,
    *,
    company_id: int,
    return_id: int,
    details: list[dict[str, Any]],
) -> None:
    cur.execute(
        f"DELETE FROM {RETURN_DETAILS} WHERE company_id = %s AND return_id = %s",
        (company_id, return_id),
    )
    for d in details:
        cur.execute(
            f"""
            INSERT INTO {RETURN_DETAILS} (
                company_id, return_id, bsale_detail_id, document_detail_id,
                variant_id, product_name, variant_description,
                quantity, unit_value, total_amount, raw_data, synced_at
            ) VALUES (
                %(company_id)s, %(return_id)s, %(bsale_detail_id)s, %(document_detail_id)s,
                %(variant_id)s, %(product_name)s, %(variant_description)s,
                %(quantity)s, %(unit_value)s, %(total_amount)s, %(raw_data)s, NOW()
            )
            ON CONFLICT (company_id, return_id, bsale_detail_id) DO UPDATE SET
                document_detail_id = EXCLUDED.document_detail_id,
                variant_id = EXCLUDED.variant_id,
                product_name = EXCLUDED.product_name,
                variant_description = EXCLUDED.variant_description,
                quantity = EXCLUDED.quantity,
                unit_value = EXCLUDED.unit_value,
                total_amount = EXCLUDED.total_amount,
                raw_data = EXCLUDED.raw_data,
                synced_at = NOW()
            """,
            {**d, "raw_data": Json(d.get("raw_data") or {})},
        )


def _period_filter(
    date_from: datetime | None,
    date_to: datetime | None,
    *,
    alias: str = "r",
) -> tuple[str, list[Any]]:
    parts: list[str] = []
    params: list[Any] = []
    if date_from:
        parts.append(f"{alias}.return_date >= %s")
        params.append(date_from)
    if date_to:
        parts.append(f"{alias}.return_date <= %s")
        params.append(date_to)
    sql = (" AND " + " AND ".join(parts)) if parts else ""
    return sql, params
