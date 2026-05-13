"""DDL Distribuidora y estado de sync (``sync_process_cursor``, ``sync_state``, ``sync_logs``)."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Solo líneas que son exclusivamente el marcador (no confundir con "-- +go texto..." en comentarios).
_STMT_SPLIT_GO = re.compile(r"^\s*--\s*\+go\s*$", re.MULTILINE)

_SQL_DIR = Path(__file__).resolve().parents[2] / "sql" / "distribuidora"


def _run_sql_file(cur, name: str) -> None:
    path = _SQL_DIR / name
    if not path.is_file():
        raise FileNotFoundError(f"SQL no encontrado: {path}")
    text = path.read_text(encoding="utf-8")
    for chunk in _STMT_SPLIT_GO.split(text):
        stmt = chunk.strip()
        if not stmt:
            continue
        cur.execute(stmt)
    logger.info("SQL aplicado: %s", name)


def ensure_distribuidora_schema(cur) -> None:
    for fn in (
        "001_schema.sql",
        "002_indexes.sql",
        "003_views.sql",
        "004_route_planning_summary.sql",
        "005_route_picking.sql",
        "006_route_planning_seller.sql",
        "007_document_related_sync_status_views.sql",
        "008_documents_fk_on_update_cascade.sql",
        "009_v_sales_with_credit_notes.sql",
        "010_document_sellers.sql",
        "011_v_sales_document_sellers.sql",
        "012_trucks.sql",
        "013_operational_sync_state.sql",
    ):
        _run_sql_file(cur, fn)


def get_last_sync(cur, process_name: str) -> Any:
    cur.execute(
        """
        SELECT last_sync
        FROM distribuidora.sync_process_cursor
        WHERE process_name = %s
        """,
        (process_name,),
    )
    row = cur.fetchone()
    return row[0] if row else None


def set_sync_state(
    cur,
    *,
    process_name: str,
    last_sync: Any = None,
    last_status: str | None = None,
    last_message: str | None = None,
) -> None:
    cur.execute(
        """
        UPDATE distribuidora.sync_process_cursor
        SET last_sync = COALESCE(%s, last_sync),
            last_status = COALESCE(%s, last_status),
            last_message = COALESCE(%s, last_message),
            updated_at = NOW()
        WHERE process_name = %s
        """,
        (last_sync, last_status, last_message, process_name),
    )


def ensure_sync_state_row(
    cur,
    process_name: str,
    *,
    default_last_sync: Any = None,
) -> None:
    """
    Garantiza una fila en ``sync_process_cursor`` para ``process_name`` (``set_sync_state`` solo hace UPDATE).
    """
    if default_last_sync is None:
        default_last_sync = "2000-01-01 00:00:00+00"
    cur.execute(
        """
        INSERT INTO distribuidora.sync_process_cursor (process_name, last_sync, last_status)
        VALUES (%s, %s::timestamptz, NULL)
        ON CONFLICT (process_name) DO NOTHING
        """,
        (process_name, default_last_sync),
    )


def start_sync_log(cur, process_name: str) -> int:
    cur.execute(
        """
        INSERT INTO distribuidora.sync_logs (process_name, started_at, status)
        VALUES (%s, NOW(), 'running')
        RETURNING id
        """,
        (process_name,),
    )
    return int(cur.fetchone()[0])


def insert_sync_status_row(
    cur,
    *,
    sync_type: str,
    records_processed: int,
    status: str,
) -> None:
    """Auditoría desacoplada de corridas (documentos, detalles, related, ventas/órdenes)."""
    cur.execute(
        """
        INSERT INTO distribuidora.sync_status (sync_type, last_run, records_processed, status)
        VALUES (%s, NOW(), %s, %s)
        """,
        (sync_type, int(records_processed), status),
    )


def finish_sync_log(
    cur,
    log_id: int,
    *,
    status: str,
    stats: dict[str, Any],
    message: str | None = None,
) -> None:
    cur.execute(
        """
        UPDATE distribuidora.sync_logs
        SET finished_at = NOW(),
            status = %s,
            documents_processed = %s,
            documents_inserted = %s,
            documents_updated = %s,
            details_inserted = %s,
            attributes_inserted = %s,
            references_inserted = %s,
            message = %s
        WHERE id = %s
        """,
        (
            status,
            stats.get("documents_processed", 0),
            stats.get("documents_inserted", 0),
            stats.get("documents_updated", 0),
            stats.get("details_inserted", 0),
            stats.get("attributes_inserted", 0),
            stats.get("references_inserted", 0),
            message,
            log_id,
        ),
    )
