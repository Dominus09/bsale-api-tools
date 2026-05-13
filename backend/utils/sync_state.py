"""
Estado operacional de sync (watermark, ventanas, overlap) en ``distribuidora.sync_state``.

Los cursores legados por ``process_name`` viven en ``distribuidora.sync_process_cursor``
(``sync_repo.get_last_sync`` / ``set_sync_state``). El historial por corrida sigue en ``sync_status``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

MODE_INCREMENTAL = "incremental"
MODE_BACKFILL = "backfill"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def get_sync_state(
    cur,
    *,
    sync_type: str,
    mode: str,
    office_id: int = 1,
) -> dict[str, Any] | None:
    """
    Devuelve una fila de ``distribuidora.sync_state`` o ``None`` si no existe.
    """
    cur.execute(
        """
        SELECT id, sync_type, mode, office_id, last_success_at, last_window_from, last_window_to,
               last_watermark, overlap_seconds, overlap_days, status, items_processed,
               error_summary, updated_at
        FROM distribuidora.sync_state
        WHERE sync_type = %s AND mode = %s AND office_id = %s
        """,
        (sync_type, mode, office_id),
    )
    r = cur.fetchone()
    if not r:
        return None
    return {
        "id": r[0],
        "sync_type": r[1],
        "mode": r[2],
        "office_id": r[3],
        "last_success_at": r[4],
        "last_window_from": r[5],
        "last_window_to": r[6],
        "last_watermark": r[7],
        "overlap_seconds": r[8],
        "overlap_days": r[9],
        "status": r[10],
        "items_processed": r[11],
        "error_summary": r[12],
        "updated_at": r[13],
    }


def update_sync_state_success(
    cur,
    *,
    sync_type: str,
    mode: str,
    office_id: int = 1,
    last_window_from: datetime | None = None,
    last_window_to: datetime | None = None,
    last_watermark: datetime | None = None,
    overlap_seconds: int | None = None,
    overlap_days: int | None = None,
    items_processed: int = 0,
    status: str = "success",
    last_success_at: datetime | None = None,
) -> None:
    """
    Inserta o actualiza la fila (``sync_type``, ``mode``, ``office_id``) tras una corrida exitosa.

    Limpia ``error_summary`` y fija ``last_success_at`` (por defecto ahora UTC).
    """
    ts = last_success_at or _utc_now()
    cur.execute(
        """
        INSERT INTO distribuidora.sync_state (
            sync_type, mode, office_id, last_success_at, last_window_from, last_window_to,
            last_watermark, overlap_seconds, overlap_days, status, items_processed, error_summary,
            updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, NOW()
        )
        ON CONFLICT (sync_type, mode, office_id) DO UPDATE SET
            last_success_at = EXCLUDED.last_success_at,
            last_window_from = EXCLUDED.last_window_from,
            last_window_to = EXCLUDED.last_window_to,
            last_watermark = EXCLUDED.last_watermark,
            overlap_seconds = EXCLUDED.overlap_seconds,
            overlap_days = EXCLUDED.overlap_days,
            status = EXCLUDED.status,
            items_processed = EXCLUDED.items_processed,
            error_summary = NULL,
            updated_at = NOW()
        """,
        (
            sync_type,
            mode,
            office_id,
            ts,
            last_window_from,
            last_window_to,
            last_watermark,
            overlap_seconds,
            overlap_days,
            status,
            int(items_processed),
        ),
    )


def update_sync_state_error(
    cur,
    *,
    sync_type: str,
    mode: str,
    office_id: int = 1,
    error_summary: str,
    status: str = "error",
    items_processed: int | None = None,
) -> None:
    """
    Registra fallo sin borrar ``last_success_at`` / watermarks previos.
    Si la fila no existe, la crea con el error (útil para primer arranque fallido).
    """
    summary = (error_summary or "").strip()[:8000]
    if items_processed is None:
        cur.execute(
            """
            INSERT INTO distribuidora.sync_state (
                sync_type, mode, office_id, status, error_summary, updated_at
            ) VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (sync_type, mode, office_id) DO UPDATE SET
                status = EXCLUDED.status,
                error_summary = EXCLUDED.error_summary,
                updated_at = NOW()
            """,
            (sync_type, mode, office_id, status, summary or None),
        )
    else:
        cur.execute(
            """
            INSERT INTO distribuidora.sync_state (
                sync_type, mode, office_id, status, items_processed, error_summary, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (sync_type, mode, office_id) DO UPDATE SET
                status = EXCLUDED.status,
                items_processed = EXCLUDED.items_processed,
                error_summary = EXCLUDED.error_summary,
                updated_at = NOW()
            """,
            (sync_type, mode, office_id, status, int(items_processed), summary or None),
        )
