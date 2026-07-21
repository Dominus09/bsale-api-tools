"""DDL Distribuidora y estado de sync (``sync_process_cursor``, ``sync_state``, ``sync_logs``)."""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Solo líneas que son exclusivamente el marcador (no confundir con "-- +go texto..." en comentarios).
_STMT_SPLIT_GO = re.compile(r"^\s*--\s*\+go\s*$", re.MULTILINE)

_SQL_DIR = Path(__file__).resolve().parents[2] / "sql" / "distribuidora"


def _live_sync_debug_enabled() -> bool:
    return os.getenv("LIVE_SYNC_DEBUG", "").strip().lower() in ("1", "true", "yes")


def _sql_chunk_has_executable_sql(stmt: str) -> bool:
    """
    True si el fragmento tiene SQL ejecutable (no solo comentarios o whitespace).

    psycopg2 falla con ``can't execute an empty query`` si se envía solo ``-- comentario``.
    """
    for line in stmt.splitlines():
        s = line.strip()
        if not s:
            continue
        if s.startswith("--"):
            continue
        return True
    return False


def _run_sql_file(cur, name: str) -> None:
    path = _SQL_DIR / name
    if not path.is_file():
        raise FileNotFoundError(f"SQL no encontrado: {path}")
    text = path.read_text(encoding="utf-8")
    chunk_idx = 0
    for chunk in _STMT_SPLIT_GO.split(text):
        chunk_idx += 1
        stmt = chunk.strip()
        if not stmt:
            if _live_sync_debug_enabled():
                logger.info(
                    "[LIVE_SYNC_DEBUG] skip empty chunk file=%s idx=%s",
                    name,
                    chunk_idx,
                )
            continue
        if not _sql_chunk_has_executable_sql(stmt):
            if _live_sync_debug_enabled():
                preview = stmt.replace("\n", " ")[:120]
                logger.info(
                    "[LIVE_SYNC_DEBUG] skip comment-only chunk file=%s idx=%s preview=%r",
                    name,
                    chunk_idx,
                    preview,
                )
            continue
        if _live_sync_debug_enabled():
            preview = stmt.replace("\n", " ")[:200]
            logger.info(
                "[LIVE_SYNC_DEBUG] execute file=%s idx=%s len=%s preview=%r",
                name,
                chunk_idx,
                len(stmt),
                preview,
            )
        cur.execute(stmt)
    logger.info("SQL aplicado: %s", name)


# Orden versionado de DDL (única fuente aplicada por el runner de migraciones).
DISTRIBUIDORA_SCHEMA_FILES: tuple[str, ...] = (
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
    "014_document_probable_matches.sql",
    "015_v_purchase_document_status_full.sql",
    "016_system_config.sql",
    "017_trucks_fuel.sql",
    "018_trucks_real_consumption.sql",
    "019_logistics_cost_settings.sql",
    "020_ors_route_crew_costs.sql",
    "021_dispatch_plan.sql",
    "022_dispatch_plan_invoiced_view.sql",
    "023_dispatch_plan_identity.sql",
    "024_dispatch_plan_picking_snapshots.sql",
    "025_dispatch_plan_margin_and_snapshot.sql",
    "026_dispatch_plan_invoiced_view_perf.sql",
    "027_dispatch_plan_pickings.sql",
    "028_planning_rows_indexes.sql",
    "029_planning_rows_sort_index.sql",
    "030_purchase_document_status_cache.sql",
    "031_dispatch_plan_snapshot_views.sql",
    "032_route_operational_costs.sql",
    "033_diesel_price_default_1500.sql",
    "034_dispatch_plan_crew_cuadratura.sql",
    "035_dispatch_plan_cuadratura_v2.sql",
    "036_dispatch_plan_cuadratura_cash_count.sql",
    "037_dispatch_plan_load_batches.sql",
    "044_documents_source_sync_metadata.sql",
)

_ENSURE_SCHEMA_NOOP_WARNED = False


def apply_distribuidora_migrations(cur) -> list[str]:
    """Aplica el DDL versionado de ``backend/sql/distribuidora/``.

    SOLO debe invocarse desde el runner explícito
    ``python -m backend.jobs.apply_distribuidora_schema`` (deploy/migración).
    No llamar desde syncs, endpoints HTTP ni jobs recurrentes: los
    ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS`` piden AccessExclusiveLock y
    encolan los SELECT de planning-rows.
    """
    applied: list[str] = []
    for fn in DISTRIBUIDORA_SCHEMA_FILES:
        _run_sql_file(cur, fn)
        applied.append(fn)
    logger.info(
        "apply_distribuidora_migrations: %s archivos SQL aplicados",
        len(applied),
    )
    return applied


def ensure_distribuidora_schema(cur) -> None:
    """NO-OP deliberado (anti-bloqueo planning-rows).

    Históricamente reaplicaba todo el DDL en cada sync. Eso provocaba
    ``ALTER TABLE distribuidora.documents`` concurrente con lecturas.
    El DDL vive solo en ``apply_distribuidora_migrations``.
    """
    global _ENSURE_SCHEMA_NOOP_WARNED
    if not _ENSURE_SCHEMA_NOOP_WARNED:
        _ENSURE_SCHEMA_NOOP_WARNED = True
        logger.warning(
            "ensure_distribuidora_schema es NO-OP: el DDL no se ejecuta en sync/HTTP. "
            "Usar: python -m backend.jobs.apply_distribuidora_schema"
        )


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
