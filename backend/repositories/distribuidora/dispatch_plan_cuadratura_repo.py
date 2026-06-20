"""Persistencia cuadratura operacional por plan (v1 legacy + v2 documental)."""

from __future__ import annotations

import json
from typing import Any

from backend.utils.dispatch_plan_cuadratura_v2 import MEDIOS_PAGO


def _table_exists(cur) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'distribuidora'
          AND table_name = 'dispatch_plan_cuadratura'
        """
    )
    return cur.fetchone() is not None


def _history_table_exists(cur) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'distribuidora'
          AND table_name = 'dispatch_plan_cuadratura_history'
        """
    )
    return cur.fetchone() is not None


def _column_exists(cur, column: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'distribuidora'
          AND table_name = 'dispatch_plan_cuadratura'
          AND column_name = %s
        """,
        (column,),
    )
    return cur.fetchone() is not None


def _parse_json(val: Any, default: Any):
    if val is None:
        return default
    if isinstance(val, (list, dict)):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except json.JSONDecodeError:
            return default
    return default


def get_cuadratura_row(cur, plan_id: int) -> dict[str, Any] | None:
    if not _table_exists(cur):
        return None
    has_v2 = _column_exists(cur, "schema_version")
    if has_v2:
        cur.execute(
            """
            SELECT transferencia_clp, efectivo_clp, cheque_clp, debito_clp,
                   observacion, credit_notes, not_loaded, updated_at,
                   schema_version, status, documents, credit_notes_v2,
                   not_loaded_v2, picking_id, picking_version,
                   closed_at, closed_by, resultado_cache
            FROM distribuidora.dispatch_plan_cuadratura
            WHERE dispatch_plan_id = %s
            """,
            (int(plan_id),),
        )
    else:
        cur.execute(
            """
            SELECT transferencia_clp, efectivo_clp, cheque_clp, debito_clp,
                   observacion, credit_notes, not_loaded, updated_at
            FROM distribuidora.dispatch_plan_cuadratura
            WHERE dispatch_plan_id = %s
            """,
            (int(plan_id),),
        )
    row = cur.fetchone()
    if not row:
        return None
    cols = [d[0] for d in cur.description]
    out = dict(zip(cols, row))
    for key in ("credit_notes", "not_loaded", "documents", "credit_notes_v2", "not_loaded_v2"):
        if key in out:
            out[key] = _parse_json(out.get(key), [])
    if "resultado_cache" in out:
        out["resultado_cache"] = _parse_json(out.get("resultado_cache"), None)
    return out


def upsert_cuadratura(
    cur,
    *,
    plan_id: int,
    transferencia_clp: int,
    efectivo_clp: int,
    cheque_clp: int,
    debito_clp: int,
    observacion: str | None,
    credit_notes: list[dict[str, Any]],
    not_loaded: list[dict[str, Any]],
) -> None:
    if not _table_exists(cur):
        return
    cur.execute(
        """
        INSERT INTO distribuidora.dispatch_plan_cuadratura (
            dispatch_plan_id,
            transferencia_clp, efectivo_clp, cheque_clp, debito_clp,
            observacion, credit_notes, not_loaded, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, NOW())
        ON CONFLICT (dispatch_plan_id) DO UPDATE
        SET transferencia_clp = EXCLUDED.transferencia_clp,
            efectivo_clp = EXCLUDED.efectivo_clp,
            cheque_clp = EXCLUDED.cheque_clp,
            debito_clp = EXCLUDED.debito_clp,
            observacion = EXCLUDED.observacion,
            credit_notes = EXCLUDED.credit_notes,
            not_loaded = EXCLUDED.not_loaded,
            updated_at = NOW()
        """,
        (
            int(plan_id),
            max(0, int(transferencia_clp)),
            max(0, int(efectivo_clp)),
            max(0, int(cheque_clp)),
            max(0, int(debito_clp)),
            (observacion or "").strip() or None,
            json.dumps(credit_notes or []),
            json.dumps(not_loaded or []),
        ),
    )


def upsert_cuadratura_v2(
    cur,
    *,
    plan_id: int,
    observacion: str | None,
    documents: list[dict[str, Any]],
    credit_notes_v2: list[dict[str, Any]],
    not_loaded_v2: list[dict[str, Any]],
    picking_id: int | None,
    picking_version: int | None,
    status: str,
    resultado_cache: dict[str, Any],
    closed_at: Any = None,
    closed_by: str | None = None,
) -> None:
    if not _table_exists(cur) or not _column_exists(cur, "schema_version"):
        return
    cur.execute(
        """
        INSERT INTO distribuidora.dispatch_plan_cuadratura (
            dispatch_plan_id,
            schema_version, status,
            documents, credit_notes_v2, not_loaded_v2,
            picking_id, picking_version,
            observacion, resultado_cache,
            closed_at, closed_by,
            transferencia_clp, efectivo_clp, cheque_clp, debito_clp,
            credit_notes, not_loaded,
            updated_at
        )
        VALUES (
            %s, 2, %s,
            %s::jsonb, %s::jsonb, %s::jsonb,
            %s, %s,
            %s, %s::jsonb,
            %s, %s,
            0, 0, 0, 0,
            '[]'::jsonb, '[]'::jsonb,
            NOW()
        )
        ON CONFLICT (dispatch_plan_id) DO UPDATE
        SET schema_version = 2,
            status = EXCLUDED.status,
            documents = EXCLUDED.documents,
            credit_notes_v2 = EXCLUDED.credit_notes_v2,
            not_loaded_v2 = EXCLUDED.not_loaded_v2,
            picking_id = EXCLUDED.picking_id,
            picking_version = EXCLUDED.picking_version,
            observacion = EXCLUDED.observacion,
            resultado_cache = EXCLUDED.resultado_cache,
            closed_at = EXCLUDED.closed_at,
            closed_by = EXCLUDED.closed_by,
            updated_at = NOW()
        """,
        (
            int(plan_id),
            status,
            json.dumps(documents or []),
            json.dumps(credit_notes_v2 or []),
            json.dumps(not_loaded_v2 or []),
            picking_id,
            picking_version,
            (observacion or "").strip() or None,
            json.dumps(resultado_cache or {}),
            closed_at,
            closed_by,
        ),
    )


def insert_cuadratura_history(
    cur,
    *,
    plan_id: int,
    version: int,
    status: str,
    snapshot: dict[str, Any],
    closed_by: str | None,
    observacion: str | None,
    diferencia_clp: int,
    diferencia_status: str,
) -> None:
    if not _history_table_exists(cur):
        return
    cur.execute(
        """
        INSERT INTO distribuidora.dispatch_plan_cuadratura_history (
            dispatch_plan_id, version, schema_version, status, snapshot,
            closed_by, observacion, diferencia_clp, diferencia_status
        )
        VALUES (%s, %s, 2, %s, %s::jsonb, %s, %s, %s, %s)
        """,
        (
            int(plan_id),
            int(version),
            status,
            json.dumps(snapshot or {}),
            closed_by,
            (observacion or "").strip() or None,
            int(diferencia_clp),
            diferencia_status,
        ),
    )


def next_history_version(cur, plan_id: int) -> int:
    if not _history_table_exists(cur):
        return 1
    cur.execute(
        """
        SELECT COALESCE(MAX(version), 0) + 1
        FROM distribuidora.dispatch_plan_cuadratura_history
        WHERE dispatch_plan_id = %s
        """,
        (int(plan_id),),
    )
    return int(cur.fetchone()[0])


def list_cuadratura_history(cur, plan_id: int, *, limit: int = 20) -> list[dict[str, Any]]:
    if not _history_table_exists(cur):
        return []
    cur.execute(
        """
        SELECT id, version, status, closed_at, closed_by, observacion,
               diferencia_clp, diferencia_status, created_at
        FROM distribuidora.dispatch_plan_cuadratura_history
        WHERE dispatch_plan_id = %s
        ORDER BY closed_at DESC, id DESC
        LIMIT %s
        """,
        (int(plan_id), int(limit)),
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def list_cuadraturas(
    cur,
    *,
    status_filter: str = "all",
    search: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    has_v2 = _table_exists(cur) and _column_exists(cur, "status")
    params: list[Any] = []
    where = [
        """
        EXISTS (
            SELECT 1
            FROM distribuidora.dispatch_plan_pickings pk
            WHERE pk.dispatch_plan_id = dp.id
              AND pk.is_current = TRUE
        )
        """
    ]
    if search and search.strip():
        q = f"%{search.strip()}%"
        where.append(
            """
            (
                dp.planning_code ILIKE %s
                OR dp.planning_name ILIKE %s
                OR dp.truck_name ILIKE %s
                OR dp.route_name ILIKE %s
                OR COALESCE(dp.driver_name, '') ILIKE %s
                OR dp.planning_date::text ILIKE %s
            )
            """
        )
        params.extend([q, q, q, q, q, q])

    status_expr = "COALESCE(c.status, 'pending')" if has_v2 else "'pending'"
    if has_v2 and status_filter == "pending":
        where.append(f"({status_expr} = 'pending' AND c.closed_at IS NULL)")
    elif has_v2 and status_filter == "squared":
        where.append(f"{status_expr} = 'squared'")
    elif has_v2 and status_filter == "difference":
        where.append(f"{status_expr} IN ('in_review', 'difference')")
    elif has_v2 and status_filter == "with_diff":
        where.append(
            f"({status_expr} IN ('in_review', 'difference') OR "
            f"(c.resultado_cache->>'diferencia_clp')::int IS DISTINCT FROM 0)"
        )

    sql = f"""
        SELECT
            dp.id AS dispatch_plan_id,
            dp.planning_code,
            dp.planning_name,
            dp.planning_date,
            dp.truck_name,
            dp.route_name,
            dp.driver_name,
            dp.status AS plan_status,
            pk.document_total_clp AS venta_picking_clp,
            pk.id AS picking_id,
            pk.version AS picking_version,
            {status_expr} AS cuadratura_status,
            c.resultado_cache,
            c.closed_at,
            c.updated_at AS cuadratura_updated_at
        FROM distribuidora.dispatch_plan dp
        INNER JOIN distribuidora.dispatch_plan_pickings pk
            ON pk.dispatch_plan_id = dp.id AND pk.is_current = TRUE
        LEFT JOIN distribuidora.dispatch_plan_cuadratura c
            ON c.dispatch_plan_id = dp.id
        WHERE {" AND ".join(where)}
        ORDER BY dp.planning_date DESC, dp.id DESC
        LIMIT %s OFFSET %s
    """
    params.extend([int(limit), int(offset)])
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    rows = []
    for row in cur.fetchall():
        item = dict(zip(cols, row))
        cache = _parse_json(item.pop("resultado_cache", None), {}) or {}
        item["total_recaudado_clp"] = int(cache.get("total_recaudado_clp") or 0)
        item["diferencia_clp"] = int(cache.get("diferencia_clp") or 0)
        item["diferencia_status"] = cache.get("diferencia_status")
        rows.append(item)
    return rows


def list_medios_pago() -> list[str]:
    return list(MEDIOS_PAGO)
