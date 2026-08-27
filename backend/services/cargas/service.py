"""Servicio de Cargas: importación, búsqueda, certificación y auditoría."""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from backend.db import get_connection
from backend.services.cargas.parse_common import ParsedLoadPreview
from backend.services.cargas.parse_excel import PickingParseError, parse_picking_excel
from backend.services.cargas.parse_pdf import parse_picking_pdf
from backend.services.cargas.sec import (
    boxes_and_loose_from_units,
    normalize_search_text,
    units_from_boxes_and_loose,
)

logger = logging.getLogger(__name__)

CANCELABLE_STATUSES = frozenset({"pending", "in_progress"})
CERTIFIABLE_STATUSES = frozenset({"pending", "in_progress", "completed"})


def compute_file_hash(data: bytes) -> str:
    """SHA-256 hex de los bytes originales del archivo."""
    return hashlib.sha256(data).hexdigest()


def _f(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def _row_dict(cur, row) -> dict[str, Any]:
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


def _serialize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "isoformat") and not isinstance(value, str):
        try:
            return value.isoformat()
        except Exception:
            return value
    return value


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    out = {k: _serialize(v) for k, v in row.items()}
    if "requested_units" in out and "certified_units" in out:
        req = float(out.get("requested_units") or 0)
        cert = float(out.get("certified_units") or 0)
        out["remaining_units"] = round(req - cert, 3)
        sec = out.get("sec")
        out["requested_boxes"], out["requested_loose"] = boxes_and_loose_from_units(
            req, int(sec) if sec else None
        )
        out["certified_boxes"], out["certified_loose"] = boxes_and_loose_from_units(
            cert, int(sec) if sec else None
        )
        rem = out["remaining_units"]
        out["remaining_boxes"], out["remaining_loose"] = boxes_and_loose_from_units(
            rem if rem > 0 else 0, int(sec) if sec else None
        )
    return out


def _item_status(requested: float, certified: float, *, has_open_issue: bool = False) -> str:
    if has_open_issue:
        return "issue"
    if certified <= 0:
        return "pending"
    if certified < requested - 1e-9:
        return "partial"
    if abs(certified - requested) <= 1e-9:
        return "complete"
    return "excess"


def _parse_file(*, data: bytes, filename: str) -> ParsedLoadPreview:
    name = (filename or "").lower()
    if name.endswith(".pdf"):
        preview = parse_picking_pdf(data=data, filename=filename)
    elif name.endswith((".xlsx", ".xls")):
        preview = parse_picking_excel(data=data, filename=filename)
    else:
        raise PickingParseError("Formato no soportado. Use .xlsx, .xls o .pdf")
    preview.file_hash = compute_file_hash(data)
    return preview


def _insert_status_event(
    cur,
    *,
    load_id: int,
    from_status: str | None,
    to_status: str,
    user_email: str,
    reason: str | None,
) -> None:
    cur.execute(
        """
        INSERT INTO distribuidora.load_status_events (
            load_id, from_status, to_status, user_email, reason
        ) VALUES (%s, %s, %s, %s, %s)
        """,
        (load_id, from_status, to_status, user_email, reason),
    )


def preview_import(*, data: bytes, filename: str) -> dict[str, Any]:
    preview = _parse_file(data=data, filename=filename)
    return preview.to_dict()


def confirm_import(
    *,
    data: bytes,
    filename: str,
    user_email: str,
    picking_number_override: str | None = None,
    expected_file_hash: str | None = None,
) -> dict[str, Any]:
    """
    Re-parsea y valida el archivo definitivo.

    Si ``expected_file_hash`` viene del preview, debe coincidir con el SHA-256
    de estos bytes (vínculo preview↔import por contenido real).
    """
    preview = _parse_file(data=data, filename=filename)
    actual_hash = preview.file_hash or compute_file_hash(data)

    if expected_file_hash:
        expected = expected_file_hash.strip().lower()
        if expected != actual_hash.lower():
            raise PickingParseError(
                "El archivo de confirmación no coincide con el preview "
                f"(hash esperado {expected[:12]}…, recibido {actual_hash[:12]}…). "
                "Vuelva a generar el preview con el mismo archivo."
            )

    if picking_number_override:
        preview.picking_number = picking_number_override.strip()

    payload = preview.to_dict()
    if not payload["can_import"]:
        raise PickingParseError(
            "; ".join(payload["errors"]) or "Importación bloqueada por errores de validación"
        )
    if not preview.picking_number:
        raise PickingParseError("N.º Picking requerido")

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, status FROM distribuidora.loads
            WHERE picking_number = %s AND status <> 'cancelled'
            LIMIT 1
            """,
            (preview.picking_number,),
        )
        existing = cur.fetchone()
        if existing:
            raise PickingParseError(
                f"Ya existe la carga picking {preview.picking_number} "
                f"(id={existing[0]}, status={existing[1]})"
            )

        cur.execute(
            """
            INSERT INTO distribuidora.loads (
                picking_number, picking_date, destination, truck, seal,
                status, original_filename, source_type, file_hash,
                total_requested_units, total_items, total_value,
                document_units_total, document_value_total,
                created_by
            ) VALUES (
                %s, %s, %s, %s, %s,
                'pending', %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s
            )
            RETURNING id
            """,
            (
                preview.picking_number,
                preview.picking_date,
                preview.destination,
                preview.truck,
                preview.seal,
                filename,
                preview.source_type,
                actual_hash,
                preview.summed_units,
                len(preview.valid_lines),
                preview.summed_value,
                preview.document_units_total,
                preview.document_value_total,
                user_email,
            ),
        )
        load_id = int(cur.fetchone()[0])

        for i, ln in enumerate(preview.valid_lines):
            cur.execute(
                """
                INSERT INTO distribuidora.load_items (
                    load_id, line_number, branch, product_type, product_name,
                    normalized_product_name, barcode, sec,
                    requested_units, source_boxes_value, certified_units,
                    total_value, status
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, 0,
                    %s, 'pending'
                )
                """,
                (
                    load_id,
                    i + 1,
                    ln.branch,
                    ln.product_type,
                    ln.product_name,
                    ln.normalized_product_name or normalize_search_text(ln.product_name),
                    ln.barcode,
                    ln.sec,
                    ln.requested_units,
                    ln.source_boxes_value,
                    ln.total_value,
                ),
            )
        _insert_status_event(
            cur,
            load_id=load_id,
            from_status=None,
            to_status="pending",
            user_email=user_email,
            reason="import",
        )
        conn.commit()
        cur.close()
        return get_load(load_id)
    except PickingParseError:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def list_loads(*, limit: int = 50, status: str | None = None) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        params: list[Any] = []
        where = "WHERE 1=1"
        if status:
            where += " AND status = %s"
            params.append(status)
        params.append(limit)
        cur.execute(
            f"""
            SELECT
                l.*,
                (
                    SELECT COUNT(*) FROM distribuidora.load_items i
                    WHERE i.load_id = l.id AND i.status = 'complete'
                ) AS items_complete,
                (
                    SELECT COUNT(*) FROM distribuidora.load_items i
                    WHERE i.load_id = l.id AND i.status IN ('pending', 'partial', 'issue')
                ) AS items_pending,
                (
                    SELECT COUNT(*) FROM distribuidora.load_issues iss
                    WHERE iss.load_id = l.id AND iss.status = 'open'
                ) AS open_issues
            FROM distribuidora.loads l
            {where}
            ORDER BY l.created_at DESC
            LIMIT %s
            """,
            params,
        )
        rows = [_serialize_row(_row_dict(cur, r)) for r in cur.fetchall()]
        for row in rows:
            total = int(row.get("total_items") or 0)
            done = int(row.get("items_complete") or 0)
            row["progress_pct"] = round(100.0 * done / total, 1) if total else 0.0
        cur.close()
        return rows
    finally:
        conn.close()


def get_load(load_id: int) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM distribuidora.loads WHERE id = %s", (load_id,))
        row = cur.fetchone()
        if not row:
            cur.close()
            raise LookupError("Carga no encontrada")
        load = _serialize_row(_row_dict(cur, row))
        cur.execute(
            """
            SELECT * FROM distribuidora.load_items
            WHERE load_id = %s
            ORDER BY line_number NULLS LAST, id
            """,
            (load_id,),
        )
        items = [_serialize_row(_row_dict(cur, r)) for r in cur.fetchall()]
        summary = _summary_from_items(items)
        cur.execute(
            """
            SELECT COUNT(*) FROM distribuidora.load_issues
            WHERE load_id = %s AND status = 'open'
            """,
            (load_id,),
        )
        open_issues = int(cur.fetchone()[0] or 0)
        summary["open_issues"] = open_issues
        cur.execute(
            """
            SELECT i.product_name, i.barcode, e.created_at, e.action, e.units_after
            FROM distribuidora.load_item_events e
            JOIN distribuidora.load_items i ON i.id = e.load_item_id
            WHERE e.load_id = %s AND e.action IN ('add', 'complete', 'correction')
            ORDER BY e.created_at DESC
            LIMIT 5
            """,
            (load_id,),
        )
        recent = [_serialize_row(_row_dict(cur, r)) for r in cur.fetchall()]
        cur.close()
        load["items"] = items
        load["summary"] = summary
        load["recent_certified"] = recent
        load["product_types"] = sorted(
            {str(i.get("product_type") or "").strip() for i in items if i.get("product_type")}
        )
        return load
    finally:
        conn.close()


def _summary_from_items(items: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(items)
    complete = sum(1 for i in items if i.get("status") == "complete")
    partial = sum(1 for i in items if i.get("status") == "partial")
    pending = sum(1 for i in items if i.get("status") in {"pending", "issue"})
    excess = sum(1 for i in items if i.get("status") == "excess")
    req = round(sum(float(i.get("requested_units") or 0) for i in items), 3)
    cert = round(sum(float(i.get("certified_units") or 0) for i in items), 3)
    return {
        "total_items": total,
        "items_complete": complete,
        "items_partial": partial,
        "items_pending": pending,
        "items_excess": excess,
        "requested_units": req,
        "certified_units": cert,
        "progress_pct": round(100.0 * complete / total, 1) if total else 0.0,
    }


def search_items(
    load_id: int,
    *,
    q: str = "",
    status: str | None = None,
    product_type: str | None = None,
) -> list[dict[str, Any]]:
    load = get_load(load_id)
    items = load["items"]
    query = normalize_search_text(q)
    tokens = [t for t in query.split(" ") if t]

    def match(item: dict[str, Any]) -> bool:
        if status and status != "all":
            if status == "pending" and item.get("status") not in {"pending", "partial", "issue"}:
                return False
            if status == "partial" and item.get("status") != "partial":
                return False
            if status == "complete" and item.get("status") != "complete":
                return False
            if status == "diff" and item.get("status") not in {"excess", "issue", "partial"}:
                return False
        if product_type and product_type != "all":
            if normalize_search_text(item.get("product_type")) != normalize_search_text(
                product_type
            ):
                return False
        if not tokens:
            return True
        hay = " ".join(
            [
                normalize_search_text(item.get("product_name")),
                normalize_search_text(item.get("barcode")),
                normalize_search_text(item.get("product_type")),
            ]
        )
        return all(tok in hay for tok in tokens)

    return [i for i in items if match(i)]


def start_loading(load_id: int, *, user_email: str) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT status FROM distribuidora.loads WHERE id = %s FOR UPDATE
            """,
            (load_id,),
        )
        row = cur.fetchone()
        if not row:
            raise LookupError("Carga no encontrada")
        if row[0] == "cancelled":
            raise ValueError("La carga está cancelada")
        if row[0] == "certified":
            raise ValueError("La carga ya está certificada")
        cur.execute(
            """
            UPDATE distribuidora.loads
            SET status = 'in_progress',
                loading_started_at = COALESCE(loading_started_at, NOW())
            WHERE id = %s AND status IN ('pending', 'draft', 'in_progress')
            RETURNING id, status
            """,
            (load_id,),
        )
        updated = cur.fetchone()
        if not updated:
            raise LookupError("Carga no disponible para iniciar")
        if row[0] != "in_progress":
            _insert_status_event(
                cur,
                load_id=load_id,
                from_status=row[0],
                to_status="in_progress",
                user_email=user_email,
                reason="start",
            )
        conn.commit()
        cur.close()
        logger.info("load_started load_id=%s user=%s", load_id, user_email)
        return get_load(load_id)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def add_units(
    *,
    load_id: int,
    item_id: int,
    user_email: str,
    boxes: float = 0,
    loose_units: float = 0,
    notes: str | None = None,
    complete_remaining: bool = False,
    register_excess_issue: bool = False,
) -> dict[str, Any]:
    """
    Acumula unidades certificadas. No hay bypass ``allow_excess``.

    Si la operación superaría ``requested_units``:
    - no actualiza certified_units;
    - opcionalmente registra incidencia ``excess`` (trazabilidad);
    - lanza ValueError claro.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT status FROM distribuidora.loads WHERE id = %s FOR UPDATE",
            (load_id,),
        )
        load_row = cur.fetchone()
        if not load_row:
            raise LookupError("Carga no encontrada")
        if load_row[0] in {"certified", "cancelled"}:
            raise ValueError("La carga está bloqueada")
        if load_row[0] == "pending":
            cur.execute(
                """
                UPDATE distribuidora.loads
                SET status = 'in_progress',
                    loading_started_at = COALESCE(loading_started_at, NOW())
                WHERE id = %s
                """,
                (load_id,),
            )

        cur.execute(
            """
            SELECT id, requested_units, certified_units, sec, status
            FROM distribuidora.load_items
            WHERE id = %s AND load_id = %s
            FOR UPDATE
            """,
            (item_id, load_id),
        )
        item = cur.fetchone()
        if not item:
            raise LookupError("Producto no pertenece a esta carga")

        requested = _f(item[1])
        certified = _f(item[2])
        sec = int(item[3]) if item[3] is not None else None

        if complete_remaining:
            delta = round(requested - certified, 3)
            if delta <= 0:
                raise ValueError("No hay unidades pendientes")
            boxes = 0
            loose_units = delta
            action = "complete"
        else:
            delta = round(
                units_from_boxes_and_loose(boxes=boxes, loose=loose_units, sec=sec),
                3,
            )
            action = "add" if delta >= 0 else "subtract"

        if delta == 0:
            raise ValueError("La operación no agrega unidades")

        new_total = round(certified + delta, 3)
        if new_total < 0:
            raise ValueError("No se puede dejar unidades negativas")

        if new_total > requested + 1e-9:
            excess = round(new_total - requested, 3)
            if register_excess_issue:
                _create_excess_issue(
                    cur,
                    load_id=load_id,
                    item_id=item_id,
                    user_email=user_email,
                    requested=requested,
                    attempted=new_total,
                    certified=certified,
                    notes=notes,
                )
                conn.commit()
            else:
                conn.rollback()
            raise ValueError(
                f"EXCESO DE {excess:g} UNIDADES "
                f"(solicitadas={requested:g}, intento={new_total:g}). "
                "No se certificó el exceso. "
                "Registre incidencia 'excess' o corrija la cantidad."
            )

        status = _item_status(requested, new_total)
        cur.execute(
            """
            UPDATE distribuidora.load_items
            SET certified_units = %s,
                status = %s,
                last_event_at = NOW(),
                updated_at = NOW()
            WHERE id = %s
            """,
            (new_total, status, item_id),
        )
        cur.execute(
            """
            INSERT INTO distribuidora.load_item_events (
                load_id, load_item_id, user_email, action,
                boxes, loose_units, units_delta, units_after, notes
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                load_id,
                item_id,
                user_email,
                action,
                boxes or None,
                loose_units or None,
                delta,
                new_total,
                notes,
            ),
        )
        conn.commit()
        cur.close()
        return get_load(load_id)
    except ValueError:
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _create_excess_issue(
    cur,
    *,
    load_id: int,
    item_id: int,
    user_email: str,
    requested: float,
    attempted: float,
    certified: float,
    notes: str | None,
) -> None:
    cur.execute(
        """
        INSERT INTO distribuidora.load_issues (
            load_id, load_item_id, issue_type, description,
            expected_units, actual_units, status, created_by
        ) VALUES (%s, %s, 'excess', %s, %s, %s, 'open', %s)
        """,
        (
            load_id,
            item_id,
            notes
            or f"Intento de certificar {attempted:g} u sobre {requested:g} solicitadas",
            requested,
            attempted,
            user_email,
        ),
    )
    cur.execute(
        """
        UPDATE distribuidora.load_items
        SET status = 'issue', updated_at = NOW()
        WHERE id = %s
        """,
        (item_id,),
    )
    cur.execute(
        """
        INSERT INTO distribuidora.load_item_events (
            load_id, load_item_id, user_email, action,
            units_delta, units_after, notes
        ) VALUES (%s, %s, %s, 'issue', 0, %s, %s)
        """,
        (
            load_id,
            item_id,
            user_email,
            certified,
            f"excess_attempted={attempted:g}",
        ),
    )


def report_issue(
    *,
    load_id: int,
    item_id: int,
    user_email: str,
    issue_type: str,
    description: str | None = None,
) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT status FROM distribuidora.loads WHERE id = %s FOR UPDATE",
            (load_id,),
        )
        load_row = cur.fetchone()
        if not load_row:
            raise LookupError("Carga no encontrada")
        if load_row[0] in {"certified", "cancelled"}:
            raise ValueError("La carga está bloqueada")
        cur.execute(
            """
            SELECT requested_units, certified_units
            FROM distribuidora.load_items
            WHERE id = %s AND load_id = %s
            FOR UPDATE
            """,
            (item_id, load_id),
        )
        item = cur.fetchone()
        if not item:
            raise LookupError("Producto no pertenece a esta carga")
        cur.execute(
            """
            INSERT INTO distribuidora.load_issues (
                load_id, load_item_id, issue_type, description,
                expected_units, actual_units, status, created_by
            ) VALUES (%s, %s, %s, %s, %s, %s, 'open', %s)
            RETURNING id
            """,
            (
                load_id,
                item_id,
                issue_type,
                description,
                item[0],
                item[1],
                user_email,
            ),
        )
        cur.execute(
            """
            UPDATE distribuidora.load_items
            SET status = 'issue', updated_at = NOW()
            WHERE id = %s
            """,
            (item_id,),
        )
        cur.execute(
            """
            INSERT INTO distribuidora.load_item_events (
                load_id, load_item_id, user_email, action,
                units_delta, units_after, notes
            ) VALUES (%s, %s, %s, 'issue', 0, %s, %s)
            """,
            (load_id, item_id, user_email, item[1], description or issue_type),
        )
        conn.commit()
        cur.close()
        return get_load(load_id)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def resolve_issue(
    *,
    load_id: int,
    item_id: int,
    user_email: str,
    issue_id: int | None = None,
) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT status FROM distribuidora.loads WHERE id = %s FOR UPDATE",
            (load_id,),
        )
        load_row = cur.fetchone()
        if not load_row:
            raise LookupError("Carga no encontrada")
        if load_row[0] in {"certified", "cancelled"}:
            raise ValueError("La carga está bloqueada")
        if issue_id is not None:
            cur.execute(
                """
                UPDATE distribuidora.load_issues
                SET status = 'resolved', resolved_by = %s, resolved_at = NOW()
                WHERE id = %s AND load_id = %s AND load_item_id = %s AND status = 'open'
                """,
                (user_email, issue_id, load_id, item_id),
            )
        else:
            cur.execute(
                """
                UPDATE distribuidora.load_issues
                SET status = 'resolved', resolved_by = %s, resolved_at = NOW()
                WHERE load_id = %s AND load_item_id = %s AND status = 'open'
                """,
                (user_email, load_id, item_id),
            )
        cur.execute(
            """
            SELECT requested_units, certified_units FROM distribuidora.load_items
            WHERE id = %s AND load_id = %s FOR UPDATE
            """,
            (item_id, load_id),
        )
        item = cur.fetchone()
        if not item:
            raise LookupError("Producto no pertenece a esta carga")
        status = _item_status(_f(item[0]), _f(item[1]))
        cur.execute(
            """
            UPDATE distribuidora.load_items
            SET status = %s, updated_at = NOW()
            WHERE id = %s
            """,
            (status, item_id),
        )
        cur.execute(
            """
            INSERT INTO distribuidora.load_item_events (
                load_id, load_item_id, user_email, action,
                units_delta, units_after, notes
            ) VALUES (%s, %s, %s, 'resolve_issue', 0, %s, NULL)
            """,
            (load_id, item_id, user_email, item[1]),
        )
        conn.commit()
        cur.close()
        return get_load(load_id)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def certify_load(*, load_id: int, user_email: str) -> dict[str, Any]:
    """Validación final + UPDATE a certified en UNA sola transacción con FOR UPDATE."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT status, certified_by, certified_at
            FROM distribuidora.loads
            WHERE id = %s
            FOR UPDATE
            """,
            (load_id,),
        )
        load_row = cur.fetchone()
        if not load_row:
            raise LookupError("Carga no encontrada")
        current_status = load_row[0]
        if current_status == "cancelled":
            raise ValueError("No se puede certificar una carga cancelada")
        if current_status == "certified":
            raise ValueError("La carga ya está certificada")
        if current_status not in CERTIFIABLE_STATUSES:
            raise ValueError(f"Estado no certificable: {current_status}")

        # Bloquear también ítems para lectura consistente bajo la misma TX
        cur.execute(
            """
            SELECT id, requested_units, certified_units, status
            FROM distribuidora.load_items
            WHERE load_id = %s
            ORDER BY id
            FOR UPDATE
            """,
            (load_id,),
        )
        items = cur.fetchall()
        if not items:
            raise ValueError("La carga no tiene ítems")

        total = len(items)
        complete = 0
        excess = 0
        sum_req = 0.0
        sum_cert = 0.0
        for _id, req, cert, st in items:
            req_f = _f(req)
            cert_f = _f(cert)
            sum_req += req_f
            sum_cert += cert_f
            if st == "excess" or cert_f > req_f + 1e-9:
                excess += 1
            elif abs(cert_f - req_f) <= 1e-9 and cert_f > 0:
                complete += 1

        if complete != total:
            raise ValueError("Quedan productos incompletos")
        if excess > 0:
            raise ValueError("Hay excesos sin resolver")
        if abs(sum_req - sum_cert) > 1e-6:
            raise ValueError("Unidades certificadas no coinciden con solicitadas")

        cur.execute(
            """
            SELECT COUNT(*) FROM distribuidora.load_issues
            WHERE load_id = %s AND status = 'open'
            """,
            (load_id,),
        )
        open_issues = int(cur.fetchone()[0] or 0)
        if open_issues > 0:
            raise ValueError("Hay incidencias abiertas")

        cur.execute(
            """
            UPDATE distribuidora.loads
            SET status = 'certified',
                certified_by = %s,
                certified_at = NOW(),
                last_certified_by = %s,
                last_certified_at = NOW(),
                loading_finished_at = COALESCE(loading_finished_at, NOW())
            WHERE id = %s
            RETURNING id
            """,
            (user_email, user_email, load_id),
        )
        if not cur.fetchone():
            raise ValueError("No se pudo certificar la carga")
        _insert_status_event(
            cur,
            load_id=load_id,
            from_status=current_status,
            to_status="certified",
            user_email=user_email,
            reason="certify",
        )
        conn.commit()
        cur.close()
        return get_load(load_id)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def cancel_load(*, load_id: int, user_email: str, reason: str) -> dict[str, Any]:
    reason_clean = (reason or "").strip()
    if len(reason_clean) < 3:
        raise ValueError("Motivo de cancelación obligatorio (mín. 3 caracteres)")

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT status FROM distribuidora.loads WHERE id = %s FOR UPDATE
            """,
            (load_id,),
        )
        row = cur.fetchone()
        if not row:
            raise LookupError("Carga no encontrada")
        current = row[0]
        if current == "certified":
            raise ValueError(
                "No se puede cancelar una carga certificada sin flujo especial"
            )
        if current == "cancelled":
            raise ValueError("La carga ya está cancelada")
        if current not in CANCELABLE_STATUSES:
            raise ValueError(f"Solo se pueden cancelar cargas {sorted(CANCELABLE_STATUSES)}")

        cur.execute(
            """
            UPDATE distribuidora.loads
            SET status = 'cancelled',
                cancel_reason = %s,
                cancelled_by = %s,
                cancelled_at = NOW()
            WHERE id = %s
            RETURNING id
            """,
            (reason_clean, user_email, load_id),
        )
        if not cur.fetchone():
            raise ValueError("No se pudo cancelar la carga")
        _insert_status_event(
            cur,
            load_id=load_id,
            from_status=current,
            to_status="cancelled",
            user_email=user_email,
            reason=reason_clean,
        )
        conn.commit()
        cur.close()
        return get_load(load_id)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def reopen_load(*, load_id: int, user_email: str, reason: str) -> dict[str, Any]:
    reason_clean = (reason or "").strip()
    if len(reason_clean) < 3:
        raise ValueError("Motivo de reapertura obligatorio (mín. 3 caracteres)")

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT status, certified_by, certified_at, last_certified_by, last_certified_at
            FROM distribuidora.loads
            WHERE id = %s
            FOR UPDATE
            """,
            (load_id,),
        )
        row = cur.fetchone()
        if not row:
            raise LookupError("Carga no encontrada")
        if row[0] != "certified":
            raise ValueError("Solo se pueden reabrir cargas certificadas")

        certified_by = row[1]
        certified_at = row[2]
        # Conservar histórico: no perder quién certificó
        last_by = row[3] or certified_by
        last_at = row[4] or certified_at

        cur.execute(
            """
            UPDATE distribuidora.loads
            SET status = 'in_progress',
                reopened_by = %s,
                reopened_at = NOW(),
                reopen_reason = %s,
                last_certified_by = COALESCE(%s, last_certified_by),
                last_certified_at = COALESCE(%s, last_certified_at),
                certified_by = NULL,
                certified_at = NULL
            WHERE id = %s AND status = 'certified'
            RETURNING id
            """,
            (user_email, reason_clean, last_by, last_at, load_id),
        )
        if not cur.fetchone():
            raise ValueError("Solo se pueden reabrir cargas certificadas")
        _insert_status_event(
            cur,
            load_id=load_id,
            from_status="certified",
            to_status="in_progress",
            user_email=user_email,
            reason=reason_clean,
        )
        cur.execute(
            """
            INSERT INTO distribuidora.load_item_events (
                load_id, load_item_id, user_email, action,
                units_delta, units_after, notes
            )
            SELECT %s, i.id, %s, 'reopen', 0, i.certified_units, %s
            FROM distribuidora.load_items i
            WHERE i.load_id = %s
            ORDER BY i.id
            LIMIT 1
            """,
            (load_id, user_email, reason_clean, load_id),
        )
        conn.commit()
        cur.close()
        return get_load(load_id)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
