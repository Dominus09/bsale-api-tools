"""P4: visitas de entrega (inicio / reanudación / finalización).

No muta Bsale, picking histórico, ORS ni cuadratura.
Online-first: no afirma soporte offline.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import HTTPException

from backend.db import get_connection
from backend.repositories.distribuidora import delivery_visit_repo as visit_repo
from backend.services import object_storage_service as storage
from backend.services.distribuidora import client_location_service as loc_svc
from backend.services.distribuidora import dispatch_plan_pwa_driver_read_service as read_svc
from backend.utils.json_safe import serialize_value

EVIDENCE_KINDS = frozenset(
    {
        "closed_store",
        "not_found",
        "rejected_order",
        "absent_customer",
        "signature",
        "other",
    }
)

SANTIAGO = ZoneInfo("America/Santiago")

VISIT_RESULT_STATUSES = frozenset(
    {"completed", "partial", "not_delivered", "cancelled"}
)
NOT_DELIVERED_REASONS = frozenset(
    {
        "closed_store",
        "not_found",
        "rejected_order",
        "absent_customer",
        "other",
    }
)


def schema_status() -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        ready = visit_repo.tables_ready(cur)
        cur.close()
    finally:
        conn.close()
    return {
        "tables_ready": ready,
        "migration": "056_delivery_visits_evidence.sql",
        "mode": "online_only",
        "note_es": (
            "P4 inicia online. Sin almacenamiento offline persistente ni sync "
            "idempotente aún; no se simula entrega confirmada sin red."
        ),
    }


def _uid(user: dict[str, Any]) -> int:
    raw = user.get("id") if user.get("id") is not None else user.get("uid")
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=401, detail="Usuario sin id") from exc


def _parse_iso_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Timestamp GPS inválido") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _local_santiago(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(SANTIAGO).isoformat()


def _visit_public(row: dict[str, Any]) -> dict[str, Any]:
    started = row.get("started_at")
    ended = row.get("ended_at")
    if isinstance(started, str):
        try:
            started_dt = datetime.fromisoformat(started.replace("Z", "+00:00"))
        except ValueError:
            started_dt = None
    else:
        started_dt = started
    if isinstance(ended, str):
        try:
            ended_dt = datetime.fromisoformat(ended.replace("Z", "+00:00"))
        except ValueError:
            ended_dt = None
    else:
        ended_dt = ended

    return {
        "id": int(row["id"]),
        "company_id": int(row["company_id"]),
        "dispatch_plan_id": int(row["dispatch_plan_id"]),
        "customer_key": row["customer_key"],
        "client_id": row.get("client_id"),
        "driver_user_id": int(row["driver_user_id"]),
        "status": row["status"],
        "result_reason": row.get("result_reason"),
        "result_note": row.get("result_note"),
        "started_at": started,
        "started_at_local": _local_santiago(started_dt) if started_dt else None,
        "ended_at": ended,
        "ended_at_local": _local_santiago(ended_dt) if ended_dt else None,
        "start_gps": {
            "lat": row.get("start_lat"),
            "lng": row.get("start_lng"),
            "accuracy_m": row.get("start_accuracy_m"),
            "captured_at": row.get("start_gps_captured_at"),
            "source": row.get("start_gps_source"),
        },
        "end_gps": {
            "lat": row.get("end_lat"),
            "lng": row.get("end_lng"),
            "accuracy_m": row.get("end_accuracy_m"),
            "captured_at": row.get("end_gps_captured_at"),
            "source": row.get("end_gps_source"),
        },
        "gps_exception_code": row.get("gps_exception_code"),
        "gps_exception_note": row.get("gps_exception_note"),
        "closure_review_status": row.get("closure_review_status"),
        "version": int(row.get("version") or 1),
        "idempotency_key": row.get("idempotency_key"),
    }


def get_active_visit_for_client(
    *, plan_id: int, customer_key: str
) -> dict[str, Any] | None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        if not visit_repo.tables_ready(cur):
            cur.close()
            return None
        row = visit_repo.get_active_visit(
            cur, plan_id=int(plan_id), customer_key=customer_key.strip()
        )
        cur.close()
    finally:
        conn.close()
    return _visit_public(row) if row else None


def start_visit(
    *,
    user: dict[str, Any],
    plan_id: int,
    customer_key: str,
    latitude: float | None = None,
    longitude: float | None = None,
    accuracy_m: float | None = None,
    gps_captured_at: str | None = None,
    gps_source: str | None = None,
    gps_exception_code: str | None = None,
    gps_exception_note: str | None = None,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    """Crea o reanuda visita activa. No duplica in_progress."""
    uid = read_svc.enforce_driver_plan_read_access(user, int(plan_id))
    key = (customer_key or "").strip()
    if not key:
        raise HTTPException(status_code=400, detail="customer_key requerido")

    client = loc_svc._resolve_client(int(plan_id), key)  # noqa: SLF001
    try:
        client_id = loc_svc._parse_client_id(key, client)  # noqa: SLF001
    except HTTPException:
        raise HTTPException(
            status_code=422,
            detail=(
                "Cliente sin identidad comprobada (name:…); "
                "bloqueado para escrituras de entrega hasta asociación segura."
            ),
        )
    company_id = loc_svc._authorized_company_id_for_plan(user, int(plan_id))  # noqa: SLF001

    has_gps = latitude is not None and longitude is not None
    if has_gps:
        try:
            lat_f = float(latitude)  # type: ignore[arg-type]
            lng_f = float(longitude)  # type: ignore[arg-type]
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="GPS inválido") from exc
        if not (-90.0 <= lat_f <= 90.0 and -180.0 <= lng_f <= 180.0):
            raise HTTPException(status_code=400, detail="GPS fuera de rango")
        gps_exception_code = None
        gps_exception_note = None
    else:
        lat_f = None
        lng_f = None
        code = (gps_exception_code or "").strip() or "gps_unavailable"
        note = (gps_exception_note or "").strip() or None
        if not note:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Sin GPS: indique gps_exception_code y gps_exception_note "
                    "(excepción auditable). No se inventan coordenadas."
                ),
            )
        gps_exception_code = code
        gps_exception_note = note

    captured_at = _parse_iso_ts(gps_captured_at) if has_gps else None
    source = (gps_source or "").strip() or ("device" if has_gps else "exception")
    idem = (idempotency_key or "").strip() or None

    conn = get_connection()
    try:
        cur = conn.cursor()
        if not visit_repo.tables_ready(cur):
            raise HTTPException(
                status_code=503,
                detail="Migración 056 pendiente; visitas de entrega no disponibles",
            )

        if idem:
            existing_idem = visit_repo.find_by_idem(
                cur, driver_user_id=uid, idempotency_key=idem
            )
            if existing_idem:
                conn.commit()
                cur.close()
                return serialize_value(
                    {
                        "visit": _visit_public(existing_idem),
                        "created": False,
                        "resumed": existing_idem.get("status") == "in_progress",
                        "idempotent_replay": True,
                        "mode": "online_only",
                    }
                )

        active = visit_repo.get_active_visit(
            cur, plan_id=int(plan_id), customer_key=key
        )
        if active:
            if int(active["driver_user_id"]) != uid:
                raise HTTPException(
                    status_code=409,
                    detail="Ya hay una visita en atención de otro chofer",
                )
            visit_repo.insert_event(
                cur,
                visit_id=int(active["id"]),
                action="visit_resumed",
                actor_user_id=uid,
                payload={"via": "start_visit"},
            )
            conn.commit()
            out = _visit_public(active)
            cur.close()
            return serialize_value(
                {
                    "visit": out,
                    "created": False,
                    "resumed": True,
                    "idempotent_replay": False,
                    "mode": "online_only",
                    "note_es": "Visita en atención reanudada; no se duplicó.",
                }
            )

        row = visit_repo.insert_visit(
            cur,
            company_id=company_id,
            dispatch_plan_id=int(plan_id),
            customer_key=key,
            client_id=int(client_id),
            driver_user_id=uid,
            start_lat=lat_f,
            start_lng=lng_f,
            start_accuracy_m=float(accuracy_m) if accuracy_m is not None else None,
            start_gps_captured_at=captured_at,
            start_gps_source=source,
            gps_exception_code=gps_exception_code,
            gps_exception_note=gps_exception_note,
            idempotency_key=idem,
        )
        visit_repo.insert_event(
            cur,
            visit_id=int(row["id"]),
            action="visit_started",
            actor_user_id=uid,
            payload={
                "has_gps": has_gps,
                "gps_exception_code": gps_exception_code,
                "client_id": int(client_id),
            },
        )
        conn.commit()
        out = _visit_public(row)
        cur.close()
    except HTTPException:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        # Unique active / idem races → re-fetch
        msg = str(exc).lower()
        if "uq_delivery_visit_active" in msg or "uq_delivery_visit_idem" in msg:
            conn2 = get_connection()
            try:
                cur2 = conn2.cursor()
                if idem:
                    again = visit_repo.find_by_idem(
                        cur2, driver_user_id=uid, idempotency_key=idem
                    )
                else:
                    again = visit_repo.get_active_visit(
                        cur2, plan_id=int(plan_id), customer_key=key
                    )
                cur2.close()
            finally:
                conn2.close()
            if again:
                return serialize_value(
                    {
                        "visit": _visit_public(again),
                        "created": False,
                        "resumed": again.get("status") == "in_progress",
                        "idempotent_replay": True,
                        "mode": "online_only",
                    }
                )
        raise HTTPException(status_code=500, detail=f"No se pudo iniciar visita: {exc}") from exc
    finally:
        conn.close()

    return serialize_value(
        {
            "visit": out,
            "created": True,
            "resumed": False,
            "idempotent_replay": False,
            "mode": "online_only",
            "ui_status_es": "En atención",
        }
    )


def get_visit(
    *,
    user: dict[str, Any],
    plan_id: int,
    visit_id: int,
) -> dict[str, Any]:
    uid = read_svc.enforce_driver_plan_read_access(user, int(plan_id))
    conn = get_connection()
    try:
        cur = conn.cursor()
        if not visit_repo.tables_ready(cur):
            raise HTTPException(status_code=503, detail="Migración 056 pendiente")
        row = visit_repo.get_visit(cur, int(visit_id))
        cur.close()
    finally:
        conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Visita no encontrada")
    if int(row["dispatch_plan_id"]) != int(plan_id):
        raise HTTPException(status_code=404, detail="Visita no pertenece al despacho")
    if int(row["driver_user_id"]) != uid:
        raise HTTPException(status_code=403, detail="Visita de otro chofer")
    return serialize_value({"visit": _visit_public(row), "mode": "online_only"})


def _expected_related_document_ids(plan_id: int, customer_key: str) -> list[int]:
    try:
        ficha = read_svc.get_client_ficha_for_driver(int(plan_id), customer_key)
    except Exception:
        return []
    out: list[int] = []
    for d in ficha.get("documents") or []:
        rid = d.get("related_document_id")
        if rid is None:
            continue
        try:
            out.append(int(rid))
        except (TypeError, ValueError):
            continue
    return out


def finalize_visit(
    *,
    user: dict[str, Any],
    plan_id: int,
    visit_id: int,
    expected_version: int,
    status: str,
    result_reason: str | None = None,
    result_note: str | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
    accuracy_m: float | None = None,
    gps_captured_at: str | None = None,
    gps_source: str | None = None,
    gps_exception_code: str | None = None,
    gps_exception_note: str | None = None,
) -> dict[str, Any]:
    """Cierre transaccional e idempotente por versión. Sin mutar Bsale/stock."""
    from backend.services.distribuidora import delivery_visit_closure as closure

    uid = read_svc.enforce_driver_plan_read_access(user, int(plan_id))
    st = (status or "").strip()
    if st not in VISIT_RESULT_STATUSES:
        raise HTTPException(status_code=400, detail="Estado de cierre inválido")

    if st == "not_delivered":
        reason = (result_reason or "").strip()
        if reason not in NOT_DELIVERED_REASONS:
            raise HTTPException(
                status_code=400,
                detail="Motivo de no entrega requerido",
            )
        if reason == "other" and not (result_note or "").strip():
            raise HTTPException(
                status_code=400,
                detail="Observación obligatoria cuando el motivo es 'other'",
            )
    else:
        reason = (result_reason or "").strip() or None

    has_gps = latitude is not None and longitude is not None
    if has_gps:
        lat_f = float(latitude)  # type: ignore[arg-type]
        lng_f = float(longitude)  # type: ignore[arg-type]
        if not (-90.0 <= lat_f <= 90.0 and -180.0 <= lng_f <= 180.0):
            raise HTTPException(status_code=400, detail="GPS final fuera de rango")
        captured = _parse_iso_ts(gps_captured_at)
        source = (gps_source or "").strip() or "device"
    else:
        lat_f = lng_f = None
        captured = None
        if (gps_exception_code or "").strip():
            source = "exception"
        else:
            source = None

    conn = get_connection()
    try:
        cur = conn.cursor()
        if not visit_repo.tables_ready(cur):
            raise HTTPException(status_code=503, detail="Migración 056 pendiente")

        current = visit_repo.get_visit(cur, int(visit_id))
        if not current:
            raise HTTPException(status_code=404, detail="Visita no encontrada")
        if int(current["dispatch_plan_id"]) != int(plan_id):
            raise HTTPException(status_code=404, detail="Visita no pertenece al despacho")
        if int(current["driver_user_id"]) != uid:
            raise HTTPException(status_code=403, detail="Visita de otro chofer")

        if current.get("status") != "in_progress":
            if (
                current.get("status") == st
                and int(current.get("version") or 0) >= int(expected_version)
            ):
                cur.close()
                return serialize_value(
                    {
                        "visit": _visit_public(current),
                        "finalized": False,
                        "idempotent_replay": True,
                        "persisted": True,
                        "mode": "online_only",
                    }
                )
            raise HTTPException(
                status_code=409,
                detail=f"Visita ya cerrada ({current.get('status')})",
            )

        # Propagar excepción GPS de cierre a la fila si aplica
        if not has_gps and (gps_exception_code or "").strip():
            current = dict(current)
            current["gps_exception_code"] = (gps_exception_code or "").strip()
            current["gps_exception_note"] = (gps_exception_note or "").strip() or None

        expected_ids = _expected_related_document_ids(
            int(plan_id), str(current.get("customer_key") or "")
        )
        gate = closure.assert_finalize_ready(
            cur,
            visit=current,
            status=st,
            result_reason=reason,
            result_note=result_note,
            has_end_gps=has_gps,
            expected_related_ids=expected_ids,
        )

        updated = visit_repo.finalize_visit(
            cur,
            int(visit_id),
            expected_version=int(expected_version),
            status=st,
            result_reason=reason,
            result_note=(result_note or "").strip() or None,
            end_lat=lat_f,
            end_lng=lng_f,
            end_accuracy_m=float(accuracy_m) if accuracy_m is not None else None,
            end_gps_captured_at=captured,
            end_gps_source=source,
            closure_review_status=gate["closure_review_status"],
        )
        if not updated:
            raise HTTPException(
                status_code=409,
                detail="Conflicto de versión o visita ya finalizada",
            )
        visit_repo.insert_event(
            cur,
            visit_id=int(visit_id),
            action="visit_finalized",
            actor_user_id=uid,
            payload={
                "status": st,
                "result_reason": reason,
                "expected_version": int(expected_version),
                "new_version": int(updated.get("version") or 0),
                "has_end_gps": has_gps,
                "closure_review_status": gate["closure_review_status"],
                "gate_notes": gate.get("notes") or [],
            },
        )
        conn.commit()
        out = _visit_public(updated)
        cur.close()
    except HTTPException:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        raise HTTPException(
            status_code=500, detail=f"No se pudo finalizar visita: {exc}"
        ) from exc
    finally:
        conn.close()

    return serialize_value(
        {
            "visit": out,
            "finalized": True,
            "idempotent_replay": False,
            "persisted": True,
            "closure_review_status": gate["closure_review_status"],
            "mode": "online_only",
            "note_es": (
                "Cierre persistido. No se descontó stock ni se modificó Bsale/"
                "picking/cuadratura."
            ),
        }
    )


UNDELIVERED_REASONS = frozenset(
    {
        "physical_shortage",
        "damaged",
        "customer_rejection",
        "prep_error",
        "other",
    }
)
COLLECTION_METHODS = frozenset(
    {
        "cash",
        "transfer_declared",
        "transfer_verified",
        "credit",
        "partial",
        "pending",
        "not_applicable",
    }
)


def _require_own_open_visit(
    cur, *, user: dict[str, Any], plan_id: int, visit_id: int
) -> dict[str, Any]:
    uid = read_svc.enforce_driver_plan_read_access(user, int(plan_id))
    row = visit_repo.get_visit(cur, int(visit_id))
    if not row:
        raise HTTPException(status_code=404, detail="Visita no encontrada")
    if int(row["dispatch_plan_id"]) != int(plan_id):
        raise HTTPException(status_code=404, detail="Visita no pertenece al despacho")
    if int(row["driver_user_id"]) != uid:
        raise HTTPException(status_code=403, detail="Visita de otro chofer")
    if row.get("status") != "in_progress":
        raise HTTPException(status_code=409, detail="Visita cerrada; no editable")
    return row


def save_document_lines(
    *,
    user: dict[str, Any],
    plan_id: int,
    visit_id: int,
    related_document_id: int,
    oc_document_id: int | None,
    document_number: int | None,
    document_type: str | None,
    document_total: float | None,
    result_status: str,
    notes: str | None,
    lines: list[dict[str, Any]],
    expected_version: int | None = None,
) -> dict[str, Any]:
    """Persiste cantidades entregadas por documento. No muta document_details/Bsale.

    expected_version es opcional para compatibilidad con PWA anteriores.
    Si se envía, detecta conflictos concurrentes (409) y no sobrescribe a ciegas.
    """
    if result_status not in {"pending", "full", "partial", "none", "skipped"}:
        raise HTTPException(status_code=400, detail="result_status inválido")
    if not lines:
        raise HTTPException(status_code=400, detail="Se requieren líneas")

    normalized: list[dict[str, Any]] = []
    seen_details: set[int] = set()
    for raw in lines:
        try:
            planned = float(raw.get("planned_units") or 0)
            delivered = float(raw.get("delivered_units") or 0)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="Cantidades inválidas") from exc
        if planned < 0 or delivered < 0:
            raise HTTPException(status_code=400, detail="Cantidades negativas no permitidas")
        if delivered > planned:
            raise HTTPException(
                status_code=400,
                detail="Entregado no puede superar lo planificado",
            )
        undelivered = round(planned - delivered, 6)
        reason = (raw.get("undelivered_reason") or "").strip() or None
        note = (raw.get("undelivered_note") or "").strip() or None
        if undelivered > 0:
            if reason not in UNDELIVERED_REASONS:
                raise HTTPException(
                    status_code=400,
                    detail="Motivo de no entrega requerido por línea",
                )
            if reason == "other" and not note:
                raise HTTPException(
                    status_code=400,
                    detail="Observación obligatoria cuando el motivo es 'other'",
                )
        else:
            reason = None
            note = None
        src_raw = raw.get("source_detail_id")
        if src_raw is None:
            raise HTTPException(
                status_code=400,
                detail="source_detail_id obligatorio (línea exacta del documento)",
            )
        try:
            source_detail_id = int(src_raw)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="source_detail_id inválido") from exc
        if source_detail_id in seen_details:
            raise HTTPException(
                status_code=400,
                detail=f"Línea duplicada source_detail_id={source_detail_id}",
            )
        seen_details.add(source_detail_id)
        line_number = raw.get("line_number")
        try:
            line_number_n = int(line_number) if line_number is not None else None
        except (TypeError, ValueError):
            line_number_n = None
        normalized.append(
            {
                "source_detail_id": source_detail_id,
                "line_number": line_number_n,
                "variant_id": raw.get("variant_id"),
                "sku_code": (raw.get("sku_code") or None),
                "product_name": (raw.get("product_name") or None),
                "planned_units": planned,
                "delivered_units": delivered,
                "undelivered_units": undelivered,
                "undelivered_reason": reason,
                "undelivered_note": note,
            }
        )

    conn = get_connection()
    bumped: dict[str, Any] | None = None
    invalidated = False
    try:
        cur = conn.cursor()
        if not visit_repo.tables_ready(cur):
            raise HTTPException(status_code=503, detail="Migración 056 pendiente")
        # FOR UPDATE evita dos escrituras concurrentes sobre la misma visita.
        locked = visit_repo.get_visit_for_update(cur, int(visit_id))
        if not locked:
            raise HTTPException(status_code=404, detail="Visita no encontrada")
        visit = _require_own_open_visit(
            cur, user=user, plan_id=int(plan_id), visit_id=int(visit_id)
        )
        if expected_version is not None:
            current_ver = int(visit.get("version") or 1)
            if current_ver != int(expected_version):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "Conflicto de versión: la visita fue actualizada por otra "
                        "operación. Recargue e intente de nuevo."
                    ),
                )
        uid = int(visit["driver_user_id"])
        # Validar source_detail_id contra document_details cuando el doc tiene líneas.
        detail_ids = [int(x["source_detail_id"]) for x in normalized]
        cur.execute(
            "SELECT to_regclass('distribuidora.document_details') IS NOT NULL AS ok"
        )
        row_ok = cur.fetchone()
        details_exist = bool(
            (
                row_ok["ok"]
                if isinstance(row_ok, dict) or hasattr(row_ok, "keys")
                else row_ok[0]
            )
        )
        if not details_exist:
            raise HTTPException(
                status_code=503,
                detail=(
                    "document_details no disponible; "
                    "no se registran cantidades sobre líneas ambiguas"
                ),
            )
        cur.execute(
            """
            SELECT COUNT(*) AS n
            FROM distribuidora.document_details
            WHERE document_id = %s
            """,
            (int(related_document_id),),
        )
        crow = cur.fetchone()
        n_doc = int(
            crow["n"]
            if isinstance(crow, dict) or hasattr(crow, "keys")
            else crow[0] or 0
        )
        if n_doc <= 0:
            raise HTTPException(
                status_code=422,
                detail=(
                    "Documento sin detail_id confiable; "
                    "solo lectura informativa, no se registran cantidades"
                ),
            )
        cur.execute(
            """
            SELECT detail_id
            FROM distribuidora.document_details
            WHERE document_id = %s
              AND detail_id = ANY(%s)
            """,
            (int(related_document_id), detail_ids),
        )
        found = {
            int(r["detail_id"] if isinstance(r, dict) or hasattr(r, "keys") else r[0])
            for r in cur.fetchall()
        }
        missing = [d for d in detail_ids if d not in found]
        if missing:
            raise HTTPException(
                status_code=422,
                detail=(
                    "source_detail_id no pertenece al documento: "
                    + ", ".join(str(x) for x in missing)
                ),
            )
        # Estado inicial: pending si aún no se completan todas las líneas del doc.
        initial_status = result_status if result_status == "pending" else result_status
        doc = visit_repo.upsert_document_result(
            cur,
            visit_id=int(visit_id),
            related_document_id=int(related_document_id),
            oc_document_id=int(oc_document_id) if oc_document_id is not None else None,
            document_number=document_number,
            document_type=document_type,
            document_total=document_total,
            result_status=initial_status,
            notes=(notes or "").strip() or None,
        )
        if int(doc["visit_id"]) != int(visit_id):
            raise HTTPException(status_code=409, detail="Documento no pertenece a la visita")
        saved_lines = visit_repo.replace_line_results(
            cur,
            visit_id=int(visit_id),
            document_result_id=int(doc["id"]),
            lines=normalized,
        )
        # Derivar estado real tras merge: no marcar documento completo si faltan líneas.
        n_saved = len(saved_lines)
        if n_saved < n_doc:
            derived_status = "pending"
        else:
            undel = sum(float(x.get("undelivered_units") or 0) for x in saved_lines)
            deliv = sum(float(x.get("delivered_units") or 0) for x in saved_lines)
            if undel <= 0:
                derived_status = "full"
            elif deliv <= 0:
                derived_status = "none"
            else:
                derived_status = "partial"
        if str(doc.get("result_status") or "") != derived_status:
            doc = visit_repo.update_document_result_status(
                cur,
                document_result_id=int(doc["id"]),
                result_status=derived_status,
            )
        result_status = derived_status
        invalidated = visit_repo.invalidate_conformity(
            cur,
            int(visit_id),
            reason="Cantidades modificadas tras conformidad",
        )
        # Siempre incrementar versión tras mutar líneas (concurrencia + firma).
        bumped = visit_repo.bump_visit_version(cur, int(visit_id))
        visit_repo.insert_event(
            cur,
            visit_id=int(visit_id),
            action="document_lines_saved",
            actor_user_id=uid,
            payload={
                "related_document_id": int(related_document_id),
                "line_count": len(saved_lines),
                "lines_upserted": len(normalized),
                "expected_detail_count": n_doc,
                "result_status": result_status,
                "conformity_invalidated": bool(invalidated),
                "expected_version": expected_version,
                "new_version": int((bumped or visit).get("version") or 0),
            },
        )
        conn.commit()
        cur.close()
    except HTTPException:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        raise HTTPException(
            status_code=500, detail=f"No se pudieron guardar líneas: {exc}"
        ) from exc
    finally:
        conn.close()

    return serialize_value(
        {
            "document_result": doc,
            "lines": saved_lines,
            "persisted": True,
            "lines_registered": len(saved_lines),
            "lines_expected": n_doc,
            "document_complete": len(saved_lines) >= n_doc,
            "visit_version": int((bumped or visit).get("version") or 1),
            "conformity_invalidated": bool(invalidated),
            "note_es": "Cantidades guardadas sin modificar Bsale ni picking histórico.",
        }
    )


def declare_collection(
    *,
    user: dict[str, Any],
    plan_id: int,
    visit_id: int,
    document_result_id: int | None,
    method: str,
    document_amount: float | None,
    declared_amount: float | None,
    note: str | None,
) -> dict[str, Any]:
    """Declaración de cobro del chofer. No genera recibo ni movimiento contable."""
    method_n = (method or "").strip()
    if method_n not in COLLECTION_METHODS:
        raise HTTPException(status_code=400, detail="Método de cobranza inválido")
    if method_n == "transfer_verified":
        raise HTTPException(
            status_code=403,
            detail=(
                "transfer_verified solo vía verificación administrativa; "
                "el chofer declara transfer_declared"
            ),
        )

    doc_amt = float(document_amount) if document_amount is not None else None
    decl = float(declared_amount) if declared_amount is not None else None
    if decl is not None and decl < 0:
        raise HTTPException(status_code=400, detail="Monto declarado inválido")
    # pending admite monto > 0: es la porción declarada como pendiente (pago mixto),
    # no un "recibido" verificable.
    if method_n == "pending" and decl is None:
        decl = 0.0
    pending = None
    if method_n == "pending":
        pending = float(decl or 0)
    elif doc_amt is not None and decl is not None:
        pending = round(doc_amt - decl, 2)

    conn = get_connection()
    try:
        cur = conn.cursor()
        if not visit_repo.tables_ready(cur):
            raise HTTPException(status_code=503, detail="Migración 056 pendiente")
        visit = _require_own_open_visit(
            cur, user=user, plan_id=int(plan_id), visit_id=int(visit_id)
        )
        uid = int(visit["driver_user_id"])
        doc_res_id = (
            int(document_result_id) if document_result_id is not None else None
        )
        if doc_res_id is not None:
            docs = visit_repo.list_document_results(cur, int(visit_id))
            owned = {int(d["id"]) for d in docs}
            if doc_res_id not in owned:
                raise HTTPException(
                    status_code=422,
                    detail="document_result_id no pertenece a esta visita",
                )
        row = visit_repo.insert_collection(
            cur,
            visit_id=int(visit_id),
            document_result_id=doc_res_id,
            method=method_n,
            document_amount=doc_amt,
            declared_amount=decl,
            verified_amount=None,
            pending_balance=pending,
            note=(note or "").strip() or None,
            created_by_user_id=uid,
        )
        visit_repo.insert_event(
            cur,
            visit_id=int(visit_id),
            action="collection_declared",
            actor_user_id=uid,
            payload={"method": method_n, "declared_amount": decl},
        )
        conn.commit()
        cur.close()
    except HTTPException:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        raise HTTPException(
            status_code=500, detail=f"No se pudo declarar cobranza: {exc}"
        ) from exc
    finally:
        conn.close()

    return serialize_value(
        {
            "collection": row,
            "persisted": True,
            "note_es": (
                "Declaración auditada. No es recibo oficial ni cuadratura Bsale."
            ),
        }
    )


def declare_collections_mixed(
    *,
    user: dict[str, Any],
    plan_id: int,
    visit_id: int,
    document_result_id: int,
    document_amount: float | None,
    payments: list[dict[str, Any]],
) -> dict[str, Any]:
    """Reemplaza las formas de pago de un documento (pago mixto auditado)."""
    if not payments:
        raise HTTPException(status_code=400, detail="Se requiere al menos una forma de pago")
    doc_amt = float(document_amount) if document_amount is not None else None
    normalized: list[dict[str, Any]] = []
    sum_decl = 0.0
    for raw in payments:
        method_n = str(raw.get("method") or "").strip()
        if method_n not in COLLECTION_METHODS:
            raise HTTPException(status_code=400, detail="Método de cobranza inválido")
        if method_n == "transfer_verified":
            raise HTTPException(
                status_code=403,
                detail=(
                    "transfer_verified solo vía verificación administrativa; "
                    "el chofer declara transfer_declared"
                ),
            )
        try:
            decl = (
                float(raw["declared_amount"])
                if raw.get("declared_amount") is not None
                else None
            )
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="Monto declarado inválido") from exc
        if decl is not None and decl < 0:
            raise HTTPException(status_code=400, detail="Monto declarado inválido")
        if method_n == "pending" and decl is None:
            decl = 0.0
        if method_n == "not_applicable" and decl is not None and decl > 0:
            raise HTTPException(
                status_code=400, detail="No corresponde no admite monto positivo"
            )
        if method_n != "not_applicable" and decl is not None:
            sum_decl += float(decl)
        pending = float(decl or 0) if method_n == "pending" else None
        if pending is None and doc_amt is not None and decl is not None:
            pending = None  # saldo global se calcula al final
        normalized.append(
            {
                "method": method_n,
                "declared_amount": decl,
                "pending_balance": pending,
                "note": (str(raw.get("note") or "").strip() or None),
            }
        )
    if doc_amt is not None and sum_decl - float(doc_amt) > 0.009:
        raise HTTPException(
            status_code=400,
            detail="La suma de montos supera el total del documento",
        )
    remaining = None
    if doc_amt is not None:
        remaining = round(float(doc_amt) - sum_decl, 2)

    conn = get_connection()
    try:
        cur = conn.cursor()
        if not visit_repo.tables_ready(cur):
            raise HTTPException(status_code=503, detail="Migración 056 pendiente")
        visit = _require_own_open_visit(
            cur, user=user, plan_id=int(plan_id), visit_id=int(visit_id)
        )
        uid = int(visit["driver_user_id"])
        docs = visit_repo.list_document_results(cur, int(visit_id))
        owned = {int(d["id"]) for d in docs}
        if int(document_result_id) not in owned:
            raise HTTPException(
                status_code=422,
                detail="document_result_id no pertenece a esta visita",
            )
        visit_repo.delete_collections_for_document(
            cur, visit_id=int(visit_id), document_result_id=int(document_result_id)
        )
        rows: list[dict[str, Any]] = []
        for item in normalized:
            rows.append(
                visit_repo.insert_collection(
                    cur,
                    visit_id=int(visit_id),
                    document_result_id=int(document_result_id),
                    method=item["method"],
                    document_amount=doc_amt,
                    declared_amount=item["declared_amount"],
                    verified_amount=None,
                    pending_balance=item["pending_balance"],
                    note=item["note"],
                    created_by_user_id=uid,
                )
            )
        visit_repo.insert_event(
            cur,
            visit_id=int(visit_id),
            action="collections_mixed_declared",
            actor_user_id=uid,
            payload={
                "document_result_id": int(document_result_id),
                "payment_count": len(rows),
                "sum_declared": sum_decl,
                "pending_balance": remaining,
            },
        )
        conn.commit()
        cur.close()
    except HTTPException:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        raise HTTPException(
            status_code=500, detail=f"No se pudo declarar cobranza mixta: {exc}"
        ) from exc
    finally:
        conn.close()

    return serialize_value(
        {
            "collections": rows,
            "persisted": True,
            "sum_declared": sum_decl,
            "pending_balance": remaining,
            "note_es": (
                "Declaración mixta auditada. No es recibo oficial ni verificación Bsale."
            ),
        }
    )


def build_whatsapp_report(
    *,
    user: dict[str, Any],
    plan_id: int,
    visit_id: int,
    client_name: str | None = None,
    planning_code: str | None = None,
    driver_name: str | None = None,
) -> dict[str, Any]:
    """Texto compartible solo desde registros confirmados. Sin coords/firma/RUT completo."""
    from backend.services.distribuidora import delivery_whatsapp_report as wa_report
    from backend.services.distribuidora import dispatch_plan_driver_service as driver_svc

    uid = read_svc.enforce_driver_plan_read_access(user, int(plan_id))
    conn = get_connection()
    try:
        cur = conn.cursor()
        if not visit_repo.tables_ready(cur):
            raise HTTPException(status_code=503, detail="Migración 056 pendiente")
        visit = visit_repo.get_visit(cur, int(visit_id))
        if not visit:
            raise HTTPException(status_code=404, detail="Visita no encontrada")
        if int(visit["dispatch_plan_id"]) != int(plan_id):
            raise HTTPException(status_code=404, detail="Visita no pertenece al despacho")
        if int(visit["driver_user_id"]) != uid:
            raise HTTPException(status_code=403, detail="Visita de otro chofer")
        if visit.get("status") == "in_progress":
            raise HTTPException(
                status_code=409,
                detail="Finalize la visita antes de generar el reporte compartible",
            )

        docs = visit_repo.list_document_results(cur, int(visit_id))
        lines = visit_repo.list_line_results(cur, int(visit_id))
        cols = visit_repo.list_collections(cur, int(visit_id))
        try:
            evid_n = visit_repo.count_evidence(cur, int(visit_id))
        except Exception:
            evid_n = 0
        try:
            conformity = visit_repo.get_conformity(cur, int(visit_id))
        except Exception:
            conformity = None

        resolved_plan_code = planning_code
        resolved_driver = driver_name
        try:
            summary = driver_svc.get_plan_summary_for_driver(uid, int(plan_id))
            if summary:
                resolved_plan_code = resolved_plan_code or summary.get("planning_code")
                resolved_driver = resolved_driver or summary.get("driver_name")
        except Exception:
            pass
        if not resolved_driver:
            resolved_driver = (
                str(user.get("name") or user.get("email") or f"Chofer {uid}").strip()
            )

        has_sig = None
        refused = None
        if conformity:
            if conformity.get("signature_object_key") and not conformity.get("refused"):
                has_sig = True
            elif conformity.get("refused"):
                has_sig = False
                refused = True
            else:
                has_sig = False

        report_data = wa_report.visit_row_to_report_data(
            visit,
            docs=docs,
            lines=lines,
            cols=cols,
            planning_code=resolved_plan_code or str(plan_id),
            client_name=client_name,
            driver_name=resolved_driver,
            has_evidence=True if evid_n > 0 else False,
            has_signature=has_sig,
            signature_refused=refused,
        )
        bundle = wa_report.build_report_bundle(report_data, demo=False)
        report_text = bundle["report_text"]
        payload = {
            "visit_id": int(visit_id),
            "template_key": bundle["template_key"],
            "demo": False,
            "omitted": bundle["omitted"],
            "report_data_keys": sorted(report_data.keys()),
        }
        snap = visit_repo.insert_report_snapshot(
            cur,
            visit_id=int(visit_id),
            report_text=report_text,
            payload=payload,
            generated_by_user_id=uid,
        )
        visit_repo.insert_event(
            cur,
            visit_id=int(visit_id),
            action="report_snapshot_generated",
            actor_user_id=uid,
            payload={"snapshot_id": int(snap["id"]), "template_key": bundle["template_key"]},
        )
        conn.commit()
        cur.close()
    except HTTPException:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        raise HTTPException(
            status_code=500, detail=f"No se pudo generar reporte: {exc}"
        ) from exc
    finally:
        conn.close()

    return serialize_value(
        {
            **bundle,
            "snapshot_id": int(snap["id"]),
            "persisted": True,
            "mode": "online_only",
            "adapter": {
                "kind": "manual_share_v1",
                "official_api_ready": False,
                "note_es": (
                    "Adaptador futuro para API oficial; hoy solo compartir manual."
                ),
            },
        }
    )


def list_demo_whatsapp_reports(*, user: dict[str, Any]) -> dict[str, Any]:
    """Modo prueba familiar: solo textos ficticios en memoria. Sin escritura productiva."""
    from backend.services.distribuidora import delivery_whatsapp_report as wa_report

    _uid(user)
    scenarios = wa_report.demo_scenarios()
    return serialize_value(
        {
            "demo": True,
            "writes_production": False,
            "count": len(scenarios),
            "scenarios": scenarios,
            "note_es": (
                "Datos ficticios para evaluar el aspecto del mensaje. "
                "No modifica planes, cobranzas, inventario ni evidencias reales. "
                "No incluye enlaces de invitación a grupos."
            ),
            "adapter": {
                "kind": "manual_share_v1",
                "official_api_ready": False,
            },
        }
    )


def list_visits_for_erp(
    *, user: dict[str, Any], limit: int = 100
) -> dict[str, Any]:
    """Visibilidad operativa. Sin mutaciones de stock/financieras."""
    from backend.auth.multiapp import get_scope

    scope = get_scope(user)
    if scope.load_failed or getattr(scope, "companies_unverified", False):
        raise HTTPException(
            status_code=503, detail="No se pudo verificar empresas autorizadas"
        )

    conn = get_connection()
    try:
        cur = conn.cursor()
        if not visit_repo.tables_ready(cur):
            cur.close()
            return serialize_value(
                {
                    "tables_ready": False,
                    "items": [],
                    "message_es": "Migración 056 pendiente",
                    "mode": "online_only",
                }
            )
        # Si el alcance limita empresas, filtrar por la primera autorizada
        # cuando hay una sola; si hay varias, listar todas y filtrar en Python.
        rows = visit_repo.list_recent_visits(cur, limit=max(1, min(int(limit), 200)))
        cur.close()
    finally:
        conn.close()

    allowed = scope.company_ids
    items = []
    conn2 = get_connection()
    try:
        cur2 = conn2.cursor()
        for r in rows:
            cid = int(r["company_id"])
            if allowed is not None and cid not in allowed:
                continue
            vid = int(r["id"])
            try:
                evid = visit_repo.list_evidence(cur2, vid)
                lines = visit_repo.list_line_results(cur2, vid)
                docs = visit_repo.list_document_results(cur2, vid)
                conf = visit_repo.get_conformity(cur2, vid)
            except Exception:
                evid, lines, docs, conf = [], [], [], None
            evid_ok = sum(
                1
                for e in evid
                if e.get("upload_confirmed") or e.get("validation_status") == "exception"
            )
            delivered = sum(float(x.get("delivered_units") or 0) for x in lines)
            pending_u = sum(float(x.get("undelivered_units") or 0) for x in lines)
            if conf and not conf.get("invalidated_at"):
                conf_status = "refused" if conf.get("refused") else "signed"
            elif conf and conf.get("invalidated_at"):
                conf_status = "invalidated"
            else:
                conf_status = "pending"
            review = r.get("closure_review_status")
            items.append(
                {
                    **_visit_public(r),
                    "declared_collected": float(r.get("declared_collected") or 0),
                    "pending_balance_sum": float(r.get("pending_balance_sum") or 0),
                    "closure_review_status": review,
                    "evidence_count": evid_ok,
                    "conformity_status": conf_status,
                    "delivered_units": delivered,
                    "pending_units": pending_u,
                    "document_count": len(docs),
                    "review_label_es": (
                        "Pendiente de revisión"
                        if review == "pending_review"
                        else ("Verificada" if review == "verified" else "—")
                    ),
                }
            )
        cur2.close()
    finally:
        conn2.close()

    by_status: dict[str, int] = {}
    for it in items:
        st = str(it.get("status") or "?")
        by_status[st] = by_status.get(st, 0) + 1

    return serialize_value(
        {
            "tables_ready": True,
            "items": items,
            "summary": by_status,
            "mode": "online_only",
            "note_es": (
                "Solo visibilidad. Sin descuento de stock, devoluciones ni cierre "
                "financiero automático."
            ),
        }
    )


def get_visit_detail_for_erp(
    *, user: dict[str, Any], visit_id: int
) -> dict[str, Any]:
    """Detalle ERP con URLs R2 firmadas (TTL corto). Sin exponer object_key."""
    from backend.auth.multiapp import get_scope
    from backend.repositories.distribuidora import client_location_repo as loc_repo

    scope = get_scope(user)
    if scope.load_failed or getattr(scope, "companies_unverified", False):
        raise HTTPException(
            status_code=503, detail="No se pudo verificar empresas autorizadas"
        )

    conn = get_connection()
    try:
        cur = conn.cursor()
        if not visit_repo.tables_ready(cur):
            raise HTTPException(status_code=503, detail="Migración 056 pendiente")
        visit = visit_repo.get_visit(cur, int(visit_id))
        if not visit:
            raise HTTPException(status_code=404, detail="Visita no encontrada")
        company_id = int(visit["company_id"])
        allowed = scope.company_ids
        if allowed is not None and company_id not in allowed:
            raise HTTPException(status_code=404, detail="Visita no encontrada")

        evidence = visit_repo.list_evidence(cur, int(visit_id))
        conf = visit_repo.get_conformity(cur, int(visit_id))
        docs = visit_repo.list_document_results(cur, int(visit_id))
        lines = visit_repo.list_line_results(cur, int(visit_id))
        cols = visit_repo.list_collections(cur, int(visit_id))

        facade_out = None
        client_id = visit.get("client_id")
        if client_id is not None:
            try:
                facade = loc_repo.get_current_facade(cur, int(client_id))
            except Exception:
                facade = None
            if facade and int(facade.get("company_id") or -1) == company_id:
                furl = None
                if facade.get("upload_confirmed") and facade.get("object_key"):
                    try:
                        furl = storage.presign_get(
                            object_key=str(facade["object_key"]), expires_in=300
                        )
                    except Exception:
                        furl = None
                facade_out = {
                    "kind": "facade",
                    "label_es": "Fotografía de fachada (maestro cliente)",
                    "photo_id": int(facade["id"]),
                    "validation_status": facade.get("validation_status"),
                    "upload_confirmed": bool(facade.get("upload_confirmed")),
                    "url": furl,
                    "expires_in_sec": 300 if furl else None,
                    "note_es": (
                        "Fachada del cliente, no es evidencia de esta visita."
                    ),
                }
        cur.close()
    finally:
        conn.close()

    evid_out: list[dict[str, Any]] = []
    for e in evidence:
        if int(e.get("company_id") or -1) != company_id:
            continue
        url = None
        if e.get("upload_confirmed") and e.get("object_key"):
            try:
                url = storage.presign_get(
                    object_key=str(e["object_key"]), expires_in=300
                )
            except Exception:
                url = None
        kind = str(e.get("evidence_kind") or "other")
        evid_out.append(
            {
                "id": int(e["id"]),
                "kind": "incident_evidence",
                "evidence_kind": kind,
                "label_es": f"Evidencia de incidencia ({kind})",
                "validation_status": e.get("validation_status"),
                "upload_confirmed": bool(e.get("upload_confirmed")),
                "exception_note": e.get("exception_note"),
                "captured_at": e.get("captured_at"),
                "has_gps": e.get("capture_lat") is not None
                and e.get("capture_lng") is not None,
                "url": url,
                "expires_in_sec": 300 if url else None,
            }
        )

    conf_out = None
    if conf:
        surl = None
        if (
            conf.get("signature_object_key")
            and not conf.get("invalidated_at")
            and not conf.get("refused")
        ):
            try:
                surl = storage.presign_get(
                    object_key=str(conf["signature_object_key"]), expires_in=300
                )
            except Exception:
                surl = None
        conf_out = {
            "refused": bool(conf.get("refused")),
            "refuse_reason": conf.get("refuse_reason"),
            "receptor_name": conf.get("receptor_name"),
            "invalidated_at": conf.get("invalidated_at"),
            "signed_at": conf.get("signed_at"),
            "signature_url": surl,
            "expires_in_sec": 300 if surl else None,
            "valid": not bool(conf.get("invalidated_at")),
        }

    return serialize_value(
        {
            "visit": _visit_public(visit),
            "documents": docs,
            "lines": lines,
            "collections": cols,
            "facade_photo": facade_out,
            "incident_evidence": evid_out,
            "conformity": conf_out,
            "mode": "online_only",
            "note_es": (
                "URLs firmadas de corta duración. Bucket privado; "
                "acceso filtrado por empresa autorizada."
            ),
        }
    )


def get_visit_detail(
    *, user: dict[str, Any], plan_id: int, visit_id: int
) -> dict[str, Any]:
    uid = read_svc.enforce_driver_plan_read_access(user, int(plan_id))
    conn = get_connection()
    try:
        cur = conn.cursor()
        if not visit_repo.tables_ready(cur):
            raise HTTPException(status_code=503, detail="Migración 056 pendiente")
        visit = visit_repo.get_visit(cur, int(visit_id))
        if not visit:
            raise HTTPException(status_code=404, detail="Visita no encontrada")
        if int(visit["dispatch_plan_id"]) != int(plan_id):
            raise HTTPException(status_code=404, detail="Visita no pertenece al despacho")
        if int(visit["driver_user_id"]) != uid:
            raise HTTPException(status_code=403, detail="Visita de otro chofer")
        docs = visit_repo.list_document_results(cur, int(visit_id))
        lines = visit_repo.list_line_results(cur, int(visit_id))
        cols = visit_repo.list_collections(cur, int(visit_id))
        evidence = visit_repo.list_evidence(cur, int(visit_id))
        conf = visit_repo.get_conformity(cur, int(visit_id))
        cur.close()
    finally:
        conn.close()

    evid_out = []
    for e in evidence:
        url = None
        if e.get("upload_confirmed") and e.get("object_key"):
            try:
                url = storage.presign_get(object_key=str(e["object_key"]))
            except Exception:
                url = None
        evid_out.append({**e, "url": url, "object_key": None})

    conf_out = None
    if conf:
        conf_out = dict(conf)
        conf_out.pop("signature_object_key", None)
        if conf.get("signature_object_key") and not conf.get("invalidated_at"):
            try:
                conf_out["signature_url"] = storage.presign_get(
                    object_key=str(conf["signature_object_key"])
                )
            except Exception:
                conf_out["signature_url"] = None
        conf_out["valid"] = not bool(conf.get("invalidated_at"))

    return serialize_value(
        {
            "visit": _visit_public(visit),
            "documents": docs,
            "lines": lines,
            "collections": cols,
            "evidence": evid_out,
            "conformity": conf_out,
            "mode": "online_only",
        }
    )


def upload_incident_evidence(
    *,
    user: dict[str, Any],
    plan_id: int,
    visit_id: int,
    evidence_kind: str,
    data: bytes,
    content_type: str | None,
    latitude: float | None,
    longitude: float | None,
    accuracy_m: float | None,
    idempotency_key: str | None,
) -> dict[str, Any]:
    kind = (evidence_kind or "").strip()
    if kind not in EVIDENCE_KINDS or kind == "signature":
        raise HTTPException(status_code=400, detail="evidence_kind inválido")
    uid = read_svc.enforce_driver_plan_read_access(user, int(plan_id))
    company_id = loc_svc._authorized_company_id_for_plan(user, int(plan_id))  # noqa: SLF001

    data_n, mime, _meta = loc_svc._normalize_facade_image(data, content_type)  # noqa: SLF001
    digest = hashlib.sha256(data_n).hexdigest()
    idem = (idempotency_key or "").strip() or None

    conn = get_connection()
    try:
        cur = conn.cursor()
        if not visit_repo.tables_ready(cur):
            raise HTTPException(status_code=503, detail="Migración 056 pendiente")
        visit = _require_own_open_visit(
            cur, user=user, plan_id=int(plan_id), visit_id=int(visit_id)
        )
        if idem:
            existing = visit_repo.find_evidence_by_idem(
                cur, user_id=uid, idempotency_key=idem
            )
            if existing:
                conn.commit()
                cur.close()
                return serialize_value(
                    {
                        "evidence": existing,
                        "created": False,
                        "idempotent_replay": True,
                        "persisted": True,
                    }
                )

        object_key = storage.build_delivery_evidence_object_key(
            company_id=company_id,
            plan_id=int(plan_id),
            visit_id=int(visit_id),
            kind=kind,
            ext="jpg",
        )
        try:
            storage.put_bytes(object_key=object_key, body=data_n, content_type=mime)
            if not storage.object_exists(object_key):
                raise HTTPException(status_code=502, detail="R2 no confirmó el objeto")
        except storage.ObjectStorageNotConfigured as exc:
            raise HTTPException(
                status_code=503,
                detail="Almacenamiento R2 no disponible; registre excepción auditable",
            ) from exc
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(
                status_code=502, detail=f"Error al almacenar evidencia: {exc}"
            ) from exc

        event_id = visit_repo.insert_event(
            cur,
            visit_id=int(visit_id),
            action="evidence_uploaded",
            actor_user_id=uid,
            payload={"kind": kind, "sha256": digest},
        )
        row = visit_repo.insert_evidence(
            cur,
            visit_id=int(visit_id),
            company_id=company_id,
            evidence_kind=kind,
            object_key=object_key,
            content_type=mime,
            size_bytes=len(data_n),
            sha256=digest,
            captured_by_user_id=uid,
            capture_lat=float(latitude) if latitude is not None else None,
            capture_lng=float(longitude) if longitude is not None else None,
            capture_accuracy_m=float(accuracy_m) if accuracy_m is not None else None,
            upload_confirmed=True,
            validation_status="pending",
            exception_note=None,
            idempotency_key=idem,
            visit_event_id=event_id,
        )
        _ = visit
        conn.commit()
        cur.close()
    except HTTPException:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        conn.close()

    safe = dict(row)
    safe.pop("object_key", None)
    return serialize_value(
        {"evidence": safe, "created": True, "persisted": True, "idempotent_replay": False}
    )


def register_evidence_exception(
    *,
    user: dict[str, Any],
    plan_id: int,
    visit_id: int,
    evidence_kind: str,
    exception_note: str,
    latitude: float | None = None,
    longitude: float | None = None,
) -> dict[str, Any]:
    """Incidencia sin foto usable: excepción auditable (pending_review al cerrar)."""
    kind = (evidence_kind or "").strip()
    if kind not in EVIDENCE_KINDS or kind == "signature":
        raise HTTPException(status_code=400, detail="evidence_kind inválido")
    note = (exception_note or "").strip()
    if len(note) < 8:
        raise HTTPException(
            status_code=400, detail="Detalle de excepción obligatorio (≥8 caracteres)"
        )
    uid = read_svc.enforce_driver_plan_read_access(user, int(plan_id))
    company_id = loc_svc._authorized_company_id_for_plan(user, int(plan_id))  # noqa: SLF001

    conn = get_connection()
    try:
        cur = conn.cursor()
        visit = _require_own_open_visit(
            cur, user=user, plan_id=int(plan_id), visit_id=int(visit_id)
        )
        event_id = visit_repo.insert_event(
            cur,
            visit_id=int(visit_id),
            action="evidence_exception",
            actor_user_id=uid,
            payload={"kind": kind, "note": note},
        )
        placeholder_key = (
            f"operaciones/entregas/{company_id}/{int(plan_id)}/{int(visit_id)}/"
            f"evidencias/{kind}/exception-{event_id}.txt"
        )
        row = visit_repo.insert_evidence(
            cur,
            visit_id=int(visit_id),
            company_id=company_id,
            evidence_kind=kind,
            object_key=placeholder_key,
            content_type="text/plain",
            size_bytes=0,
            sha256=None,
            captured_by_user_id=uid,
            capture_lat=float(latitude) if latitude is not None else None,
            capture_lng=float(longitude) if longitude is not None else None,
            capture_accuracy_m=None,
            upload_confirmed=False,
            validation_status="exception",
            exception_note=note,
            idempotency_key=None,
            visit_event_id=event_id,
        )
        _ = visit
        conn.commit()
        cur.close()
    except HTTPException:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()

    safe = dict(row)
    safe.pop("object_key", None)
    return serialize_value(
        {
            "evidence": safe,
            "persisted": True,
            "closure_hint": "pending_review",
            "note_es": "Excepción auditada; el cierre no será plenamente verificado.",
        }
    )


def submit_conformity(
    *,
    user: dict[str, Any],
    plan_id: int,
    visit_id: int,
    receptor_name: str,
    signature_png: bytes | None,
    refused: bool,
    refuse_reason: str | None,
    latitude: float | None,
    longitude: float | None,
    accuracy_m: float | None,
) -> dict[str, Any]:
    from backend.services.distribuidora import delivery_visit_closure as closure

    uid = read_svc.enforce_driver_plan_read_access(user, int(plan_id))
    company_id = loc_svc._authorized_company_id_for_plan(user, int(plan_id))  # noqa: SLF001
    name = (receptor_name or "").strip()
    if not refused and len(name) < 2:
        raise HTTPException(status_code=400, detail="Nombre del receptor requerido")
    if refused and not (refuse_reason or "").strip():
        raise HTTPException(status_code=400, detail="Motivo de negativa requerido")
    if not refused and not signature_png:
        raise HTTPException(status_code=400, detail="Firma requerida")

    conn = get_connection()
    try:
        cur = conn.cursor()
        visit = _require_own_open_visit(
            cur, user=user, plan_id=int(plan_id), visit_id=int(visit_id)
        )
        docs = visit_repo.list_document_results(cur, int(visit_id))
        lines = visit_repo.list_line_results(cur, int(visit_id))
        if not refused and (not docs or not lines):
            raise HTTPException(
                status_code=422,
                detail="Registre cantidades antes de solicitar conformidad",
            )
        summary = closure.build_conformity_summary(
            visit=visit, docs=docs, lines=lines
        )
        import json

        summary_bytes = json.dumps(summary, sort_keys=True, default=str).encode("utf-8")
        summary_sha = hashlib.sha256(summary_bytes).hexdigest()

        sig_key = None
        sig_ctype = None
        if not refused and signature_png:
            # Validar PNG/JPEG y re-encode PNG sin EXIF
            data_n, mime, _m = loc_svc._normalize_facade_image(  # noqa: SLF001
                signature_png, "image/png"
            )
            # Facade normalizer returns JPEG; acceptable for storage
            sig_ctype = mime
            sig_key = storage.build_delivery_signature_object_key(
                company_id=company_id,
                plan_id=int(plan_id),
                visit_id=int(visit_id),
                ext="jpg" if "jpeg" in mime else "png",
            )
            try:
                storage.put_bytes(object_key=sig_key, body=data_n, content_type=mime)
                if not storage.object_exists(sig_key):
                    raise HTTPException(status_code=502, detail="R2 no confirmó la firma")
            except storage.ObjectStorageNotConfigured as exc:
                raise HTTPException(
                    status_code=503, detail="R2 no disponible para firmas"
                ) from exc

        row = visit_repo.upsert_conformity(
            cur,
            visit_id=int(visit_id),
            receptor_name=name or None,
            signature_object_key=sig_key,
            signature_content_type=sig_ctype,
            accepted_summary_sha256=summary_sha,
            summary_payload=summary,
            signed_at=datetime.now(timezone.utc) if not refused else None,
            refused=bool(refused),
            refuse_reason=(refuse_reason or "").strip() or None,
            gps_lat=float(latitude) if latitude is not None else None,
            gps_lng=float(longitude) if longitude is not None else None,
            gps_accuracy_m=float(accuracy_m) if accuracy_m is not None else None,
            visit_version_at_sign=int(visit.get("version") or 1),
        )
        visit_repo.insert_event(
            cur,
            visit_id=int(visit_id),
            action="conformity_refused" if refused else "conformity_signed",
            actor_user_id=uid,
            payload={"summary_sha256": summary_sha, "refused": bool(refused)},
        )
        conn.commit()
        cur.close()
    except HTTPException:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        conn.close()

    safe = dict(row)
    safe.pop("signature_object_key", None)
    return serialize_value(
        {
            "conformity": safe,
            "summary_sha256": summary_sha,
            "persisted": True,
            "valid": True,
            "note_es": (
                "Conformidad registrada. Cambios posteriores de cantidades la invalidan."
            ),
        }
    )
