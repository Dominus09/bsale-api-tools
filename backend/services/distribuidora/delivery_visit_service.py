"""P4: visitas de entrega (inicio / reanudación / finalización).

No muta Bsale, picking histórico, ORS ni cuadratura.
Online-first: no afirma soporte offline.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import HTTPException

from backend.db import get_connection
from backend.repositories.distribuidora import delivery_visit_repo as visit_repo
from backend.services.distribuidora import client_location_service as loc_svc
from backend.services.distribuidora import dispatch_plan_pwa_driver_read_service as read_svc
from backend.utils.json_safe import serialize_value

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
) -> dict[str, Any]:
    """Cierre transaccional e idempotente por versión. Sin mutar Bsale/stock."""
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
            # Idempotente: mismo cierre ya persistido
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
            "mode": "online_only",
            "note_es": (
                "Cierre persistido. No se descontó stock ni se modificó Bsale/"
                "picking/cuadratura."
            ),
        }
    )
