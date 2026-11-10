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
) -> dict[str, Any]:
    """Persiste cantidades entregadas por documento. No muta document_details/Bsale."""
    if result_status not in {"pending", "full", "partial", "none", "skipped"}:
        raise HTTPException(status_code=400, detail="result_status inválido")
    if not lines:
        raise HTTPException(status_code=400, detail="Se requieren líneas")

    normalized: list[dict[str, Any]] = []
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
        normalized.append(
            {
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
    try:
        cur = conn.cursor()
        if not visit_repo.tables_ready(cur):
            raise HTTPException(status_code=503, detail="Migración 056 pendiente")
        visit = _require_own_open_visit(
            cur, user=user, plan_id=int(plan_id), visit_id=int(visit_id)
        )
        uid = int(visit["driver_user_id"])
        doc = visit_repo.upsert_document_result(
            cur,
            visit_id=int(visit_id),
            related_document_id=int(related_document_id),
            oc_document_id=int(oc_document_id) if oc_document_id is not None else None,
            document_number=document_number,
            document_type=document_type,
            document_total=document_total,
            result_status=result_status,
            notes=(notes or "").strip() or None,
        )
        saved_lines = visit_repo.replace_line_results(
            cur,
            visit_id=int(visit_id),
            document_result_id=int(doc["id"]),
            lines=normalized,
        )
        visit_repo.insert_event(
            cur,
            visit_id=int(visit_id),
            action="document_lines_saved",
            actor_user_id=uid,
            payload={
                "related_document_id": int(related_document_id),
                "line_count": len(saved_lines),
                "result_status": result_status,
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
    pending = None
    if doc_amt is not None and decl is not None:
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
        row = visit_repo.insert_collection(
            cur,
            visit_id=int(visit_id),
            document_result_id=(
                int(document_result_id) if document_result_id is not None else None
            ),
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
    for r in rows:
        cid = int(r["company_id"])
        if allowed is not None and cid not in allowed:
            continue
        items.append(
            {
                **_visit_public(r),
                "declared_collected": float(r.get("declared_collected") or 0),
                "pending_balance_sum": float(r.get("pending_balance_sum") or 0),
            }
        )

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
