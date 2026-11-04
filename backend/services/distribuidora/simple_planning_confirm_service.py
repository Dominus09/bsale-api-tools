"""Confirmación segura de planificación simplificada (un camión por request).

Reutiliza ``confirm_dispatch_plan``. No genera facturas ni loads.
Bloquea sobrecarga, capacidad desconocida y pesos no confiables (sin override admin).
"""

from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable

from backend.db import get_connection
from backend.repositories.distribuidora import simple_planning_session_repo as session_repo
from backend.services.distribuidora.dispatch_plan_oc_guard import (
    OcConfirmConflictError,
    OcDocumentStateError,
    REPLAN_MODEL_GAP,
)
from backend.services.distribuidora.dispatch_plan_service import (
    confirm_dispatch_plan,
    get_dispatch_plan,
)
from backend.services.distribuidora.simple_planning_assignment_service import (
    PLANNING_ORIGIN_SIMPLE,
    detect_fingerprint_drift,
    preview_truck_assignments,
    validate_truck_capacity,
)
from backend.services.distribuidora.simple_planning_preview_service import (
    SimplePlanningPreviewError,
    STATUS_FOUND_AVAILABLE,
    STATUS_NOT_FOUND,
    _int,
    format_fingerprint_drift_message,
)

logger = logging.getLogger(__name__)

SESSION_TTL_MINUTES = 30
WEIGHT_KNOWN = "known"
WEIGHT_ESTIMATED = "estimated"
WEIGHT_PARTIAL = "partial"
WEIGHT_UNEVALUABLE = "unevaluable"
WEIGHT_UNAVAILABLE = WEIGHT_UNEVALUABLE  # legacy alias

ROUTE_DEPARTURE_DISCLAIMER = (
    "La confirmación de planificación con cobertura de peso incompleta "
    "NO autoriza salida a ruta; el control de despacho físico queda para "
    "la futura PWA de bodega/chofer."
)


class SimplePlanningConfirmError(ValueError):
    """Error de negocio en prepare/confirm (mapeable a 400/409)."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "confirm_rejected",
        http_status: int = 400,
        details: dict[str, Any] | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.http_status = http_status
        self.details = details or {}


def _mark_session_failed_new_conn(
    *,
    conn_factory: Callable[[], Any],
    session_id: Any,
    last_error: str,
) -> None:
    conn = conn_factory()
    try:
        cur = conn.cursor()
        session_repo.mark_session_status(
            cur,
            session_id,
            status="failed",
            last_error=last_error[:500],
            now=datetime.now(timezone.utc),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("mark_session_failed failed session=%s", session_id)
    finally:
        conn.close()


def _parse_date(value: Any) -> date:
    if value is None:
        return date.today()
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        return date.fromisoformat(str(value).strip()[:10])
    except ValueError as exc:
        raise SimplePlanningConfirmError(f"planning_date inválida: {value!r}") from exc


def _public_session(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("payload")
    if isinstance(payload, str):
        import json

        payload = json.loads(payload)
    return {
        "session_id": str(row["id"]),
        "plan_session_id": row.get("plan_session_id"),
        "user_id": row.get("user_id"),
        "company_id": row.get("company_id"),
        "office_id": row.get("office_id"),
        "planning_date": (
            row["planning_date"].isoformat()
            if hasattr(row.get("planning_date"), "isoformat")
            else row.get("planning_date")
        ),
        "truck_id": row.get("truck_id"),
        "dispatch_name": row.get("dispatch_name"),
        "driver_name": row.get("driver_name"),
        "status": row.get("status"),
        "expires_at": (
            row["expires_at"].isoformat()
            if hasattr(row.get("expires_at"), "isoformat")
            else row.get("expires_at")
        ),
        "confirmed_plan_id": row.get("confirmed_plan_id"),
        "orders": (payload or {}).get("orders") if isinstance(payload, dict) else None,
        "planning_origin": PLANNING_ORIGIN_SIMPLE,
        "ors_metrics_calculated": False,
    }


def _capacity_from_orders(orders: list[dict[str, Any]], truck_max: int | None) -> dict[str, Any]:
    known = 0.0
    estimated = 0.0
    incomplete = 0
    unevaluable = 0
    for order in orders:
        weight = order.get("weight") or {}
        quality = weight.get("quality") or WEIGHT_UNEVALUABLE
        if quality == "unavailable":
            quality = WEIGHT_UNEVALUABLE
        peso = weight.get("known_weight_kg")
        if peso is None:
            peso = weight.get("peso_total_kg")
        if quality == WEIGHT_KNOWN and peso is not None:
            known += float(peso)
        elif quality == WEIGHT_ESTIMATED and peso is not None:
            estimated += float(peso)
        elif quality == WEIGHT_PARTIAL and peso is not None and float(peso) > 0:
            known += float(peso)
            incomplete += 1
        else:
            unevaluable += 1
    return validate_truck_capacity(
        known_weight_kg=known,
        estimated_weight_kg=estimated,
        incomplete_orders=incomplete,
        unevaluable_orders=unevaluable,
        truck_max_weight_kg=truck_max,
    )


def _folios_by_weight_quality(
    orders: list[dict[str, Any]], qualities: set[str]
) -> list[int]:
    out: list[int] = []
    for o in orders:
        q = ((o.get("weight") or {}).get("quality") or WEIGHT_UNEVALUABLE)
        if q == "unavailable":
            q = WEIGHT_UNEVALUABLE
        if q in qualities:
            f = _int(o.get("folio"))
            if f is not None:
                out.append(f)
    return sorted(set(out))


def _assert_confirmable_capacity(
    capacity: dict[str, Any],
    *,
    orders: list[dict[str, Any]] | None = None,
    acknowledge_incomplete_weight: bool = False,
    acknowledge_unevaluable_weight: bool = False,
) -> dict[str, Any]:
    """Bloquea sobrecarga y capacidad desconocida; cobertura incompleta requiere ack."""
    if not capacity.get("capacity_known"):
        raise SimplePlanningConfirmError(
            "Capacidad del camión desconocida: no se confirma sin max_weight_kg registrado",
            code="truck_capacity_unknown",
            http_status=409,
        )
    if capacity.get("overloaded") or capacity.get("blocking"):
        raise SimplePlanningConfirmError(
            "Camión sobrecargado: el peso conocido supera la capacidad máxima",
            code="truck_overloaded",
            http_status=409,
        )

    incomplete_n = int(capacity.get("incomplete_order_count") or 0)
    unevaluable_n = int(capacity.get("unevaluable_order_count") or 0)
    # Compat: si solo viene unavailable_order_count legacy sin desglose
    if (
        incomplete_n == 0
        and unevaluable_n == 0
        and int(capacity.get("unavailable_order_count") or 0) > 0
        and not capacity.get("requires_incomplete_weight_ack")
        and not capacity.get("requires_unevaluable_weight_ack")
    ):
        unevaluable_n = int(capacity["unavailable_order_count"])

    ack_audit: dict[str, Any] = {
        "incomplete_weight": False,
        "unevaluable_weight": False,
        "does_not_authorize_route_departure": True,
        "note": ROUTE_DEPARTURE_DISCLAIMER,
    }

    if unevaluable_n:
        folios = _folios_by_weight_quality(
            orders or [], {WEIGHT_UNEVALUABLE, "unavailable"}
        )
        if not acknowledge_unevaluable_weight:
            listed = ", ".join(str(f) for f in folios[:8]) or "varias"
            more = f" (+{len(folios) - 8} más)" if len(folios) > 8 else ""
            raise SimplePlanningConfirmError(
                f"OC {listed}{more}: capacidad total aún no verificable "
                "(ningún peso usable). Marca la revisión manual expresa para continuar. "
                + ROUTE_DEPARTURE_DISCLAIMER,
                code="weight_unevaluable_ack_required",
                http_status=409,
                details={
                    "folios": folios,
                    "stage": "prepare-confirm",
                    "requires_unevaluable_weight_ack": True,
                },
            )
        ack_audit["unevaluable_weight"] = True
        ack_audit["folios_unevaluable"] = folios

    if incomplete_n:
        folios = _folios_by_weight_quality(orders or [], {WEIGHT_PARTIAL})
        if not acknowledge_incomplete_weight:
            listed = ", ".join(str(f) for f in folios[:8]) or "varias"
            more = f" (+{len(folios) - 8} más)" if len(folios) > 8 else ""
            raise SimplePlanningConfirmError(
                f"OC {listed}{more}: cobertura de peso incompleta "
                "(productos sin peso registrado). Acepta explícitamente para planificar. "
                + ROUTE_DEPARTURE_DISCLAIMER,
                code="weight_incomplete_ack_required",
                http_status=409,
                details={
                    "folios": folios,
                    "stage": "prepare-confirm",
                    "requires_incomplete_weight_ack": True,
                    "known_weight_kg": capacity.get("known_weight_kg"),
                },
            )
        ack_audit["incomplete_weight"] = True
        ack_audit["folios_incomplete"] = folios

    return ack_audit


def prepare_confirm_session(
    *,
    user_id: int,
    company_id: int,
    office_id: int,
    planning_date: Any,
    truck_id: int,
    dispatch_name: str | None,
    driver_name: str | None,
    assignments: list[dict[str, Any]],
    idempotency_key: str | None = None,
    acknowledge_incomplete_weight: bool = False,
    acknowledge_unevaluable_weight: bool = False,
    preview_fn: Callable[..., dict[str, Any]] | None = None,
    trucks_loader: Callable[[list[int]], dict[int, dict[str, Any]]] | None = None,
    conn_factory: Callable[[], Any] | None = None,
) -> dict[str, Any]:
    """Valida un camión + OC y crea sesión server-side (no confirma)."""
    if int(truck_id) <= 0:
        raise SimplePlanningConfirmError("truck_id inválido")
    if not assignments:
        raise SimplePlanningConfirmError("Indique asignaciones folio→camión")

    for row in assignments:
        if _int((row or {}).get("truck_id")) != int(truck_id):
            raise SimplePlanningConfirmError(
                "prepare-confirm admite un solo truck_id por sesión",
                code="multi_truck_not_allowed",
            )

    pdate = _parse_date(planning_date)
    report = preview_truck_assignments(
        company_id=int(company_id),
        office_id=int(office_id),
        planning_date=pdate,
        dispatch_name=dispatch_name,
        assignments=list(assignments),
        preview_fn=preview_fn,
        trucks_loader=trucks_loader,
    )
    if report.get("request_duplicate_folios"):
        raise SimplePlanningConfirmError(
            f"OC duplicadas en la solicitud: {report['request_duplicate_folios']}",
            code="duplicate_folios",
            http_status=409,
        )
    if report.get("fingerprint_drifts"):
        drifts = list(report.get("fingerprint_drifts") or [])
        for d in drifts:
            if isinstance(d, dict) and not d.get("stage"):
                d["stage"] = "prepare-confirm"
        raise SimplePlanningConfirmError(
            format_fingerprint_drift_message(drifts),
            code="fingerprint_drift",
            http_status=409,
            details={"drifts": drifts, "stage": "prepare-confirm"},
        )
    if report.get("active_plan_conflicts"):
        raise SimplePlanningConfirmError(
            "Hay OC en planes activos o entregados",
            code="active_plan_conflict",
            http_status=409,
        )
    if report.get("blocking_count"):
        raise SimplePlanningConfirmError(
            "La asignación tiene bloqueos; no se puede preparar la sesión",
            code="assignment_blocking",
            http_status=409,
        )

    trucks = report.get("trucks") or []
    if len(trucks) != 1:
        raise SimplePlanningConfirmError("Se esperaba exactamente un camión en el preview")
    truck_block = trucks[0]
    if truck_block.get("truck_missing") or truck_block.get("truck_inactive"):
        raise SimplePlanningConfirmError(
            "Camión inexistente o inactivo",
            code="truck_invalid",
            http_status=409,
        )
    capacity = truck_block.get("capacity") or {}
    truck_orders = list(truck_block.get("orders") or [])
    weight_ack = _assert_confirmable_capacity(
        capacity,
        orders=truck_orders,
        acknowledge_incomplete_weight=bool(acknowledge_incomplete_weight),
        acknowledge_unevaluable_weight=bool(acknowledge_unevaluable_weight),
    )

    orders_payload: list[dict[str, Any]] = []
    for order in truck_orders:
        folio = _int(order.get("folio"))
        doc_id = _int(order.get("document_id"))
        fp = order.get("identity_fingerprint")
        if folio is None or doc_id is None or not fp:
            raise SimplePlanningConfirmError(
                f"OC incompleta para confirmar (folio={folio})",
                code="order_incomplete",
            )
        if order.get("blocking"):
            raise SimplePlanningConfirmError(
                f"OC {folio} bloqueada",
                code="order_blocking",
                http_status=409,
            )
        orders_payload.append(
            {
                "folio": folio,
                "document_id": doc_id,
                "identity_fingerprint": str(fp),
                "fingerprint_parts": order.get("fingerprint_parts"),
                "client": order.get("client"),
                "weight": order.get("weight"),
            }
        )
    if not orders_payload:
        raise SimplePlanningConfirmError("No hay OC válidas para la sesión")

    get_conn = conn_factory or get_connection
    conn = get_conn()
    try:
        cur = conn.cursor()
        if idempotency_key:
            existing = session_repo.get_session_by_idempotency(
                cur, user_id=int(user_id), idempotency_key=str(idempotency_key)
            )
            if existing:
                status = str(existing.get("status") or "")
                if status == "confirmed" and existing.get("confirmed_plan_id"):
                    conn.commit()
                    return {
                        "status": "already_confirmed",
                        "idempotent": True,
                        "session": _public_session(existing),
                        "confirmed_plan_id": existing.get("confirmed_plan_id"),
                    }
                if status in ("open", "confirming"):
                    expires = existing.get("expires_at")
                    now = datetime.now(timezone.utc)
                    if expires and getattr(expires, "tzinfo", None) is None:
                        expires = expires.replace(tzinfo=timezone.utc)
                    if expires and expires > now:
                        conn.commit()
                        return {
                            "status": "reused",
                            "idempotent": True,
                            "session": _public_session(existing),
                        }

        session_uuid = uuid.uuid4()
        plan_session_id = f"simple-{session_uuid}"
        now = datetime.now(timezone.utc)
        weight_ack_payload = {
            **weight_ack,
            "acknowledged_by_user_id": int(user_id),
            "acknowledged_at": now.isoformat(),
        }
        row = session_repo.insert_session(
            cur,
            {
                "id": str(session_uuid),
                "user_id": int(user_id),
                "company_id": int(company_id),
                "office_id": int(office_id),
                "planning_date": pdate,
                "truck_id": int(truck_id),
                "dispatch_name": (dispatch_name or "").strip() or None,
                "driver_name": (driver_name or "").strip() or None,
                "plan_session_id": plan_session_id,
                "payload": {
                    "orders": orders_payload,
                    "capacity_snapshot": capacity,
                    "truck": truck_block.get("truck"),
                    "weight_acknowledgement": weight_ack_payload,
                },
                "status": "open",
                "expires_at": now + timedelta(minutes=SESSION_TTL_MINUTES),
                "idempotency_key": (str(idempotency_key).strip() or None)
                if idempotency_key
                else None,
            },
        )
        if weight_ack_payload.get("incomplete_weight") or weight_ack_payload.get(
            "unevaluable_weight"
        ):
            logger.info(
                "simple_planning_weight_ack user_id=%s incomplete=%s unevaluable=%s "
                "known_kg=%s truck_id=%s",
                user_id,
                weight_ack_payload.get("incomplete_weight"),
                weight_ack_payload.get("unevaluable_weight"),
                capacity.get("known_weight_kg"),
                truck_id,
            )
        conn.commit()
        return {
            "status": "created",
            "idempotent": False,
            "session": _public_session(row),
            "capacity": capacity,
            "weight_acknowledgement": weight_ack_payload,
            "notes": {
                "ttl_minutes": SESSION_TTL_MINUTES,
                "confirm_blocks": [
                    "overload",
                    "unknown_capacity",
                    "fingerprint_drift",
                    "active_or_delivered_plans",
                    "incomplete_weight_without_ack",
                    "unevaluable_weight_without_ack",
                ],
                "route_departure_not_authorized": True,
                "admin_override": False,
            },
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def confirm_simple_planning_session(
    *,
    session_id: str,
    user_id: int,
    preview_fn: Callable[..., dict[str, Any]] | None = None,
    trucks_loader: Callable[[list[int]], dict[int, dict[str, Any]]] | None = None,
    confirm_fn: Callable[..., dict[str, Any]] | None = None,
    conn_factory: Callable[[], Any] | None = None,
) -> dict[str, Any]:
    """Confirma la sesión (un camión) de forma transaccional e idempotente."""
    get_conn = conn_factory or get_connection
    confirm = confirm_fn or confirm_dispatch_plan
    conn = get_conn()
    plan_id: int | None = None
    try:
        cur = conn.cursor()
        session = session_repo.get_session_by_id_for_update(cur, session_id)
        if not session:
            raise SimplePlanningConfirmError(
                "Sesión no encontrada",
                code="session_not_found",
                http_status=404,
            )
        if int(session["user_id"]) != int(user_id):
            raise SimplePlanningConfirmError(
                "Sesión ajena: no se puede suplantar",
                code="session_forbidden",
                http_status=403,
            )

        if session.get("status") == "confirmed" and session.get("confirmed_plan_id"):
            plan_id = int(session["confirmed_plan_id"])
            conn.commit()
            data = get_dispatch_plan(plan_id) or {"plan": {"id": plan_id}}
            return {
                "status": "already_confirmed",
                "idempotent": True,
                "wrote": False,
                "planning_origin": PLANNING_ORIGIN_SIMPLE,
                "ors_metrics_calculated": False,
                "session_id": str(session["id"]),
                "plan": data.get("plan") if isinstance(data, dict) else None,
                "result": data,
            }

        now = datetime.now(timezone.utc)
        expires = session.get("expires_at")
        if expires and getattr(expires, "tzinfo", None) is None:
            expires = expires.replace(tzinfo=timezone.utc)
        if expires and expires <= now:
            session_repo.mark_session_status(
                cur, session["id"], status="expired", last_error="expired", now=now
            )
            conn.commit()
            raise SimplePlanningConfirmError(
                "Sesión expirada",
                code="session_expired",
                http_status=409,
            )
        if session.get("status") not in ("open", "confirming"):
            raise SimplePlanningConfirmError(
                f"Sesión en estado no confirmable: {session.get('status')}",
                code="session_invalid_status",
                http_status=409,
            )

        session_repo.mark_session_status(
            cur, session["id"], status="confirming", now=now
        )

        payload = session.get("payload") or {}
        if isinstance(payload, str):
            import json

            payload = json.loads(payload)
        orders_meta = list((payload or {}).get("orders") or [])
        if not orders_meta:
            raise SimplePlanningConfirmError("Sesión sin OC")

        expected_fp = {
            int(o["folio"]): str(o["identity_fingerprint"])
            for o in orders_meta
            if o.get("folio") is not None and o.get("identity_fingerprint")
        }
        expected_parts = {
            int(o["folio"]): dict(o["fingerprint_parts"])
            for o in orders_meta
            if o.get("folio") is not None
            and isinstance(o.get("fingerprint_parts"), dict)
        }
        assignments = [
            {
                "folio": o["folio"],
                "truck_id": int(session["truck_id"]),
                "driver_name": session.get("driver_name"),
                "expected_fingerprint": o.get("identity_fingerprint"),
            }
            for o in orders_meta
        ]

        # Liberar el cursor de sesión no es necesario; revalidamos en memoria
        # vía preview (lecturas) y el motor confirmará con locks advisory.
        report = preview_truck_assignments(
            company_id=int(session["company_id"]),
            office_id=int(session["office_id"]),
            planning_date=session.get("planning_date"),
            dispatch_name=session.get("dispatch_name"),
            assignments=assignments,
            expected_fingerprints=expected_fp,
            preview_fn=preview_fn,
            trucks_loader=trucks_loader,
        )
        order_items = [
            o
            for t in (report.get("trucks") or [])
            for o in (t.get("orders") or [])
        ] or [
            a.get("order")
            for a in (report.get("assignments") or [])
            if a.get("order")
        ]
        drifts = detect_fingerprint_drift(
            expected_by_folio=expected_fp,
            items=order_items,
            expected_parts_by_folio=expected_parts,
            stage="confirm",
        )
        if not drifts and report.get("fingerprint_drifts"):
            drifts = list(report.get("fingerprint_drifts") or [])
            for d in drifts:
                if isinstance(d, dict) and not d.get("stage"):
                    d["stage"] = "confirm"
        if drifts:
            session_repo.mark_session_status(
                cur,
                session["id"],
                status="failed",
                last_error="fingerprint_drift",
                now=now,
            )
            conn.commit()
            raise SimplePlanningConfirmError(
                format_fingerprint_drift_message(drifts),
                code="fingerprint_drift",
                http_status=409,
                details={"drifts": drifts, "stage": "confirm", "session_obsolete": True},
            )
        if report.get("active_plan_conflicts") or report.get("blocking_count"):
            session_repo.mark_session_status(
                cur,
                session["id"],
                status="failed",
                last_error="active_plan_conflict",
                now=now,
            )
            conn.commit()
            raise SimplePlanningConfirmError(
                "OC no disponible (plan previo, bloqueo o sin saldo de reprogramación). "
                + REPLAN_MODEL_GAP,
                code="active_plan_conflict",
                http_status=409,
            )

        truck_block = (report.get("trucks") or [None])[0]
        if not truck_block or truck_block.get("truck_missing"):
            raise SimplePlanningConfirmError(
                "Camión inválido al confirmar",
                code="truck_invalid",
                http_status=409,
            )
        capacity = truck_block.get("capacity") or {}
        session_weight_ack = (payload or {}).get("weight_acknowledgement") or {}
        try:
            _assert_confirmable_capacity(
                capacity,
                orders=list(truck_block.get("orders") or []),
                acknowledge_incomplete_weight=bool(
                    session_weight_ack.get("incomplete_weight")
                ),
                acknowledge_unevaluable_weight=bool(
                    session_weight_ack.get("unevaluable_weight")
                ),
            )
        except SimplePlanningConfirmError as exc:
            session_repo.mark_session_status(
                cur, session["id"], status="failed", last_error=exc.code, now=now
            )
            conn.commit()
            raise

        confirm_orders: list[dict[str, Any]] = []
        for order in truck_block.get("orders") or []:
            doc_id = _int(order.get("document_id"))
            if doc_id is None:
                raise SimplePlanningConfirmError("document_id ausente al confirmar")
            client = order.get("client") or {}
            confirm_orders.append(
                {
                    "oc_document_id": doc_id,
                    "oc_number": order.get("folio"),
                    "route_order": len(confirm_orders),
                    "client_id": client.get("client_id"),
                    "client_name": client.get("client_name"),
                    "address": client.get("address"),
                    "city": client.get("municipality"),
                    "oc_total_amount": order.get("total_amount"),
                }
            )

        dispatch_name = (
            (session.get("dispatch_name") or "").strip()
            or f"Simple camión {session['truck_id']}"
        )
        try:
            confirmed = confirm(
                plan_session_id=str(session["plan_session_id"]),
                truck_id=int(session["truck_id"]),
                route_name=dispatch_name,
                planning_name=dispatch_name,
                driver_count=1,
                assistant_count=0,
                driver_cost_clp=0,
                assistant_cost_clp=0,
                diesel_price_per_liter=None,
                km_total=None,
                duration_min=None,
                liters_estimated=None,
                fuel_cost_clp=0,
                ferry_cost_clp=0,
                toll_cost_clp=0,
                extras_cost_clp=0,
                crew_cost_clp=0,
                total_route_cost_clp=0,
                route_geometry=None,
                orders=confirm_orders,
                planning_date=session.get("planning_date"),
                planning_origin=PLANNING_ORIGIN_SIMPLE,
                ors_metrics_calculated=False,
                conn=conn,
                run_auto_picking=False,
            )
            plan_id = int((confirmed.get("plan") or {}).get("id"))
        except (
            OcConfirmConflictError,
            OcDocumentStateError,
            ValueError,
            SimplePlanningPreviewError,
        ) as exc:
            # Nunca confirmar parcial: rollback de inserts + marcar sesión en TX nueva.
            err_code = getattr(exc, "code", None) or (
                "oc_conflict"
                if isinstance(exc, (OcConfirmConflictError, OcDocumentStateError))
                else "confirm_rejected"
            )
            try:
                conn.rollback()
            except Exception:
                logger.exception("rollback after confirm failure")
            _mark_session_failed_new_conn(
                conn_factory=get_conn,
                session_id=session["id"],
                last_error=f"{err_code}:{str(exc)[:400]}",
            )
            raise SimplePlanningConfirmError(
                str(exc),
                code=err_code,
                http_status=409,
            ) from exc
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                logger.exception("rollback after unexpected confirm failure")
            _mark_session_failed_new_conn(
                conn_factory=get_conn,
                session_id=session["id"],
                last_error=str(exc)[:500],
            )
            raise

        if plan_id is None:
            conn.rollback()
            raise SimplePlanningConfirmError(
                "Confirmación incompleta: plan no encontrado tras insert",
                code="confirm_incomplete",
                http_status=500,
            )
        # Reabrir cursor tras confirm (cerró el suyo) sobre la misma TX.
        cur = conn.cursor()
        session_repo.mark_session_status(
            cur,
            session["id"],
            status="confirmed",
            confirmed_plan_id=plan_id,
            now=datetime.now(timezone.utc),
        )
        conn.commit()
    except SimplePlanningConfirmError:
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    # Auto-picking best-effort fuera de la TX de confirmación (igual que ORS).
    from backend.services.distribuidora.dispatch_plan_service import (
        _try_auto_generate_picking_snapshot,
    )

    if plan_id is not None:
        _try_auto_generate_picking_snapshot(plan_id)
        data = get_dispatch_plan(plan_id) or {"plan": {"id": plan_id}}
    else:
        data = {}

    return {
        "status": "confirmed",
        "idempotent": False,
        "wrote": True,
        "planning_origin": PLANNING_ORIGIN_SIMPLE,
        "ors_metrics_calculated": False,
        "session_id": str(session_id),
        "plan_id": plan_id,
        "result": data,
        "notes": {
            "ors_metrics": "NULL / costos 0 legacy; ors_metrics_calculated=false",
            "picking": "Picking definitivo solo tras facturar (generate_plan_picking)",
            "loads": "No se crean loads automáticamente",
        },
    }
