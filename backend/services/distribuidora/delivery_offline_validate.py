"""Validación real Offline V2 — picking/docs desde servicios oficiales.

No confía en IndexedDB. No muta Bsale/picking/stock.
"""

from __future__ import annotations

from typing import Any, Callable

from fastapi import HTTPException

from backend.services.distribuidora import dispatch_plan_pwa_driver_read_service as read_svc
from backend.services.distribuidora import dispatch_plan_service as plan_svc
from backend.services.distribuidora import delivery_visit_service as visit_svc
from backend.repositories.distribuidora import delivery_visit_repo as visit_repo
from backend.db import get_connection

ERR_PICKING_CHANGED = "PICKING_CHANGED"
ERR_PICKING_NOT_READY = "PICKING_NOT_READY"
ERR_DOC_MISMATCH = "DOCUMENT_MISMATCH"
ERR_CLIENT_NOT_IN_PLAN = "CLIENT_NOT_IN_PLAN"
ERR_PLAN_STATUS = "PLAN_NOT_OPERABLE"
ERR_VISIT_INCOMPATIBLE = "VISIT_INCOMPATIBLE"

LiveContextFetcher = Callable[[int, str], dict[str, Any]]


def _http_conflict(code: str, message_es: str, **extra: Any) -> HTTPException:
    return HTTPException(
        status_code=409,
        detail={
            "code": code,
            "message": message_es,
            "message_es": message_es,
            **extra,
        },
    )


def fetch_live_offline_context(plan_id: int, customer_key: str) -> dict[str, Any]:
    """Consulta oficial: picking vigente + ficha del cliente (solo lectura)."""
    picking = plan_svc.get_picking_by_client(int(plan_id))
    ficha = read_svc.get_client_ficha_for_driver(int(plan_id), customer_key)
    plan_status = None
    try:
        # Estado del despacho si está disponible en ficha/bundle
        plan_status = (ficha.get("dispatch_plan") or {}).get("status")
    except Exception:
        plan_status = None
    return {
        "picking_ready": bool(picking.get("ready")),
        "picking_reason": picking.get("reason"),
        "picking_id": picking.get("picking_id"),
        "picking_version": (
            str(picking.get("version"))
            if picking.get("version") is not None
            else None
        ),
        "is_current": picking.get("is_current", True if picking.get("ready") else None),
        "plan_status": plan_status,
        "ficha": ficha,
        "customer_key": customer_key,
    }


def _norm_ver(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def validate_offline_body_against_context(
    body: dict[str, Any],
    ctx: dict[str, Any],
    *,
    op_type: str,
) -> dict[str, Any]:
    """Compara payload offline vs contexto vivo. Lanza HTTPException en conflicto."""
    if not ctx.get("picking_ready"):
        raise _http_conflict(
            ERR_PICKING_NOT_READY,
            "No hay picking vigente para sincronizar esta operación",
            picking_reason=ctx.get("picking_reason"),
        )

    is_current = ctx.get("is_current")
    if is_current is False:
        raise _http_conflict(
            ERR_PICKING_CHANGED,
            "El picking ya no es el vigente (is_current=false). No se aplica la entrega offline.",
            live_picking_id=ctx.get("picking_id"),
            live_picking_version=ctx.get("picking_version"),
        )

    body_pid = body.get("picking_id")
    body_pver = _norm_ver(body.get("picking_version"))
    live_pid = ctx.get("picking_id")
    live_pver = _norm_ver(ctx.get("picking_version"))

    if body_pid is None or body_pver is None:
        raise HTTPException(
            status_code=422,
            detail={
                "code": ERR_PICKING_CHANGED,
                "message_es": "Operación offline sin picking_id/version; no se puede reconciliar",
            },
        )

    if int(body_pid) != int(live_pid or -1) or body_pver != live_pver:
        raise _http_conflict(
            ERR_PICKING_CHANGED,
            "El picking cambió desde la captura offline. No se aplica automáticamente.",
            captured_picking_id=int(body_pid),
            captured_picking_version=body_pver,
            live_picking_id=live_pid,
            live_picking_version=live_pver,
        )

    ficha = ctx.get("ficha") or {}
    client = ficha.get("client") or {}
    if not client:
        raise _http_conflict(
            ERR_CLIENT_NOT_IN_PLAN,
            "Cliente no pertenece al despacho o no está en el picking vigente",
        )

    plan_status = (ctx.get("plan_status") or ficha.get("plan_status") or "").strip()
    if plan_status and plan_status.lower() in {"cancelled", "canceled", "void", "anulado"}:
        raise _http_conflict(
            ERR_PLAN_STATUS,
            f"Despacho no operable (status={plan_status})",
            plan_status=plan_status,
        )

    if op_type == "complete_delivery":
        _validate_complete_documents(body, ficha)

    return {
        "ok": True,
        "picking_id": live_pid,
        "picking_version": live_pver,
        "client_id": client.get("client_id"),
    }


def _validate_complete_documents(body: dict[str, Any], ficha: dict[str, Any]) -> None:
    live_docs = ficha.get("documents") or []
    live_by_rel: dict[int, dict[str, Any]] = {}
    for d in live_docs:
        rid = d.get("related_document_id")
        if rid is None:
            continue
        live_by_rel[int(rid)] = d

    body_docs = body.get("documents") if isinstance(body.get("documents"), list) else []
    if not body_docs:
        raise HTTPException(
            status_code=422,
            detail={
                "code": ERR_DOC_MISMATCH,
                "message_es": "Entrega completa requiere documentos del snapshot",
            },
        )

    for doc in body_docs:
        rid = doc.get("related_document_id")
        if rid is None:
            raise _http_conflict(
                ERR_DOC_MISMATCH,
                "Documento sin related_document_id en operación offline",
            )
        live = live_by_rel.get(int(rid))
        if not live:
            raise _http_conflict(
                ERR_DOC_MISMATCH,
                f"Documento {rid} ya no está en el picking vigente",
                related_document_id=int(rid),
            )
        # Cantidades: solo líneas con source_detail_id confiable en vivo
        live_products = live.get("products") or []
        live_lines: dict[int, float] = {}
        for p in live_products:
            sid = p.get("source_detail_id")
            if sid is None:
                continue
            units = p.get("unidades")
            if units is None:
                units = p.get("planned_units")
            try:
                live_lines[int(sid)] = float(units or 0)
            except (TypeError, ValueError):
                continue
        if not live_lines:
            raise _http_conflict(
                ERR_DOC_MISMATCH,
                f"Documento {rid} sin líneas confiables (source_detail_id) en picking vigente",
                related_document_id=int(rid),
            )

        body_lines = doc.get("lines") if isinstance(doc.get("lines"), list) else []
        body_sids: dict[int, float] = {}
        for ln in body_lines:
            sid = ln.get("source_detail_id")
            if sid is None:
                raise _http_conflict(
                    ERR_DOC_MISMATCH,
                    "Línea offline sin source_detail_id",
                )
            planned = ln.get("planned_units")
            if planned is None:
                planned = ln.get("delivered_units")
            body_sids[int(sid)] = float(planned or 0)

        if set(body_sids.keys()) != set(live_lines.keys()):
            raise _http_conflict(
                ERR_DOC_MISMATCH,
                f"Las líneas del documento {rid} no coinciden con el picking vigente",
                related_document_id=int(rid),
                captured_detail_ids=sorted(body_sids.keys()),
                live_detail_ids=sorted(live_lines.keys()),
            )
        for sid, planned in body_sids.items():
            live_u = live_lines[sid]
            if abs(planned - live_u) > 0.0001:
                raise _http_conflict(
                    ERR_DOC_MISMATCH,
                    f"Cantidad planificada distinta en detail_id={sid} (doc {rid})",
                    source_detail_id=sid,
                    captured_units=planned,
                    live_units=live_u,
                )


def assert_visit_compatible_for_offline(
    *,
    plan_id: int,
    customer_key: str,
    driver_user_id: int,
    allow_closed_visit_id: int | None = None,
) -> None:
    """Visita cerrada/anulada o de otro chofer bloquea nueva op.

    ``allow_closed_visit_id``: reanudación de la misma op offline que ya cerró
    esa visita (respuesta HTTP perdida / reinicio).
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        closed = visit_repo.get_valid_closed_visit(
            cur, plan_id=int(plan_id), customer_key=str(customer_key)
        )
        if visit_svc.is_valid_closed_visit(closed):
            if (
                allow_closed_visit_id is not None
                and int(closed["id"]) == int(allow_closed_visit_id)
            ):
                cur.close()
                return
            raise _http_conflict(
                "VISIT_ALREADY_CLOSED",
                "Visita ya cerrada en servidor; no se acepta operación offline",
                closed_visit_id=int(closed["id"]),
            )
        active = visit_repo.get_active_visit(
            cur, plan_id=int(plan_id), customer_key=str(customer_key)
        )
        if active and int(active.get("driver_user_id") or 0) != int(driver_user_id):
            raise _http_conflict(
                ERR_VISIT_INCOMPATIBLE,
                "Hay una visita en atención de otro chofer",
                active_visit_id=int(active["id"]),
            )
        cur.close()
    finally:
        conn.close()


def validate_offline_operation(
    *,
    user: dict[str, Any],
    plan_id: int,
    body: dict[str, Any],
    context_fetcher: LiveContextFetcher | None = None,
    allow_closed_visit_id: int | None = None,
) -> dict[str, Any]:
    """Validador real por defecto. context_fetcher solo para fixtures de laboratorio."""
    uid = read_svc.enforce_driver_plan_read_access(user, int(plan_id))
    customer_key = str(body.get("customer_key") or "").strip()
    if not customer_key:
        raise HTTPException(status_code=400, detail="customer_key requerido")
    op_type = str(body.get("op_type") or "").strip()

    fetcher = context_fetcher or fetch_live_offline_context
    try:
        ctx = fetcher(int(plan_id), customer_key)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"No se pudo validar picking vigente: {exc}",
        ) from exc

    result = validate_offline_body_against_context(body, ctx, op_type=op_type)
    assert_visit_compatible_for_offline(
        plan_id=int(plan_id),
        customer_key=customer_key,
        driver_user_id=uid,
        allow_closed_visit_id=allow_closed_visit_id,
    )
    result["driver_user_id"] = uid
    return result
