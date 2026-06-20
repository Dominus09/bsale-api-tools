"""Cuadratura operacional de plan de despacho."""

from __future__ import annotations

from typing import Any

from backend.db import get_connection
from backend.repositories.distribuidora import dispatch_plan_cuadratura_repo as cuad_repo
from backend.repositories.distribuidora import dispatch_plan_repo as plan_repo
from backend.services.distribuidora.dispatch_plan_service import (
    get_invoiced_documents,
    get_picking_by_client,
)
from backend.utils.dispatch_plan_cuadratura import (
    compute_cuadratura_result,
    observacion_required,
)


def _default_row() -> dict[str, Any]:
    return {
        "transferencia_clp": 0,
        "efectivo_clp": 0,
        "cheque_clp": 0,
        "debito_clp": 0,
        "observacion": None,
        "credit_notes": [],
        "not_loaded": [],
    }


def _venta_facturada_clp(plan_id: int) -> int:
    try:
        inv = get_invoiced_documents(plan_id)
    except Exception:
        return 0
    total = 0
    for item in inv.get("items") or []:
        if item.get("status") != "confirmed":
            continue
        try:
            total += int(round(float(item.get("document_total") or 0)))
        except (TypeError, ValueError):
            continue
    return total


def _venta_picking_clp(plan_id: int) -> int:
    try:
        pk = get_picking_by_client(plan_id)
    except Exception:
        return 0
    totals = pk.get("totals") or {}
    if totals.get("document_total_clp") is not None:
        return int(round(float(totals["document_total_clp"])))
    clients = pk.get("clients") or []
    return sum(int(round(float(c.get("document_total") or 0))) for c in clients)


def get_dispatch_plan_cuadratura(plan_id: int) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        plan = plan_repo.get_plan_by_id(cur, plan_id)
        if not plan:
            raise ValueError("Plan no encontrado")
        cur.execute(
            """
            SELECT COALESCE(SUM(oc_total_amount), 0)
            FROM distribuidora.dispatch_plan_orders
            WHERE dispatch_plan_id = %s
            """,
            (plan_id,),
        )
        venta_oc = int(round(float((cur.fetchone() or [0])[0] or 0)))
        saved = cuad_repo.get_cuadratura_row(cur, plan_id) or _default_row()
        cur.close()
    finally:
        conn.close()

    venta_facturada = _venta_facturada_clp(plan_id)
    venta_picking = _venta_picking_clp(plan_id)
    credit_notes = saved.get("credit_notes") or []
    not_loaded = saved.get("not_loaded") or []
    if isinstance(credit_notes, str):
        credit_notes = []
    if isinstance(not_loaded, str):
        not_loaded = []

    result = compute_cuadratura_result(
        venta_picking_clp=venta_picking,
        credit_notes=credit_notes if isinstance(credit_notes, list) else [],
        not_loaded=not_loaded if isinstance(not_loaded, list) else [],
        transferencia_clp=int(saved.get("transferencia_clp") or 0),
        efectivo_clp=int(saved.get("efectivo_clp") or 0),
        cheque_clp=int(saved.get("cheque_clp") or 0),
        debito_clp=int(saved.get("debito_clp") or 0),
    )

    return {
        "dispatch_plan_id": plan_id,
        "ventas": {
            "venta_oc_clp": venta_oc,
            "venta_facturada_clp": venta_facturada,
            "venta_picking_clp": venta_picking,
        },
        "pagos": {
            "transferencia_clp": int(saved.get("transferencia_clp") or 0),
            "efectivo_clp": int(saved.get("efectivo_clp") or 0),
            "cheque_clp": int(saved.get("cheque_clp") or 0),
            "debito_clp": int(saved.get("debito_clp") or 0),
        },
        "credit_notes": credit_notes,
        "not_loaded": not_loaded,
        "observacion": saved.get("observacion"),
        "resultado": result,
        "observacion_required": observacion_required(int(result["diferencia_clp"])),
    }


def save_dispatch_plan_cuadratura(plan_id: int, body: dict[str, Any]) -> dict[str, Any]:
    preview = get_dispatch_plan_cuadratura(plan_id)
    body_result = compute_cuadratura_result(
        venta_picking_clp=int(preview["ventas"]["venta_picking_clp"]),
        credit_notes=body.get("credit_notes") or [],
        not_loaded=body.get("not_loaded") or [],
        transferencia_clp=int(body.get("transferencia_clp") or 0),
        efectivo_clp=int(body.get("efectivo_clp") or 0),
        cheque_clp=int(body.get("cheque_clp") or 0),
        debito_clp=int(body.get("debito_clp") or 0),
    )
    diff = int(body_result["diferencia_clp"])
    obs = (body.get("observacion") or "").strip()
    if observacion_required(diff) and not obs:
        raise ValueError(
            "Debe ingresar una observación cuando la diferencia es distinta de cero."
        )

    credit_notes = body.get("credit_notes") or []
    not_loaded = body.get("not_loaded") or []
    if not isinstance(credit_notes, list):
        raise ValueError("credit_notes debe ser una lista")
    if not isinstance(not_loaded, list):
        raise ValueError("not_loaded debe ser una lista")

    conn = get_connection()
    try:
        cur = conn.cursor()
        if not plan_repo.get_plan_by_id(cur, plan_id):
            raise ValueError("Plan no encontrado")
        cuad_repo.upsert_cuadratura(
            cur,
            plan_id=plan_id,
            transferencia_clp=int(body.get("transferencia_clp") or 0),
            efectivo_clp=int(body.get("efectivo_clp") or 0),
            cheque_clp=int(body.get("cheque_clp") or 0),
            debito_clp=int(body.get("debito_clp") or 0),
            observacion=obs or None,
            credit_notes=credit_notes,
            not_loaded=not_loaded,
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()
    return get_dispatch_plan_cuadratura(plan_id)
