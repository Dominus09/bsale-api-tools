"""Cuadratura operacional de plan de despacho (v1 legacy + v2 documental)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from backend.db import get_connection
from backend.repositories.distribuidora import dispatch_plan_cuadratura_repo as cuad_repo
from backend.repositories.distribuidora import dispatch_plan_repo as plan_repo
from backend.services.distribuidora.dispatch_plan_service import (
    get_invoiced_documents,
    get_picking_by_client,
    get_picking_by_product,
)
from backend.utils.dispatch_plan_cuadratura import (
    compute_cuadratura_result as compute_cuadratura_v1,
    observacion_required as observacion_required_v1,
)
from backend.utils.dispatch_plan_cuadratura_snapshot import (
    build_documents_from_picking_clients,
    build_product_catalog_from_picking,
    enrich_not_loaded_rows,
)
from backend.utils.dispatch_plan_cuadratura_v2 import (
    MEDIO_PAGO_LABELS,
    MEDIOS_PAGO,
    build_analytics_meta,
    compute_cuadratura_v2_result,
    derive_operational_status,
    observacion_required,
    operational_status_label,
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
        "schema_version": 1,
        "status": "pending",
        "documents": [],
        "credit_notes_v2": [],
        "not_loaded_v2": [],
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


def _load_picking_bundle(plan_id: int) -> dict[str, Any]:
    client_data = get_picking_by_client(plan_id)
    product_data = get_picking_by_product(plan_id)
    clients = client_data.get("clients") or []
    items = product_data.get("items") or []
    totals = client_data.get("totals") or {}
    venta_picking = int(round(float(totals.get("document_total_clp") or 0)))
    if not venta_picking:
        venta_picking = sum(
            int(round(float(c.get("document_total") or 0))) for c in clients
        )
    return {
        "ready": bool(clients),
        "clients": clients,
        "items": items,
        "venta_picking_clp": venta_picking,
        "picking_id": client_data.get("picking_id"),
        "picking_version": client_data.get("version"),
        "header": client_data.get("header") or {},
    }


def _venta_picking_clp(plan_id: int) -> int:
    return _load_picking_bundle(plan_id)["venta_picking_clp"]


def _build_v2_payload(
    *,
    plan_id: int,
    plan: dict[str, Any],
    saved: dict[str, Any],
    picking: dict[str, Any],
) -> dict[str, Any]:
    documents = build_documents_from_picking_clients(
        picking["clients"],
        saved_documents=saved.get("documents") or [],
    )
    catalog = build_product_catalog_from_picking(picking["items"])
    not_loaded_v2 = enrich_not_loaded_rows(
        saved.get("not_loaded_v2") or [],
        catalog,
    )
    credit_notes_v2 = saved.get("credit_notes_v2") or []
    venta_picking = picking["venta_picking_clp"]
    resultado = compute_cuadratura_v2_result(
        venta_picking_clp=venta_picking,
        documents=documents,
        credit_notes=credit_notes_v2,
        not_loaded=not_loaded_v2,
    )
    has_work = bool(saved.get("updated_at")) or bool(saved.get("documents"))
    op_status = saved.get("status") or derive_operational_status(
        resultado=resultado,
        closed_at=saved.get("closed_at"),
        has_work=has_work,
    )
    return {
        "schema_version": 2,
        "documents": documents,
        "credit_notes_v2": credit_notes_v2,
        "not_loaded_v2": not_loaded_v2,
        "product_catalog": catalog,
        "resultado": resultado,
        "operational_status": op_status,
        "operational_status_label": operational_status_label(str(op_status)),
        "picking_id": picking.get("picking_id"),
        "picking_version": picking.get("picking_version"),
        "venta_picking_clp": venta_picking,
        "analytics_meta": build_analytics_meta(
            plan=plan,
            resultado=resultado,
            documents=documents,
            credit_notes=credit_notes_v2,
            not_loaded=not_loaded_v2,
        ),
    }


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
        history = cuad_repo.list_cuadratura_history(cur, plan_id)
        cur.close()
    finally:
        conn.close()

    picking = _load_picking_bundle(plan_id)
    venta_facturada = _venta_facturada_clp(plan_id)
    venta_picking = picking["venta_picking_clp"]

    schema_version = int(saved.get("schema_version") or 1)
    if schema_version >= 2 or picking["ready"]:
        v2 = _build_v2_payload(
            plan_id=plan_id,
            plan=plan,
            saved=saved,
            picking=picking,
        )
        resultado = v2["resultado"]
        return {
            "dispatch_plan_id": plan_id,
            "schema_version": 2,
            "picking_ready": picking["ready"],
            "picking_id": v2["picking_id"],
            "picking_version": v2["picking_version"],
            "header": picking.get("header") or {},
            "ventas": {
                "venta_oc_clp": venta_oc,
                "venta_facturada_clp": venta_facturada,
                "venta_picking_clp": venta_picking,
            },
            "documents": v2["documents"],
            "resumen_pagos": resultado["resumen_pagos"],
            "resumen_pagos_labels": MEDIO_PAGO_LABELS,
            "medios_pago_options": list(MEDIOS_PAGO),
            "credit_notes_v2": v2["credit_notes_v2"],
            "not_loaded_v2": v2["not_loaded_v2"],
            "product_catalog": v2["product_catalog"],
            "observacion": saved.get("observacion"),
            "operational_status": v2["operational_status"],
            "operational_status_label": v2["operational_status_label"],
            "closed_at": saved.get("closed_at"),
            "closed_by": saved.get("closed_by"),
            "resultado": resultado,
            "observacion_required": observacion_required(int(resultado["diferencia_clp"])),
            "history": history,
            "analytics_meta": v2["analytics_meta"],
            "legacy": _legacy_block(saved, venta_picking),
        }

    return _legacy_response(plan_id, venta_oc, venta_facturada, venta_picking, saved)


def _legacy_block(saved: dict[str, Any], venta_picking: int) -> dict[str, Any]:
    credit_notes = saved.get("credit_notes") or []
    not_loaded = saved.get("not_loaded") or []
    result = compute_cuadratura_v1(
        venta_picking_clp=venta_picking,
        credit_notes=credit_notes if isinstance(credit_notes, list) else [],
        not_loaded=not_loaded if isinstance(not_loaded, list) else [],
        transferencia_clp=int(saved.get("transferencia_clp") or 0),
        efectivo_clp=int(saved.get("efectivo_clp") or 0),
        cheque_clp=int(saved.get("cheque_clp") or 0),
        debito_clp=int(saved.get("debito_clp") or 0),
    )
    return {
        "pagos": {
            "transferencia_clp": int(saved.get("transferencia_clp") or 0),
            "efectivo_clp": int(saved.get("efectivo_clp") or 0),
            "cheque_clp": int(saved.get("cheque_clp") or 0),
            "debito_clp": int(saved.get("debito_clp") or 0),
        },
        "credit_notes": credit_notes,
        "not_loaded": not_loaded,
        "resultado": result,
    }


def _legacy_response(
    plan_id: int,
    venta_oc: int,
    venta_facturada: int,
    venta_picking: int,
    saved: dict[str, Any],
) -> dict[str, Any]:
    credit_notes = saved.get("credit_notes") or []
    not_loaded = saved.get("not_loaded") or []
    if isinstance(credit_notes, str):
        credit_notes = []
    if isinstance(not_loaded, str):
        not_loaded = []
    result = compute_cuadratura_v1(
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
        "schema_version": 1,
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
        "observacion_required": observacion_required_v1(int(result["diferencia_clp"])),
    }


def save_dispatch_plan_cuadratura(plan_id: int, body: dict[str, Any]) -> dict[str, Any]:
    if body.get("documents") is not None or body.get("schema_version") == 2:
        return save_dispatch_plan_cuadratura_v2(plan_id, body)
    return _save_dispatch_plan_cuadratura_v1(plan_id, body)


def save_dispatch_plan_cuadratura_v2(plan_id: int, body: dict[str, Any]) -> dict[str, Any]:
    preview = get_dispatch_plan_cuadratura(plan_id)
    if not preview.get("picking_ready"):
        raise ValueError("Se requiere picking generado para cuadratura documental.")

    picking = _load_picking_bundle(plan_id)
    catalog = build_product_catalog_from_picking(picking["items"])
    documents = body.get("documents")
    if documents is None:
        documents = build_documents_from_picking_clients(
            picking["clients"],
            saved_documents=preview.get("documents") or [],
        )
    credit_notes_v2 = body.get("credit_notes_v2") or body.get("credit_notes") or []
    not_loaded_v2 = enrich_not_loaded_rows(
        body.get("not_loaded_v2") or body.get("not_loaded") or [],
        catalog,
    )
    venta_picking = int(preview["ventas"]["venta_picking_clp"])
    resultado = compute_cuadratura_v2_result(
        venta_picking_clp=venta_picking,
        documents=documents,
        credit_notes=credit_notes_v2,
        not_loaded=not_loaded_v2,
    )
    diff = int(resultado["diferencia_clp"])
    obs = (body.get("observacion") or "").strip()
    if observacion_required(diff) and not obs:
        raise ValueError(
            "Debe ingresar una observación cuando la diferencia es distinta de cero."
        )

    op_status = derive_operational_status(
        resultado=resultado,
        closed_at=None,
        has_work=True,
    )

    conn = get_connection()
    try:
        cur = conn.cursor()
        if not plan_repo.get_plan_by_id(cur, plan_id):
            raise ValueError("Plan no encontrado")
        cuad_repo.upsert_cuadratura_v2(
            cur,
            plan_id=plan_id,
            observacion=obs or None,
            documents=documents,
            credit_notes_v2=credit_notes_v2,
            not_loaded_v2=not_loaded_v2,
            picking_id=picking.get("picking_id"),
            picking_version=picking.get("picking_version"),
            status=str(op_status),
            resultado_cache=resultado,
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()
    return get_dispatch_plan_cuadratura(plan_id)


def close_dispatch_plan_cuadratura(
    plan_id: int,
    *,
    closed_by: str | None = None,
    observacion: str | None = None,
) -> dict[str, Any]:
    data = get_dispatch_plan_cuadratura(plan_id)
    if int(data.get("schema_version") or 1) < 2:
        raise ValueError("Cuadratura v2 requerida para cierre.")
    diff = int(data["resultado"]["diferencia_clp"])
    obs = (observacion or data.get("observacion") or "").strip()
    if observacion_required(diff) and not obs:
        raise ValueError(
            "No se puede cerrar la cuadratura sin observación cuando hay diferencia."
        )
    op_status = "squared" if diff == 0 else "difference"
    now = datetime.now(timezone.utc)
    snapshot = {
        "ventas": data.get("ventas"),
        "documents": data.get("documents"),
        "resumen_pagos": data.get("resumen_pagos"),
        "credit_notes_v2": data.get("credit_notes_v2"),
        "not_loaded_v2": data.get("not_loaded_v2"),
        "resultado": data.get("resultado"),
        "observacion": obs,
        "picking_id": data.get("picking_id"),
        "picking_version": data.get("picking_version"),
    }

    conn = get_connection()
    try:
        cur = conn.cursor()
        version = cuad_repo.next_history_version(cur, plan_id)
        cuad_repo.insert_cuadratura_history(
            cur,
            plan_id=plan_id,
            version=version,
            status=op_status,
            snapshot=snapshot,
            closed_by=closed_by,
            observacion=obs,
            diferencia_clp=diff,
            diferencia_status=str(data["resultado"]["diferencia_status"]),
        )
        cuad_repo.upsert_cuadratura_v2(
            cur,
            plan_id=plan_id,
            observacion=obs,
            documents=data.get("documents") or [],
            credit_notes_v2=data.get("credit_notes_v2") or [],
            not_loaded_v2=data.get("not_loaded_v2") or [],
            picking_id=data.get("picking_id"),
            picking_version=data.get("picking_version"),
            status=op_status,
            resultado_cache=data.get("resultado") or {},
            closed_at=now,
            closed_by=closed_by,
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()
    return get_dispatch_plan_cuadratura(plan_id)


def list_cuadraturas(
    *,
    status: str = "all",
    search: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        items = cuad_repo.list_cuadraturas(
            cur,
            status_filter=status,
            search=search,
            limit=limit,
            offset=offset,
        )
        cur.close()
    finally:
        conn.close()
    for item in items:
        st = item.get("cuadratura_status") or "pending"
        item["operational_status_label"] = operational_status_label(str(st))
    return {"items": items, "count": len(items)}


def _save_dispatch_plan_cuadratura_v1(plan_id: int, body: dict[str, Any]) -> dict[str, Any]:
    preview = get_dispatch_plan_cuadratura(plan_id)
    legacy = preview.get("legacy") or preview
    venta_picking = int(
        preview.get("ventas", {}).get("venta_picking_clp")
        or legacy.get("ventas", {}).get("venta_picking_clp")
        or _venta_picking_clp(plan_id)
    )
    body_result = compute_cuadratura_v1(
        venta_picking_clp=venta_picking,
        credit_notes=body.get("credit_notes") or [],
        not_loaded=body.get("not_loaded") or [],
        transferencia_clp=int(body.get("transferencia_clp") or 0),
        efectivo_clp=int(body.get("efectivo_clp") or 0),
        cheque_clp=int(body.get("cheque_clp") or 0),
        debito_clp=int(body.get("debito_clp") or 0),
    )
    diff = int(body_result["diferencia_clp"])
    obs = (body.get("observacion") or "").strip()
    if observacion_required_v1(diff) and not obs:
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
