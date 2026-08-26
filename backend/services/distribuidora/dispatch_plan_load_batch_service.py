"""Asignación de pickings múltiples y órdenes posteriores al cierre operacional."""

from __future__ import annotations

from typing import Any

from backend.db import get_connection
from backend.repositories.distribuidora import dispatch_plan_load_batch_repo as batch_repo
from backend.repositories.distribuidora import dispatch_plan_picking_repo as picking_repo
from backend.repositories.distribuidora import dispatch_plan_repo as plan_repo
from backend.services.distribuidora import dispatch_plan_service as plan_svc
from backend.services.distribuidora.oc_document_chain_resolver import (
    resolve_operational_statuses_batch,
)
from backend.services.distribuidora.oc_operational_status import (
    admission_block_message,
    blocks_planning_admission,
)
from backend.utils.json_safe import serialize_value

BLOCKED_ADD_ORDER_STATUSES = frozenset({"dispatched", "delivered"})


def _admission_block_payload(status) -> dict[str, Any]:
    inv = status.confirmed_invoice or {}
    return {
        "blocked": True,
        "message": admission_block_message(status),
        "billing_status": status.billing_status,
        "billing_label_es": status.billing_label_es,
        "dispatch_closed": status.dispatch_closed,
        "planning_eligible": status.planning_eligible,
        "confirmed_invoice": inv,
        "invoice_number": inv.get("number"),
        "invoice_issued_at": inv.get("issued_at"),
        "credit_notes": status.credit_notes,
    }


def _staff_label(user: dict[str, Any] | None) -> str | None:
    if not user:
        return None
    return str(user.get("email") or user.get("name") or user.get("sub") or "").strip() or None


def ensure_default_load_batch(plan_id: int) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        if not plan_repo.get_plan_by_id(cur, plan_id):
            raise ValueError("Plan no encontrado")
        batches = batch_repo.list_load_batches(cur, plan_id)
        if not batches:
            batch_repo.insert_load_batch(
                cur,
                plan_id=plan_id,
                name="Picking 1",
                description=None,
                sort_order=1,
            )
            conn.commit()
            batches = batch_repo.list_load_batches(cur, plan_id)
        cur.close()
    finally:
        conn.close()
    return serialize_value({"dispatch_plan_id": plan_id, "items": batches})


def list_load_batches(plan_id: int) -> dict[str, Any]:
    ensure_default_load_batch(plan_id)
    conn = get_connection()
    try:
        cur = conn.cursor()
        items = batch_repo.list_load_batches(cur, plan_id)
        cur.close()
    finally:
        conn.close()
    return serialize_value({"dispatch_plan_id": plan_id, "items": items})


def create_load_batch(
    plan_id: int,
    *,
    name: str,
    description: str | None = None,
) -> dict[str, Any]:
    nm = (name or "").strip()
    if not nm:
        raise ValueError("El nombre del picking es obligatorio.")
    conn = get_connection()
    try:
        cur = conn.cursor()
        if not plan_repo.get_plan_by_id(cur, plan_id):
            raise ValueError("Plan no encontrado")
        sort_order = batch_repo.next_batch_sort_order(cur, plan_id)
        row = batch_repo.insert_load_batch(
            cur,
            plan_id=plan_id,
            name=nm,
            description=description,
            sort_order=sort_order,
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()
    return serialize_value(row)


def update_load_batch(
    plan_id: int,
    batch_id: int,
    *,
    name: str,
    description: str | None = None,
    sort_order: int | None = None,
) -> dict[str, Any]:
    nm = (name or "").strip()
    if not nm:
        raise ValueError("El nombre del picking es obligatorio.")
    conn = get_connection()
    try:
        cur = conn.cursor()
        row = batch_repo.update_load_batch(
            cur,
            plan_id=plan_id,
            batch_id=batch_id,
            name=nm,
            description=description,
            sort_order=sort_order,
        )
        if not row:
            raise ValueError("Grupo de picking no encontrado")
        conn.commit()
        cur.close()
    finally:
        conn.close()
    return serialize_value(row)


def delete_load_batch(plan_id: int, batch_id: int) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        if not batch_repo.delete_load_batch(cur, plan_id=plan_id, batch_id=batch_id):
            raise ValueError("Grupo de picking no encontrado")
        conn.commit()
        cur.close()
    finally:
        conn.close()
    return serialize_value({"ok": True, "dispatch_plan_id": plan_id, "batch_id": batch_id})


def _sync_assignments_from_picking(cur, plan_id: int) -> None:
    row = picking_repo.get_picking_row(cur, plan_id, current_only=True)
    if not row:
        return
    clients = picking_repo.list_picking_clients(cur, int(row["id"]))
    for c in clients:
        batch_repo.upsert_document_assignment(
            cur,
            plan_id=plan_id,
            related_document_id=int(c["related_document_id"]),
            load_batch_id=None,
            oc_document_id=int(c["oc_document_id"]) if c.get("oc_document_id") else None,
            document_number=int(c["document_number"]) if c.get("document_number") else None,
            client_name=c.get("client_name") or c.get("fantasy_name"),
            document_total=float(c["document_total"]) if c.get("document_total") is not None else None,
        )


def get_picking_assignments(plan_id: int) -> dict[str, Any]:
    ensure_default_load_batch(plan_id)
    conn = get_connection()
    try:
        cur = conn.cursor()
        _sync_assignments_from_picking(cur, plan_id)
        batches = batch_repo.list_load_batches(cur, plan_id)
        assignments = batch_repo.list_document_assignments(cur, plan_id)
        has_picking = batch_repo.plan_has_current_picking(cur, plan_id)
        picking_row = picking_repo.get_picking_row(cur, plan_id, current_only=True)
        conn.commit()
        cur.close()
    finally:
        conn.close()
    return serialize_value(
        {
            "dispatch_plan_id": plan_id,
            "batches": batches,
            "assignments": assignments,
            "has_picking": has_picking,
            "picking_version": int(picking_row["version"]) if picking_row else None,
            "picking_id": int(picking_row["id"]) if picking_row else None,
        }
    )


def save_picking_assignments(
    plan_id: int,
    assignments: list[dict[str, Any]],
) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        if not plan_repo.get_plan_by_id(cur, plan_id):
            raise ValueError("Plan no encontrado")
        batch_ids = {int(b["id"]) for b in batch_repo.list_load_batches(cur, plan_id)}
        for a in assignments:
            rel_id = int(a["related_document_id"])
            batch_id = a.get("load_batch_id")
            if batch_id is not None:
                bid = int(batch_id)
                if bid not in batch_ids:
                    raise ValueError(f"Picking asignado inválido: {bid}")
            batch_repo.upsert_document_assignment(
                cur,
                plan_id=plan_id,
                related_document_id=rel_id,
                load_batch_id=int(batch_id) if batch_id is not None else None,
                oc_document_id=int(a["oc_document_id"]) if a.get("oc_document_id") else None,
                document_number=int(a["document_number"]) if a.get("document_number") else None,
                client_name=a.get("client_name"),
                document_total=float(a["document_total"]) if a.get("document_total") is not None else None,
            )
        conn.commit()
        cur.close()
    finally:
        conn.close()
    return get_picking_assignments(plan_id)


def search_orders_for_plan(plan_id: int, *, q: str, limit: int = 20) -> dict[str, Any]:
    q = (q or "").strip()
    if len(q) < 2:
        raise ValueError("Ingrese al menos 2 caracteres para buscar.")
    conn = get_connection()
    try:
        cur = conn.cursor()
        if not plan_repo.get_plan_by_id(cur, plan_id):
            raise ValueError("Plan no encontrado")
        items = batch_repo.search_orders_not_in_plan(cur, plan_id, q=q, limit=limit)
        cur.close()
    finally:
        conn.close()
    return serialize_value({"dispatch_plan_id": plan_id, "q": q, "items": items})


def list_picking_regeneration_log(plan_id: int, *, limit: int = 50) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        items = batch_repo.list_order_events(cur, plan_id, limit=limit)
        cur.close()
    finally:
        conn.close()
    return serialize_value({"dispatch_plan_id": plan_id, "items": items})


def preview_add_order(plan_id: int) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        plan = plan_repo.get_plan_by_id(cur, plan_id)
        if not plan:
            raise ValueError("Plan no encontrado")
        status = str(plan.get("status") or "")
        has_picking = batch_repo.plan_has_current_picking(cur, plan_id)
        cur.close()
    finally:
        conn.close()
    blocked = status in BLOCKED_ADD_ORDER_STATUSES
    warning = None
    if has_picking:
        warning = (
            "Esta planificación ya posee pickings generados. "
            "Agregar nuevas órdenes obligará a regenerar los documentos."
        )
    return serialize_value(
        {
            "dispatch_plan_id": plan_id,
            "can_add": not blocked,
            "blocked_reason": (
                "No se pueden agregar órdenes a un plan ya despachado."
                if blocked
                else None
            ),
            "has_picking": has_picking,
            "warning": warning,
        }
    )


def add_order_to_plan(
    plan_id: int,
    *,
    oc_document_id: int,
    regenerate_picking: bool = False,
    reason: str | None = None,
    user: dict[str, Any] | None = None,
) -> dict[str, Any]:
    preview = preview_add_order(plan_id)
    if not preview.get("can_add"):
        raise ValueError(preview.get("blocked_reason") or "No se puede agregar la orden.")
    has_picking = bool(preview.get("has_picking"))
    if has_picking and not regenerate_picking:
        return serialize_value(
            {
                **preview,
                "requires_regenerate": True,
                "added": False,
            }
        )

    conn = get_connection()
    try:
        cur = conn.cursor()
        plan = plan_repo.get_plan_by_id(cur, plan_id)
        if not plan:
            raise ValueError("Plan no encontrado")
        cur.execute(
            """
            SELECT 1 FROM distribuidora.dispatch_plan_orders
            WHERE dispatch_plan_id = %s AND oc_document_id = %s
            """,
            (plan_id, oc_document_id),
        )
        if cur.fetchone():
            raise ValueError("La OC ya pertenece a este plan.")
        status_map = resolve_operational_statuses_batch(cur, [oc_document_id])
        oc_status = status_map.get(int(oc_document_id))
        if oc_status is not None and blocks_planning_admission(oc_status):
            detail = _admission_block_payload(oc_status)
            raise ValueError(
                f"{detail['message']} "
                f"(factura={detail.get('invoice_number')}, "
                f"estado={detail.get('billing_label_es')})"
            )
        route_order = batch_repo.max_route_order(cur, plan_id) + 1
        order = {"oc_document_id": oc_document_id, "route_order": route_order}
        enriched = plan_svc._enrich_orders_snapshot(cur, [order])
        plan_repo.insert_plan_orders(cur, plan_id, enriched)
        oc_row = enriched[0]
        batch_repo.insert_order_event(
            cur,
            plan_id=plan_id,
            action="add_order",
            user_name=_staff_label(user),
            reason=reason,
            oc_document_id=oc_document_id,
            oc_number=int(oc_row["oc_number"]) if oc_row.get("oc_number") else None,
            payload={"route_order": route_order},
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()

    picking_result = None
    if has_picking and regenerate_picking:
        picking_result = plan_svc.generate_plan_picking(
            plan_id,
            validate=False,
            include_probable=False,
            regenerated_by=_staff_label(user),
            regeneration_reason=reason or "Orden agregada al plan",
            regeneration_action="add_order",
        )

    return serialize_value(
        {
            "dispatch_plan_id": plan_id,
            "added": True,
            "oc_document_id": oc_document_id,
            "requires_regenerate": False,
            "picking": picking_result,
            "plan": plan_svc.get_dispatch_plan(plan_id),
        }
    )


def filter_picking_by_load_batch(
    picking_payload: dict[str, Any],
    plan_id: int,
    load_batch_id: int,
) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        rel_ids = batch_repo.related_document_ids_for_batch(cur, plan_id, load_batch_id)
        batch_row = None
        for b in batch_repo.list_load_batches(cur, plan_id):
            if int(b["id"]) == load_batch_id:
                batch_row = b
                break
        include_probable = bool(picking_payload.get("include_probable"))
        items = plan_svc.compute_picking_products_for_related_documents(
            plan_id,
            rel_ids,
            include_probable=include_probable,
        )
        cur.close()
    finally:
        conn.close()

    if not batch_row:
        raise ValueError("Grupo de picking no encontrado")

    rel_set = set(rel_ids)
    clients = [
        c
        for c in (picking_payload.get("clients") or [])
        if int(c.get("related_document_id") or 0) in rel_set
    ]
    header = dict(picking_payload.get("header") or {})
    doc_total = sum(float(c.get("document_total") or 0) for c in clients)
    header["load_kpis"] = {
        "clients": len({c.get("client_id") for c in clients if c.get("client_id")}),
        "documents": len(clients),
        "sales_total_clp": int(round(doc_total)),
        "distinct_products": len(items),
        "total_units": sum(float(i.get("unidades") or 0) for i in items),
        "estimated_boxes": sum(
            float(i.get("cajas") or 0) for i in items if not i.get("sin_unidad_caja")
        ),
    }
    header["load_batch"] = {
        "id": int(batch_row["id"]),
        "name": batch_row["name"],
        "description": batch_row.get("description"),
    }
    out = dict(picking_payload)
    out["clients"] = clients
    out["items"] = items
    out["header"] = header
    out["load_batch_id"] = load_batch_id
    out["load_batch_name"] = batch_row["name"]
    if "totals" in out and out["totals"]:
        out["totals"] = {
            **out["totals"],
            "stops": len(clients),
            "clients": len({c.get("client_id") for c in clients if c.get("client_id")}) or len(clients),
            "document_total_clp": doc_total,
            "lines": len(items),
            "unidades": sum(float(i.get("unidades") or 0) for i in items),
            "cajas": sum(float(i.get("cajas") or 0) for i in items if i.get("cajas") is not None),
            "total_monto_clp": sum(float(i.get("total_monto") or 0) for i in items),
        }
    return serialize_value(out)
