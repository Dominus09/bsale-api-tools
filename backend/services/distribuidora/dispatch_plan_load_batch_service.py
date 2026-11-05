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
    from backend.services.distribuidora.dispatch_plan_load_organization_service import (
        merge_description_preserving_role,
    )

    conn = get_connection()
    try:
        cur = conn.cursor()
        existing_rows = [
            b for b in batch_repo.list_load_batches(cur, plan_id) if int(b["id"]) == batch_id
        ]
        if not existing_rows:
            raise ValueError("Grupo de picking no encontrado")
        merged_description = merge_description_preserving_role(
            existing_rows[0].get("description"), description
        )
        row = batch_repo.update_load_batch(
            cur,
            plan_id=plan_id,
            batch_id=batch_id,
            name=nm,
            description=merged_description,
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
        # Fase F: no borrar un grupo con OC pre-factura asignadas (RESTRICT en 053).
        if batch_repo.count_order_load_assignments_for_batch(
            cur, plan_id=plan_id, batch_id=batch_id
        ):
            raise ValueError(
                "No se puede eliminar el grupo: tiene OC asignadas en la organización "
                "de carga. Reasigne o quite las OC antes de eliminar."
            )
        if not batch_repo.delete_load_batch(cur, plan_id=plan_id, batch_id=batch_id):
            raise ValueError("Grupo de picking no encontrado")
        conn.commit()
        cur.close()
    finally:
        conn.close()
    return serialize_value({"ok": True, "dispatch_plan_id": plan_id, "batch_id": batch_id})


def _sync_assignments_from_picking(cur, plan_id: int) -> None:
    """Crea filas de asignación documental desde el picking actual.

    No inventa ``load_batch_id``: eso lo hace ``_reconcile_document_batches_from_oc``.
    """
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


def _reconcile_document_batches_from_oc(
    cur,
    plan_id: int,
    *,
    overwrite_manual: bool = False,
) -> dict[str, Any]:
    """Asigna facturas/boletas del picking al grupo de su OC canónica.

    Fuente: ``dispatch_plan_order_load_assignments`` (organización anticipada).
    No divide facturas. Si un mismo documento tributario quedaría en dos grupos
    distintos, lo deja sin lote y reporta conflicto.
    Solo rellena ``load_batch_id`` vacío salvo ``overwrite_manual``.
    """
    from backend.repositories.distribuidora import (
        dispatch_plan_order_load_repo as org_repo,
    )

    if not org_repo.assignments_table_ready(cur):
        return {
            "reconciled": 0,
            "already_assigned": 0,
            "pending": 0,
            "conflicts": [],
            "organization_ready": False,
            "note_es": "Migración 053 no disponible: no hay OC→grupo.",
        }

    oc_to_batch: dict[int, int] = {}
    for row in org_repo.list_order_assignments(cur, plan_id):
        oc_id = int(row["oc_document_id"])
        batch_id = int(row["load_batch_id"])
        prev = oc_to_batch.get(oc_id)
        if prev is not None and prev != batch_id:
            # Datos inconsistentes; no auto-asignar esa OC.
            oc_to_batch.pop(oc_id, None)
            continue
        oc_to_batch[oc_id] = batch_id

    assignments = batch_repo.list_document_assignments(cur, plan_id)
    # Detectar documento tributario con OCs en grupos distintos (payloads múltiples).
    doc_batches: dict[int, set[int]] = {}
    doc_ocs: dict[int, set[int]] = {}
    for a in assignments:
        rel = int(a["related_document_id"])
        oc = a.get("oc_document_id")
        if oc is None:
            continue
        batch = oc_to_batch.get(int(oc))
        if batch is None:
            continue
        doc_batches.setdefault(rel, set()).add(batch)
        doc_ocs.setdefault(rel, set()).add(int(oc))

    conflicts: list[dict[str, Any]] = []
    conflict_docs: set[int] = set()
    for rel, batches in doc_batches.items():
        if len(batches) > 1:
            conflict_docs.add(rel)
            conflicts.append(
                {
                    "related_document_id": rel,
                    "oc_document_ids": sorted(doc_ocs.get(rel) or []),
                    "load_batch_ids": sorted(batches),
                    "code": "invoice_spans_groups",
                    "message_es": (
                        f"Documento {rel} relaciona OC de grupos distintos "
                        f"{sorted(batches)}. Requiere resolución auditada; "
                        "no se asigna automáticamente."
                    ),
                }
            )

    reconciled = 0
    already = 0
    pending = 0
    for a in assignments:
        rel = int(a["related_document_id"])
        current = a.get("load_batch_id")
        if rel in conflict_docs:
            pending += 1
            continue
        oc = a.get("oc_document_id")
        if oc is None:
            pending += 1
            continue
        target = oc_to_batch.get(int(oc))
        if target is None:
            pending += 1
            continue
        if current is not None and int(current) == int(target):
            already += 1
            continue
        if current is not None and not overwrite_manual:
            # Asignación manual distinta: respetar.
            if int(current) != int(target):
                conflicts.append(
                    {
                        "related_document_id": rel,
                        "oc_document_id": int(oc),
                        "current_load_batch_id": int(current),
                        "suggested_load_batch_id": int(target),
                        "code": "manual_differs_from_oc_group",
                        "message_es": (
                            f"Documento {rel} tiene lote manual {current}; "
                            f"la OC sugiere grupo {target}."
                        ),
                    }
                )
            already += 1
            continue
        batch_repo.upsert_document_assignment(
            cur,
            plan_id=plan_id,
            related_document_id=rel,
            load_batch_id=int(target),
            oc_document_id=int(oc),
            document_number=int(a["document_number"]) if a.get("document_number") else None,
            client_name=a.get("client_name"),
            document_total=float(a["document_total"]) if a.get("document_total") is not None else None,
        )
        reconciled += 1

    return {
        "reconciled": reconciled,
        "already_assigned": already,
        "pending": pending,
        "conflicts": conflicts,
        "organization_ready": bool(oc_to_batch),
        "oc_group_count": len(oc_to_batch),
    }


def reconcile_document_load_assignments(
    plan_id: int,
    *,
    overwrite_manual: bool = False,
) -> dict[str, Any]:
    """Sincroniza picking → filas documentales y concilia OC→grupo→factura."""
    ensure_default_load_batch(plan_id)
    conn = get_connection()
    assignments: list[dict[str, Any]] = []
    picking_row = None
    unassigned = 0
    group_status: list[dict[str, Any]] = []
    result: dict[str, Any] = {}
    try:
        cur = conn.cursor()
        if not plan_repo.get_plan_by_id(cur, plan_id):
            raise ValueError("Plan no encontrado")
        _sync_assignments_from_picking(cur, plan_id)
        result = _reconcile_document_batches_from_oc(
            cur, plan_id, overwrite_manual=overwrite_manual
        )
        assignments = batch_repo.list_document_assignments(cur, plan_id)
        batches = batch_repo.list_load_batches(cur, plan_id)
        by_batch: dict[int, int] = {}
        for a in assignments:
            bid = a.get("load_batch_id")
            if bid is None:
                unassigned += 1
            else:
                by_batch[int(bid)] = by_batch.get(int(bid), 0) + 1
        # Completitud: UNIQUE evita duplicados, no omisiones. Export por grupo
        # solo es “completo” si todo documento del picking tiene lote.
        assignment_complete = bool(assignments) and unassigned == 0
        hard_conflicts = [
            c
            for c in (result.get("conflicts") or [])
            if c.get("code") == "invoice_spans_groups"
        ]
        if hard_conflicts:
            assignment_complete = False
        for b in batches:
            bid = int(b["id"])
            n = by_batch.get(bid, 0)
            group_status.append(
                {
                    "load_batch_id": bid,
                    "name": b.get("name"),
                    "sort_order": b.get("sort_order"),
                    "documents_assigned": n,
                    "picking_export_ready": bool(n > 0 and assignment_complete),
                }
            )
        picking_row = picking_repo.get_picking_row(cur, plan_id, current_only=True)
        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return serialize_value(
        {
            "dispatch_plan_id": plan_id,
            "has_picking": bool(picking_row),
            "picking_version": int(picking_row["version"]) if picking_row else None,
            **result,
            "documents_total": len(assignments),
            "documents_unassigned": unassigned,
            "document_assignment_complete": assignment_complete,
            "groups": group_status,
        }
    )


def get_picking_assignments(plan_id: int) -> dict[str, Any]:
    ensure_default_load_batch(plan_id)
    conn = get_connection()
    try:
        cur = conn.cursor()
        _sync_assignments_from_picking(cur, plan_id)
        # Conciliación automática al consultar: rellena lotes vacíos desde OC→grupo.
        reconciliation = _reconcile_document_batches_from_oc(
            cur, plan_id, overwrite_manual=False
        )
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
            "document_reconciliation": reconciliation,
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


def preview_add_order(
    plan_id: int,
    *,
    oc_document_ids: list[int] | None = None,
    oc_document_id: int | None = None,
) -> dict[str, Any]:
    """Preview de incorporación; delega a F3-D cuando hay OC candidatas."""
    from backend.services.distribuidora.dispatch_plan_supplemental_order_service import (
        preview_supplemental_orders,
    )

    ids = list(oc_document_ids or [])
    if oc_document_id is not None:
        ids.append(int(oc_document_id))
    if ids:
        return preview_supplemental_orders(plan_id, oc_document_ids=ids)

    # Sin OC: solo estado/picking (compat UI antigua).
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
            "En planes con organización completa se crea un grupo suplementario; "
            "la regeneración global versiona el picking sin rehacer físicamente "
            "los grupos originales."
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
    oc_document_id: int | None = None,
    oc_document_ids: list[int] | None = None,
    regenerate_picking: bool = False,
    reason: str | None = None,
    user: dict[str, Any] | None = None,
    acknowledge_incomplete_weight: bool = False,
    acknowledge_unevaluable_weight: bool = False,
) -> dict[str, Any]:
    """Punto de entrada F3-D / legacy. Preferir ``oc_document_ids``."""
    from backend.services.distribuidora.dispatch_plan_supplemental_order_service import (
        add_supplemental_orders_to_plan,
        preview_supplemental_orders,
    )

    ids: list[int] = []
    if oc_document_ids:
        ids.extend(int(x) for x in oc_document_ids)
    if oc_document_id is not None:
        ids.append(int(oc_document_id))
    ids = sorted(set(ids))
    if not ids:
        raise ValueError("Indique al menos una OC.")

    preview = preview_supplemental_orders(plan_id, oc_document_ids=ids)
    uses_supp = bool(preview.get("uses_supplemental_group"))
    has_picking = bool(preview.get("has_picking"))

    # Legacy con picking: exigir confirmación de regeneración (comportamiento previo).
    if has_picking and not regenerate_picking and not uses_supp:
        return serialize_value(
            {
                **preview,
                "requires_regenerate": True,
                "added": False,
            }
        )

    # Organized: D1 no exige regenerate; D2 puede pedirlo para versionar picking.
    result = add_supplemental_orders_to_plan(
        plan_id,
        oc_document_ids=ids,
        reason=reason or "OC adicional",
        user=user,
        acknowledge_incomplete_weight=acknowledge_incomplete_weight,
        acknowledge_unevaluable_weight=acknowledge_unevaluable_weight,
        regenerate_picking=bool(regenerate_picking),
    )

    # Compat campo singular.
    if result.get("oc_document_ids") and len(result["oc_document_ids"]) == 1:
        result = {**result, "oc_document_id": result["oc_document_ids"][0]}
    return result


def assess_document_assignment_completeness(cur, plan_id: int) -> dict[str, Any]:
    """Detecta omisiones (docs sin lote) además de la UNIQUE anti-duplicado."""
    assignments = batch_repo.list_document_assignments(cur, plan_id)
    unassigned_ids = [
        int(a["related_document_id"])
        for a in assignments
        if a.get("load_batch_id") is None
    ]
    total = len(assignments)
    complete = total > 0 and len(unassigned_ids) == 0
    return {
        "documents_total": total,
        "documents_unassigned": len(unassigned_ids),
        "unassigned_related_document_ids": unassigned_ids,
        "document_assignment_complete": complete,
    }


def assert_group_export_complete(plan_id: int) -> dict[str, Any]:
    """Bloquea export por grupo si hay documentos del picking sin asignar."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        status = assess_document_assignment_completeness(cur, plan_id)
        cur.close()
    finally:
        conn.close()
    if not status["document_assignment_complete"]:
        pending = status["documents_unassigned"]
        raise ValueError(
            "Exportación por grupo incompleta: hay "
            f"{pending} documento(s) del picking sin load_batch_id. "
            "Concilie o asigne manualmente antes de descargar; "
            "una descarga parcial no se presenta como completa."
        )
    return status


def filter_picking_by_load_batch(
    picking_payload: dict[str, Any],
    plan_id: int,
    load_batch_id: int,
    *,
    require_complete: bool = False,
) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        completeness = assess_document_assignment_completeness(cur, plan_id)
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
    if require_complete and not completeness["document_assignment_complete"]:
        pending = completeness["documents_unassigned"]
        raise ValueError(
            "Exportación por grupo incompleta: hay "
            f"{pending} documento(s) del picking sin load_batch_id. "
            "Concilie o asigne manualmente antes de descargar; "
            "una descarga parcial no se presenta como completa."
        )

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
    out["document_assignment_complete"] = completeness["document_assignment_complete"]
    out["documents_unassigned"] = completeness["documents_unassigned"]
    out["documents_total"] = completeness["documents_total"]
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
