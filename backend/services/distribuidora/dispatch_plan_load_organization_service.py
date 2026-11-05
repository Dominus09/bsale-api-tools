"""Organización anticipada de carga: OC canónica → grupo del mismo plan.

No genera documentos tributarios ni modifica el motor de picking.
Planes ``legacy`` siguen operando sin esta agrupación.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from backend.db import get_connection
from backend.repositories.distribuidora import dispatch_plan_load_batch_repo as batch_repo
from backend.repositories.distribuidora import dispatch_plan_order_load_repo as org_repo
from backend.repositories.distribuidora import dispatch_plan_repo as plan_repo
from backend.utils.json_safe import serialize_value


class LoadOrganizationConflictError(Exception):
    """Conflicto de versión / concurrencia o regla de negocio."""

    def __init__(self, message: str, *, code: str = "conflict"):
        super().__init__(message)
        self.code = code


def _staff_label(user: dict[str, Any] | None) -> str | None:
    if not user:
        return None
    return str(user.get("email") or user.get("name") or user.get("sub") or "").strip() or None


def validate_client_group_coherence(
    orders: list[dict[str, Any]],
    assignments_by_oc: dict[int, int],
) -> list[str]:
    """Por defecto: todas las OC del mismo cliente deben compartir grupo.

    Retorna mensajes de error (vacío = OK).
    """
    by_client: dict[int, set[int]] = defaultdict(set)
    for o in orders:
        cid = o.get("client_id")
        oc_id = int(o["oc_document_id"])
        if cid is None:
            continue
        batch = assignments_by_oc.get(oc_id)
        if batch is None:
            continue
        by_client[int(cid)].add(int(batch))
    errors: list[str] = []
    for cid, batches in by_client.items():
        if len(batches) > 1:
            errors.append(
                f"Cliente {cid}: las OC del plan deben pertenecer al mismo grupo "
                f"(grupos {sorted(batches)})."
            )
    return errors


def organization_completeness(
    orders: list[dict[str, Any]],
    assignments_by_oc: dict[int, int],
) -> dict[str, Any]:
    total = len(orders)
    assigned = sum(1 for o in orders if int(o["oc_document_id"]) in assignments_by_oc)
    missing = [
        {
            "oc_document_id": int(o["oc_document_id"]),
            "oc_number": o.get("oc_number"),
            "client_id": o.get("client_id"),
            "client_name": o.get("client_name"),
        }
        for o in orders
        if int(o["oc_document_id"]) not in assignments_by_oc
    ]
    return {
        "total_orders": total,
        "assigned_orders": assigned,
        "unassigned_orders": total - assigned,
        "is_complete": total > 0 and assigned == total,
        "missing": missing,
    }


def get_load_organization(plan_id: int) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        plan = plan_repo.get_plan_by_id(cur, plan_id)
        if not plan:
            raise ValueError("Plan no encontrado")
        meta = org_repo.get_organization_meta(cur, plan_id) or {
            "load_organization_status": "legacy",
            "load_organization_version": 0,
            "schema_ready": False,
        }
        if not meta.get("schema_ready") or not org_repo.assignments_table_ready(cur):
            batches = batch_repo.list_load_batches(cur, plan_id)
            cur.close()
            return serialize_value(
                {
                    "dispatch_plan_id": plan_id,
                    "schema_ready": False,
                    "load_organization_status": "legacy",
                    "load_organization_version": 0,
                    "batches": batches,
                    "orders": [],
                    "assignments": [],
                    "completeness": {
                        "total_orders": 0,
                        "assigned_orders": 0,
                        "unassigned_orders": 0,
                        "is_complete": False,
                        "missing": [],
                    },
                    "message": (
                        "Migración 053 pendiente: organización anticipada no disponible "
                        "en este entorno. Los planes legacy siguen operativos."
                    ),
                }
            )

        batches = batch_repo.list_load_batches(cur, plan_id)
        orders = org_repo.list_plan_orders_for_organization(cur, plan_id)
        assignments = org_repo.list_order_assignments(cur, plan_id)
        by_oc = {
            int(a["oc_document_id"]): int(a["load_batch_id"]) for a in assignments
        }
        completeness = organization_completeness(orders, by_oc)
        cur.close()
        return serialize_value(
            {
                "dispatch_plan_id": plan_id,
                "schema_ready": True,
                "load_organization_status": meta["load_organization_status"],
                "load_organization_version": meta["load_organization_version"],
                "load_organization_updated_at": meta.get("load_organization_updated_at"),
                "batches": batches,
                "orders": orders,
                "assignments": assignments,
                "completeness": completeness,
            }
        )
    finally:
        conn.close()


def save_load_organization(
    plan_id: int,
    *,
    assignments: list[dict[str, Any]],
    organization_version: int,
    mark_organized: bool = False,
    allow_split_client: bool = False,
    user: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Guarda asignaciones OC→grupo de forma atómica e idempotente.

    ``organization_version`` debe coincidir con la versión actual (optimistic lock).
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        if not org_repo.organization_columns_ready(cur) or not org_repo.assignments_table_ready(
            cur
        ):
            raise ValueError(
                "Migración 053 no aplicada: no se puede guardar organización anticipada."
            )

        locked = org_repo.lock_plan_for_organization(cur, plan_id)
        if not locked:
            raise ValueError("Plan no encontrado")

        current_ver = int(locked["load_organization_version"])
        if int(organization_version) != current_ver:
            raise LoadOrganizationConflictError(
                f"Versión desactualizada (esperada {current_ver}, recibida {organization_version}). "
                "Recargue la organización e intente de nuevo.",
                code="version_conflict",
            )

        orders = plan_repo.list_plan_orders(cur, plan_id)
        order_by_oc = {int(o["oc_document_id"]): o for o in orders}
        if not orders:
            raise ValueError("El plan no tiene OC para organizar.")

        batches = batch_repo.list_load_batches(cur, plan_id)
        batch_ids = {int(b["id"]) for b in batches}
        if not batch_ids:
            raise ValueError("Cree al menos un grupo de carga (load batch) antes de asignar.")

        normalized: list[dict[str, Any]] = []
        seen_oc: set[int] = set()
        for raw in assignments:
            try:
                oc_id = int(raw["oc_document_id"])
                batch_id = int(raw["load_batch_id"])
            except (KeyError, TypeError, ValueError) as e:
                raise ValueError(
                    "Cada asignación requiere oc_document_id y load_batch_id enteros canónicos."
                ) from e
            if oc_id in seen_oc:
                raise ValueError(f"OC {oc_id} duplicada en el payload.")
            seen_oc.add(oc_id)
            if oc_id not in order_by_oc:
                raise ValueError(
                    f"OC document_id={oc_id} no pertenece a este despacho ({plan_id})."
                )
            if batch_id not in batch_ids:
                raise ValueError(
                    f"Grupo load_batch_id={batch_id} no pertenece a este despacho ({plan_id})."
                )
            order = order_by_oc[oc_id]
            normalized.append(
                {
                    "oc_document_id": oc_id,
                    "load_batch_id": batch_id,
                    "client_id": order.get("client_id"),
                }
            )

        by_oc = {a["oc_document_id"]: a["load_batch_id"] for a in normalized}
        if not allow_split_client:
            coherence = validate_client_group_coherence(orders, by_oc)
            if coherence:
                raise ValueError("; ".join(coherence))

        completeness = organization_completeness(orders, by_oc)
        if mark_organized and not completeness["is_complete"]:
            raise ValueError(
                "No se puede marcar como organized: hay OC sin grupo. "
                f"Faltan {completeness['unassigned_orders']} de {completeness['total_orders']}."
            )

        staff = _staff_label(user)
        saved = org_repo.replace_order_assignments(
            cur,
            plan_id=plan_id,
            assignments=normalized,
            assigned_by=staff,
        )

        prev_status = str(locked["load_organization_status"] or "legacy")
        if mark_organized:
            next_status = "organized"
        elif completeness["is_complete"] and prev_status == "organized":
            next_status = "organized"
        else:
            # Opt-in: el primer guardado saca al plan de legacy sin bloquear picking histórico.
            next_status = "draft"

        meta = org_repo.update_organization_meta(
            cur,
            plan_id=plan_id,
            status=next_status,
            from_version=current_ver,
        )
        if not meta:
            raise LoadOrganizationConflictError(
                "Conflicto de concurrencia al actualizar la versión de organización.",
                code="version_conflict",
            )

        org_repo.insert_organization_event(
            cur,
            plan_id=plan_id,
            action="load_organization_save",
            user_name=staff,
            payload={
                "from_version": current_ver,
                "to_version": meta["load_organization_version"],
                "status": next_status,
                "assignment_count": len(saved),
                "mark_organized": mark_organized,
                "allow_split_client": allow_split_client,
            },
        )
        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return get_load_organization(plan_id)
