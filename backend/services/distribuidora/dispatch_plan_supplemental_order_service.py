"""F3-D: OC adicionales con grupo suplementario (sin rehacer picking físico previo).

F3-D1: capacidad, estado, incorporación atómica, grupo suplementario, asignaciones OC.
F3-D2 (picking/UI) se apoya en este módulo.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from backend.db import get_connection
from backend.repositories.distribuidora import dispatch_plan_load_batch_repo as batch_repo
from backend.repositories.distribuidora import dispatch_plan_order_load_repo as org_repo
from backend.repositories.distribuidora import dispatch_plan_repo as plan_repo
from backend.services.distribuidora import dispatch_plan_service as plan_svc
from backend.services.distribuidora.dispatch_plan_load_organization_service import (
    encode_supplemental_batch_description,
    parse_batch_presentation_role,
    parse_supplemental_seq,
)
from backend.services.distribuidora.oc_document_chain_resolver import (
    resolve_operational_statuses_batch,
)
from backend.services.distribuidora.oc_operational_status import (
    admission_block_message,
    blocks_planning_admission,
)
from backend.services.distribuidora.simple_planning_assignment_service import (
    validate_truck_capacity,
)
from backend.utils.json_safe import serialize_value

logger = logging.getLogger(__name__)

ACTION_ADD_SUPPLEMENTAL = "add_supplemental_orders"

# Incorporación ordinaria: estados donde aún se puede modificar la carga.
ALLOW_ADD_PLAN_STATUSES = frozenset(
    {
        "planned",
        "invoicing",
        "ready_for_picking",
        "picking_generated",
    }
)
BLOCK_ADD_PLAN_STATUSES = frozenset({"dispatched", "delivered", "draft"})

# Cuadratura: closed_at o status squared bloquean incorporación ordinaria.
CUADRATURA_BLOCK_STATUSES = frozenset({"squared"})

_SUPPLEMENTAL_NAME_RE = re.compile(r"^OC adicionales\s+(\d+)\s*$", re.I)


class SupplementalOrderError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "supplemental_rejected",
        http_status: int = 400,
        details: dict[str, Any] | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.http_status = http_status
        self.details = details or {}


def _staff_label(user: dict[str, Any] | None) -> str | None:
    if not user:
        return None
    return str(user.get("email") or user.get("name") or user.get("sub") or "").strip() or None


def _admission_block_payload(status) -> dict[str, Any]:
    inv = status.confirmed_invoice or {}
    return {
        "blocked": True,
        "message": admission_block_message(status),
        "billing_status": status.billing_status,
        "billing_label_es": status.billing_label_es,
        "confirmed_invoice": inv,
        "invoice_number": inv.get("number"),
    }


def _cuadratura_blocks_add(cur, plan_id: int) -> dict[str, Any] | None:
    try:
        from backend.repositories.distribuidora import (
            dispatch_plan_cuadratura_repo as cuad_repo,
        )

        row = cuad_repo.get_cuadratura_row(cur, plan_id)
    except Exception:
        return None
    if not row:
        return None
    status = str(row.get("status") or "").lower()
    closed_at = row.get("closed_at")
    if closed_at or status in CUADRATURA_BLOCK_STATUSES:
        return {
            "blocked": True,
            "code": "cuadratura_closed",
            "message": (
                "No se pueden agregar OC: la cuadratura está cerrada o cuadrada. "
                "No se reabre automáticamente."
            ),
            "cuadratura_status": status,
            "closed_at": closed_at,
        }
    return None


def _weight_bucket_from_order_row(o: dict[str, Any]) -> tuple[str, float | None]:
    """Clasifica peso congelado del plan: known|partial|unevaluable."""
    peso = o.get("peso_total_kg")
    try:
        peso_f = float(peso) if peso is not None else None
    except (TypeError, ValueError):
        peso_f = None
    try:
        cov = float(o.get("cobertura_logistica") or 0)
    except (TypeError, ValueError):
        cov = 0.0
    if peso_f is not None and peso_f > 0 and cov >= 99.5:
        return "known", peso_f
    if peso_f is not None and peso_f > 0:
        return "partial", peso_f
    return "unevaluable", peso_f


def _weight_bucket_from_snapshot(w: dict[str, Any] | None) -> tuple[str, float | None]:
    if not w:
        return "unevaluable", None
    try:
        peso = float(w["peso_total_kg"]) if w.get("peso_total_kg") is not None else None
    except (TypeError, ValueError):
        peso = None
    try:
        cov = float(
            w.get("porcentaje_cobertura_peso")
            or w.get("porcentaje_cobertura")
            or 0
        )
    except (TypeError, ValueError):
        cov = 0.0
    sin = int(w.get("productos_sin_peso") or 0)
    if peso is not None and peso > 0 and cov >= 99.5 and sin == 0:
        return "known", peso
    if peso is not None and peso > 0:
        return "partial", peso
    return "unevaluable", peso


def _capacity_for_plan_plus_ocs(
    cur,
    plan_id: int,
    *,
    new_oc_ids: list[int],
    truck_max: int | None,
) -> dict[str, Any]:
    existing = plan_repo.list_plan_orders(cur, plan_id)
    known = 0.0
    estimated = 0.0
    incomplete = 0
    unevaluable = 0
    for o in existing:
        if int(o["oc_document_id"]) in set(new_oc_ids):
            continue  # reintento: no doble contar
        bucket, peso = _weight_bucket_from_order_row(o)
        if bucket == "known" and peso is not None:
            known += peso
        elif bucket == "partial" and peso is not None:
            known += peso
            incomplete += 1
        else:
            unevaluable += 1

    from backend.services.order_weight_service import fetch_weights_by_document_ids

    snaps = fetch_weights_by_document_ids(new_oc_ids)
    new_details: list[dict[str, Any]] = []
    for oc_id in new_oc_ids:
        bucket, peso = _weight_bucket_from_snapshot(snaps.get(int(oc_id)))
        units = None
        if snaps.get(int(oc_id)):
            try:
                units = float(snaps[int(oc_id)].get("cantidad_unidades") or 0) or None
            except (TypeError, ValueError):
                units = None
        new_details.append(
            {
                "oc_document_id": int(oc_id),
                "weight_quality": bucket,
                "known_weight_kg": peso,
                "cantidad_unidades": units,
            }
        )
        if bucket == "known" and peso is not None:
            known += peso
        elif bucket == "partial" and peso is not None:
            known += peso
            incomplete += 1
        else:
            unevaluable += 1

    capacity = validate_truck_capacity(
        known_weight_kg=known,
        estimated_weight_kg=estimated,
        incomplete_orders=incomplete,
        unevaluable_orders=unevaluable,
        truck_max_weight_kg=truck_max,
    )
    capacity["new_orders"] = new_details
    return capacity


def assert_capacity_allows_add(
    capacity: dict[str, Any],
    *,
    acknowledge_incomplete_weight: bool = False,
    acknowledge_unevaluable_weight: bool = False,
) -> dict[str, Any]:
    if not capacity.get("capacity_known"):
        raise SupplementalOrderError(
            "Capacidad del camión desconocida: no se aceptan OC adicionales "
            "sin max_weight_kg registrado.",
            code="truck_capacity_unknown",
            http_status=409,
            details=capacity,
        )
    if capacity.get("overloaded") or capacity.get("blocking"):
        raise SupplementalOrderError(
            "Sobrecarga conocida: el peso conocido del despacho (incl. OC nuevas) "
            "supera la capacidad del camión.",
            code="truck_overloaded",
            http_status=409,
            details=capacity,
        )
    ack: dict[str, Any] = {
        "incomplete_weight": False,
        "unevaluable_weight": False,
        "does_not_authorize_route_departure": True,
    }
    if int(capacity.get("unevaluable_order_count") or 0) > 0:
        if not acknowledge_unevaluable_weight:
            raise SupplementalOrderError(
                "Hay OC sin peso usable: la capacidad total no es verificable. "
                "Confirme explícitamente acknowledge_unevaluable_weight para continuar "
                "(no autoriza salida a ruta).",
                code="weight_unevaluable_ack_required",
                http_status=409,
                details={**capacity, "requires_unevaluable_weight_ack": True},
            )
        ack["unevaluable_weight"] = True
    if int(capacity.get("incomplete_order_count") or 0) > 0:
        if not acknowledge_incomplete_weight:
            raise SupplementalOrderError(
                "Cobertura de peso incompleta en el despacho (productos sin peso). "
                "Confirme acknowledge_incomplete_weight para continuar "
                "(no autoriza salida a ruta).",
                code="weight_incomplete_ack_required",
                http_status=409,
                details={**capacity, "requires_incomplete_weight_ack": True},
            )
        ack["incomplete_weight"] = True
    return ack


def _next_supplemental_seq(batches: list[dict[str, Any]]) -> int:
    max_seq = 0
    for b in batches:
        role = parse_batch_presentation_role(b.get("description"))
        if role == "supplemental":
            seq = parse_supplemental_seq(b.get("description")) or 0
            max_seq = max(max_seq, seq)
            m = _SUPPLEMENTAL_NAME_RE.match(str(b.get("name") or ""))
            if m:
                max_seq = max(max_seq, int(m.group(1)))
    return max_seq + 1


def _find_existing_assignment_batch(
    cur, plan_id: int, oc_ids: list[int]
) -> dict[str, Any] | None:
    """Si todas las OC ya están en el mismo grupo supplemental (reintento), reutilizar."""
    if not org_repo.assignments_table_ready(cur):
        return None
    assigns = {
        int(a["oc_document_id"]): a
        for a in org_repo.list_order_assignments(cur, plan_id)
        if int(a["oc_document_id"]) in set(oc_ids)
    }
    if len(assigns) != len(set(oc_ids)):
        return None
    batch_ids = {int(a["load_batch_id"]) for a in assigns.values()}
    if len(batch_ids) != 1:
        return None
    bid = next(iter(batch_ids))
    for b in batch_repo.list_load_batches(cur, plan_id):
        if int(b["id"]) == bid and parse_batch_presentation_role(b.get("description")) == "supplemental":
            return b
    return None


def preview_supplemental_orders(
    plan_id: int,
    *,
    oc_document_ids: list[int],
) -> dict[str, Any]:
    oc_ids = [int(x) for x in oc_document_ids if x is not None]
    if not oc_ids:
        raise SupplementalOrderError("Indique al menos una OC.", code="oc_required")

    conn = get_connection()
    try:
        cur = conn.cursor()
        plan = plan_repo.get_plan_by_id(cur, plan_id)
        if not plan:
            raise SupplementalOrderError("Plan no encontrado", code="plan_not_found", http_status=404)
        status = str(plan.get("status") or "")
        org_meta = org_repo.get_organization_meta(cur, plan_id) or {}
        org_status = str(org_meta.get("load_organization_status") or "legacy")
        has_picking = batch_repo.plan_has_current_picking(cur, plan_id)
        cuad = _cuadratura_blocks_add(cur, plan_id)

        blocked_reason = None
        can_add = True
        if status in BLOCK_ADD_PLAN_STATUSES or status not in ALLOW_ADD_PLAN_STATUSES:
            can_add = False
            blocked_reason = (
                f"Plan en estado '{status}' no admite incorporación ordinaria de OC."
            )
        if cuad:
            can_add = False
            blocked_reason = cuad["message"]

        truck_max = plan.get("truck_max_weight_kg")
        if truck_max is None and plan.get("truck_id") is not None:
            cur.execute(
                "SELECT max_weight_kg FROM distribuidora.trucks WHERE id = %s",
                (int(plan["truck_id"]),),
            )
            trow = cur.fetchone()
            truck_max = int(trow[0]) if trow and trow[0] is not None else None

        capacity = _capacity_for_plan_plus_ocs(
            cur, plan_id, new_oc_ids=oc_ids, truck_max=truck_max
        )

        existing = {
            int(o["oc_document_id"])
            for o in plan_repo.list_plan_orders(cur, plan_id)
        }
        already = [i for i in oc_ids if i in existing]
        novel = [i for i in oc_ids if i not in existing]

        batches = batch_repo.list_load_batches(cur, plan_id)
        next_seq = _next_supplemental_seq(batches)
        proposed_name = f"OC adicionales {next_seq:02d}"
        uses_supplemental = org_status == "organized"

        cur.close()
    finally:
        conn.close()

    return serialize_value(
        {
            "dispatch_plan_id": plan_id,
            "can_add": can_add and not already,
            "blocked_reason": (
                blocked_reason
                or (
                    f"OC ya en el plan: {already}"
                    if already
                    else None
                )
            ),
            "plan_status": status,
            "load_organization_status": org_status,
            "has_picking": has_picking,
            "uses_supplemental_group": uses_supplemental,
            "proposed_supplemental_group_name": (
                proposed_name if uses_supplemental else None
            ),
            "proposed_supplemental_seq": next_seq if uses_supplemental else None,
            "oc_document_ids": oc_ids,
            "already_in_plan": already,
            "novel_oc_document_ids": novel,
            "capacity": capacity,
            "requires_incomplete_weight_ack": bool(
                capacity.get("requires_incomplete_weight_ack")
            ),
            "requires_unevaluable_weight_ack": bool(
                capacity.get("requires_unevaluable_weight_ack")
            ),
            "note_es": (
                "Con organización completa se crea un grupo suplementario; "
                "los grupos originales no se modifican. "
                "El picking de productos del suplemento requiere facturación (F3-D2)."
                if uses_supplemental
                else "Plan legacy/draft: se conserva el flujo clásico sin grupo suplementario automático."
            ),
        }
    )


def add_supplemental_orders_to_plan(
    plan_id: int,
    *,
    oc_document_ids: list[int],
    reason: str,
    user: dict[str, Any] | None = None,
    acknowledge_incomplete_weight: bool = False,
    acknowledge_unevaluable_weight: bool = False,
    regenerate_picking: bool = False,
) -> dict[str, Any]:
    """Incorpora OC de forma atómica. Si organized → grupo suplementario + asignaciones.

    Idempotente: reintento con las mismas OC ya en el mismo grupo supplemental → unchanged.
    ``regenerate_picking`` queda para F3-D2 (por defecto False en D1).
    """
    reason_n = (reason or "").strip()
    if len(reason_n) < 3:
        raise SupplementalOrderError(
            "Motivo obligatorio (mínimo 3 caracteres).",
            code="reason_required",
        )
    oc_ids = sorted({int(x) for x in oc_document_ids if x is not None})
    if not oc_ids:
        raise SupplementalOrderError("Indique al menos una OC.", code="oc_required")

    preview = preview_supplemental_orders(plan_id, oc_document_ids=oc_ids)
    if not preview.get("can_add") and preview.get("already_in_plan"):
        # Puede ser reintento idempotente — se valida abajo con lock.
        pass
    elif not preview.get("can_add"):
        raise SupplementalOrderError(
            preview.get("blocked_reason") or "No se puede agregar la orden.",
            code="plan_not_addable",
            http_status=409,
            details=preview,
        )

    actor = _staff_label(user)
    conn = get_connection()
    try:
        cur = conn.cursor()
        locked = org_repo.lock_plan_for_organization(cur, plan_id)
        if not locked:
            raise SupplementalOrderError("Plan no encontrado", code="plan_not_found", http_status=404)
        status = str(locked.get("status") or "")
        if status not in ALLOW_ADD_PLAN_STATUSES:
            raise SupplementalOrderError(
                f"Plan en estado '{status}' no admite incorporación ordinaria de OC.",
                code="plan_not_addable",
                http_status=409,
            )
        cuad = _cuadratura_blocks_add(cur, plan_id)
        if cuad:
            raise SupplementalOrderError(
                cuad["message"],
                code=cuad["code"],
                http_status=409,
                details=cuad,
            )

        org_status = str(locked.get("load_organization_status") or "legacy")
        uses_supplemental = org_status == "organized"

        plan = plan_repo.get_plan_by_id(cur, plan_id) or {}
        truck_max = plan.get("truck_max_weight_kg")
        if truck_max is None and plan.get("truck_id") is not None:
            cur.execute(
                "SELECT max_weight_kg FROM distribuidora.trucks WHERE id = %s",
                (int(plan["truck_id"]),),
            )
            trow = cur.fetchone()
            truck_max = int(trow[0]) if trow and trow[0] is not None else None

        capacity = _capacity_for_plan_plus_ocs(
            cur, plan_id, new_oc_ids=oc_ids, truck_max=truck_max
        )
        weight_ack = assert_capacity_allows_add(
            capacity,
            acknowledge_incomplete_weight=acknowledge_incomplete_weight,
            acknowledge_unevaluable_weight=acknowledge_unevaluable_weight,
        )

        status_map = resolve_operational_statuses_batch(cur, oc_ids)
        for oc_id in oc_ids:
            oc_status = status_map.get(int(oc_id))
            if oc_status is not None and blocks_planning_admission(oc_status):
                detail = _admission_block_payload(oc_status)
                raise SupplementalOrderError(
                    f"OC {oc_id}: {detail['message']}",
                    code="oc_not_admissible",
                    http_status=409,
                    details=detail,
                )

        existing_ids = {
            int(o["oc_document_id"]) for o in plan_repo.list_plan_orders(cur, plan_id)
        }
        already = [i for i in oc_ids if i in existing_ids]
        novel = [i for i in oc_ids if i not in existing_ids]

        # Idempotencia: todas ya en el mismo supplemental → unchanged.
        if not novel and already:
            reused = _find_existing_assignment_batch(cur, plan_id, already)
            if reused:
                conn.commit()
                cur.close()
                return serialize_value(
                    {
                        "status": "unchanged",
                        "dispatch_plan_id": plan_id,
                        "added": False,
                        "oc_document_ids": already,
                        "supplemental_group": {
                            "load_batch_id": int(reused["id"]),
                            "name": reused.get("name"),
                            "seq": parse_supplemental_seq(reused.get("description")),
                        },
                        "capacity": capacity,
                        "note_es": "Reintento idempotente: OC ya estaban en el grupo suplementario.",
                    }
                )
            raise SupplementalOrderError(
                f"OC ya pertenecen al plan: {already}",
                code="oc_duplicate",
                http_status=409,
            )

        supplemental_batch = None
        seq = None
        if uses_supplemental:
            if not org_repo.assignments_table_ready(cur):
                raise SupplementalOrderError(
                    "Migración 053 no disponible: no se puede asignar grupo suplementario.",
                    code="schema_not_ready",
                    http_status=409,
                )
            batches = batch_repo.list_load_batches(cur, plan_id)
            seq = _next_supplemental_seq(batches)
            name = f"OC adicionales {seq:02d}"
            sort_order = max((int(b.get("sort_order") or 0) for b in batches), default=0) + 1
            desc = encode_supplemental_batch_description(
                seq, "Grupo suplementario F3-D (OC de última hora)."
            )
            supplemental_batch = batch_repo.insert_load_batch(
                cur,
                plan_id=plan_id,
                name=name,
                description=desc,
                sort_order=sort_order,
            )

        # Insertar solo OC nuevas.
        route_base = batch_repo.max_route_order(cur, plan_id)
        orders_payload = [
            {"oc_document_id": oc_id, "route_order": route_base + i + 1}
            for i, oc_id in enumerate(novel)
        ]
        enriched = plan_svc._enrich_orders_snapshot(cur, orders_payload)
        # Adjuntar peso desde snapshot si falta en enrich.
        from backend.services.order_weight_service import fetch_weights_by_document_ids

        snaps = fetch_weights_by_document_ids(novel)
        for row in enriched:
            oc_id = int(row["oc_document_id"])
            w = snaps.get(oc_id)
            if w and row.get("peso_total_kg") is None and w.get("peso_total_kg") is not None:
                row["peso_total_kg"] = w.get("peso_total_kg")
                row["cobertura_logistica"] = w.get("porcentaje_cobertura_peso") or w.get(
                    "porcentaje_cobertura"
                )
                row["cantidad_unidades"] = w.get("cantidad_unidades")
                row["cantidad_cajas"] = w.get("cantidad_cajas")
                row["productos_sin_peso"] = w.get("productos_sin_peso")
        plan_repo.insert_plan_orders(cur, plan_id, enriched)

        client_same_route_exceptions: list[dict[str, Any]] = []
        meta: dict[str, Any] | None = None
        if uses_supplemental and supplemental_batch:
            bid = int(supplemental_batch["id"])
            # Clientes ya presentes en otros grupos.
            prior_by_client: dict[int, list[int]] = {}
            for a in org_repo.list_order_assignments(cur, plan_id):
                cid = a.get("client_id")
                if cid is None:
                    continue
                prior_by_client.setdefault(int(cid), []).append(int(a["oc_document_id"]))

            for row in enriched:
                oc_id = int(row["oc_document_id"])
                client_id = int(row["client_id"]) if row.get("client_id") is not None else None
                org_repo.upsert_order_assignment(
                    cur,
                    plan_id=plan_id,
                    oc_document_id=oc_id,
                    load_batch_id=bid,
                    client_id=client_id,
                    assigned_by=actor,
                )
                if client_id is not None and prior_by_client.get(client_id):
                    client_same_route_exceptions.append(
                        {
                            "client_id": client_id,
                            "client_name": row.get("client_name"),
                            "new_oc_document_id": oc_id,
                            "existing_oc_document_ids": prior_by_client[client_id],
                            "code": "client_split_intentional_supplemental",
                            "message_es": (
                                "Cliente ya tenía OC en otro grupo; la nueva OC queda "
                                "en el grupo suplementario (mercadería posterior al picking)."
                            ),
                        }
                    )

            # Bump organización version (sigue organized).
            from_ver = int(locked.get("load_organization_version") or 0)
            meta = org_repo.update_organization_meta(
                cur, plan_id=plan_id, status="organized", from_version=from_ver
            )
            if meta is None:
                # Conflicto de versión: reintentar lectura (no revertir inserts si posible)
                raise SupplementalOrderError(
                    "Conflicto de versión de organización; reintente.",
                    code="organization_version_conflict",
                    http_status=409,
                )
        else:
            meta = org_repo.get_organization_meta(cur, plan_id)

        batch_repo.insert_order_event(
            cur,
            plan_id=plan_id,
            action=ACTION_ADD_SUPPLEMENTAL if uses_supplemental else "add_order",
            user_name=actor,
            reason=reason_n,
            oc_document_id=novel[0] if len(novel) == 1 else None,
            oc_number=(
                int(enriched[0]["oc_number"])
                if len(enriched) == 1 and enriched[0].get("oc_number")
                else None
            ),
            payload={
                "oc_document_ids": novel,
                "supplemental": uses_supplemental,
                "supplemental_seq": seq,
                "load_batch_id": (
                    int(supplemental_batch["id"]) if supplemental_batch else None
                ),
                "load_batch_name": (
                    supplemental_batch.get("name") if supplemental_batch else None
                ),
                "capacity": capacity,
                "weight_ack": weight_ack,
                "client_same_route_exceptions": client_same_route_exceptions,
                "organization_version": (meta or {}).get("load_organization_version"),
                "regenerate_picking_requested": bool(regenerate_picking),
            },
        )
        conn.commit()
        cur.close()
    except SupplementalOrderError:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    picking_result = None
    has_picking = False
    conn2 = get_connection()
    try:
        cur2 = conn2.cursor()
        has_picking = batch_repo.plan_has_current_picking(cur2, plan_id)
        cur2.close()
    finally:
        conn2.close()

    # F3-D2: nueva versión global + conciliación; export por grupo aísla el suplemento.
    if regenerate_picking and has_picking:
        picking_result = plan_svc.generate_plan_picking(
            plan_id,
            validate=False,
            include_probable=False,
            regenerated_by=actor,
            regeneration_reason=reason_n or "OC adicionales",
            regeneration_action=ACTION_ADD_SUPPLEMENTAL,
        )
        try:
            from backend.services.distribuidora import (
                dispatch_plan_load_batch_service as batch_svc,
            )

            batch_svc.reconcile_document_load_assignments(
                plan_id, overwrite_manual=False
            )
        except Exception:
            logger.exception("reconcile_after_supplemental plan_id=%s", plan_id)

    return serialize_value(
        {
            "status": "added",
            "dispatch_plan_id": plan_id,
            "added": True,
            "oc_document_ids": novel,
            "supplemental_group": (
                {
                    "load_batch_id": int(supplemental_batch["id"]),
                    "name": supplemental_batch.get("name"),
                    "seq": seq,
                    "presentation_role": "supplemental",
                }
                if supplemental_batch
                else None
            ),
            "client_same_route_exceptions": client_same_route_exceptions,
            "capacity": capacity,
            "weight_ack": weight_ack,
            "picking": picking_result,
            "picking_updated": bool(picking_result),
            "message_es": (
                f"OC agregadas correctamente al grupo {supplemental_batch.get('name')}."
                if supplemental_batch
                else "OC agregadas al plan (sin grupo suplementario: plan no organized)."
            ),
            "picking_ready": bool(
                picking_result and uses_supplemental and supplemental_batch
            ),
            "plan": plan_svc.get_dispatch_plan(plan_id),
        }
    )
