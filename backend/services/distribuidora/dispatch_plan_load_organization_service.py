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


ROLE_PREFIX_PRIORITY = "role=priority"
ROLE_PREFIX_CONSOLIDATED = "role=consolidated"
ROLE_PREFIX_SUPPLEMENTAL = "role=supplemental"

# Despachos nuevos con organización anticipada: cuatro grupos fijos (sin auto-asignar OC).
RESERVED_SUPPLEMENTAL_GROUP_NAME = "Grupo adicional"
PREDEFINED_LOAD_GROUPS: tuple[dict[str, Any], ...] = (
    {
        "name": "Grupo 1",
        "role": "priority",
        "sort_order": 1,
        "note": "Clientes prioritarios (recomendado 1–2 por grupo)",
    },
    {
        "name": "Grupo 2",
        "role": "priority",
        "sort_order": 2,
        "note": "Clientes prioritarios (recomendado 1–2 por grupo)",
    },
    {
        "name": "Grupo consolidado",
        "role": "consolidated",
        "sort_order": 3,
        "note": "Clientes restantes de la ruta",
    },
    {
        "name": RESERVED_SUPPLEMENTAL_GROUP_NAME,
        "role": "supplemental:1",
        "sort_order": 4,
        "note": "Reservado para pedidos de última hora",
    },
)


def ensure_predefined_load_batches(cur, plan_id: int) -> tuple[list[dict[str, Any]], bool]:
    """Crea los 4 grupos estándar solo si el plan aún no tiene ningún batch.

    Idempotente: reabrir / refrescar / reintentar no duplica.
    Planes con grupos ya creados (históricos, PLAN-00079, etc.) no se modifican.
    """
    batches = batch_repo.list_load_batches(cur, plan_id)
    if batches:
        return batches, False
    for spec in PREDEFINED_LOAD_GROUPS:
        batch_repo.insert_load_batch(
            cur,
            plan_id=plan_id,
            name=str(spec["name"]),
            description=encode_batch_description(str(spec["role"]), spec.get("note")),
            sort_order=int(spec["sort_order"]),
        )
    return batch_repo.list_load_batches(cur, plan_id), True


def find_reserved_supplemental_batch(
    batches: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Prefiere «Grupo adicional»; si no, el primer batch role=supplemental."""
    supplemental = [
        b
        for b in batches
        if parse_batch_presentation_role(b.get("description")) == "supplemental"
    ]
    if not supplemental:
        return None
    for b in supplemental:
        if str(b.get("name") or "").strip().lower() == RESERVED_SUPPLEMENTAL_GROUP_NAME.lower():
            return b
    supplemental.sort(key=lambda x: (int(x.get("sort_order") or 0), int(x["id"])))
    return supplemental[0]


def parse_batch_presentation_role(description: str | None) -> str | None:
    """Rol visual prioritario|consolidado|supplemental persistido en description."""
    text = (description or "").strip().lower()
    if not text:
        return None
    first = text.splitlines()[0].strip()
    if first.startswith(ROLE_PREFIX_PRIORITY):
        return "priority"
    if first.startswith(ROLE_PREFIX_CONSOLIDATED):
        return "consolidated"
    if first.startswith(ROLE_PREFIX_SUPPLEMENTAL):
        return "supplemental"
    return None


def parse_supplemental_seq(description: str | None) -> int | None:
    """Lee seq=N del prefijo ``role=supplemental;seq=N``."""
    text = (description or "").strip().lower()
    if not text:
        return None
    first = text.splitlines()[0].strip()
    if not first.startswith(ROLE_PREFIX_SUPPLEMENTAL):
        return None
    for part in first.split(";"):
        part = part.strip()
        if part.startswith("seq="):
            try:
                return int(part.split("=", 1)[1])
            except (TypeError, ValueError):
                return None
    return None


def strip_batch_role_description(description: str | None) -> str:
    """Quita el prefijo técnico role=* para mostrar o re-guardar notas humanas."""
    text = (description or "").strip()
    if not text:
        return ""
    lines = text.splitlines()
    first = lines[0].strip().lower()
    if (
        first.startswith(ROLE_PREFIX_PRIORITY)
        or first.startswith(ROLE_PREFIX_CONSOLIDATED)
        or first.startswith(ROLE_PREFIX_SUPPLEMENTAL)
    ):
        return "\n".join(lines[1:]).strip()
    return text


def encode_batch_description(role: str | None, note: str | None) -> str | None:
    note_clean = strip_batch_role_description(note)
    if role == "priority":
        prefix = ROLE_PREFIX_PRIORITY
    elif role == "consolidated":
        prefix = ROLE_PREFIX_CONSOLIDATED
    elif role == "supplemental" or (isinstance(role, str) and role.startswith("supplemental")):
        # role puede ser "supplemental" o "supplemental:2"
        seq = 1
        if isinstance(role, str) and ":" in role:
            try:
                seq = int(role.split(":", 1)[1])
            except (TypeError, ValueError):
                seq = 1
        prefix = f"{ROLE_PREFIX_SUPPLEMENTAL};seq={seq}"
    else:
        return note_clean or None
    if not note_clean:
        return prefix
    return f"{prefix}\n{note_clean}"


def encode_supplemental_batch_description(seq: int, note: str | None = None) -> str:
    return encode_batch_description(f"supplemental:{int(seq)}", note) or (
        f"{ROLE_PREFIX_SUPPLEMENTAL};seq={int(seq)}"
    )


def merge_description_preserving_role(
    existing: str | None, incoming: str | None
) -> str | None:
    """Conserva role= del batch si el cliente actualiza solo la nota/nombre vía description.

    - Si ``incoming`` trae role=, se respeta.
    - Si no, se reutiliza el role de ``existing`` (planes ya clasificados).
    - Descripciones históricas sin role siguen sin clasificación.
    """
    incoming_role = parse_batch_presentation_role(incoming)
    if incoming_role:
        return encode_batch_description(
            incoming_role, strip_batch_role_description(incoming)
        )
    existing_role = parse_batch_presentation_role(existing)
    note = (
        strip_batch_role_description(incoming)
        if incoming is not None
        else strip_batch_role_description(existing)
    )
    return encode_batch_description(existing_role, note)


def _num(v: Any) -> float | None:
    if v is None:
        return None
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    if n != n:  # NaN
        return None
    return n


def build_client_rows(
    orders: list[dict[str, Any]],
    *,
    variants_by_oc: dict[int, list[int]] | None = None,
    boxes_by_oc: dict[int, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Agrega métricas por cliente (todas las OC del cliente juntas)."""
    variants_by_oc = variants_by_oc or {}
    boxes_by_oc = boxes_by_oc or {}
    by_client: dict[Any, dict[str, Any]] = {}

    for o in orders:
        cid = o.get("client_id")
        key = cid if cid is not None else f"name:{o.get('client_name') or 'sin-cliente'}"
        row = by_client.get(key)
        if row is None:
            row = {
                "client_id": cid,
                "client_name": o.get("client_name") or "Sin nombre",
                "oc_count": 0,
                "folios": [],
                "oc_document_ids": [],
                "venta_oc_clp": 0.0,
                "sku_distinct": 0,
                "units": 0.0,
                "cajas_equivalentes": None,
                "cajas_partial": False,
                "peso_conocido_kg": 0.0,
                "peso_coverage_percent": None,
                "load_batch_id": None,
                "load_batch_name": None,
                "limitations": [],
                "_variants": set(),
                "_coverages": [],
                "_cajas_sum": 0.0,
                "_cajas_lines": 0,
                "_cajas_with": 0,
                "_has_cajas": False,
            }
            by_client[key] = row

        oc_id = int(o["oc_document_id"])
        row["oc_count"] += 1
        if o.get("oc_number") is not None:
            row["folios"].append(int(o["oc_number"]))
        row["oc_document_ids"].append(oc_id)
        venta = _num(o.get("oc_total_amount")) or 0.0
        row["venta_oc_clp"] += venta
        units = _num(o.get("cantidad_unidades"))
        if units is None:
            units = 0.0
            if "units_missing" not in row["limitations"]:
                row["limitations"].append("units_missing")
        row["units"] += units
        peso = _num(o.get("peso_total_kg")) or 0.0
        row["peso_conocido_kg"] += peso
        cov = _num(o.get("cobertura_logistica"))
        if cov is not None:
            row["_coverages"].append(cov)

        variants = variants_by_oc.get(oc_id) or []
        if variants:
            row["_variants"].update(variants)
        else:
            # Fallback snapshot: cantidad_productos ≈ líneas, no necesariamente SKU únicos.
            qp = o.get("cantidad_productos")
            if qp is not None and "sku_estimado_por_lineas" not in row["limitations"]:
                row["limitations"].append("sku_estimado_por_lineas")
            # No sumamos líneas entre OC (doble conteo); solo si no hay variantes.

        box = boxes_by_oc.get(oc_id)
        if box:
            row["_has_cajas"] = True
            row["_cajas_sum"] += float(box.get("cajas_known") or 0)
            row["_cajas_lines"] += int(box.get("lines_n") or 0)
            row["_cajas_with"] += int(box.get("lines_with_box") or 0)
            if int(box.get("lines_with_box") or 0) < int(box.get("lines_n") or 0):
                row["cajas_partial"] = True
        else:
            snap_cajas = _num(o.get("cantidad_cajas"))
            if snap_cajas is not None and snap_cajas > 0:
                row["_has_cajas"] = True
                row["_cajas_sum"] += snap_cajas
                if "cajas_desde_snapshot_plan" not in row["limitations"]:
                    row["limitations"].append("cajas_desde_snapshot_plan")
            elif "cajas_no_disponibles" not in row["limitations"]:
                row["limitations"].append("cajas_no_disponibles")

        batch_id = o.get("load_batch_id")
        if batch_id is not None:
            row["load_batch_id"] = int(batch_id)
            row["load_batch_name"] = o.get("load_batch_name")

    clients: list[dict[str, Any]] = []
    for row in by_client.values():
        variants: set[int] = row.pop("_variants")
        coverages: list[float] = row.pop("_coverages")
        cajas_sum = row.pop("_cajas_sum")
        cajas_lines = row.pop("_cajas_lines")
        cajas_with = row.pop("_cajas_with")
        has_cajas = row.pop("_has_cajas")
        if variants:
            row["sku_distinct"] = len(variants)
            row["limitations"] = [
                x for x in row["limitations"] if x != "sku_estimado_por_lineas"
            ]
        else:
            # Sin detalles de variantes: no inventar SKU ni sumar líneas entre OC.
            row["sku_distinct"] = 0
        if has_cajas:
            row["cajas_equivalentes"] = round(cajas_sum, 2)
            if cajas_lines > 0 and cajas_with < cajas_lines:
                row["cajas_partial"] = True
                if "cajas_parciales" not in row["limitations"]:
                    row["limitations"].append("cajas_parciales")
        if coverages:
            row["peso_coverage_percent"] = round(sum(coverages) / len(coverages), 1)
            if any(c < 99.5 for c in coverages):
                if "peso_cobertura_incompleta" not in row["limitations"]:
                    row["limitations"].append("peso_cobertura_incompleta")
        elif row["peso_conocido_kg"] <= 0:
            if "peso_no_disponible" not in row["limitations"]:
                row["limitations"].append("peso_no_disponible")
        row["folios"] = sorted(set(row["folios"]))
        row["oc_document_ids"] = sorted(set(row["oc_document_ids"]))
        row["venta_oc_clp"] = round(float(row["venta_oc_clp"]), 2)
        row["units"] = round(float(row["units"]), 2)
        row["peso_conocido_kg"] = round(float(row["peso_conocido_kg"]), 3)
        clients.append(row)

    clients.sort(key=lambda c: (-float(c["venta_oc_clp"]), str(c["client_name"])))
    return clients


def build_group_summaries(
    clients: list[dict[str, Any]],
    batches: list[dict[str, Any]],
    *,
    variants_by_oc: dict[int, list[int]] | None = None,
) -> list[dict[str, Any]]:
    """Resumen por grupo con SKU distintos consolidados (sin sumar por cliente)."""
    variants_by_oc = variants_by_oc or {}
    by_batch_clients: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for c in clients:
        bid = c.get("load_batch_id")
        if bid is None:
            continue
        by_batch_clients[int(bid)].append(c)

    summaries: list[dict[str, Any]] = []
    for b in batches:
        bid = int(b["id"])
        members = by_batch_clients.get(bid, [])
        role = parse_batch_presentation_role(b.get("description"))
        variant_set: set[int] = set()
        for c in members:
            for oc_id in c.get("oc_document_ids") or []:
                variant_set.update(variants_by_oc.get(int(oc_id), []))
        client_count = len(members)
        priority_review = role == "priority" and client_count > 2
        cajas_vals = [
            float(c["cajas_equivalentes"])
            for c in members
            if c.get("cajas_equivalentes") is not None
        ]
        summaries.append(
            {
                "load_batch_id": bid,
                "name": b.get("name"),
                "description": b.get("description"),
                "sort_order": b.get("sort_order"),
                "presentation_role": role,
                "priority_review_warning": priority_review,
                "client_count": client_count,
                "clients": [
                    {
                        "client_id": c.get("client_id"),
                        "client_name": c.get("client_name"),
                        "oc_count": c.get("oc_count"),
                        "venta_oc_clp": c.get("venta_oc_clp"),
                        "sku_distinct": c.get("sku_distinct"),
                    }
                    for c in members
                ],
                "oc_count": sum(int(c.get("oc_count") or 0) for c in members),
                "venta_oc_clp": round(sum(float(c.get("venta_oc_clp") or 0) for c in members), 2),
                "sku_distinct": len(variant_set),
                "units": round(sum(float(c.get("units") or 0) for c in members), 2),
                "cajas_equivalentes": round(sum(cajas_vals), 2) if cajas_vals else None,
                "peso_conocido_kg": round(
                    sum(float(c.get("peso_conocido_kg") or 0) for c in members), 3
                ),
                "peso_coverage_incomplete": any(
                    "peso_cobertura_incompleta" in (c.get("limitations") or [])
                    or "peso_no_disponible" in (c.get("limitations") or [])
                    for c in members
                ),
            }
        )
    summaries.sort(key=lambda g: (int(g.get("sort_order") or 0), int(g["load_batch_id"])))
    return summaries


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
                    "clients": [],
                    "group_summaries": [],
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

        batches, created_defaults = ensure_predefined_load_batches(cur, plan_id)
        if created_defaults:
            org_repo.insert_organization_event(
                cur,
                plan_id=plan_id,
                action="load_organization_defaults",
                user_name=None,
                payload={
                    "groups": [g["name"] for g in PREDEFINED_LOAD_GROUPS],
                    "note_es": "Grupos predeterminados creados (idempotente).",
                },
            )
            conn.commit()
            batches = batch_repo.list_load_batches(cur, plan_id)

        orders = org_repo.list_plan_orders_for_organization(cur, plan_id)
        assignments = org_repo.list_order_assignments(cur, plan_id)
        by_oc = {
            int(a["oc_document_id"]): int(a["load_batch_id"]) for a in assignments
        }
        completeness = organization_completeness(orders, by_oc)

        variants_by_oc: dict[int, list[int]] = {}
        boxes_by_oc: dict[int, dict[str, Any]] = {}
        try:
            variants_by_oc = org_repo.list_oc_variant_ids_for_plan(cur, plan_id)
        except Exception:  # noqa: BLE001 — métrica opcional
            variants_by_oc = {}
        try:
            boxes_by_oc = org_repo.list_oc_box_coverage_for_plan(cur, plan_id)
        except Exception:  # noqa: BLE001
            boxes_by_oc = {}

        clients = build_client_rows(
            orders, variants_by_oc=variants_by_oc, boxes_by_oc=boxes_by_oc
        )
        enriched_batches = []
        for b in batches:
            item = dict(b)
            role = parse_batch_presentation_role(b.get("description"))
            item["presentation_role"] = role
            item["display_description"] = strip_batch_role_description(b.get("description")) or None
            item["is_reserved_supplemental"] = (
                role == "supplemental"
                and str(b.get("name") or "").strip().lower()
                == RESERVED_SUPPLEMENTAL_GROUP_NAME.lower()
            )
            enriched_batches.append(item)
        group_summaries = build_group_summaries(
            clients, enriched_batches, variants_by_oc=variants_by_oc
        )
        cur.close()
        return serialize_value(
            {
                "dispatch_plan_id": plan_id,
                "schema_ready": True,
                "load_organization_status": meta["load_organization_status"],
                "load_organization_version": meta["load_organization_version"],
                "load_organization_updated_at": meta.get("load_organization_updated_at"),
                "batches": enriched_batches,
                "orders": orders,
                "clients": clients,
                "group_summaries": group_summaries,
                "assignments": assignments,
                "completeness": completeness,
                "predefined_groups_created": created_defaults,
                "guidance": {
                    "manual_only": True,
                    "priority_max_clients_suggested": 2,
                    "empty_groups_ok": True,
                    "sort_default": "venta_desc",
                    "note_es": (
                        "Grupos predeterminados: Grupo 1 y Grupo 2 (prioritarios, "
                        "1–2 clientes), Grupo consolidado y Grupo adicional "
                        "(reservado para OC de última hora). La asignación es manual; "
                        "grupos vacíos no impiden marcar organización completa si "
                        "todas las OC originales están asignadas."
                    ),
                },
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

    # Tras organización completa (o guardado), intentar OC→factura→grupo si hay picking.
    try:
        from backend.services.distribuidora import (
            dispatch_plan_load_batch_service as batch_svc,
        )

        batch_svc.reconcile_document_load_assignments(plan_id, overwrite_manual=False)
    except Exception:
        # No bloquear el guardado de organización por conciliación documental.
        import logging

        logging.getLogger(__name__).exception(
            "document_batch_reconcile_after_organization plan_id=%s", plan_id
        )

    return get_load_organization(plan_id)
