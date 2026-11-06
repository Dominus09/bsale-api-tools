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
    RESERVED_SUPPLEMENTAL_GROUP_NAME,
    encode_supplemental_batch_description,
    find_reserved_supplemental_batch,
    parse_batch_presentation_role,
    parse_supplemental_seq,
)
from backend.services.distribuidora.oc_document_chain_resolver import (
    resolve_operational_statuses_batch,
)
from backend.services.distribuidora.oc_operational_status import (
    BILLING_CANCELLED,
    BILLING_INVOICED_FULL_CN,
    BILLING_PENDING,
    BILLING_PROBABLE,
    admission_block_message,
    supplemental_admission_block,
)
from backend.services.distribuidora.dispatch_plan_oc_guard import (
    find_blocking_oc_assignments,
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
    """Conservado para diagnósticos de planificación nueva. El alta suplementaria no lo usa."""
    inv = status.confirmed_invoice or {}
    return {
        "blocked": True,
        "message": admission_block_message(status),
        "billing_status": status.billing_status,
        "billing_label_es": status.billing_label_es,
        "confirmed_invoice": inv,
        "invoice_number": inv.get("number"),
    }


def _oc_folio(status, oc_id: int) -> int | None:
    if status is not None and status.chain is not None and status.chain.oc_number is not None:
        return int(status.chain.oc_number)
    inv = (status.confirmed_invoice if status is not None else None) or {}
    number = inv.get("oc_number")
    if number is not None:
        try:
            return int(number)
        except (TypeError, ValueError):
            return None
    return None


def _rejection(
    *,
    oc_document_id: int,
    oc_number: int | None,
    code: str,
    message: str,
    invoice_number: int | None = None,
) -> dict[str, Any]:
    return {
        "oc_document_id": int(oc_document_id),
        "oc_number": oc_number,
        "folio": oc_number if oc_number is not None else int(oc_document_id),
        "conflict": code,
        "code": code,
        "message": message,
        "invoice_number": invoice_number,
        "saved": False,
        "rollback": "complete",
    }


def format_supplemental_rejections(rejections: list[dict[str, Any]]) -> str:
    lines = [
        "No se incorporó ninguna OC. La transacción no se confirmó (rollback completo)."
    ]
    for row in rejections:
        folio = row.get("oc_number") or row.get("oc_document_id")
        lines.append(
            f"OC {folio}: tipo={row.get('conflict') or row.get('code')}; "
            f"motivo={row.get('message')}"
        )
    return " ".join(lines)


def _cursor_is_live(cur) -> bool:
    return "psycopg2" in (type(cur).__module__ or "")


def _confirmed_invoice_ids(status) -> list[int]:
    if status is None:
        return []
    if status.chain is not None:
        return sorted({int(x) for x in status.chain.confirmed_invoice_ids})
    inv = status.confirmed_invoice or {}
    if inv.get("document_id") is None:
        return []
    return [int(inv["document_id"])]


def _invoice_number(status) -> int | None:
    inv = (status.confirmed_invoice if status is not None else None) or {}
    number = inv.get("number")
    if number is None and status is not None and status.chain and status.chain.confirmed_invoices:
        number = status.chain.confirmed_invoices[0].number
    try:
        return int(number) if number is not None else None
    except (TypeError, ValueError):
        return None


def collect_supplemental_admission_rejections(
    cur,
    plan_id: int,
    oc_ids: list[int],
    status_map: dict[int, Any],
) -> list[dict[str, Any]]:
    """Reglas de incorporación suplementaria. No aplica el bloqueo de planificación nueva.

    Una OC facturada es válida. Se rechazan anulación, NC total, empresa/oficina
    ajenas, otro despacho, factura ambigua o ya asignada, y duplicado en el lote.
    """
    rejections: list[dict[str, Any]] = []
    invoice_owners: dict[int, list[int]] = {}

    for oc_id in oc_ids:
        status = status_map.get(int(oc_id))
        folio = _oc_folio(status, int(oc_id))
        if status is None:
            if _cursor_is_live(cur):
                rejections.append(
                    _rejection(
                        oc_document_id=int(oc_id),
                        oc_number=folio,
                        code="oc_status_unknown",
                        message="No se pudo resolver el estado documental de la OC.",
                    )
                )
            continue
        blocked = supplemental_admission_block(status)
        if blocked:
            code, message = blocked
            rejections.append(
                _rejection(
                    oc_document_id=int(oc_id),
                    oc_number=folio or (
                        int(status.chain.oc_number)
                        if status.chain is not None and status.chain.oc_number is not None
                        else None
                    ),
                    code=code,
                    message=message,
                    invoice_number=_invoice_number(status),
                )
            )
            continue
        inv_ids = _confirmed_invoice_ids(status)
        if len(inv_ids) == 1:
            invoice_owners.setdefault(inv_ids[0], []).append(int(oc_id))

    for invoice_id, owners in invoice_owners.items():
        if len(owners) < 2:
            continue
        for oc_id in owners:
            status = status_map.get(int(oc_id))
            rejections.append(
                _rejection(
                    oc_document_id=int(oc_id),
                    oc_number=_oc_folio(status, int(oc_id)),
                    code="invoice_shared_in_batch",
                    message=(
                        f"La factura {invoice_id} está vinculada a más de una OC "
                        "del mismo alta y no se puede asignar a un solo grupo."
                    ),
                    invoice_number=_invoice_number(status),
                )
            )

    if not _cursor_is_live(cur):
        return rejections

    rejections.extend(
        _scope_rejections_live(cur, plan_id, oc_ids, status_map, invoice_owners)
    )
    return rejections


def _schema_missing(exc: BaseException) -> bool:
    pgcode = getattr(exc, "pgcode", None)
    if pgcode == "42P01":
        return True
    return "does not exist" in str(exc)


def _query_rows(cur, sql: str, params: tuple):
    """Ejecuta dentro de un savepoint para no abortar el alta si el esquema no está."""
    cur.execute("SAVEPOINT supplemental_scope_q")
    try:
        cur.execute(sql, params)
        rows = cur.fetchall() or []
        cur.execute("RELEASE SAVEPOINT supplemental_scope_q")
        return rows
    except Exception:
        cur.execute("ROLLBACK TO SAVEPOINT supplemental_scope_q")
        cur.execute("RELEASE SAVEPOINT supplemental_scope_q")
        raise


def _scope_rejections_live(
    cur,
    plan_id: int,
    oc_ids: list[int],
    status_map: dict[int, Any],
    invoice_owners: dict[int, list[int]],
) -> list[dict[str, Any]]:
    from backend.services.distribuidora.oc_document_chain_resolver import (
        COMPANY_ID_DEFAULT,
        OFFICE_ID_DEFAULT,
    )

    out: list[dict[str, Any]] = []
    try:
        rows = _query_rows(
            cur,
            """
            SELECT document_id, number, company_id, office_id, COALESCE(state, 0)
            FROM distribuidora.documents
            WHERE document_id = ANY(%s)
            """,
            (list(oc_ids),),
        )
    except Exception as exc:
        if _schema_missing(exc):
            return []
        logger.exception("supplemental_scope_documents_failed plan_id=%s", plan_id)
        return [
            _rejection(
                oc_document_id=int(oc_ids[0]),
                oc_number=None,
                code="oc_scope_unverified",
                message="No se pudo verificar empresa, oficina y vigencia de las OC.",
            )
        ]

    found: dict[int, tuple] = {}
    for row in rows:
        found[int(row[0])] = row
    for oc_id in oc_ids:
        status = status_map.get(int(oc_id))
        row = found.get(int(oc_id))
        folio = int(row[1]) if row and row[1] is not None else _oc_folio(status, int(oc_id))
        if row is None:
            out.append(
                _rejection(
                    oc_document_id=int(oc_id),
                    oc_number=folio,
                    code="oc_not_found",
                    message="La OC no existe en los documentos de la empresa.",
                )
            )
            continue
        company_id = int(row[2]) if row[2] is not None else None
        office_id = int(row[3]) if row[3] is not None else None
        state = int(row[4] or 0)
        if company_id != COMPANY_ID_DEFAULT or office_id != OFFICE_ID_DEFAULT:
            out.append(
                _rejection(
                    oc_document_id=int(oc_id),
                    oc_number=folio,
                    code="oc_wrong_company_or_office",
                    message=(
                        f"Empresa/oficina {company_id}/{office_id} no corresponde "
                        f"a {COMPANY_ID_DEFAULT}/{OFFICE_ID_DEFAULT}."
                    ),
                )
            )
        if state != 0:
            out.append(
                _rejection(
                    oc_document_id=int(oc_id),
                    oc_number=folio,
                    code="oc_cancelled_or_replaced",
                    message="El documento está anulado o reemplazado (state distinto de 0).",
                )
            )

    try:
        cur.execute("SAVEPOINT supplemental_scope_q")
        try:
            conflicts = find_blocking_oc_assignments(
                cur, list(oc_ids), exclude_plan_id=int(plan_id)
            )
            cur.execute("RELEASE SAVEPOINT supplemental_scope_q")
        except Exception:
            cur.execute("ROLLBACK TO SAVEPOINT supplemental_scope_q")
            cur.execute("RELEASE SAVEPOINT supplemental_scope_q")
            raise
    except Exception as exc:
        logger.exception("supplemental_scope_plans_failed plan_id=%s", plan_id)
        conflicts = []
        if not _schema_missing(exc):
            out.append(
                _rejection(
                    oc_document_id=int(oc_ids[0]),
                    oc_number=None,
                    code="oc_scope_unverified",
                    message="No se pudo verificar si la OC ya está en otro despacho.",
                )
            )
    for sample in conflicts:
        oc_id = int(sample.get("oc_document_id"))
        status = status_map.get(oc_id)
        out.append(
            _rejection(
                oc_document_id=oc_id,
                oc_number=_oc_folio(status, oc_id),
                code="oc_on_other_plan",
                message=(
                    "Ya está en el despacho "
                    f"{sample.get('planning_code') or sample.get('dispatch_plan_id')} "
                    f"(estado={sample.get('status')})."
                ),
            )
        )

    invoice_ids = list(invoice_owners.keys())
    if not invoice_ids:
        return out
    try:
        assigned = _query_rows(
            cur,
            """
            SELECT a.related_document_id, a.dispatch_plan_id, a.load_batch_id,
                   p.planning_code, b.name
            FROM distribuidora.dispatch_plan_document_load_assignments a
            JOIN distribuidora.dispatch_plan p ON p.id = a.dispatch_plan_id
            LEFT JOIN distribuidora.dispatch_plan_load_batches b ON b.id = a.load_batch_id
            WHERE a.related_document_id = ANY(%s)
            """,
            (invoice_ids,),
        )
    except Exception as exc:
        if _schema_missing(exc):
            return out
        logger.exception("supplemental_scope_invoices_failed plan_id=%s", plan_id)
        return out + [
            _rejection(
                oc_document_id=int(oc_ids[0]),
                oc_number=None,
                code="oc_scope_unverified",
                message="No se pudo verificar si la factura ya está asignada a un grupo.",
            )
        ]
    for related_id, other_plan, _batch_id, code, batch_name in assigned:
        owners = invoice_owners.get(int(related_id), [])
        for oc_id in owners:
            status = status_map.get(int(oc_id))
            if int(other_plan) != int(plan_id):
                conflict = "invoice_on_other_plan"
                message = (
                    f"La factura ya está asignada al despacho {code or other_plan}."
                )
            else:
                conflict = "invoice_crosses_groups"
                message = (
                    "La factura ya está asignada al grupo "
                    f"«{batch_name or _batch_id}» de este despacho."
                )
            out.append(
                _rejection(
                    oc_document_id=int(oc_id),
                    oc_number=_oc_folio(status, int(oc_id)),
                    code=conflict,
                    message=message,
                    invoice_number=_invoice_number(status),
                )
            )
    return out


def _oc_ready_for_product_picking(status) -> bool:
    """Factura/boleta confirmada usable; no inventa picking sobre OC pendientes."""
    if status is None:
        return False
    if not status.confirmed_invoice:
        return False
    billing = str(status.billing_status or "")
    if billing in {
        BILLING_PENDING,
        BILLING_CANCELLED,
        BILLING_PROBABLE,
        BILLING_INVOICED_FULL_CN,
    }:
        return False
    return True


def assess_oc_invoicing_for_picking(cur, oc_ids: list[int]) -> dict[str, Any]:
    status_map = resolve_operational_statuses_batch(cur, oc_ids)
    items: list[dict[str, Any]] = []
    ready_ids: list[int] = []
    pending_ids: list[int] = []
    for oc_id in oc_ids:
        st = status_map.get(int(oc_id))
        ready = _oc_ready_for_product_picking(st)
        inv = (st.confirmed_invoice if st else None) or {}
        row = {
            "oc_document_id": int(oc_id),
            "billing_status": (st.billing_status if st else BILLING_PENDING),
            "billing_label_es": (st.billing_label_es if st else "Pendiente por facturar"),
            "ready_for_product_picking": ready,
            "confirmed_invoice_id": inv.get("document_id"),
            "invoice_number": inv.get("number"),
        }
        items.append(row)
        if ready:
            ready_ids.append(int(oc_id))
        else:
            pending_ids.append(int(oc_id))
    return {
        "items": items,
        "ready_oc_document_ids": ready_ids,
        "pending_invoice_oc_document_ids": pending_ids,
        "any_invoiced": bool(ready_ids),
        "all_invoiced": bool(oc_ids) and not pending_ids,
        "picking_pending_invoice": bool(pending_ids),
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


def _weight_label_es(
    bucket: str, peso: float | None, *, coverage: float, sin_peso: int
) -> str:
    if bucket == "known" and peso is not None:
        return f"{round(peso, 3)} kg"
    if bucket == "partial" and peso is not None:
        return f"Peso incompleto ({round(peso, 3)} kg conocidos)"
    return "Peso incompleto"


def _oc_meta_for_capacity(cur, oc_ids: list[int]) -> dict[int, dict[str, Any]]:
    """Número OC, cliente, venta, SKU y unidades (lectura; no inventa peso)."""
    if not oc_ids:
        return {}
    cur.execute(
        """
        SELECT
            d.document_id,
            d.number,
            d.total_amount,
            d.client_id,
            COALESCE(
                NULLIF(BTRIM(c.nombre_fantasia), ''),
                NULLIF(BTRIM(c.company), ''),
                CONCAT_WS(
                    ' ',
                    NULLIF(BTRIM(c.first_name), ''),
                    NULLIF(BTRIM(c.last_name), '')
                )
            ) AS client_name
        FROM distribuidora.documents d
        LEFT JOIN bsale.clients c
            ON c.company_id = d.company_id AND c.bsale_id = d.client_id
        WHERE d.document_id = ANY(%s)
          AND d.company_id = 3
        """,
        (list(oc_ids),),
    )
    by_id: dict[int, dict[str, Any]] = {}
    for row in cur.fetchall():
        by_id[int(row[0])] = {
            "oc_number": int(row[1]) if row[1] is not None else None,
            "oc_total_amount": float(row[2]) if row[2] is not None else None,
            "client_id": int(row[3]) if row[3] is not None else None,
            "client_name": (row[4] or None),
        }
    cur.execute(
        """
        SELECT
            document_id,
            COUNT(DISTINCT variant_id)::int AS sku_distinct,
            COALESCE(SUM(quantity), 0) AS units
        FROM distribuidora.document_details
        WHERE document_id = ANY(%s)
        GROUP BY document_id
        """,
        (list(oc_ids),),
    )
    for row in cur.fetchall():
        meta = by_id.setdefault(int(row[0]), {})
        meta["sku_distinct"] = int(row[1] or 0)
        try:
            meta["cantidad_unidades"] = float(row[2] or 0)
        except (TypeError, ValueError):
            meta["cantidad_unidades"] = None
    return by_id


def _capacity_for_plan_plus_ocs(
    cur,
    plan_id: int,
    *,
    new_oc_ids: list[int],
    truck_max: int | None,
) -> dict[str, Any]:
    existing = plan_repo.list_plan_orders(cur, plan_id)
    new_set = set(int(x) for x in new_oc_ids)
    plan_known = 0.0
    estimated = 0.0
    plan_incomplete = 0
    plan_unevaluable = 0
    for o in existing:
        if int(o["oc_document_id"]) in new_set:
            continue  # reintento: no doble contar
        bucket, peso = _weight_bucket_from_order_row(o)
        if bucket == "known" and peso is not None:
            plan_known += peso
        elif bucket == "partial" and peso is not None:
            plan_known += peso
            plan_incomplete += 1
        else:
            plan_unevaluable += 1

    from backend.services.order_weight_service import fetch_weights_by_document_ids

    snaps = fetch_weights_by_document_ids(new_oc_ids, cur=cur)
    meta_by_id = _oc_meta_for_capacity(cur, [int(x) for x in new_oc_ids])
    new_details: list[dict[str, Any]] = []
    additional_known = 0.0
    new_incomplete = 0
    new_unevaluable = 0
    for oc_id in new_oc_ids:
        snap = snaps.get(int(oc_id)) or {}
        bucket, peso = _weight_bucket_from_snapshot(snap if snap else None)
        meta = meta_by_id.get(int(oc_id)) or {}
        try:
            cov = float(
                snap.get("porcentaje_cobertura_peso")
                or snap.get("porcentaje_cobertura")
                or 0
            )
        except (TypeError, ValueError):
            cov = 0.0
        sin = int(snap.get("productos_sin_peso") or 0)
        units = meta.get("cantidad_unidades")
        if units is None and snap:
            try:
                units = float(snap.get("cantidad_unidades") or 0) or None
            except (TypeError, ValueError):
                units = None
        try:
            cajas = float(snap.get("cantidad_cajas")) if snap.get("cantidad_cajas") is not None else None
        except (TypeError, ValueError):
            cajas = None
        # Cajas solo si el snapshot las trae; no inventar equivalencia.
        cajas_confiables = cajas if cajas is not None and cajas > 0 else None
        detail = {
            "oc_document_id": int(oc_id),
            "oc_number": meta.get("oc_number"),
            "client_id": meta.get("client_id"),
            "client_name": meta.get("client_name"),
            "oc_total_amount": meta.get("oc_total_amount"),
            "sku_distinct": int(meta.get("sku_distinct") or 0),
            "cantidad_unidades": units,
            "cantidad_cajas": cajas_confiables,
            "cajas_equivalentes_confiables": cajas_confiables,
            "weight_quality": bucket,
            "known_weight_kg": peso,
            "coverage_percent": cov,
            "productos_sin_peso": sin,
            "weight_complete": bucket == "known",
            "weight_label_es": _weight_label_es(bucket, peso, coverage=cov, sin_peso=sin),
        }
        new_details.append(detail)
        if bucket == "known" and peso is not None:
            additional_known += peso
        elif bucket == "partial" and peso is not None:
            additional_known += peso
            new_incomplete += 1
        else:
            new_unevaluable += 1

    known = plan_known + additional_known
    incomplete = plan_incomplete + new_incomplete
    unevaluable = plan_unevaluable + new_unevaluable

    capacity = validate_truck_capacity(
        known_weight_kg=known,
        estimated_weight_kg=estimated,
        incomplete_orders=incomplete,
        unevaluable_orders=unevaluable,
        truck_max_weight_kg=truck_max,
    )
    capacity["new_orders"] = new_details
    capacity["plan_known_weight_kg"] = round(plan_known, 3)
    capacity["additional_known_weight_kg"] = round(additional_known, 3)
    capacity["projected_known_weight_kg"] = round(known, 3)
    capacity["plan_incomplete_order_count"] = plan_incomplete
    capacity["plan_unevaluable_order_count"] = plan_unevaluable
    capacity["new_incomplete_order_count"] = new_incomplete
    capacity["new_unevaluable_order_count"] = new_unevaluable
    capacity["new_orders_weight_incomplete"] = (new_incomplete + new_unevaluable) > 0
    capacity["weight_coverage_warnings"] = list(capacity.get("warnings") or [])
    if plan_incomplete or plan_unevaluable:
        capacity["weight_coverage_warnings"] = list(
            dict.fromkeys(
                [
                    *(capacity.get("weight_coverage_warnings") or []),
                    "plan_existing_weight_incomplete",
                ]
            )
        )
    if new_incomplete or new_unevaluable:
        capacity["weight_coverage_warnings"] = list(
            dict.fromkeys(
                [
                    *(capacity.get("weight_coverage_warnings") or []),
                    "new_oc_weight_incomplete",
                ]
            )
        )
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


def _resolve_supplemental_target(
    cur, plan_id: int, batches: list[dict[str, Any]]
) -> tuple[dict[str, Any], int, bool]:
    """Reutiliza «Grupo adicional» (o el suplementario existente). Crea solo si no hay ninguno.

    Returns: (batch, seq, created_now)
    """
    existing = find_reserved_supplemental_batch(batches)
    if existing:
        seq = parse_supplemental_seq(existing.get("description")) or 1
        return existing, seq, False
    seq = _next_supplemental_seq(batches)
    sort_order = max((int(b.get("sort_order") or 0) for b in batches), default=0) + 1
    desc = encode_supplemental_batch_description(
        seq, "Reservado para pedidos de última hora"
    )
    created = batch_repo.insert_load_batch(
        cur,
        plan_id=plan_id,
        name=RESERVED_SUPPLEMENTAL_GROUP_NAME,
        description=desc,
        sort_order=sort_order,
    )
    return created, seq, True


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
        reserved = find_reserved_supplemental_batch(batches)
        next_seq = (
            parse_supplemental_seq(reserved.get("description"))
            if reserved
            else _next_supplemental_seq(batches)
        ) or 1
        proposed_name = (
            str(reserved.get("name") or RESERVED_SUPPLEMENTAL_GROUP_NAME)
            if reserved
            else RESERVED_SUPPLEMENTAL_GROUP_NAME
        )
        uses_supplemental = org_status == "organized"
        invoicing = assess_oc_invoicing_for_picking(cur, novel or oc_ids)
        admission = collect_supplemental_admission_rejections(
            cur, plan_id, novel or oc_ids, 
            resolve_operational_statuses_batch(cur, novel or oc_ids),
        )
        if admission and can_add:
            can_add = False
            blocked_reason = format_supplemental_rejections(admission)

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
            "proposed_supplemental_group_id": (
                int(reserved["id"]) if uses_supplemental and reserved else None
            ),
            "proposed_supplemental_reused": bool(uses_supplemental and reserved),
            "proposed_supplemental_seq": next_seq if uses_supplemental else None,
            "oc_document_ids": oc_ids,
            "already_in_plan": already,
            "novel_oc_document_ids": novel,
            "capacity": capacity,
            "invoicing": invoicing,
            "picking_pending_invoice": bool(invoicing.get("picking_pending_invoice")),
            "requires_incomplete_weight_ack": bool(
                capacity.get("requires_incomplete_weight_ack")
            ),
            "requires_unevaluable_weight_ack": bool(
                capacity.get("requires_unevaluable_weight_ack")
            ),
            "note_es": (
                (
                    f"Las OC irán al grupo «{proposed_name}»"
                    + (" (ya existente)." if reserved else " (se creará si falta).")
                    + " Los grupos originales no se modifican. "
                    "El picking de productos del suplemento requiere facturación vigente."
                )
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
    F3-D2: regenera picking versionado solo si hay facturación usable (o flag explícito).
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
            dup = [
                _rejection(
                    oc_document_id=int(oc_id),
                    oc_number=_oc_folio(status_map.get(int(oc_id)), int(oc_id)),
                    code="oc_duplicate",
                    message="Ya pertenece a este despacho y no está en el grupo suplementario.",
                )
                for oc_id in already
            ]
            raise SupplementalOrderError(
                format_supplemental_rejections(dup),
                code="oc_duplicate",
                http_status=409,
                details={"rejections": dup, "saved": False, "rollback": "complete"},
            )

        if already and novel:
            dup = [
                _rejection(
                    oc_document_id=int(oc_id),
                    oc_number=_oc_folio(status_map.get(int(oc_id)), int(oc_id)),
                    code="oc_duplicate",
                    message=(
                        "Ya pertenece a este despacho. "
                        "No se incorporó ninguna OC del lote."
                    ),
                )
                for oc_id in already
            ]
            raise SupplementalOrderError(
                format_supplemental_rejections(dup),
                code="oc_duplicate",
                http_status=409,
                details={"rejections": dup, "saved": False, "rollback": "complete"},
            )

        rejections = collect_supplemental_admission_rejections(
            cur, plan_id, novel, status_map
        )
        if rejections:
            raise SupplementalOrderError(
                format_supplemental_rejections(rejections),
                code=str(rejections[0].get("code") or "oc_not_admissible"),
                http_status=409,
                details={
                    "rejections": rejections,
                    "saved": False,
                    "rollback": "complete",
                },
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
            supplemental_batch, seq, _created_supp = _resolve_supplemental_target(
                cur, plan_id, batches
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

        snaps = fetch_weights_by_document_ids(novel, cur=cur)
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
    reconciliation = None
    has_picking = False
    invoicing: dict[str, Any] = {
        "items": [],
        "ready_oc_document_ids": [],
        "pending_invoice_oc_document_ids": list(novel),
        "any_invoiced": False,
        "all_invoiced": False,
        "picking_pending_invoice": True,
    }
    conn2 = get_connection()
    try:
        cur2 = conn2.cursor()
        has_picking = batch_repo.plan_has_current_picking(cur2, plan_id)
        invoicing = assess_oc_invoicing_for_picking(cur2, novel)
        cur2.close()
    finally:
        conn2.close()

    # F3-D2: versionar picking global solo con docs facturados (no inventar suplemento).
    auto_regen = bool(has_picking and uses_supplemental and invoicing.get("any_invoiced"))
    force_regen = bool(has_picking and regenerate_picking)
    should_regen = auto_regen or force_regen
    if should_regen:
        if not invoicing.get("any_invoiced") and force_regen and uses_supplemental:
            # Flag explícito sin facturas: no generar picking ficticio del suplemento.
            logger.info(
                "supplemental_skip_regen_no_invoice plan_id=%s ocs=%s",
                plan_id,
                novel,
            )
        else:
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

                reconciliation = batch_svc.reconcile_document_load_assignments(
                    plan_id, overwrite_manual=False
                )
            except Exception:
                logger.exception("reconcile_after_supplemental plan_id=%s", plan_id)

    supplemental_picking_ready = bool(
        uses_supplemental
        and supplemental_batch
        and invoicing.get("any_invoiced")
        and picking_result
        and reconciliation
        and reconciliation.get("document_assignment_complete")
    )
    n_added = len(novel)
    group_name = supplemental_batch.get("name") if supplemental_batch else None
    if group_name:
        msg = f"{n_added} OC incorporadas correctamente a {group_name}."
    else:
        msg = (
            f"{n_added} OC incorporadas correctamente al plan "
            "(sin grupo suplementario: plan no organized)."
        )
    if invoicing.get("picking_pending_invoice"):
        msg += " Picking definitivo pendiente de facturación."
    elif supplemental_picking_ready:
        msg += " Picking suplementario listo (exportar por grupo)."
    elif picking_result:
        msg += " Picking general versionado; revise conciliación documental."

    oc_numbers: list[int] = []
    for row in enriched:
        n = row.get("oc_number")
        if n is not None:
            try:
                oc_numbers.append(int(n))
            except (TypeError, ValueError):
                pass
    picking_version = None
    if isinstance(picking_result, dict):
        picking_version = (
            picking_result.get("version")
            or picking_result.get("picking_version")
            or (picking_result.get("picking") or {}).get("version")
        )
    billing_labels = [
        str(it.get("billing_label_es") or it.get("billing_status") or "")
        for it in (invoicing.get("items") or [])
    ]
    verification = {
        "oc_document_ids": novel,
        "oc_numbers": oc_numbers,
        "oc_count": len(novel),
        "supplemental_group_id": (
            int(supplemental_batch["id"]) if supplemental_batch else None
        ),
        "supplemental_group_name": (
            supplemental_batch.get("name") if supplemental_batch else None
        ),
        "additional_known_weight_kg": capacity.get("additional_known_weight_kg"),
        "billing_labels_es": billing_labels,
        "picking_pending_invoice": bool(invoicing.get("picking_pending_invoice")),
        "picking_updated": bool(picking_result),
        "picking_version": picking_version,
        "cuadratura_note_es": (
            "Hay una nueva versión de picking; la cuadratura abierta debe detectarla "
            "sin adopción automática."
            if picking_result
            else (
                "Sin nueva versión de picking: facturación pendiente o no regenerada."
                if invoicing.get("picking_pending_invoice")
                else None
            )
        ),
    }

    return serialize_value(
        {
            "status": "added",
            "dispatch_plan_id": plan_id,
            "added": True,
            "oc_document_ids": novel,
            "oc_numbers": oc_numbers,
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
            "invoicing": invoicing,
            "picking": picking_result,
            "picking_updated": bool(picking_result),
            "picking_version": picking_version,
            "document_reconciliation": reconciliation,
            "message_es": msg,
            "picking_ready": supplemental_picking_ready,
            "supplemental_picking_ready": supplemental_picking_ready,
            "picking_pending_invoice": bool(invoicing.get("picking_pending_invoice")),
            "verification": verification,
            "plan": plan_svc.get_dispatch_plan(plan_id),
        }
    )
