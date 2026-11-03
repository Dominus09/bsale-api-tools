"""Asignación preliminar OC → camiones (solo lectura).

Reutiliza ``preview_ocs_from_folios``. No confirma planes ni genera picking real.
Prepara validaciones de capacidad y duplicados para el confirmador futuro.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timezone
from typing import Any, Callable

from backend.db import get_connection
from backend.services.distribuidora.oc_source_resolver import COMPANY_ID, OFFICE_ID
from backend.services.distribuidora.simple_planning_preview_service import (
    CONFLICTING_PLAN_STATUSES,
    MAX_FOLIOS,
    SimplePlanningPreviewError,
    STATUS_DUPLICATE_IN_LIST,
    STATUS_NOT_FOUND,
    _int,
    classify_weight_quality,
    normalize_folio_inputs,
    oc_identity_fingerprint,
    preview_ocs_from_folios,
)

logger = logging.getLogger(__name__)

PLANNING_ORIGIN_SIMPLE = "simple_list"
PLANNING_ORIGIN_ORS = "ors"

WEIGHT_KNOWN = "known"
WEIGHT_ESTIMATED = "estimated"
WEIGHT_UNAVAILABLE = "unavailable"


def validate_truck_capacity(
    *,
    assigned_weight_kg: float | None,
    known_weight_kg: float | None,
    estimated_weight_kg: float | None,
    unavailable_orders: int,
    truck_max_weight_kg: int | None,
) -> dict[str, Any]:
    """Validación reutilizable (preview y futuro confirm). No inventa capacidad."""
    cap = int(truck_max_weight_kg) if truck_max_weight_kg is not None else None
    assigned = float(assigned_weight_kg or 0)
    known = float(known_weight_kg or 0)
    estimated = float(estimated_weight_kg or 0)
    util_pct = None
    overloaded = False
    capacity_known = cap is not None and cap > 0
    if capacity_known:
        util_pct = round((assigned / cap) * 100, 2)
        overloaded = assigned > float(cap)
    return {
        "truck_max_weight_kg": cap,
        "capacity_known": capacity_known,
        "assigned_weight_kg": round(assigned, 3),
        "known_weight_kg": round(known, 3),
        "estimated_weight_kg": round(estimated, 3),
        "unavailable_order_count": int(unavailable_orders),
        "utilization_pct": util_pct,
        "overloaded": overloaded,
        "blocking": overloaded,
        "warnings": (
            (["truck_overloaded"] if overloaded else [])
            + (["truck_capacity_unknown"] if not capacity_known else [])
            + (["orders_with_unavailable_weight"] if unavailable_orders else [])
        ),
    }


def detect_request_duplicate_folios(assignments: list[dict[str, Any]]) -> list[int]:
    """OC repetida dentro de la misma solicitud de asignación."""
    seen: set[int] = set()
    dups: set[int] = set()
    for row in assignments:
        folio = _int(row.get("folio"))
        if folio is None:
            continue
        if folio in seen:
            dups.add(folio)
        seen.add(folio)
    return sorted(dups)


def detect_active_plan_conflicts(
    items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """OC con planes en estados que bloquearían el confirm futuro."""
    out: list[dict[str, Any]] = []
    for item in items:
        folio = _int(item.get("folio"))
        doc_id = _int(item.get("document_id"))
        conflicts = item.get("conflicting_active_plans") or []
        if not conflicts:
            # Fallback: filtrar plans por status
            conflicts = [
                p
                for p in (item.get("plans") or [])
                if str(p.get("status") or "") in CONFLICTING_PLAN_STATUSES
            ]
        if conflicts:
            out.append(
                {
                    "folio": folio,
                    "document_id": doc_id,
                    "plans": conflicts,
                    "blocking_for_future_confirm": True,
                    "note": (
                        "Informativo en esta fase; el confirmador bloqueará "
                        "transaccionalmente estados planned…dispatched "
                        "(no closed/squared)."
                    ),
                }
            )
    return out


_PART_LABELS = {
    "identity": "identidad_documental",
    "header": "cabecera_comercial",
    "lines": "lineas_producto",
}


def detect_fingerprint_drift(
    *,
    expected_by_folio: dict[int, str],
    items: list[dict[str, Any]],
    expected_parts_by_folio: dict[int, dict[str, str]] | None = None,
    stage: str | None = None,
) -> list[dict[str, Any]]:
    """OC cuyo identity_fingerprint cambió respecto al preview previo.

    No expone huellas completas (solo prefijos) para logs/API seguros.
    Si hay ``fingerprint_parts`` esperadas y actuales, categoriza el cambio.
    """
    drifts: list[dict[str, Any]] = []
    if not expected_by_folio:
        return drifts
    parts_map = expected_parts_by_folio or {}
    for item in items:
        folio = _int(item.get("folio"))
        if folio is None or folio not in expected_by_folio:
            continue
        current = item.get("identity_fingerprint")
        expected = expected_by_folio[folio]
        if not (current and expected and current != expected):
            continue
        current_parts = item.get("fingerprint_parts") or {}
        expected_parts = parts_map.get(folio) or {}
        changed_parts: list[str] = []
        if expected_parts and current_parts:
            for key in ("identity", "header", "lines"):
                if expected_parts.get(key) != current_parts.get(key):
                    changed_parts.append(_PART_LABELS.get(key, key))
        if not changed_parts:
            # Sin partes previas (p.ej. FE solo envió hash): no inventar categoría.
            changed_parts = ["desconocido"]
        entry = {
            "folio": folio,
            "document_id": item.get("document_id"),
            "changed_parts": changed_parts,
            "blocking": True,
            "detail": (
                f"OC {folio}: sus datos cambiaron después de la previsualización"
            ),
            "expected_fingerprint_prefix": str(expected)[:8],
            "current_fingerprint_prefix": str(current)[:8],
        }
        if stage:
            entry["stage"] = stage
        drifts.append(entry)
        logger.info(
            "fingerprint_drift folio=%s stage=%s changed_parts=%s "
            "expected_prefix=%s current_prefix=%s",
            folio,
            stage or "unspecified",
            ",".join(changed_parts),
            entry["expected_fingerprint_prefix"],
            entry["current_fingerprint_prefix"],
        )
    return drifts


def _load_trucks_by_ids(truck_ids: list[int]) -> dict[int, dict[str, Any]]:
    if not truck_ids:
        return {}
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, name, plate, max_weight_kg, active
            FROM distribuidora.trucks
            WHERE id = ANY(%s)
            """,
            (list(truck_ids),),
        )
        cols = [d[0] for d in cur.description]
        out: dict[int, dict[str, Any]] = {}
        for row in cur.fetchall():
            item = dict(zip(cols, row))
            tid = int(item["id"])
            out[tid] = {
                "truck_id": tid,
                "name": item.get("name"),
                "plate": item.get("plate"),
                "max_weight_kg": int(item["max_weight_kg"])
                if item.get("max_weight_kg") is not None
                else None,
                "active": bool(item.get("active")),
            }
        cur.close()
        return out
    finally:
        conn.close()


def _parse_planning_date(value: Any) -> date:
    if value is None:
        return date.today()
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    text = str(value).strip()
    try:
        return date.fromisoformat(text[:10])
    except ValueError as exc:
        raise SimplePlanningPreviewError(
            f"planning_date inválida: {value!r}"
        ) from exc


def _consolidate_products(orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Consolidado preliminar por producto (desde líneas OC, no facturas)."""
    bucket: dict[str, dict[str, Any]] = {}
    for order in orders:
        for line in order.get("products") or []:
            key = str(
                line.get("variant_id")
                or line.get("code")
                or line.get("description")
                or line.get("detail_id")
                or "unknown"
            )
            qty = float(line.get("quantity") or 0)
            entry = bucket.get(key)
            if entry is None:
                bucket[key] = {
                    "variant_id": line.get("variant_id"),
                    "code": line.get("code"),
                    "description": line.get("description"),
                    "quantity": qty,
                    "oc_folios": [order.get("folio")],
                }
            else:
                entry["quantity"] = float(entry["quantity"]) + qty
                folio = order.get("folio")
                if folio not in entry["oc_folios"]:
                    entry["oc_folios"].append(folio)
    return sorted(
        bucket.values(),
        key=lambda x: (str(x.get("description") or ""), str(x.get("code") or "")),
    )


def preview_truck_assignments(
    *,
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
    planning_date: Any = None,
    dispatch_name: str | None = None,
    assignments: list[dict[str, Any]],
    expected_fingerprints: dict[int, str] | None = None,
    preview_fn: Callable[..., dict[str, Any]] | None = None,
    trucks_loader: Callable[[list[int]], dict[int, dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Previsualiza asignación OC→camión. Solo lectura; wrote=false."""
    if not isinstance(assignments, list) or not assignments:
        raise SimplePlanningPreviewError("Indique al menos una asignación folio→camión")
    if len(assignments) > MAX_FOLIOS:
        raise SimplePlanningPreviewError(
            f"Máximo {MAX_FOLIOS} asignaciones por solicitud"
        )

    started = time.perf_counter()
    started_at = datetime.now(timezone.utc)
    pdate = _parse_planning_date(planning_date)
    dname = (dispatch_name or "").strip() or None

    # Normalizar filas de asignación
    normalized_rows: list[dict[str, Any]] = []
    raw_folios: list[Any] = []
    unknown_truck: list[dict[str, Any]] = []
    folio_first_index: dict[int, int] = {}
    request_dups_set: set[int] = set()
    for idx, raw in enumerate(assignments):
        if not isinstance(raw, dict):
            raise SimplePlanningPreviewError(f"assignments[{idx}] debe ser objeto")
        folio_raw = raw.get("folio")
        truck_id = _int(raw.get("truck_id"))
        if truck_id is None or truck_id <= 0:
            unknown_truck.append(
                {
                    "index": idx,
                    "folio": folio_raw,
                    "detail": "truck_id obligatorio y > 0",
                    "blocking": True,
                }
            )
            continue
        parsed = normalize_folio_inputs([folio_raw])
        folio_n = parsed["unique_folios"][0] if parsed["unique_folios"] else None
        if folio_n is None:
            unknown_truck.append(
                {
                    "index": idx,
                    "folio": folio_raw,
                    "detail": (parsed["invalid"][0]["detail"] if parsed["invalid"] else "folio inválido"),
                    "blocking": True,
                }
            )
            continue
        if folio_n in folio_first_index:
            request_dups_set.add(folio_n)
        else:
            folio_first_index[folio_n] = idx
        raw_folios.append(folio_raw)
        normalized_rows.append(
            {
                "index": idx,
                "folio_raw": folio_raw,
                "folio": folio_n,
                "truck_id": truck_id,
                "driver_name": (
                    str(raw.get("driver_name")).strip()
                    if raw.get("driver_name") is not None
                    else None
                )
                or None,
                "expected_fingerprint": raw.get("expected_fingerprint")
                or (expected_fingerprints or {}).get(folio_n),
                "duplicate_in_request": folio_n in request_dups_set
                and folio_first_index.get(folio_n) != idx,
            }
        )
    # Segunda pasada: marcar duplicados (todas las apariciones tras la primera)
    for row in normalized_rows:
        folio_n = int(row["folio"])
        row["duplicate_in_request"] = (
            folio_n in request_dups_set and folio_first_index.get(folio_n) != row["index"]
        )
    request_dups = sorted(request_dups_set)

    preview = (preview_fn or preview_ocs_from_folios)(
        company_id=int(company_id),
        office_id=int(office_id),
        folios=raw_folios,
    )
    items_by_folio: dict[int, dict[str, Any]] = {}
    for item in preview.get("items") or []:
        folio = _int(item.get("folio"))
        if folio is None:
            continue
        # Primera aparición no-duplicada del listado gana para asignación
        if item.get("status") == STATUS_DUPLICATE_IN_LIST:
            continue
        if folio not in items_by_folio:
            items_by_folio[folio] = item

    # Fingerprints esperados (body o mapa)
    expected_map: dict[int, str] = dict(expected_fingerprints or {})
    for row in normalized_rows:
        fp = row.get("expected_fingerprint")
        folio = int(row["folio"])
        if fp:
            expected_map[folio] = str(fp)
    drifts = detect_fingerprint_drift(
        expected_by_folio=expected_map,
        items=list(items_by_folio.values()),
        stage="assign-preview",
    )
    drift_folios = {int(d["folio"]) for d in drifts if d.get("folio") is not None}

    truck_ids = sorted({int(r["truck_id"]) for r in normalized_rows})
    load_trucks = trucks_loader or _load_trucks_by_ids
    trucks = load_trucks(truck_ids)

    # Armar por camión
    trucks_out: list[dict[str, Any]] = []
    assignment_rows: list[dict[str, Any]] = []
    blocking_count = 0
    warning_count = 0

    for tid in truck_ids:
        truck = trucks.get(tid)
        truck_missing = truck is None
        truck_inactive = bool(truck and not truck.get("active"))
        truck_orders: list[dict[str, Any]] = []
        known_kg = 0.0
        estimated_kg = 0.0
        unavailable_n = 0
        driver_names: list[str] = []

        for row in normalized_rows:
            if int(row["truck_id"]) != tid:
                continue
            folio = int(row["folio"])
            item = items_by_folio.get(folio)

            row_blocking = False
            row_warnings: list[str] = []
            if row.get("duplicate_in_request"):
                row_blocking = True
                status = STATUS_DUPLICATE_IN_LIST
                row_warnings.append("duplicate_in_request")
            elif item is None:
                row_blocking = True
                status = STATUS_NOT_FOUND
            else:
                status = str(item.get("status") or "unknown")
                row_blocking = bool(item.get("blocking"))
                row_warnings = list(item.get("warnings") or [])
                if folio in drift_folios:
                    row_blocking = True
                    row_warnings.append("modified_after_preview")
                if item.get("conflicting_active_plans"):
                    row_warnings.append("already_on_conflicting_plan")

            if truck_missing:
                row_blocking = True
                row_warnings.append("truck_not_found")
            if truck_inactive:
                row_blocking = True
                row_warnings.append("truck_inactive")

            # Pesos: solo contar la primera aparición no duplicada
            if not row.get("duplicate_in_request"):
                weight = (item or {}).get("weight") or {}
                quality = weight.get("quality") or classify_weight_quality(weight)
                peso = weight.get("peso_total_kg")
                if quality == WEIGHT_KNOWN and peso is not None:
                    known_kg += float(peso)
                elif quality == WEIGHT_ESTIMATED and peso is not None:
                    estimated_kg += float(peso)
                else:
                    unavailable_n += 1
                if item is not None:
                    truck_orders.append(item)

            if row.get("driver_name"):
                driver_names.append(str(row["driver_name"]))

            assigned = {
                "index": row["index"],
                "folio": folio,
                "truck_id": tid,
                "driver_name": row.get("driver_name"),
                "status": status,
                "blocking": row_blocking,
                "warnings": row_warnings,
                "order": item,
                "identity_fingerprint": (item or {}).get("identity_fingerprint"),
            }
            assignment_rows.append(assigned)
            if row_blocking:
                blocking_count += 1
            elif row_warnings:
                warning_count += 1

        assigned_kg = known_kg + estimated_kg
        capacity = validate_truck_capacity(
            assigned_weight_kg=assigned_kg,
            known_weight_kg=known_kg,
            estimated_weight_kg=estimated_kg,
            unavailable_orders=unavailable_n,
            truck_max_weight_kg=(truck or {}).get("max_weight_kg"),
        )
        if capacity["blocking"]:
            blocking_count += 1
        elif capacity["warnings"]:
            warning_count += 1

        trucks_out.append(
            {
                "truck_id": tid,
                "truck": truck,
                "truck_missing": truck_missing,
                "truck_inactive": truck_inactive,
                "driver_names": sorted(set(driver_names)),
                "order_count": len(truck_orders),
                "orders": [
                    {
                        "folio": o.get("folio"),
                        "document_id": o.get("document_id"),
                        "client": o.get("client"),
                        "total_amount": o.get("total_amount"),
                        "billing_status": o.get("billing_status"),
                        "billing_label": o.get("billing_label"),
                        "invoices": o.get("invoices"),
                        "weight": o.get("weight"),
                        "status": o.get("status"),
                        "blocking": o.get("blocking"),
                        "warnings": o.get("warnings"),
                        "identity_fingerprint": o.get("identity_fingerprint"),
                        "fingerprint_parts": o.get("fingerprint_parts"),
                        "products": o.get("products"),
                    }
                    for o in truck_orders
                ],
                "capacity": capacity,
                "picking_preview": {
                    "kind": "preliminary_from_oc",
                    "not_final_invoiced_picking": True,
                    "note": (
                        "Detalle preliminar desde líneas de OC. El picking "
                        "definitivo se genera solo con boletas/facturas "
                        "confirmadas tras confirmar el plan."
                    ),
                    "products_consolidated": _consolidate_products(truck_orders),
                    "clients": [
                        {
                            "folio": o.get("folio"),
                            "client": o.get("client"),
                            "product_lines": len(o.get("products") or []),
                        }
                        for o in truck_orders
                    ],
                },
                "blocking": truck_missing
                or truck_inactive
                or bool(capacity.get("blocking")),
                "warnings": list(capacity.get("warnings") or []),
            }
        )

    plan_conflicts = detect_active_plan_conflicts(list(items_by_folio.values()))

    return {
        "status": "completed",
        "mode": "simple_planning_assign_preview_readonly",
        "planning_origin": PLANNING_ORIGIN_SIMPLE,
        "wrote": False,
        "company_id": int(company_id),
        "office_id": int(office_id),
        "planning_date": pdate.isoformat(),
        "dispatch_name": dname,
        "truck_count": len(trucks_out),
        "assignment_count": len(assignment_rows),
        "blocking_count": blocking_count,
        "warning_count": warning_count,
        "request_duplicate_folios": request_dups,
        "fingerprint_drifts": drifts,
        "active_plan_conflicts": plan_conflicts,
        "unknown_truck_assignments": unknown_truck,
        "oc_preview_summary": {
            "unique_folios": preview.get("unique_folios"),
            "counts": preview.get("counts"),
            "blocking_count": preview.get("blocking_count"),
        },
        "trucks": trucks_out,
        "assignments": assignment_rows,
        "future_confirm_notes": {
            "planning_origin": PLANNING_ORIGIN_SIMPLE,
            "ors_metrics": (
                "Al confirmar sin ORS: persistir km/duration/liters/fuel como NULL "
                "(no calculados), no como cero. Requiere migración aditiva "
                "planning_origin + nullable costs o flag ors_metrics_calculated."
            ),
            "plan_session_id": (
                "Generar UUID firmado/ligado al user_id; rechazar sesiones ajenas."
            ),
            "duplicate_guard": (
                "SELECT … FOR UPDATE de document_ids en conflicto con "
                f"status IN {sorted(CONFLICTING_PLAN_STATUSES)}; "
                "closed/squared permiten reprogramar."
            ),
            "fingerprint": (
                "Revalidar identity_fingerprint antes de insertar dispatch_plan_orders."
            ),
        },
        "notes": {
            "no_writes": "Esta fase no crea dispatch_plan ni pickings/loads.",
            "picking_is_preliminary": True,
            "same_motor": (
                "La confirmación futura debe llamar confirm_dispatch_plan "
                "(o wrapper) hacia las mismas tablas dispatch_plan*."
            ),
        },
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.perf_counter() - started, 3),
    }
