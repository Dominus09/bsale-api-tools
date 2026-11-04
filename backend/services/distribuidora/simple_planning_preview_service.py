"""Preview de listado de OC para planificación simplificada (solo lectura).

Normaliza folios pegados desde Excel, resuelve la versión vigente en PostgreSQL
y arma una previsualización reutilizando cadena documental, pesos y planes.

No escribe ni confirma planes. No inventa relaciones documentales.
"""

from __future__ import annotations

import hashlib
import logging
import re
import time
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Callable

from backend.db import get_connection
from backend.services.distribuidora.oc_document_chain_resolver import (
    resolve_oc_document_chains_batch,
)
from backend.services.distribuidora.oc_operational_status import (
    BILLING_CANCELLED,
    BILLING_INVOICED_PARTIAL_CN,
    BILLING_LABELS_ES,
    BILLING_PENDING,
    BILLING_PROBABLE,
    build_operational_status,
)
from backend.services.distribuidora.oc_source_resolver import (
    COMPANY_ID,
    OC_DOCUMENT_TYPE_ID,
    OFFICE_ID,
)
from backend.services.order_weight_service import get_order_weight_summaries_batch

logger = logging.getLogger(__name__)

MAX_FOLIOS = 200
# Cobertura mínima para considerar el peso usable en preview.
MIN_WEIGHT_COVERAGE_PCT = 50.0

STATUS_FOUND_AVAILABLE = "found_available"
STATUS_NOT_FOUND = "not_found"
STATUS_CANCELLED_OR_REPLACED = "cancelled_or_replaced"
STATUS_ALREADY_PLANNED = "already_planned"
STATUS_PENDING_INVOICE = "pending_invoice"
STATUS_PARTIAL_INVOICE = "partial_invoice"
# Compat: insufficient_weight se mantiene como alias de cobertura incompleta.
STATUS_INCOMPLETE_WEIGHT = "incomplete_weight_coverage"
STATUS_WEIGHT_UNEVALUABLE = "weight_unevaluable"
STATUS_INSUFFICIENT_WEIGHT = STATUS_INCOMPLETE_WEIGHT  # legacy alias
STATUS_DUPLICATE_IN_LIST = "duplicate_in_list"
STATUS_INVALID_FOLIO = "invalid_folio"

WEIGHT_KNOWN = "known"
WEIGHT_ESTIMATED = "estimated"
WEIGHT_PARTIAL = "partial"
WEIGHT_UNEVALUABLE = "unevaluable"

# En esta fase, ya_planificada es informativa (no bloquea).
BLOCKING_STATUSES = frozenset(
    {
        STATUS_NOT_FOUND,
        STATUS_CANCELLED_OR_REPLACED,
        STATUS_DUPLICATE_IN_LIST,
        STATUS_INVALID_FOLIO,
    }
)

# Planes que bloquean reasignación futura (no closed/squared: permiten reprogramar).
ACTIVE_PLAN_STATUSES = frozenset(
    {
        "draft",
        "planned",
        "invoicing",
        "ready_for_picking",
        "picking_generated",
        "dispatched",
        "delivered",
    }
)

# Conflictos duros para el confirmador. closed/squared también bloquean:
# no hay saldos de reprogramación confiables en el modelo actual.
CONFLICTING_PLAN_STATUSES = frozenset(
    {
        "planned",
        "invoicing",
        "ready_for_picking",
        "picking_generated",
        "dispatched",
        "delivered",
        "closed",
        "squared",
    }
)

_FOLIO_CLEAN_RE = re.compile(r"[^\d]")


class SimplePlanningPreviewError(ValueError):
    """Entrada inválida o fuera de límites."""


def _int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _canon_decimal(value: Any, *, places: int = 2) -> str:
    """Representación decimal canónica (evita 10 vs 10.0 vs '10.00')."""
    if value is None or value == "":
        return ""
    try:
        quant = Decimal("1").scaleb(-places) if places >= 0 else Decimal("1")
        d = Decimal(str(value)).quantize(quant, rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError, TypeError):
        return str(value).strip()
    return format(d, "f")


def _canon_emission_date(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    return text[:10] if text else ""


def _hash_part(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def oc_lines_fingerprint_raw(products: list[dict[str, Any]] | None) -> str:
    """Líneas comerciales ordenadas (variant_id + qty). Independiente del orden SQL."""
    lines: list[str] = []
    for prod in products or []:
        if not isinstance(prod, dict):
            continue
        vid = _int(prod.get("variant_id"))
        qty = _canon_decimal(prod.get("quantity"), places=3)
        # Sin description/code/detail_id: no deben alterar la huella comercial.
        lines.append(f"{vid if vid is not None else ''}:{qty}")
    lines.sort()
    return "|".join(lines)


def oc_fingerprint_parts(
    *,
    document_id: int | None,
    source_document_id: int | None = None,
    state: int | None = None,
    client_id: int | None = None,
    total_amount: Any = None,
    emission_date: Any = None,
    products: list[dict[str, Any]] | None = None,
) -> dict[str, str] | None:
    """Partes canónicas para categorizar drift (identity / header / lines)."""
    if document_id is None:
        return None
    identity_raw = (
        f"{int(document_id)}|"
        f"{'' if source_document_id is None else int(source_document_id)}|"
        f"{'' if state is None else int(state)}"
    )
    header_raw = (
        f"{'' if client_id is None else int(client_id)}|"
        f"{_canon_decimal(total_amount, places=2)}|"
        f"{_canon_emission_date(emission_date)}"
    )
    lines_raw = oc_lines_fingerprint_raw(products)
    return {
        "identity": _hash_part(identity_raw),
        "header": _hash_part(header_raw),
        "lines": _hash_part(lines_raw),
    }


def oc_identity_fingerprint(
    *,
    document_id: int | None,
    source_document_id: int | None = None,
    state: int | None = None,
    client_id: int | None = None,
    total_amount: Any = None,
    emission_date: Any = None,
    products: list[dict[str, Any]] | None = None,
    updated_at: Any = None,
) -> str | None:
    """Huella comercial estable entre preview y confirmación.

    Incluye identidad documental, cabecera comercial y líneas (variant+qty).
    ``updated_at`` se ignora a propósito: la sincronización Bsale lo toca sin
    cambio comercial y generaba falsos positivos de fingerprint_drift.
    """
    del updated_at  # ruido de sync; no forma parte de la huella comercial
    parts = oc_fingerprint_parts(
        document_id=document_id,
        source_document_id=source_document_id,
        state=state,
        client_id=client_id,
        total_amount=total_amount,
        emission_date=emission_date,
        products=products,
    )
    if not parts:
        return None
    raw = f"{parts['identity']}#{parts['header']}#{parts['lines']}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def format_fingerprint_drift_message(drifts: list[dict[str, Any]]) -> str:
    """Mensaje accionable por folio para prepare/confirm."""
    folios = sorted(
        {f for f in (_int(d.get("folio")) for d in drifts) if f is not None}
    )
    if len(folios) == 1:
        return (
            f"OC {folios[0]}: sus datos cambiaron después de la previsualización. "
            "Actualiza las órdenes y vuelve a preparar la confirmación"
        )
    if folios:
        listed = ", ".join(str(f) for f in folios[:8])
        more = f" (+{len(folios) - 8} más)" if len(folios) > 8 else ""
        return (
            f"OC {listed}{more}: sus datos cambiaron después de la previsualización. "
            "Actualiza las órdenes y vuelve a preparar la confirmación"
        )
    return (
        "Una o más OC cambiaron después de la previsualización. "
        "Actualiza las órdenes y vuelve a preparar la confirmación"
    )


def normalize_folio_inputs(raw_folios: list[Any]) -> dict[str, Any]:
    """Normaliza entradas pegadas (Excel / líneas): limpia, valida y detecta duplicados."""
    if not isinstance(raw_folios, list):
        raise SimplePlanningPreviewError("folios debe ser una lista")

    entries: list[dict[str, Any]] = []
    seen: dict[int, int] = {}
    invalid: list[dict[str, Any]] = []
    duplicates: list[int] = []

    for idx, raw in enumerate(raw_folios):
        original = "" if raw is None else str(raw).strip()
        if not original:
            continue
        # Permitir "70625.0" típico de Excel.
        cleaned = original.replace("\u00a0", " ").strip()
        if cleaned.endswith(".0") and cleaned[:-2].isdigit():
            cleaned = cleaned[:-2]
        digits = _FOLIO_CLEAN_RE.sub("", cleaned)
        if not digits or not digits.isdigit():
            invalid.append(
                {
                    "index": idx,
                    "input": original,
                    "status": STATUS_INVALID_FOLIO,
                    "blocking": True,
                    "detail": "Folio inválido (se esperan dígitos)",
                }
            )
            continue
        folio = int(digits)
        if folio <= 0:
            invalid.append(
                {
                    "index": idx,
                    "input": original,
                    "status": STATUS_INVALID_FOLIO,
                    "blocking": True,
                    "detail": "Folio debe ser > 0",
                }
            )
            continue
        is_dup = folio in seen
        if is_dup:
            duplicates.append(folio)
        else:
            seen[folio] = idx
        entries.append(
            {
                "index": idx,
                "input": original,
                "folio": folio,
                "duplicate_in_list": is_dup,
            }
        )

    unique_folios = list(seen.keys())
    if len(unique_folios) > MAX_FOLIOS:
        raise SimplePlanningPreviewError(
            f"Máximo {MAX_FOLIOS} folios únicos por solicitud "
            f"(recibidos {len(unique_folios)})"
        )
    if not entries and not invalid:
        raise SimplePlanningPreviewError("Indique al menos un folio de OC")

    return {
        "entries": entries,
        "unique_folios": unique_folios,
        "invalid": invalid,
        "duplicate_folios": sorted(set(duplicates)),
        "empty_lines_skipped": len(raw_folios) - len(entries) - len(invalid),
    }


def _load_ocs_by_folios(
    *,
    company_id: int,
    office_id: int,
    folios: list[int],
    document_type_id: int = OC_DOCUMENT_TYPE_ID,
) -> dict[int, dict[str, Any]]:
    """Versión vigente por número: preferir state=0, luego más reciente."""
    if not folios:
        return {}
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT ON (d.number)
                d.document_id,
                d.number AS folio,
                d.state,
                d.client_id,
                d.total_amount,
                d.emission_date,
                d.company_id,
                d.office_id,
                d.document_type_id,
                d.source_document_id,
                d.updated_at,
                COALESCE(
                    NULLIF(BTRIM(d.municipality), ''),
                    NULLIF(BTRIM(c.municipality), ''),
                    NULLIF(BTRIM(d.city), ''),
                    NULLIF(BTRIM(c.city), '')
                ) AS municipality,
                COALESCE(
                    NULLIF(BTRIM(d.address), ''),
                    NULLIF(BTRIM(c.address), '')
                ) AS address,
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
                ON c.company_id = d.company_id
               AND c.bsale_id = d.client_id
            WHERE d.company_id = %s
              AND d.office_id = %s
              AND d.document_type_id = %s
              AND d.number = ANY(%s)
            ORDER BY
                d.number,
                CASE WHEN COALESCE(d.state, 0) = 0 THEN 0 ELSE 1 END,
                COALESCE(d.source_updated_at, d.updated_at, d.emission_date) DESC NULLS LAST,
                d.document_id DESC
            """,
            (int(company_id), int(office_id), int(document_type_id), list(folios)),
        )
        cols = [d[0] for d in cur.description]
        out: dict[int, dict[str, Any]] = {}
        for row in cur.fetchall():
            item = dict(zip(cols, row))
            folio = _int(item.get("folio"))
            if folio is None:
                continue
            out[folio] = item
        cur.close()
        return out
    finally:
        conn.close()


def _load_products_by_document_ids(document_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
    """Líneas de la OC (no de facturas) para no duplicar cantidades."""
    if not document_ids:
        return {}
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                document_id,
                detail_id,
                variant_id,
                variant_description,
                variant_code,
                quantity,
                total_amount
            FROM distribuidora.document_details
            WHERE document_id = ANY(%s)
            ORDER BY document_id, detail_id
            """,
            (list(document_ids),),
        )
        cols = [d[0] for d in cur.description]
        out: dict[int, list[dict[str, Any]]] = {}
        for row in cur.fetchall():
            item = dict(zip(cols, row))
            doc_id = int(item["document_id"])
            out.setdefault(doc_id, []).append(
                {
                    "detail_id": _int(item.get("detail_id")),
                    "variant_id": _int(item.get("variant_id")),
                    "description": item.get("variant_description"),
                    "code": item.get("variant_code"),
                    "quantity": float(item["quantity"])
                    if item.get("quantity") is not None
                    else None,
                    "total_amount": float(item["total_amount"])
                    if item.get("total_amount") is not None
                    else None,
                }
            )
        cur.close()
        return out
    finally:
        conn.close()


def _load_plan_assignments(document_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
    if not document_ids:
        return {}
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                dpo.oc_document_id,
                p.id AS dispatch_plan_id,
                p.planning_code,
                p.status,
                p.planning_date,
                p.truck_name,
                p.route_name,
                p.planning_name
            FROM distribuidora.dispatch_plan_orders dpo
            INNER JOIN distribuidora.dispatch_plan p
                ON p.id = dpo.dispatch_plan_id
            WHERE dpo.oc_document_id = ANY(%s)
            ORDER BY p.confirmed_at DESC NULLS LAST, p.id DESC
            """,
            (list(document_ids),),
        )
        cols = [d[0] for d in cur.description]
        out: dict[int, list[dict[str, Any]]] = {}
        for row in cur.fetchall():
            item = dict(zip(cols, row))
            oid = int(item["oc_document_id"])
            status = str(item.get("status") or "")
            out.setdefault(oid, []).append(
                {
                    "dispatch_plan_id": int(item["dispatch_plan_id"]),
                    "planning_code": item.get("planning_code"),
                    "status": status,
                    "planning_date": item["planning_date"].isoformat()
                    if item.get("planning_date") is not None
                    else None,
                    "truck_name": item.get("truck_name"),
                    "route_name": item.get("route_name"),
                    "planning_name": item.get("planning_name"),
                    "active": status in ACTIVE_PLAN_STATUSES,
                }
            )
        cur.close()
        return out
    finally:
        conn.close()


def _load_chains_and_status(
    document_ids: list[int],
    *,
    company_id: int,
) -> tuple[dict[int, Any], dict[int, Any]]:
    if not document_ids:
        return {}, {}
    conn = get_connection()
    try:
        cur = conn.cursor()
        chains = resolve_oc_document_chains_batch(
            cur, document_ids, company_id=int(company_id)
        )
        statuses = {
            oid: build_operational_status(chain, in_plan=False)
            for oid, chain in chains.items()
        }
        cur.close()
        return chains, statuses
    finally:
        conn.close()


def _serialize_invoices(chain: Any) -> list[dict[str, Any]]:
    if chain is None:
        return []
    docs = []
    for inv in list(getattr(chain, "confirmed_invoices", None) or []):
        docs.append({**inv.as_dict(), "relation": "confirmed"})
    for inv in list(getattr(chain, "probable_invoices", None) or []):
        docs.append({**inv.as_dict(), "relation": "probable"})
    return docs


def _serialize_weight(
    weight: dict[str, Any] | None,
    *,
    products_without_weight: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    quality = classify_weight_quality(weight)
    peso = (weight or {}).get("peso_total_kg")
    known = None
    if peso is not None and float(peso) > 0:
        known = float(peso)
    return {
        "peso_total_kg": peso,
        "known_weight_kg": known,
        "coverage_percent": (weight or {}).get("coverage_percent"),
        "missing_products": (weight or {}).get("missing_products"),
        "productos_totales": (weight or {}).get("productos_totales"),
        "cantidad_cajas": (weight or {}).get("cantidad_cajas"),
        "cantidad_unidades": (weight or {}).get("cantidad_unidades"),
        "estimated_products": (weight or {}).get("estimated_products"),
        "quality": quality,
        "weight_total_determined": quality in (WEIGHT_KNOWN, WEIGHT_ESTIMATED),
        "products_without_weight": list(products_without_weight or []),
        "label": (
            "Cobertura completa"
            if quality == WEIGHT_KNOWN
            else "Cobertura estimada"
            if quality == WEIGHT_ESTIMATED
            else "Cobertura de peso incompleta"
            if quality == WEIGHT_PARTIAL
            else "Capacidad total aún no verificable"
        ),
    }


def classify_weight_quality(weight: dict[str, Any] | None) -> str:
    """known | estimated | partial | unevaluable.

    - known: todos los productos con peso.
    - estimated: cobertura completa con estimaciones.
    - partial: hay kg conocidos pero faltan productos (cobertura incompleta).
    - unevaluable: ningún peso usable (no inventar ceros).

    Compat: el valor legacy ``unavailable`` se mapea a unevaluable en consumidores.
    """
    if not weight:
        return WEIGHT_UNEVALUABLE
    missing = int(weight.get("missing_products") or 0)
    coverage = float(weight.get("coverage_percent") or 0)
    estimated = int(weight.get("estimated_products") or 0)
    peso = weight.get("peso_total_kg")
    has_known_mass = peso is not None and float(peso) > 0

    if missing > 0:
        return WEIGHT_PARTIAL if has_known_mass else WEIGHT_UNEVALUABLE
    if not has_known_mass:
        # Snapshot ausente o 0 kg sin líneas con peso.
        return WEIGHT_UNEVALUABLE
    if estimated > 0 or coverage < 99.9:
        return WEIGHT_ESTIMATED
    return WEIGHT_KNOWN


def _weight_coverage_warning(weight: dict[str, Any] | None) -> bool:
    return classify_weight_quality(weight) in (WEIGHT_PARTIAL, WEIGHT_UNEVALUABLE)


def _load_products_without_weight(
    document_ids: list[int],
) -> dict[int, list[dict[str, Any]]]:
    """Productos sin peso en snapshot (solo lectura). Vacío si no hay líneas."""
    if not document_ids:
        return {}
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                ows.document_id,
                l.variant_id,
                l.codigo,
                COALESCE(NULLIF(BTRIM(l.producto), ''), NULLIF(BTRIM(l.variante), ''), l.codigo)
                    AS description
            FROM distribuidora.order_weight_snapshots ows
            INNER JOIN distribuidora.order_weight_snapshot_lines l
                ON l.snapshot_id = ows.id
            WHERE ows.document_id = ANY(%s)
              AND l.estado_linea = 'sin_peso'
            ORDER BY ows.document_id, l.line_number NULLS LAST, l.id
            """,
            (list(document_ids),),
        )
        out: dict[int, list[dict[str, Any]]] = {}
        for doc_id, variant_id, code, description in cur.fetchall():
            out.setdefault(int(doc_id), []).append(
                {
                    "variant_id": _int(variant_id),
                    "code": code,
                    "description": description,
                }
            )
        cur.close()
        return out
    except Exception:
        logger.exception("simple_planning_missing_weight_products_failed")
        return {}
    finally:
        conn.close()


def _classify_item(
    *,
    entry: dict[str, Any],
    oc_row: dict[str, Any] | None,
    billing_status: str | None,
    active_plans: list[dict[str, Any]],
    weight: dict[str, Any] | None,
) -> tuple[str, bool, list[str]]:
    """Devuelve (status_principal, blocking, warnings_secundarias)."""
    warnings: list[str] = []
    if entry.get("duplicate_in_list"):
        return STATUS_DUPLICATE_IN_LIST, True, warnings
    if oc_row is None:
        return STATUS_NOT_FOUND, True, warnings

    state = int(oc_row.get("state") or 0)
    if state != 0 or billing_status == BILLING_CANCELLED:
        return STATUS_CANCELLED_OR_REPLACED, True, warnings

    if active_plans:
        warnings.append(STATUS_ALREADY_PLANNED)
    if billing_status == BILLING_INVOICED_PARTIAL_CN:
        warnings.append(STATUS_PARTIAL_INVOICE)
    elif billing_status in (BILLING_PENDING, BILLING_PROBABLE):
        warnings.append(STATUS_PENDING_INVOICE)
    wq = classify_weight_quality(weight)
    if wq == WEIGHT_UNEVALUABLE:
        warnings.append(STATUS_WEIGHT_UNEVALUABLE)
    elif wq == WEIGHT_PARTIAL:
        warnings.append(STATUS_INCOMPLETE_WEIGHT)

    # Prioridad del status principal (informativos; no bloquean en esta fase).
    if active_plans:
        return STATUS_ALREADY_PLANNED, False, [w for w in warnings if w != STATUS_ALREADY_PLANNED]
    if billing_status == BILLING_INVOICED_PARTIAL_CN:
        return STATUS_PARTIAL_INVOICE, False, [w for w in warnings if w != STATUS_PARTIAL_INVOICE]
    if billing_status in (BILLING_PENDING, BILLING_PROBABLE):
        return STATUS_PENDING_INVOICE, False, [w for w in warnings if w != STATUS_PENDING_INVOICE]
    if STATUS_WEIGHT_UNEVALUABLE in warnings:
        return (
            STATUS_WEIGHT_UNEVALUABLE,
            False,
            [w for w in warnings if w != STATUS_WEIGHT_UNEVALUABLE],
        )
    if STATUS_INCOMPLETE_WEIGHT in warnings:
        return (
            STATUS_INCOMPLETE_WEIGHT,
            False,
            [w for w in warnings if w != STATUS_INCOMPLETE_WEIGHT],
        )
    return STATUS_FOUND_AVAILABLE, False, warnings


def preview_ocs_from_folios(
    *,
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
    folios: list[Any],
    document_type_id: int = OC_DOCUMENT_TYPE_ID,
    oc_loader: Callable[..., dict[int, dict[str, Any]]] | None = None,
    products_loader: Callable[..., dict[int, list[dict[str, Any]]]] | None = None,
    plans_loader: Callable[..., dict[int, list[dict[str, Any]]]] | None = None,
    chains_loader: Callable[..., tuple[dict[int, Any], dict[int, Any]]] | None = None,
    weight_loader: Callable[..., dict[int, dict[str, Any]]] | None = None,
    missing_weight_loader: Callable[..., dict[int, list[dict[str, Any]]]] | None = None,
) -> dict[str, Any]:
    """Resuelve y previsualiza un listado de OC. Solo lectura."""
    if int(document_type_id) != int(OC_DOCUMENT_TYPE_ID):
        raise SimplePlanningPreviewError(
            f"Solo se admiten OC tipo {OC_DOCUMENT_TYPE_ID}; recibido={document_type_id}"
        )

    started = time.perf_counter()
    started_at = datetime.now(timezone.utc)
    normalized = normalize_folio_inputs(list(folios))
    entries = normalized["entries"]
    unique_folios = normalized["unique_folios"]

    load_ocs = oc_loader or _load_ocs_by_folios
    load_products = products_loader or _load_products_by_document_ids
    load_plans = plans_loader or _load_plan_assignments
    load_chains = chains_loader or _load_chains_and_status
    load_weights = weight_loader or (
        lambda ids, **kw: get_order_weight_summaries_batch(
            ids,
            company_id=int(company_id),
            office_id=int(office_id),
            persist_cache=False,
            log_planning=False,
        )
    )
    load_missing_weight = missing_weight_loader or _load_products_without_weight

    by_folio = load_ocs(
        company_id=int(company_id),
        office_id=int(office_id),
        folios=unique_folios,
        document_type_id=int(document_type_id),
    )
    doc_ids = [
        int(row["document_id"])
        for row in by_folio.values()
        if row.get("document_id") is not None
    ]

    products_by_doc = load_products(doc_ids) if doc_ids else {}
    plans_by_doc = load_plans(doc_ids) if doc_ids else {}
    chains, op_statuses = (
        load_chains(doc_ids, company_id=int(company_id)) if doc_ids else ({}, {})
    )
    weights = load_weights(doc_ids) if doc_ids else {}
    missing_weight_by_doc = load_missing_weight(doc_ids) if doc_ids else {}

    items: list[dict[str, Any]] = []
    counts: dict[str, int] = {}
    blocking_count = 0
    warning_count = 0

    for bad in normalized["invalid"]:
        items.append(
            {
                **bad,
                "folio": None,
                "warnings": [],
                "wrote": False,
                "audit_oc": {
                    "suggested": False,
                    "path": "/distribuidora/auditoria-oc",
                    "hint": None,
                },
            }
        )
        counts[STATUS_INVALID_FOLIO] = counts.get(STATUS_INVALID_FOLIO, 0) + 1
        blocking_count += 1

    for entry in entries:
        folio = int(entry["folio"])
        oc_row = by_folio.get(folio)
        doc_id = int(oc_row["document_id"]) if oc_row and oc_row.get("document_id") else None
        chain = chains.get(doc_id) if doc_id is not None else None
        op = op_statuses.get(doc_id) if doc_id is not None else None
        billing = op.billing_status if op is not None else None
        weight = weights.get(doc_id) if doc_id is not None else None
        plans = plans_by_doc.get(doc_id, []) if doc_id is not None else []
        active_plans = [p for p in plans if p.get("active")]

        status, blocking, warnings = _classify_item(
            entry=entry,
            oc_row=oc_row,
            billing_status=billing,
            active_plans=active_plans,
            weight=weight,
        )
        if blocking:
            blocking_count += 1
        if warnings:
            warning_count += 1
        counts[status] = counts.get(status, 0) + 1

        emission = oc_row.get("emission_date") if oc_row else None
        updated_at = (oc_row or {}).get("updated_at")
        source_id = _int((oc_row or {}).get("source_document_id"))
        state_n = _int((oc_row or {}).get("state"))
        client_id_n = _int((oc_row or {}).get("client_id"))
        total_amount = (
            oc_row.get("total_amount")
            if oc_row and oc_row.get("total_amount") is not None
            else None
        )
        products = products_by_doc.get(doc_id, []) if doc_id is not None else []
        fp_parts = (
            oc_fingerprint_parts(
                document_id=doc_id,
                source_document_id=source_id,
                state=state_n,
                client_id=client_id_n,
                total_amount=total_amount,
                emission_date=emission,
                products=products,
            )
            if doc_id is not None
            else None
        )
        conflicting_plans = [
            p
            for p in plans
            if str(p.get("status") or "") in CONFLICTING_PLAN_STATUSES
        ]
        item: dict[str, Any] = {
            "index": entry["index"],
            "input": entry["input"],
            "folio": folio,
            "status": status,
            "blocking": blocking,
            "warnings": warnings,
            "wrote": False,
            "document_id": doc_id,
            "source_document_id": source_id,
            "state": state_n,
            "updated_at": updated_at.isoformat()
            if hasattr(updated_at, "isoformat")
            else (str(updated_at) if updated_at else None),
            "identity_fingerprint": oc_identity_fingerprint(
                document_id=doc_id,
                source_document_id=source_id,
                state=state_n,
                client_id=client_id_n,
                total_amount=total_amount,
                emission_date=emission,
                products=products,
            ),
            "fingerprint_parts": fp_parts,
            "client": {
                "client_id": client_id_n,
                "client_name": (oc_row or {}).get("client_name"),
                "municipality": (oc_row or {}).get("municipality"),
                "address": (oc_row or {}).get("address"),
            }
            if oc_row
            else None,
            "emission_date": emission.isoformat()
            if hasattr(emission, "isoformat")
            else (str(emission) if emission else None),
            "total_amount": float(total_amount) if total_amount is not None else None,
            "products": products,
            "products_source": "oc_document_details",
            "weight": _serialize_weight(
                weight,
                products_without_weight=(
                    missing_weight_by_doc.get(doc_id, [])
                    if doc_id is not None
                    else []
                ),
            ),
            "billing_status": billing,
            "billing_label": BILLING_LABELS_ES.get(billing or "", billing),
            "planning_eligible": bool(op.planning_eligible) if op else None,
            "invoices": _serialize_invoices(chain),
            "invoice_count": len(_serialize_invoices(chain)),
            "plans": plans,
            "assigned_to_plan": bool(active_plans),
            "conflicting_active_plans": conflicting_plans,
            "audit_oc": {
                "suggested": status == STATUS_NOT_FOUND,
                "path": "/distribuidora/auditoria-oc",
                "hint": (
                    "Investigue el folio en Auditoría OC (sin recuperación automática)."
                    if status == STATUS_NOT_FOUND
                    else None
                ),
            },
        }
        items.append(item)

    items.sort(key=lambda x: (x.get("index") is None, x.get("index") or 0))

    return {
        "status": "completed",
        "mode": "simple_planning_preview_readonly",
        "wrote": False,
        "company_id": int(company_id),
        "office_id": int(office_id),
        "document_type_id": int(document_type_id),
        "limits": {"max_folios": MAX_FOLIOS},
        "requested_raw": len(folios),
        "parsed_entries": len(entries),
        "unique_folios": len(unique_folios),
        "duplicate_folios": normalized["duplicate_folios"],
        "blocking_count": blocking_count,
        "warning_count": warning_count,
        "counts": counts,
        "items": items,
        "notes": {
            "products_from_oc_only": (
                "Las cantidades vienen de document_details de la OC; "
                "las facturas se listan aparte para no duplicar."
            ),
            "already_planned_informative": (
                "La asignación a otros planes es informativa en esta fase; "
                "el bloqueo transaccional llega antes de confirmar."
            ),
            "no_auto_recovery": (
                "OC no halladas remiten a Auditoría OC; no se recupera automáticamente."
            ),
            "document_id_vs_source": (
                "document_id es PK local; source_document_id es el id Bsale vigente."
            ),
        },
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.perf_counter() - started, 3),
    }
