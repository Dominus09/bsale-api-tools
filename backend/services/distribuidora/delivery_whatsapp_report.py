"""Formatos de reporte WhatsApp P4 (solo texto). Sin envío automático.

No incluye RUT completo, teléfono personal, coordenadas precisas, fotos ni firmas.
No inserta enlaces de invitación a grupos.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

from backend.services.distribuidora.delivery_semaphore import (
    REASON_LABEL_ES,
    WHATSAPP_HEADERS,
    resolve_semaphore,
)

SANTIAGO = ZoneInfo("America/Santiago")

PAYMENT_METHOD_ES = {
    "cash": "Efectivo",
    "transfer_declared": "Transf. informada",
    "transfer_verified": "Transf. verificada",
    "credit": "Crédito",
    "partial": "Cobranza parcial",
    "pending": "Pago pendiente",
    "not_applicable": "Sin cobro",
    "cash_and_transfer": "Efectivo + transf.",
}

REASON_ES = dict(REASON_LABEL_ES)
REASON_ES.setdefault("other", "Otra incidencia")

TEMPLATE_HEADERS = dict(WHATSAPP_HEADERS)
TEMPLATE_HEADERS.setdefault("absent_customer", "🔴 QUILLOTANA | CLIENTE AUSENTE")

MAX_REPORT_LINES = 6


def _local(dt: datetime | str | None) -> datetime | None:
    if dt is None:
        return None
    if isinstance(dt, str):
        text = dt.strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(SANTIAGO)


def _fmt_dt(dt: datetime | None) -> str:
    if not dt:
        return "—"
    return dt.strftime("%d/%m/%Y %H:%M")


def _fmt_date(dt: datetime | None) -> str:
    if not dt:
        return "—"
    return dt.strftime("%d/%m/%Y")


def _fmt_time(dt: datetime | None) -> str:
    if not dt:
        return "—"
    return dt.strftime("%H:%M")


def _fmt_duration(start: datetime | None, end: datetime | None) -> str:
    if not start or not end:
        return "—"
    delta = end - start
    if delta.total_seconds() < 0:
        return "—"
    total_m = int(delta.total_seconds() // 60)
    h, m = divmod(total_m, 60)
    if h:
        return f"{h} h {m} min"
    return f"{m} min"


def _fmt_money(n: float | int | None) -> str:
    if n is None:
        return "—"
    try:
        v = float(n)
    except (TypeError, ValueError):
        return "—"
    return f"${v:,.0f}".replace(",", ".")


def _fmt_qty(n: float | int | None) -> str:
    if n is None:
        return "—"
    try:
        v = float(n)
    except (TypeError, ValueError):
        return "—"
    if abs(v - round(v)) < 1e-9:
        return str(int(round(v)))
    return f"{v:.2f}".rstrip("0").rstrip(".")


def resolve_template_key(
    *, status: str | None, result_reason: str | None
) -> str:
    st = (status or "").strip()
    reason = (result_reason or "").strip()
    if st == "completed":
        return "completed"
    if st == "partial":
        return "partial"
    if st in ("not_delivered", "cancelled"):
        if reason == "closed_store":
            return "closed_store"
        if reason == "not_found":
            return "not_found"
        if reason == "rejected_order":
            return "rejected_order"
        if reason == "absent_customer":
            return "absent_customer"
        return "other_incident"
    return "other_incident"


def whatsapp_share_url(text: str) -> str:
    """Enlace oficial wa.me con texto prellenado. Sin chat_id ni invite link."""
    return "https://wa.me/?text=" + quote(text)


def _collection_compact(cols: list[dict[str, Any]]) -> tuple[str, str]:
    """Forma de pago declarada (sin montos). Sin afirmar verificación falsa."""
    methods: list[str] = []
    seen: set[str] = set()
    has_amounts = False
    for c in cols or []:
        method = str(c.get("method") or "")
        if not method or method in seen:
            continue
        seen.add(method)
        methods.append(PAYMENT_METHOD_ES.get(method, method))
        if c.get("declared_amount") is not None:
            has_amounts = True
    if not methods:
        pay_line = "💵 Forma de pago: sin declarar"
    else:
        pay_line = "💵 " + " · ".join(methods)
    # Segunda línea: aviso operativo (no montos).
    if "transfer_declared" in seen or "cash_and_transfer" in seen:
        note_line = "ℹ️ Transf. informada (no verificada)"
    elif "pending" in seen:
        note_line = "ℹ️ Cobro pendiente de gestión"
    elif has_amounts:
        note_line = "ℹ️ Declaración con montos (legado)"
    else:
        note_line = "ℹ️ Solo forma de pago (sin montos)"
    return pay_line, note_line


def _line_counts(products: list[dict[str, Any]]) -> tuple[int, int, int]:
    full = partial = none = 0
    for p in products or []:
        try:
            planned = float(p.get("planned_units") or 0)
            delivered = float(p.get("delivered_units") or 0)
        except (TypeError, ValueError):
            planned = delivered = 0.0
        if "undelivered_units" in p and p.get("planned_units") is None:
            try:
                undel = float(p.get("undelivered_units") or 0)
                delivered = float(p.get("delivered_units") or 0)
                planned = delivered + undel
            except (TypeError, ValueError):
                undel = 0.0
        undel = max(0.0, planned - delivered)
        if undel <= 0 and delivered > 0:
            full += 1
        elif delivered <= 0:
            none += 1
        else:
            partial += 1
    return full, partial, none


def build_report_text(data: dict[str, Any], *, demo: bool = False) -> str:
    """Mensaje compacto (máx. 6 líneas). Sin GPS, RUT, fotos ni firma imagen."""
    template = resolve_template_key(
        status=data.get("status"),
        result_reason=data.get("result_reason"),
    )
    header = TEMPLATE_HEADERS[template]
    if demo:
        header = header.replace("QUILLOTANA", "QUILLOTANA · PRUEBA")
        if not header.startswith("🧪"):
            header = "🧪 " + header

    docs = data.get("documents") or []
    doc_bits: list[str] = []
    for d in docs[:2]:
        dtype = d.get("document_type") or "Doc"
        folio = d.get("document_number")
        folio_s = str(folio) if folio is not None else "—"
        doc_bits.append(f"{dtype} {folio_s}")
    docs_s = " · ".join(doc_bits) if doc_bits else "Sin documento"
    client = data.get("client_name") or "—"
    sem = resolve_semaphore(
        status=data.get("status"), result_reason=data.get("result_reason")
    )

    products = data.get("products") or []
    full_n, partial_n, none_n = _line_counts(products)
    pending_review = str(data.get("closure_review_status") or "") == "pending_review"
    if pending_review and template in (
        "closed_store",
        "not_found",
        "rejected_order",
        "absent_customer",
        "other_incident",
        "partial",
    ):
        header = "🟡 QUILLOTANA | PENDIENTE DE VALIDACIÓN"
    gps_line = (
        "📍 GPS validado (≤300 m)"
        if data.get("gps_within_radius") and not pending_review
        else (
            "📍 GPS: pendiente de validación"
            if pending_review
            else (
                f"📍 GPS: {'registrado' if data.get('gps_registered') else 'no'} "
                "(sin coords en mensaje)"
            )
        )
    )
    evid = data.get("evidence_status") or "pendiente"
    if template in (
        "closed_store",
        "not_found",
        "rejected_order",
        "absent_customer",
        "other_incident",
    ):
        reason = data.get("result_reason")
        reason_es = REASON_ES.get(reason, reason or sem.get("label_es") or "Incidencia")
        footer = (
            "✍️ Datos de prueba"
            if demo
            else (
                "✍️ PENDIENTE DE VALIDACIÓN"
                if pending_review
                else "✍️ Incidencia registrada"
            )
        )
        lines = [
            header,
            f"🏪 {client} · {docs_s}",
            f"📝 {reason_es}",
            f"📷 Evidencia: {evid}",
            gps_line,
            footer,
        ]
    else:
        pay_line, pend_line = _collection_compact(list(data.get("collections") or []))
        sig = data.get("signature_status") or "pendiente"
        lines = [
            header,
            f"🏪 {client} · {docs_s}",
            f"📦 {full_n} completas · {partial_n} parciales · {none_n} no entregadas",
            pay_line,
            pend_line,
            (
                f"✍️ Firma: {sig} · Datos ficticios"
                if demo
                else f"✍️ Firma: {sig}"
            ),
        ]

    # Garantizar máximo 6 líneas visibles.
    lines = [ln for ln in lines if ln is not None][:MAX_REPORT_LINES]
    return "\n".join(lines)


def build_report_bundle(data: dict[str, Any], *, demo: bool = False) -> dict[str, Any]:
    text = build_report_text(data, demo=demo)
    template = resolve_template_key(
        status=data.get("status"), result_reason=data.get("result_reason")
    )
    return {
        "report_text": text,
        "template_key": template,
        "header": TEMPLATE_HEADERS[template],
        "demo": demo,
        "share": {
            "whatsapp_url": whatsapp_share_url(text),
            "supports_native_share": True,
            "delivery_claimed": False,
            "auto_send": False,
            "states": {
                "report_generated": True,
                "share_requested": False,
                "delivery_verified": False,
            },
            "note_es": (
                "Reporte generado. Abrir compartir no implica envío confirmado. "
                "Elija el grupo y confirme en WhatsApp."
            ),
        },
        "omitted": [
            "signature_image",
            "photos",
            "precise_gps",
            "full_rut",
            "phone",
            "group_invite_link",
        ],
    }


def visit_row_to_report_data(
    visit: dict[str, Any],
    *,
    docs: list[dict[str, Any]],
    lines: list[dict[str, Any]],
    cols: list[dict[str, Any]],
    planning_code: str | None,
    client_name: str | None,
    driver_name: str | None,
    has_evidence: bool | None,
    has_signature: bool | None,
    signature_refused: bool | None = None,
) -> dict[str, Any]:
    pub_start = _local(visit.get("started_at"))
    pub_end = _local(visit.get("ended_at"))
    gps_ok = visit.get("end_lat") is not None or visit.get("start_lat") is not None

    status = visit.get("status")
    status_label = {
        "completed": "Entrega completa",
        "partial": "Entrega parcial",
        "not_delivered": "No entregada",
        "cancelled": "Cancelada",
    }.get(str(status), str(status))

    if has_signature is True:
        sig = "sí"
    elif signature_refused:
        sig = "rechazada / pendiente revisión"
    elif has_signature is False:
        sig = "no"
    else:
        sig = "pendiente"

    if has_evidence is True:
        evid = "sí"
    elif has_evidence is False:
        evid = "no"
    else:
        evid = "pendiente"

    products = []
    for ln in lines:
        products.append(
            {
                "product_name": ln.get("product_name"),
                "planned_units": ln.get("planned_units"),
                "delivered_units": ln.get("delivered_units"),
                "undelivered_units": ln.get("undelivered_units"),
                "undelivered_reason": ln.get("undelivered_reason"),
                "undelivered_note": ln.get("undelivered_note"),
            }
        )

    documents = []
    for d in docs:
        documents.append(
            {
                "document_type": d.get("document_type"),
                "document_number": d.get("document_number"),
                "document_total": d.get("document_total"),
            }
        )

    collections = []
    for c in cols:
        collections.append(
            {
                "method": c.get("method"),
                "document_amount": c.get("document_amount"),
                "declared_amount": c.get("declared_amount"),
                "verified_amount": c.get("verified_amount"),
                "pending_balance": c.get("pending_balance"),
            }
        )

    return {
        "status": status,
        "status_label": status_label,
        "result_reason": visit.get("result_reason"),
        "result_note": visit.get("result_note"),
        "planning_code": planning_code,
        "plan_id": visit.get("dispatch_plan_id"),
        "client_name": client_name or visit.get("customer_key"),
        "driver_name": driver_name,
        "visit_date": _fmt_date(pub_start),
        "started_at_local": _fmt_time(pub_start),
        "ended_at_local": _fmt_time(pub_end),
        "duration_label": _fmt_duration(pub_start, pub_end),
        "documents": documents,
        "products": products,
        "collections": collections,
        "gps_registered": bool(gps_ok),
        # Solo afirmamos radio OK si el cierre quedó verified (excepción → pending_review).
        "gps_within_radius": (
            bool(gps_ok)
            and str(visit.get("closure_review_status") or "") == "verified"
            and str(visit.get("result_reason") or "")
            in ("closed_store", "rejected_order")
        ),
        "closure_review_status": visit.get("closure_review_status"),
        "evidence_status": evid,
        "signature_status": sig,
        "observations": visit.get("result_note"),
    }


def demo_scenarios() -> list[dict[str, Any]]:
    """Seis escenarios ficticios. No tocan DB ni planes reales."""
    now = datetime.now(SANTIAGO)
    start = now.replace(hour=10, minute=15, second=0, microsecond=0)
    end = start + timedelta(minutes=18)

    base_docs = [
        {
            "document_type": "Boleta",
            "document_number": 45821,
            "document_total": 86500,
        }
    ]

    scenarios: list[tuple[str, dict[str, Any]]] = [
        (
            "complete_cash",
            {
                "status": "completed",
                "result_reason": None,
                "result_note": None,
                "planning_code": "PLAN-DEMO-01",
                "client_name": "Minimarket Demo Sur",
                "driver_name": "Chofer de prueba",
                "visit_date": _fmt_date(start),
                "started_at_local": _fmt_time(start),
                "ended_at_local": _fmt_time(end),
                "duration_label": _fmt_duration(start, end),
                "documents": base_docs,
                "products": [
                    {
                        "product_name": "Aceite 1L",
                        "delivered_units": 12,
                        "undelivered_units": 0,
                    },
                    {
                        "product_name": "Arroz 1kg",
                        "delivered_units": 24,
                        "undelivered_units": 0,
                    },
                ],
                "collections": [
                    {
                        "method": "cash",
                        "document_amount": 86500,
                        "declared_amount": 86500,
                        "verified_amount": None,
                        "pending_balance": 0,
                    }
                ],
                "gps_registered": True,
                "evidence_status": "no",
                "signature_status": "sí",
                "observations": None,
            },
        ),
        (
            "complete_transfer",
            {
                "status": "completed",
                "result_reason": None,
                "result_note": "Cliente informa transferencia al terminar",
                "planning_code": "PLAN-DEMO-02",
                "client_name": "Almacén Demo Norte",
                "driver_name": "Chofer de prueba",
                "visit_date": _fmt_date(start),
                "started_at_local": _fmt_time(start),
                "ended_at_local": _fmt_time(end + timedelta(minutes=5)),
                "duration_label": _fmt_duration(start, end + timedelta(minutes=5)),
                "documents": [
                    {
                        "document_type": "Factura",
                        "document_number": 12044,
                        "document_total": 152300,
                    }
                ],
                "products": [
                    {
                        "product_name": "Azúcar 1kg",
                        "delivered_units": 30,
                        "undelivered_units": 0,
                    }
                ],
                "collections": [
                    {
                        "method": "transfer_declared",
                        "document_amount": 152300,
                        "declared_amount": 152300,
                        "verified_amount": None,
                        "pending_balance": 0,
                    }
                ],
                "gps_registered": True,
                "evidence_status": "no",
                "signature_status": "sí",
                "observations": "Transferencia informada — pendiente verificación admin",
            },
        ),
        (
            "partial",
            {
                "status": "partial",
                "result_reason": None,
                "result_note": "Faltante en camión",
                "planning_code": "PLAN-DEMO-03",
                "client_name": "Botillería Demo",
                "driver_name": "Chofer de prueba",
                "visit_date": _fmt_date(start),
                "started_at_local": _fmt_time(start),
                "ended_at_local": _fmt_time(end + timedelta(minutes=12)),
                "duration_label": _fmt_duration(start, end + timedelta(minutes=12)),
                "documents": base_docs,
                "products": [
                    {
                        "product_name": "Bebida 1.5L",
                        "delivered_units": 8,
                        "undelivered_units": 4,
                        "undelivered_reason": "physical_shortage",
                    },
                    {
                        "product_name": "Snack surtido",
                        "delivered_units": 10,
                        "undelivered_units": 2,
                        "undelivered_reason": "damaged",
                        "undelivered_note": "Caja aplastada",
                    },
                ],
                "collections": [
                    {
                        "method": "cash",
                        "document_amount": 86500,
                        "declared_amount": 30000,
                        "verified_amount": None,
                        "pending_balance": None,
                    },
                    {
                        "method": "transfer_declared",
                        "document_amount": 86500,
                        "declared_amount": 50000,
                        "verified_amount": None,
                        "pending_balance": None,
                    },
                    {
                        "method": "pending",
                        "document_amount": 86500,
                        "declared_amount": 6500,
                        "verified_amount": None,
                        "pending_balance": 6500,
                    },
                ],
                "gps_registered": True,
                "evidence_status": "no",
                "signature_status": "sí",
                "observations": "Cliente firma solo lo recibido",
            },
        ),
        (
            "closed_store",
            {
                "status": "not_delivered",
                "result_reason": "closed_store",
                "result_note": "Persiana baja, sin personal",
                "planning_code": "PLAN-DEMO-04",
                "client_name": "Kiosco Demo Centro",
                "driver_name": "Chofer de prueba",
                "visit_date": _fmt_date(start),
                "started_at_local": _fmt_time(start),
                "ended_at_local": _fmt_time(start + timedelta(minutes=7)),
                "duration_label": _fmt_duration(start, start + timedelta(minutes=7)),
                "documents": base_docs,
                "products": [],
                "collections": [
                    {
                        "method": "not_applicable",
                        "document_amount": 86500,
                        "declared_amount": 0,
                        "verified_amount": None,
                        "pending_balance": 86500,
                    }
                ],
                "gps_registered": True,
                "gps_within_radius": True,
                "evidence_status": "sí",
                "signature_status": "no",
                "observations": "Fotografía de fachada cerrada requerida en flujo real",
            },
        ),
        (
            "not_found",
            {
                "status": "not_delivered",
                "result_reason": "not_found",
                "result_note": "Dirección no coincide con el local",
                "planning_code": "PLAN-DEMO-05",
                "client_name": "Cliente Demo Rural",
                "driver_name": "Chofer de prueba",
                "visit_date": _fmt_date(start),
                "started_at_local": _fmt_time(start),
                "ended_at_local": _fmt_time(start + timedelta(minutes=11)),
                "duration_label": _fmt_duration(start, start + timedelta(minutes=11)),
                "documents": base_docs,
                "products": [],
                "collections": [
                    {"method": "pending", "document_amount": 86500, "declared_amount": 0, "pending_balance": 86500}
                ],
                "gps_registered": True,
                "evidence_status": "sí",
                "signature_status": "no",
                "observations": "Se registró evidencia del entorno (simulado)",
            },
        ),
        (
            "rejected_order",
            {
                "status": "not_delivered",
                "result_reason": "rejected_order",
                "result_note": "Cliente no acepta el pedido del día",
                "planning_code": "PLAN-DEMO-06",
                "client_name": "Restobar Demo",
                "driver_name": "Chofer de prueba",
                "visit_date": _fmt_date(start),
                "started_at_local": _fmt_time(start),
                "ended_at_local": _fmt_time(start + timedelta(minutes=9)),
                "duration_label": _fmt_duration(start, start + timedelta(minutes=9)),
                "documents": [
                    {
                        "document_type": "Factura",
                        "document_number": 12099,
                        "document_total": 210000,
                    }
                ],
                "products": [
                    {
                        "product_name": "Caja cerveza",
                        "delivered_units": 0,
                        "undelivered_units": 10,
                        "undelivered_reason": "customer_rejection",
                    }
                ],
                "collections": [
                    {
                        "method": "not_applicable",
                        "document_amount": 210000,
                        "declared_amount": 0,
                        "pending_balance": 210000,
                    }
                ],
                "gps_registered": True,
                "gps_within_radius": True,
                "evidence_status": "sí",
                "signature_status": "rechazada / pendiente revisión",
                "observations": "Pedido completo rechazado",
            },
        ),
    ]

    out = []
    for key, payload in scenarios:
        bundle = build_report_bundle(payload, demo=True)
        out.append(
            {
                "scenario_key": key,
                "title_es": {
                    "complete_cash": "1. Entrega completa + efectivo",
                    "complete_transfer": "2. Completa + transferencia informada",
                    "partial": "3. Entrega parcial",
                    "closed_store": "4. Local cerrado",
                    "not_found": "5. Local no encontrado",
                    "rejected_order": "6. Pedido rechazado",
                }[key],
                **bundle,
                "writes_production": False,
            }
        )
    return out
