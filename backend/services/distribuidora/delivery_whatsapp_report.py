"""Formatos de reporte WhatsApp P4 (solo texto). Sin envío automático.

No incluye RUT completo, teléfono personal, coordenadas precisas, fotos ni firmas.
No inserta enlaces de invitación a grupos.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

SANTIAGO = ZoneInfo("America/Santiago")

PAYMENT_METHOD_ES = {
    "cash": "Efectivo recibido (declarado)",
    "transfer_declared": "Transferencia informada (pendiente verificación)",
    "transfer_verified": "Transferencia verificada",
    "credit": "Crédito autorizado",
    "partial": "Cobranza parcial",
    "pending": "Pago pendiente",
    "not_applicable": "No corresponde cobranza",
}

REASON_ES = {
    "closed_store": "Local cerrado",
    "not_found": "Local no encontrado",
    "rejected_order": "Pedido rechazado",
    "absent_customer": "Cliente ausente",
    "other": "Otra incidencia",
    "physical_shortage": "Faltante físico",
    "damaged": "Producto dañado",
    "customer_rejection": "Rechazo del cliente",
    "prep_error": "Error de preparación",
}

TEMPLATE_HEADERS = {
    "completed": "✅ ENTREGA COMPLETADA",
    "partial": "⚠️ ENTREGA PARCIAL",
    "closed_store": "🔒 LOCAL CERRADO",
    "not_found": "📍 LOCAL NO ENCONTRADO",
    "rejected_order": "❌ PEDIDO RECHAZADO",
    "other_incident": "⚠️ INCIDENCIA DE ENTREGA",
}


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
        return "other_incident"
    return "other_incident"


def whatsapp_share_url(text: str) -> str:
    """Enlace oficial wa.me con texto prellenado. Sin chat_id ni invite link."""
    return "https://wa.me/?text=" + quote(text)


def build_report_text(data: dict[str, Any], *, demo: bool = False) -> str:
    """Arma el mensaje a partir de datos ya confirmados (o ficticios marcados)."""
    template = resolve_template_key(
        status=data.get("status"),
        result_reason=data.get("result_reason"),
    )
    header = TEMPLATE_HEADERS[template]
    lines: list[str] = []
    if demo:
        lines.extend(
            [
                "🧪 PRUEBA — QUILLOTANA APP",
                "DATOS FICTICIOS — NO CORRESPONDE A UNA ENTREGA REAL",
                "",
            ]
        )
    lines.append(header)
    lines.append("Grupo La Quillotana")
    lines.append(f"Planificación: {data.get('planning_code') or data.get('plan_id') or '—'}")
    lines.append(f"Cliente: {data.get('client_name') or '—'}")
    lines.append(f"Chofer: {data.get('driver_name') or '—'}")
    lines.append(f"Fecha: {data.get('visit_date') or '—'}")
    lines.append(f"Inicio: {data.get('started_at_local') or '—'}")
    lines.append(f"Término: {data.get('ended_at_local') or '—'}")
    lines.append(f"Duración: {data.get('duration_label') or '—'}")

    docs = data.get("documents") or []
    if docs:
        lines.append("")
        lines.append("Documentos:")
        for d in docs:
            dtype = d.get("document_type") or "Documento"
            folio = d.get("document_number")
            folio_s = str(folio) if folio is not None else "—"
            lines.append(
                f"• {dtype} {folio_s} · {_fmt_money(d.get('document_total'))}"
            )
    else:
        lines.append("")
        lines.append("Documentos: sin detalle registrado")

    lines.append("")
    lines.append(f"Estado: {data.get('status_label') or header}")

    products = data.get("products") or []
    if template == "partial" or products:
        lines.append("")
        lines.append("Productos:")
        if not products:
            lines.append("• (sin líneas registradas)")
        for p in products:
            name = p.get("product_name") or "Producto"
            deliv = _fmt_qty(p.get("delivered_units"))
            pend = _fmt_qty(p.get("undelivered_units"))
            line = f"• {name}: entregado {deliv} · pendiente {pend}"
            reason = p.get("undelivered_reason")
            if reason and float(p.get("undelivered_units") or 0) > 0:
                line += f" ({REASON_ES.get(reason, reason)})"
                if p.get("undelivered_note"):
                    line += f" — {p['undelivered_note']}"
            lines.append(line)

    if template in ("closed_store", "not_found", "rejected_order", "other_incident"):
        lines.append("")
        reason = data.get("result_reason")
        lines.append(f"Motivo: {REASON_ES.get(reason, reason or '—')}")
        if data.get("result_note"):
            lines.append(f"Observación: {data['result_note']}")
        # Sin coordenadas precisas: solo disponibilidad
        lines.append(
            f"GPS registrado: {'sí' if data.get('gps_registered') else 'no'}"
        )
        lines.append(
            f"Evidencia fotográfica: {data.get('evidence_status') or 'pendiente'}"
        )

    cols = data.get("collections") or []
    lines.append("")
    lines.append("Cobranza declarada (no es pago confirmado contable):")
    if not cols:
        lines.append("• Sin declaración registrada")
        lines.append("• Pago verificado administrativamente: no")
    else:
        for c in cols:
            method = c.get("method")
            method_es = PAYMENT_METHOD_ES.get(method, method or "—")
            lines.append(f"• Medio declarado: {method_es}")
            lines.append(
                f"  Monto documento: {_fmt_money(c.get('document_amount'))}"
            )
            lines.append(
                f"  Monto declarado por chofer: {_fmt_money(c.get('declared_amount'))}"
            )
            lines.append(
                f"  Saldo pendiente declarado: {_fmt_money(c.get('pending_balance'))}"
            )
            verified = c.get("verified_amount")
            if verified is not None and float(verified) > 0:
                lines.append(
                    f"  Pago verificado administrativamente: sí ({_fmt_money(verified)})"
                )
            elif method == "transfer_verified":
                lines.append("  Pago verificado administrativamente: sí")
            else:
                lines.append("  Pago verificado administrativamente: no")

    lines.append("")
    lines.append(f"Firma de conformidad: {data.get('signature_status') or 'pendiente'}")
    lines.append(
        f"Evidencia fotográfica registrada: {data.get('evidence_status') or 'no'}"
    )
    if data.get("observations"):
        lines.append(f"Observaciones: {data['observations']}")

    lines.append("")
    lines.append(
        "Seleccione el grupo en WhatsApp y confirme el envío manualmente."
    )
    lines.append(
        "Este mensaje no acredita entrega automática ni pago verificado."
    )
    if demo:
        lines.append("SIMULACIÓN — no es operación productiva.")
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
                        "method": "partial",
                        "document_amount": 86500,
                        "declared_amount": 52000,
                        "verified_amount": None,
                        "pending_balance": 34500,
                    }
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
                "evidence_status": "no",
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
