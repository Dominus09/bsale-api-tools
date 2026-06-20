"""Cuadratura v2: documentos, resumen por medio y no cargados por producto."""

from __future__ import annotations

from typing import Any, Literal

CuadraturaDiffStatus = Literal["green", "yellow", "red"]
CuadraturaOperationalStatus = Literal[
    "pending", "draft", "in_review", "difference", "squared"
]

DIFF_YELLOW_MAX_CLP = 5000

MEDIOS_PAGO = (
    "transferencia",
    "efectivo",
    "cheque",
    "caja_vecina",
    "debito",
    "credito",
    "pendiente",
)

MEDIO_PAGO_LABELS: dict[str, str] = {
    "transferencia": "Transferencia",
    "efectivo": "Efectivo",
    "cheque": "Cheque",
    "caja_vecina": "Caja Vecina",
    "debito": "Débito",
    "credito": "Crédito",
    "pendiente": "Pendiente",
}


def normalize_medio_pago(raw: str | None) -> str:
    key = (raw or "").strip().lower().replace(" ", "_")
    if key in MEDIOS_PAGO:
        return key
    text = (raw or "").lower()
    if "transfer" in text:
        return "transferencia"
    if "efectivo" in text or "cash" in text:
        return "efectivo"
    if "cheque" in text:
        return "cheque"
    if "caja" in text and "vecina" in text:
        return "caja_vecina"
    if "debit" in text or "débito" in text:
        return "debito"
    if "credit" in text or "crédito" in text:
        return "credito"
    return "pendiente"


def guess_medio_from_payment_method(payment_method: str | None) -> str:
    return normalize_medio_pago(payment_method)


def compute_diff_status(diferencia_clp: int) -> CuadraturaDiffStatus:
    diff = int(diferencia_clp)
    if diff == 0:
        return "green"
    ad = abs(diff)
    if ad < DIFF_YELLOW_MAX_CLP:
        return "yellow"
    return "red"


def observacion_required(diferencia_clp: int) -> bool:
    return int(diferencia_clp) != 0


def _sum_applied_credit_notes(rows: list[dict[str, Any]]) -> int:
    total = 0
    for row in rows:
        if row.get("aplicada") is False:
            continue
        try:
            total += int(round(float(row.get("monto") or 0)))
        except (TypeError, ValueError):
            continue
    return total


def _sum_not_loaded(rows: list[dict[str, Any]]) -> int:
    total = 0
    for row in rows:
        try:
            total += int(round(float(row.get("monto_clp") or row.get("monto") or 0)))
        except (TypeError, ValueError):
            continue
    return total


def summarize_medios(documents: list[dict[str, Any]]) -> dict[str, int]:
    out = {k: 0 for k in MEDIOS_PAGO}
    for doc in documents:
        medio = normalize_medio_pago(doc.get("medio_pago"))
        try:
            monto = int(round(float(doc.get("monto_clp") or 0)))
        except (TypeError, ValueError):
            monto = 0
        out[medio] = out.get(medio, 0) + monto
    return out


def compute_cuadratura_v2_result(
    *,
    venta_picking_clp: int,
    documents: list[dict[str, Any]],
    credit_notes: list[dict[str, Any]],
    not_loaded: list[dict[str, Any]],
) -> dict[str, Any]:
    resumen = summarize_medios(documents)
    notas = _sum_applied_credit_notes(credit_notes)
    no_cargados = _sum_not_loaded(not_loaded)
    venta_ajustada = int(venta_picking_clp) - notas - no_cargados
    total_recaudado = sum(resumen.values())
    diferencia = venta_ajustada - total_recaudado
    status = compute_diff_status(diferencia)
    return {
        "resumen_pagos": resumen,
        "notas_credito_clp": notas,
        "no_cargados_clp": no_cargados,
        "venta_ajustada_clp": venta_ajustada,
        "total_recaudado_clp": total_recaudado,
        "diferencia_clp": diferencia,
        "diferencia_status": status,
    }


def derive_operational_status(
    *,
    resultado: dict[str, Any],
    closed_at: Any,
    has_work: bool,
) -> CuadraturaOperationalStatus:
    if closed_at:
        if int(resultado.get("diferencia_clp") or 0) == 0:
            return "squared"
        return "difference"
    if not has_work:
        return "pending"
    st = resultado.get("diferencia_status")
    if st == "green":
        return "draft"
    if st == "yellow":
        return "in_review"
    return "difference"


def operational_status_label(status: str) -> str:
    return {
        "pending": "Pendiente",
        "draft": "Borrador",
        "in_review": "En revisión",
        "difference": "Diferencia",
        "squared": "Cuadrado",
    }.get(status, status)


def build_analytics_meta(
    *,
    plan: dict[str, Any],
    resultado: dict[str, Any],
    documents: list[dict[str, Any]],
    credit_notes: list[dict[str, Any]],
    not_loaded: list[dict[str, Any]],
) -> dict[str, Any]:
    """Estructura preparada para dashboard futuro (sin UI)."""
    return {
        "driver_name": plan.get("driver_name") or "",
        "truck_name": plan.get("truck_name") or plan.get("route_name") or "",
        "planning_code": plan.get("planning_code") or "",
        "diferencia_clp": int(resultado.get("diferencia_clp") or 0),
        "document_count": len(documents),
        "credit_note_count": len(credit_notes),
        "not_loaded_count": len(not_loaded),
        "ready_for_dashboard": True,
        "future_dashboards": [
            "differences_by_driver",
            "differences_by_vehicle",
            "top_not_loaded_products",
            "top_credit_note_clients",
            "cuadratura_history",
            "effective_sales_recovery",
        ],
    }
