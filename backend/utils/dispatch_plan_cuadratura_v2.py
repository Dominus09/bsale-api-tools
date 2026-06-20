"""Cuadratura v2: documentos, NC, conteo efectivo y resultado operacional."""

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

CASH_DENOMINATIONS_CLP = (
    20_000,
    10_000,
    5_000,
    2_000,
    1_000,
    500,
    100,
    50,
    10,
)


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


def default_cash_count() -> list[dict[str, int]]:
    return [
        {"denominacion_clp": d, "cantidad": 0, "subtotal_clp": 0}
        for d in CASH_DENOMINATIONS_CLP
    ]


def normalize_cash_count(rows: list[dict[str, Any]] | None) -> list[dict[str, int]]:
    by_denom: dict[int, int] = {}
    for row in rows or []:
        try:
            denom = int(row.get("denominacion_clp") or 0)
            qty = max(0, int(row.get("cantidad") or 0))
        except (TypeError, ValueError):
            continue
        if denom > 0:
            by_denom[denom] = qty
    out: list[dict[str, int]] = []
    for denom in CASH_DENOMINATIONS_CLP:
        qty = by_denom.get(denom, 0)
        out.append(
            {
                "denominacion_clp": denom,
                "cantidad": qty,
                "subtotal_clp": denom * qty,
            }
        )
    return out


def compute_diff_status(diferencia_clp: int) -> CuadraturaDiffStatus:
    diff = int(diferencia_clp)
    if diff == 0:
        return "green"
    if abs(diff) < DIFF_YELLOW_MAX_CLP:
        return "yellow"
    return "red"


def observacion_required(resultado: dict[str, Any]) -> bool:
    gen = int(
        resultado.get("diferencia_general_clp")
        or resultado.get("diferencia_clp")
        or 0
    )
    cash = int(resultado.get("diferencia_efectivo_clp") or 0)
    return gen != 0 or cash != 0


def _sum_credit_notes(rows: list[dict[str, Any]]) -> int:
    total = 0
    for row in rows:
        try:
            total += int(round(float(row.get("monto") or 0)))
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
    cash_count: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    resumen = summarize_medios(documents)
    notas = _sum_credit_notes(credit_notes)
    venta_ajustada = int(venta_picking_clp) - notas
    total_recaudado_documental = sum(resumen.values())
    total_efectivo_documental = int(resumen.get("efectivo") or 0)
    cash_rows = normalize_cash_count(cash_count)
    total_efectivo_contado = sum(int(r["subtotal_clp"]) for r in cash_rows)
    diferencia_efectivo = total_efectivo_contado - total_efectivo_documental
    diferencia_general = venta_ajustada - total_recaudado_documental
    status = compute_diff_status(diferencia_general)
    return {
        "resumen_pagos": resumen,
        "notas_credito_clp": notas,
        "no_cargados_clp": 0,
        "venta_ajustada_clp": venta_ajustada,
        "total_recaudado_clp": total_recaudado_documental,
        "total_recaudado_documental_clp": total_recaudado_documental,
        "total_efectivo_documental_clp": total_efectivo_documental,
        "total_efectivo_contado_clp": total_efectivo_contado,
        "diferencia_efectivo_clp": diferencia_efectivo,
        "diferencia_clp": diferencia_general,
        "diferencia_general_clp": diferencia_general,
        "diferencia_status": status,
        "cash_count": cash_rows,
    }


def derive_operational_status(
    *,
    resultado: dict[str, Any],
    closed_at: Any,
    has_work: bool,
) -> CuadraturaOperationalStatus:
    gen = int(
        resultado.get("diferencia_general_clp")
        or resultado.get("diferencia_clp")
        or 0
    )
    cash = int(resultado.get("diferencia_efectivo_clp") or 0)

    if closed_at:
        if gen == 0 and cash == 0:
            return "squared"
        if abs(gen) < DIFF_YELLOW_MAX_CLP and abs(cash) < DIFF_YELLOW_MAX_CLP:
            return "in_review"
        return "difference"

    if not has_work:
        return "pending"

    if gen == 0 and cash == 0:
        return "draft"
    if abs(gen) < DIFF_YELLOW_MAX_CLP and abs(cash) < DIFF_YELLOW_MAX_CLP:
        return "in_review"
    return "difference"


def operational_status_label(status: str) -> str:
    return {
        "pending": "Pendiente",
        "draft": "Borrador",
        "in_review": "En revisión",
        "difference": "Con diferencia",
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
    return {
        "driver_name": plan.get("driver_name") or "",
        "truck_name": plan.get("truck_name") or plan.get("route_name") or "",
        "planning_code": plan.get("planning_code") or "",
        "diferencia_general_clp": int(
            resultado.get("diferencia_general_clp")
            or resultado.get("diferencia_clp")
            or 0
        ),
        "diferencia_efectivo_clp": int(resultado.get("diferencia_efectivo_clp") or 0),
        "document_count": len(documents),
        "credit_note_count": len(credit_notes),
        "not_loaded_count": len(not_loaded),
        "ready_for_dashboard": True,
        "future_dashboards": [
            "differences_by_driver",
            "differences_by_vehicle",
            "top_not_loaded_products",
            "top_credit_note_documents",
            "cuadratura_history",
            "cash_count_variance",
        ],
    }
