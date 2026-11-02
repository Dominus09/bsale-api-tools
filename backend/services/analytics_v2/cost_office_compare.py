"""Comparativo de costos por oficina (Costos V2 / bsale_raw).

No inventa promedio ni FIFO por oficina. No compara guía vs promedio Bsale
como anomalía. La compra a proveedor es solo ``factura``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import StrEnum
from typing import Any

from backend.services.analytics_v2.contracts import (
    INCREASE_ALERT_PCT,
    DECREASE_ALERT_PCT,
    STALE_COST_DAYS,
    MovementClass,
    money,
    iso,
)
from backend.services.analytics_v2.cost_signals import change_pct, to_decimal
from backend.services.analytics_v2.movement_classification import classify_movement

# Antigüedad «actualizado» (más estricto que STALE_COST_DAYS).
OFFICE_FRESH_DAYS = 45
# Spread mínimo entre facturas de oficinas distintas para ranking/alerta.
OFFICE_SPREAD_ALERT_PCT = Decimal("5")
# Máximo de líneas recientes por oficina usadas para armar el snapshot.
OFFICE_LINE_LOOKBACK = 40


class OfficeInfoTag(StrEnum):
    ACTUALIZADO = "actualizado"
    ANTIGUO = "antiguo"
    SOLO_GUIA = "solo_guia"
    SIN_INFORMACION = "sin_informacion"


class OfficeAlertCode(StrEnum):
    ALZA_COMPRA = "alza_compra_oficina"
    BAJA_COMPRA = "baja_compra_oficina"
    SIN_COSTO_VALIDO = "sin_costo_valido"
    DATOS_DESACTUALIZADOS = "datos_desactualizados"
    DATOS_INSUFICIENTES = "datos_insuficientes"
    SPREAD_ENTRE_OFICINAS = "spread_entre_oficinas"
    # Reservado: solo si hay vínculo confiable traspaso↔origen (MVP: no se emite).
    DIFERENCIA_TRASPASO_ORIGEN = "diferencia_traspaso_origen"


def movement_display(movement: MovementClass) -> str:
    if movement == MovementClass.FACTURA:
        return "Factura"
    if movement == MovementClass.GUIA_DESPACHO:
        return "Guía"
    if movement == MovementClass.TRANSFERENCIA_INTERNA:
        return "Transferencia"
    return "Otro"


def info_tag_for_cell(
    *,
    last_documented_date: date | None,
    last_factura_date: date | None,
    last_movement: MovementClass | None,
    today: date,
) -> OfficeInfoTag:
    """Etiqueta de frescura orientada a compra a proveedor.

    Si hay factura, la antigüedad se mide con la factura (no con una guía posterior).
    Transferencias internas no cuentan como recepción comercial documentada.
    """
    if last_documented_date is None and last_factura_date is None:
        return OfficeInfoTag.SIN_INFORMACION
    if last_factura_date is None and last_movement == MovementClass.GUIA_DESPACHO:
        return OfficeInfoTag.SOLO_GUIA
    # Compra válida: frescura de la última factura. Sin factura pero con guía: solo_guia arriba.
    anchor = last_factura_date or last_documented_date
    if anchor is None:
        return OfficeInfoTag.SIN_INFORMACION
    age = (today - anchor).days
    if age > STALE_COST_DAYS:
        return OfficeInfoTag.ANTIGUO
    if age <= OFFICE_FRESH_DAYS:
        return OfficeInfoTag.ACTUALIZADO
    return OfficeInfoTag.ANTIGUO


def _as_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not hasattr(value, "hour"):
        return value
    if hasattr(value, "date") and callable(value.date):
        try:
            return value.date()
        except Exception:
            return None
    return None


def _line_payload(row: dict[str, Any], offices: dict[int, str]) -> dict[str, Any]:
    office = row.get("office_id")
    oid = int(office) if office is not None else None
    movement = MovementClass(row["movement_class"])
    return {
        "reception_id": int(row["reception_id"]),
        "detail_id": int(row["detail_id"]),
        "admission_date": iso(row.get("admission_date")),
        "office_id": oid,
        "office_name": offices.get(oid) if oid is not None else None,
        "document": row.get("document"),
        "document_number": row.get("document_number"),
        "movement_class": movement.value,
        "movement_label": movement_display(movement),
        "cost": money(to_decimal(row.get("cost"))),
        "quantity": money(to_decimal(row.get("quantity"))),
        "comparable_supplier": movement == MovementClass.FACTURA,
        "details_complete": bool(row.get("details_complete")),
    }


def _is_documented(row: dict[str, Any]) -> bool:
    """Recepción comercial documentada (factura/guía). Excluye transferencias internas."""
    cost = to_decimal(row.get("cost"))
    qty = to_decimal(row.get("quantity"))
    movement = MovementClass(row["movement_class"])
    return (
        row.get("details_complete") is True
        and cost is not None
        and cost > 0
        and qty is not None
        and qty > 0
        and movement in (MovementClass.FACTURA, MovementClass.GUIA_DESPACHO)
    )


def _is_supplier_factura(row: dict[str, Any]) -> bool:
    cost = to_decimal(row.get("cost"))
    qty = to_decimal(row.get("quantity"))
    return (
        MovementClass(row["movement_class"]) == MovementClass.FACTURA
        and row.get("details_complete") is True
        and cost is not None
        and cost > 0
        and qty is not None
        and qty > 0
    )


def _is_guia(row: dict[str, Any]) -> bool:
    cost = to_decimal(row.get("cost"))
    return (
        MovementClass(row["movement_class"]) == MovementClass.GUIA_DESPACHO
        and cost is not None
        and cost > 0
    )


@dataclass
class OfficeCellBuild:
    office_id: int
    office_name: str
    stock_qty: Decimal | None
    lines_newest_first: list[dict[str, Any]]


def build_office_cell(cell: OfficeCellBuild, *, today: date, offices: dict[int, str]) -> dict[str, Any]:
    documented = [r for r in cell.lines_newest_first if _is_documented(r)]
    facturas = [r for r in cell.lines_newest_first if _is_supplier_factura(r)]
    guias = [r for r in cell.lines_newest_first if _is_guia(r)]

    last_doc = documented[0] if documented else None
    last_fact = facturas[0] if facturas else None
    prev_fact = facturas[1] if len(facturas) > 1 else None
    last_guia = guias[0] if guias else None

    last_mov = MovementClass(last_doc["movement_class"]) if last_doc else None
    last_doc_date = _as_date(last_doc.get("admission_date")) if last_doc else None
    last_fact_date = _as_date(last_fact.get("admission_date")) if last_fact else None
    tag = info_tag_for_cell(
        last_documented_date=last_doc_date,
        last_factura_date=last_fact_date,
        last_movement=last_mov,
        today=today,
    )

    fact_cost = to_decimal(last_fact.get("cost")) if last_fact else None
    prev_cost = to_decimal(prev_fact.get("cost")) if prev_fact else None
    supplier_change_pct = change_pct(fact_cost, prev_cost)

    alerts: list[dict[str, Any]] = []
    if last_doc is None and (cell.stock_qty is None or cell.stock_qty <= 0):
        alerts.append(
            {
                "code": OfficeAlertCode.DATOS_INSUFICIENTES.value,
                "severity": "baja",
                "message": "Sin recepción documentada ni stock en la oficina",
            }
        )
    elif last_doc is None:
        alerts.append(
            {
                "code": OfficeAlertCode.SIN_COSTO_VALIDO.value,
                "severity": "media",
                "message": "Hay stock u oficina activa sin recepción documentada con costo",
            }
        )
    if tag == OfficeInfoTag.ANTIGUO:
        anchor_label = (
            "última factura a proveedor"
            if last_fact_date is not None
            else "última recepción documentada"
        )
        alerts.append(
            {
                "code": OfficeAlertCode.DATOS_DESACTUALIZADOS.value,
                "severity": "media",
                "message": f"{anchor_label.capitalize()} hace más de {OFFICE_FRESH_DAYS} días",
            }
        )
    if supplier_change_pct is not None and supplier_change_pct >= INCREASE_ALERT_PCT:
        alerts.append(
            {
                "code": OfficeAlertCode.ALZA_COMPRA.value,
                "severity": "alta" if supplier_change_pct >= 100 else "media",
                "message": f"Alza de {supplier_change_pct}% entre facturas de la oficina",
            }
        )
    elif supplier_change_pct is not None and supplier_change_pct <= DECREASE_ALERT_PCT:
        alerts.append(
            {
                "code": OfficeAlertCode.BAJA_COMPRA.value,
                "severity": "alta" if supplier_change_pct <= -60 else "media",
                "message": f"Baja de {supplier_change_pct}% entre facturas de la oficina",
            }
        )

    age_days = (today - last_doc_date).days if last_doc_date else None

    return {
        "office_id": cell.office_id,
        "office_name": cell.office_name,
        "stock": {
            "quantity": money(cell.stock_qty),
            "state": (
                "con_stock"
                if cell.stock_qty is not None and cell.stock_qty > 0
                else "cero"
                if cell.stock_qty is not None
                else "sin_sincronizar"
            ),
        },
        # Último costo unitario de recepción documentado (NO es promedio de oficina).
        "last_reception_unit_cost": _line_payload(last_doc, offices) if last_doc else None,
        "last_supplier_purchase": _line_payload(last_fact, offices) if last_fact else None,
        "previous_supplier_purchase": _line_payload(prev_fact, offices) if prev_fact else None,
        "last_guide": _line_payload(last_guia, offices) if last_guia else None,
        "supplier_cost_change_pct": money(supplier_change_pct),
        "info_tag": tag.value,
        "age_days": age_days,
        "alerts": alerts,
    }


def office_supplier_spread(cells: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Spread entre últimas facturas de oficina (misma base: cost unitario RAW)."""
    pairs: list[tuple[int, Decimal]] = []
    for c in cells:
        purchase = c.get("last_supplier_purchase")
        if not purchase or purchase.get("cost") is None:
            continue
        cost = to_decimal(purchase["cost"])
        if cost is None or cost <= 0:
            continue
        pairs.append((int(c["office_id"]), cost))
    if len(pairs) < 2:
        return None
    lo = min(pairs, key=lambda x: x[1])
    hi = max(pairs, key=lambda x: x[1])
    if lo[1] <= 0:
        return None
    pct = ((hi[1] - lo[1]) / lo[1] * Decimal("100")).quantize(Decimal("0.01"))
    return {
        "min_office_id": lo[0],
        "max_office_id": hi[0],
        "min_cost": money(lo[1]),
        "max_cost": money(hi[1]),
        "spread_pct": money(pct),
        "alert": pct >= OFFICE_SPREAD_ALERT_PCT,
    }


def enrich_timeline_line(row: dict[str, Any], offices: dict[int, str]) -> dict[str, Any]:
    """Línea de timeline con etiquetas Factura/Guía/Otro."""
    movement = classify_movement(row.get("document"), row.get("internal_dispatch_id"))
    # Preferir movement_class ya calculado en SQL si viene.
    if row.get("movement_class"):
        try:
            movement_class = MovementClass(row["movement_class"])
        except ValueError:
            movement_class = movement.movement_class
    else:
        movement_class = movement.movement_class
    payload = dict(row)
    payload["movement_class"] = movement_class.value
    base = _line_payload(payload, offices)
    base["internal_dispatch_id"] = row.get("internal_dispatch_id")
    base["comparable"] = bool(row.get("comparable"))
    base["anomalies"] = row.get("anomalies") or []
    base["change_pct"] = row.get("change_pct")
    base["previous_comparable_cost"] = row.get("previous_comparable_cost")
    return base
