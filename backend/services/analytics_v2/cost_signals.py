"""Anomalías por línea de recepción y confiabilidad del costo por SKU (funciones puras).

Nada aquí modifica costos: solo clasifica y explica. Toda alerta conserva la identidad de la
recepción original (reception_id + detail_id) para ubicarla en Bsale.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from backend.services.analytics_v2.contracts import (
    AVERAGE_DIVERGENCE_PCT,
    COMPARABLE_MOVEMENTS,
    DECREASE_ALERT_PCT,
    DECREASE_SEVERE_PCT,
    EXTREME_QUANTITY_FACTOR,
    INCREASE_ALERT_PCT,
    INCREASE_SEVERE_PCT,
    STALE_COST_DAYS,
    AnomalyCode,
    CostReliability,
    MovementClass,
    Severity,
)

_REVIEW_CODES = frozenset(
    {
        AnomalyCode.ALZA_ANORMAL,
        AnomalyCode.BAJA_SOSPECHOSA,
        AnomalyCode.RECEPCION_MODIFICADA,
        AnomalyCode.CAMBIO_VARIANTE,
        AnomalyCode.CANTIDAD_EXTREMA,
        AnomalyCode.DETALLE_INCOMPLETO,
        AnomalyCode.DIFERENCIA_PROMEDIO_BSALE,
    }
)


@dataclass(frozen=True, slots=True)
class Anomaly:
    code: AnomalyCode
    severity: Severity
    message: str

    def to_json(self) -> dict[str, str]:
        return {"code": self.code.value, "severity": self.severity.value, "message": self.message}


@dataclass(frozen=True, slots=True)
class LineFacts:
    movement_class: MovementClass
    cost: Decimal | None
    quantity: Decimal | None
    details_complete: bool | None
    previous_comparable_cost: Decimal | None = None
    median_comparable_quantity: Decimal | None = None
    cost_modified: bool = False
    quantity_modified: bool = False
    variant_modified: bool = False


def to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def change_pct(current: Decimal | None, previous: Decimal | None) -> Decimal | None:
    if current is None or previous is None or previous <= 0:
        return None
    pct = (current - previous) / previous * Decimal("100")
    return pct.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def is_comparable_line(
    movement_class: MovementClass,
    cost: Decimal | None,
    quantity: Decimal | None,
    details_complete: bool | None,
) -> bool:
    return (
        movement_class in COMPARABLE_MOVEMENTS
        and cost is not None
        and cost > 0
        and quantity is not None
        and quantity > 0
        and details_complete is True
    )


def line_anomalies(line: LineFacts) -> list[Anomaly]:
    out: list[Anomaly] = []
    purchase_like = line.movement_class in COMPARABLE_MOVEMENTS
    if line.cost is None:
        out.append(
            Anomaly(
                AnomalyCode.COSTO_NULO,
                Severity.ALTA if purchase_like else Severity.MEDIA,
                "La línea no informa costo",
            )
        )
    elif line.cost == 0 and line.movement_class != MovementClass.TRANSFERENCIA_INTERNA:
        out.append(
            Anomaly(
                AnomalyCode.COSTO_CERO,
                Severity.ALTA if purchase_like else Severity.MEDIA,
                "Costo unitario cero",
            )
        )
    if line.quantity is None or line.quantity <= 0:
        out.append(Anomaly(AnomalyCode.CANTIDAD_NO_POSITIVA, Severity.MEDIA, "Cantidad nula o no positiva"))
    elif (
        line.median_comparable_quantity is not None
        and line.median_comparable_quantity > 0
        and line.quantity > line.median_comparable_quantity * EXTREME_QUANTITY_FACTOR
    ):
        out.append(
            Anomaly(
                AnomalyCode.CANTIDAD_EXTREMA,
                Severity.MEDIA,
                f"Cantidad {line.quantity} supera {EXTREME_QUANTITY_FACTOR}× la mediana habitual "
                f"({line.median_comparable_quantity})",
            )
        )
    if purchase_like and line.cost is not None and line.cost > 0:
        pct = change_pct(line.cost, line.previous_comparable_cost)
        if pct is not None and pct >= INCREASE_ALERT_PCT:
            out.append(
                Anomaly(
                    AnomalyCode.ALZA_ANORMAL,
                    Severity.ALTA if pct >= INCREASE_SEVERE_PCT else Severity.MEDIA,
                    f"Alza de {pct}% respecto del costo comparable anterior",
                )
            )
        elif pct is not None and pct <= DECREASE_ALERT_PCT:
            out.append(
                Anomaly(
                    AnomalyCode.BAJA_SOSPECHOSA,
                    Severity.ALTA if pct <= DECREASE_SEVERE_PCT else Severity.MEDIA,
                    f"Baja de {pct}% respecto del costo comparable anterior",
                )
            )
    if line.cost_modified or line.quantity_modified:
        changed = [
            label
            for flag, label in ((line.cost_modified, "costo"), (line.quantity_modified, "cantidad"))
            if flag
        ]
        out.append(
            Anomaly(
                AnomalyCode.RECEPCION_MODIFICADA,
                Severity.ALTA if line.cost_modified else Severity.MEDIA,
                f"La recepción fue modificada en Bsale ({', '.join(changed)})",
            )
        )
    if line.variant_modified:
        out.append(
            Anomaly(AnomalyCode.CAMBIO_VARIANTE, Severity.ALTA, "La línea cambió de variante en Bsale")
        )
    if line.details_complete is not True:
        out.append(
            Anomaly(
                AnomalyCode.DETALLE_INCOMPLETO,
                Severity.MEDIA,
                "El detalle de la recepción no está completo en RAW",
            )
        )
    if line.movement_class == MovementClass.NO_DETERMINADO:
        out.append(
            Anomaly(
                AnomalyCode.MOVIMIENTO_NO_DETERMINADO,
                Severity.BAJA,
                "Tipo de movimiento no determinado: requiere revisión administrativa",
            )
        )
    return out


def average_divergence(
    last_comparable_cost: Decimal | None, average_cost: Decimal | None
) -> Anomaly | None:
    pct = change_pct(last_comparable_cost, average_cost)
    if pct is None or abs(pct) <= AVERAGE_DIVERGENCE_PCT:
        return None
    return Anomaly(
        AnomalyCode.DIFERENCIA_PROMEDIO_BSALE,
        Severity.MEDIA,
        f"Último costo comparable difiere {pct}% del costo promedio Bsale",
    )


def sku_reliability(
    *,
    average_cost: Decimal | None,
    last_comparable_cost: Decimal | None,
    last_comparable_date: date | None,
    last_comparable_anomalies: list[Anomaly],
    today: date,
) -> tuple[CostReliability, str]:
    if last_comparable_cost is None or last_comparable_cost <= 0:
        if average_cost is not None and average_cost > 0:
            return (
                CostReliability.SOLO_PROMEDIO_BSALE,
                "Sin compra comparable desde 2025; solo existe el costo promedio Bsale",
            )
        return CostReliability.SIN_COSTO, "Sin costo promedio Bsale ni compra comparable"
    review = [a for a in last_comparable_anomalies if a.code in _REVIEW_CODES]
    if review:
        return CostReliability.REQUIERE_REVISION, "; ".join(a.message for a in review)
    if last_comparable_date is not None and (today - last_comparable_date).days > STALE_COST_DAYS:
        return (
            CostReliability.DESACTUALIZADO,
            f"Última compra comparable hace más de {STALE_COST_DAYS} días",
        )
    return CostReliability.CONSISTENTE, "Compra comparable reciente sin anomalías"
