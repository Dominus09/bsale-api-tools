"""Cálculos de cuadratura operacional (sin I/O)."""

from __future__ import annotations

from typing import Any, Literal

CuadraturaDiffStatus = Literal["green", "yellow", "red"]

DIFF_YELLOW_MAX_CLP = 5000


def _sum_rows(rows: list[dict[str, Any]], key: str = "monto") -> int:
    total = 0
    for row in rows:
        try:
            total += int(round(float(row.get(key) or 0)))
        except (TypeError, ValueError):
            continue
    return total


def compute_cuadratura_result(
    *,
    venta_picking_clp: int,
    credit_notes: list[dict[str, Any]],
    not_loaded: list[dict[str, Any]],
    transferencia_clp: int = 0,
    efectivo_clp: int = 0,
    cheque_clp: int = 0,
    debito_clp: int = 0,
) -> dict[str, int | CuadraturaDiffStatus]:
    notas = _sum_rows(credit_notes)
    no_cargados = _sum_rows(not_loaded)
    venta_ajustada = int(venta_picking_clp) - notas - no_cargados
    total_recaudado = (
        int(transferencia_clp)
        + int(efectivo_clp)
        + int(cheque_clp)
        + int(debito_clp)
    )
    diferencia = venta_ajustada - total_recaudado
    ad = abs(diferencia)
    if ad == 0:
        status: CuadraturaDiffStatus = "green"
    elif ad < DIFF_YELLOW_MAX_CLP:
        status = "yellow"
    else:
        status = "red"
    return {
        "notas_credito_clp": notas,
        "no_cargados_clp": no_cargados,
        "venta_ajustada_clp": venta_ajustada,
        "total_recaudado_clp": total_recaudado,
        "diferencia_clp": diferencia,
        "diferencia_status": status,
    }


def observacion_required(diferencia_clp: int) -> bool:
    return int(diferencia_clp) != 0
