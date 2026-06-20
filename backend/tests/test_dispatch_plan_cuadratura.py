"""Tests cuadratura operacional."""

from backend.utils.dispatch_plan_cuadratura import (
    compute_cuadratura_result,
    observacion_required,
)


def test_cuadratura_diferencia_verde():
    r = compute_cuadratura_result(
        venta_picking_clp=1_000_000,
        credit_notes=[{"monto": 50_000}],
        not_loaded=[{"monto": 50_000}],
        transferencia_clp=900_000,
    )
    assert r["venta_ajustada_clp"] == 900_000
    assert r["diferencia_clp"] == 0
    assert r["diferencia_status"] == "green"
    assert observacion_required(int(r["diferencia_clp"])) is False


def test_cuadratura_diferencia_amarilla():
    r = compute_cuadratura_result(
        venta_picking_clp=100_000,
        credit_notes=[],
        not_loaded=[],
        efectivo_clp=97_000,
    )
    assert r["diferencia_clp"] == 3_000
    assert r["diferencia_status"] == "yellow"


def test_cuadratura_diferencia_roja():
    r = compute_cuadratura_result(
        venta_picking_clp=500_000,
        credit_notes=[],
        not_loaded=[],
        efectivo_clp=400_000,
    )
    assert r["diferencia_clp"] == 100_000
    assert r["diferencia_status"] == "red"
    assert observacion_required(int(r["diferencia_clp"])) is True
