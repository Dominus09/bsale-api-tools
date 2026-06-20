"""Tests cuadratura v2 documental."""

from backend.utils.dispatch_plan_cuadratura_v2 import (
    compute_cuadratura_v2_result,
    compute_diff_status,
    derive_operational_status,
    summarize_medios,
)


def test_summarize_medios_from_documents():
    docs = [
        {"medio_pago": "transferencia", "monto_clp": 100_000},
        {"medio_pago": "efectivo", "monto_clp": 50_000},
        {"medio_pago": "transferencia", "monto_clp": 25_000},
    ]
    resumen = summarize_medios(docs)
    assert resumen["transferencia"] == 125_000
    assert resumen["efectivo"] == 50_000


def test_cuadratura_v2_verde():
    r = compute_cuadratura_v2_result(
        venta_picking_clp=500_000,
        documents=[{"medio_pago": "transferencia", "monto_clp": 480_000}],
        credit_notes=[{"monto": 20_000, "aplicada": True}],
        not_loaded=[],
    )
    assert r["venta_ajustada_clp"] == 480_000
    assert r["total_recaudado_clp"] == 480_000
    assert r["diferencia_clp"] == 0
    assert r["diferencia_status"] == "green"


def test_cuadratura_v2_nc_no_aplicada():
    r = compute_cuadratura_v2_result(
        venta_picking_clp=100_000,
        documents=[{"medio_pago": "efectivo", "monto_clp": 100_000}],
        credit_notes=[
            {"monto": 10_000, "aplicada": False},
            {"monto": 5_000, "aplicada": True},
        ],
        not_loaded=[],
    )
    assert r["notas_credito_clp"] == 5_000
    assert r["venta_ajustada_clp"] == 95_000
    assert r["diferencia_clp"] == -5_000


def test_diff_status_yellow_and_red():
    assert compute_diff_status(3_000) == "yellow"
    assert compute_diff_status(-3_000) == "yellow"
    assert compute_diff_status(5_000) == "red"
    assert compute_diff_status(0) == "green"


def test_operational_status_squared():
    st = derive_operational_status(
        resultado={"diferencia_clp": 0, "diferencia_status": "green"},
        closed_at="2026-01-01",
        has_work=True,
    )
    assert st == "squared"
