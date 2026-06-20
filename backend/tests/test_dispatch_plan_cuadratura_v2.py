"""Tests cuadratura v2 documental."""

from backend.utils.dispatch_plan_cuadratura_v2 import (
    compute_cuadratura_v2_result,
    compute_diff_status,
    default_cash_count,
    derive_operational_status,
    normalize_cash_count,
    observacion_required,
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
    cash = normalize_cash_count([{"denominacion_clp": 10000, "cantidad": 48}])
    r = compute_cuadratura_v2_result(
        venta_picking_clp=500_000,
        documents=[{"medio_pago": "transferencia", "monto_clp": 480_000}],
        credit_notes=[{"monto": 20_000}],
        cash_count=cash,
    )
    assert r["venta_ajustada_clp"] == 480_000
    assert r["total_recaudado_documental_clp"] == 480_000
    assert r["diferencia_general_clp"] == 0
    assert r["diferencia_status"] == "green"


def test_cuadratura_v2_diferencia_efectivo():
    cash = normalize_cash_count(
        [
            {"denominacion_clp": 20000, "cantidad": 10},
            {"denominacion_clp": 10000, "cantidad": 5},
            {"denominacion_clp": 1000, "cantidad": 6},
        ]
    )
    r = compute_cuadratura_v2_result(
        venta_picking_clp=265_000,
        documents=[{"medio_pago": "efectivo", "monto_clp": 265_000}],
        credit_notes=[],
        cash_count=cash,
    )
    assert r["total_efectivo_documental_clp"] == 265_000
    assert r["total_efectivo_contado_clp"] == 256_000
    assert r["diferencia_efectivo_clp"] == -9_000
    assert observacion_required(r) is True


def test_not_loaded_no_descuenta():
    r = compute_cuadratura_v2_result(
        venta_picking_clp=100_000,
        documents=[{"medio_pago": "efectivo", "monto_clp": 100_000}],
        credit_notes=[],
        cash_count=default_cash_count(),
    )
    assert r["no_cargados_clp"] == 0
    assert r["venta_ajustada_clp"] == 100_000


def test_diff_status_yellow_and_red():
    assert compute_diff_status(3_000) == "yellow"
    assert compute_diff_status(-3_000) == "yellow"
    assert compute_diff_status(5_000) == "red"
    assert compute_diff_status(0) == "green"


def test_operational_status_squared():
    st = derive_operational_status(
        resultado={
            "diferencia_general_clp": 0,
            "diferencia_efectivo_clp": 0,
            "diferencia_status": "green",
        },
        closed_at="2026-01-01",
        has_work=True,
    )
    assert st == "squared"
