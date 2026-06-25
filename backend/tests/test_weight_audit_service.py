"""Tests clasificación estado peso en auditoría logística."""

from backend.services.logistics_weight_audit_service import classify_order_weight_estado


def test_completo():
    assert (
        classify_order_weight_estado(
            productos_totales=5,
            productos_con_peso=5,
            porcentaje_cobertura=100.0,
            peso_total_kg=12.5,
        )
        == "completo"
    )


def test_parcial():
    assert (
        classify_order_weight_estado(
            productos_totales=5,
            productos_con_peso=2,
            porcentaje_cobertura=40.0,
            peso_total_kg=3.0,
        )
        == "parcial"
    )


def test_sin_peso():
    assert (
        classify_order_weight_estado(
            productos_totales=3,
            productos_con_peso=0,
            porcentaje_cobertura=0.0,
            peso_total_kg=0.0,
        )
        == "sin_peso"
    )
