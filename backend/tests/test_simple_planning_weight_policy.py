"""Política de pesos: cobertura completa/parcial/0%, overload, ack."""

from __future__ import annotations

from backend.services.distribuidora.simple_planning_assignment_service import (
    validate_truck_capacity,
)
from backend.services.distribuidora.simple_planning_preview_service import (
    classify_weight_quality,
)


def test_classify_complete_partial_unevaluable():
    assert (
        classify_weight_quality(
            {"peso_total_kg": 10, "coverage_percent": 100, "missing_products": 0}
        )
        == "known"
    )
    assert (
        classify_weight_quality(
            {
                "peso_total_kg": 8,
                "coverage_percent": 80,
                "missing_products": 1,
                "productos_totales": 5,
            }
        )
        == "partial"
    )
    assert (
        classify_weight_quality(
            {"peso_total_kg": None, "coverage_percent": 0, "missing_products": 3}
        )
        == "unevaluable"
    )
    assert (
        classify_weight_quality(
            {"peso_total_kg": 0, "coverage_percent": 0, "missing_products": 2}
        )
        == "unevaluable"
    )


def test_capacity_100_partial_zero_and_overload():
    full = validate_truck_capacity(
        known_weight_kg=100,
        estimated_weight_kg=0,
        incomplete_orders=0,
        unevaluable_orders=0,
        truck_max_weight_kg=500,
    )
    assert full["weight_total_determined"] is True
    assert full["blocking"] is False
    assert full["utilization_pct"] == 20.0

    partial = validate_truck_capacity(
        known_weight_kg=80,
        estimated_weight_kg=0,
        incomplete_orders=2,
        unevaluable_orders=0,
        truck_max_weight_kg=500,
    )
    assert partial["requires_incomplete_weight_ack"] is True
    assert partial["blocking"] is False
    assert "incomplete_weight_coverage" in partial["warnings"]
    assert partial["known_weight_kg"] == 80

    zero = validate_truck_capacity(
        known_weight_kg=0,
        estimated_weight_kg=0,
        incomplete_orders=0,
        unevaluable_orders=3,
        truck_max_weight_kg=500,
    )
    assert zero["requires_unevaluable_weight_ack"] is True
    assert zero["blocking"] is False
    assert zero["utilization_pct"] == 0.0

    over = validate_truck_capacity(
        known_weight_kg=600,
        estimated_weight_kg=0,
        incomplete_orders=1,
        unevaluable_orders=0,
        truck_max_weight_kg=500,
    )
    assert over["overloaded"] is True
    assert over["blocking"] is True


def test_unknown_truck_capacity_separate_from_zero_weight():
    unknown = validate_truck_capacity(
        known_weight_kg=50,
        estimated_weight_kg=0,
        incomplete_orders=0,
        unevaluable_orders=0,
        truck_max_weight_kg=None,
    )
    assert unknown["capacity_known"] is False
    assert "truck_capacity_unknown" in unknown["warnings"]
    assert unknown["overloaded"] is False
    assert unknown["utilization_pct"] is None
