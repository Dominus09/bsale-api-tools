"""Regresión: conflicto logístico no tumba el detalle ni muestra 0 kg falso."""

from __future__ import annotations

from backend.services.order_weight_service import build_weight_payload
from backend.utils.order_weight_calc import aggregate_order_summary


def test_build_weight_payload_zero_only_when_calculated():
    lines = [
        {
            "cantidad_unitaria": 1,
            "peso_unitario_kg": 0.0,
            "peso_linea_kg": 0.0,
            "estado_linea": "completo",
            "fuente_peso": "erp",
        }
    ]
    # línea con peso 0 explícito y cobertura completa → calculated 0
    summary = {
        "productos_totales": 1,
        "productos_sin_peso": 0,
        "peso_total_kg": 0.0,
    }
    payload = build_weight_payload(summary, lines=lines)
    assert payload["status"] == "calculated"
    assert payload["value_kg"] == 0.0


def test_build_weight_payload_unavailable_when_all_missing():
    lines = [
        {
            "cantidad_unitaria": 2,
            "peso_unitario_kg": None,
            "peso_linea_kg": 0,
            "estado_linea": "sin_peso",
            "fuente_peso": "sin_datos",
            "warnings": ["logistics_match_conflict:variant_id:[1,2]"],
        }
    ]
    summary = aggregate_order_summary(lines)
    payload = build_weight_payload(summary, lines=lines)
    assert payload["status"] == "unavailable"
    assert payload["value_kg"] is None
    assert payload["reason"] in {
        "all_lines_without_weight",
        "logistics_match_conflict",
    }


def test_build_weight_payload_partial_with_some_weights():
    lines = [
        {
            "cantidad_unitaria": 1,
            "peso_unitario_kg": 1.0,
            "peso_linea_kg": 1.0,
            "estado_linea": "completo",
            "fuente_peso": "erp",
        },
        {
            "cantidad_unitaria": 1,
            "peso_unitario_kg": None,
            "peso_linea_kg": 0,
            "estado_linea": "sin_peso",
            "fuente_peso": "sin_datos",
            "warnings": ["logistics_match_conflict:variant_id:[1,2]"],
        },
    ]
    summary = aggregate_order_summary(lines)
    payload = build_weight_payload(summary, lines=lines)
    assert payload["status"] == "partial"
    assert payload["value_kg"] == 1.0
    assert payload["lines_missing_weight"] == 1


def test_format_kg_null_contract_not_zero():
    """Documenta el bug Number(null)===0 del frontend."""
    assert Number_null_is_zero()
    assert format_kg_safe(None) == "Peso no disponible"
    assert format_kg_safe(0) == "0 kg"


def Number_null_is_zero() -> bool:
    # equivalente JS: Number(null) === 0
    return float(None or 0) == 0.0


def format_kg_safe(n: float | None) -> str:
    if n is None:
        return "Peso no disponible"
    return f"{n:g} kg"


def test_apply_unavailable_does_not_write_zero():
    from backend.services.order_weight_service import apply_order_weight_summary_to_row

    row: dict = {}
    apply_order_weight_summary_to_row(
        row,
        {
            "total_weight": 0.0,
            "missing_products": 2,
            "coverage_percent": 0.0,
            "manual_products": 0,
            "estimated_products": 0,
        },
        weight_status="unavailable",
        weight_reason="products_load_failed",
    )
    assert row["peso_total_kg"] is None
    assert row["weight_kg"] is None
    assert row["weight"]["value_kg"] is None
    assert row["weight"]["status"] == "unavailable"


def test_snapshot_missing_payload_contract():
    from backend.services.order_weight_service import _unavailable_planning_weight

    payload = _unavailable_planning_weight(reason="snapshot_missing")
    assert payload["peso_total_kg"] is None
    assert payload["weight"]["status"] == "unavailable"
    assert payload["weight"]["reason"] == "snapshot_missing"
    assert payload["weight"]["value_kg"] is None
