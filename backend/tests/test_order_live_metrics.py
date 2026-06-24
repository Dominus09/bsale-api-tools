"""Tests métricas live de planificación."""

from datetime import datetime, timezone

from backend.utils.order_live_metrics import (
    amounts_differ,
    bsale_modified_after_snapshot,
    cities_differ,
    delivery_days_differ,
    evaluate_planning_staleness,
)


def test_amounts_differ_tolerance():
    assert amounts_differ(1000.0, 1000.5, tolerance_clp=1.0) is False
    assert amounts_differ(1000.0, 1002.0, tolerance_clp=1.0) is True


def test_cities_differ_casefold():
    assert cities_differ("Santiago", "santiago") is False
    assert cities_differ("Quillota", "La Calera") is True


def test_delivery_days_differ():
    assert delivery_days_differ("viernes", "Viernes") is False
    assert delivery_days_differ("miercoles", "viernes") is True


def test_bsale_modified_after_snapshot():
    snap = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
    bs_old = datetime(2026, 5, 19, 10, 0, tzinfo=timezone.utc)
    bs_new = datetime(2026, 5, 21, 8, 0, tzinfo=timezone.utc)
    assert bsale_modified_after_snapshot(bs_old, snap) is False
    assert bsale_modified_after_snapshot(bs_new, snap) is True


def test_evaluate_planning_staleness_monto():
    live = {
        "last_bs_update": datetime(2026, 5, 21, 8, 0, tzinfo=timezone.utc),
        "last_erp_update": datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc),
        "total_amount": 1_500_000,
        "city": "Quillota",
        "dia_entrega_detectado": "viernes",
        "weight_kg": 1200,
    }
    snapshot = {
        "created_at": datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc),
        "oc_total_amount": 1_000_000,
        "city": "Quillota",
    }
    result = evaluate_planning_staleness(live=live, snapshot=snapshot)
    assert result["planning_stale"] is True
    assert "monto" in result["planning_stale_reasons"]
    assert "bsale_modificada" in result["planning_stale_reasons"]
