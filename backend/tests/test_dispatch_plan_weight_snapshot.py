"""Tests snapshot de peso en planificación."""

from backend.utils.dispatch_plan_weight_snapshot import (
    aggregate_plan_weight,
    build_stop_snapshots,
    plan_weight_is_frozen,
)


def test_plan_weight_is_frozen():
    assert plan_weight_is_frozen("planned") is True
    assert plan_weight_is_frozen("draft") is False


def test_build_stop_snapshots_groups_by_client():
    orders = [
        {
            "oc_document_id": 1,
            "client_id": 10,
            "address": "Calle 1",
            "route_order": 2,
            "peso_total_kg": 100,
            "cantidad_cajas": 2,
            "cantidad_unidades": 20,
            "cantidad_productos": 3,
            "oc_total_amount": 50000,
        },
        {
            "oc_document_id": 2,
            "client_id": 10,
            "address": "Calle 1",
            "route_order": 3,
            "peso_total_kg": 50,
            "cantidad_cajas": 1,
            "cantidad_unidades": 10,
            "cantidad_productos": 2,
            "oc_total_amount": 30000,
        },
    ]
    stops = build_stop_snapshots(orders)
    assert len(stops) == 1
    assert stops[0]["peso_total_kg"] == 150.0
    assert stops[0]["monto_total"] == 80000.0
    assert stops[0]["cantidad_productos"] == 5


def test_aggregate_plan_weight_utilization():
    orders = [{"peso_total_kg": 1200, "cantidad_productos": 10, "cantidad_unidades": 100, "cobertura_logistica": 100}]
    agg = aggregate_plan_weight(orders, truck_max_weight_kg=1500)
    assert agg["weight_total_kg"] == 1200.0
    assert agg["weight_utilization_pct"] == 80.0
