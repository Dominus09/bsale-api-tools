"""Avance del recorrido (unitario, sin DB)."""

from __future__ import annotations

from backend.services.distribuidora.delivery_route_progress import compute_route_progress


def _client(key: str, *, order: int | None = None, lat=None, lng=None):
    return {
        "customer_key": key,
        "client_name": key,
        "route_order": order,
        "has_coordinates": lat is not None and lng is not None,
        "latitude": lat,
        "longitude": lng,
        "document_count": 1,
        "amount_total": 1000,
    }


def test_empty_plan_no_division_by_zero():
    out = compute_route_progress(clients=[], attention_by_key={})
    assert out["total_clients"] == 0
    assert out["percent"] == 0.0
    assert out["remaining_clients"] == 0
    assert out["next_customer"] is None


def test_zero_managed_is_zero_percent():
    clients = [_client(f"id:{i}", order=i) for i in range(1, 9)]
    out = compute_route_progress(clients=clients, attention_by_key={})
    assert out["total_clients"] == 8
    assert out["managed_clients"] == 0
    assert out["percent"] == 0.0
    assert out["by_result"]["pending"] == 8


def test_six_of_eight_is_75_percent():
    clients = [_client(f"id:{i}", order=i) for i in range(1, 9)]
    att = {
        "id:1": {"attention_status": "completed"},
        "id:2": {"attention_status": "completed"},
        "id:3": {"attention_status": "completed"},
        "id:4": {"attention_status": "partial"},
        "id:5": {"attention_status": "partial"},
        "id:6": {"attention_status": "not_delivered", "result_reason": "closed_store"},
    }
    out = compute_route_progress(clients=clients, attention_by_key=att)
    assert out["managed_clients"] == 6
    assert out["remaining_clients"] == 2
    assert out["percent"] == 75.0
    assert out["by_result"]["completed"] == 3
    assert out["by_result"]["partial"] == 2
    assert out["by_result"]["not_delivered"] == 1
    assert out["by_result"]["pending"] == 2


def test_two_documents_same_client_count_once():
    # El índice ya es por customer_key; un cliente = una entrada.
    clients = [_client("id:10", order=1)]
    att = {"id:10": {"attention_status": "completed"}}
    out = compute_route_progress(clients=clients, attention_by_key=att)
    assert out["total_clients"] == 1
    assert out["managed_clients"] == 1


def test_in_progress_does_not_count_as_managed():
    clients = [_client("id:1"), _client("id:2")]
    att = {"id:1": {"attention_status": "in_progress"}}
    out = compute_route_progress(clients=clients, attention_by_key=att)
    assert out["managed_clients"] == 0
    assert out["by_result"]["in_progress"] == 1
    assert out["by_result"]["pending"] == 1


def test_pending_review_is_orthogonal():
    clients = [_client("id:1")]
    att = {
        "id:1": {
            "attention_status": "not_delivered",
            "closure_review_status": "pending_review",
        }
    }
    out = compute_route_progress(clients=clients, attention_by_key=att)
    assert out["managed_clients"] == 1
    assert out["by_result"]["not_delivered"] == 1
    assert out["by_result"]["pending_review"] == 1


def test_next_by_route_order_without_gps():
    clients = [
        _client("id:2", order=2),
        _client("id:1", order=1),
        _client("id:3", order=3),
    ]
    att = {"id:1": {"attention_status": "completed"}}
    out = compute_route_progress(clients=clients, attention_by_key=att)
    assert out["next_customer"]["customer_key"] == "id:2"
    assert out["next_customer"]["suggestion_reason"] == "route_order"


def test_next_nearest_with_origin():
    clients = [
        _client("id:a", order=1, lat=-33.4500, lng=-70.6500),
        _client("id:b", order=2, lat=-33.4600, lng=-70.6500),
    ]
    out = compute_route_progress(
        clients=clients,
        attention_by_key={},
        origin_lat=-33.4501,
        origin_lng=-70.6500,
    )
    assert out["next_customer"]["customer_key"] == "id:a"
    assert out["next_customer"]["suggestion_reason"] == "nearest"
    assert out["next_customer"]["distance_m"] is not None
