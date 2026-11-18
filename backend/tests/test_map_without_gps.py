"""Clientes sin GPS deben aparecer en without_gps, no como marcadores inventados."""

from __future__ import annotations

from backend.services.distribuidora.dispatch_plan_pwa_driver_read_service import (
    _map_client_card,
    get_map_for_driver,
)


def test_map_client_card_without_coords_has_note():
    card = _map_client_card(
        {
            "customer_key": "id:1",
            "client_name": "Sin GPS",
            "address": "Calle 1",
            "city": "Ancud",
            "has_coordinates": False,
            "document_count": 2,
            "load_groups": [],
        },
        {"attention_status": "pending", "attention_label_es": "Pendiente"},
    )
    assert card["has_coordinates"] is False
    assert card["latitude"] is None
    assert card["longitude"] is None
    assert "inventan" in (card.get("location_note_es") or "")


def test_get_map_separates_without_gps(monkeypatch):
    from backend.services.distribuidora import dispatch_plan_pwa_driver_read_service as svc

    monkeypatch.setattr(
        svc,
        "_build_client_index",
        lambda _pid: {
            "clients": [
                {
                    "customer_key": "id:gps",
                    "client_name": "Con GPS",
                    "address": "A",
                    "city": "Ancud",
                    "has_coordinates": True,
                    "latitude": -41.87,
                    "longitude": -73.82,
                    "route_order": 1,
                    "document_count": 1,
                    "units_total": 1,
                    "amount_total": 1000,
                    "load_groups": [],
                },
                {
                    "customer_key": "id:nogps",
                    "client_name": "Sin GPS",
                    "address": "B",
                    "city": "Ancud",
                    "has_coordinates": False,
                    "latitude": None,
                    "longitude": None,
                    "route_order": 2,
                    "document_count": 1,
                    "units_total": 1,
                    "amount_total": 2000,
                    "load_groups": [],
                },
            ],
            "summary": {"clients": 2, "without_coordinates": 1},
            "sources": {},
        },
    )
    monkeypatch.setattr(svc, "_visit_attention_by_customer", lambda _pid: {})
    monkeypatch.setattr(
        svc,
        "build_client_navigation",
        lambda *a, **k: {"google_maps_url": "https://maps.example/x"},
    )

    out = get_map_for_driver(83)
    assert len(out["markers"]) == 1
    assert out["markers"][0]["customer_key"] == "id:gps"
    assert len(out["without_gps"]) == 1
    assert out["without_gps"][0]["customer_key"] == "id:nogps"
    assert out["without_gps"][0]["latitude"] is None
    assert out["summary"]["clients"] == 2
    assert out["summary"]["with_coordinates"] == 1
    assert out["summary"]["without_coordinates"] == 1
