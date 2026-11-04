"""Georref en preview simple_planning desde bsale.clients."""

from backend.services.distribuidora.simple_planning_preview_service import (
    _serialize_client_geo,
)


def test_serialize_client_geo_with_coords():
    out = _serialize_client_geo(
        {
            "client_id": 5,
            "client_name": "ACME",
            "municipality": "Quellón",
            "address": "Calle 1",
            "has_georef": True,
            "lat": -43.12,
            "lng": -73.64,
        }
    )
    assert out["has_georef"] is True
    assert out["lat"] == -43.12
    assert out["lng"] == -73.64


def test_serialize_client_geo_without_coords():
    out = _serialize_client_geo(
        {
            "client_id": 5,
            "client_name": "ACME",
            "municipality": "Quellón",
            "address": "Calle 1",
            "has_georef": True,
            "lat": None,
            "lng": None,
        }
    )
    assert out["has_georef"] is False
    assert out["lat"] is None
    assert out["lng"] is None
