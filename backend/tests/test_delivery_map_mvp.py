"""Tests unitarios delivery map — agrupación y claves (sin PostgreSQL)."""

from backend.services.distribuidora.delivery_map_service import (
    _customer_key,
    _valid_coords,
)


def test_customer_key_prefers_id():
    assert _customer_key(2244, "Hosteria") == "id:2244"
    assert _customer_key(None, "  Casa Esquila  ") == "name:casa esquila"


def test_valid_coords_rejects_zero_and_null():
    assert _valid_coords(None, -73.0) is None
    assert _valid_coords(0, 0) is None
    assert _valid_coords(-42.25, -73.34) == (-42.25, -73.34)
