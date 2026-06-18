"""Tests for planificación operational cost helpers."""

from backend.services.distribuidora.route_operational_costs_service import (
    _defaults,
)


def test_defaults_operational_costs():
    d = _defaults(42)
    assert d["truck_id"] == 42
    assert d["ferry_clp"] == 0
    assert d["per_diem_clp"] == 0
    assert d["other_clp"] == 0
