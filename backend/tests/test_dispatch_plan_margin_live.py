"""Margen comercial vivo: persist=False no escribe; ruta parcial sin post-transporte."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from backend.services.distribuidora import dispatch_plan_service as svc


def _cm(**kwargs):
    base = dict(
        invoiced_revenue_clp=100_000,
        invoiced_cost_clp=60_000,
        commercial_margin_clp=40_000,
        lines_with_cost=3,
        lines_total=3,
        source="variant_cost",
        partial=False,
        message=None,
    )
    base.update(kwargs)

    class CM:
        def __init__(self, d):
            self.__dict__.update(d)

        def as_dict(self):
            return dict(self.__dict__)

    return CM(base)


def test_compute_plan_margin_persist_false_skips_update():
    plan = {
        "id": 77,
        "ors_metrics_calculated": False,
        "total_route_cost_clp": 0,
        "commercial_margin_clp": None,
        "route_geometry": None,
    }
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur

    with (
        patch.object(svc, "get_connection", return_value=conn),
        patch.object(svc.repo, "get_plan_by_id", return_value=plan),
        patch.object(svc, "compute_plan_commercial_margin", return_value=_cm()),
        patch.object(svc.repo, "update_plan_margin") as upd,
    ):
        out = svc.compute_plan_margin(77, persist=False)

    upd.assert_not_called()
    assert out["persisted"] is False
    assert out["commercial_margin_clp"] == 40_000
    assert out["net_operational_clp"] is None
    assert out["route_coverage"] == "none"
    assert out["route_cost_clp"] is None


def test_compute_plan_margin_partial_route_blocks_net_operational():
    plan = {
        "id": 88,
        "ors_metrics_calculated": True,
        "total_route_cost_clp": 15_000,
        "commercial_margin_clp": 10_000,
        "route_geometry": {
            "type": "LineString",
            "coordinates": [],
            "quillotana_route_meta": {"route_coverage": "partial"},
        },
    }
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur

    with (
        patch.object(svc, "get_connection", return_value=conn),
        patch.object(svc.repo, "get_plan_by_id", return_value=plan),
        patch.object(svc, "compute_plan_commercial_margin", return_value=_cm()),
        patch.object(svc.repo, "update_plan_margin") as upd,
    ):
        out = svc.compute_plan_margin(88, persist=True)

    upd.assert_called_once()
    assert out["persisted"] is True
    assert out["route_coverage"] == "partial"
    assert out["net_operational_clp"] is None
    assert out["route_cost_clp"] is None
    assert "parcial" in (out.get("route_cost_unavailable_reason") or "").lower()


def test_compute_plan_margin_full_ors_allows_net_operational():
    plan = {
        "id": 99,
        "ors_metrics_calculated": True,
        "total_route_cost_clp": 5_000,
        "commercial_margin_clp": None,
        "route_geometry": {
            "type": "LineString",
            "coordinates": [],
            "quillotana_route_meta": {"route_coverage": "full"},
        },
    }
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur

    with (
        patch.object(svc, "get_connection", return_value=conn),
        patch.object(svc.repo, "get_plan_by_id", return_value=plan),
        patch.object(svc, "compute_plan_commercial_margin", return_value=_cm()),
        patch.object(svc.repo, "update_plan_margin"),
    ):
        out = svc.compute_plan_margin(99, persist=False)

    assert out["route_coverage"] == "full"
    assert out["route_cost_clp"] == 5_000
    assert out["net_operational_clp"] == 35_000
