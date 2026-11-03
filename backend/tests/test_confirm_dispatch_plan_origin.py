"""Motor confirm_dispatch_plan: origen y métricas ORS nulas."""

from __future__ import annotations

from datetime import date

import pytest

from backend.services.distribuidora import dispatch_plan_service as svc


class _Cur:
    def __init__(self):
        self.q = []
        self._truck = ("Hino", 2000, True)
        self.description = []

    def execute(self, sql, params=None):
        self.q.append((sql, params))
        sql_l = sql.lower()
        if "from distribuidora.trucks" in sql_l:
            self._fetch = self._truck
        elif "pg_advisory_xact_lock" in sql_l:
            self._fetch = (1,)
        elif "for update" in sql_l:
            self._fetch_all = []
            self._fetch = None
        else:
            self._fetch = None
            self._fetch_all = []

    def fetchone(self):
        return getattr(self, "_fetch", None)

    def fetchall(self):
        return getattr(self, "_fetch_all", [])

    def close(self):
        pass


class _Conn:
    def __init__(self):
        self.cur = _Cur()
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return self.cur

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass


def test_simple_list_persists_null_ors_metrics(monkeypatch):
    conn = _Conn()
    inserted = {}

    import backend.services.distribuidora.dispatch_plan_oc_guard as guard

    monkeypatch.setattr(guard, "acquire_oc_confirm_locks", lambda *_a, **_k: None)
    monkeypatch.setattr(guard, "assert_ocs_available_for_confirm", lambda *_a, **_k: None)

    monkeypatch.setattr(
        svc.repo,
        "get_latest_plan_for_truck_session",
        lambda *_a, **_k: None,
    )

    def _insert(cur, fields):
        inserted["fields"] = dict(fields)
        return 101, "PLAN-00101"

    monkeypatch.setattr(svc.repo, "insert_dispatch_plan", _insert)
    monkeypatch.setattr(svc.repo, "update_plan_weight_snapshot", lambda *_a, **_k: None)
    monkeypatch.setattr(svc.repo, "insert_plan_orders", lambda *_a, **_k: None)
    monkeypatch.setattr(svc.repo, "insert_plan_stop_snapshots", lambda *_a, **_k: None)
    monkeypatch.setattr(svc, "_enrich_orders_snapshot", lambda cur, orders: orders)
    monkeypatch.setattr(svc, "freeze_orders_weight", lambda cur, orders, calculated_at=None: orders)
    monkeypatch.setattr(svc, "build_stop_snapshots", lambda orders: [])
    monkeypatch.setattr(svc, "aggregate_plan_weight", lambda *_a, **_k: {})
    monkeypatch.setattr(
        "backend.utils.dispatch_plan_picking.resolve_crew_from_route_summary",
        lambda *_a, **_k: (None, [], None),
    )

    out = svc.confirm_dispatch_plan(
        plan_session_id="simple-abc",
        truck_id=2,
        route_name="Sur",
        planning_name="Sur",
        driver_count=1,
        assistant_count=0,
        driver_cost_clp=0,
        assistant_cost_clp=0,
        diesel_price_per_liter=None,
        km_total=None,
        duration_min=None,
        liters_estimated=None,
        fuel_cost_clp=0,
        ferry_cost_clp=0,
        toll_cost_clp=0,
        extras_cost_clp=0,
        crew_cost_clp=0,
        total_route_cost_clp=0,
        route_geometry=None,
        orders=[{"oc_document_id": 1, "oc_number": 1, "route_order": 0}],
        planning_date=date(2026, 11, 10),
        planning_origin="simple_list",
        ors_metrics_calculated=False,
        conn=conn,
        run_auto_picking=False,
    )
    assert out["plan"]["id"] == 101
    fields = inserted["fields"]
    assert fields["planning_origin"] == "simple_list"
    assert fields["ors_metrics_calculated"] is False
    assert fields["km_total"] is None
    assert fields["duration_min"] is None
    assert fields["liters_estimated"] is None
    assert fields["diesel_price_per_liter"] is None
    assert fields["total_route_cost_clp"] == 0
    assert conn.committed is False  # caller owns TX


def test_ors_defaults_require_metrics(monkeypatch):
    with pytest.raises(ValueError, match="diesel_price_per_liter"):
        svc.confirm_dispatch_plan(
            plan_session_id="ors-1",
            truck_id=1,
            route_name="R",
            driver_count=1,
            assistant_count=0,
            driver_cost_clp=0,
            assistant_cost_clp=0,
            diesel_price_per_liter=None,
            km_total=0,
            duration_min=0,
            liters_estimated=0,
            fuel_cost_clp=0,
            ferry_cost_clp=0,
            toll_cost_clp=0,
            extras_cost_clp=0,
            crew_cost_clp=0,
            total_route_cost_clp=0,
            route_geometry=None,
            orders=[{"oc_document_id": 1}],
            planning_origin="ors",
            ors_metrics_calculated=True,
            conn=_Conn(),
            run_auto_picking=False,
        )
