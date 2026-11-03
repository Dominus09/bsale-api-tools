"""load_summary distingue métricas ORS no calculadas de ceros reales."""

from __future__ import annotations

from backend.utils.load_summary import build_load_summary


def _base(**plan_extra):
    plan = {
        "id": 1,
        "status": "planned",
        "route_name": "Sur",
        "fuel_cost_clp": 0,
        "crew_cost_clp": 0,
        "toll_cost_clp": 0,
        "ferry_cost_clp": 0,
        "extras_cost_clp": 0,
        "total_route_cost_clp": 0,
        "km_total": None,
        **plan_extra,
    }
    inv = {"summary": {"confirmed": 0, "probable": 0, "missing": 1}}
    invoicing = {
        "confirmed": {"count": 0, "amount_clp": 0},
        "probable": {"count": 0, "amount_clp": 0},
        "pending": {"count": 1, "amount_clp": 100},
    }
    return build_load_summary(
        plan=plan,
        inv=inv,
        invoicing_block=invoicing,
        margin_block=None,
        picking_meta=None,
        picking_kpis=None,
        header=None,
        orders=[],
    )


def test_simple_list_costs_are_null_not_zero():
    out = _base(planning_origin="simple_list", ors_metrics_calculated=False)
    assert out["ors_metrics_calculated"] is False
    assert out["costs"]["route_total_clp"] is None
    assert out["costs"]["km_total"] is None
    assert out["costs"]["ors_metrics_calculated"] is False
    assert out["operational_status"] == "FACTURACION_PENDIENTE"
    assert out["picking"]["has_snapshot"] is False
    assert out["picking"]["pending_invoices"] is True
    assert out["picking"]["not_load_ready"] is True


def test_ors_zeros_remain_numeric():
    out = _base(
        planning_origin="ors",
        ors_metrics_calculated=True,
        km_total=0,
        total_route_cost_clp=0,
    )
    assert out["costs"]["route_total_clp"] == 0
    assert out["costs"]["ors_metrics_calculated"] is True
