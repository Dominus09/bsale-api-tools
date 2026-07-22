"""Regresión: dashboard de planificación con load_summary e include_margin."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from unittest.mock import patch

import pytest

from backend.services.distribuidora import dispatch_plan_service as svc
from backend.utils.dashboard_stage import DashboardStageRun
from backend.utils.ors_stability import global_invoicing_warning


def _plan_header(plan_id: int = 42) -> dict:
    return {
        "id": plan_id,
        "planning_code": f"PLAN-{plan_id}",
        "planning_date": date(2026, 7, 22),
        "planning_name": "Ruta test",
        "route_name": "Camión 1",
        "truck_name": "Camión 1",
        "status": "planned",
        "fuel_cost_clp": 1000,
        "crew_cost_clp": 2000,
        "toll_cost_clp": 0,
        "ferry_cost_clp": 0,
        "extras_cost_clp": 0,
        "total_route_cost_clp": 3000,
        "commercial_margin_clp": None,
    }


@contextmanager
def _noop_plan_detail_step(*_a, **_k):
    yield


def _patch_dashboard(*, orders, inv, margin_access: bool = True):
    return (
        patch.object(svc, "get_plan_header", return_value=_plan_header()),
        patch.object(svc, "_load_plan_orders_safe", return_value=orders),
        patch.object(svc, "get_invoiced_documents", return_value=inv),
        patch.object(svc, "plan_detail_step", _noop_plan_detail_step),
        patch.object(svc, "fetch_picking_header", side_effect=ValueError("sin picking")),
        patch.object(svc, "has_margin_view_access", return_value=margin_access),
        patch.object(svc, "get_connection", side_effect=RuntimeError("no db in unit test")),
    )


def test_global_invoicing_warning_has_no_fake_oc_folio():
    w = global_invoicing_warning("Fallo de carga")
    assert w["scope"] == "global"
    assert w["oc_document_id"] is None
    assert w["oc_number"] is None
    assert w["message"] == "Fallo de carga"


def test_dashboard_include_margin_zero_operative_ocs():
    """Plan válido, OC anulada/excluida → 0 OCs operativas, HTTP-level 200 payload."""
    inv = {
        "items": [],
        "summary": {
            "confirmed": 0,
            "auto_confirmed": 0,
            "probable": 0,
            "missing": 0,
            "total": 0,
        },
        "warnings": [],
        "probable_notes": [],
        "invoicing_degraded": False,
        "invoicing_source": "full",
    }
    patches = _patch_dashboard(orders=[], inv=inv)
    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6]:
        stage = DashboardStageRun(42)
        result = svc.get_plan_dashboard(
            42,
            include_margin=True,
            user_role="admin",
            stage_run=stage,
        )

    assert result["plan"]["id"] == 42
    assert "load_summary" in result
    assert result["load_summary"]["kpis"]["oc_total_amount_clp"] == 0
    assert result["invoicing"]["total_orders"] == 0
    assert result["invoicing"]["total_oc_amount_clp"] == 0
    assert result["invoicing"]["confirmed"]["count"] == 0
    assert result["invoicing"]["probable"]["count"] == 0
    assert result["invoicing"]["pending"]["count"] == 0
    assert result["plan_orders"] == []
    assert str(stage.last_stage) == "4"
    assert stage.last_label == "build_response"
    # No NameError / degraded fallback from programming error
    assert result.get("degraded") is False
    for w in result.get("warnings") or []:
        assert w.get("oc_document_id") not in (0, "0")


def test_dashboard_include_margin_with_operative_ocs():
    orders = [
        {
            "oc_document_id": 1001,
            "oc_number": 50001,
            "client_name": "Cliente A",
            "oc_total_amount": 100_000,
            "peso_total_kg": 12.5,
            "weight_kg": 12.5,
            "route_order": 1,
        },
        {
            "oc_document_id": 1002,
            "oc_number": 50002,
            "client_name": "Cliente B",
            "oc_total_amount": 50_000,
            "peso_total_kg": 7.5,
            "weight_kg": 7.5,
            "route_order": 2,
        },
        {
            "oc_document_id": 1003,
            "oc_number": 50003,
            "client_name": "Cliente C",
            "oc_total_amount": 25_000,
            "peso_total_kg": 3.0,
            "weight_kg": 3.0,
            "route_order": 3,
        },
    ]
    inv = {
        "items": [
            {"oc_document_id": 1001, "oc_number": 50001, "status": "confirmed"},
            {"oc_document_id": 1002, "oc_number": 50002, "status": "probable"},
            {"oc_document_id": 1003, "oc_number": 50003, "status": "missing"},
        ],
        "summary": {
            "confirmed": 1,
            "auto_confirmed": 0,
            "probable": 1,
            "missing": 1,
            "total": 3,
        },
        "warnings": [
            {
                "oc_document_id": 1003,
                "oc_number": 50003,
                "message": "OC aún sin documento facturado asociado",
            }
        ],
        "probable_notes": [
            {
                "oc_document_id": 1002,
                "oc_number": 50002,
                "message": "Coincidencia probable",
            }
        ],
        "invoicing_degraded": False,
        "invoicing_source": "full",
    }
    patches = _patch_dashboard(orders=orders, inv=inv)
    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6]:
        result = svc.get_plan_dashboard(
            42,
            include_margin=True,
            user_role="admin",
        )

    assert result["invoicing"]["total_orders"] == 3
    assert result["invoicing"]["total_oc_amount_clp"] == 175_000
    assert result["invoicing"]["confirmed"]["count"] == 1
    assert result["invoicing"]["confirmed"]["amount_clp"] == 100_000
    assert result["invoicing"]["probable"]["count"] == 1
    assert result["invoicing"]["probable"]["amount_clp"] == 50_000
    assert result["invoicing"]["pending"]["count"] == 1
    assert result["invoicing"]["pending"]["amount_clp"] == 25_000

    ls = result["load_summary"]
    assert ls["kpis"]["oc_total_amount_clp"] == 175_000
    assert ls["invoicing"]["confirmed_total"] == 1
    assert ls["invoicing"]["probable"] == 1
    assert ls["invoicing"]["pending"] == 1
    assert ls["invoicing"]["confirmed_amount_clp"] == 100_000

    assert len(result["plan_orders"]) == 3
    assert sum(float(o["peso_total_kg"] or 0) for o in result["plan_orders"]) == pytest.approx(
        23.0
    )
    assert result["warnings"][0]["oc_document_id"] == 1003
    assert result.get("degraded") is False


def test_build_load_summary_is_imported_and_callable():
    """NameError de build_load_summary no debe volver a aparecer en build_response."""
    assert callable(svc.build_load_summary)
    with patch.object(svc, "fetch_picking_header", side_effect=ValueError("sin picking")), patch.object(
        svc, "get_connection", side_effect=RuntimeError("no db in unit test")
    ):
        summary = svc._load_summary_context(
            42,
            _plan_header(),
            {
                "summary": {
                    "confirmed": 0,
                    "auto_confirmed": 0,
                    "probable": 0,
                    "missing": 0,
                    "total": 0,
                }
            },
            {
                "total_oc_amount_clp": 0,
                "confirmed": {"count": 0, "amount_clp": 0, "auto_confirmed_count": 0},
                "probable": {"count": 0, "amount_clp": 0},
                "pending": {"count": 0, "amount_clp": 0},
            },
            None,
            orders=[],
        )
    assert "kpis" in summary
    assert "invoicing" in summary
    assert summary["kpis"]["oc_total_amount_clp"] == 0
