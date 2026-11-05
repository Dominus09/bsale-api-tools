"""F3-C: tarifa histórica unitaria de peonetas (sin escrituras analytics)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from backend.services.distribuidora import dispatch_plan_driver_service as svc
from backend.services.distribuidora.logistics_cost_service import LogisticsCostSettings


PLAN_COLS = [
    ("id",),
    ("status",),
    ("assistant_count",),
    ("driver_count",),
    ("peonetas_recorded",),
    ("crew_cost_clp",),
    ("assistant_cost_clp",),
    ("driver_cost_clp",),
    ("total_route_cost_clp",),
    ("fuel_cost_clp",),
    ("ferry_cost_clp",),
    ("toll_cost_clp",),
    ("extras_cost_clp",),
    ("km_total",),
    ("duration_min",),
    ("truck_id",),
    ("ors_metrics_calculated",),
    ("planning_origin",),
]


def _settings(*, assistant=38102, driver=50895):
    return LogisticsCostSettings(
        assistant_cost_clp_per_trip=float(assistant),
        driver_cost_clp_per_trip=float(driver),
        enabled_modules=["fuel", "crew"],
    )


def _plan_row(
    *,
    assistant_count=1,
    assistant_cost_clp=30000,
    peonetas_recorded=True,
    ors=False,
    km=10.0,
    duration=30.0,
    fuel=5000,
    driver_cost_clp=50895,
    crew_cost_clp=80895,
    total=85895,
    status="picking_generated",
):
    return (
        42,
        status,
        assistant_count,
        1,
        peonetas_recorded,
        crew_cost_clp,
        assistant_cost_clp,
        driver_cost_clp,
        total,
        fuel,
        0,
        0,
        0,
        km if ors else None,
        duration if ors else None,
        7 if ors else None,
        ors,
        "simple_list" if not ors else "ors",
    )


def _patch_common(monkeypatch, plan_row, *, settings=None, events=None):
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    cur.fetchone.return_value = plan_row
    cur.description = PLAN_COLS
    updates = []

    def _execute(sql, params=None):
        if isinstance(sql, str) and "UPDATE distribuidora.dispatch_plan" in sql:
            updates.append({"sql": sql, "params": params})

    cur.execute.side_effect = _execute
    fetch_seq = [plan_row]

    def _fetchone():
        if fetch_seq:
            return fetch_seq.pop(0)
        return (7, "Camion", 8.0, "diesel")

    cur.fetchone.side_effect = _fetchone

    monkeypatch.setattr(svc, "get_connection", lambda: conn)
    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_access.user_can_assign_driver_on_plan",
        lambda *_a, **_k: True,
    )
    monkeypatch.setattr(
        "backend.services.distribuidora.logistics_cost_service.get_logistics_cost_settings",
        lambda: settings or _settings(),
    )
    monkeypatch.setattr(
        "backend.services.distribuidora.peonetas.mark_peonetas_recorded",
        lambda *_a, **_k: None,
    )
    captured = events if events is not None else []

    def _ins(*_a, **kwargs):
        captured.append(kwargs)
        return 1

    monkeypatch.setattr(svc.events_repo, "insert_order_event", _ins)
    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_service.compute_plan_margin",
        lambda *_a, **_k: {"ok": True},
    )
    return conn, cur, updates, captured


def test_resolve_unit_rate_prefers_snapshot():
    rate, src = svc.resolve_peonetas_unit_rate_clp(30000, 38102)
    assert rate == 30000
    assert src == svc.PEONETAS_RATE_SOURCE_SNAPSHOT
    rate2, src2 = svc.resolve_peonetas_unit_rate_clp(0, 38102)
    assert rate2 == 38102
    assert src2 == svc.PEONETAS_RATE_SOURCE_SETTINGS
    rate3, src3 = svc.resolve_peonetas_unit_rate_clp(None, 38102)
    assert rate3 == 38102
    assert src3 == svc.PEONETAS_RATE_SOURCE_SETTINGS


def test_case_a_historical_rate_1_to_2(monkeypatch):
    """Histórica 30.000 vs vigente 38.102; 1→2 peonetas conserva snapshot."""
    plan = _plan_row(assistant_count=1, assistant_cost_clp=30000, ors=False)
    _conn, _cur, updates, events = _patch_common(
        monkeypatch, plan, settings=_settings(assistant=38102)
    )
    out = svc.update_peonetas_on_plan(
        plan_id=42,
        assistant_count=2,
        reason="Ajuste dotación ruta",
        actor={"role": "gerencia", "email": "boss@x.cl", "id": 1},
    )
    assert out["peonetas_unit_cost_clp"] == 30000
    assert out["peonetas_unit_cost_source"] == svc.PEONETAS_RATE_SOURCE_SNAPSHOT
    assert out["peonetas_cost_clp"] == 60000
    assert out["previous_peonetas_cost_clp"] == 30000
    assert out["cost_delta_clp"] == 30000
    assert out["margin_updated"] is False
    assert updates and updates[0]["params"][0] == 2
    assert updates[0]["params"][1] == 30000  # snapshot preserved
    assert events[0]["payload"]["peonetas_unit_cost_source"] == svc.PEONETAS_RATE_SOURCE_SNAPSHOT


def test_case_b_settings_when_no_frozen(monkeypatch):
    plan = _plan_row(
        assistant_count=0,
        assistant_cost_clp=0,
        peonetas_recorded=False,
        ors=False,
    )
    _conn, _cur, updates, events = _patch_common(
        monkeypatch, plan, settings=_settings(assistant=38102)
    )
    out = svc.update_peonetas_on_plan(
        plan_id=42,
        assistant_count=1,
        reason="Primera carga peonetas",
        actor={"role": "gerencia", "email": "boss@x.cl", "id": 1},
    )
    assert out["peonetas_unit_cost_clp"] == 38102
    assert out["peonetas_unit_cost_source"] == svc.PEONETAS_RATE_SOURCE_SETTINGS
    assert out["peonetas_cost_clp"] == 38102
    assert updates[0]["params"][1] == 38102
    assert events[0]["payload"]["peonetas_unit_cost_source"] == svc.PEONETAS_RATE_SOURCE_SETTINGS


def test_case_c_reversible_2_1_2(monkeypatch):
    """Sin acumulaciones: costo = count × tarifa unitaria."""
    costs = []

    def _run(count, frozen_count, frozen_cost=30000):
        plan = _plan_row(
            assistant_count=frozen_count,
            assistant_cost_clp=frozen_cost,
            peonetas_recorded=True,
            ors=False,
        )
        _patch_common(monkeypatch, plan, settings=_settings(assistant=38102))
        out = svc.update_peonetas_on_plan(
            plan_id=42,
            assistant_count=count,
            reason="Cambio reversible peonetas",
            actor={"role": "operativo", "email": "op@x.cl", "id": 2},
        )
        costs.append(out["peonetas_cost_clp"])
        return out

    assert _run(2, 1)["peonetas_cost_clp"] == 60000
    assert _run(1, 2)["peonetas_cost_clp"] == 30000
    assert _run(2, 1)["peonetas_cost_clp"] == 60000
    assert costs == [60000, 30000, 60000]


def test_case_d_zero_keeps_historical_rate(monkeypatch):
    plan = _plan_row(assistant_count=2, assistant_cost_clp=30000, ors=False)
    _conn, _cur, updates, _events = _patch_common(monkeypatch, plan)
    out = svc.update_peonetas_on_plan(
        plan_id=42,
        assistant_count=0,
        reason="Sin peonetas hoy",
        actor={"role": "gerencia", "email": "boss@x.cl", "id": 1},
    )
    assert out["peonetas_cost_clp"] == 0
    assert out["peonetas_unit_cost_clp"] == 30000
    assert updates[0]["params"] == (0, 30000, 42)


def test_case_e_no_ors_no_fake_margin(monkeypatch):
    plan = _plan_row(assistant_count=1, assistant_cost_clp=30000, ors=False)
    _patch_common(monkeypatch, plan)
    out = svc.update_peonetas_on_plan(
        plan_id=42,
        assistant_count=2,
        reason="Simple list sin ORS",
        actor={"role": "gerencia", "email": "boss@x.cl", "id": 1},
    )
    assert out["ors_metrics_calculated"] is False
    assert out["margin_updated"] is False
    assert out["cost_note_es"]
    assert "margen post-transporte" in out["cost_note_es"].lower()


def test_case_f_ors_recomputes_crew_without_double_count(monkeypatch):
    plan = (
        42,
        "picking_generated",
        1,
        1,
        True,
        80000,
        30000,
        50000,
        90000,
        10000,
        2000,
        1000,
        500,
        40.0,
        60.0,
        7,
        True,
        "ors",
    )
    _conn, _cur, updates, _events = _patch_common(monkeypatch, plan)
    margins = []
    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_service.compute_plan_margin",
        lambda *_a, **_k: margins.append(True) or {"ok": True},
    )
    out = svc.update_peonetas_on_plan(
        plan_id=42,
        assistant_count=2,
        reason="ORS con tarifa histórica",
        actor={"role": "gerencia", "email": "boss@x.cl", "id": 1},
    )
    assert out["peonetas_unit_cost_clp"] == 30000
    assert out["margin_updated"] is True
    assert margins == [True]
    # crew = 1*50000 + 2*30000 = 110000; total = fuel+ferry+toll+extras+crew
    assert out["crew_cost_clp"] == 110000
    assert out["total_route_cost_clp"] == 10000 + 2000 + 1000 + 500 + 110000
    # UPDATE params: count, assistant_rate, driver_rate, crew, total, plan_id
    params = updates[0]["params"]
    assert params[1] == 30000
    assert params[2] == 50000  # driver snapshot preserved (not 38102 settings)


def test_case_g_permissions_and_status(monkeypatch):
    with pytest.raises(svc.DriverAssignError) as ei:
        svc.update_peonetas_on_plan(
            plan_id=1,
            assistant_count=1,
            reason="ok reason",
            actor={"role": "vendedor", "email": "v@x.cl"},
        )
    assert ei.value.http_status == 403

    with pytest.raises(svc.DriverAssignError) as ei2:
        svc.update_peonetas_on_plan(
            plan_id=1,
            assistant_count=1,
            reason="ab",
            actor={"role": "gerencia"},
        )
    assert ei2.value.code == "reason_required"

    plan = _plan_row(status="draft", assistant_cost_clp=30000)
    _patch_common(monkeypatch, plan)
    with pytest.raises(svc.DriverAssignError) as ei3:
        svc.update_peonetas_on_plan(
            plan_id=42,
            assistant_count=2,
            reason="Estado inválido",
            actor={"role": "gerencia", "email": "boss@x.cl"},
        )
    assert ei3.value.code == "plan_not_assignable"
