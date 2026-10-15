"""Dashboard V2: composición de Costos, Márgenes y Promociones V2, permisos, frescura y API."""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI

from backend.routers import analytics_v2 as router_mod
from backend.services.analytics.validate_distribuidora_source import AnalyticsValidationError
from backend.services.analytics_v2 import dashboard_service as dash_mod
from backend.services.analytics_v2.access import AnalyticsV2AccessError
from backend.services.analytics_v2.contracts import (
    DASHBOARD_RAW_RESOURCES,
    DataClass,
    FreshnessStatus,
    MarginComplianceStatus as S,
    MarginRisk,
)
from backend.services.analytics_v2.cost_control_service import CostControlV2Service
from backend.services.analytics_v2.dashboard_service import (
    BLOCK_FORBIDDEN,
    BLOCK_OK,
    BLOCK_UNAVAILABLE,
    DashboardV2Service,
    classify_freshness,
    dashboard_capabilities,
    summarize_freshness,
)
from backend.services.analytics_v2.margins_service import MarginsV2Service, MarginsValidationError, data_class
from backend.services.analytics_v2.promotion_source import CampaignExceptionSource
from backend.services.analytics_v2.promotions_service import PromotionsV2Service
from backend.tests.test_analytics_v2_access import _auth
from backend.tests.test_analytics_v2_cost_control import FakeRepo, purchase
from backend.tests.test_analytics_v2_margins import STD_RULES, FakeMarginsRepo, sku, vp
from backend.tests.test_analytics_v2_promotions_access import JsonAsgiClient
from backend.tests.test_analytics_v2_promotions_service import (
    FINANZAS,
    GERENCIA,
    SCOPE3,
    TODAY,
    MemRepo,
    add,
    approve,
    new_campaign,
    seed,
    svc,
)

D = Decimal
NOW = datetime(2026, 10, 13, 15, 0, tzinfo=timezone.utc)
REPO_ROOT = Path(__file__).resolve().parents[2]


class SyncRepo(FakeRepo):
    def __init__(self, *args, sync_rows=None, **kw) -> None:
        super().__init__(*args, **kw)
        self._sync = sync_rows if sync_rows is not None else [
            {"resource": r, "scope": "global", "status": "SUCCESS", "last_success_at": NOW - timedelta(hours=5), "last_attempt_at": NOW, "last_full_reconcile_at": None}
            for r in DASHBOARD_RAW_RESOURCES
        ]

    def sync_state(self, **kw):
        self._rec("sync_state", **kw)
        return self._sync


def scenario(*, ready: bool = True, user: dict[str, Any] = GERENCIA, facts=None, sync_rows=None, campaigns: bool = True):
    """3 SKUs × listas 12/13/16. SKU 11 bajo política en 12 y 13; SKU 10 en oferta verificada; SKU 12 en remate bajo costo."""
    repo = seed(MemRepo(ready=ready))
    repo.prices[(3, 13, 10)]["variant_value_with_taxes"] = D("1300")
    repo.prices[(3, 13, 11)]["variant_value_with_taxes"] = D("1300")
    repo.prices[(3, 12, 11)]["variant_value_with_taxes"] = D("1300")
    repo.prices[(3, 16, 12)]["variant_value_with_taxes"] = D("900")
    if ready and campaigns:
        s = svc(repo)
        approve(s, add(s, new_campaign(s, name="Ofertas octubre"), variant_id=10, list_id=13, price="1300"))
        remate = new_campaign(s, kind="remate", name="Remate vencimiento", responsible="Jefe bodega")
        approve(s, add(s, remate, variant_id=12, list_id=16, price="900", clearance_reason="vencimiento"))
    facts = facts if facts is not None else [sku(10), sku(11), sku(12)]
    prices = [
        vp(lid, vid, str(repo.prices[(3, lid, vid)]["variant_value_with_taxes"]))
        for vid in sorted({f["variant_id"] for f in facts} & {10, 11, 12})
        for lid in (12, 13, 16)
    ]
    cost_repo = SyncRepo(facts=facts, sync_rows=sync_rows)
    costs = CostControlV2Service(cost_repo, SCOPE3, today=TODAY, use_cache=False)
    margins = MarginsV2Service(
        FakeMarginsRepo(prices=prices, rules=STD_RULES),
        costs,
        SCOPE3,
        today=TODAY,
        use_cache=False,
        promotions=CampaignExceptionSource(repo, today=TODAY),
    )
    promotions = PromotionsV2Service(repo, SCOPE3, user=user, today=TODAY)
    resets: list[int] = []
    service = DashboardV2Service(
        scope=SCOPE3,
        capabilities=dashboard_capabilities(user),
        costs=costs,
        margins=margins,
        promotions=promotions,
        reset=lambda: resets.append(1),
        now=NOW,
    )
    return service, {"repo": repo, "cost_repo": cost_repo, "margins": margins, "costs": costs, "resets": resets}


# ---------------------------------------------------------------- composición e indicadores


def test_dashboard_uses_engine_results_without_own_formulas():
    dash, ctx = scenario()
    out = dash.build(cost_basis="ultimo_comparable")
    m = out["margins"]
    assert m["status"] == BLOCK_OK
    data = m["data"]
    rows = ctx["margins"].list_rows(cost_basis="ultimo_comparable", page_size=200)["items"]
    assert data["totals"] == {"combinations": len(rows), "skus": 3, "price_lists": 3}
    for status in S:
        expected = [r for r in rows if r["status"] == status]
        assert data["by_status"][status]["combinations"] == len(expected)
        assert data["by_status"][status]["skus"] == len({r["variant_id"] for r in expected})
    assert {(r["variant_id"], r["price_list"]["id"]) for r in data["top_alerts"]} == {(11, 12), (11, 13)}


def test_unique_skus_are_distinguished_from_sku_list_combinations():
    dash, _ = scenario()
    data = dash.build(cost_basis="ultimo_comparable")["margins"]["data"]
    assert data["alerts"] == {"combinations": 2, "skus": 1}
    assert data["promotion_exceptions"] == {"combinations": 2, "skus": 2}
    assert data["totals"]["combinations"] == 9 and data["totals"]["skus"] == 3


def test_only_authorized_vigent_lists_are_evaluated_and_comoditi_is_excluded():
    dash, ctx = scenario()
    data = dash.build(cost_basis="ultimo_comparable")["margins"]["data"]
    assert [b["price_list_id"] for b in data["by_price_list"]] == [12, 13, 16]
    assert [e["price_list_id"] for e in data["scope"]["evaluated"]] == [12, 13, 16]
    assert {"price_list_id": 14, "name": "COMODITI ALCOHOLES", "status": "retirada"} in data["scope"]["excluded"]
    assert all(r["price_list"]["id"] != 14 for r in data["top_alerts"] + data["top_exception_risks"])
    assert sum(b["combinations"] for b in data["by_price_list"]) == data["totals"]["combinations"]


def test_valid_promotions_leave_normal_alerts_but_cost_risks_stay_visible():
    dash, _ = scenario()
    data = dash.build(cost_basis="ultimo_comparable")["margins"]["data"]
    assert data["by_status"][S.PROMOCION_VIGENTE] == {"combinations": 1, "skus": 1}
    assert data["by_status"][S.REMATE_VIGENTE] == {"combinations": 1, "skus": 1}
    assert {r["variant_id"] for r in data["top_alerts"]} == {11}
    assert data["promotion_exceptions_with_risk"] == {"combinations": 1, "skus": 1}
    risky = data["top_exception_risks"][0]
    assert risky["variant_id"] == 12 and risky["status"] == S.REMATE_VIGENTE
    assert MarginRisk.VENTA_BAJO_COSTO in risky["risks"]
    assert data["by_risk"][MarginRisk.VENTA_BAJO_COSTO]["skus"] == 1


def test_without_campaigns_the_same_products_remain_as_alerts():
    dash, _ = scenario(campaigns=False)
    data = dash.build(cost_basis="ultimo_comparable")["margins"]["data"]
    assert data["promotion_exceptions"] == {"combinations": 0, "skus": 0}
    assert data["alerts"] == {"combinations": 4, "skus": 3}


def test_data_classes_confirmed_provisional_unavailable():
    base = {"status": S.DENTRO_POLITICA.value, "confirmation": "calculado", "validity": {"status": "provisional"}}
    assert data_class(base) is DataClass.PROVISIONAL
    assert data_class({**base, "confirmation": "confirmado", "validity": {"status": "validado"}}) is DataClass.CONFIRMADO
    assert data_class({**base, "confirmation": "confirmado"}) is DataClass.PROVISIONAL
    for status in (S.SIN_PRECIO, S.SIN_COSTO, S.SIN_REGLA, S.COSTO_NO_VERIFICADO):
        assert data_class({**base, "status": status.value, "confirmation": "confirmado", "validity": {"status": "validado"}}) is DataClass.NO_DISPONIBLE


def test_data_class_counts_in_dashboard():
    dash, _ = scenario(facts=[sku(10), sku(11), sku(12, net=None)])
    data = dash.build(cost_basis="ultimo_comparable")["margins"]["data"]
    classes = data["by_data_class"]
    assert classes[DataClass.CONFIRMADO] == {"combinations": 0, "skus": 0}
    assert classes[DataClass.NO_DISPONIBLE] == {"combinations": 3, "skus": 1}
    assert classes[DataClass.PROVISIONAL]["combinations"] == 6
    assert sum(c["combinations"] for c in classes.values()) == data["totals"]["combinations"]


def test_costs_block_reports_reliability_increases_and_dates():
    rising = sku(11)
    rising.update(purchase("lc_", 11, "1500", date(2026, 9, 20)))
    rising.update(purchase("pc_", 12, "1000", date(2026, 8, 1)))
    rising["average_cost_fetched_at"] = NOW - timedelta(hours=10)
    dash, ctx = scenario(facts=[sku(10), rising, sku(12)])
    out = dash.build(cost_basis="ultimo_comparable")["costs"]
    assert out["status"] == BLOCK_OK
    data = out["data"]
    assert data["skus_active"] == 3
    assert sum(data["skus_by_reliability"].values()) == 3
    assert [i["variant_id"] for i in data["relevant_increases"]["top"]] == [11]
    assert data["relevant_increases"]["top"][0]["change_pct"] == "50.00"
    assert data["data_dates"]["average_cost_fetched_max"] == (NOW - timedelta(hours=10)).isoformat()
    assert data["high_alerts"]["total"] == 0
    alert_call = next(kw for name, kw in ctx["cost_repo"].calls if name == "alert_candidates")
    assert alert_call["company_id"] == 3 and alert_call["date_to"] - alert_call["date_from"] == timedelta(days=30)


def test_promotions_block_uses_promotions_engine():
    dash, _ = scenario()
    p = dash.build(cost_basis="ultimo_comparable")["promotions"]
    assert p["status"] == BLOCK_OK
    k = p["data"]["kpis"]
    assert k["campaigns_vigentes"] == 2 and k["products_on_offer"] == 1 and k["products_on_clearance"] == 1
    assert k["exceptions_verified"] == 2 and k["exceptions_pending"] == 0
    assert p["data"]["tables_ready"] is True and p["data"]["writes_bsale"] is False


def test_expired_campaign_with_promo_price_still_in_bsale_is_reported():
    repo = seed(MemRepo())
    repo.prices[(3, 13, 10)]["variant_value_with_taxes"] = D("1300")
    s = svc(repo, today=date(2026, 10, 1))
    approve(s, add(s, new_campaign(s, starts="2026-10-01", ends="2026-10-10"), variant_id=10, list_id=13, price="1300"))
    k = svc(repo).summary()["kpis"]
    assert k["items_expired_not_reverted"] == 1


def test_without_promotion_tables_dashboard_degrades_and_says_so():
    dash, _ = scenario(ready=False)
    out = dash.build(cost_basis="ultimo_comparable")
    assert out["promotions"]["status"] == BLOCK_OK and out["promotions"]["data"]["tables_ready"] is False
    assert out["margins"]["data"]["promotion_exceptions"]["combinations"] == 0
    assert "promociones_no_integradas" in out["reliability"]["warnings"]


# ---------------------------------------------------------------- frescura y confiabilidad


def test_classify_freshness():
    assert classify_freshness("FAILED", NOW, NOW) is FreshnessStatus.CON_ERROR
    assert classify_freshness("SUCCESS", None, NOW) is FreshnessStatus.SIN_DATOS
    assert classify_freshness("SUCCESS", NOW - timedelta(hours=37), NOW) is FreshnessStatus.ATRASADA
    assert classify_freshness("RUNNING", (NOW - timedelta(hours=2)).isoformat(), NOW) is FreshnessStatus.AL_DIA
    assert classify_freshness("SUCCESS", datetime(2026, 10, 13, 10, 0), NOW) is FreshnessStatus.AL_DIA


def test_freshness_summary_worst_scope_and_missing_resources():
    rows = [
        {"resource": "variant_prices", "scope": "12", "status": "SUCCESS", "last_success_at": NOW - timedelta(hours=1)},
        {"resource": "variant_prices", "scope": "13", "status": "SUCCESS", "last_success_at": NOW - timedelta(hours=50)},
        {"resource": "variant_costs", "scope": "global", "status": "SUCCESS", "last_success_at": NOW - timedelta(hours=1)},
        {"resource": "documents", "scope": "global", "status": "FAILED", "last_success_at": None},
    ]
    out = summarize_freshness(rows, NOW)
    by = {r["resource"]: r for r in out["resources"]}
    assert set(by) == set(DASHBOARD_RAW_RESOURCES)
    assert by["variant_prices"]["status"] == "atrasada" and by["variant_prices"]["scopes"] == 2
    assert by["variant_prices"]["last_success_at"] == (NOW - timedelta(hours=50)).isoformat()
    assert by["variant_costs"]["status"] == "al_dia"
    assert by["stocks"]["status"] == "sin_datos"
    assert out["overall"] == "sin_datos"


def test_point_scope_does_not_mark_periodic_resource_stale():
    rows = [
        {"resource": "stocks", "scope": "office:1", "status": "SUCCESS", "last_success_at": NOW - timedelta(hours=1)},
        {"resource": "stocks", "scope": "point", "status": "FAILED", "last_success_at": NOW - timedelta(days=5)},
        {"resource": "variant_prices", "scope": "price_list:12", "status": "SUCCESS", "last_success_at": NOW - timedelta(hours=2)},
        {"resource": "variant_prices", "scope": "point", "status": "SUCCESS", "last_success_at": NOW - timedelta(days=4)},
        {"resource": "products", "scope": "point", "status": "SUCCESS", "last_success_at": NOW - timedelta(days=4)},
    ]
    by = {r["resource"]: r for r in summarize_freshness(rows, NOW)["resources"]}
    assert by["stocks"]["status"] == "al_dia" and by["stocks"]["scopes"] == 1
    assert by["variant_prices"]["status"] == "al_dia"
    assert by["products"]["status"] == "atrasada"


def test_reliability_is_provisional_with_pending_policy_and_unverified_cost():
    dash, _ = scenario()
    rel = dash.build(cost_basis="ultimo_comparable")["reliability"]
    assert rel["raw_freshness"] == "al_dia"
    assert rel["level"] == DataClass.PROVISIONAL
    assert {"politica_margenes_pendiente_revision", "costo_bruto_preliminar"} <= set(rel["warnings"])
    assert not any(w.startswith("sincronizacion_") for w in rel["warnings"])


def test_stale_sync_is_flagged():
    rows = [{"resource": r, "scope": "global", "status": "SUCCESS", "last_success_at": NOW - timedelta(days=3)} for r in DASHBOARD_RAW_RESOURCES]
    dash, _ = scenario(sync_rows=rows)
    out = dash.build(cost_basis="ultimo_comparable")
    assert out["freshness"]["data"]["overall"] == "atrasada"
    assert "sincronizacion_atrasada" in out["reliability"]["warnings"]


# ---------------------------------------------------------------- permisos y aislamiento de fallos


def test_capabilities_by_role():
    assert dashboard_capabilities({"role": "gerencia"}) == {"costs": True, "margins": True, "promotions": True}
    assert dashboard_capabilities({"role": "finanzas"}) == {"costs": False, "margins": True, "promotions": True}
    assert dashboard_capabilities({"role": "comercial"}) == {"costs": False, "margins": False, "promotions": True}
    assert not any(dashboard_capabilities({"role": "operativo"}).values())


def test_finance_does_not_receive_cost_block():
    dash, ctx = scenario(user=FINANZAS)
    out = dash.build(cost_basis="ultimo_comparable")
    assert out["costs"] == {"status": BLOCK_FORBIDDEN, "reason": "sin_permiso", "elapsed_ms": 0, "data": None}
    assert out["margins"]["status"] == BLOCK_OK and out["promotions"]["status"] == BLOCK_OK
    assert not any(name == "alert_candidates" for name, _ in ctx["cost_repo"].calls)


def test_failed_block_is_isolated_and_transaction_reset(monkeypatch):
    dash, ctx = scenario()

    def boom(**_kw):
        raise RuntimeError("password=secreto")

    monkeypatch.setattr(ctx["costs"], "overview", boom)
    out = dash.build(cost_basis="ultimo_comparable")
    assert out["costs"]["status"] == BLOCK_UNAVAILABLE and out["costs"]["reason"] == "error_interno"
    assert "secreto" not in repr(out)
    assert out["margins"]["status"] == BLOCK_OK and out["promotions"]["status"] == BLOCK_OK
    assert ctx["resets"] == [1]
    assert "bloque_costos_no_disponible" in out["reliability"]["warnings"]


def test_timeout_and_missing_scope_are_reported_as_unavailable(monkeypatch):
    dash, ctx = scenario()

    def timeout(**_kw):
        raise AnalyticsValidationError("statement_timeout", error_type="statement_timeout")

    def no_scope(**_kw):
        raise MarginsValidationError("sin alcance")

    monkeypatch.setattr(ctx["costs"], "overview", timeout)
    monkeypatch.setattr(ctx["margins"], "overview", no_scope)
    out = dash.build(cost_basis="ultimo_comparable")
    assert out["costs"]["reason"] == "tiempo_agotado"
    assert out["margins"]["reason"] == "sin_alcance_comercial"
    assert out["reliability"]["level"] == DataClass.NO_DISPONIBLE


def test_dashboard_is_read_only():
    source = (REPO_ROOT / "backend" / "services" / "analytics_v2" / "dashboard_service.py").read_text(encoding="utf-8")
    assert "open_readwrite_connection" not in source and "make_psycopg_rw_executor" not in source
    for keyword in ("INSERT ", "UPDATE ", "DELETE ", "ALTER ", "CREATE ", "DROP "):
        assert keyword not in source


# ---------------------------------------------------------------- runner: caché y empresa


class _Conn:
    def __init__(self) -> None:
        self.rollbacks = 0
        self.closed = False

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


@pytest.fixture()
def runner(monkeypatch):
    dash_mod.clear_cache()
    state: dict[str, Any] = {"scope_calls": 0, "builds": 0, "conns": []}

    def open_conn(_get):
        c = _Conn()
        state["conns"].append(c)
        return c

    def resolve(user, company_id, _execute):
        state["scope_calls"] += 1
        if company_id != 3:
            raise AnalyticsV2AccessError(409, "sin RAW")
        return SCOPE3

    def build(self, *, cost_basis):
        state["builds"] += 1
        blocks = {b: {"status": BLOCK_OK, "data": {}} for b in ("freshness", "margins", "costs", "taxes", "promotions", "packs")}
        return {**blocks, "cost_basis": cost_basis, "meta": {"cached": False}}

    monkeypatch.setattr(dash_mod, "open_readonly_connection", open_conn)
    monkeypatch.setattr(dash_mod, "make_psycopg_executor", lambda conn, **kw: (lambda sql, params: []))
    monkeypatch.setattr(dash_mod, "resolve_company_scope", resolve)
    monkeypatch.setattr(DashboardV2Service, "build", build)
    yield state
    dash_mod.clear_cache()


def test_runner_caches_but_always_validates_company(runner):
    first = dash_mod.run_dashboard(GERENCIA, 3)
    second = dash_mod.run_dashboard(GERENCIA, 3)
    assert first["meta"]["cached"] is False and second["meta"]["cached"] is True
    assert runner["builds"] == 1 and runner["scope_calls"] == 2
    assert all(c.closed for c in runner["conns"])
    dash_mod.run_dashboard(GERENCIA, 3, refresh=True)
    assert runner["builds"] == 2
    with pytest.raises(AnalyticsV2AccessError):
        dash_mod.run_dashboard(GERENCIA, 1)


def test_cache_is_separated_by_permissions_and_cost_basis(runner):
    dash_mod.run_dashboard(GERENCIA, 3)
    dash_mod.run_dashboard(FINANZAS, 3)
    dash_mod.run_dashboard(GERENCIA, 3, cost_basis="ultimo_comparable")
    assert runner["builds"] == 3


def test_promotion_writes_invalidate_dashboard_cache(runner, monkeypatch):
    from backend.services.analytics_v2 import promotions_service as promo_mod

    dash_mod.run_dashboard(GERENCIA, 3)

    class _RW(_Conn):
        def commit(self) -> None:
            pass

    monkeypatch.setattr(promo_mod, "open_readwrite_connection", lambda _g: _RW())
    monkeypatch.setattr(promo_mod, "make_psycopg_rw_executor", lambda conn, **kw: (lambda sql, params: []))
    monkeypatch.setattr(promo_mod, "resolve_company_scope", lambda u, c, e: SCOPE3)
    promo_mod.run_promotions_write(GERENCIA, 3, lambda s: {"ok": True})
    dash_mod.run_dashboard(GERENCIA, 3)
    assert runner["builds"] == 2


def test_invalid_cost_basis_is_rejected_before_querying(runner):
    with pytest.raises(MarginsValidationError):
        dash_mod.run_dashboard(GERENCIA, 3, cost_basis="otro")
    assert runner["conns"] == []


# ---------------------------------------------------------------- API


@pytest.fixture()
def api(monkeypatch):
    calls: list[tuple[dict, int, str, bool]] = []

    def fake(user, company_id, *, cost_basis, refresh):
        calls.append((user, company_id, cost_basis, refresh))
        if company_id == 1:
            raise AnalyticsV2AccessError(409, "La empresa aún no tiene BSALE RAW completo")
        if company_id == 5:
            raise RuntimeError("password=secreto host=db")
        if cost_basis == "otro":
            raise MarginsValidationError("cost_basis inválido: otro")
        return {"ok": True}

    monkeypatch.setattr(router_mod, "run_dashboard", fake)
    app = FastAPI()
    app.include_router(router_mod.router)
    client = JsonAsgiClient(app)
    client.calls = calls  # type: ignore[attr-defined]
    return client


URL = "/analytics-v2/dashboard?company_id=3"


def test_api_requires_authentication(api):
    assert api.request("GET", URL).status_code == 401
    assert api.request("GET", URL, {"Authorization": "Bearer demo-token-12345"}).status_code == 401
    assert api.calls == []


def test_api_requires_margins_permission(api):
    for role in ("operativo", "comercial"):
        assert api.request("GET", URL, _auth(role)).status_code == 403
    for role in ("gerencia", "adm", "finanzas"):
        assert api.request("GET", URL, _auth(role)).status_code == 200
    assert {c[1] for c in api.calls} == {3}


def test_api_params_and_errors(api):
    assert api.request("GET", "/analytics-v2/dashboard", _auth("gerencia")).status_code == 422
    api.request("GET", URL + "&cost_basis=promedio_bsale&refresh=true", _auth("gerencia"))
    assert api.calls[-1][2:] == ("promedio_bsale", True)
    assert api.request("GET", "/analytics-v2/dashboard?company_id=1", _auth("gerencia")).status_code == 409
    assert api.request("GET", URL + "&cost_basis=otro", _auth("gerencia")).status_code == 422
    res = api.request("GET", "/analytics-v2/dashboard?company_id=5", _auth("gerencia"))
    assert res.status_code == 500 and "secreto" not in res.text


# ---------------------------------------------------------------- rendimiento


def test_dashboard_performance_full_company_scale():
    n = 6000
    facts = [sku(i) for i in range(1, n + 1)]
    prices = [vp(lst, i, "1500") for lst in (12, 13, 16) for i in range(1, n + 1)]
    costs = CostControlV2Service(SyncRepo(facts=facts), SCOPE3, today=TODAY, use_cache=False)
    margins = MarginsV2Service(FakeMarginsRepo(prices=prices, rules=STD_RULES), costs, SCOPE3, today=TODAY, use_cache=False)
    promotions = PromotionsV2Service(seed(MemRepo()), SCOPE3, user=GERENCIA, today=TODAY)
    dash = DashboardV2Service(
        scope=SCOPE3, capabilities=dashboard_capabilities(GERENCIA), costs=costs, margins=margins, promotions=promotions, now=NOW
    )
    start = time.perf_counter()
    out = dash.build(cost_basis="ultimo_comparable")
    elapsed = time.perf_counter() - start
    assert out["margins"]["data"]["totals"] == {"combinations": n * 3, "skus": n, "price_lists": 3}
    assert out["costs"]["data"]["skus_active"] == n
    assert elapsed < 10, elapsed
