"""Promociones V2 — servicio con repositorio en memoria, integración con Márgenes V2, SQL y migración.

El repositorio en memoria reproduce la semántica relevante del SQL (filtro por empresa, versión
optimista, historial append-only). La persistencia real en PostgreSQL NO queda validada aquí.
"""

from __future__ import annotations

import copy
import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from backend.services.analytics.validate_distribuidora_source import assert_sql_is_read_only
from backend.services.analytics_v2 import promotions_repo as repo_mod
from backend.services.analytics_v2.access import CompanyScope
from backend.services.analytics_v2.contracts import MarginComplianceStatus as S, PromotionCheck as C
from backend.services.analytics_v2.promotion_campaigns import (
    CampaignConflict,
    CampaignNotFound,
    CampaignValidationError,
    effective_end,
)
from backend.services.analytics_v2.promotion_source import CampaignExceptionSource
from backend.services.analytics_v2.promotions_repo import PromotionsV2Repository
from backend.services.analytics_v2.promotions_service import (
    PromotionsForbidden,
    PromotionsNotReady,
    PromotionsV2Service,
)
from backend.tests.test_analytics_v2_cost_control import FakeRepo
from backend.services.analytics_v2.cost_control_service import CostControlV2Service
from backend.services.analytics_v2.margins_service import MarginsV2Service
from backend.tests.test_analytics_v2_margins import STD_RULES, FakeMarginsRepo, sku, vp

D = Decimal
TODAY = date(2026, 10, 13)
FRESH = datetime(2026, 10, 13, 3, 0, tzinfo=timezone.utc)
SCOPE3 = CompanyScope(company_id=3, company_name="La Quillotana SpA")
SCOPE2 = CompanyScope(company_id=2, company_name="Otra empresa")
GERENCIA = {"email": "Gerencia@Test.local", "role": "gerencia"}
COMERCIAL = {"email": "comercial@test.local", "role": "comercial"}
FINANZAS = {"email": "finanzas@test.local", "role": "finanzas"}
REPO_ROOT = Path(__file__).resolve().parents[2]

LISTS = [
    {"price_list_id": 12, "name": "SUPERMERCADO", "state": 0, "missing_since": None},
    {"price_list_id": 13, "name": "RUTA Y WEB", "state": 0, "missing_since": None},
    {"price_list_id": 14, "name": "COMODITI ALCOHOLES", "state": 1, "missing_since": None},
    {"price_list_id": 16, "name": "MELINKA", "state": 0, "missing_since": None},
]


class MemRepo(PromotionsV2Repository):
    """Repositorio en memoria (sin SQL): emula filtros por empresa, versión optimista y append-only."""

    def __init__(self, *, ready: bool = True) -> None:
        self.ready = ready
        self.lists = {3: LISTS, 2: LISTS}
        self.variants: dict[tuple[int, int], dict[str, Any]] = {}
        self.prices: dict[tuple[int, int, int], dict[str, Any]] = {}
        self.stocks: dict[tuple[int, int], dict[str, Any]] = {}
        self.legacy_rows: list[dict[str, Any]] = []
        self.legacy_heads: list[dict[str, Any]] = []
        self.campaigns: dict[int, dict[str, Any]] = {}
        self.items_: dict[int, dict[str, Any]] = {}
        self.history_: list[dict[str, Any]] = []
        self._seq = 0

    def _id(self) -> int:
        self._seq += 1
        return self._seq

    # ---------- RAW / legacy
    def tables_ready(self):
        return self.ready

    def price_lists(self, *, company_id):
        return self.lists.get(company_id, [])

    def raw_prices(self, *, company_id, variant_ids, price_list_ids):
        return [
            dict(p)
            for (c, l, v), p in self.prices.items()
            if c == company_id and v in set(variant_ids) and l in set(price_list_ids)
        ]

    def raw_variant(self, *, company_id, variant_id):
        v = self.variants.get((company_id, variant_id))
        return dict(v) if v else None

    def raw_stock(self, *, company_id, variant_id):
        return self.stocks.get((company_id, variant_id))

    def search_products(self, *, company_id, q, limit):
        return [dict(v) for (c, _), v in self.variants.items() if c == company_id and q.lower() in (v["product_name"] or "").lower()][:limit]

    def legacy_snapshots(self, *, company_id):
        return [dict(r) for r in self.legacy_rows if r["company_id"] == company_id]

    def legacy_promotions(self, *, company_id):
        return [dict(r) for r in self.legacy_heads if company_id in r["companies"]]

    def legacy_imported(self, *, company_id):
        return {c["legacy_promotion_id"]: c["id"] for c in self.campaigns.values() if c["company_id"] == company_id and c["legacy_promotion_id"]}

    # ---------- campañas
    def campaigns_in_range(self, *, company_id, date_from, date_to):
        rows = [c for c in self.campaigns.values() if c["company_id"] == company_id and c["starts_on"] <= date_to and c["ends_on"] >= date_from]
        return [dict(c) for c in sorted(rows, key=lambda c: (c["starts_on"], c["id"]), reverse=True)]

    def campaign(self, *, company_id, campaign_id):
        c = self.campaigns.get(campaign_id)
        return dict(c) if c and c["company_id"] == company_id else None

    def items(self, *, company_id, campaign_ids):
        return [dict(i) for i in self.items_.values() if i["company_id"] == company_id and i["campaign_id"] in set(campaign_ids)]

    def item(self, *, company_id, campaign_id, item_id):
        i = self.items_.get(item_id)
        return dict(i) if i and i["company_id"] == company_id and i["campaign_id"] == campaign_id else None

    def covering_items(self, *, company_id, date_from, date_to):
        out = []
        for i in self.items_.values():
            c = self.campaigns[i["campaign_id"]]
            if (
                c["company_id"] == company_id
                and i["item_status"] == "activo"
                and c["approval_status"] in ("aprobada", "pendiente_aprobacion")
                and c["starts_on"] <= date_to
                and c["ends_on"] >= date_from
            ):
                out.append(
                    {
                        "item_id": i["id"], "campaign_id": c["id"], "company_id": c["company_id"],
                        "campaign_name": c["name"], "kind": c["kind"], "approval_status": c["approval_status"],
                        "starts_on": c["starts_on"], "ends_on": c["ends_on"], "approved_by": c["approved_by"],
                        "approved_at": c["approved_at"], "variant_id": i["variant_id"],
                        "price_list_id": i["price_list_id"], "promo_gross_price": i["promo_gross_price"],
                        "item_status": i["item_status"],
                    }
                )
        return out

    def campaign_history(self, *, company_id, campaign_id):
        return [copy.deepcopy(h) for h in self.history_ if h["company_id"] == company_id and h["campaign_id"] == campaign_id]

    def company_history(self, *, company_id, since, until, limit, offset):
        rows = [h for h in self.history_ if h["company_id"] == company_id and since <= h["occurred_at"] < until]
        rows = sorted(rows, key=lambda h: (h["occurred_at"], h["id"]), reverse=True)
        return [{**copy.deepcopy(h), "total_rows": len(rows)} for h in rows[offset : offset + limit]]

    # ---------- escrituras
    def insert_campaign(self, *, company_id, values, actor):
        cid = self._id()
        row = {
            "id": cid, "company_id": company_id, "kind": values["kind"], "name": values["name"],
            "starts_on": values["starts_on"], "ends_on": values["ends_on"], "approval_status": "borrador",
            "responsible": values.get("responsible"), "notes": values.get("notes"), "created_by": actor,
            "created_at": FRESH, "updated_by": None, "updated_at": None, "submitted_by": None,
            "submitted_at": None, "approved_by": None, "approved_at": None, "rejected_by": None,
            "rejected_at": None, "cancelled_by": None, "cancelled_at": None, "status_reason": None,
            "legacy_promotion_id": values.get("legacy_promotion_id"), "version": 1,
        }
        self.campaigns[cid] = row
        return dict(row)

    def save_campaign(self, *, company_id, campaign_id, expected_version, values, actor):
        c = self.campaigns.get(campaign_id)
        if not c or c["company_id"] != company_id or c["version"] != expected_version:
            return None
        for k in ("name", "starts_on", "ends_on", "responsible", "notes", "approval_status", "submitted_by",
                  "submitted_at", "approved_by", "approved_at", "rejected_by", "rejected_at", "cancelled_by",
                  "cancelled_at", "status_reason"):
            c[k] = values.get(k)
        c.update(updated_by=actor, updated_at=FRESH, version=c["version"] + 1)
        assert c["approval_status"] != "aprobada" or (c["approved_by"] and c["approved_at"])
        return dict(c)

    def insert_item(self, *, company_id, campaign_id, values, actor):
        assert self.campaigns[campaign_id]["company_id"] == company_id
        iid = self._id()
        row = {"id": iid, "campaign_id": campaign_id, "company_id": company_id, "item_status": "activo",
               "created_by": actor, "created_at": FRESH, "updated_by": None, "updated_at": None,
               "removed_by": None, "removed_at": None, "version": 1, "legacy_snapshot_id": None}
        row.update({k: v for k, v in values.items()})
        self.items_[iid] = row
        return dict(row)

    def save_item(self, *, company_id, campaign_id, item_id, expected_version, values, actor):
        i = self.items_.get(item_id)
        if not i or i["company_id"] != company_id or i["campaign_id"] != campaign_id or i["version"] != expected_version:
            return None
        for k in ("promo_gross_price", "clearance_reason", "notes", "item_status", "removed_by", "removed_at"):
            i[k] = values.get(k)
        i.update(updated_by=actor, updated_at=FRESH, version=i["version"] + 1)
        return dict(i)

    def insert_history(self, **kw):
        row = {"id": self._id(), "occurred_at": FRESH + timedelta(seconds=len(self.history_)), **copy.deepcopy(kw)}
        row["previous_values"] = _jsonish(row["previous_values"])
        row["new_values"] = _jsonish(row["new_values"])
        self.history_.append(row)
        return {"id": row["id"], "occurred_at": row["occurred_at"]}


def _jsonish(values):
    import json

    text = repo_mod.to_json_text(values)
    return json.loads(text) if text else None


def seed(repo: MemRepo, company_id: int = 3) -> MemRepo:
    for vid, name in ((10, "ACEITE 1L"), (11, "ARROZ 1KG"), (12, "PISCO 35")):
        repo.variants[(company_id, vid)] = {
            "variant_id": vid, "product_id": vid * 10, "code": f"SKU{vid}", "bar_code": f"780{vid}",
            "description": "UN", "state": 0, "missing_since": None, "product_name": name,
        }
        for lid, gross in ((12, "1500"), (13, "1400"), (14, "1100"), (16, "1450")):
            repo.prices[(company_id, lid, vid)] = {
                "price_list_id": lid, "variant_id": vid, "variant_value_with_taxes": D(gross),
                "missing_since": None, "api_fetched_at": FRESH,
            }
        repo.stocks[(company_id, vid)] = {"quantity_available": D("37"), "fetched_at": FRESH, "offices": 2}
    return repo


def svc(repo, user=GERENCIA, scope=SCOPE3, today=TODAY) -> PromotionsV2Service:
    midday_chile = datetime(today.year, today.month, today.day, 15, tzinfo=timezone.utc)
    return PromotionsV2Service(repo, scope, user=user, today=today, clock=lambda: midday_chile)


def new_campaign(s, kind="oferta", starts="2026-10-01", ends="2026-10-31", **kw):
    body = {"kind": kind, "name": kw.pop("name", f"{kind} octubre"), "starts_on": starts, "ends_on": ends}
    body.update(kw)
    return s.create_campaign(body)


def add(s, campaign, variant_id=10, list_id=13, price="1300", **kw):
    body = {"version": campaign["version"], "variant_id": variant_id, "price_list_id": list_id, "promo_gross_price": price}
    body.update(kw)
    return s.add_item(campaign["id"], body)["campaign"]


def approve(s, campaign):
    c = s.transition(campaign["id"], "submit", {"version": campaign["version"]})
    return s.transition(c["id"], "approve", {"version": c["version"]})


# ---------------------------------------------------------------- registro y trazabilidad


def test_create_campaign_records_actor_and_history():
    repo = seed(MemRepo())
    c = new_campaign(svc(repo), notes="Folleto octubre")
    assert c["approval_status"] == "borrador" and c["timeline"] == "vigente"
    assert c["created_by"] == "gerencia@test.local"
    assert [h["event"] for h in c["history"]] == ["campania_creada"]
    assert c["history"][0]["actor_role"] == "gerencia"


def test_item_prices_and_stock_come_from_raw_not_from_the_browser():
    repo = seed(MemRepo())
    s = svc(repo)
    c = add(s, new_campaign(s, kind="remate", responsible="Jefe bodega"), clearance_reason="vencimiento")
    item = c["items"][0]
    assert item["normal_gross_price"] == "1400" and item["promo_gross_price"] == "1300.00"
    assert item["reference_stock"] == "37" and item["product_name"] == "ACEITE 1L"
    assert item["discount_pct"] == "7.14"
    stored = next(iter(repo.items_.values()))
    assert stored["normal_gross_price"] == D("1400") and stored["normal_price_fetched_at"] == FRESH


def test_remate_requires_responsible_and_reason():
    repo = seed(MemRepo())
    s = svc(repo)
    with pytest.raises(CampaignValidationError):
        new_campaign(s, kind="remate")
    c = new_campaign(s, kind="remate", responsible="Jefe bodega")
    with pytest.raises(CampaignValidationError):
        add(s, c)
    o = new_campaign(s, name="oferta")
    with pytest.raises(CampaignValidationError):
        add(s, o, clearance_reason="vencimiento")


@pytest.mark.parametrize("list_id", [14, 99, 2])
def test_items_only_for_evaluated_lists(list_id):
    repo = seed(MemRepo())
    s = svc(repo)
    with pytest.raises(CampaignValidationError):
        add(s, new_campaign(s), list_id=list_id)


def test_items_only_for_variants_of_the_same_company():
    repo = seed(MemRepo(), company_id=2)
    s = svc(repo)
    with pytest.raises(CampaignValidationError):
        add(s, new_campaign(s))


def test_product_without_price_is_accepted_with_warning_and_never_verified():
    repo = seed(MemRepo())
    del repo.prices[(3, 13, 10)]
    s = svc(repo)
    c = new_campaign(s)
    out = s.add_item(c["id"], {"version": c["version"], "variant_id": 10, "price_list_id": 13, "promo_gross_price": "1300"})
    assert out["warnings"][0]["code"] == "sin_precio_bsale"
    c = approve(s, out["campaign"])
    item = c["items"][0]
    assert item["normal_gross_price"] is None
    assert item["verification"] == "sin_precio_bsale" and item["exception_check"] == C.PRECIO_NO_VERIFICABLE


def test_duplicate_product_in_same_campaign_and_list_is_rejected():
    repo = seed(MemRepo())
    s = svc(repo)
    c = add(s, new_campaign(s))
    with pytest.raises(CampaignConflict):
        add(s, c)
    assert len(add(s, c, list_id=12)["items"]) == 2


def test_campaigns_dates_and_past_registration():
    s = svc(seed(MemRepo()))
    with pytest.raises(CampaignValidationError):
        new_campaign(s, starts="2026-09-01", ends="2026-09-30")
    with pytest.raises(CampaignValidationError):
        new_campaign(s, starts="2026-10-20", ends="2026-10-10")


# ---------------------------------------------------------------- aprobación, permisos y concurrencia


def test_approval_flow_and_permissions_are_separated():
    repo = seed(MemRepo())
    com = svc(repo, user=COMERCIAL)
    c = add(com, new_campaign(com))
    c = com.transition(c["id"], "submit", {"version": c["version"]})
    assert c["approval_status"] == "pendiente_aprobacion"
    with pytest.raises(PromotionsForbidden):
        com.transition(c["id"], "approve", {"version": c["version"]})
    with pytest.raises(PromotionsForbidden):
        com.transition(c["id"], "cancel", {"version": c["version"], "reason": "x"})
    with pytest.raises(PromotionsForbidden):
        svc(repo, user=FINANZAS).create_campaign({"kind": "oferta", "name": "x", "starts_on": "2026-10-13", "ends_on": "2026-10-20"})
    c = svc(repo).transition(c["id"], "approve", {"version": c["version"]})
    assert c["approval_status"] == "aprobada" and c["approved_by"] == "gerencia@test.local"
    events = [h["event"] for h in c["history"]]
    assert events == ["campania_creada", "item_agregado", "enviada_aprobacion", "aprobada"]
    assert c["history"][-1]["new_values"]["self_approved"] is False


def test_submit_or_approve_without_products_is_rejected():
    s = svc(seed(MemRepo()))
    c = new_campaign(s)
    with pytest.raises(CampaignConflict):
        s.transition(c["id"], "submit", {"version": c["version"]})


def test_reject_and_cancel_require_reason():
    s = svc(seed(MemRepo()))
    c = add(s, new_campaign(s))
    c = s.transition(c["id"], "submit", {"version": c["version"]})
    with pytest.raises(CampaignValidationError):
        s.transition(c["id"], "reject", {"version": c["version"]})
    c = s.transition(c["id"], "reject", {"version": c["version"], "reason": "precio muy bajo"})
    assert c["approval_status"] == "rechazada" and c["status_reason"] == "precio muy bajo"
    with pytest.raises(CampaignValidationError):
        s.transition(c["id"], "cancel", {"version": c["version"]})
    c = s.transition(c["id"], "cancel", {"version": c["version"], "reason": "se descarta"})
    assert c["approval_status"] == "cancelada" and c["cancelled_by"] == "gerencia@test.local"


def test_stale_version_is_rejected():
    s = svc(seed(MemRepo()))
    c = add(s, new_campaign(s))
    with pytest.raises(CampaignConflict):
        s.update_campaign(c["id"], {"version": 1, "notes": "otro"})


def test_price_change_on_approved_campaign_requires_new_approval_and_keeps_history():
    repo = seed(MemRepo())
    s = svc(repo)
    c = approve(s, add(s, new_campaign(s)))
    item_id = c["items"][0]["item_id"]
    before = copy.deepcopy(repo.history_)
    c = s.update_item(c["id"], item_id, {"version": c["version"], "promo_gross_price": "1250", "reason": "ajuste competencia"})["campaign"]
    assert c["approval_status"] == "pendiente_aprobacion" and c["approved_by"] is None
    assert repo.history_[: len(before)] == before
    revoked = next(h for h in c["history"] if h["event"] == "aprobacion_revocada_por_cambio")
    assert revoked["previous_status"] == "aprobada" and revoked["previous_values"]["approved_by"] == "gerencia@test.local"
    change = next(h for h in c["history"] if h["event"] == "item_precio_cambiado")
    assert change["previous_values"]["promo_gross_price"] == "1300.00"
    assert change["previous_values"]["normal_gross_price_registro"] == "1400"
    assert change["new_values"]["promo_gross_price"] == "1250.00"
    assert change["reason"] == "ajuste competencia"


def test_notes_only_edit_keeps_approval():
    s = svc(seed(MemRepo()))
    c = approve(s, add(s, new_campaign(s)))
    c = s.update_campaign(c["id"], {"version": c["version"], "notes": "agregar cartel"})
    assert c["approval_status"] == "aprobada"


def test_removed_items_are_kept_as_history():
    repo = seed(MemRepo())
    s = svc(repo)
    c = add(s, new_campaign(s))
    item_id = c["items"][0]["item_id"]
    with pytest.raises(CampaignValidationError):
        s.remove_item(c["id"], item_id, {"version": c["version"]})
    c = s.remove_item(c["id"], item_id, {"version": c["version"], "reason": "sin stock"})["campaign"]
    assert c["items"][0]["item_status"] == "retirado" and c["items_active"] == 0
    assert item_id in repo.items_


def test_cancelled_campaign_is_immutable():
    s = svc(seed(MemRepo()))
    c = approve(s, add(s, new_campaign(s)))
    c = s.transition(c["id"], "cancel", {"version": c["version"], "reason": "proveedor retiró apoyo"})
    assert c["timeline"] == "cancelada" and c["effective_end_on"] == "2026-10-12"
    with pytest.raises(CampaignConflict):
        s.update_campaign(c["id"], {"version": c["version"], "notes": "x"})
    with pytest.raises(CampaignConflict):
        add(s, c, variant_id=11)


def test_cancellation_day_uses_chile_time():
    late_evening_chile = datetime(2026, 10, 14, 1, 30, tzinfo=timezone.utc)  # 13-oct 22:30 en Santiago
    assert effective_end(date(2026, 10, 31), late_evening_chile) == date(2026, 10, 12)
    assert effective_end(date(2026, 10, 31), datetime(2026, 10, 13, 15, tzinfo=timezone.utc)) == date(2026, 10, 12)
    assert effective_end(date(2026, 10, 5), late_evening_chile) == date(2026, 10, 5)


def test_overlapping_approved_campaign_blocks_approval():
    s = svc(seed(MemRepo()))
    approve(s, add(s, new_campaign(s, name="A")))
    b = add(s, new_campaign(s, name="B", starts="2026-10-10", ends="2026-10-20"), price="1250")
    b = s.transition(b["id"], "submit", {"version": b["version"]})
    with pytest.raises(CampaignConflict):
        s.transition(b["id"], "approve", {"version": b["version"]})


def test_overlap_is_warned_when_adding():
    s = svc(seed(MemRepo()))
    approve(s, add(s, new_campaign(s, name="A")))
    c = new_campaign(s, name="B")
    out = s.add_item(c["id"], {"version": c["version"], "variant_id": 10, "price_list_id": 13, "promo_gross_price": "1250"})
    assert any(w["code"] == "superposicion_con_otra_campania" for w in out["warnings"])


# ---------------------------------------------------------------- aislamiento y ausencia de datos


def test_company_isolation():
    repo = seed(MemRepo())
    seed(repo, company_id=2)
    c = add(svc(repo), new_campaign(svc(repo)))
    other = svc(repo, scope=SCOPE2)
    with pytest.raises(CampaignNotFound):
        other.campaign_detail(c["id"])
    with pytest.raises(CampaignNotFound):
        other.transition(c["id"], "submit", {"version": c["version"]})
    assert repo.covering_items(company_id=2, date_from=TODAY, date_to=TODAY) == []
    # Empresa sin alcance comercial configurado: se rechaza en vez de mezclar listas.
    with pytest.raises(CampaignValidationError):
        other.list_campaigns()


def test_empty_company_returns_zeros():
    out = svc(seed(MemRepo())).summary()
    assert set(out["kpis"].values()) == {0}
    assert out["legacy"] == {"promotions": 0, "active_flag": 0}


def test_without_migration_reads_degrade_and_writes_are_refused():
    repo = seed(MemRepo(ready=False))
    s = svc(repo)
    assert s.summary()["meta"]["tables_ready"] is False
    assert s.list_items(section="vigentes")["total"] == 0
    assert s.history()["total"] == 0
    with pytest.raises(PromotionsNotReady):
        new_campaign(s)


# ---------------------------------------------------------------- secciones, meses y KPIs


def build_month(repo):
    s = svc(repo)
    live = approve(s, add(s, add(s, new_campaign(s, name="Ofertas octubre")), variant_id=11, list_id=12, price="1400"))
    remate = new_campaign(s, kind="remate", name="Remate vencimiento", responsible="Jefe bodega", starts="2026-10-05", ends="2026-10-25")
    remate = approve(s, add(s, remate, variant_id=12, list_id=16, price="900", clearance_reason="vencimiento"))
    upcoming = add(s, new_campaign(s, name="Noviembre", starts="2026-11-01", ends="2026-11-30"), variant_id=11)
    pending = add(s, new_campaign(s, name="Pendiente", starts="2026-10-10", ends="2026-10-20"), variant_id=12, list_id=12, price="1000")
    pending = s.transition(pending["id"], "submit", {"version": pending["version"]})
    return live, remate, upcoming, pending


def test_sections_and_kpis_for_current_month():
    repo = seed(MemRepo())
    repo.prices[(3, 13, 10)]["variant_value_with_taxes"] = D("1300")
    build_month(repo)
    s = svc(repo)
    k = s.summary()["kpis"]
    assert k["campaigns_vigentes"] == 2
    assert k["products_on_offer"] == 2 and k["products_on_clearance"] == 1
    assert k["campaigns_pending_approval"] == 1 and k["campaigns_upcoming"] == 1
    assert k["exceptions_verified"] == 1
    assert k["exceptions_pending"] == 3
    vig = s.list_items(section="vigentes")["items"]
    assert {(i["variant_id"], i["price_list"]["id"]) for i in vig} == {(10, 13), (11, 12)}
    by_variant = {i["variant_id"]: i for i in vig}
    assert by_variant[10]["verification"] == "coincide_bsale" and by_variant[10]["exception_check"] == C.EXCEPCION_VERIFICADA
    assert by_variant[11]["verification"] == "no_coincide_bsale"
    assert [i["variant_id"] for i in s.list_items(section="remates")["items"]] == [12]
    assert [i["campaign_name"] for i in s.list_items(section="proximas")["items"]] == ["Noviembre"]
    pend = s.list_items(section="pendientes")["items"]
    assert {i["campaign_name"] for i in pend} == {"Pendiente", "Ofertas octubre", "Remate vencimiento"}


def test_filters_by_list_kind_and_product_and_invalid_filters():
    repo = seed(MemRepo())
    build_month(repo)
    s = svc(repo)
    assert s.list_items(section="historial", price_list_id=16)["total"] == 1
    assert s.list_items(section="historial", kind="remate")["total"] == 1
    assert s.list_items(section="historial", q="arroz")["total"] == 1
    assert s.list_campaigns(approval="aprobada")["total"] == 2
    with pytest.raises(CampaignValidationError):
        s.list_items(section="historial", price_list_id=14)
    with pytest.raises(CampaignValidationError):
        s.list_items(section="otra")
    with pytest.raises(CampaignValidationError):
        s.list_campaigns(kind="liquidacion")


def test_finished_campaigns_stay_in_their_month_and_stop_applying():
    repo = seed(MemRepo())
    repo.prices[(3, 13, 10)]["variant_value_with_taxes"] = D("1300")
    build_month(repo)
    later = svc(repo, today=date(2026, 11, 15))
    october = later.list_campaigns(month="2026-10")
    assert {c["timeline"] for c in october["items"]} == {"finalizada"}
    assert later.summary(month="2026-10")["kpis"]["campaigns_finished_month"] == 3
    assert later.list_items(section="vigentes")["total"] == 0
    hist = later.list_items(section="historial", month="2026-10")["items"]
    assert {i["verification"] for i in hist if i["variant_id"] == 10} == {"vencida_sin_revertir"}


def test_history_endpoint_lists_events_of_the_month():
    repo = seed(MemRepo())
    build_month(repo)
    out = svc(repo).history(month="2026-10", page_size=5)
    assert out["total"] == len(repo.history_) and len(out["items"]) == 5
    assert svc(repo).history(month="2026-09")["total"] == 0


def test_campaign_detail_prepares_sales_measurement_without_numbers():
    s = svc(seed(MemRepo()))
    c = s.campaign_detail(add(s, new_campaign(s))["id"])
    assert c["measurement"]["status"] == "no_implementado" and c["measurement"]["metrics"] is None


# ---------------------------------------------------------------- compatibilidad legacy


def legacy_seed(repo):
    heads = [
        {"promotion_id": 1, "tipo": "oferta", "canal": "detalle", "fecha_inicio": date(2026, 10, 1), "fecha_fin": date(2026, 10, 31), "activa": True, "created_at": FRESH, "items": 1, "snapshots": 2, "companies": {3}},
        {"promotion_id": 2, "tipo": "remate", "canal": "ruta", "fecha_inicio": date(2026, 9, 1), "fecha_fin": date(2026, 9, 30), "activa": True, "created_at": FRESH, "items": 1, "snapshots": 2, "companies": {3}},
        {"promotion_id": 3, "tipo": "promocion", "canal": "detalle", "fecha_inicio": date(2026, 10, 1), "fecha_fin": date(2026, 12, 31), "activa": True, "created_at": FRESH, "items": 1, "snapshots": 1, "companies": {3}},
    ]

    def snap(sid, pid, variants, lists, sale="1300"):
        h = next(x for x in heads if x["promotion_id"] == pid)
        return {"snapshot_id": sid, "promotion_id": pid, "company_id": 3, "tipo": h["tipo"], "canal": h["canal"],
                "fecha_inicio": h["fecha_inicio"], "fecha_fin": h["fecha_fin"], "activa": h["activa"],
                "created_at": FRESH, "barcode": "78010", "price_list": "RUTA Y WEB", "regular_price": D("1400"),
                "sale_price": D(sale), "fecha_generado": FRESH, "variant_ids": variants, "price_list_ids": lists}

    repo.legacy_heads = heads
    repo.legacy_rows = [
        snap(1, 1, [10], [13]),
        snap(2, 1, [10], [14]),
        snap(3, 2, [10, 11], [13]),
        snap(4, 2, [11], None),
        snap(5, 3, [12], [16], sale="990"),
    ]
    return repo


def test_legacy_promotions_are_listed_read_only_with_mapping_status():
    repo = legacy_seed(seed(MemRepo()))
    original = copy.deepcopy(repo.legacy_rows)
    out = svc(repo).legacy()
    assert out["total"] == 3
    snaps = {s["snapshot_id"]: s["mapping_status"] for p in out["promotions"] for s in p["snapshots"]}
    assert snaps == {1: "mapeable", 2: "lista_fuera_de_alcance", 3: "variante_ambigua", 4: "lista_no_encontrada", 5: "mapeable"}
    assert {p["legacy_status"] for p in out["promotions"]} == {"activa", "vencida"}
    assert all(p["exception_policy"] == "nunca_otorga_excepcion" for p in out["promotions"])
    assert repo.legacy_rows == original


def test_legacy_import_creates_draft_and_is_idempotent():
    repo = legacy_seed(seed(MemRepo()))
    original = copy.deepcopy(repo.legacy_rows)
    s = svc(repo)
    out = s.import_legacy(1)
    c = out["campaign"]
    assert c["approval_status"] == "borrador" and c["legacy_promotion_id"] == 1
    assert out["imported"] == 1 and out["skipped"] == [{"snapshot_id": 2, "reason": "lista_fuera_de_alcance"}]
    assert c["items"][0]["legacy_snapshot_id"] == 1 and c["items"][0]["normal_gross_price"] == "1400"
    with pytest.raises(CampaignConflict):
        s.import_legacy(1)
    with pytest.raises(CampaignNotFound):
        s.import_legacy(99)
    assert repo.legacy_rows == original


def test_past_legacy_promotion_imports_as_history_and_cannot_be_approved():
    repo = legacy_seed(seed(MemRepo()))
    s = svc(repo)
    c = s.import_legacy(2)["campaign"]
    assert c["timeline"] == "finalizada" and c["allowed_actions"] == []


# ---------------------------------------------------------------- integración con Márgenes V2


def margins_with(repo, today=TODAY):
    facts = [sku(10), sku(11), sku(12)]
    prices = [vp(lid, vid, str(repo.prices[(3, lid, vid)]["variant_value_with_taxes"])) for vid in (10, 11, 12) for lid in (12, 13, 16)]
    costs = CostControlV2Service(FakeRepo(facts=facts), SCOPE3, today=today, use_cache=False)
    source = CampaignExceptionSource(repo, today=today)
    return MarginsV2Service(FakeMarginsRepo(prices=prices, rules=STD_RULES), costs, SCOPE3, today=today, use_cache=False, promotions=source)


def row_for(m, variant_id, list_id):
    return next(r for r in m.list_rows(cost_basis="ultimo_comparable", page_size=200)["items"] if r["variant_id"] == variant_id and r["price_list"]["id"] == list_id)


def test_margins_without_campaign_keeps_normal_low_margin_alert():
    repo = seed(MemRepo())
    repo.prices[(3, 13, 10)]["variant_value_with_taxes"] = D("1300")
    r = row_for(margins_with(repo), 10, 13)
    assert r["status"] == S.BAJO_POLITICA and r["promotion"]["check"] == C.SIN_EXCEPCION
    assert S.PROMOCION_PENDIENTE_VERIFICACION not in r["qualifiers"]
    assert margins_with(repo).summary(cost_basis="ultimo_comparable")["meta"]["promotions_integrated"] is True


def test_margins_verified_exception_and_automatic_expiry():
    repo = seed(MemRepo())
    repo.prices[(3, 13, 10)]["variant_value_with_taxes"] = D("1300")
    s = svc(repo)
    approve(s, add(s, new_campaign(s, ends="2026-10-20")))
    r = row_for(margins_with(repo), 10, 13)
    assert r["status"] == S.PROMOCION_VIGENTE
    assert r["promotion"]["exception"]["campaign_name"] == "oferta octubre"
    assert "costo_no_verificado" in r["risks"]
    other_list = row_for(margins_with(repo), 10, 12)
    assert other_list["promotion"]["check"] == C.SIN_EXCEPCION
    expired = row_for(margins_with(repo, today=date(2026, 10, 21)), 10, 13)
    assert expired["status"] == S.BAJO_POLITICA and expired["promotion"]["check"] == C.SIN_EXCEPCION


@pytest.mark.parametrize(
    "setup,check",
    [
        ("pending", C.EXCEPCION_PENDIENTE_APROBACION),
        ("price_mismatch", C.EXCEPCION_NO_VERIFICADA),
        ("wrong_list", C.SIN_EXCEPCION),
        ("cancelled", C.SIN_EXCEPCION),
        ("draft", C.SIN_EXCEPCION),
    ],
)
def test_margins_do_not_grant_silent_exceptions(setup, check):
    repo = seed(MemRepo())
    repo.prices[(3, 13, 10)]["variant_value_with_taxes"] = D("1300")
    s = svc(repo)
    c = new_campaign(s)
    if setup == "pending":
        c = add(s, c)
        s.transition(c["id"], "submit", {"version": c["version"]})
    elif setup == "price_mismatch":
        approve(s, add(s, c, price="1250"))
    elif setup == "wrong_list":
        approve(s, add(s, c, list_id=12, price="1300"))
    elif setup == "cancelled":
        c = approve(s, add(s, c))
        s.transition(c["id"], "cancel", {"version": c["version"], "reason": "x"})
    else:
        add(s, c)
    r = row_for(margins_with(repo), 10, 13)
    assert r["status"] == S.BAJO_POLITICA
    assert r["promotion"]["check"] == check


def test_margins_without_migration_report_source_not_integrated():
    repo = seed(MemRepo(ready=False))
    repo.prices[(3, 13, 10)]["variant_value_with_taxes"] = D("1300")
    m = margins_with(repo)
    r = row_for(m, 10, 13)
    assert r["promotion"]["check"] == C.FUENTE_NO_INTEGRADA and S.PROMOCION_PENDIENTE_VERIFICACION in r["qualifiers"]
    assert m.summary(cost_basis="ultimo_comparable")["meta"]["promotions_integrated"] is False


def test_margins_flag_active_legacy_promotion_as_unverifiable():
    repo = legacy_seed(seed(MemRepo()))
    repo.prices[(3, 13, 10)]["variant_value_with_taxes"] = D("1300")
    r = row_for(margins_with(repo), 10, 13)
    assert r["status"] == S.BAJO_POLITICA
    assert r["promotion"]["check"] == C.PROMOCION_LEGACY_NO_VERIFICABLE
    assert S.PROMOCION_PENDIENTE_VERIFICACION in r["qualifiers"]


# ---------------------------------------------------------------- SQL y migración (estático)


def _tables(sql: str) -> set[tuple[str, str]]:
    return set(re.findall(r"\b(bsale_raw|bsale|app|analytics)\.(\w+)", sql))


def test_read_sql_is_read_only_and_scoped_by_company():
    for name, sql in repo_mod.READ_SQL.items():
        assert_sql_is_read_only(sql)
        assert "payload" not in sql, name
        if name != "TABLES_READY_SQL":
            assert "company_id = %s" in sql, name
        for schema, _table in _tables(sql):
            assert schema in ("bsale_raw", "app"), (name, schema)


def test_write_sql_only_touches_v2_tables():
    for name, sql in repo_mod.WRITE_SQL.items():
        targets = re.findall(r"(?:INSERT INTO|UPDATE)\s+(\w+\.\w+)", sql)
        assert targets and all(t.startswith("app.commercial_") and t.endswith("_v2") for t in targets), name
        assert "DELETE" not in sql.upper()
        assert "bsale_raw" not in sql and "app.promotion" not in sql
    assert "c.version = %s" in repo_mod.SAVE_CAMPAIGN_SQL and "i.version = %s" in repo_mod.SAVE_ITEM_SQL


def test_legacy_tables_are_only_read():
    for sql in list(repo_mod.READ_SQL.values()) + list(repo_mod.WRITE_SQL.values()):
        for verb in ("INSERT INTO app.promotion", "UPDATE app.promotion", "DELETE FROM app.promotion"):
            assert verb not in sql


MIGRATION = REPO_ROOT / "backend" / "sql" / "analytics_v2" / "001_commercial_campaigns_v2.sql"


def test_migration_is_additive_and_preserves_legacy_promotions():
    sql = MIGRATION.read_text(encoding="utf-8")
    code = "\n".join(line for line in sql.splitlines() if not line.strip().startswith("--"))
    assert code.strip().startswith("BEGIN;") and code.strip().endswith("COMMIT;")
    assert not re.search(r"(ALTER|DROP|TRUNCATE|DELETE|UPDATE|INSERT)\s+(TABLE\s+)?(INTO\s+|FROM\s+)?app\.promotion", code, re.I)
    assert not re.search(r"(TABLE|INTO|FROM|ON|UPDATE)\s+bsale_raw\.", code, re.I)
    assert not re.search(r"\bDROP\s+TABLE\b", code, re.I)
    for table in ("commercial_campaigns_v2", "commercial_campaign_items_v2", "commercial_campaign_history_v2"):
        assert f"CREATE TABLE IF NOT EXISTS app.{table}" in code


def test_migration_enforces_integrity_and_append_only_history():
    code = MIGRATION.read_text(encoding="utf-8")
    for fragment in (
        "FOREIGN KEY (campaign_id, company_id)",
        "uq_commercial_campaign_items_v2_active",
        "WHERE item_status = 'activo'",
        "CHECK (promo_gross_price > 0)",
        "CHECK (ends_on >= starts_on)",
        "uq_commercial_campaigns_v2_legacy",
        "BEFORE UPDATE OR DELETE ON app.commercial_campaign_history_v2",
        "BEFORE DELETE ON app.commercial_campaigns_v2",
        "BEFORE DELETE ON app.commercial_campaign_items_v2",
        "ck_commercial_campaigns_v2_remate_responsible",
    ):
        assert fragment in code, fragment
    for status in ("borrador", "pendiente_aprobacion", "aprobada", "rechazada", "cancelada"):
        assert f"'{status}'" in code
