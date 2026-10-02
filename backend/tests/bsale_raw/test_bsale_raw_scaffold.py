"""Tests puros del scaffold ``bsale_raw`` (sin red, sin BD)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from backend.services.bsale.companies import BsaleCompany
from backend.services.bsale_raw.core.client import build_company_client
from backend.services.bsale_raw.core.freshness import FreshnessLevel, FreshnessState
from backend.services.bsale_raw.core.models import RawRecord, StockRecord, payload_hash
from backend.services.bsale_raw.core.rate_limit import (
    CompanyRateLimiters,
    RateLimitConfig,
    RateLimitedSession,
    TokenBucket,
)
from backend.services.bsale_raw.core.registry import REGISTRY, KeyKind, Priority
from backend.services.bsale_raw.webhooks import WebhookValidationError, parse_webhook, route
import backend.services.bsale_raw.resources  # noqa: F401

CPN_MAP = {9001: 1, 9002: 2, 9003: 3}


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def test_token_bucket_burst_then_throttles():
    fc = FakeClock()
    bucket = TokenBucket(RateLimitConfig(requests_per_second=2.0, burst=3), clock=fc.clock, sleep=fc.sleep)
    for _ in range(3):
        assert bucket.acquire() == 0.0
    waited = bucket.acquire()
    assert waited == pytest.approx(0.5)


def test_rate_limits_are_independent_per_company():
    fc = FakeClock()
    limiters = CompanyRateLimiters(
        lambda cid: TokenBucket(RateLimitConfig(1.0, 1), clock=fc.clock, sleep=fc.sleep)
    )
    assert limiters.for_company(1) is limiters.for_company(1)
    assert limiters.for_company(1) is not limiters.for_company(3)
    assert limiters.for_company(1).acquire() == 0.0
    assert limiters.for_company(3).acquire() == 0.0


def test_rate_limit_config_rejects_above_documented_limit(monkeypatch):
    monkeypatch.setenv("BSALE_RAW_RPS_3", "11")
    with pytest.raises(ValueError):
        RateLimitConfig.from_env(3)
    monkeypatch.setenv("BSALE_RAW_RPS_3", "4")
    assert RateLimitConfig.from_env(3).requests_per_second == 4.0


def test_build_company_client_injects_rate_limited_session_without_leaking_token():
    company = BsaleCompany(company_id=3, name="SPA", token_env="BSALE_TOKEN_SPA", token="secret-xyz")
    client = build_company_client(company, CompanyRateLimiters())
    assert isinstance(client.session, RateLimitedSession)
    assert "secret-xyz" not in repr(client)
    assert "secret-xyz" not in repr(company)


def test_payload_hash_is_order_independent():
    assert payload_hash({"a": 1, "b": [1, 2]}) == payload_hash({"b": [1, 2], "a": 1})
    assert payload_hash({"a": 1}) != payload_hash({"a": 2})


def test_raw_record_identity_is_company_scoped():
    a = RawRecord(company_id=1, bsale_id=10203, payload={"id": 10203, "state": 0})
    b = RawRecord(company_id=3, bsale_id=10203, payload={"id": 10203, "state": 0})
    assert a.key != b.key
    assert a.state == 0


def test_stock_record_from_payload_keeps_full_json_and_string_relation_ids():
    payload = {
        "id": 55,
        "quantity": 10.0,
        "quantityReserved": 2.0,
        "quantityAvailable": 8.0,
        "variant": {"href": "x", "id": "31300"},
        "office": {"href": "y", "id": "1"},
    }
    rec = StockRecord.from_payload(3, payload)
    assert rec.key == (3, 31300, 1)
    assert rec.quantity_available == 8.0
    assert rec.payload is payload


def test_stock_record_requires_variant_and_office():
    with pytest.raises(ValueError):
        StockRecord.from_payload(1, {"id": 1, "variant": {"id": "5"}})


def test_registry_declares_all_core_resources_with_parents_first():
    names = REGISTRY.names()
    for required in (
        "offices", "taxes", "document_types", "product_types", "price_lists",
        "products", "variants", "clients", "variant_prices", "variant_costs",
        "stocks", "stock_receptions", "stock_reception_details",
        "stock_consumptions", "stock_consumption_details",
        "documents", "document_details", "document_references", "document_sellers",
    ):
        assert required in names
    for spec in REGISTRY.all():
        assert spec.raw_table.startswith("bsale_raw.")
        if spec.parent:
            assert names.index(spec.parent) < names.index(spec.name)
    assert REGISTRY.get("stocks").key_kind is KeyKind.STOCK
    assert REGISTRY.get("stocks").priority is Priority.CRITICAL
    assert REGISTRY.get("documents").priority is Priority.CRITICAL


def test_registry_webhook_topics_cover_documented_topics():
    for topic in ("product", "variant", "price", "stock", "document"):
        assert REGISTRY.by_webhook_topic(topic), topic


def test_freshness_uses_latest_confirmation():
    now = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
    st = FreshnessState(company_id=3, resource="stocks")
    assert st.evaluate(900, now) is FreshnessLevel.NEVER
    st.last_full_reconcile_at = now - timedelta(hours=2)
    assert st.evaluate(900, now) is FreshnessLevel.STALE
    st.last_webhook_at = now - timedelta(minutes=5)
    assert st.evaluate(900, now) is FreshnessLevel.FRESH
    assert st.evaluate(None, now) is FreshnessLevel.NO_SLA


def test_stock_webhook_uses_exact_resource_path_on_official_host():
    ev = parse_webhook(
        {
            "cpnId": 9003, "resource": "/v2/stocks.json?variant=7079&office=1",
            "resourceId": "7079", "topic": "stock", "action": "put", "officeId": "1", "send": 1503500856,
        },
        CPN_MAP,
    )
    assert ev.company_id == 3
    tasks = route(ev)
    assert tasks[0].resource == "stocks"
    assert tasks[0].url == "https://api.bsale.io/v2/stocks.json?variant=7079&office=1"
    assert dict(tasks[0].params) == {"id": 7079, "office_id": 1}


def test_document_webhook_keeps_unversioned_path_and_refreshes_stock():
    ev = parse_webhook(
        {"cpnId": 9003, "resource": "/documents/14417.json", "resourceId": "14417",
         "topic": "document", "action": "post", "officeId": "2"},
        CPN_MAP,
    )
    tasks = route(ev)
    assert [t.resource for t in tasks] == ["documents", "stocks_for_document"]
    assert tasks[0].url == "https://api.bsale.io/documents/14417.json"


def test_price_webhook_path_must_match_price_list_id():
    base = {"cpnId": 9001, "resourceId": "7079", "topic": "price", "action": "put",
            "resource": "/v2/price_lists/2/details.json?variant=7079"}
    assert parse_webhook({**base, "priceListId": "2"}, CPN_MAP).price_list_id == 2
    with pytest.raises(WebhookValidationError):
        parse_webhook({**base, "priceListId": "3"}, CPN_MAP)


@pytest.mark.parametrize(
    "resource",
    [
        "https://evil.example.com/v2/products/952.json",
        "//evil.example.com/v2/products/952.json",
        "/v2/products/952.json?x=https://evil",
        "/v2/products/953.json",
        "/v1/products/952.json",
        "/v2/products/952.json/../../admin",
        "/v2/variants/952.json",
    ],
)
def test_product_webhook_rejects_unexpected_or_external_resource(resource):
    with pytest.raises(WebhookValidationError):
        parse_webhook(
            {"cpnId": 9002, "resourceId": "952", "topic": "product", "action": "put", "resource": resource},
            CPN_MAP,
        )


@pytest.mark.parametrize(
    "payload",
    [
        {"cpnId": 1234, "resourceId": "1", "topic": "variant", "action": "put", "resource": "/v2/variants/1.json"},
        {"cpnId": 9001, "resourceId": "1", "topic": "unknown", "action": "put", "resource": "/v2/variants/1.json"},
        {"cpnId": 9001, "resourceId": "abc", "topic": "variant", "action": "put", "resource": "/v2/variants/1.json"},
        {"resourceId": "1", "topic": "variant", "action": "put", "resource": "/v2/variants/1.json"},
        {"cpnId": 9001, "resourceId": "1", "topic": "variant", "action": "put"},
    ],
)
def test_parse_webhook_rejects_invalid(payload):
    with pytest.raises(WebhookValidationError):
        parse_webhook(payload, CPN_MAP)


def test_webhook_dedupe_key_is_stable_for_redelivery():
    p = {"cpnId": 9002, "resourceId": "5", "topic": "variant", "action": "put", "send": 1,
         "resource": "/v2/variants/5.json"}
    assert parse_webhook(p, CPN_MAP).dedupe_key == parse_webhook(dict(p), CPN_MAP).dedupe_key


