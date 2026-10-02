"""Tests puros del scaffold ``bsale_raw`` (sin red, sin BD)."""

from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

import backend.services.bsale_raw.resources  # noqa: F401
from backend.services.bsale.companies import BsaleCompany
from backend.services.bsale_raw.core.client import build_company_client
from backend.services.bsale_raw.core.freshness import FreshnessLevel, FreshnessState, office_scope
from backend.services.bsale_raw.core.models import RawRecord, StockRecord, payload_hash
from backend.services.bsale_raw.core.rate_limit import (
    CompanyRateLimiters,
    PriorityRateLimiter,
    RateLimitConfig,
    RateLimitedSession,
    RequestPriority,
    TokenBucket,
)
from backend.services.bsale_raw.core.registry import (
    GLOBAL_SCOPE,
    REGISTRY,
    KeyKind,
    Priority,
    ResourceRegistry,
    ResourceSpec,
    document_type_scope,
    price_list_scope,
)
from backend.services.bsale_raw.resources.documents import (
    DOCUMENT_REFRESH_PARTS,
    FORBIDDEN_DOCUMENT_FILTERS,
    OPEN_DOCUMENT_WATCHES,
    DocumentVersion,
    OpenDocumentWatch,
    WatchAction,
    affected_variants,
    changed_parts,
)
from backend.services.bsale_raw.webhooks import (
    TaskKind,
    WebhookValidationError,
    classify_exact_response,
    parse_webhook,
    route,
)

CPN_MAP = {96674: 1, 5807: 2, 21884: 3}


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def _wait_until(predicate, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("timeout esperando condición")
        time.sleep(0.005)


# --- rate limit -------------------------------------------------------------------------------


def test_token_bucket_burst_then_throttles():
    fc = FakeClock()
    bucket = TokenBucket(RateLimitConfig(requests_per_second=2.0, burst=3), clock=fc.clock, sleep=fc.sleep)
    for _ in range(3):
        assert bucket.acquire() == 0.0
    assert bucket.acquire() == pytest.approx(0.5)


def test_rate_limit_config_rejects_above_documented_limit(monkeypatch):
    monkeypatch.setenv("BSALE_RAW_RPS_3", "11")
    with pytest.raises(ValueError):
        RateLimitConfig.from_env(3)
    monkeypatch.setenv("BSALE_RAW_RPS_3", "1")
    assert RateLimitConfig.from_env(3).requests_per_second == 1.0


def test_priority_limiter_serves_highest_priority_first():
    fc = FakeClock()
    bucket = TokenBucket(RateLimitConfig(requests_per_second=1.0, burst=1), clock=fc.clock, sleep=fc.sleep)
    limiter = PriorityRateLimiter(bucket)
    limiter.acquire(RequestPriority.P6_CLIENTS_CONFIG)  # consume el único token

    served: list[str] = []

    def worker(name: str, prio: RequestPriority) -> None:
        limiter.acquire(prio)
        served.append(name)

    threads = [threading.Thread(target=worker, args=("costs", RequestPriority.P5_COSTS))]
    threads[0].start()
    _wait_until(lambda: limiter.waiting_count() == 1)
    for name, prio in (("stock", RequestPriority.P2_STOCK), ("webhook", RequestPriority.P0_TARGETED)):
        t = threading.Thread(target=worker, args=(name, prio))
        threads.append(t)
        t.start()
    _wait_until(lambda: limiter.waiting_count() == 3)

    for expected in (1, 2, 3):
        fc.now += 1.0
        limiter.poke()
        _wait_until(lambda: len(served) == expected)
    for t in threads:
        t.join(timeout=2)
    assert served == ["webhook", "stock", "costs"]


def test_company_limiters_are_independent_and_shared_within_company():
    limiters = CompanyRateLimiters(
        lambda cid: PriorityRateLimiter(TokenBucket(RateLimitConfig(1.0, 1)))
    )
    assert limiters.for_company(1) is limiters.for_company(1)
    assert limiters.for_company(1) is not limiters.for_company(3)


def test_build_company_client_shares_company_limiter_and_hides_token():
    company = BsaleCompany(company_id=3, name="SPA", token_env="BSALE_TOKEN_SPA", token="secret-xyz")
    limiters = CompanyRateLimiters()
    stock = build_company_client(company, limiters, RequestPriority.P2_STOCK)
    costs = build_company_client(company, limiters, RequestPriority.P5_COSTS)
    assert isinstance(stock.session, RateLimitedSession)
    assert stock.session._limiter is costs.session._limiter
    assert stock.session.priority is RequestPriority.P2_STOCK
    assert "secret-xyz" not in repr(stock)
    assert "secret-xyz" not in repr(company)


# --- modelos ----------------------------------------------------------------------------------


def test_payload_hash_is_order_independent():
    assert payload_hash({"a": 1, "b": [1, 2]}) == payload_hash({"b": [1, 2], "a": 1})
    assert payload_hash({"a": 1}) != payload_hash({"a": 2})


def test_raw_record_identity_is_company_scoped():
    a = RawRecord(company_id=1, bsale_id=10203, payload={"id": 10203, "state": 0})
    b = RawRecord(company_id=3, bsale_id=10203, payload={"id": 10203, "state": 1})
    assert a.key != b.key
    assert (a.state, b.state) == (0, 1)


def test_stock_record_from_payload_keeps_full_json_and_string_relation_ids():
    payload = {
        "id": 55, "quantity": 10.0, "quantityReserved": 2.0, "quantityAvailable": 8.0,
        "variant": {"href": "x", "id": "31300"}, "office": {"href": "y", "id": "1"},
    }
    rec = StockRecord.from_payload(3, payload)
    assert rec.key == (3, 31300, 1)
    assert rec.quantity_available == 8.0
    assert rec.payload is payload


def test_stock_record_requires_variant_and_office():
    with pytest.raises(ValueError):
        StockRecord.from_payload(1, {"id": 1, "variant": {"id": "5"}})


# --- registry ---------------------------------------------------------------------------------


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


def test_registry_request_priorities_follow_central_order():
    get = REGISTRY.get
    assert get("documents").request_priority is RequestPriority.P1_OC33
    assert get("stocks").request_priority is RequestPriority.P2_STOCK
    assert get("variant_prices").request_priority is RequestPriority.P3_PRICES
    assert get("products").request_priority is RequestPriority.P4_CATALOG
    assert get("variants").request_priority is RequestPriority.P4_CATALOG
    assert get("variant_costs").request_priority is RequestPriority.P5_COSTS
    assert get("clients").request_priority is RequestPriority.P6_CLIENTS_CONFIG
    assert get("offices").request_priority is RequestPriority.P6_CLIENTS_CONFIG


def test_documents_forbid_global_full_scan_and_generationdaterange():
    docs = REGISTRY.get("documents")
    assert docs.full_scan_global_allowed is False
    assert docs.reconcile_window_days == 45
    assert docs.incremental_filter == "emissiondaterange"
    assert "generationdaterange" in FORBIDDEN_DOCUMENT_FILTERS
    assert all(s.incremental_filter != "generationdaterange" for s in REGISTRY.all())


def test_registry_rejects_no_global_scan_without_window():
    reg = ResourceRegistry()
    with pytest.raises(ValueError):
        reg.register(
            ResourceSpec(
                name="x", list_endpoint="/v1/x.json", item_endpoint=None, raw_table="bsale_raw.x",
                key_kind=KeyKind.ENTITY, priority=Priority.LOW,
                request_priority=RequestPriority.P6_CLIENTS_CONFIG, full_scan_global_allowed=False,
            )
        )


def test_stocks_partitioned_by_office():
    stocks = REGISTRY.get("stocks")
    assert stocks.key_kind is KeyKind.STOCK
    assert stocks.partition_by_office is True
    assert stocks.priority is Priority.CRITICAL


def test_registry_webhook_topics_cover_documented_topics():
    for topic in ("product", "variant", "price", "stock", "document"):
        assert REGISTRY.by_webhook_topic(topic), topic


# --- frescura ---------------------------------------------------------------------------------


def test_freshness_uses_latest_confirmation():
    now = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
    st = FreshnessState(company_id=3, resource="stocks")
    assert st.evaluate(900, now) is FreshnessLevel.NEVER
    st.last_full_reconcile_at = now - timedelta(hours=2)
    assert st.evaluate(900, now) is FreshnessLevel.STALE
    st.last_webhook_at = now - timedelta(minutes=5)
    assert st.evaluate(900, now) is FreshnessLevel.FRESH
    assert st.evaluate(None, now) is FreshnessLevel.NO_SLA


def test_freshness_scope_per_office():
    st = FreshnessState(company_id=3, resource="stocks", scope=office_scope(2))
    assert st.scope == "office:2"
    assert FreshnessState(company_id=3, resource="products").scope == "global"


# --- webhooks ---------------------------------------------------------------------------------


def test_stock_webhook_exact_then_canonical_v1():
    ev = parse_webhook(
        {
            "cpnId": 21884, "resource": "/v2/stocks.json?variant=7079&office=1",
            "resourceId": "7079", "topic": "stock", "action": "put", "officeId": "1", "send": 1503500856,
        },
        CPN_MAP,
    )
    assert ev.company_id == 3
    tasks = route(ev)
    assert [t.kind for t in tasks] == [TaskKind.RESOURCE_EXACT, TaskKind.CANONICAL_V1]
    assert tasks[0].url == "https://api.bsale.io/v2/stocks.json?variant=7079&office=1"
    assert tasks[1].url == "https://api.bsale.io/v1/stocks.json?variantid=7079&officeid=1"
    assert all(t.priority is RequestPriority.P0_TARGETED for t in tasks)


def test_document_webhook_exact_unversioned_then_v1_details_and_stock():
    ev = parse_webhook(
        {"cpnId": 21884, "resource": "/documents/14417.json", "resourceId": "14417",
         "topic": "document", "action": "post", "officeId": "2"},
        CPN_MAP,
    )
    tasks = route(ev)
    assert tasks[0].url == "https://api.bsale.io/documents/14417.json"
    assert tasks[1].url == "https://api.bsale.io/v1/documents/14417.json"
    assert [t.resource for t in tasks[2:]] == ["document_details", "stocks_for_document"]


def test_variant_costs_only_for_new_variant():
    base = {"cpnId": 5807, "resourceId": "5", "topic": "variant", "resource": "/v2/variants/5.json"}
    post = [t.resource for t in route(parse_webhook({**base, "action": "post"}, CPN_MAP))]
    put = [t.resource for t in route(parse_webhook({**base, "action": "put"}, CPN_MAP))]
    assert "variant_costs" in post
    assert "variant_costs" not in put


def test_price_webhook_canonical_v1_uses_variantid():
    ev = parse_webhook(
        {"cpnId": 96674, "resourceId": "7079", "topic": "price", "action": "put",
         "resource": "/v2/price_lists/2/details.json?variant=7079", "priceListId": "2"},
        CPN_MAP,
    )
    assert route(ev)[1].url == "https://api.bsale.io/v1/price_lists/2/details.json?variantid=7079"


def test_price_webhook_path_must_match_price_list_id():
    base = {"cpnId": 96674, "resourceId": "7079", "topic": "price", "action": "put",
            "resource": "/v2/price_lists/2/details.json?variant=7079"}
    with pytest.raises(WebhookValidationError):
        parse_webhook({**base, "priceListId": "3"}, CPN_MAP)


def test_classify_exact_response_detects_v2_envelope():
    v2 = classify_exact_response(200, {"code": 200, "data": {"id": 1}})
    assert v2.envelope == "V2_CODE_DATA"
    assert classify_exact_response(200, {"id": 1}).envelope == "OTHER"
    assert classify_exact_response(503, None).envelope == "NO_JSON"


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
            {"cpnId": 5807, "resourceId": "952", "topic": "product", "action": "put", "resource": resource},
            CPN_MAP,
        )


@pytest.mark.parametrize(
    "payload",
    [
        {"cpnId": 1234, "resourceId": "1", "topic": "variant", "action": "put", "resource": "/v2/variants/1.json"},
        {"cpnId": 96674, "resourceId": "1", "topic": "unknown", "action": "put", "resource": "/v2/variants/1.json"},
        {"cpnId": 96674, "resourceId": "abc", "topic": "variant", "action": "put", "resource": "/v2/variants/1.json"},
        {"resourceId": "1", "topic": "variant", "action": "put", "resource": "/v2/variants/1.json"},
        {"cpnId": 96674, "resourceId": "1", "topic": "variant", "action": "put"},
    ],
)
def test_parse_webhook_rejects_invalid(payload):
    with pytest.raises(WebhookValidationError):
        parse_webhook(payload, CPN_MAP)


def test_webhook_dedupe_key_is_stable_for_redelivery():
    p = {"cpnId": 5807, "resourceId": "5", "topic": "variant", "action": "put", "send": 1,
         "resource": "/v2/variants/5.json"}
    assert parse_webhook(p, CPN_MAP).dedupe_key == parse_webhook(dict(p), CPN_MAP).dedupe_key


def test_webhook_refresh_key_ignores_action_and_send_but_not_resource():
    base = {"cpnId": 21884, "resourceId": "7", "topic": "stock", "officeId": "2",
            "resource": "/v2/stocks.json?variant=7&office=2"}
    first = parse_webhook({**base, "action": "put", "send": 100}, CPN_MAP)
    later = parse_webhook({**base, "action": "put", "send": 200}, CPN_MAP)
    other_office = parse_webhook(
        {**base, "action": "put", "send": 100, "officeId": "3", "resource": "/v2/stocks.json?variant=7&office=3"},
        CPN_MAP,
    )
    assert first.dedupe_key_text != later.dedupe_key_text
    assert first.refresh_key == later.refresh_key == "3|stock|7|2|"
    assert other_office.refresh_key != first.refresh_key


def test_open_document_watch_oc33_and_terminal_states_from_python():
    (watch,) = OPEN_DOCUMENT_WATCHES
    assert (watch.company_id, watch.document_type_id) == (3, 33)
    assert watch.scope == "document_type:33"
    assert 30 * 60 <= watch.grace_seconds <= 60 * 60
    assert not watch.is_terminal(0, None) and not watch.is_terminal(1, "x")


def test_grace_watch_does_not_close_immediately():
    watch = OpenDocumentWatch(3, 33, 45, 900, grace_seconds=1800, grace_min_stable_reads=3,
                              terminal_states=frozenset({1}))
    t0 = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)
    kw = {"state": 1, "commercial_state": None}
    assert watch.decide(state=0, commercial_state=None, terminal_seen_at=None, stable_reads=0, now=t0) == WatchAction.ACTIVE
    assert watch.decide(**kw, terminal_seen_at=None, stable_reads=0, now=t0) == WatchAction.GRACE
    assert watch.decide(**kw, terminal_seen_at=t0, stable_reads=5, now=t0 + timedelta(minutes=10)) == WatchAction.GRACE
    assert watch.decide(**kw, terminal_seen_at=t0, stable_reads=2, now=t0 + timedelta(minutes=40)) == WatchAction.GRACE
    assert watch.decide(**kw, terminal_seen_at=t0, stable_reads=3, now=t0 + timedelta(minutes=30)) == WatchAction.CLOSE


def _version(details, sellers=(), header=None):
    return DocumentVersion(header=header or {"id": 1, "totalAmount": 100}, details=list(details),
                           references=[], sellers=list(sellers), attributes=None)


def test_document_version_hash_changes_with_line_discount_and_sellers():
    base = _version([{"id": 10, "quantity": 2, "totalDiscount": 0, "variant": {"id": "5"}}])
    discount = _version([{"id": 10, "quantity": 2, "totalDiscount": 50, "variant": {"id": "5"}}])
    seller = _version(base.details, sellers=[{"id": 7}])
    assert base.version_hash != discount.version_hash
    assert base.part_hashes()["header"] == discount.part_hashes()["header"]
    assert changed_parts(base.part_hashes(), discount) == {
        "header": False, "details": True, "references": False, "sellers": False, "attributes": False}
    assert changed_parts(base.part_hashes(), seller)["sellers"]
    assert changed_parts(None, base) == dict.fromkeys(DOCUMENT_REFRESH_PARTS, True)


def test_document_version_hash_ignores_detail_order():
    a = {"id": 1, "variant": {"id": "5"}}
    b = {"id": 2, "variant": {"id": "6"}}
    assert _version([a, b]).version_hash == _version([b, a]).version_hash


def test_affected_variants_is_union_previous_and_current():
    previous = _version([{"id": 1, "variant": {"id": "5"}}, {"id": 2, "variant": {"id": "6"}}])
    current = _version([{"id": 2, "variant": {"id": "6"}}, {"id": 3, "variant": {"id": 9}}])
    assert affected_variants(previous.variant_ids(), current.variant_ids()) == {5, 6, 9}
    assert affected_variants(previous.variant_ids(), []) == {5, 6}  # anulación / líneas quitadas


def test_canonical_scopes():
    assert GLOBAL_SCOPE == "global"
    assert office_scope(4) == "office:4"
    assert price_list_scope(2) == "price_list:2"
    assert document_type_scope(33) == "document_type:33"
