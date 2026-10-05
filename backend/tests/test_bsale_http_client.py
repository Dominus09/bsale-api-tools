"""Cliente HTTP Bsale: retry sólo transitorio, Retry-After, backoff, límites y paginación."""

from __future__ import annotations

import pytest

from backend.services.bsale.http_client import (
    BsaleHttpClient,
    BsaleHttpError,
    BsalePaginationError,
    BsaleResponseError,
    BsaleRetryExhaustedError,
)
from backend.tests.bsale_sync_fakes import TIMEOUT_EXC, FakeResponse, FakeSession

TOKEN = "super-secret-token-xyz"


def _client(outcomes, sleeps):
    session = FakeSession(outcomes)
    client = BsaleHttpClient(TOKEN, session=session, sleep=sleeps.append, rng=lambda: 0.5)
    return client, session


def test_429_retry_after_then_success():
    sleeps: list[float] = []
    client, session = _client(
        [FakeResponse(429, {"error": "rate"}, headers={"Retry-After": "7"}), FakeResponse(200, {"ok": 1})],
        sleeps,
    )
    assert client.get_json("taxes.json") == {"ok": 1}
    assert sleeps == [7.0]
    assert len(session.calls) == 2
    assert session.calls[0]["timeout"] == client.timeout


def test_429_without_header_uses_body_retry_after():
    sleeps: list[float] = []
    client, _ = _client(
        [FakeResponse(429, {"retry_after": 3}), FakeResponse(200, {"ok": 1})], sleeps
    )
    client.get_json("taxes.json")
    assert sleeps == [3.0]


def test_503_retries_with_backoff():
    sleeps: list[float] = []
    client, session = _client(
        [FakeResponse(503, None, text="busy"), FakeResponse(503, None), FakeResponse(200, {"a": 1})],
        sleeps,
    )
    assert client.get_json("products.json") == {"a": 1}
    assert len(session.calls) == 3
    assert sleeps == [0.75, 1.5]


def test_timeout_retries():
    sleeps: list[float] = []
    client, session = _client([TIMEOUT_EXC, FakeResponse(200, {"x": 1})], sleeps)
    assert client.get_json("variants.json") == {"x": 1}
    assert len(session.calls) == 2
    assert len(sleeps) == 1


@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_non_transient_4xx_fails_without_retry(status):
    sleeps: list[float] = []
    client, session = _client([FakeResponse(status, None, text="denied")] * 5, sleeps)
    with pytest.raises(BsaleHttpError) as ei:
        client.get_json("taxes.json")
    assert not isinstance(ei.value, BsaleRetryExhaustedError)
    assert ei.value.status == status
    assert len(session.calls) == 1
    assert sleeps == []
    assert TOKEN not in str(ei.value)


def test_retries_exhausted_raises_after_max_attempts():
    sleeps: list[float] = []
    client, session = _client([FakeResponse(503, None)] * 5, sleeps)
    with pytest.raises(BsaleRetryExhaustedError) as ei:
        client.get_json("stocks.json", {"access_token": TOKEN})
    assert len(session.calls) == 5
    assert len(sleeps) == 4
    msg = str(ei.value)
    assert "/v1/stocks.json" in msg and "status=503" in msg and "attempt=5" in msg
    assert TOKEN not in msg


def test_token_sent_only_in_header_and_not_in_repr():
    sleeps: list[float] = []
    client, session = _client([FakeResponse(200, {"items": []})], sleeps)
    client.get_json("taxes.json")
    assert session.calls[0]["headers"]["access_token"] == TOKEN
    assert TOKEN not in repr(client)


def test_foreign_host_rejected():
    client, session = _client([], [])
    with pytest.raises(BsaleHttpError):
        client.get_json("https://evil.example.com/v1/products/1/product_taxes.json")
    assert session.calls == []


def test_invalid_json_and_non_object_fail():
    client, _ = _client([FakeResponse(200, ValueError("bad"), text="<html>")], [])
    with pytest.raises(BsaleResponseError):
        client.get_json("taxes.json")
    client, _ = _client([FakeResponse(200, [1, 2])], [])
    with pytest.raises(BsaleResponseError):
        client.get_json("taxes.json")


def test_fetch_all_items_paginates_until_short_page():
    page1 = {"items": [{"id": i} for i in range(50)]}
    page2 = {"items": [{"id": 100}]}
    client, session = _client([FakeResponse(200, page1), FakeResponse(200, page2)], [])
    items = client.fetch_all_items("taxes.json")
    assert len(items) == 51
    assert [c["params"]["offset"] for c in session.calls] == [0, 50]


def test_fetch_all_items_empty_page_before_count_fails():
    page1 = {"count": 120, "items": [{"id": i} for i in range(50)]}
    page2 = {"count": 120, "items": []}
    client, _ = _client([FakeResponse(200, page1), FakeResponse(200, page2)], [])
    with pytest.raises(BsalePaginationError, match="count=120"):
        client.fetch_all_items("price_lists/4/details.json")


def test_fetch_all_items_empty_first_page_with_positive_count_fails():
    client, _ = _client([FakeResponse(200, {"count": 4387, "items": []})], [])
    with pytest.raises(BsalePaginationError):
        client.fetch_all_items("price_lists/4/details.json")


def test_fetch_all_items_empty_page_with_zero_count_is_ok_and_reports_stats():
    client, _ = _client([FakeResponse(200, {"count": 0, "items": []})], [])
    assert client.fetch_all_items("price_lists/14/details.json") == []
    assert client.last_pagination["reported_count"] == 0
    assert client.last_pagination["stop_reason"] == "empty_page"


def test_fetch_all_items_records_reported_count():
    page1 = {"count": 51, "items": [{"id": i} for i in range(50)]}
    page2 = {"count": 51, "items": [{"id": 100}]}
    client, _ = _client([FakeResponse(200, page1), FakeResponse(200, page2)], [])
    assert len(client.fetch_all_items("price_lists/2/details.json")) == 51
    assert client.last_pagination["reported_count"] == 51
    assert client.last_pagination["pages"] == 2


def test_fetch_all_items_detects_repeated_page():
    page = {"items": [{"id": i} for i in range(50)]}
    client, _ = _client([FakeResponse(200, page), FakeResponse(200, page)], [])
    with pytest.raises(BsalePaginationError):
        client.fetch_all_items("products.json")


def test_fetch_all_items_max_pages_guard():
    pages = [FakeResponse(200, {"items": [{"id": p * 50 + i} for i in range(50)]}) for p in range(3)]
    client, _ = _client(pages, [])
    with pytest.raises(BsalePaginationError):
        client.fetch_all_items("products.json", max_pages=2)


def test_fetch_all_items_requires_items_key():
    client, _ = _client([FakeResponse(200, {"data": []})], [])
    with pytest.raises(BsaleResponseError):
        client.fetch_all_items("offices.json")
