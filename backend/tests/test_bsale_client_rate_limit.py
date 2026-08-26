"""Tests rate-limit / Retry-After / backoff de BsaleClient (opt-in catchup)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from backend.services.distribuidora.bsale_client import (
    BsaleClient,
    compute_429_wait_seconds,
    parse_retry_after_seconds,
)


def _resp(*, status: int = 200, headers: dict | None = None, json_body=None, text: str = ""):
    r = MagicMock()
    r.status_code = status
    r.headers = headers or {}
    r.text = text
    if json_body is not None:
        r.json.return_value = json_body
    else:
        r.json.side_effect = ValueError("no json")
    r.request = MagicMock()
    r.request.url = "https://api.bsale.io/v1/documents.json"
    return r


def test_parse_retry_after_header_exact():
    r = _resp(status=429, headers={"Retry-After": "17"})
    assert parse_retry_after_seconds(r) == 17.0


def test_parse_retry_after_body_fallback():
    r = _resp(status=429, json_body={"retry_after": 42})
    assert parse_retry_after_seconds(r) == 42.0


def test_compute_wait_uses_retry_after_exactly():
    r = _resp(status=429, headers={"Retry-After": "12"})
    with patch(
        "backend.services.distribuidora.bsale_client.random.uniform",
        return_value=0.0,
    ):
        assert compute_429_wait_seconds(r, retry_index=0, jitter_max=0) == 12.0


def test_compute_wait_exponential_schedule():
    r = _resp(status=429)
    with patch(
        "backend.services.distribuidora.bsale_client.random.uniform",
        return_value=0.0,
    ):
        assert compute_429_wait_seconds(r, retry_index=0, jitter_max=0) == 5.0
        assert compute_429_wait_seconds(r, retry_index=1, jitter_max=0) == 10.0
        assert compute_429_wait_seconds(r, retry_index=2, jitter_max=0) == 20.0
        assert compute_429_wait_seconds(r, retry_index=3, jitter_max=0) == 40.0
        assert compute_429_wait_seconds(r, retry_index=4, jitter_max=0) == 60.0
        assert compute_429_wait_seconds(r, retry_index=9, jitter_max=0) == 60.0


def test_max_5_retries_then_raise():
    stats: dict = {}
    client = BsaleClient("tok", max_429_retries=5, rate_stats=stats)
    # 6 respuestas 429 → 5 retries + raise en el 6º
    client.session.get = MagicMock(
        side_effect=[_resp(status=429, headers={"Retry-After": "0"}) for _ in range(6)]
    )
    with patch("backend.services.distribuidora.bsale_client.time.sleep"):
        with patch(
            "backend.services.distribuidora.bsale_client.compute_429_wait_seconds",
            return_value=0.01,
        ):
            with pytest.raises(RuntimeError, match="429"):
                client.get("/documents.json")
    assert stats["rate_limit_events"] == 6
    assert stats["retry_count"] == 5
    assert stats["requests_total"] == 6


def test_429_then_success_counts_retries():
    stats: dict = {}
    client = BsaleClient("tok", max_429_retries=5, rate_stats=stats)
    client.session.get = MagicMock(
        side_effect=[
            _resp(status=429, headers={"Retry-After": "1"}),
            _resp(status=200, json_body={"items": []}),
        ]
    )
    with patch("backend.services.distribuidora.bsale_client.time.sleep"):
        with patch(
            "backend.services.distribuidora.bsale_client.compute_429_wait_seconds",
            return_value=0.01,
        ):
            out = client.get("/documents.json")
    assert out == {"items": []}
    assert stats["rate_limit_events"] == 1
    assert stats["retry_count"] == 1


def test_legacy_client_no_max_retries_keeps_retrying():
    """Sin max_429_retries (legado) no levanta tras 429; sigue hasta éxito."""
    client = BsaleClient("tok")  # unlimited
    client.session.get = MagicMock(
        side_effect=[
            _resp(status=429, headers={"Retry-After": "0"}),
            _resp(status=429, headers={"Retry-After": "0"}),
            _resp(status=200, json_body={"ok": True}),
        ]
    )
    with patch("backend.services.distribuidora.bsale_client.time.sleep"):
        with patch(
            "backend.services.distribuidora.bsale_client.compute_429_wait_seconds",
            return_value=0.0,
        ):
            assert client.get("/x") == {"ok": True}


def test_throttle_between_requests():
    stats: dict = {}
    client = BsaleClient(
        "tok",
        min_interval_sec=0.2,
        min_interval_jitter_sec=0.0,
        rate_stats=stats,
    )
    client.session.get = MagicMock(
        side_effect=[
            _resp(status=200, json_body={"a": 1}),
            _resp(status=200, json_body={"b": 2}),
        ]
    )
    sleeps: list[float] = []

    def _sleep(sec: float) -> None:
        sleeps.append(sec)

    with patch("backend.services.distribuidora.bsale_client.time.sleep", side_effect=_sleep):
        client.get("/a")
        client.get("/b")
    assert stats["requests_total"] == 2
    assert any(s >= 0.15 for s in sleeps) or float(stats.get("wait_seconds_total") or 0) >= 0.15
