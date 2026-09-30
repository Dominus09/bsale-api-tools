"""Serialización real (requests) del query ``GET /documents.json`` por rango: live vs reconcile."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qsl, urlsplit

import pytest
import requests

from backend.services.distribuidora import live_sync_service, sync_service
from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.distribuidora.bsale_params import (
    build_documents_range_params,
    build_emission_date_range,
    documents_query_preview,
)
from backend.services.distribuidora.recent_documents_reconcile_service import (
    reconcile_recent_oc_documents,
)

UTC = timezone.utc
NOW = datetime(2026, 9, 30, 16, 40, tzinfo=UTC)
TOKEN = "SECRET-TOKEN-NO-LOG"
HISTORIC_KEYS = ["limit", "offset", "emissiondaterange", "officeid"]


class CapturingSession:
    """Sustituye ``requests.Session``: prepara la URL real y responde lista vacía."""

    def __init__(self) -> None:
        self.urls: list[str] = []
        self.headers: list[dict[str, Any]] = []

    def get(self, url, headers=None, params=None, timeout=None):
        prepared = requests.Request("GET", url, params=params).prepare()
        self.urls.append(prepared.url)
        self.headers.append(dict(headers or {}))
        return SimpleNamespace(
            status_code=200,
            json=lambda: {"items": []},
            text="",
            headers={},
            request=SimpleNamespace(url=prepared.url),
        )


def _query(url: str) -> list[tuple[str, str]]:
    return parse_qsl(urlsplit(url).query, keep_blank_values=True)


def _capture_live() -> CapturingSession:
    session = CapturingSession()
    fake_client = SimpleNamespace(session=session, access_token=TOKEN)
    conn = MagicMock()
    conn.cursor.return_value.fetchone.return_value = (True,)
    with patch.object(live_sync_service, "bsale_token_distribuidora_configured", return_value=True), patch.object(
        live_sync_service, "_utc_now", return_value=NOW
    ), patch.object(live_sync_service, "get_connection", return_value=conn), patch.object(
        live_sync_service, "get_sync_state", return_value=None
    ), patch.object(live_sync_service, "update_sync_state_success"), patch.object(
        live_sync_service, "BsaleClient", return_value=fake_client
    ), patch.object(live_sync_service, "_bsale_token", return_value=TOKEN), patch.object(
        live_sync_service, "log_tx"
    ), patch.object(live_sync_service, "pg_backend_pid", return_value=1), patch.object(
        sync_service, "release_transaction"
    ), patch.object(sync_service.time, "sleep"):
        live_sync_service.live_sync_documents(strict_token=True)
    return session


def _capture_reconcile() -> CapturingSession:
    client = BsaleClient(TOKEN)
    session = CapturingSession()
    client.session = session
    reconcile_recent_oc_documents(
        client,
        company_id=3,
        office_id=1,
        days=3,
        apply=False,
        load_local_folios=lambda folios: set(),
        now=NOW,
    )
    return session


def test_canonical_range_is_bracketed_string():
    assert build_emission_date_range(1790467200, 1790786400) == "[1790467200,1790786400]"


def test_list_range_is_rejected_regression():
    with pytest.raises(TypeError):
        build_emission_date_range([1790467200, 1790786400], 0)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        build_emission_date_range(2, 1)
    url = requests.Request(
        "GET", "https://api.bsale.io/v1/documents.json", params={"emissiondaterange": [1, 2]}
    ).prepare().url
    assert url.count("emissiondaterange=") == 2, "una lista se serializa como parámetro repetido"


def test_live_request_matches_historic_shape():
    session = _capture_live()
    assert len(session.urls) == 1
    url = session.urls[0]
    q = _query(url)
    assert [k for k, _ in q] == HISTORIC_KEYS
    d = dict(q)
    assert d["limit"] == "50" and d["offset"] == "0" and d["officeid"] == "1"
    assert re.fullmatch(r"\[\d+,\d+\]", d["emissiondaterange"])
    lo, hi = (int(x) for x in d["emissiondaterange"].strip("[]").split(","))
    assert lo == int(datetime(2026, 9, 29, tzinfo=UTC).timestamp())
    assert hi == int(NOW.timestamp())
    assert "emissiondaterange=%5B" in url and url.count("emissiondaterange=") == 1
    assert "generationdaterange" not in url and "documenttypeid" not in url
    assert "officeId" not in url
    assert TOKEN not in url


def test_reconcile_request_matches_live_construction():
    rec = _capture_reconcile()
    assert len(rec.urls) == 1
    q = _query(rec.urls[0])
    assert [k for k, _ in q] == HISTORIC_KEYS
    d = dict(q)
    assert d["officeid"] == "1" and d["limit"] == "50" and d["offset"] == "0"
    lo, hi = (int(x) for x in d["emissiondaterange"].strip("[]").split(","))
    assert lo == int(datetime(2026, 9, 27, tzinfo=UTC).timestamp())
    assert hi == int(NOW.timestamp()), "fin = now, nunca futuro"
    assert "generationdaterange" not in rec.urls[0] and "documenttypeid" not in rec.urls[0]
    assert TOKEN not in rec.urls[0]
    assert rec.headers[0] == {"access_token": TOKEN}


def test_live_and_reconcile_share_builder():
    live_q = dict(_query(_capture_live().urls[0]))
    rec_q = dict(_query(_capture_reconcile().urls[0]))
    live_lo, live_hi = (int(x) for x in live_q["emissiondaterange"].strip("[]").split(","))
    rec_lo, rec_hi = (int(x) for x in rec_q["emissiondaterange"].strip("[]").split(","))
    expected_live = build_documents_range_params(
        start_epoch=live_lo, end_epoch=live_hi, office_id=1, limit=50, offset=0
    )
    expected_rec = build_documents_range_params(
        start_epoch=rec_lo, end_epoch=rec_hi, office_id=1, limit=50, offset=0
    )
    assert live_q == {k: str(v) for k, v in expected_live.items()}
    assert rec_q == {k: str(v) for k, v in expected_rec.items()}


def test_second_page_offset_advances():
    client = BsaleClient(TOKEN)
    urls: list[str] = []
    page = [{"id": i, "number": i, "document_type": {"id": "33"}, "office": {"id": "1"}} for i in range(50)]

    def _get(url, headers=None, params=None, timeout=None):
        prepared = requests.Request("GET", url, params=params).prepare()
        urls.append(prepared.url)
        items = page if len(urls) == 1 else []
        return SimpleNamespace(
            status_code=200, json=lambda: {"items": items}, text="", headers={},
            request=SimpleNamespace(url=prepared.url),
        )

    client.session = SimpleNamespace(get=_get)
    reconcile_recent_oc_documents(
        client, company_id=3, office_id=1, days=3, apply=False,
        load_local_folios=lambda f: set(f), now=NOW,
    )
    offsets = [dict(_query(u))["offset"] for u in urls]
    assert offsets == ["0", "50"]


def test_error_message_includes_query_but_not_token():
    client = BsaleClient(TOKEN)

    def _get(url, headers=None, params=None, timeout=None):
        return SimpleNamespace(status_code=403, text="Forbidden", headers={}, json=lambda: {})

    client.session = SimpleNamespace(get=_get)
    with pytest.raises(RuntimeError) as exc:
        client.get("/documents.json", {"limit": 50, "emissiondaterange": "[1,2]", "officeid": 1})
    msg = str(exc.value)
    assert "403" in msg and "emissiondaterange=%5B1%2C2%5D" in msg
    assert TOKEN not in msg


def test_query_preview_has_no_token():
    params = build_documents_range_params(start_epoch=1, end_epoch=2, office_id=1, limit=50, offset=0)
    assert documents_query_preview(params) == "limit=50&offset=0&emissiondaterange=%5B1%2C2%5D&officeid=1"
