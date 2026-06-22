"""Tests fetch recepciones Bsale (orden ASC / filtro por día)."""

from datetime import datetime, timezone

from backend.services.cost_receptions_fetch import (
    day_start_ts,
    detect_page_order,
    iter_day_starts,
)


def test_detect_page_order_asc():
    items = [
        {"admissionDate": 1000},
        {"admissionDate": 2000},
        {"admissionDate": 3000},
    ]
    assert detect_page_order(items) == "ASC"


def test_detect_page_order_desc():
    items = [
        {"admissionDate": 3000},
        {"admissionDate": 2000},
    ]
    assert detect_page_order(items) == "DESC"


def test_iter_day_starts():
    since = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp())
    until = int(datetime(2026, 1, 3, 12, tzinfo=timezone.utc).timestamp())
    days = iter_day_starts(since, until)
    assert len(days) == 3
    assert days[0] == day_start_ts(datetime(2026, 1, 1, tzinfo=timezone.utc).date())
