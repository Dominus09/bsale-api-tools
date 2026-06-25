"""Tests ventanas de sync de órdenes."""

from backend.services.distribuidora import sync_service as svc


def test_orders_window_defaults():
    assert svc._orders_emission_window_days() >= 1
    assert svc._orders_generation_window_days() >= 1


def test_fetch_documents_window_rejects_unknown_range_field():
    import pytest

    with pytest.raises(ValueError, match="date_range_field"):
        svc._fetch_documents_window(
            None,  # type: ignore[arg-type]
            None,  # type: ignore[arg-type]
            None,  # type: ignore[arg-type]
            desde_ts=1,
            hasta_ts=2,
            stats={},
            log_id=None,
            date_range_field="invalid",
        )
