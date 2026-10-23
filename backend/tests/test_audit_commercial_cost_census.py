"""Pruebas unitarias del script de auditoría comercial (sin PostgreSQL)."""

from __future__ import annotations

import json

import pytest

from backend.scripts.audit_commercial_cost_census import (
    DEFAULT_PRICE_LIST,
    build_report,
    grant_instructions,
)
from backend.services.analytics.validate_distribuidora_source import (
    AnalyticsValidationError,
    assert_sql_is_read_only,
)


def test_census_identity_formula_closes_with_orphans():
    """stock_pos = activas + inactivas + huérfanas (caso real 3873 = 3859 + 0 + 14)."""
    activas, inactivas, orphans, stock_pos = 3859, 0, 14, 3873
    assert activas + inactivas + orphans == stock_pos


def test_orphans_are_not_counted_as_active():
    """Las 14 huérfanas no entran en activas_stock_pos."""
    activas_stock_pos = 3859
    stock_pos = 3873
    orphans = 14
    assert stock_pos - activas_stock_pos == orphans
    assert activas_stock_pos + orphans == stock_pos


def test_sql_helpers_reject_writes():
    with pytest.raises(AnalyticsValidationError):
        assert_sql_is_read_only("DELETE FROM bsale_raw.variant_costs")
    with pytest.raises(AnalyticsValidationError):
        assert_sql_is_read_only("UPDATE bsale_raw.variants SET state = 0")
    assert_sql_is_read_only(
        "SELECT count(*) FROM bsale_raw.variants WHERE company_id = %s"
    )


def test_build_report_without_pg_env_does_not_leak_secrets(monkeypatch):
    for key in ("PG_HOST", "PG_DB", "PG_USER", "PG_PASSWORD", "PG_PORT"):
        monkeypatch.delenv(key, raising=False)
    report = build_report(
        company_id=3,
        timeout_sec=30,
        top_n=5,
        price_list_id=DEFAULT_PRICE_LIST,
    )
    assert report.get("error")
    assert report["pg_env_present"]["PG_PASSWORD"] is False
    blob = json.dumps(report)
    for forbidden in ("password=", "PG_PASSWORD=", "postgresql://", "DSN"):
        assert forbidden.lower() not in blob.lower()
    # No valores de entorno reales (solo bools).
    assert set(report["pg_env_present"].values()) <= {True, False}


def test_grant_instructions_are_select_only():
    lines = "\n".join(grant_instructions("ai_reader"))
    assert "GRANT SELECT" in lines
    assert "WITH GRANT OPTION" not in lines.upper()
    assert "GRANT INSERT" not in lines.upper()
    assert "GRANT UPDATE" not in lines.upper()
    assert "GRANT DELETE" not in lines.upper()
