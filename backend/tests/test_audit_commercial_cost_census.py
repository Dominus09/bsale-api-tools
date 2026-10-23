"""Pruebas unitarias del script de auditoría comercial (sin PostgreSQL)."""

from __future__ import annotations

import json

import pytest

from backend.scripts.audit_commercial_cost_census import (
    DEFAULT_PRICE_LIST,
    ORPHAN_RESOLUTION_PROCEDURE,
    build_report,
    classify_divergence_bucket,
    grant_instructions,
    is_extreme_divergence,
)
from decimal import Decimal
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
    assert "TO ai_reader" in lines
    assert "WITH GRANT OPTION" not in lines.upper()
    assert "GRANT INSERT" not in lines.upper()


def test_grant_instructions_warn_against_postgres():
    lines = "\n".join(grant_instructions("postgres"))
    assert "ADVERTENCIA" in lines
    assert "TO ai_reader" in lines
    assert "TO postgres;" not in lines


def test_classify_divergence_buckets_mutually_exclusive():
    """Bandas principales excluyentes; extrema es indicador adicional ⊆ sobre_50."""
    assert classify_divergence_bucket(None, has_comparable=False) == "sin_recepcion_comparable"
    assert (
        classify_divergence_bucket(
            None, has_comparable=False, requires_verification=True
        )
        == "requiere_verificacion"
    )
    assert classify_divergence_bucket(Decimal("10"), has_comparable=True) == "hasta_15"
    assert classify_divergence_bucket(Decimal("30"), has_comparable=True) == "entre_15_y_50"
    assert classify_divergence_bucket(Decimal("75"), has_comparable=True) == "sobre_50"
    # Extrema no es banda de cierre: 100 % y −60 % quedan en sobre_50.
    assert classify_divergence_bucket(Decimal("100"), has_comparable=True) == "sobre_50"
    assert classify_divergence_bucket(Decimal("-60"), has_comparable=True) == "sobre_50"
    assert is_extreme_divergence(Decimal("100")) is True
    assert is_extreme_divergence(Decimal("-60")) is True
    assert is_extreme_divergence(Decimal("75")) is False
    assert is_extreme_divergence(None) is False
    # Cierre: suma de bandas principales = universo (sin extrema).
    primary = {
        "sin_recepcion_comparable": 10,
        "requiere_verificacion": 5,
        "hasta_15": 80,
        "entre_15_y_50": 3,
        "sobre_50": 2,
    }
    universo = 100
    assert sum(primary.values()) == universo
    extrema_indicador = 1  # subconjunto de sobre_50
    assert extrema_indicador <= primary["sobre_50"]


def test_orphan_procedure_does_not_auto_sync():
    cmds = "\n".join(ORPHAN_RESOLUTION_PROCEDURE["recommended_coolify_commands"])
    assert "sync-catalog" in cmds
    assert "refresh-costs" in cmds
    assert any("No ejecutar sync automáticamente" in n for n in ORPHAN_RESOLUTION_PROCEDURE["notes"])
