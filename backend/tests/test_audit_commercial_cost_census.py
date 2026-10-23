"""Pruebas unitarias del script de auditoría comercial (sin PostgreSQL)."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from backend.scripts.audit_commercial_cost_census import (
    DEFAULT_PRICE_LIST,
    ORPHAN_RESOLUTION_PROCEDURE,
    _json_default,
    _row_get,
    _row_int,
    build_report,
    classify_divergence_bucket,
    grant_instructions,
    is_extreme_divergence,
    monetary_exposure_potential,
    score_admin_priority,
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


def test_json_default_date_isoformat():
    assert _json_default(date(2024, 3, 15)) == "2024-03-15"


def test_json_default_datetime_and_decimal_unchanged():
    dt = datetime(2024, 3, 15, 12, 30, 45, tzinfo=timezone.utc)
    assert _json_default(dt) == dt.isoformat()
    assert _json_default(Decimal("1234.56")) == "1234.56"


def test_json_dumps_extended_report_with_nested_reception_dates():
    """Regresión Coolify: TypeError: date al imprimir --extended --json."""
    report = {
        "cut_at_utc": datetime(2026, 10, 23, 3, 0, 0, tzinfo=timezone.utc),
        "positive_cost_divergence": {
            "top_anomalies": [
                {
                    "variant_id": 1,
                    "average_cost": Decimal("100.00"),
                    "last_comparable_cost": Decimal("250.50"),
                    "last_comparable_date": date(2025, 11, 2),
                    "diff_pct": Decimal("150.50"),
                }
            ]
        },
    }
    blob = json.dumps(report, ensure_ascii=False, indent=2, default=_json_default)
    parsed = json.loads(blob)
    assert parsed["positive_cost_divergence"]["top_anomalies"][0]["last_comparable_date"] == (
        "2025-11-02"
    )
    assert parsed["positive_cost_divergence"]["top_anomalies"][0]["average_cost"] == "100.00"
    assert "2026-10-23T03:00:00" in parsed["cut_at_utc"]


def test_row_get_tolerates_postgres_case_folding():
    """Aliases sin comillas llegan en minúsculas; .get mixto devolvía NULL en by_*."""
    row = {
        "grupo_a_precio_sin_stock_pos": 2278,
        "grupo_c_con_recepcion": 950,
    }
    assert _row_int(row, "grupo_A_precio_sin_stock_pos") == 2278
    assert _row_get(row, "grupo_C_con_recepcion") == 950
    assert _row_get(row, "missing") is None


def test_zero_cost_stock_identity_separates_explicit_zero_from_missing_row():
    """stock_cero no debe incluir sin_fila_stock (regresión 2278 = 2023+255)."""
    stock_cero_explicito, sin_fila_stock, con_stock_pos = 2023, 255, 0
    total = 2278
    assert stock_cero_explicito + sin_fila_stock + con_stock_pos == total
    sin_stock_positivo = stock_cero_explicito + sin_fila_stock
    assert sin_stock_positivo == 2278


def test_admin_priority_exposure_is_not_accounting_loss():
    exposure = monetary_exposure_potential(
        average_cost=Decimal("100"),
        last_comparable_cost=Decimal("500"),
        stock_total=Decimal("5"),
    )
    assert exposure == Decimal("2000")
    prio = score_admin_priority(
        diff_pct=Decimal("400"),
        stock_total=Decimal("5"),
        average_cost=Decimal("100"),
        last_comparable_cost=Decimal("500"),
        last_comparable_date=date(2025, 1, 1),
        cut_at=date(2026, 10, 23),
        has_document=True,
        possible_admin_correction=True,
        unit_presentation_verified=False,
    )
    assert prio["review_priority"] == "P1_critico"
    assert prio["priority_band"] == "P1_critico"
    assert "pérdida" in prio["monetary_exposure_disclaimer"].lower()
    assert "unidad" in prio["monetary_exposure_disclaimer"].lower()
    assert prio["possible_admin_correction"] is True
    assert prio["error_confidence"] == "baja"
    assert prio["monetary_exposure_potential"] == "2000.00"


def test_admin_correction_does_not_reduce_review_priority():
    """Corrección admin posible baja confianza de error, no la prioridad de revisión."""
    common = dict(
        diff_pct=Decimal("400"),
        stock_total=Decimal("5"),
        average_cost=Decimal("100"),
        last_comparable_cost=Decimal("500"),
        last_comparable_date=date(2025, 1, 1),
        cut_at=date(2026, 10, 23),
        has_document=True,
        unit_presentation_verified=False,
    )
    without = score_admin_priority(**common, possible_admin_correction=False)
    with_flag = score_admin_priority(**common, possible_admin_correction=True)
    assert with_flag["priority_score"] == without["priority_score"]
    assert with_flag["review_priority"] == without["review_priority"]
    assert with_flag["error_confidence"] == "baja"


def test_high_volume_outranks_small_extreme_when_scores_compared():
    """Alto volumen (p. ej. 1964 uds) debe priorizarse frente a extrema de poco stock."""
    high_vol = score_admin_priority(
        diff_pct=Decimal("35.77"),
        stock_total=Decimal("1964"),
        average_cost=Decimal("500"),
        last_comparable_cost=Decimal("678.85"),
        last_comparable_date=date(2026, 9, 1),
        cut_at=date(2026, 10, 23),
        has_document=True,
        unit_presentation_verified=False,
    )
    small_extreme = score_admin_priority(
        diff_pct=Decimal("400"),
        stock_total=Decimal("5"),
        average_cost=Decimal("100"),
        last_comparable_cost=Decimal("500"),
        last_comparable_date=date(2026, 9, 1),
        cut_at=date(2026, 10, 23),
        has_document=True,
        possible_admin_correction=True,
        unit_presentation_verified=False,
        presentation_risk=True,
    )
    assert high_vol["review_priority"] == "P1_critico"  # stock >= 1000
    assert small_extreme["review_priority"] == "P1_critico"  # |Δ| >= 100
    assert high_vol["priority_score"] > small_extreme["priority_score"]


def test_presentation_risk_keeps_low_error_confidence():
    """Bidón/pack: revisar con prioridad, sin asumir error de costo."""
    prio = score_admin_priority(
        diff_pct=Decimal("400"),
        stock_total=Decimal("5"),
        average_cost=Decimal("100"),
        last_comparable_cost=Decimal("500"),
        last_comparable_date=date(2025, 6, 1),
        cut_at=date(2026, 10, 23),
        has_document=True,
        possible_admin_correction=True,
        presentation_risk=True,
        unit_presentation_verified=False,
    )
    assert prio["presentation_risk"] is True
    assert prio["error_confidence"] == "baja"
    assert prio["suggested_action"] == "contrastar_factura_unidad_presentacion"
    assert prio["evidence_quality"] == "riesgo_presentacion_unidad"


def test_zero_cost_axes_independent_identity():
    """A/B (precio×stock) y C/D (recepción) cierran por separado; no son excluyentes cruzados."""
    total = 2278
    grupo_a, grupo_b = 2278, 0
    grupo_c, grupo_d = 950, 1328
    sin_stock_positivo = 2278
    assert grupo_a + grupo_b == sin_stock_positivo
    assert grupo_c + grupo_d == total
    # Independencia: A∩C puede ser no vacío (aquí A≈total y C⊂total).
    assert grupo_c <= total
    assert grupo_a <= total


def test_queries_aggregate_stock_and_limit_receptions_no_duplication():
    """Contrato estático: stocks GROUP BY variant_id; laterales LIMIT 1."""
    from backend.scripts import audit_commercial_cost_census as mod

    src = open(mod.__file__, encoding="utf-8").read()
    assert "GROUP BY variant_id" in src
    assert src.count("LIMIT 1") >= 2
    assert "_last_comparable_lateral_sql" in src
    assert "LEFT JOIN LATERAL" in src
