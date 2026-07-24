"""Tests resolvedor costo bruto máximo válido (sin PG)."""

from __future__ import annotations

from datetime import date

from backend.services.analytics.max_valid_gross_cost import (
    GrossCostCandidate,
    resolve_max_valid_gross_cost,
)
from backend.services.analytics.money import D


def test_max_valid_prefers_highest_bruto_erp():
    as_of = date(2026, 7, 20)
    res = resolve_max_valid_gross_cost(
        [
            GrossCostCandidate(
                gross_cost=D("1190"),
                cost_date=date(2026, 6, 1),
                reception_type="recepcion_normal",
            ),
            GrossCostCandidate(
                gross_cost=D("1395"),
                cost_date=date(2026, 7, 1),
                reception_type="recepcion_normal",
            ),
        ],
        as_of=as_of,
    )
    assert res.gross_cost == D("1395.0000")
    assert res.min_gross_among_valid == D("1190.0000")
    assert res.max_gross_among_valid == D("1395.0000")
    assert res.gross_cost_quality == "actual_purchase_gross"
    assert res.resolution_reason.startswith("max_valid_purchase_gross")


def test_excludes_nc_and_uses_fallback():
    as_of = date(2026, 7, 20)
    res = resolve_max_valid_gross_cost(
        [
            GrossCostCandidate(
                gross_cost=D("2000"),
                cost_date=date(2026, 7, 1),
                reception_type="recepcion_nc",
            ),
        ],
        as_of=as_of,
        fallback=GrossCostCandidate(
            gross_cost=D("1100"),
            cost_date=date(2026, 7, 15),
            cost_source="variant_cost.average_cost_gross",
        ),
    )
    assert res.gross_cost == D("1100.0000")
    assert res.resolution_reason == "variant_cost_fallback"


def test_flags_outlier_without_auto_correct():
    as_of = date(2026, 7, 20)
    res = resolve_max_valid_gross_cost(
        [
            GrossCostCandidate(gross_cost=D("1000"), cost_date=date(2026, 5, 1)),
            GrossCostCandidate(gross_cost=D("1050"), cost_date=date(2026, 6, 1)),
            GrossCostCandidate(gross_cost=D("5000"), cost_date=date(2026, 7, 1)),
        ],
        as_of=as_of,
    )
    assert res.gross_cost == D("5000.0000")
    assert res.is_outlier is True
    assert "outlier" in res.resolution_reason


def test_reconstruct_from_taxes_when_no_bruto():
    as_of = date(2026, 7, 20)
    res = resolve_max_valid_gross_cost(
        [
            GrossCostCandidate(
                gross_cost=None,
                net_cost=D("1000"),
                iva_amount=D("190"),
                other_taxes=D("205"),
                cost_date=date(2026, 7, 1),
            ),
        ],
        as_of=as_of,
    )
    assert res.gross_cost == D("1395.0000")
    assert res.gross_cost_quality == "reconstructed_from_actual_taxes"
