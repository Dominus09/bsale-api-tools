"""Fase 2.7 — stock operativo vs stock total Bsale."""

from __future__ import annotations

from decimal import Decimal

import pytest

from backend.schemas.cost_v2_company_read import COST_CONTROL_OFFICE_IDS_BY_COMPANY
from backend.services.analytics_v2.stock_scope import (
    AUXILIARY_ONLY_LEGEND,
    DEFAULT_STOCK_FILTER,
    OPERATIONAL_OFFICE_IDS_BY_COMPANY,
    STOCK_SCOPE_BANNER,
    has_operational_office_scope,
    index_stock_totals,
    matches_stock_filter,
    operational_office_ids,
    pack_availability_from_components,
    reconcile_stock_scopes,
    resolve_stock_entry,
    stock_cte_sql,
    stock_filter_sql_clause,
    stock_join_subquery,
    stock_totals_query,
)

D = Decimal


def test_config_centralized_company_3_only():
    assert OPERATIONAL_OFFICE_IDS_BY_COMPANY is COST_CONTROL_OFFICE_IDS_BY_COMPANY
    assert operational_office_ids(3) == (1, 3, 4, 5)
    assert 2 not in operational_office_ids(3)
    assert 6 not in operational_office_ids(3)
    assert operational_office_ids(99) is None
    assert has_operational_office_scope(3) is True
    assert has_operational_office_scope(1) is False


def test_stock_totals_query_dual_vs_passthrough():
    sql3, params3 = stock_totals_query(3)
    assert "FILTER" in sql3 and "ANY" in sql3
    assert params3[0] == [1, 3, 4, 5]
    assert params3[-1] == 3
    sql_other, params_other = stock_totals_query(99)
    assert "FILTER" not in sql_other
    assert params_other == (99,)


def _index_company3(rows):
    return index_stock_totals(rows, company_id=3)


def test_a_stock_office_1_only():
    stocks = _index_company3(
        [{"variant_id": 10, "stock_total": D("40"), "stock_total_bsale": D("40"), "stock_offices": 1}]
    )
    entry = resolve_stock_entry(stocks, 10)
    assert entry["state"] == "con_stock"
    assert entry["total"] == "40"
    assert entry["total_bsale"] == "40"
    assert matches_stock_filter(state=entry["state"], stock_filter="with")


def test_b_stock_split_operational_offices():
    stocks = _index_company3(
        [{"variant_id": 11, "stock_total": D("100"), "stock_total_bsale": D("100"), "stock_offices": 4}]
    )
    assert matches_stock_filter(total=stocks[11]["total"], stock_filter="with")


def test_c_only_bodega_1_auxiliary():
    stocks = _index_company3(
        [{"variant_id": 12, "stock_total": D("0"), "stock_total_bsale": D("100"), "stock_offices": 1}]
    )
    entry = resolve_stock_entry(stocks, 12)
    assert entry["total"] == "0"
    assert entry["total_bsale"] == "100"
    assert entry["auxiliary_only"] is True
    assert entry["legend"] == AUXILIARY_ONLY_LEGEND
    assert not matches_stock_filter(state=entry["state"], stock_filter="with")
    assert matches_stock_filter(state=entry["state"], stock_filter="without")


def test_d_only_bajas_auxiliary():
    stocks = _index_company3(
        [{"variant_id": 13, "stock_total": D("0"), "stock_total_bsale": D("25"), "stock_offices": 1}]
    )
    entry = resolve_stock_entry(stocks, 13)
    assert entry["auxiliary_only"] is True
    assert matches_stock_filter(state=entry["state"], stock_filter="without")


def test_e_operational_plus_auxiliary():
    stocks = _index_company3(
        [{"variant_id": 14, "stock_total": D("50"), "stock_total_bsale": D("80"), "stock_offices": 3}]
    )
    entry = resolve_stock_entry(stocks, 14)
    assert entry["total"] == "50"
    assert entry["total_bsale"] == "80"
    assert entry.get("auxiliary_only") is False
    assert matches_stock_filter(state=entry["state"], stock_filter="with")


def test_f_negative_operational():
    stocks = _index_company3(
        [{"variant_id": 15, "stock_total": D("-3"), "stock_total_bsale": D("-3"), "stock_offices": 1}]
    )
    entry = resolve_stock_entry(stocks, 15)
    assert entry["state"] == "negativo"
    assert matches_stock_filter(state=entry["state"], stock_filter="without")


def test_g_negative_auxiliary_only():
    stocks = _index_company3(
        [{"variant_id": 16, "stock_total": D("0"), "stock_total_bsale": D("-8"), "stock_offices": 1}]
    )
    entry = resolve_stock_entry(stocks, 16)
    assert entry["total"] == "0"
    assert entry["total_bsale"] == "-8"
    assert matches_stock_filter(state=entry["state"], stock_filter="without")


def test_h_compensation_within_operational():
    # +10 y -10 en oficinas operativas → saldo 0 → SIN STOCK OPERATIVO
    assert matches_stock_filter(total=D("0"), stock_filter="without")
    assert not matches_stock_filter(total=D("0"), stock_filter="with")


def test_i_no_stock_rows():
    entry = resolve_stock_entry({}, 99)
    assert entry["state"] == "sin_sincronizar"
    assert entry["total"] is None
    assert entry["total_bsale"] is None
    assert matches_stock_filter(state=None, stock_filter="without")
    assert not matches_stock_filter(state=None, stock_filter="with")


def test_j_other_company_passthrough():
    stocks = index_stock_totals(
        [{"variant_id": 1, "stock_total": D("7"), "stock_offices": 2}],
        company_id=99,
    )
    entry = resolve_stock_entry(stocks, 1)
    assert entry["total"] == "7"
    assert entry["total_bsale"] == "7"
    assert entry["scope"] == "total_bsale"
    sql, params = stock_join_subquery(99)
    assert "FILTER" not in sql
    assert params == [99]


def test_o_no_duplicate_by_office_in_index():
    rows = [
        {"variant_id": 1, "stock_total": D("5"), "stock_total_bsale": D("12"), "stock_offices": 3},
    ]
    stocks = _index_company3(rows)
    assert list(stocks.keys()) == [1]


def test_m_packs_use_operational_components():
    stocks = _index_company3(
        [
            {"variant_id": 1, "stock_total": D("0"), "stock_total_bsale": D("100"), "stock_offices": 1},
            {"variant_id": 2, "stock_total": D("20"), "stock_total_bsale": D("20"), "stock_offices": 1},
        ]
    )
    # Componente 1 solo auxiliar → aporta 0 armables
    out = pack_availability_from_components({1: D("1"), 2: D("1")}, stocks)
    assert D(out["available_units"] or "0") == 0
    assert out["scope"] == "operativo_componentes"


def test_reconcile_and_banner():
    stocks = _index_company3(
        [
            {"variant_id": 1, "stock_total": D("10"), "stock_total_bsale": D("10"), "stock_offices": 1},
            {"variant_id": 2, "stock_total": D("0"), "stock_total_bsale": D("5"), "stock_offices": 1},
        ]
    )
    recon = reconcile_stock_scopes(stocks)
    assert recon["variants_positive_operational"] == 1
    assert recon["variants_positive_total_bsale"] == 2
    assert recon["variants_auxiliary_only_positive"] == 1
    assert STOCK_SCOPE_BANNER.startswith("ALCANCE:")
    assert DEFAULT_STOCK_FILTER == "with"
    assert "stock_total" in (stock_filter_sql_clause("st", stock_filter="with") or "")


def test_cte_params_order():
    cte, params = stock_cte_sql(3)
    assert "stock_total_bsale" in cte
    assert params[0] == [1, 3, 4, 5]
    assert params[-1] == 3
