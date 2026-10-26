"""Fase 2.4 — filtro de stock global (SUM quantity por company+variant)."""

from __future__ import annotations

from decimal import Decimal

import pytest

from backend.services.analytics_v2.stock_scope import (
    DEFAULT_STOCK_FILTER,
    STOCK_FILTER_ALL,
    STOCK_FILTER_WITH,
    STOCK_FILTER_WITHOUT,
    matches_stock_filter,
    normalize_stock_filter,
    pack_availability_from_components,
    stock_filter_sql_clause,
    stock_payload,
    stock_state_from_total,
)


D = Decimal


def test_normalize_default_is_con_stock():
    assert normalize_stock_filter(None) == DEFAULT_STOCK_FILTER == STOCK_FILTER_WITH
    assert normalize_stock_filter("") == STOCK_FILTER_WITH
    assert normalize_stock_filter("all") == STOCK_FILTER_ALL
    assert normalize_stock_filter("con_stock") == STOCK_FILTER_WITH
    assert normalize_stock_filter("sin_stock") == STOCK_FILTER_WITHOUT
    assert normalize_stock_filter("todos") == STOCK_FILTER_ALL
    with pytest.raises(ValueError):
        normalize_stock_filter("poco")


def test_states_and_payload():
    assert stock_state_from_total(None) == "sin_sincronizar"
    assert stock_state_from_total(D("0")) == "cero"
    assert stock_state_from_total(D("-2")) == "negativo"
    assert stock_state_from_total(D("85")) == "con_stock"
    assert stock_payload(D("85"), offices=4)["offices"] == 4
    assert stock_payload(None)["total"] is None


def test_filter_with_without_all():
    assert matches_stock_filter(state="con_stock", stock_filter="with")
    assert not matches_stock_filter(state="cero", stock_filter="with")
    assert not matches_stock_filter(state="sin_sincronizar", stock_filter="with")
    assert matches_stock_filter(state="cero", stock_filter="without")
    assert matches_stock_filter(state="negativo", stock_filter="without")
    assert matches_stock_filter(state="sin_sincronizar", stock_filter="without")
    assert not matches_stock_filter(state="con_stock", stock_filter="without")
    assert matches_stock_filter(state="sin_sincronizar", stock_filter="all")
    assert matches_stock_filter(total=D("50"), stock_filter="with")
    assert matches_stock_filter(total=D("-1"), stock_filter="without")
    # Compensación entre oficinas: el saldo global importa, no oficina a oficina.
    assert matches_stock_filter(total=D("0"), stock_filter="without")  # 10 + (-10) = 0


def test_sql_clause():
    assert stock_filter_sql_clause("st", stock_filter="with") == "st.stock_total > 0"
    assert "IS NULL" in (stock_filter_sql_clause("st", stock_filter="without") or "")
    assert stock_filter_sql_clause("st", stock_filter="all") is None


def test_pack_availability_from_components():
    stocks = {
        1: {"total": D("50"), "offices": 2},
        2: {"total": D("25"), "offices": 1},
        3: {"total": D("10"), "offices": 1},
    }
    # min(floor(50/2), floor(25/1)) = min(25, 25) = 25
    out = pack_availability_from_components({1: D("2"), 2: D("1")}, stocks)
    assert out["state"] == "con_stock"
    assert out["available_units"] == "25"
    assert out["derived"] is True
    assert out["scope"] == "operativo_componentes"
    assert "sucursal" in (out.get("label") or "").lower()

    # Componente sin fila → 0 disponibles
    sin = pack_availability_from_components({1: D("1"), 99: D("1")}, stocks)
    assert sin["state"] in ("cero", "sin_sincronizar") or D(sin["available_units"] or "0") == 0

    # Negativo en un componente
    neg = pack_availability_from_components({1: D("1")}, {1: {"total": D("-3"), "offices": 1}})
    assert neg["state"] == "negativo"
