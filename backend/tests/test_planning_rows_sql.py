"""Tests SQL de planning-rows (templates renderizados)."""

import pytest

from backend.services.distribuidora.orders_service import (
    _assert_sql_template_rendered,
    _planning_rows_base_orders_sql,
    _planning_rows_enrich_sql,
    _planning_rows_ids_sql,
)


def test_planning_rows_base_orders_sql_renders_weight_lateral():
    sql = _planning_rows_base_orders_sql()
    _assert_sql_template_rendered(sql, context="base_orders")
    assert "{_PLANNING_ROWS_WEIGHT_LATERAL}" not in sql
    assert "LEFT JOIN LATERAL" in sql
    assert "COALESCE(pl.weight_unit_kg, 0)" in sql
    assert "productos_sin_peso" in sql


def test_planning_rows_enrich_sql_renders_weight_lateral():
    sql = _planning_rows_enrich_sql()
    _assert_sql_template_rendered(sql, context="enrich")
    assert "{_PLANNING_ROWS_WEIGHT_LATERAL}" not in sql
    assert "LEFT JOIN LATERAL" in sql


def test_planning_rows_ids_sql_renders_day_clause():
    sql = _planning_rows_ids_sql(day_tokens=())
    _assert_sql_template_rendered(sql, context="ids")
    assert "{day_clause}" not in sql


def test_assert_sql_template_rendered_raises_on_literal_placeholder():
    with pytest.raises(RuntimeError, match="_PLANNING_ROWS_WEIGHT_LATERAL"):
        _assert_sql_template_rendered(
            "SELECT 1 FROM t {_PLANNING_ROWS_WEIGHT_LATERAL}",
            context="test",
        )
