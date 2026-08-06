"""Tests SQL de planning-rows (templates renderizados)."""

import pytest

from backend.services.distribuidora.orders_service import (
    _assert_sql_template_rendered,
    _planning_rows_base_orders_sql,
    _planning_rows_enrich_sql,
    _planning_rows_ids_sql,
)


def _ids_sql(**kwargs):
    sql, _params = _planning_rows_ids_sql(**kwargs)
    return sql


def test_planning_rows_base_orders_sql_uses_weight_placeholder_not_lateral():
    sql = _planning_rows_base_orders_sql()
    _assert_sql_template_rendered(sql, context="base_orders")
    assert "{_PLANNING_ROWS_WEIGHT_LATERAL}" not in sql
    assert "NULL::numeric AS peso_total_kg" in sql
    assert "NULL::numeric AS weight_kg" in sql
    assert "productos_sin_peso" in sql
    assert "COALESCE(pl.weight_unit_kg, 0)" not in sql


def test_planning_rows_enrich_sql_uses_weight_placeholder_not_lateral():
    sql = _planning_rows_enrich_sql()
    _assert_sql_template_rendered(sql, context="enrich")
    assert "{_PLANNING_ROWS_WEIGHT_LATERAL}" not in sql
    assert "NULL::numeric AS peso_total_kg" in sql
    assert "COALESCE(pl.weight_unit_kg, 0)" not in sql


def test_planning_rows_ids_sql_renders_day_clause():
    sql = _ids_sql(day_tokens=())
    _assert_sql_template_rendered(sql, context="ids")
    assert "{day_clause}" not in sql


def test_planning_rows_sql_templates_all_render_without_placeholders():
    """Evita NameError / placeholders sin reemplazar en producción."""
    for name, builder in (
        ("base_orders", _planning_rows_base_orders_sql),
        ("enrich", _planning_rows_enrich_sql),
        ("ids", lambda: _ids_sql(day_tokens=())),
    ):
        sql = builder()
        _assert_sql_template_rendered(sql, context=name)
        assert "{PLANNING_WEIGHT_SELECT}" not in sql, name
        assert "PLANNING_WEIGHT_SELECT" not in sql, name


def test_assert_sql_template_rendered_raises_on_literal_placeholder():
    with pytest.raises(RuntimeError, match="PLANNING_WEIGHT_LATERAL"):
        _assert_sql_template_rendered(
            "SELECT 1 FROM t {PLANNING_WEIGHT_LATERAL}",
            context="test",
        )


def test_planning_rows_ids_respects_only_not_invoiced_param():
    """only_not_invoiced=false no aplica EXISTS; true sí excluye facturadas."""
    sql_false = _ids_sql(day_tokens=(), only_not_invoiced=False)
    sql_true = _ids_sql(day_tokens=(), only_not_invoiced=True)
    assert "EXISTS" not in sql_false
    assert "EXISTS" in sql_true
    assert "document_type_id = 33" in sql_true or "document_type_id=33" in sql_true.replace(
        " ", ""
    )
