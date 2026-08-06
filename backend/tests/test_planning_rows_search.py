"""Búsqueda backend planning-rows: folio / cliente / paginación filtrada."""

from __future__ import annotations

from backend.services.distribuidora.orders_service import (
    _planning_rows_ids_sql,
    _planning_rows_search_clause,
    resolve_planning_rows_search,
)


def test_search_digits_resolves_to_exact_folio():
    r = resolve_planning_rows_search(search="68513")
    assert r["order_number"] == 68513
    assert r["customer_name"] is None
    assert r["active"] is True


def test_search_text_resolves_to_customer():
    r = resolve_planning_rows_search(search="Callo")
    assert r["order_number"] is None
    assert r["customer_name"] == "Callo"


def test_order_number_param_has_priority_over_search_text():
    r = resolve_planning_rows_search(search="Callo", order_number=68513)
    assert r["order_number"] == 68513
    assert r["customer_name"] == "Callo"  # explícitos se mantienen


def test_folio_exact_sql_outside_first_page():
    """Folio exacto filtra en SQL (no depende de offset de la 1ª página)."""
    resolved = resolve_planning_rows_search(search="68513")
    sql, params = _planning_rows_ids_sql(
        day_tokens=(),
        only_not_invoiced=False,
        search=resolved,
    )
    assert "d.number = %s" in sql
    assert 68513 in params
    assert "LIMIT %s OFFSET %s" in sql
    # offset se aplica sobre el resultado ya filtrado (params: search…, limit, offset)
    assert params == [68513]


def test_customer_search_sql_accent_insensitive_and_join():
    resolved = resolve_planning_rows_search(search="Calló")
    sql, params = _planning_rows_ids_sql(
        day_tokens=(),
        only_not_invoiced=False,
        search=resolved,
    )
    assert "c_search" in sql
    assert "translate(lower" in sql
    assert "áéíóúü" in sql
    assert params == ["Calló"]
    assert "LIKE" in sql


def test_search_works_with_invoiced_included():
    resolved = resolve_planning_rows_search(order_number=68513)
    sql, _ = _planning_rows_ids_sql(
        day_tokens=(),
        only_not_invoiced=False,
        search=resolved,
    )
    assert "EXISTS" not in sql  # false → incluye facturadas
    assert "d.number = %s" in sql


def test_clear_search_restores_unfiltered_ids_sql():
    resolved_empty = resolve_planning_rows_search(search="")
    assert resolved_empty["active"] is False
    sql, params = _planning_rows_ids_sql(
        day_tokens=(),
        only_not_invoiced=False,
        search=resolved_empty,
    )
    assert "d.number = %s" not in sql
    assert "c_search" not in sql
    assert params == []


def test_pagination_params_not_mixed_into_search_clause():
    """LIMIT/OFFSET no se confunden con parámetros de búsqueda."""
    resolved = resolve_planning_rows_search(customer_name="Callo", seller_name="Juan")
    clause, params, needs_client = _planning_rows_search_clause(resolved)
    assert needs_client is True
    assert "AND" in clause
    assert params == ["Callo", "Juan"]
    sql, search_params = _planning_rows_ids_sql(
        day_tokens=(),
        only_not_invoiced=False,
        search=resolved,
    )
    assert search_params == ["Callo", "Juan"]
    assert sql.strip().endswith("LIMIT %s OFFSET %s")
