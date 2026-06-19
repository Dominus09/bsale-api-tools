"""Regresión SQL sync related (PostgreSQL SELECT DISTINCT + ORDER BY)."""

from __future__ import annotations

import inspect
import re

from backend.services.distribuidora import sync_related_service as srs


def _select_list_columns(select_sql: str) -> str:
    """Fragmento entre SELECT [DISTINCT ...] y FROM (minúsculas)."""
    m = re.search(
        r"select\s+(?:distinct\s+(?:on\s*\([^)]+\)\s*)?)?(.+?)\s+from\s",
        select_sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    return (m.group(1) if m else "").lower()


def _order_by_expressions(order_sql: str) -> list[str]:
    m = re.search(r"order\s+by\s+(.+?)(?:\s+limit\s|\s*$)", order_sql, flags=re.I | re.S)
    if not m:
        return []
    chunk = m.group(1)
    parts = re.split(r",(?![^()]*\))", chunk)
    out: list[str] = []
    for part in parts:
        expr = part.strip().lower()
        expr = re.sub(r"\s+nulls\s+(first|last)\s*$", "", expr, flags=re.I)
        expr = re.sub(r"\s+(asc|desc)\s*$", "", expr, flags=re.I)
        if expr:
            out.append(expr)
    return out


def assert_pg_distinct_order_compatible(sql: str) -> None:
    """
    PostgreSQL: con SELECT DISTINCT, cada expresión de ORDER BY debe aparecer
    en la lista del SELECT.
    """
    if not re.search(r"\bselect\s+distinct\b", sql, flags=re.I):
        return
    select_cols = _select_list_columns(sql)
    for expr in _order_by_expressions(sql):
        bare = expr.split(".")[-1]
        if bare not in select_cols and expr not in select_cols:
            raise AssertionError(
                f"ORDER BY {expr!r} no está en SELECT DISTINCT: {select_cols!r}"
            )


def _incremental_fetch_sql_blocks() -> list[str]:
    src = inspect.getsource(srs._fetch_oc_document_ids_for_incremental)
    return re.findall(r'"""(.*?)"""', src, flags=re.DOTALL)


def test_incremental_oc_pick_queries_pg_distinct_order():
    for block in _incremental_fetch_sql_blocks():
        assert_pg_distinct_order_compatible(block)


def test_incremental_pending_orders_by_emission_date():
    blocks = _incremental_fetch_sql_blocks()
    assert any("order by d.emission_date asc nulls last" in b.lower() for b in blocks)


def test_emission_day_fetch_has_no_distinct():
    src = inspect.getsource(srs._fetch_oc_document_ids_for_emission_day)
    assert "select distinct" not in src.lower()
