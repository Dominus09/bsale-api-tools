"""Cabecera del plan expone origen y flag ORS para la UI."""

from __future__ import annotations


def test_get_plan_header_sql_includes_ors_flags():
    import inspect

    from backend.repositories.distribuidora import dispatch_plan_repo as repo

    src = inspect.getsource(repo.get_plan_header)
    assert "planning_origin" in src
    assert "ors_metrics_calculated" in src
