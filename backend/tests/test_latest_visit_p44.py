"""P4 hotfix: latest_visit / last_closed_visit lectura sin mutar."""

from __future__ import annotations

import inspect

from backend.repositories.distribuidora import delivery_visit_repo as repo
from backend.services.distribuidora import delivery_visit_service as svc


def test_repo_exposes_get_latest_visit():
    src = inspect.getsource(repo.get_latest_visit)
    assert "ORDER BY id DESC" in src
    assert "in_progress" not in src.split("WHERE", 1)[-1].split("ORDER", 1)[0]


def test_repo_last_closed_excludes_in_progress():
    src = inspect.getsource(repo.get_last_closed_visit)
    assert "status <> 'in_progress'" in src or 'status <> "in_progress"' in src
    assert "ORDER BY id DESC" in src


def test_service_exposes_closed_and_latest():
    assert callable(svc.get_latest_visit_for_client)
    assert callable(svc.get_last_closed_visit_for_client)


def test_dual_visit_contract_in_ficha_payload_source():
    """Regresión: ficha debe poder exponer activa + cerrada a la vez."""
    from backend.services.distribuidora import (
        dispatch_plan_pwa_driver_read_service as read_svc,
    )

    src = inspect.getsource(read_svc.get_client_ficha_for_driver)
    assert "last_closed_visit" in src
    assert "active_visit" in src
    assert "latest_visit" in src
