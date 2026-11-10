"""P4 hotfix: latest_visit lectura sin mutar."""

from __future__ import annotations

import inspect

from backend.repositories.distribuidora import delivery_visit_repo as repo
from backend.services.distribuidora import delivery_visit_service as svc


def test_repo_exposes_get_latest_visit():
    src = inspect.getsource(repo.get_latest_visit)
    assert "ORDER BY id DESC" in src
    assert "in_progress" not in src.split("WHERE", 1)[-1].split("ORDER", 1)[0]


def test_service_exposes_get_latest_visit_for_client():
    assert callable(svc.get_latest_visit_for_client)
