"""Hotfix: router ERP visitas usa require_erp_ops_staff (no require_app ficticio)."""

from __future__ import annotations

import inspect

from fastapi import HTTPException
import pytest

from backend.auth.permissions import require_erp_ops_staff
from backend.routers import distribuidora_delivery_visits_erp as erp_router


def test_router_depends_on_require_erp_ops_staff():
    src = inspect.getsource(erp_router)
    assert "require_app" not in src
    assert "require_erp_ops_staff" in src
    # Router-level dependency
    dep_calls = erp_router.router.dependencies
    assert dep_calls, "router debe exigir auth a nivel de APIRouter"
    # La dependencia resuelve a la misma callable operativa ERP
    callables = [d.dependency for d in dep_calls]
    assert require_erp_ops_staff in callables


def test_import_backend_main_includes_erp_visits_routes():
    import backend.main as main

    paths = set(main.app.openapi().get("paths", {}))
    assert any("/distribuidora/delivery-visits" in p for p in paths), sorted(paths)


def test_pwa_only_denied_via_erp_ops_staff(monkeypatch):
    """Chofer PWA-only no entra a evidencia ERP."""

    def _deny(*_a, **_k):
        raise HTTPException(status_code=403, detail="Acceso ERP no autorizado para esta cuenta")

    monkeypatch.setattr(
        "backend.utils.auth_staff.require_staff_user",
        _deny,
    )
    with pytest.raises(HTTPException) as ei:
        require_erp_ops_staff(authorization="Bearer fake")
    assert ei.value.status_code == 403
