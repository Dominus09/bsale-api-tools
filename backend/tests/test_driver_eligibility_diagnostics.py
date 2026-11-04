"""Diagnóstico de elegibilidad de chofer (sin ampliar permisos)."""

from unittest.mock import MagicMock

from backend.auth.multiapp import APP_ERP, APP_PWA, MultiAppScope
from backend.services.distribuidora import dispatch_plan_driver_service as svc


def test_operativo_without_pwa_is_not_eligible(monkeypatch):
    cur = MagicMock()
    cur.fetchone.return_value = (15, "chofer1@quillotana.cl", "operativo", True, "Claudio")
    cur.description = [("id",), ("email",), ("role",), ("active",), ("nombre_usuario",)]
    monkeypatch.setattr(
        svc,
        "load_multiapp_scope",
        lambda _uid: MultiAppScope(
            tables_ready=True,
            apps_rows_present=True,
            apps=frozenset({APP_ERP}),
            profiles=frozenset(),
        ),
    )
    ok, missing = svc._eligibility_from_scope(
        {"id": 15, "active": True},
        MultiAppScope(
            tables_ready=True,
            apps_rows_present=True,
            apps=frozenset({APP_ERP}),
            profiles=frozenset(),
        ),
    )
    assert ok is False
    assert "pwa_required" in missing
    assert "chofer_profile_required" in missing


def test_pwa_chofer_eligible():
    ok, missing = svc._eligibility_from_scope(
        {"id": 15, "active": True},
        MultiAppScope(
            tables_ready=True,
            apps_rows_present=True,
            apps=frozenset({APP_PWA}),
            profiles=frozenset({"chofer"}),
        ),
    )
    assert ok is True
    assert missing == []
