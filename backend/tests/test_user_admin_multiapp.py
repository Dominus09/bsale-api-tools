"""Fase 1B: asignaciones multiapp en administración de usuarios."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from backend.auth.multiapp import APP_ERP, APP_PWA, has_app_access, MultiAppScope
from backend.services.user_admin import multiapp_admin as ma
from backend.services.user_admin import service as svc


class _Cur:
    def __init__(self, scripted=None):
        self.scripted = list(scripted or [])
        self.executed = []
        self.rowcount = 0

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if not self.scripted:
            # default: table exists / ok
            self._kind = "one"
            self._payload = (1,)
            return
        kind, payload = self.scripted.pop(0)
        self._kind = kind
        self._payload = payload
        if kind == "rowcount":
            self.rowcount = payload

    def fetchone(self):
        if self._kind in ("one", "fetchone_then"):
            return self._payload if self._kind == "one" else self._payload[0]
        return None

    def fetchall(self):
        if self._kind == "all":
            return self._payload
        if self._kind == "fetchone_then":
            return self._payload[1]
        return []

    def close(self):
        return None


def test_normalize_apps_and_profiles():
    assert ma.normalize_apps(["ERP", "pwa"]) == ["erp", "pwa"]
    assert ma.normalize_profiles(["Chofer", "supervisor"]) == ["chofer", "supervisor"]
    assert ma.normalize_profiles(["jefe_sucursal"]) == ["jefe_sucursal"]
    with pytest.raises(HTTPException):
        ma.normalize_apps(["mobile"])
    with pytest.raises(HTTPException):
        ma.normalize_profiles(["admin"])
    with pytest.raises(HTTPException):
        ma.normalize_profiles(["gerencia"])  # rol ERP ≠ perfil PWA


def test_pwa_profiles_require_pwa_app():
    with pytest.raises(HTTPException) as err:
        ma.validate_apps_profiles_consistency(["erp"], ["chofer"])
    assert err.value.status_code == 400
    ma.validate_apps_profiles_consistency(["pwa"], ["chofer"])
    ma.validate_apps_profiles_consistency(["erp", "pwa"], ["bodega", "chofer"])
    ma.validate_apps_profiles_consistency(["pwa"], ["supervisor"])


def test_office_rejects_other_company(monkeypatch):
    cur = _Cur(
        [
            ("one", None),  # office not found
        ]
    )
    monkeypatch.setattr(svc, "_table_exists", lambda *_a, **_k: True)
    with pytest.raises(HTTPException) as err:
        ma.set_user_offices(
            cur,
            1,
            [(3, 99)],
            allowed_company_ids=[3],
        )
    assert err.value.status_code == 400
    assert "no pertenece" in err.value.detail.lower() or "inexistente" in err.value.detail.lower()


def test_office_rejects_outside_authorized_companies(monkeypatch):
    monkeypatch.setattr(svc, "_table_exists", lambda *_a, **_k: True)
    cur = _Cur()
    with pytest.raises(HTTPException) as err:
        ma.set_user_offices(
            cur,
            1,
            [(9, 1)],
            allowed_company_ids=[3],
        )
    assert err.value.status_code == 400
    assert "empresas autorizadas" in err.value.detail.lower()


def test_set_apps_requires_tables(monkeypatch):
    monkeypatch.setattr(svc, "_table_exists", lambda *_a, **_k: False)
    cur = _Cur()
    with pytest.raises(HTTPException) as err:
        ma.set_user_apps(cur, 1, ["erp"])
    assert err.value.status_code == 503


def test_create_pwa_only_does_not_imply_erp():
    scope = MultiAppScope(
        tables_ready=True,
        apps_rows_present=True,
        apps=frozenset({APP_PWA}),
        profiles=frozenset({"chofer"}),
    )
    assert has_app_access(scope, APP_PWA) is True
    assert has_app_access(scope, APP_ERP) is False


def test_create_user_writes_apps_transactionally(monkeypatch):
    """Simula alta ERP+PWA: si falla oficinas, no queda usuario (rollback)."""
    calls = {"commit": 0, "rollback": 0}
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("AUTH_USER_ADMIN_EMAILS", "dueno@x.cl")

    class Conn:
        def cursor(self):
            return _Cur(
                [
                    ("one", None),  # email not exists
                    ("one", (42, "a@x.cl", "operativo", True, None, "Ana")),  # insert user
                ]
            )

        def commit(self):
            calls["commit"] += 1

        def rollback(self):
            calls["rollback"] += 1

        def close(self):
            return None

    monkeypatch.setattr(svc, "get_connection", lambda: Conn())
    monkeypatch.setattr(svc, "_set_user_companies", lambda *a, **k: None)
    monkeypatch.setattr(
        "backend.services.user_admin.service.ma.apply_multiapp_writes",
        lambda *a, **k: (_ for _ in ()).throw(
            HTTPException(status_code=400, detail="Oficina inválida")
        ),
    )

    with pytest.raises(HTTPException) as err:
        svc.create_user(
            actor={"id": 1, "role": "gerencia", "email": "dueno@x.cl"},
            email="nuevo@x.cl",
            nombre_usuario="Nuevo",
            role="operativo",
            company_ids=[3],
            password=None,
            invite=True,
            apps=["erp", "pwa"],
            profiles=["chofer"],
            office_scopes=[{"company_id": 3, "office_id": 1}],
        )
    assert err.value.status_code == 400
    assert calls["rollback"] == 1
    assert calls["commit"] == 0


def test_create_user_pwa_only_payload(monkeypatch):
    written = {}

    class Conn:
        def cursor(self):
            return _Cur(
                [
                    ("one", None),
                    ("one", (77, "p@x.cl", "operativo", True, None, "Pwa")),
                ]
            )

        def commit(self):
            return None

        def rollback(self):
            return None

        def close(self):
            return None

    monkeypatch.setattr(svc, "get_connection", lambda: Conn())
    monkeypatch.setattr(svc, "_set_user_companies", lambda *a, **k: None)
    monkeypatch.setattr(
        svc,
        "_create_password_token",
        lambda *a, **k: {"token_id": 1, "one_time_token": "x", "purpose": "invite"},
    )

    def fake_apply(cur, **kwargs):
        written.update(kwargs)
        return {"apps": kwargs["apps"], "profiles": kwargs["profiles"]}

    monkeypatch.setattr(
        "backend.services.user_admin.service.ma.apply_multiapp_writes",
        fake_apply,
    )
    monkeypatch.setattr(
        svc,
        "get_user",
        lambda uid: {
            "id": uid,
            "email": "p@x.cl",
            "role": "operativo",
            "apps": ["pwa"],
            "profiles": ["chofer"],
            "has_erp_access": False,
            "has_pwa_access": True,
        },
    )
    monkeypatch.setenv("AUTH_USER_ADMIN_EMAILS", "dueno@x.cl")
    monkeypatch.setenv("ENVIRONMENT", "production")

    out = svc.create_user(
        actor={"id": 1, "role": "gerencia", "email": "dueno@x.cl"},
        email="p@x.cl",
        nombre_usuario="Pwa",
        role="operativo",
        company_ids=[3],
        password=None,
        invite=True,
        apps=["pwa"],
        profiles=["chofer"],
        office_scopes=None,
    )
    assert written["apps"] == ["pwa"]
    assert "erp" not in written["apps"]
    assert out["has_erp_access"] is False
    assert out["has_pwa_access"] is True


def test_update_preserves_unrelated_when_partial(monkeypatch):
    """PATCH solo apps no debe enviar profiles/offices a apply (None = no tocar)."""
    captured = {}

    class Conn:
        def cursor(self):
            return _Cur(
                [
                    ("one", ("u@x.cl", "operativo", True, "U")),
                ]
            )

        def commit(self):
            return None

        def rollback(self):
            return None

        def close(self):
            return None

    monkeypatch.setattr(svc, "get_connection", lambda: Conn())
    monkeypatch.setenv("AUTH_USER_ADMIN_EMAILS", "dueno@x.cl")
    monkeypatch.setenv("ENVIRONMENT", "production")

    def fake_apply(cur, **kwargs):
        captured.update(kwargs)
        return {"apps": kwargs["apps"]}

    monkeypatch.setattr(
        "backend.services.user_admin.service.ma.apply_multiapp_writes",
        fake_apply,
    )
    monkeypatch.setattr(
        svc,
        "get_user",
        lambda uid: {"id": uid, "apps": ["erp", "pwa"]},
    )
    monkeypatch.setattr(svc, "_ensure_not_last_admin", lambda *a, **k: None)
    monkeypatch.setattr(svc, "_guard_self_privilege_change", lambda *a, **k: None)

    svc.update_user(
        actor={"id": 1, "role": "gerencia", "email": "dueno@x.cl"},
        user_id=5,
        apps=["erp", "pwa"],
    )
    assert captured["apps"] == ["erp", "pwa"]
    assert captured["profiles"] is None
    assert captured["office_scopes"] is None


def test_enrich_legacy_flags():
    row = ma.enrich_user_row(
        {"id": 1, "email": "a@x.cl"},
        apps=[],
        profiles=[],
        offices=[],
        tables_ready=True,
    )
    assert row["apps_legacy_erp"] is True
    assert row["has_erp_access"] is True
    assert row["has_pwa_access"] is False

    pwa = ma.enrich_user_row(
        {"id": 2},
        apps=["pwa"],
        profiles=["chofer"],
        offices=[{"company_id": 3, "office_id": 1}],
        tables_ready=True,
    )
    assert pwa["has_erp_access"] is False
    assert pwa["has_pwa_access"] is True


def test_catalogs_static():
    apps = ma.apps_catalog()["items"]
    assert {a["code"] for a in apps} == {"erp", "pwa"}
    profiles = ma.profiles_catalog()["items"]
    codes = {p["code"] for p in profiles}
    assert "chofer" in codes and "jefe_sucursal" in codes
    assert "supervisor" in codes
    labels = {p["code"]: p["label"] for p in profiles}
    assert labels["supervisor"] == "Supervisor"
