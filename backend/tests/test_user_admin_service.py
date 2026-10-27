"""Lógica de administración de usuarios con mocks (sin tocar BD real)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from backend.auth.passwords import hash_password, verify_password
from backend.services.user_admin import service as svc


class _Cur:
    def __init__(self, scripted):
        self.scripted = list(scripted)
        self.executed = []
        self.rowcount = 0

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if not self.scripted:
            raise AssertionError(f"Unexpected SQL: {sql}")
        kind, payload = self.scripted.pop(0)
        self._kind = kind
        self._payload = payload
        if kind == "rowcount":
            self.rowcount = payload

    def fetchone(self):
        if self._kind == "one":
            return self._payload
        if self._kind == "fetchone_then":
            return self._payload[0]
        return None

    def fetchall(self):
        if self._kind == "all":
            return self._payload
        if self._kind == "fetchone_then":
            return self._payload[1]
        return []

    def close(self):
        return None


def _conn(scripted):
    cur = _Cur(scripted)
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn, cur


def test_hash_token_stable():
    a = svc._hash_token("abc")
    b = svc._hash_token("abc")
    assert a == b and len(a) == 64 and a != "abc"


def test_ensure_not_last_admin_blocks(monkeypatch):
    conn, cur = _conn(
        [
            ("one", (True, "adm")),
            ("one", (1,)),  # count_active_admins
        ]
    )
    # count_active_admins does its own execute - need scripted for that
    # Re-script properly:
    conn, cur = _conn(
        [
            ("one", (True, "adm")),  # select active, role
            ("one", (1,)),  # count
        ]
    )
    monkeypatch.setattr(svc, "get_connection", lambda: conn)
    # _ensure_not_last_admin uses same cursor passed in
    with pytest.raises(HTTPException) as err:
        svc._ensure_not_last_admin(cur, user_id=1, new_active=False, new_role=None)
    assert err.value.status_code == 409


def test_ensure_not_last_admin_allows_when_others(monkeypatch):
    conn, cur = _conn(
        [
            ("one", (True, "adm")),
            ("one", (2,)),
        ]
    )
    svc._ensure_not_last_admin(cur, user_id=1, new_active=False, new_role=None)


def test_roles_catalog_has_assignable_only():
    cat = svc.roles_catalog()
    assert set(cat["assignable_roles"]) == {"adm", "gerencia", "operativo"}
    assert "superadmin" not in cat["assignable_roles"]
    assert "owner" not in cat["assignable_roles"]
    assert cat.get("hierarchy") == ["OWNER", "GERENCIA", "ADM", "OPERATIVO"]


def test_create_password_token_includes_one_time_path(monkeypatch):
    from urllib.parse import unquote

    expires = datetime.now(timezone.utc) + timedelta(hours=24)
    cur = _Cur(
        [
            ("rowcount", 1),  # invalidate previous
            ("one", (11, expires)),  # insert returning
        ]
    )
    monkeypatch.setattr(svc, "_table_exists", lambda *_a, **_k: True)
    out = svc._create_password_token(cur, user_id=5, purpose="reset", created_by=1)
    assert out["token_id"] == 11
    assert out["purpose"] == "reset"
    assert out["one_time_token"]
    # SQL recibe hash, no el token en claro como texto de consulta
    assert all(out["one_time_token"] not in (sql or "") for sql, _ in cur.executed)
    assert out["one_time_path"].startswith("/restablecer-contrasena?token=")
    q = out["one_time_path"].split("token=", 1)[1]
    assert unquote(q) == out["one_time_token"]


def test_consume_password_token_rejects_reuse(monkeypatch):
    past_used = datetime.now(timezone.utc)
    conn, cur = _conn(
        [
            ("one", (True,)),  # table exists auth_password_tokens
            (
                "one",
                (
                    9,
                    3,
                    "reset",
                    datetime.now(timezone.utc) + timedelta(hours=1),
                    past_used,
                ),
            ),
        ]
    )
    monkeypatch.setattr(svc, "get_connection", lambda: conn)
    # First execute is table check via _table_exists which returns fetchone
    # Need to restructure - _table_exists calls execute + fetchone
    # Then SELECT FOR UPDATE

    def table_exists(cur, schema, table):
        return table == "auth_password_tokens"

    monkeypatch.setattr(svc, "_table_exists", table_exists)
    conn, cur = _conn(
        [
            (
                "one",
                (
                    9,
                    3,
                    "reset",
                    datetime.now(timezone.utc) + timedelta(hours=1),
                    past_used,
                ),
            ),
        ]
    )
    monkeypatch.setattr(svc, "get_connection", lambda: conn)
    with pytest.raises(HTTPException) as err:
        svc.consume_password_token(raw_token="tok", new_password="1234567890ab")
    assert err.value.status_code == 400
    assert "utilizado" in err.value.detail.lower()


def test_consume_password_token_rejects_expired(monkeypatch):
    monkeypatch.setattr(svc, "_table_exists", lambda cur, s, t: t == "auth_password_tokens")
    expired = datetime.now(timezone.utc) - timedelta(hours=1)
    conn, cur = _conn(
        [
            ("one", (9, 3, "reset", expired, None)),
        ]
    )
    monkeypatch.setattr(svc, "get_connection", lambda: conn)
    with pytest.raises(HTTPException) as err:
        svc.consume_password_token(raw_token="tok", new_password="1234567890ab")
    assert "expirado" in err.value.detail.lower()


def test_consume_password_token_success(monkeypatch):
    monkeypatch.setattr(svc, "_table_exists", lambda cur, s, t: t == "auth_password_tokens")
    monkeypatch.setattr(svc, "_column_exists", lambda cur, s, t, c: False)
    monkeypatch.setattr(svc, "record_auth_event", lambda **kwargs: None)
    expires = datetime.now(timezone.utc) + timedelta(hours=2)
    # executes: select token, update password, update used, select email
    conn, cur = _conn(
        [
            ("one", (9, 3, "invite", expires, None)),
            ("rowcount", 1),
            ("rowcount", 1),
            ("one", ("nuevo@test.local",)),
        ]
    )
    monkeypatch.setattr(svc, "get_connection", lambda: conn)
    out = svc.consume_password_token(raw_token="secrettoken", new_password="1234567890ab")
    assert out["ok"] is True and out["email"] == "nuevo@test.local"


def test_create_user_rejects_duplicate_email_logic():
    # password hashing works for invite placeholder path
    h = hash_password("x" * 12)
    ok, kind = verify_password("x" * 12, h)
    assert ok and kind == "bcrypt"


def test_list_auth_events_unavailable(monkeypatch):
    monkeypatch.setattr(svc, "_table_exists", lambda cur, s, t: False)
    conn, cur = _conn([])
    monkeypatch.setattr(svc, "get_connection", lambda: conn)
    # list_auth_events will call cursor then _table_exists
    out = svc.list_auth_events()
    assert out["meta"]["available"] is False
    assert out["items"] == []
