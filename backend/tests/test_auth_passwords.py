"""Contraseñas staff: bcrypt, legacy y flags de transición."""

from __future__ import annotations

from backend.auth import passwords as pw


def test_bcrypt_roundtrip():
    hashed = pw.hash_password("secreto-seguro-1")
    assert pw.looks_like_bcrypt_hash(hashed)
    ok, kind = pw.verify_password("secreto-seguro-1", hashed)
    assert ok and kind == "bcrypt"
    ok2, _ = pw.verify_password("otra", hashed)
    assert not ok2


def test_legacy_plain_match_and_kind():
    ok, kind = pw.verify_password("plain-legacy", "plain-legacy")
    assert ok and kind == "legacy_plain"
    ok2, kind2 = pw.verify_password("plain-legacy", "otro")
    assert not ok2 and kind2 == "legacy_plain"


def test_empty_stored():
    ok, kind = pw.verify_password("x", None)
    assert not ok and kind == "empty"
    ok2, kind2 = pw.verify_password("x", "   ")
    assert not ok2 and kind2 == "empty"


def test_upgrade_flag_defaults_on(monkeypatch):
    monkeypatch.delenv("AUTH_UPGRADE_LEGACY_PASSWORD", raising=False)
    assert pw.should_upgrade_legacy_on_login() is True
    monkeypatch.setenv("AUTH_UPGRADE_LEGACY_PASSWORD", "0")
    assert pw.should_upgrade_legacy_on_login() is False


def test_allow_legacy_defaults_on(monkeypatch):
    monkeypatch.delenv("AUTH_ALLOW_LEGACY_PASSWORD", raising=False)
    assert pw.allow_legacy_plaintext() is True
    monkeypatch.setenv("AUTH_ALLOW_LEGACY_PASSWORD", "false")
    assert pw.allow_legacy_plaintext() is False
