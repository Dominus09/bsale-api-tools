"""Verificación y hash de contraseñas staff (bcrypt + transición legacy).

Nunca registrar contraseñas ni hashes en logs desde este módulo.
"""

from __future__ import annotations

import hmac
import logging
import os

from passlib.context import CryptContext

logger = logging.getLogger(__name__)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def _env_flag(name: str, *, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def looks_like_bcrypt_hash(value: str) -> bool:
    return value.startswith(("$2a$", "$2b$", "$2y$"))


def verify_password(plain: str, stored: str | None) -> tuple[bool, str]:
    """Devuelve ``(ok, storage_kind)`` donde kind es ``bcrypt`` | ``legacy_plain`` | ``empty``."""
    if not stored or not str(stored).strip():
        return False, "empty"
    s = str(stored).strip()
    if looks_like_bcrypt_hash(s):
        try:
            return bool(pwd_context.verify(plain, s)), "bcrypt"
        except Exception:
            logger.exception("bcrypt verify raised for stored hash")
            return False, "bcrypt"
    pe, se = plain.encode("utf-8"), s.encode("utf-8")
    if len(pe) != len(se):
        return False, "legacy_plain"
    return hmac.compare_digest(pe, se), "legacy_plain"


def hash_password(plain: str) -> str:
    return pwd_context.hash(plain)


def should_upgrade_legacy_on_login() -> bool:
    """Por defecto activo: convierte legacy→bcrypt tras login correcto (reversible vía flag)."""
    return _env_flag("AUTH_UPGRADE_LEGACY_PASSWORD", default=True)


def allow_legacy_plaintext() -> bool:
    """Mantener fallback legacy hasta verificar migración completa (default: sí)."""
    return _env_flag("AUTH_ALLOW_LEGACY_PASSWORD", default=True)
