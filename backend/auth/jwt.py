"""JWT staff — único módulo que conoce secretos y operaciones PyJWT."""

from __future__ import annotations

import logging
import os
from typing import Any

import jwt
from fastapi import HTTPException

logger = logging.getLogger(__name__)

ALGORITHM = "HS256"
_DEV_JWT_FALLBACK = "quillotana_secret_key"
_MIN_JWT_SECRET_BYTES = 32


def _runtime_environment() -> str:
    return (os.getenv("ENVIRONMENT") or os.getenv("ENV") or "development").strip().lower()


def _load_jwt_secret() -> str:
    """Carga JWT_SECRET_KEY / SECRET_KEY; en producción sin clave → error al arrancar."""
    secret = (os.getenv("JWT_SECRET_KEY") or os.getenv("SECRET_KEY") or "").strip()
    env = _runtime_environment()
    is_prod = env in ("production", "staging", "prod")

    if not secret:
        if is_prod:
            raise RuntimeError(
                "JWT_SECRET_KEY (o SECRET_KEY) es obligatoria con ENVIRONMENT=production. "
                "Genere una clave aleatoria de al menos 32 caracteres, p. ej.: "
                "openssl rand -hex 32"
            )
        secret = _DEV_JWT_FALLBACK
        logger.warning(
            "JWT_SECRET_KEY / SECRET_KEY no definidas: se usa clave de desarrollo para firmar JWT. "
            "Configure JWT_SECRET_KEY en producción."
        )
    elif len(secret.encode("utf-8")) < _MIN_JWT_SECRET_BYTES:
        logger.warning(
            "JWT_SECRET_KEY tiene %s bytes (< %s recomendados para HS256). "
            "Use una clave más larga para evitar InsecureKeyLengthWarning de PyJWT.",
            len(secret.encode("utf-8")),
            _MIN_JWT_SECRET_BYTES,
        )

    return secret


SECRET = _load_jwt_secret()


def create_staff_token(
    *,
    email: str,
    role: str,
    extra: dict[str, Any] | None = None,
) -> str:
    payload: dict[str, Any] = {"email": email, "role": role}
    if extra:
        payload.update(extra)
    return jwt.encode(payload, SECRET, algorithm=ALGORITHM)


def decode_jwt_token(token: str) -> dict[str, Any]:
    """Decodifica un JWT staff sin lógica HTTP."""
    return jwt.decode(token, SECRET, algorithms=[ALGORITHM])


def bearer_token_from_authorization(authorization: str | None) -> str | None:
    if not authorization or not authorization.lower().startswith("bearer "):
        return None
    token = authorization[7:].strip()
    return token or None


def decode_staff_token(authorization: str | None) -> dict[str, Any]:
    """Authorization Bearer → payload staff (``email`` requerido)."""
    token = bearer_token_from_authorization(authorization)
    if not token:
        if not authorization:
            raise HTTPException(status_code=401, detail="Se requiere autenticación")
        raise HTTPException(status_code=401, detail="Token vacío")
    try:
        payload = decode_jwt_token(token)
    except jwt.PyJWTError as exc:
        raise HTTPException(status_code=401, detail="Token inválido") from exc
    if not payload.get("email"):
        raise HTTPException(status_code=401, detail="Token sin identidad")
    return payload


def staff_email_from_authorization(authorization: str | None) -> str | None:
    """Extrae email del Bearer sin lanzar HTTPException (p. ej. middleware)."""
    token = bearer_token_from_authorization(authorization)
    if not token:
        return None
    try:
        payload = decode_jwt_token(token)
    except jwt.PyJWTError:
        return None
    email = payload.get("email")
    return str(email) if email else None
