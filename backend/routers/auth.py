from __future__ import annotations

import hmac
import logging
import os

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from passlib.context import CryptContext
from pydantic import BaseModel

from backend.auth.jwt import create_staff_token
from backend.auth.permissions import build_auth_me_response
from backend.client_rut import require_valid_rut, city_is_melinka
from backend.db import get_connection
from backend.utils.auth_staff import require_staff_user
from backend.utils.catalog_admin_rut import is_catalog_admin_rut

router = APIRouter()
auth_router = APIRouter(prefix="/auth")

logger = logging.getLogger(__name__)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

_LOGIN_DEBUG = os.getenv("AUTH_LOGIN_DEBUG", "").strip().lower() in ("1", "true", "yes")


def _looks_like_bcrypt_hash(value: str) -> bool:
    return value.startswith(("$2a$", "$2b$", "$2y$"))


def _password_matches(plain: str, stored: str | None) -> bool:
    if not stored or not stored.strip():
        return False
    s = stored.strip()
    if _looks_like_bcrypt_hash(s):
        try:
            return pwd_context.verify(plain, s)
        except Exception:
            if _LOGIN_DEBUG:
                logger.exception("bcrypt verify raised for stored hash")
            return False
    pe, se = plain.encode("utf-8"), s.encode("utf-8")
    if len(pe) != len(se):
        return False
    return hmac.compare_digest(pe, se)


class LoginRequest(BaseModel):
    email: str
    password: str


class LoginClientRequest(BaseModel):
    rut: str


@router.post("/login-client")
def login_client(body: LoginClientRequest):
    rut_clean = require_valid_rut(body.rut)

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT bsale_id, first_name, last_name, city
            FROM bsale.clients
            WHERE rut_clean = %s
            """,
            (rut_clean,),
        )
        row = cur.fetchone()
        cur.close()
    finally:
        conn.close()

    if not row:
        return JSONResponse(
            status_code=404,
            content={
                "error": "Cliente no encontrado. Contactar al +56 9 9271 4314"
            },
        )

    bsale_id, first_name, last_name, city = row
    name = f"{first_name or ''} {last_name or ''}".strip()

    return {
        "id": bsale_id,
        "name": name,
        "city": city,
        "is_melinka": city_is_melinka(city),
        "is_catalog_admin": is_catalog_admin_rut(rut_clean),
    }


@router.post("/login")
def login(data: LoginRequest):
    email_key = data.email.strip().lower()

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT password_hash, role
        FROM bsale.users
        WHERE lower(trim(email)) = %s AND active = true
        """,
        (email_key,),
    )

    user = cur.fetchone()
    cur.close()
    conn.close()

    if _LOGIN_DEBUG:
        logger.info(
            "login attempt email_norm=%s user_found=%s",
            email_key,
            user is not None,
        )

    if not user:
        raise HTTPException(status_code=401, detail="Credenciales inválidas")

    password_hash, role = user
    stored = password_hash
    ok = _password_matches(data.password, stored)

    if _LOGIN_DEBUG:
        kind = "bcrypt" if stored and _looks_like_bcrypt_hash(stored.strip()) else "legacy_plain"
        logger.info("login password check ok=%s storage_kind=%s", ok, kind)

    if not ok:
        raise HTTPException(status_code=401, detail="Password incorrecta")

    token = create_staff_token(email=data.email, role=role)

    return {
        "token": token,
        "email": data.email,
        "role": role
    }


@auth_router.get("/me")
def auth_me(user: dict = Depends(require_staff_user)):
    """Perfil staff + permisos efectivos (depuración de accesos)."""
    return build_auth_me_response(user)
