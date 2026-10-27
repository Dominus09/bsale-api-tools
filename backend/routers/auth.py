from __future__ import annotations

import logging
import os

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from backend.auth.audit import (
    EVENT_LEGACY_PASSWORD_UPGRADED,
    EVENT_LOGIN_FAILURE,
    EVENT_LOGIN_SUCCESS,
    record_auth_event,
)
from backend.auth.jwt import create_staff_token
from backend.auth.passwords import (
    allow_legacy_plaintext,
    hash_password,
    should_upgrade_legacy_on_login,
    verify_password,
)
from backend.auth.permissions import build_auth_me_response
from backend.auth.staff_session import (
    load_staff_login_row,
    load_user_company_ids,
    upgrade_legacy_password,
)
from backend.client_rut import require_valid_rut, city_is_melinka
from backend.db import get_connection
from backend.utils.auth_staff import require_staff_user
from backend.utils.catalog_admin_rut import is_catalog_admin_rut
from backend.utils.catalog_client import lookup_catalog_client

router = APIRouter()
auth_router = APIRouter(prefix="/auth")

logger = logging.getLogger(__name__)

_LOGIN_DEBUG = os.getenv("AUTH_LOGIN_DEBUG", "").strip().lower() in ("1", "true", "yes")


def _client_meta(request: Request | None) -> tuple[str | None, str | None]:
    if request is None:
        return None, None
    ip = request.client.host if request.client else None
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        ip = forwarded.split(",")[0].strip() or ip
    ua = request.headers.get("user-agent")
    return ip, ua


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
        client = lookup_catalog_client(cur, rut_clean=rut_clean)
        cur.close()
    finally:
        conn.close()

    if client is None:
        return JSONResponse(
            status_code=404,
            content={
                "error": "Cliente no encontrado. Contactar al +56 9 9271 4314"
            },
        )

    return {
        "id": client.bsale_id,
        "name": client.name,
        "city": client.city,
        "is_melinka": city_is_melinka(client.city),
        "is_catalog_admin": is_catalog_admin_rut(rut_clean),
    }


@router.post("/login")
def login(data: LoginRequest, request: Request):
    email_key = data.email.strip().lower()
    ip, ua = _client_meta(request)

    row = load_staff_login_row(email_key)

    if _LOGIN_DEBUG:
        logger.info(
            "login attempt email_norm=%s user_found=%s",
            email_key,
            row is not None,
        )

    if not row or not row.get("active"):
        record_auth_event(
            event_type=EVENT_LOGIN_FAILURE,
            email=email_key,
            user_id=(row or {}).get("id"),
            success=False,
            ip=ip,
            user_agent=ua,
            detail={"reason": "not_found_or_inactive"},
        )
        raise HTTPException(status_code=401, detail="Credenciales inválidas")

    stored = row["password_hash"]
    ok, kind = verify_password(data.password, stored)

    if kind == "legacy_plain" and not allow_legacy_plaintext():
        ok = False

    if _LOGIN_DEBUG:
        logger.info("login password check ok=%s storage_kind=%s", ok, kind)

    if not ok:
        record_auth_event(
            event_type=EVENT_LOGIN_FAILURE,
            email=email_key,
            user_id=row["id"],
            success=False,
            ip=ip,
            user_agent=ua,
            detail={"reason": "bad_password"},
        )
        raise HTTPException(status_code=401, detail="Password incorrecta")

    if kind == "legacy_plain" and should_upgrade_legacy_on_login():
        # Nunca loguear la contraseña; solo el resultado de la actualización.
        new_hash = hash_password(data.password)
        if upgrade_legacy_password(
            user_id=row["id"], new_hash=new_hash, previous_stored=stored
        ):
            record_auth_event(
                event_type=EVENT_LEGACY_PASSWORD_UPGRADED,
                email=email_key,
                user_id=row["id"],
                success=True,
                ip=ip,
                user_agent=ua,
            )

    company_ids = load_user_company_ids(row["id"])
    if company_ids is not None and len(company_ids) == 0:
        record_auth_event(
            event_type=EVENT_LOGIN_FAILURE,
            email=email_key,
            user_id=row["id"],
            success=False,
            ip=ip,
            user_agent=ua,
            detail={"reason": "no_companies"},
        )
        raise HTTPException(
            status_code=403,
            detail="Usuario sin empresas autorizadas; contacte a administración",
        )

    role = row["role"]
    token = create_staff_token(
        email=data.email.strip(),
        role=role,
        user_id=row["id"],
        token_version=int(row.get("token_version") or 0),
        company_ids=company_ids,
    )

    record_auth_event(
        event_type=EVENT_LOGIN_SUCCESS,
        email=email_key,
        user_id=row["id"],
        success=True,
        ip=ip,
        user_agent=ua,
        detail={"storage_kind": kind},
    )

    return {
        "token": token,
        "email": data.email.strip(),
        "role": role,
        "id": row["id"],
    }


@auth_router.get("/me")
def auth_me(user: dict = Depends(require_staff_user)):
    """Perfil staff + permisos efectivos (depuración de accesos)."""
    return build_auth_me_response(user)
