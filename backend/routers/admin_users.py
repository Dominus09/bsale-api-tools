"""API Administración → Usuarios y accesos."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel, Field

from backend.auth.rate_limit import check_rate_limit
from backend.auth.user_admin_access import (
    require_user_admin_mutate,
    require_user_admin_view,
)
from backend.services.user_admin import service as svc

router = APIRouter(prefix="/admin/users", tags=["admin-users"])


class CreateUserBody(BaseModel):
    email: str
    nombre_usuario: str | None = None
    role: str
    company_ids: list[int] = Field(default_factory=list)
    password: str | None = None
    invite: bool = True


class UpdateUserBody(BaseModel):
    email: str | None = None
    nombre_usuario: str | None = None
    role: str | None = None
    active: bool | None = None
    company_ids: list[int] | None = None


class ConsumeResetBody(BaseModel):
    token: str
    new_password: str


@router.get("")
def admin_list_users(
    q: str | None = Query(None, max_length=120),
    active: bool | None = Query(None),
    _actor: dict = Depends(require_user_admin_view),
) -> dict[str, Any]:
    items = svc.list_users(q=q, active=active)
    return {"items": items, "total": len(items)}


@router.get("/meta/roles")
def admin_roles_catalog(_actor: dict = Depends(require_user_admin_view)) -> dict[str, Any]:
    return svc.roles_catalog()


@router.get("/meta/companies")
def admin_companies_catalog(
    _actor: dict = Depends(require_user_admin_view),
) -> dict[str, Any]:
    return {"items": svc.list_companies_catalog()}


@router.get("/auth-events")
def admin_auth_events(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    user_id: int | None = Query(None, ge=1),
    event_type: str | None = Query(None, max_length=64),
    _actor: dict = Depends(require_user_admin_view),
) -> dict[str, Any]:
    return svc.list_auth_events(
        limit=limit, offset=offset, user_id=user_id, event_type=event_type
    )


@router.get("/{user_id}")
def admin_get_user(
    user_id: int,
    _actor: dict = Depends(require_user_admin_view),
) -> dict[str, Any]:
    return svc.get_user(user_id)


@router.post("")
def admin_create_user(
    body: CreateUserBody,
    actor: dict = Depends(require_user_admin_mutate),
) -> dict[str, Any]:
    return svc.create_user(
        actor=actor,
        email=body.email,
        nombre_usuario=body.nombre_usuario,
        role=body.role,
        company_ids=body.company_ids,
        password=body.password,
        invite=body.invite,
    )


@router.patch("/{user_id}")
def admin_update_user(
    user_id: int,
    body: UpdateUserBody,
    actor: dict = Depends(require_user_admin_mutate),
) -> dict[str, Any]:
    return svc.update_user(
        actor=actor,
        user_id=user_id,
        nombre_usuario=body.nombre_usuario,
        email=body.email,
        role=body.role,
        active=body.active,
        company_ids=body.company_ids,
    )


@router.post("/{user_id}/reset-password")
def admin_reset_password(
    user_id: int,
    actor: dict = Depends(require_user_admin_mutate),
) -> dict[str, Any]:
    return svc.issue_password_reset(actor=actor, user_id=user_id)


# Endpoint público de consumo (sin auth staff) — montado también vía auth si se desea.
public_router = APIRouter(tags=["auth-password"])


@public_router.post("/auth/password-reset/consume")
def consume_password_reset(body: ConsumeResetBody, request: Request) -> dict[str, Any]:
    ip = request.client.host if request.client else "unknown"
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        ip = forwarded.split(",")[0].strip() or ip
    # Rate limit por IP; no incluir el token en la clave ni en logs.
    check_rate_limit(key=f"pwd_reset:{ip}")
    return svc.consume_password_token(raw_token=body.token, new_password=body.new_password)
