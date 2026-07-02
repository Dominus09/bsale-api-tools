"""Dependencias FastAPI para usuarios staff autenticados."""

from __future__ import annotations

from typing import Annotated

from fastapi import Header

from backend.auth.jwt import decode_staff_token

BearerDep = Annotated[str | None, Header(alias="Authorization")]

__all__ = ["BearerDep", "decode_staff_token", "require_staff_user"]


def require_staff_user(authorization: BearerDep) -> dict:
    return decode_staff_token(authorization)
