"""JWT staff para panel ERP (cualquier rol válido en token de ``/login``)."""

from __future__ import annotations

from typing import Annotated

import jwt
from fastapi import Header, HTTPException

from backend.routers.auth import SECRET

BearerDep = Annotated[str | None, Header(alias="Authorization")]


def decode_staff_token(authorization: str | None) -> dict:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Se requiere autenticación")
    token = authorization[7:].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Token vacío")
    try:
        payload = jwt.decode(token, SECRET, algorithms=["HS256"])
    except jwt.PyJWTError as e:
        raise HTTPException(status_code=401, detail="Token inválido") from e
    if not payload.get("email"):
        raise HTTPException(status_code=401, detail="Token sin identidad")
    return payload


def require_staff_user(authorization: BearerDep) -> dict:
    return decode_staff_token(authorization)
