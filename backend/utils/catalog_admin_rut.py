"""RUTs habilitados como administradores del catálogo web (cat.quillotana.cl)."""

from __future__ import annotations

import os

from backend.client_rut import clean_rut_for_lookup


def catalog_admin_ruts() -> frozenset[str]:
    raw = os.getenv("CATALOG_ADMIN_RUTS", "").strip()
    if not raw:
        return frozenset()
    return frozenset(
        clean_rut_for_lookup(part.strip())
        for part in raw.split(",")
        if part.strip()
    )


def is_catalog_admin_rut(rut: str | None) -> bool:
    if not rut or not str(rut).strip():
        return False
    try:
        key = clean_rut_for_lookup(str(rut).strip())
    except Exception:
        return False
    admins = catalog_admin_ruts()
    return bool(admins) and key in admins


def require_catalog_admin_rut(rut: str | None) -> str:
    from fastapi import HTTPException

    if not rut or not str(rut).strip():
        raise HTTPException(status_code=403, detail="RUT de administrador requerido")
    try:
        key = clean_rut_for_lookup(str(rut).strip())
    except Exception:
        raise HTTPException(status_code=400, detail="RUT inválido") from None
    if not is_catalog_admin_rut(key):
        raise HTTPException(status_code=403, detail="No autorizado para administración de catálogo")
    return key
