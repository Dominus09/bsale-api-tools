"""Carga estricta de empresas Bsale activas. ``bsale_token`` guarda el NOMBRE de la variable de entorno."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

DEFAULT_REQUIRED_COMPANY_IDS: tuple[int, ...] = (1, 2, 3)


class CompanyConfigError(RuntimeError):
    """Configuración de empresas inválida (sin empresas, token faltante, empresa requerida ausente)."""


@dataclass(frozen=True)
class BsaleCompany:
    company_id: int
    name: str
    token_env: str
    token: str = field(repr=False)


def required_company_ids_from_env(getenv: Callable[[str], str | None] = os.getenv) -> tuple[int, ...]:
    raw = (getenv("BSALE_REQUIRED_COMPANY_IDS") or "").strip()
    if not raw:
        return DEFAULT_REQUIRED_COMPANY_IDS
    return tuple(int(x) for x in raw.split(",") if x.strip())


def load_active_companies(
    cur: Any,
    *,
    required_ids: Iterable[int] | None = None,
    getenv: Callable[[str], str | None] = os.getenv,
) -> list[BsaleCompany]:
    """
    Lee ``bsale.companies WHERE active = true`` y resuelve tokens desde el entorno.

    Lanza ``CompanyConfigError`` (sin ``continue`` silencioso) si:
    - no hay empresas activas;
    - ``bsale_token`` es NULL/vacío;
    - la variable de entorno indicada no existe o está vacía;
    - falta alguna empresa requerida (por defecto 1, 2, 3).
    """
    cur.execute(
        """
        SELECT company_id, name, bsale_token
        FROM bsale.companies
        WHERE active = true
        ORDER BY company_id
        """
    )
    rows = cur.fetchall()
    if not rows:
        raise CompanyConfigError("No hay empresas activas en bsale.companies")

    errors: list[str] = []
    companies: list[BsaleCompany] = []
    for company_id, name, token_env in rows:
        cid = int(company_id)
        env_name = (token_env or "").strip()
        if not env_name:
            errors.append(f"company_id={cid}: bsale_token vacío/NULL")
            continue
        token = getenv(env_name)
        if not token or not token.strip():
            errors.append(f"company_id={cid}: variable de entorno {env_name} no definida")
            continue
        companies.append(
            BsaleCompany(company_id=cid, name=name or "", token_env=env_name, token=token.strip())
        )

    required = tuple(required_ids) if required_ids is not None else required_company_ids_from_env(getenv)
    active_ids = {int(r[0]) for r in rows}
    missing_required = [cid for cid in required if cid not in active_ids]
    if missing_required:
        errors.append(f"empresas requeridas no activas: {missing_required}")

    if errors:
        raise CompanyConfigError("; ".join(errors))
    return companies
