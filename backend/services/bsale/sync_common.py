"""Piezas comunes de los sync Bsale por empresa."""

from __future__ import annotations

import logging
import time
from typing import Any, Callable

from backend.db import get_connection
from backend.services.bsale.companies import BsaleCompany
from backend.services.bsale.http_client import BsaleHttpClient

logger = logging.getLogger(__name__)

ConnectionFactory = Callable[[], Any]
ClientFactory = Callable[[BsaleCompany], BsaleHttpClient]


class CompanySyncError(RuntimeError):
    """Falla de sincronización de una empresa (HTTP, validación o DB)."""


def default_client_factory(company: BsaleCompany) -> BsaleHttpClient:
    return BsaleHttpClient(company.token)


def default_connection_factory() -> Any:
    return get_connection()


def count_company_rows(conn: Any, table: str, company_id: int) -> int:
    """``table`` siempre es un literal interno, nunca entrada externa."""
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE company_id = %s", (company_id,))
    row = cur.fetchone()
    cur.close()
    return int(row[0] or 0) if row else 0


def run_company_phase(
    phase: str,
    company: BsaleCompany,
    fn: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    """Ejecuta ``fn`` y normaliza el resultado; nunca propaga la excepción."""
    t0 = time.perf_counter()
    result: dict[str, Any] = {"phase": phase, "company_id": company.company_id, "ok": False}
    try:
        result.update(fn())
        result["ok"] = True
        logger.info(
            "[BSALE_SYNC] phase=%s company_id=%s ok fetched=%s upserted=%s deleted=%s",
            phase,
            company.company_id,
            result.get("fetched"),
            result.get("upserted"),
            result.get("deleted"),
        )
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        logger.error(
            "[BSALE_SYNC] phase=%s company_id=%s FAILED error=%s",
            phase,
            company.company_id,
            result["error"],
        )
    result["duration_ms"] = int((time.perf_counter() - t0) * 1000)
    return result
