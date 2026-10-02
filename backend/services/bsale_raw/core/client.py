"""Cliente Bsale para ``bsale_raw``: reutiliza ``BsaleHttpClient`` y le agrega el limitador con prioridad de la empresa.

``BsaleHttpClient`` ya aporta timeouts connect/read, retry sólo para 408/425/429/5xx y errores
de red, backoff exponencial con jitter, ``Retry-After``, validación de host y paginación
estricta. Esta capa no lo modifica: le inyecta una ``RateLimitedSession`` que comparte el
limitador único de la empresa con todos los demás consumidores, con la prioridad del consumidor.

Los tokens se leen del entorno (``bsale.companies.bsale_token`` guarda sólo el NOMBRE de la
variable). Nunca se persisten ni se loguean.
"""

from __future__ import annotations

import os
from typing import Any

from backend.services.bsale.companies import BsaleCompany
from backend.services.bsale.http_client import BsaleHttpClient
from backend.services.bsale_raw.core.rate_limit import (
    CompanyRateLimiters,
    RateLimitedSession,
    RequestPriority,
)

DEFAULT_MAX_CONCURRENT_COMPANIES = 3


def max_concurrent_companies() -> int:
    """Concurrencia máxima entre empresas. Dentro de una empresa todos comparten un limitador."""
    value = int(os.getenv("BSALE_RAW_MAX_CONCURRENT_COMPANIES") or DEFAULT_MAX_CONCURRENT_COMPANIES)
    if value < 1:
        raise ValueError("BSALE_RAW_MAX_CONCURRENT_COMPANIES debe ser >= 1")
    return value


def build_company_client(
    company: BsaleCompany,
    limiters: CompanyRateLimiters,
    priority: RequestPriority,
    **client_kwargs: Any,
) -> BsaleHttpClient:
    session = RateLimitedSession(limiters.for_company(company.company_id), priority)
    return BsaleHttpClient(company.token, session=session, **client_kwargs)
