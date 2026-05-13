"""Token Bsale desde variables de entorno (scripts CLI, FASE 5.1).

Evita almacenar tokens en el código fuente. Acepta ``BSALE_TOKEN`` o ``BSALE_TOKEN_SPA``.
"""

from __future__ import annotations

import os
import sys

_INVALID = frozenset({"", "PEGAR_TOKEN_AQUI", "PEGAR_TOKEN"})


def load_dotenv_if_available() -> None:
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass


def read_bsale_token_from_env() -> str | None:
    load_dotenv_if_available()
    t = (os.getenv("BSALE_TOKEN") or os.getenv("BSALE_TOKEN_SPA") or "").strip()
    if not t or t in _INVALID:
        return None
    return t


def require_bsale_token(*, label: str = "bsale") -> str:
    t = read_bsale_token_from_env()
    if not t:
        print(
            f"[{label}] ERROR: defina BSALE_TOKEN o BSALE_TOKEN_SPA en .env o en el entorno "
            '(valor real; no use "PEGAR_TOKEN_AQUI").',
            file=sys.stderr,
        )
        raise SystemExit(1)
    return t
