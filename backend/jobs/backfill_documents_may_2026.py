"""
Backfill oficial: **solo** ``distribuidora.documents`` para mayo 2026 (company 3, office 1).

No sincroniza details, related ni references (FASE 7.5). Idempotente (``ON CONFLICT`` en upsert).

Ejecución (raíz del repo, con ``PG_*`` y token Bsale)::

    python -m backend.jobs.backfill_documents_may_2026

Variables: ver ``DOCUMENTS_MAY_2026_RUNBOOK.md``.
"""

from __future__ import annotations

import logging
import os
import sys

from backend.services.distribuidora.sync_service import (
    backfill_distribuidora_documents_may_2026_documents_only,
)
from backend.utils.bsale_token_env import load_dotenv_if_available

_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
_LOG_DATEFMT = "%Y-%m-%dT%H:%M:%S"


def _configure_logging() -> None:
    level_name = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    level = getattr(logging, level_name, logging.INFO)
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATEFMT))
    root.addHandler(h)


def main() -> int:
    load_dotenv_if_available()
    _configure_logging()

    print(
        "[backfill_documents_may_2026] INICIO — solo tabla documents, "
        "2026-05-01..2026-05-31 UTC (+ overlap opcional)",
        flush=True,
    )

    try:
        stats = backfill_distribuidora_documents_may_2026_documents_only(strict_token=True)
    except ValueError as e:
        print(f"[backfill_documents_may_2026] ERROR: {e}", file=sys.stderr, flush=True)
        return 1
    except Exception as e:
        logging.getLogger(__name__).exception("backfill_documents_may_2026")
        print(f"[backfill_documents_may_2026] ERROR: {e}", file=sys.stderr, flush=True)
        return 1

    if stats.get("skipped"):
        print(f"[backfill_documents_may_2026] omitido: {stats.get('skip_reason')}", flush=True)
        return 1
    if stats.get("omitido_concurrencia"):
        print(
            "[backfill_documents_may_2026] ADVERTENCIA: lock documentos ocupado; no se ejecutó.",
            flush=True,
        )
        return int(os.getenv("BACKFILL_MAY_EXIT_CODE_ON_LOCK", "1"))

    dur = stats.get("duration_seconds", 0)
    proc = int(stats.get("documents_processed") or 0)
    ins = int(stats.get("documents_inserted") or 0)
    upd = int(stats.get("documents_updated") or 0)
    pages = int(stats.get("document_api_pages") or 0)
    days = int(stats.get("days_processed") or 0)
    err_n = int(stats.get("document_errors") or 0)
    up_fail = int(stats.get("document_upsert_failures") or 0)

    print("", flush=True)
    print("=" * 60, flush=True)
    print("BACKFILL DOCUMENTS MAYO 2026 — RESUMEN", flush=True)
    print("=" * 60, flush=True)
    print(f"  rango_calendario_utc:  {stats.get('calendar_start')} .. {stats.get('calendar_end')}", flush=True)
    print(f"  overlap_days:          {stats.get('overlap_days')}", flush=True)
    print(f"  dias_procesados:       {days}", flush=True)
    print(f"  paginas_api_docs:      {pages}", flush=True)
    print(f"  documentos_procesados: {proc}", flush=True)
    print(f"  insertados_aprox:     {ins}", flush=True)
    print(f"  actualizados_aprox:   {upd}", flush=True)
    print(f"  errores_documento:    {err_n}", flush=True)
    print(f"  fallos_upsert:        {up_fail}", flush=True)
    print(f"  duracion_s:           {dur}", flush=True)
    if stats.get("errors"):
        print(f"  error_global:         {stats['errors']}", flush=True)
    print("=" * 60, flush=True)

    if err_n > 0 or up_fail > 0:
        if os.getenv("BACKFILL_MAY_EXIT1_ON_DOC_ERRORS", "").strip().lower() in ("1", "true", "yes"):
            print(
                "[backfill_documents_may_2026] ERROR controlado: hubo errores por documento.",
                file=sys.stderr,
                flush=True,
            )
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
