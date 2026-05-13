"""
Backfill oficial: **solo** ``distribuidora.document_details`` para mayo 2026 (company 3, office 1).

Lee ``document_id`` desde ``distribuidora.documents`` ya sincronizados; no related ni OC estado.

Ejecución (raíz del repo)::

    python -m backend.jobs.backfill_details_may_2026

Variables: ver ``DETAILS_MAY_2026_RUNBOOK.md``.
"""

from __future__ import annotations

import logging
import os
import sys

from backend.services.distribuidora.sync_service import (
    backfill_distribuidora_document_details_may_2026_only,
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
        "[backfill_details_may_2026] INICIO — solo document_details, "
        "emission mayo 2026 (+ overlap opcional), documentos desde PostgreSQL",
        flush=True,
    )

    try:
        stats = backfill_distribuidora_document_details_may_2026_only(strict_token=True)
    except ValueError as e:
        print(f"[backfill_details_may_2026] ERROR: {e}", file=sys.stderr, flush=True)
        return 1
    except Exception as e:
        logging.getLogger(__name__).exception("backfill_details_may_2026")
        print(f"[backfill_details_may_2026] ERROR: {e}", file=sys.stderr, flush=True)
        return 1

    if stats.get("skipped"):
        print(f"[backfill_details_may_2026] omitido: {stats.get('skip_reason')}", flush=True)
        return 1
    if stats.get("omitido_concurrencia"):
        print(
            "[backfill_details_may_2026] ADVERTENCIA: lock documentos ocupado; no se ejecutó.",
            flush=True,
        )
        return int(os.getenv("BACKFILL_MAY_DETAILS_EXIT_CODE_ON_LOCK", "1"))

    dur = stats.get("duration_seconds", 0)
    docs = int(stats.get("documents_processed") or 0)
    rows = int(stats.get("details_rows_written") or 0)
    zero = int(stats.get("documents_with_zero_lines_after") or 0)
    first = int(stats.get("documents_first_fill") or 0)
    refr = int(stats.get("documents_refreshed") or 0)
    proxy = int(stats.get("details_rows_replaced_proxy") or 0)
    err_n = int(stats.get("document_errors") or 0)
    batches = int(stats.get("document_batches") or 0)

    print("", flush=True)
    print("=" * 60, flush=True)
    print("BACKFILL DETAILS MAYO 2026 — RESUMEN", flush=True)
    print("=" * 60, flush=True)
    print(f"  rango_emision_utc:        {stats.get('calendar_start')} .. {stats.get('calendar_end')}", flush=True)
    print(f"  overlap_days:           {stats.get('overlap_days')}", flush=True)
    print(f"  lotes_sql:              {batches}", flush=True)
    print(f"  documentos_procesados:  {docs}", flush=True)
    print(f"  filas_details_escritas: {rows}", flush=True)
    print(f"  docs_primer_llenado:    {first}", flush=True)
    print(f"  docs_refresco:          {refr}", flush=True)
    print(f"  proxy_filas_sustituidas:{proxy}  (min(antes,después) por doc refrescado)", flush=True)
    print(f"  docs_sin_lineas_api:    {zero}", flush=True)
    print(f"  errores_documento:      {err_n}", flush=True)
    print(f"  duracion_s:             {dur}", flush=True)
    if stats.get("errors"):
        print(f"  error_global:           {stats['errors']}", flush=True)
    print("=" * 60, flush=True)

    if err_n > 0 and os.getenv("BACKFILL_MAY_DETAILS_EXIT1_ON_ERRORS", "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        print(
            "[backfill_details_may_2026] ERROR controlado: hubo documentos con fallo.",
            file=sys.stderr,
            flush=True,
        )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
