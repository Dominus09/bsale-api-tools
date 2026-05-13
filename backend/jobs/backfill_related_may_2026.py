"""
Backfill oficial: ``distribuidora.document_related`` mayo 2026 (OC tipo 33, office 1), solo **relateddetailid**.

Delega en ``backfill_distribuidora_related_may_2026_only`` (rango día a día UTC 2026-05-01 … 2026-05-31).

Requisitos previos: documents + ``document_details`` mayo ya sincronizados (FASE 7.5 / 7.6).

Ejecución (raíz del repo)::

    python -m backend.jobs.backfill_related_may_2026

Variables: ver ``RELATED_MAY_2026_RUNBOOK.md``.
"""

from __future__ import annotations

import logging
import os
import sys

from backend.services.distribuidora.sync_related_service import (
    backfill_distribuidora_related_may_2026_only,
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
        "[backfill_related_may_2026] INICIO — document_related, mayo 2026 UTC, relateddetailid",
        flush=True,
    )

    try:
        stats = backfill_distribuidora_related_may_2026_only(strict_token=True)
    except ValueError as e:
        print(f"[backfill_related_may_2026] ERROR: {e}", file=sys.stderr, flush=True)
        return 1
    except Exception as e:
        logging.getLogger(__name__).exception("backfill_related_may_2026")
        print(f"[backfill_related_may_2026] ERROR: {e}", file=sys.stderr, flush=True)
        return 1

    if stats.get("skipped"):
        print(f"[backfill_related_may_2026] omitido: {stats.get('skip_reason')}", flush=True)
        return 1
    if stats.get("omitido_concurrencia"):
        print(
            "[backfill_related_may_2026] ADVERTENCIA: lock related ocupado; no se ejecutó.",
            flush=True,
        )
        return int(os.getenv("BACKFILL_MAY_RELATED_EXIT_CODE_ON_LOCK", "1"))

    dur = stats.get("duration_seconds", 0)
    docs = int(stats.get("documents_processed") or 0)
    det = int(stats.get("relateddetail_details_processed") or 0)
    items = int(stats.get("relateddetail_items_total") or 0)
    ins = int(stats.get("rows_inserted") or 0)
    conf = int(stats.get("related_insert_conflicts") or 0)
    att = int(stats.get("related_insert_attempts") or 0)
    api = int(stats.get("api_calls") or 0)
    days = int(stats.get("days_processed") or 0)
    skip_off = int(stats.get("related_skipped_other_office") or 0)

    print("", flush=True)
    print("=" * 60, flush=True)
    print("BACKFILL RELATED MAYO 2026 — RESUMEN", flush=True)
    print("=" * 60, flush=True)
    print(f"  dias_procesados:           {days}", flush=True)
    print(f"  documentos_oc_procesados: {docs}", flush=True)
    print(f"  details_procesados:        {det}", flush=True)
    print(f"  relaciones_api_items:      {items}", flush=True)
    print(f"  filas_insertadas_nuevas:  {ins}", flush=True)
    print(f"  intentos_insert:           {att}", flush=True)
    print(f"  conflictos_on_conflict:    {conf}  (ya existían)", flush=True)
    print(f"  llamadas_api:              {api}", flush=True)
    print(f"  omitidas_otra_office:      {skip_off}", flush=True)
    print(f"  duracion_s:                {dur}", flush=True)
    if stats.get("errors"):
        print(f"  error_global:              {stats['errors']}", flush=True)
    print("=" * 60, flush=True)

    if stats.get("errors"):
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
