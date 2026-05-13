"""
Job oficial: relaciones OC ↔ documentos operacionales (``relateddetailid``).

- No compite con el lock del sync principal de documentos (usa otro advisory lock).
- Logs en **stdout** (Coolify) + resumen final estructurado.
- Códigos de salida: ``0`` éxito, ``1`` error controlado (token, excepción, u opciones vía env).
- Carga ``.env`` solo si ``python-dotenv`` está instalado (``load_dotenv_if_available``); en producción suelen bastar variables del contenedor.

Ejecución manual (raíz del repo, ``PG_*`` y token Bsale en entorno o ``.env``)::

    python -m backend.jobs.sync_distribuidora_related

Variables útiles: ``DISTRIBUIDORA_RELATED_LOOKBACK_DAYS``, ``DISTRIBUIDORA_RELATED_DETAIL_LIMIT``,
``DISTRIBUIDORA_RELATED_API_DELAY_SEC``, ``LOG_LEVEL``. Ver ``COOLIFY_JOB_SETUP.md`` en la raíz del repositorio.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from backend.services.distribuidora.sync_related_service import (
    sync_distribuidora_related_documents,
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


def _print_job_summary(
    stats: dict,
    *,
    lookback_days: int,
    limit_documents: int,
) -> None:
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(days=max(1, lookback_days))
    dur = stats.get("duration_seconds", 0)
    doc_err = int(stats.get("document_errors") or 0)

    print("", flush=True)
    print("=" * 60, flush=True)
    print("SYNC DISTRIBUIDORA RELATED — RESUMEN JOB", flush=True)
    print("=" * 60, flush=True)
    print(f"  fin_utc:                    {now.isoformat()}", flush=True)
    print(f"  lookback_days:              {lookback_days}", flush=True)
    print(f"  ventana_emision_aprox_utc:  emission_date >= {window_start.isoformat()}", flush=True)
    print(f"  limite_documentos_oc:      {limit_documents}", flush=True)
    print(f"  documentos_considerados:   {stats.get('documents_considered', 0)}", flush=True)
    print(f"  detalles_procesados:       {stats.get('relateddetail_details_processed', 0)}", flush=True)
    print(f"  items_related_api:         {stats.get('relateddetail_items_total', 0)}", flush=True)
    print(f"  filas_insertadas:          {stats.get('rows_inserted', 0)}", flush=True)
    print(f"  llamadas_api:              {stats.get('api_calls', 0)}", flush=True)
    print(f"  omitidas_otra_office:      {stats.get('related_skipped_other_office', 0)}", flush=True)
    print(f"  errores_por_documento:     {doc_err}", flush=True)
    print(f"  duracion_s:                {dur}", flush=True)
    print(f"  omitido_por_lock:          {stats.get('omitido_concurrencia', False)}", flush=True)
    if stats.get("errors"):
        print(f"  error_global:              {stats['errors']}", flush=True)
    print("=" * 60, flush=True)


def main() -> int:
    load_dotenv_if_available()
    _configure_logging()

    lookback = int(os.getenv("DISTRIBUIDORA_RELATED_LOOKBACK_DAYS", "7"))
    limit = int(os.getenv("DISTRIBUIDORA_RELATED_DETAIL_LIMIT", "250"))

    t_job = datetime.now(timezone.utc)
    print(
        f"[sync_distribuidora_related] INICIO utc={t_job.isoformat()} "
        f"lookback_days={lookback} limit_documentos_oc={limit}",
        flush=True,
    )

    try:
        stats = sync_distribuidora_related_documents(strict_token=True)
    except ValueError as e:
        print(f"[sync_distribuidora_related] ERROR: {e}", file=sys.stderr, flush=True)
        return 1
    except Exception as e:
        logging.getLogger(__name__).exception("sync_distribuidora_related")
        print(f"[sync_distribuidora_related] ERROR: {e}", file=sys.stderr, flush=True)
        return 1

    _print_job_summary(stats, lookback_days=lookback, limit_documents=limit)

    if stats.get("omitido_concurrencia"):
        print(
            "[sync_distribuidora_related] ADVERTENCIA: advisory lock related ocupado; "
            "no se procesaron documentos en esta corrida.",
            flush=True,
        )
        return int(os.getenv("DISTRIBUIDORA_RELATED_EXIT_CODE_ON_LOCK", "0"))

    if stats.get("skipped"):
        print(
            f"[sync_distribuidora_related] omitido: {stats.get('skip_reason', 'sin detalle')}",
            flush=True,
        )
        return 1

    if int(stats.get("document_errors") or 0) > 0:
        on_err = os.getenv("DISTRIBUIDORA_RELATED_EXIT1_ON_DOC_ERRORS", "").strip().lower()
        if on_err in ("1", "true", "yes"):
            print(
                "[sync_distribuidora_related] ERROR controlado: hubo fallos por documento "
                f"({stats.get('document_errors')}); ver logs línea a línea.",
                file=sys.stderr,
                flush=True,
            )
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
