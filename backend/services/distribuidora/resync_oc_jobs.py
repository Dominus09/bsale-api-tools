"""
Jobs en memoria para ``POST /distribuidora/resync-oc`` (evita timeouts del proxy).

El resync pesado corre en ``BackgroundTasks``; el cliente hace polling a
``GET /distribuidora/resync-oc/status/{job_id}``.
"""

from __future__ import annotations

import logging
import secrets
import threading
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any, Callable

logger = logging.getLogger(__name__)

_MAX_JOBS = 200
_lock = threading.Lock()
_jobs: "OrderedDict[str, dict[str, Any]]" = OrderedDict()


def _prune_if_needed() -> None:
    while len(_jobs) >= _MAX_JOBS:
        _jobs.popitem(last=False)


def mint_job_id() -> str:
    return secrets.token_hex(8)


def create_job(
    *,
    emission_date_from: str,
    emission_date_to: str,
) -> dict[str, Any]:
    jid = mint_job_id()
    now = datetime.now(timezone.utc).isoformat()
    rec: dict[str, Any] = {
        "job_id": jid,
        "status": "started",
        "processed_count": 0,
        "updated_count": 0,
        "error_count": 0,
        "started_at": now,
        "finished_at": None,
        "message": "Encolado",
        "emission_date_from": emission_date_from,
        "emission_date_to": emission_date_to,
    }
    with _lock:
        _prune_if_needed()
        _jobs[jid] = rec
        _jobs.move_to_end(jid)
    return rec


def update_job(job_id: str, **kwargs: Any) -> None:
    with _lock:
        rec = _jobs.get(job_id)
        if not rec:
            return
        rec.update(kwargs)
        _jobs.move_to_end(job_id)


def get_job(job_id: str) -> dict[str, Any] | None:
    with _lock:
        rec = _jobs.get(job_id)
        return dict(rec) if rec else None


def _progress_sink_for_job(job_id: str) -> Callable[[dict[str, Any]], None]:
    def _sink(snapshot: dict[str, Any]) -> None:
        update_job(
            job_id,
            status="running",
            processed_count=int(snapshot.get("documents_processed", 0) or 0),
            updated_count=int(snapshot.get("updated_documents", 0) or 0),
            error_count=int(snapshot.get("document_errors", 0) or 0),
            message=str(snapshot.get("message") or "Procesando órdenes"),
        )

    return _sink


def run_resync_oc_job(
    job_id: str,
    emission_from: datetime,
    emission_to: datetime,
    emission_date_from: str,
    emission_date_to: str,
) -> None:
    """
    Ejecutado vía ``BackgroundTasks`` (no bloquea el request HTTP).
    """
    from backend.services.distribuidora.sync_service import DistribuidoraSyncService

    logger.info(
        "resync_oc started from %s to %s (job_id=%s)",
        emission_date_from,
        emission_date_to,
        job_id,
    )
    update_job(
        job_id,
        status="running",
        message="Procesando órdenes",
    )
    sink = _progress_sink_for_job(job_id)
    try:
        result = DistribuidoraSyncService.run_resync(
            emission_from=emission_from,
            emission_to=emission_to,
            strict_token=True,
            on_progress=sink,
        )
        if result.get("omitido_concurrencia"):
            update_job(
                job_id,
                status="error",
                finished_at=datetime.now(timezone.utc).isoformat(),
                message="Otro resync o sync incremental tiene el lock; reintente en unos segundos.",
            )
            logger.warning("resync_oc job %s omitido por concurrencia", job_id)
            return

        proc = int(result.get("documents_processed", 0) or 0)
        upd = int(result.get("updated_documents", 0) or 0)
        err = int(result.get("document_errors", 0) or 0)
        logger.info(
            "resync_oc finished: processed=%s updated=%s errors=%s (job_id=%s)",
            proc,
            upd,
            err,
            job_id,
        )
        update_job(
            job_id,
            status="done",
            processed_count=proc,
            updated_count=upd,
            error_count=err,
            finished_at=datetime.now(timezone.utc).isoformat(),
            message=f"Listo: {proc} procesadas, {upd} actualizadas, {err} errores",
        )
    except Exception as e:
        logger.exception("resync_oc job %s error", job_id)
        update_job(
            job_id,
            status="error",
            finished_at=datetime.now(timezone.utc).isoformat(),
            message=str(e),
        )
