"""Handler de logging que vuelca registros seguros al buffer de diagnóstico."""

from __future__ import annotations

import logging
import traceback
from datetime import datetime, timezone

from backend.diagnostics import store
from backend.diagnostics.sanitize import sanitize_free_text
from backend.diagnostics.security import diagnostics_feature_enabled

_attached = False


class MemoryDiagnosticsHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        if not diagnostics_feature_enabled():
            return
        try:
            ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            msg = sanitize_free_text(record.getMessage(), max_len=2000)
            mod = record.name
            lvl = record.levelname
            detail: str | None = None
            if record.exc_info and record.exc_info[0] is not None:
                detail = sanitize_free_text(
                    "".join(traceback.format_exception_only(record.exc_info[0], record.exc_info[1])),
                    max_len=1500,
                )
            store.append_log(
                store.LogRecordView(
                    ts_iso=ts,
                    level=lvl,
                    module=mod,
                    message=msg,
                    detail=detail,
                )
            )
        except Exception:
            self.handleError(record)


def attach_memory_log_handler() -> None:
    global _attached
    if _attached:
        return
    if not diagnostics_feature_enabled():
        return
    h = MemoryDiagnosticsHandler()
    h.setLevel(logging.INFO)
    h.setFormatter(logging.Formatter("%(message)s"))
    # No enganchar ``uvicorn.access``: las peticiones HTTP ya van al buffer vía ``DiagnosticsRequestLogMiddleware``.
    for name in ("backend", "uvicorn.error"):
        logging.getLogger(name).addHandler(h)
    _attached = True
