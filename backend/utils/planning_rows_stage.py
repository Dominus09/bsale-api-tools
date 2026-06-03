"""Logs por etapa para GET dispatch-prep/planning-rows."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

STAGE_TAG = "[PLANNING_ROWS_STAGE]"

PLANNING_ROWS_STAGES = (
    "load_base_orders",
    "load_purchase_status",
    "load_probable_matches",
    "load_observaciones",
    "load_georef",
    "build_rows",
    "build_summary",
    "serialize_response",
)


def planning_rows_stage_enabled() -> bool:
    return os.environ.get("PLANNING_ROWS_STAGE", "true").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def log_planning_rows_stage(
    stage: str,
    *,
    elapsed_ms: float,
    rows_count: int | None = None,
    payload_size: int | None = None,
    **extra: Any,
) -> None:
    if not planning_rows_stage_enabled():
        return
    parts = [STAGE_TAG, f"stage={stage}", f"elapsed_ms={round(elapsed_ms, 2)}"]
    if rows_count is not None:
        parts.append(f"rows_count={rows_count}")
    if payload_size is not None:
        parts.append(f"payload_size={payload_size}")
    for k, v in extra.items():
        if v is not None:
            parts.append(f"{k}={v}")
    logger.info(" ".join(parts))


@dataclass
class PlanningRowsStageCollector:
    """Acumula tiempos por etapa y emite ranking al cerrar."""

    request_id: str = ""
    _records: list[dict[str, Any]] = field(default_factory=list)
    _t0: float = field(default_factory=time.perf_counter)

    def record(
        self,
        stage: str,
        *,
        elapsed_ms: float,
        rows_count: int | None = None,
        payload_size: int | None = None,
        **extra: Any,
    ) -> None:
        log_planning_rows_stage(
            stage,
            elapsed_ms=elapsed_ms,
            rows_count=rows_count,
            payload_size=payload_size,
            **extra,
        )
        if not planning_rows_stage_enabled():
            return
        self._records.append(
            {
                "stage": stage,
                "elapsed_ms": round(elapsed_ms, 2),
                "rows_count": rows_count,
                "payload_size": payload_size,
                **extra,
            }
        )

    def sum_elapsed(self, *stage_names: str) -> float:
        names = set(stage_names)
        return round(
            sum(
                r["elapsed_ms"]
                for r in self._records
                if r.get("stage") in names
            ),
            2,
        )

    def finish(self, *, rows_count: int | None = None) -> dict[str, Any]:
        total_ms = round((time.perf_counter() - self._t0) * 1000.0, 2)
        ranking = sorted(self._records, key=lambda r: r["elapsed_ms"], reverse=True)
        log_planning_rows_stage(
            "request_total",
            elapsed_ms=total_ms,
            rows_count=rows_count,
        )
        if planning_rows_stage_enabled() and ranking:
            logger.info(
                "%s stage_ranking slowest=%s elapsed_ms=%s",
                STAGE_TAG,
                ranking[0]["stage"],
                ranking[0]["elapsed_ms"],
            )
            for i, rec in enumerate(ranking[:8], start=1):
                logger.info(
                    "%s rank=%s stage=%s elapsed_ms=%s rows_count=%s",
                    STAGE_TAG,
                    i,
                    rec["stage"],
                    rec["elapsed_ms"],
                    rec.get("rows_count"),
                )
        return {"total_ms": total_ms, "stages": ranking}
