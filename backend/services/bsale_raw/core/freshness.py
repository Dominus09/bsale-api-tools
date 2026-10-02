"""Frescura por ``(company_id, resource)``.

Responde: "¿cuándo fue confirmado por última vez este dato contra Bsale?". Persistencia futura en
``bsale_raw.sync_state`` (ver ``docs/BSALE_RAW_ARCHITECTURE.md``); aquí sólo el modelo y la
evaluación pura contra el SLA.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

from backend.services.bsale_raw.core.models import RunStatus


class FreshnessLevel(str, Enum):
    FRESH = "FRESH"
    STALE = "STALE"
    NEVER = "NEVER"
    NO_SLA = "NO_SLA"


@dataclass
class FreshnessState:
    company_id: int
    resource: str
    last_attempt_at: datetime | None = None
    last_success_at: datetime | None = None
    last_webhook_at: datetime | None = None
    last_incremental_at: datetime | None = None
    last_full_reconcile_at: datetime | None = None
    last_error_at: datetime | None = None
    last_error: str | None = None
    rows_received: int | None = None
    duration_ms: int | None = None
    status: RunStatus | None = None

    def last_confirmed_at(self) -> datetime | None:
        """Último instante en que Bsale confirmó el dato por cualquier vía exitosa."""
        candidates = [
            t
            for t in (
                self.last_success_at,
                self.last_webhook_at,
                self.last_incremental_at,
                self.last_full_reconcile_at,
            )
            if t is not None
        ]
        return max(candidates) if candidates else None

    def evaluate(self, sla_seconds: int | None, now: datetime | None = None) -> FreshnessLevel:
        if sla_seconds is None:
            return FreshnessLevel.NO_SLA
        confirmed = self.last_confirmed_at()
        if confirmed is None:
            return FreshnessLevel.NEVER
        now = now or datetime.now(timezone.utc)
        age = (now - confirmed).total_seconds()
        return FreshnessLevel.FRESH if age <= sla_seconds else FreshnessLevel.STALE
