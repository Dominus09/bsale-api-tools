"""Clasificación A/B/C del desfase de frescura de costos (solo lectura / puro).

A = RAW actualizado, analytics.cost_reception_history desactualizada
B = RAW también desactualizado (sync rota o ausente)
C = limitación intencional de proceso/alcance (job manual / piloto)

Costos V2 lee ``bsale_raw``; no depende de ``analytics.cost_reception_history``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Literal

Verdict = Literal["A", "B", "C", "AB", "AC", "BC", "ABC", "UNKNOWN"]


@dataclass(frozen=True)
class FreshnessSignals:
    """Señales observables sin SELECT a stock_receptions."""

    analytics_max_admission: date | None
    analytics_last_run_at: datetime | None
    analytics_last_message: str | None
    raw_receptions_last_success_at: datetime | None
    raw_receptions_status: str | None
    raw_window_covers_after_analytics: bool
    sync_cost_receptions_is_manual: bool = True


def _as_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def classify_cost_freshness(
    signals: FreshnessSignals,
    *,
    now: datetime | None = None,
    raw_stale_hours: int = 36,
) -> dict[str, Any]:
    """Devuelve veredicto y justificación a partir de señales ya leídas."""
    now_utc = _as_utc(now) or datetime.now(timezone.utc)
    raw_ok = (signals.raw_receptions_status or "").upper() == "SUCCESS"
    raw_success = _as_utc(signals.raw_receptions_last_success_at)
    raw_fresh = False
    if raw_ok and raw_success is not None:
        age_h = (now_utc - raw_success).total_seconds() / 3600.0
        raw_fresh = age_h <= float(raw_stale_hours)

    analytics_stale = False
    if signals.analytics_max_admission is not None:
        analytics_stale = signals.analytics_max_admission < now_utc.date()

    gap_after_analytics = bool(signals.raw_window_covers_after_analytics)

    letters: list[str] = []
    reasons: list[str] = []

    if raw_fresh and analytics_stale and gap_after_analytics:
        letters.append("A")
        reasons.append(
            "RAW stock_receptions sincroniza con SUCCESS reciente y hay ventanas "
            "reconciliadas posteriores al max(admission_date) de analytics"
        )
    if not raw_fresh:
        letters.append("B")
        reasons.append(
            "RAW stock_receptions no tiene SUCCESS fresco "
            f"(status={signals.raw_receptions_status!r}, "
            f"last_success={raw_success})"
        )
    if signals.sync_cost_receptions_is_manual and analytics_stale:
        letters.append("C")
        reasons.append(
            "sync_cost_receptions es MANUAL (piloto); el watermark analytics "
            "no avanza solo tras el último apply"
        )

    if not letters:
        verdict: Verdict = "UNKNOWN"
    elif len(letters) == 1:
        verdict = letters[0]  # type: ignore[assignment]
    else:
        verdict = "".join(letters)  # type: ignore[assignment]

    return {
        "verdict": verdict,
        "letters": letters,
        "reasons": reasons,
        "raw_fresh": raw_fresh,
        "analytics_stale": analytics_stale,
        "gap_after_analytics": gap_after_analytics,
        "analytics_max_admission": (
            signals.analytics_max_admission.isoformat()
            if signals.analytics_max_admission
            else None
        ),
        "raw_last_success_at": raw_success.isoformat() if raw_success else None,
        "costos_v2_source": "bsale_raw",
        "costos_v2_blocked_by_analytics_lag": False,
        "analytics_last_message": signals.analytics_last_message,
        "analytics_last_run_at": (
            _as_utc(signals.analytics_last_run_at).isoformat()
            if signals.analytics_last_run_at
            else None
        ),
    }


def windows_cover_after(
    window_scopes: list[str],
    *,
    after: date,
) -> bool:
    """True si algún scope admission_date:from..to incluye un día > after."""
    for scope in window_scopes:
        if not scope or not scope.startswith("admission_date:"):
            continue
        body = scope.split(":", 1)[1]
        if ".." not in body:
            continue
        start_s, end_s = body.split("..", 1)
        try:
            end = date.fromisoformat(end_s.strip())
        except ValueError:
            continue
        if end > after:
            return True
    return False
