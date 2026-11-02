"""Regresión: clasificación A/B/C de frescura costos (sin DB)."""

from __future__ import annotations

from datetime import date, datetime, timezone

from backend.services.analytics_v2.cost_freshness_diagnosis import (
    FreshnessSignals,
    classify_cost_freshness,
    windows_cover_after,
)


def test_windows_cover_after_detects_aug_oct() -> None:
    assert windows_cover_after(
        [
            "admission_date:2026-06-01..2026-08-31",
            "admission_date:2026-09-01..2026-10-12",
            "admission_date:2026-10-20..2026-10-26",
        ],
        after=date(2026, 7, 31),
    )
    assert not windows_cover_after(
        ["admission_date:2026-07-01..2026-07-31"],
        after=date(2026, 7, 31),
    )


def test_classify_verdict_a_plus_c_for_quillotana_case() -> None:
    now = datetime(2026, 10, 27, 18, 0, tzinfo=timezone.utc)
    result = classify_cost_freshness(
        FreshnessSignals(
            analytics_max_admission=date(2026, 7, 31),
            analytics_last_run_at=datetime(2026, 8, 3, 19, 16, 33, tzinfo=timezone.utc),
            analytics_last_message="+3 recepciones, +9 líneas history_only",
            raw_receptions_last_success_at=datetime(
                2026, 10, 27, 9, 12, 20, tzinfo=timezone.utc
            ),
            raw_receptions_status="SUCCESS",
            raw_window_covers_after_analytics=True,
            sync_cost_receptions_is_manual=True,
        ),
        now=now,
    )
    assert "A" in result["letters"]
    assert "C" in result["letters"]
    assert "B" not in result["letters"]
    assert result["raw_fresh"] is True
    assert result["analytics_stale"] is True
    assert result["costos_v2_blocked_by_analytics_lag"] is False
    assert result["costos_v2_source"] == "bsale_raw"


def test_classify_verdict_b_when_raw_stale() -> None:
    now = datetime(2026, 10, 27, 18, 0, tzinfo=timezone.utc)
    result = classify_cost_freshness(
        FreshnessSignals(
            analytics_max_admission=date(2026, 7, 31),
            analytics_last_run_at=None,
            analytics_last_message=None,
            raw_receptions_last_success_at=datetime(
                2026, 10, 1, 9, 0, tzinfo=timezone.utc
            ),
            raw_receptions_status="SUCCESS",
            raw_window_covers_after_analytics=True,
            sync_cost_receptions_is_manual=True,
        ),
        now=now,
        raw_stale_hours=36,
    )
    assert "B" in result["letters"]
    assert result["raw_fresh"] is False
