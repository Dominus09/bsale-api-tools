"""Resolución auditable del período de comparación comercial."""

from __future__ import annotations

import calendar
from datetime import date, timedelta


def shift_month_back(d: date) -> date:
    """Mueve una fecha al mes anterior, ajustando el día al último del mes si hace falta."""
    if d.month == 1:
        year, month = d.year - 1, 12
    else:
        year, month = d.year, d.month - 1
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, min(d.day, last_day))


def period_day_count(date_from: date, date_to: date) -> int:
    return (date_to - date_from).days + 1


def resolve_compare_period(
    date_from: date,
    date_to: date,
    *,
    compare_date_from: date | None = None,
    compare_date_to: date | None = None,
) -> tuple[date, date, str]:
    """
    Período anterior con el mismo largo que el actual.

    1. Manual si viene en filtros.
    2. Desplazamiento calendario −1 mes si conserva el largo (ej. 01–15 jul → 01–15 jun).
    3. Rolling inmediatamente anterior con N días iguales (fallback).
    """
    if compare_date_from is not None and compare_date_to is not None:
        return compare_date_from, compare_date_to, "manual"

    curr_days = period_day_count(date_from, date_to)
    cal_from = shift_month_back(date_from)
    cal_to = shift_month_back(date_to)
    if period_day_count(cal_from, cal_to) == curr_days:
        return cal_from, cal_to, "calendar_month"

    prev_to = date_from - timedelta(days=1)
    prev_from = prev_to - timedelta(days=curr_days - 1)
    return prev_from, prev_to, "rolling"


def compare_period_meta(
    date_from: date,
    date_to: date,
    *,
    compare_date_from: date | None = None,
    compare_date_to: date | None = None,
) -> dict[str, object]:
    prev_from, prev_to, method = resolve_compare_period(
        date_from,
        date_to,
        compare_date_from=compare_date_from,
        compare_date_to=compare_date_to,
    )
    curr_days = period_day_count(date_from, date_to)
    prev_days = period_day_count(prev_from, prev_to)
    return {
        "current": {"from": date_from.isoformat(), "to": date_to.isoformat(), "days": curr_days},
        "previous": {"from": prev_from.isoformat(), "to": prev_to.isoformat(), "days": prev_days},
        "method": method,
        "same_length": curr_days == prev_days,
    }
