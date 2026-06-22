"""Helpers listado recepciones Bsale (orden ASC documentado en respuestas reales)."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from backend.services.distribuidora.bsale_client import BsaleClient

RECEPTIONS_PATH = "/stocks/receptions.json"
LIST_LIMIT = 50


def day_start_ts(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def iter_day_starts(since_ts: int, until_ts: int | None = None) -> list[int]:
    """Timestamps UTC inicio de día para cada día en [since_ts, until_ts]."""
    until_ts = until_ts or int(datetime.now(timezone.utc).timestamp())
    start_d = datetime.fromtimestamp(since_ts, tz=timezone.utc).date()
    end_d = datetime.fromtimestamp(until_ts, tz=timezone.utc).date()
    out: list[int] = []
    d = start_d
    while d <= end_d:
        out.append(day_start_ts(d))
        d += timedelta(days=1)
    return out


def detect_page_order(items: list[dict]) -> str:
    if len(items) < 2:
        return "UNKNOWN"
    first_ts = int(items[0].get("admissionDate") or 0)
    last_ts = int(items[-1].get("admissionDate") or 0)
    if first_ts > last_ts:
        return "DESC"
    if first_ts < last_ts:
        return "ASC"
    return "FLAT"


def iter_receptions_for_sync(
    client: BsaleClient,
    *,
    since_ts: int,
    until_ts: int | None = None,
    limit: int = LIST_LIMIT,
    use_day_filter: bool = True,
) -> tuple[list[dict], dict]:
    """
    Itera recepciones con admissionDate >= since_ts.

    Estrategia:
    1. Por día con ``admissiondate`` (filtro oficial Bsale) — evita recorrer años de historial ASC.
    2. Si no hay resultados con filtro diario, fallback: paginación offset sin corte temprano.
    """
    until_ts = until_ts or int(datetime.now(timezone.utc).timestamp())
    meta: dict = {
        "strategy": None,
        "pages_read": 0,
        "api_total_count": None,
        "page_order_hint": None,
        "min_admission_ts": None,
        "max_admission_ts": None,
        "receptions_fetched": 0,
        "receptions_in_window": 0,
        "receptions_discarded_old": 0,
        "receptions_year_2026": 0,
    }
    collected: list[dict] = []
    seen_ids: set[int] = set()

    def _track(rec: dict, *, in_window: bool) -> None:
        adm_ts = int(rec.get("admissionDate") or 0)
        if not adm_ts:
            return
        meta["min_admission_ts"] = (
            adm_ts if meta["min_admission_ts"] is None else min(meta["min_admission_ts"], adm_ts)
        )
        meta["max_admission_ts"] = (
            adm_ts if meta["max_admission_ts"] is None else max(meta["max_admission_ts"], adm_ts)
        )
        if datetime.fromtimestamp(adm_ts, tz=timezone.utc).year >= 2026:
            meta["receptions_year_2026"] += 1
        if in_window:
            meta["receptions_in_window"] += 1
        else:
            meta["receptions_discarded_old"] += 1

    def _append_unique(items: list[dict], *, in_window: bool) -> None:
        for rec in items:
            rid = int(rec["id"])
            if rid in seen_ids:
                continue
            seen_ids.add(rid)
            collected.append(rec)
            meta["receptions_fetched"] += 1
            _track(rec, in_window=in_window)

    if use_day_filter:
        meta["strategy"] = "admissiondate_by_day"
        for day_ts in iter_day_starts(since_ts, until_ts):
            offset = 0
            while True:
                data = client.get(
                    RECEPTIONS_PATH,
                    {
                        "limit": limit,
                        "offset": offset,
                        "admissiondate": day_ts,
                        "expand": "[office]",
                    },
                )
                meta["pages_read"] += 1
                if meta["api_total_count"] is None and data.get("count") is not None:
                    meta["api_total_count"] = int(data["count"])
                items = data.get("items") or []
                if not items:
                    break
                if meta["page_order_hint"] is None:
                    meta["page_order_hint"] = detect_page_order(items)
                in_window = [r for r in items if int(r.get("admissionDate") or 0) >= since_ts]
                _append_unique(in_window, in_window=True)
                for r in items:
                    if int(r.get("admissionDate") or 0) < since_ts:
                        _track(r, in_window=False)
                offset += limit
                if len(items) < limit:
                    break
        if collected:
            return collected, meta

    meta["strategy"] = "offset_full_scan"
    offset = 0
    while True:
        data = client.get(
            RECEPTIONS_PATH,
            {"limit": limit, "offset": offset, "expand": "[office]"},
        )
        meta["pages_read"] += 1
        if meta["api_total_count"] is None and data.get("count") is not None:
            meta["api_total_count"] = int(data["count"])
        items = data.get("items") or []
        if not items:
            break
        if meta["page_order_hint"] is None:
            meta["page_order_hint"] = detect_page_order(items)
        for rec in items:
            adm_ts = int(rec.get("admissionDate") or 0)
            rid = int(rec["id"])
            if rid in seen_ids:
                continue
            if adm_ts < since_ts:
                meta["receptions_discarded_old"] += 1
                _track(rec, in_window=False)
                # ASC: no cortar paginación — seguir hasta el final del listado.
                continue
            seen_ids.add(rid)
            collected.append(rec)
            meta["receptions_fetched"] += 1
            _track(rec, in_window=True)
        offset += limit
        if len(items) < limit:
            break

    return collected, meta
