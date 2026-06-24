"""Planificación de rutas: asignar OC a camiones por día."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import psycopg2
from psycopg2 import errors as pg_errors

from backend.db import get_connection
from backend.repositories.distribuidora import route_planning_repo as repo

ALLOWED_PLANNING_TRUCKS = frozenset({"HINO 2", "HINO 3", "HINO 4", "HYUNDAI"})


class MissingDocumentsError(Exception):
    """``document_id`` solicitados que no existen en la vista enriquecida."""

    def __init__(self, document_ids: set[int]) -> None:
        self.document_ids = document_ids


class AlreadyPlannedError(Exception):
    """``document_id`` ya planificados para esa fecha."""

    def __init__(self, document_ids: set[int]) -> None:
        self.document_ids = document_ids


class InvalidTruckError(Exception):
    def __init__(self, truck: str) -> None:
        self.truck = truck


def _serialize_row(d: dict[str, Any]) -> dict[str, Any]:
    out = dict(d)
    for k, v in list(out.items()):
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        elif isinstance(v, Decimal):
            out[k] = float(v)
    return out


def _blank_to_none(value: str | None) -> str | None:
    if value is None:
        return None
    s = value.strip()
    return s if s else None


def create_route_planning(
    *,
    planning_date: date,
    document_ids: list[int],
    truck: str,
    route_name: str | None = None,
    driver: str | None = None,
    assistant_1: str | None = None,
    assistant_2: str | None = None,
    departure_time: str | None = None,
    general_observation: str | None = None,
) -> dict[str, Any]:
    t = truck.strip()
    if not t:
        raise ValueError("truck es obligatorio")
    ids = list(dict.fromkeys(int(x) for x in document_ids))
    if not ids:
        raise ValueError("document_ids no puede estar vacío")

    rn = _blank_to_none(route_name)
    dr = _blank_to_none(driver)
    a1 = _blank_to_none(assistant_1)
    a2 = _blank_to_none(assistant_2)
    dt = _blank_to_none(departure_time)
    go = _blank_to_none(general_observation)

    enriched = repo.fetch_enriched_orders_by_document_ids(ids)
    found = {int(r["document_id"]) for r in enriched}
    missing = set(ids) - found
    if missing:
        raise MissingDocumentsError(missing)

    already = repo.existing_planned_document_ids(planning_date, ids)
    if already:
        raise AlreadyPlannedError(already)

    try:
        n = repo.insert_planning_rows(
            planning_date,
            t,
            enriched,
            route_name=rn,
            driver=dr,
            assistant_1=a1,
            assistant_2=a2,
            departure_time=dt,
            general_observation=go,
        )
    except pg_errors.UniqueViolation:
        raise AlreadyPlannedError(set(ids)) from None
    except psycopg2.Error:
        raise

    summary_header: dict[str, Any] = {}
    if rn is not None:
        summary_header["route_name"] = rn
    if dr is not None:
        summary_header["driver"] = dr
    if a1 is not None:
        summary_header["assistant_1"] = a1
    if a2 is not None:
        summary_header["assistant_2"] = a2
    if dt is not None:
        summary_header["departure_time"] = dt
    if go is not None:
        summary_header["general_observation"] = go
    if summary_header:
        for r in repo.list_route_planning_summaries(planning_date):
            if str(r["truck"]) == t:
                repo.update_route_planning_summary(int(r["id"]), summary_header)
                break

    items, total_clients, total_amount = repo.list_route_planning(planning_date, t)
    inserted_docs = set(ids)
    new_items = [_serialize_row(x) for x in items if int(x["document_id"]) in inserted_docs]

    summaries = [_serialize_row(x) for x in repo.list_route_planning_summaries(planning_date)]
    truck_summary = next((s for s in summaries if s["truck"] == t), None)

    return {
        "inserted": n,
        "planning_date": planning_date.isoformat(),
        "truck": t,
        "items": new_items,
        "total_clients": total_clients,
        "total_amount": float(total_amount),
        "summary": truck_summary,
    }


def create_route_planning_batch(
    *,
    planning_date: date,
    assignments: list[tuple[int, str]],
) -> dict[str, Any]:
    """Varias OC con distinto camión en una sola transacción."""
    if not assignments:
        raise ValueError("assignments no puede estar vacío")
    doc_ids = [int(d) for d, _ in assignments]
    if len(doc_ids) != len(set(doc_ids)):
        raise ValueError("Cada document_id debe aparecer una sola vez")
    for _, truck in assignments:
        t = truck.strip()
        if not t:
            raise ValueError("truck es obligatorio")
        if t not in ALLOWED_PLANNING_TRUCKS:
            raise InvalidTruckError(t)

    enriched = repo.fetch_enriched_orders_by_document_ids(doc_ids)
    found = {int(r["document_id"]) for r in enriched}
    missing = set(doc_ids) - found
    if missing:
        raise MissingDocumentsError(missing)

    already = repo.existing_planned_document_ids(planning_date, doc_ids)
    if already:
        raise AlreadyPlannedError(already)

    by_truck: dict[str, list[dict[str, Any]]] = {}
    by_id = {int(r["document_id"]): r for r in enriched}
    for doc_id, truck in assignments:
        t = truck.strip()
        by_truck.setdefault(t, []).append(by_id[int(doc_id)])

    conn = get_connection()
    cur = conn.cursor()
    inserted = 0
    try:
        for truck, rows in by_truck.items():
            inserted += repo.insert_planning_rows_cur(
                cur,
                planning_date,
                truck,
                rows,
            )
        conn.commit()
    except pg_errors.UniqueViolation:
        conn.rollback()
        raise AlreadyPlannedError(set(doc_ids)) from None
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    payload = get_route_planning(planning_date=planning_date, truck=None)
    return {
        "inserted": inserted,
        "planning_date": planning_date.isoformat(),
        "items": payload["items"],
        "summaries": payload["summaries"],
        "total_clients": payload["total_clients"],
        "total_amount": payload["total_amount"],
    }


def get_route_planning(
    *,
    planning_date: date,
    truck: str | None = None,
) -> dict[str, Any]:
    truck_f = truck.strip() if truck and truck.strip() else None
    items, total_clients, total_amount = repo.list_route_planning(planning_date, truck_f)
    doc_ids = [int(x["document_id"]) for x in items if x.get("document_id") is not None]
    live_by_id: dict[int, dict[str, Any]] = {}
    if doc_ids:
        conn = get_connection()
        try:
            cur = conn.cursor()
            from backend.utils.order_live_metrics import fetch_live_metrics_by_document_ids

            live_by_id = fetch_live_metrics_by_document_ids(cur, doc_ids)
            cur.close()
        finally:
            conn.close()

    serialized_items: list[dict[str, Any]] = []
    live_total = Decimal("0")
    for row in items:
        out = _serialize_row(row)
        live = live_by_id.get(int(row["document_id"])) or {}
        if live.get("total_amount") is not None:
            out["total_amount"] = float(live["total_amount"])
            out["snapshot_total_amount"] = float(row["total_amount"])
        if live.get("weight_kg") is not None:
            out["weight_kg"] = live["weight_kg"]
        if live.get("municipality"):
            out["municipality"] = live["municipality"]
        out["last_bs_update"] = live.get("last_bs_update")
        out["last_erp_update"] = live.get("last_erp_update")
        out["bsale_updated_pending"] = live.get("bsale_updated_pending")
        out["dia_entrega_label"] = live.get("dia_entrega_label")
        live_total += Decimal(str(out.get("total_amount") or 0))
        serialized_items.append(out)

    summaries = [_serialize_row(x) for x in repo.list_route_planning_summaries(planning_date)]
    if truck_f:
        summaries = [s for s in summaries if s.get("truck") == truck_f]
    return {
        "planning_date": planning_date.isoformat(),
        "truck": truck_f,
        "items": serialized_items,
        "total_clients": total_clients,
        "total_amount": float(live_total if live_by_id else total_amount),
        "summaries": summaries,
    }


def patch_route_planning(row_id: int, updates: dict[str, Any]) -> dict[str, Any] | None:
    if not updates:
        raise ValueError("Sin campos para actualizar")
    u = dict(updates)
    if "truck" in u and u["truck"] is not None:
        u["truck"] = str(u["truck"]).strip()
        if not u["truck"]:
            raise ValueError("truck no puede quedar vacío")
    row = repo.update_route_planning(row_id, u)
    return _serialize_row(row) if row else None


def list_route_planning_summaries(planning_date: date) -> dict[str, Any]:
    rows = repo.list_route_planning_summaries(planning_date)
    return {
        "planning_date": planning_date.isoformat(),
        "items": [_serialize_row(x) for x in rows],
    }


def patch_route_planning_summary(summary_id: int, updates: dict[str, Any]) -> dict[str, Any] | None:
    if not updates:
        raise ValueError("Sin campos para actualizar")
    row = repo.update_route_planning_summary(summary_id, updates)
    return _serialize_row(row) if row else None


def delete_route_planning_row(row_id: int) -> bool:
    return repo.delete_route_planning(row_id)
