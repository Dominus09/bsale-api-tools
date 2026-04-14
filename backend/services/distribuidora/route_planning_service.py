"""Planificación de rutas: asignar OC a camiones por día."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import psycopg2
from psycopg2 import errors as pg_errors

from backend.repositories.distribuidora import route_planning_repo as repo


class MissingDocumentsError(Exception):
    """``document_id`` solicitados que no existen en la vista enriquecida."""

    def __init__(self, document_ids: set[int]) -> None:
        self.document_ids = document_ids


class AlreadyPlannedError(Exception):
    """``document_id`` ya planificados para esa fecha."""

    def __init__(self, document_ids: set[int]) -> None:
        self.document_ids = document_ids


def _serialize_row(d: dict[str, Any]) -> dict[str, Any]:
    out = dict(d)
    for k, v in list(out.items()):
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        elif isinstance(v, Decimal):
            out[k] = float(v)
    return out


def create_route_planning(
    *,
    planning_date: date,
    document_ids: list[int],
    truck: str,
) -> dict[str, Any]:
    t = truck.strip()
    if not t:
        raise ValueError("truck es obligatorio")
    ids = list(dict.fromkeys(int(x) for x in document_ids))
    if not ids:
        raise ValueError("document_ids no puede estar vacío")

    enriched = repo.fetch_enriched_orders_by_document_ids(ids)
    found = {int(r["document_id"]) for r in enriched}
    missing = set(ids) - found
    if missing:
        raise MissingDocumentsError(missing)

    already = repo.existing_planned_document_ids(planning_date, ids)
    if already:
        raise AlreadyPlannedError(already)

    try:
        n = repo.insert_planning_rows(planning_date, t, enriched)
    except pg_errors.UniqueViolation:
        raise AlreadyPlannedError(set(ids)) from None
    except psycopg2.Error:
        raise

    items, total_clients, total_amount = repo.list_route_planning(planning_date, t)
    inserted_docs = set(ids)
    new_items = [_serialize_row(x) for x in items if int(x["document_id"]) in inserted_docs]

    return {
        "inserted": n,
        "planning_date": planning_date.isoformat(),
        "truck": t,
        "items": new_items,
        "total_clients": total_clients,
        "total_amount": float(total_amount),
    }


def get_route_planning(
    *,
    planning_date: date,
    truck: str | None = None,
) -> dict[str, Any]:
    truck_f = truck.strip() if truck and truck.strip() else None
    items, total_clients, total_amount = repo.list_route_planning(planning_date, truck_f)
    return {
        "planning_date": planning_date.isoformat(),
        "truck": truck_f,
        "items": [_serialize_row(x) for x in items],
        "total_clients": total_clients,
        "total_amount": float(total_amount),
    }


def patch_route_planning(
    row_id: int,
    *,
    truck: str | None = None,
    status: str | None = None,
) -> dict[str, Any] | None:
    if truck is not None:
        truck = truck.strip()
        if not truck:
            raise ValueError("truck no puede quedar vacío")
    if truck is None and status is None:
        raise ValueError("Debe enviar truck y/o status")

    row = repo.update_route_planning(row_id, truck=truck, status=status)
    return _serialize_row(row) if row else None


def delete_route_planning_row(row_id: int) -> bool:
    return repo.delete_route_planning(row_id)
