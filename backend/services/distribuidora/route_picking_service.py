"""Lista de picking por camión y día (despacho)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from backend.repositories.distribuidora import route_picking_repo as repo


def _serialize_row(d: dict[str, Any]) -> dict[str, Any]:
    out = dict(d)
    for k, v in list(out.items()):
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        elif isinstance(v, Decimal):
            out[k] = float(v)
    return out


def get_route_picking(*, planning_date: date, truck: str) -> dict[str, Any]:
    t = truck.strip()
    if not t:
        raise ValueError("truck es obligatorio")
    items, total_clients, total_amount = repo.refresh_and_list_route_picking(
        planning_date, t
    )
    return {
        "planning_date": planning_date.isoformat(),
        "truck": t,
        "items": [_serialize_row(x) for x in items],
        "total_clients": total_clients,
        "total_amount": float(total_amount),
    }
