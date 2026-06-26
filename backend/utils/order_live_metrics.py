"""Métricas live de OC para planificación: peso, montos y detección de desactualización."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from backend.utils.delivery_day_detect import delivery_day_label, resolve_delivery_day
from backend.utils.planning_sql_fragments import (
    LATEST_OBS_LATERAL_LIVE,
    ORDER_WEIGHT_METRICS_SQL,
    PLANNING_LAST_BS_UPDATE_EXPR,
    PLANNING_OBSERVACIONES_EXPR,
)

_LIVE_FIELDS_SQL = f"""
SELECT
    d.document_id,
    d.number AS oc,
    d.client_id,
    d.company_id,
    d.total_amount,
    {PLANNING_LAST_BS_UPDATE_EXPR} AS last_bs_update,
    d.updated_at AS last_erp_update,
    COALESCE(
        NULLIF(BTRIM(d.municipality), ''),
        NULLIF(BTRIM(d.city), ''),
        NULLIF(BTRIM(c.municipality), ''),
        NULLIF(BTRIM(c.city), '')
    ) AS municipality,
    COALESCE(
        NULLIF(BTRIM(d.city), ''),
        NULLIF(BTRIM(d.municipality), ''),
        NULLIF(BTRIM(c.city), ''),
        NULLIF(BTRIM(c.municipality), '')
    ) AS city,
    COALESCE(
        NULLIF(BTRIM(d.address), ''),
        NULLIF(BTRIM(c.address), '')
    ) AS address,
    {PLANNING_OBSERVACIONES_EXPR} AS observaciones,
    NULLIF(BTRIM(d.raw_data->>'comments'), '') AS comments,
    NULLIF(BTRIM(c.dia_atencion), '') AS dia_atencion,
    NULLIF(BTRIM(c.nombre_fantasia), '') AS nombre_fantasia,
    (c.lat IS NOT NULL AND c.lon IS NOT NULL) AS has_georef,
    c.lat::double precision AS lat,
    c.lon::double precision AS lng
FROM distribuidora.documents d
{LATEST_OBS_LATERAL_LIVE}
LEFT JOIN bsale.clients c
    ON c.company_id = d.company_id
   AND c.bsale_id = d.client_id
WHERE d.document_id = ANY(%s::bigint[])
"""


def _norm_text(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def _norm_city(value: Any) -> str | None:
    return _norm_text(value)


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    return None


def amounts_differ(live: Any, snapshot: Any, *, tolerance_clp: float = 1.0) -> bool:
    a = _to_float(live)
    b = _to_float(snapshot)
    if a is None or b is None:
        return False
    return abs(a - b) > tolerance_clp


def cities_differ(live: Any, snapshot: Any) -> bool:
    a = _norm_city(live)
    b = _norm_city(snapshot)
    if not a or not b:
        return False
    return a.casefold() != b.casefold()


def delivery_days_differ(live_day: Any, snapshot_day: Any) -> bool:
    a = _norm_text(live_day)
    b = _norm_text(snapshot_day)
    if not a or not b:
        return False
    return a.casefold() != b.casefold()


def bsale_modified_after_snapshot(
    last_bs_update: Any,
    snapshot_at: Any,
    *,
    tolerance_seconds: float = 2.0,
) -> bool:
    bs = _to_ts(last_bs_update)
    snap = _to_ts(snapshot_at)
    if bs is None or snap is None:
        return False
    return (bs - snap).total_seconds() > tolerance_seconds


def bsale_ahead_of_erp_sync(
    last_bs_update: Any,
    last_erp_update: Any,
    *,
    tolerance_seconds: float = 2.0,
) -> bool:
    """Bsale modificó la OC después del último sync ERP (sin snapshot de plan)."""
    bs = _to_ts(last_bs_update)
    erp = _to_ts(last_erp_update)
    if bs is None or erp is None:
        return False
    return (bs - erp).total_seconds() > tolerance_seconds


def enrich_delivery_day_fields(row: dict[str, Any]) -> None:
    day, source = resolve_delivery_day(
        row.get("observaciones"),
        row.get("comments"),
        row.get("dia_atencion"),
    )
    row["dia_entrega_detectado"] = day
    row["dia_entrega_fuente"] = source
    row["dia_entrega_label"] = delivery_day_label(day)


def evaluate_planning_staleness(
    *,
    live: dict[str, Any],
    snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Compara métricas live vs snapshot congelado (plan confirmado)."""
    reasons: list[str] = []
    snapshot_at = (snapshot or {}).get("created_at") or (snapshot or {}).get(
        "snapshot_at"
    )
    last_bs = live.get("last_bs_update")
    last_erp = snapshot_at or live.get("last_erp_update")

    if bsale_modified_after_snapshot(last_bs, snapshot_at):
        reasons.append("bsale_modificada")

    if amounts_differ(live.get("total_amount"), (snapshot or {}).get("oc_total_amount")):
        reasons.append("monto")

    live_city = live.get("city") or live.get("municipality")
    snap_city = (snapshot or {}).get("city")
    if cities_differ(live_city, snap_city):
        reasons.append("ciudad")

    live_day = live.get("dia_entrega_detectado")
    snap_day = (snapshot or {}).get("dia_entrega_detectado")
    if delivery_days_differ(live_day, snap_day):
        reasons.append("dia_entrega")

    snap_weight = (snapshot or {}).get("weight_kg")
    live_weight = live.get("weight_kg")
    if (
        snap_weight is not None
        and live_weight is not None
        and abs(float(live_weight) - float(snap_weight)) > 0.05
    ):
        reasons.append("peso")

    stale = bool(reasons)
    bsale_pending = bsale_ahead_of_erp_sync(
        last_bs,
        live.get("last_erp_update") if snapshot is None else snapshot_at,
    )

    return {
        "planning_stale": stale,
        "planning_stale_reasons": reasons,
        "bsale_updated_pending": bsale_pending or stale,
        "last_bs_update": last_bs,
        "last_erp_update": last_erp,
    }


def fetch_live_metrics_by_document_ids(
    cur,
    document_ids: list[int],
) -> dict[int, dict[str, Any]]:
    if not document_ids:
        return {}

    ids = list(dict.fromkeys(int(x) for x in document_ids))
    cur.execute(_LIVE_FIELDS_SQL, (ids,))
    cols = [c[0] for c in cur.description]
    by_id: dict[int, dict[str, Any]] = {}
    for row in cur.fetchall():
        item = dict(zip(cols, row))
        enrich_delivery_day_fields(item)
        by_id[int(item["document_id"])] = item

    cur.execute(ORDER_WEIGHT_METRICS_SQL, (ids,))
    weight_cols = [c[0] for c in cur.description]
    for row in cur.fetchall():
        w = dict(zip(weight_cols, row))
        doc_id = int(w["document_id"])
        entry = by_id.setdefault(doc_id, {"document_id": doc_id})
        peso = float(w["peso_total_kg"]) if w.get("peso_total_kg") is not None else 0.0
        entry["peso_total_kg"] = peso
        entry["weight_kg"] = peso
        entry["productos_sin_peso"] = int(w.get("productos_sin_peso") or 0)
        entry["porcentaje_cobertura_peso"] = float(w.get("porcentaje_cobertura_peso") or 0)

    _overlay_official_order_weights(by_id, ids)

    for entry in by_id.values():
        if "weight_kg" not in entry:
            entry["weight_kg"] = 0.0
            entry["peso_total_kg"] = 0.0
            entry["productos_sin_peso"] = 0
            entry["porcentaje_cobertura_peso"] = 0.0
        staleness = evaluate_planning_staleness(live=entry, snapshot=None)
        entry["bsale_updated_pending"] = staleness["bsale_updated_pending"]
    return by_id


def _overlay_official_order_weights(
    by_id: dict[int, dict[str, Any]],
    document_ids: list[int],
) -> None:
    """Superpone peso oficial desde módulo Peso de Órdenes."""
    try:
        from backend.services.order_weight_service import (
            ensure_order_weights,
            fetch_weights_by_document_ids,
        )

        ensure_order_weights(document_ids)
        weights = fetch_weights_by_document_ids(document_ids)
        for doc_id, w in weights.items():
            entry = by_id.setdefault(doc_id, {"document_id": doc_id})
            entry["peso_total_kg"] = w["peso_total_kg"]
            entry["weight_kg"] = w["weight_kg"]
            entry["productos_sin_peso"] = w["productos_sin_peso"]
            entry["porcentaje_cobertura_peso"] = w["porcentaje_cobertura_peso"]
            entry["cantidad_unidades"] = w.get("cantidad_unidades")
            entry["cantidad_cajas"] = w.get("cantidad_cajas")
            entry["peso_fuente"] = "order_weight_module"
    except Exception:
        pass


def overlay_snapshot_orders(
    orders: list[dict[str, Any]],
    live_by_id: dict[int, dict[str, Any]],
    *,
    freeze_weight: bool = False,
) -> list[dict[str, Any]]:
    """Superpone métricas live sobre filas con snapshot; conserva valores congelados en *_snapshot."""
    out: list[dict[str, Any]] = []
    for order in orders:
        row = dict(order)
        doc_id = int(row["oc_document_id"])
        live = live_by_id.get(doc_id) or {}
        staleness = evaluate_planning_staleness(live=live, snapshot=row)

        row["snapshot_oc_total_amount"] = row.get("oc_total_amount")
        row["snapshot_city"] = row.get("city")
        row["snapshot_at"] = row.get("created_at")
        frozen_peso = row.get("peso_total_kg")
        if freeze_weight and frozen_peso is not None:
            row["weight_kg"] = float(frozen_peso)
            row["peso_total_kg"] = float(frozen_peso)
            row["productos_sin_peso"] = row.get("productos_sin_peso")
            row["porcentaje_cobertura_peso"] = row.get("cobertura_logistica")
            row["weight_frozen"] = True
        else:
            row["weight_kg"] = live.get("weight_kg")
            row["peso_total_kg"] = live.get("peso_total_kg")
            row["productos_sin_peso"] = live.get("productos_sin_peso")
            row["porcentaje_cobertura_peso"] = live.get("porcentaje_cobertura_peso")
            row["weight_frozen"] = False
        row["last_bs_update"] = live.get("last_bs_update")
        row["last_erp_update"] = row.get("created_at")
        if freeze_weight and frozen_peso is not None:
            reasons = [
                r for r in staleness["planning_stale_reasons"] if r != "peso"
            ]
            row["planning_stale"] = bool(reasons)
            row["planning_stale_reasons"] = reasons
            row["bsale_updated_pending"] = staleness["bsale_updated_pending"]
        else:
            row["planning_stale"] = staleness["planning_stale"]
            row["planning_stale_reasons"] = staleness["planning_stale_reasons"]
            row["bsale_updated_pending"] = staleness["bsale_updated_pending"]

        if live.get("total_amount") is not None:
            row["oc_total_amount"] = live["total_amount"]
        live_city = live.get("city") or live.get("municipality")
        if live_city:
            row["city"] = live_city
        if live.get("observaciones") is not None:
            row["observaciones"] = live.get("observaciones")
        row["dia_entrega_detectado"] = live.get("dia_entrega_detectado")
        row["dia_entrega_label"] = live.get("dia_entrega_label")
        row["dia_entrega_fuente"] = live.get("dia_entrega_fuente")
        out.append(row)
    return out
