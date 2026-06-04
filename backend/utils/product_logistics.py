"""Cálculos y consultas del maestro logístico (products_master)."""

from __future__ import annotations

from typing import Any

_LOGISTICS_STATS_SQL = """
SELECT
    COUNT(*)::bigint AS total,
    COUNT(*) FILTER (
        WHERE NULLIF(BTRIM(barcode), '') IS NOT NULL
    )::bigint AS with_barcode,
    COUNT(*) FILTER (
        WHERE units_per_box IS NOT NULL AND units_per_box > 0
    )::bigint AS with_units_per_box,
    COUNT(*) FILTER (WHERE supplier_id IS NOT NULL)::bigint AS with_supplier,
    COUNT(*) FILTER (
        WHERE weight_box_kg IS NOT NULL AND weight_box_kg > 0
    )::bigint AS with_weight,
    COUNT(*) FILTER (
        WHERE height_cm IS NOT NULL AND height_cm > 0
          AND width_cm IS NOT NULL AND width_cm > 0
          AND length_cm IS NOT NULL AND length_cm > 0
    )::bigint AS with_dimensions,
    COUNT(*) FILTER (WHERE logistics_completed = TRUE)::bigint AS logistics_completed
FROM bsale.products_master
WHERE is_active = TRUE
"""


def calc_weight_unit_kg(
    weight_box_kg: Any,
    units_per_box: Any,
) -> float | None:
    if weight_box_kg is None or units_per_box is None:
        return None
    try:
        w = float(weight_box_kg)
        u = float(units_per_box)
    except (TypeError, ValueError):
        return None
    if w <= 0 or u <= 0:
        return None
    return round(w / u, 6)


def calc_volume_m3(height_cm: Any, width_cm: Any, length_cm: Any) -> float | None:
    try:
        h = float(height_cm)
        w = float(width_cm)
        l = float(length_cm)
    except (TypeError, ValueError):
        return None
    if h <= 0 or w <= 0 or l <= 0:
        return None
    return round((h * w * l) / 1_000_000.0, 6)


def logistics_completeness_pct(
    *,
    total: int,
    with_supplier: int,
    with_units_per_box: int,
    with_weight: int,
    with_dimensions: int,
) -> float:
    """Promedio de 4 ejes logísticos (proveedor, CxC, peso, dimensiones)."""
    if total <= 0:
        return 0.0
    score = (
        with_supplier + with_units_per_box + with_weight + with_dimensions
    ) / (4.0 * total)
    return round(score * 100.0, 1)


def fetch_logistics_stats(cur: Any) -> dict[str, Any]:
    cur.execute(_LOGISTICS_STATS_SQL)
    row = cur.fetchone()
    if not row:
        return {}
    cols = [d[0] for d in cur.description]
    data = dict(zip(cols, row))
    total = int(data.get("total") or 0)
    ws = int(data.get("with_supplier") or 0)
    wupb = int(data.get("with_units_per_box") or 0)
    ww = int(data.get("with_weight") or 0)
    wd = int(data.get("with_dimensions") or 0)
    lc = int(data.get("logistics_completed") or 0)
    return {
        "total": total,
        "with_barcode": int(data.get("with_barcode") or 0),
        "with_units_per_box": wupb,
        "with_supplier": ws,
        "with_weight": ww,
        "with_dimensions": wd,
        "logistics_completed": lc,
        "completeness_pct": logistics_completeness_pct(
            total=total,
            with_supplier=ws,
            with_units_per_box=wupb,
            with_weight=ww,
            with_dimensions=wd,
        ),
    }
