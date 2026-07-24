"""Control actual de precios por lista (/margins).

Une variant_prices × listas × reglas con costo bruto máximo válido
(analytics.cost_reception_history → variant_cost fallback).

No usa ventas, documentos comerciales ni unidades vendidas.
No escribe precios en Bsale.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable

from backend.services.analytics.max_valid_gross_cost import (
    GrossCostCandidate,
    STALE_DAYS_DEFAULT,
    resolve_max_valid_gross_cost,
)
from backend.services.analytics.money import optional_decimal
from backend.services.analytics.price_list_control import compute_price_list_control_row

QueryExecutor = Callable[[str, tuple[Any, ...]], list[dict[str, Any]]]


BASE_ROWS_SQL = """
SELECT
    vp.company_id,
    p.product_type_id,
    pt.name AS product_type_name,
    p.name AS product_name,
    v.bsale_id AS variant_id,
    v.description AS variant_name,
    v.bar_code AS barcode,
    v.code AS sku,
    vp.price_list_id,
    pl.name AS price_list_name,
    stq.stock_quantity,
    vp.price_gross AS gross_price,
    mr.min_margin AS min_markup_pct,
    mr.max_margin AS max_markup_pct,
    CASE WHEN mr.company_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_rule
FROM bsale.variant_prices vp
INNER JOIN bsale.variants v
    ON v.company_id = vp.company_id
   AND v.bsale_id = vp.variant_id
INNER JOIN bsale.products p
    ON p.company_id = v.company_id
   AND p.bsale_id = v.product_id
LEFT JOIN bsale.price_lists pl
    ON pl.company_id = vp.company_id
   AND pl.bsale_id = vp.price_list_id
LEFT JOIN (
    SELECT company_id, variant_id, SUM(quantity_available)::numeric AS stock_quantity
    FROM bsale.stocks
    GROUP BY company_id, variant_id
) stq
    ON stq.company_id = vp.company_id
   AND stq.variant_id = vp.variant_id
LEFT JOIN bsale.product_types pt
    ON pt.company_id = p.company_id
   AND p.product_type_id IS NOT NULL
   AND pt.bsale_id = p.product_type_id
LEFT JOIN bsale.margin_rules mr
    ON mr.company_id = vp.company_id
   AND mr.price_list_id = vp.price_list_id
   AND mr.product_type_id IS NOT DISTINCT FROM p.product_type_id
   AND COALESCE(mr.active, TRUE) IS TRUE
WHERE vp.company_id = %s
""".strip()

RECEPTION_CANDIDATES_SQL = """
SELECT
    h.id,
    h.variant_id,
    h.cost_net,
    h.admission_date,
    h.reception_id,
    h.iva_amount,
    h.other_taxes,
    h.cost_bruto_erp,
    h.reception_type
FROM analytics.cost_reception_history h
WHERE h.company_id = %s
  AND h.variant_id = ANY(%s)
  AND (
        (h.cost_bruto_erp IS NOT NULL AND h.cost_bruto_erp > 0)
     OR (h.cost_net IS NOT NULL AND h.cost_net > 0)
  )
ORDER BY h.variant_id ASC, h.admission_date DESC, h.id DESC
LIMIT %s
""".strip()

VARIANT_COST_SQL = """
SELECT
    v.variant_id,
    v.average_cost_net,
    v.average_cost_gross,
    v.last_update,
    v.cost_source
FROM bsale.variant_cost v
WHERE v.company_id = %s
  AND v.variant_id = ANY(%s)
""".strip()


def _as_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def _dec_to_api(value: Decimal | None) -> float | None:
    if value is None:
        return None
    return float(value)


def _fetch_base_rows(
    executor: QueryExecutor,
    *,
    company_id: int,
    price_list_id: int | None,
) -> list[dict[str, Any]]:
    sql = BASE_ROWS_SQL
    params: list[Any] = [int(company_id)]
    if price_list_id is not None:
        sql += " AND vp.price_list_id = %s"
        params.append(int(price_list_id))
    sql += " ORDER BY vp.price_list_id, v.bsale_id"
    return executor(sql, tuple(params))


def _build_candidates_by_variant(
    rows: list[dict[str, Any]],
) -> dict[int, list[GrossCostCandidate]]:
    by_variant: dict[int, list[GrossCostCandidate]] = defaultdict(list)
    for row in rows:
        vid = int(row["variant_id"])
        by_variant[vid].append(
            GrossCostCandidate(
                gross_cost=optional_decimal(row.get("cost_bruto_erp")),
                net_cost=optional_decimal(row.get("cost_net")),
                iva_amount=optional_decimal(row.get("iva_amount")),
                other_taxes=optional_decimal(row.get("other_taxes")),
                cost_date=_as_date(row.get("admission_date")),
                cost_source="cost_reception_history",
                reception_type=(
                    str(row["reception_type"]) if row.get("reception_type") is not None else None
                ),
                reception_id=(
                    int(row["reception_id"]) if row.get("reception_id") is not None else None
                ),
                variant_id=vid,
            )
        )
    return by_variant


def _fallback_from_variant_cost(row: dict[str, Any] | None) -> GrossCostCandidate | None:
    if row is None:
        return None
    gross = optional_decimal(row.get("average_cost_gross"))
    if gross is None or gross <= 0:
        return None
    return GrossCostCandidate(
        gross_cost=gross,
        cost_date=_as_date(row.get("last_update")),
        cost_source="variant_cost.average_cost_gross",
        variant_id=int(row["variant_id"]),
    )


def list_price_list_control_rows(
    executor: QueryExecutor,
    *,
    company_id: int,
    price_list_id: int | None = None,
    as_of: date | None = None,
    stale_days: int = STALE_DAYS_DEFAULT,
    reception_limit: int = 20000,
) -> list[dict[str, Any]]:
    """Filas canónicas variante × lista con costo de referencia y cumplimiento."""
    today = as_of or date.today()
    base = _fetch_base_rows(executor, company_id=company_id, price_list_id=price_list_id)
    if not base:
        return []

    variant_ids = sorted({int(r["variant_id"]) for r in base})
    reception_rows = executor(
        RECEPTION_CANDIDATES_SQL,
        (int(company_id), variant_ids, int(reception_limit)),
    )
    candidates_by_variant = _build_candidates_by_variant(reception_rows)

    vc_rows = executor(VARIANT_COST_SQL, (int(company_id), variant_ids))
    vc_by_variant = {int(r["variant_id"]): r for r in vc_rows}

    cost_by_variant: dict[int, Any] = {}
    for vid in variant_ids:
        cost_by_variant[vid] = resolve_max_valid_gross_cost(
            candidates_by_variant.get(vid, []),
            as_of=today,
            stale_days=stale_days,
            fallback=_fallback_from_variant_cost(vc_by_variant.get(vid)),
        )

    out: list[dict[str, Any]] = []
    for row in base:
        vid = int(row["variant_id"])
        cost_res = cost_by_variant[vid]
        gross_price = optional_decimal(row.get("gross_price"))
        min_m = optional_decimal(row.get("min_markup_pct"))
        max_m = optional_decimal(row.get("max_markup_pct"))
        has_rule = bool(row.get("has_rule"))
        is_conflicting = cost_res.gross_cost_quality == "conflicting_gross_cost"

        metrics = compute_price_list_control_row(
            gross_price=gross_price,
            reference_gross_cost=cost_res.gross_cost,
            min_markup_pct=min_m,
            max_markup_pct=max_m,
            has_rule=has_rule,
            is_stale=cost_res.is_stale,
            is_conflicting=is_conflicting,
            is_outlier=cost_res.is_outlier,
        )

        out.append(
            {
                "company_id": int(row["company_id"]),
                "product_type_id": (
                    int(row["product_type_id"]) if row.get("product_type_id") is not None else None
                ),
                "product_type_name": row.get("product_type_name"),
                "product_name": row.get("product_name"),
                "variant_id": vid,
                "variant_name": row.get("variant_name"),
                "barcode": row.get("barcode"),
                "sku": row.get("sku"),
                "price_list_id": int(row["price_list_id"]),
                "price_list_name": row.get("price_list_name"),
                "stock_quantity": _dec_to_api(optional_decimal(row.get("stock_quantity"))),
                "gross_price": _dec_to_api(metrics.gross_price),
                "reference_gross_cost": _dec_to_api(metrics.reference_gross_cost),
                "cost_date": (
                    cost_res.cost_date.isoformat() if cost_res.cost_date is not None else None
                ),
                "cost_source": cost_res.cost_source,
                "cost_age_days": cost_res.cost_age_days,
                "gross_cost_quality": cost_res.gross_cost_quality,
                "is_outlier": cost_res.is_outlier,
                "is_stale": cost_res.is_stale,
                "resolution_reason": cost_res.resolution_reason,
                "actual_markup_pct": _dec_to_api(metrics.actual_markup_pct),
                "gross_margin_pct": _dec_to_api(metrics.gross_margin_pct),
                "min_markup_pct": _dec_to_api(metrics.min_markup_pct),
                "max_markup_pct": _dec_to_api(metrics.max_markup_pct),
                "minimum_recommended_gross_price": _dec_to_api(
                    metrics.minimum_recommended_gross_price
                ),
                "maximum_recommended_gross_price": _dec_to_api(
                    metrics.maximum_recommended_gross_price
                ),
                "price_adjustment_to_minimum": _dec_to_api(metrics.price_adjustment_to_minimum),
                "price_diff_vs_cost": _dec_to_api(metrics.price_diff_vs_cost),
                "status": metrics.status.value,
                "policy_compliance": (
                    metrics.policy_compliance.value if metrics.policy_compliance else None
                ),
                "has_rule": has_rule,
                # Compat lectura parcial con campos antiguos de la vista (sin ventas)
                "price": _dec_to_api(metrics.gross_price),
                "cost": _dec_to_api(metrics.reference_gross_cost),
                "margin_percent": _dec_to_api(metrics.actual_markup_pct),
                "min_margin_percent": _dec_to_api(metrics.min_markup_pct),
            }
        )
    return out


def summarize_price_list_control(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """KPIs de cumplimiento (sin métricas de venta)."""
    total = len(rows)
    counts: dict[str, int] = defaultdict(int)
    for r in rows:
        counts[str(r.get("status") or "unknown")] += 1

    within = counts.get("within_policy", 0)
    needs_review = (
        counts.get("below_minimum", 0)
        + counts.get("above_maximum", 0)
        + counts.get("cost_outlier", 0)
        + counts.get("conflicting_cost", 0)
        + counts.get("stale_cost", 0)
        + counts.get("missing_rule", 0)
        + counts.get("missing_cost", 0)
        + counts.get("missing_price", 0)
    )
    return {
        "evaluated_pairs": total,
        "within_policy": within,
        "within_policy_pct": round((within / total) * 100, 2) if total else None,
        "below_minimum": counts.get("below_minimum", 0),
        "above_maximum": counts.get("above_maximum", 0),
        "missing_rule": counts.get("missing_rule", 0),
        "missing_cost": counts.get("missing_cost", 0),
        "missing_price": counts.get("missing_price", 0),
        "stale_cost": counts.get("stale_cost", 0),
        "cost_outlier": counts.get("cost_outlier", 0),
        "conflicting_cost": counts.get("conflicting_cost", 0),
        "needs_review": needs_review,
        "by_status": dict(counts),
    }
