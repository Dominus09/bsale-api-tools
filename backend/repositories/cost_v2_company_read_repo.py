"""Repositorio read-only Costos V2 consolidado por empresa (E.7.3).

Costo vigente = última recepción calculable (corrected_gross_cost NOT NULL)
hasta date_to, en cualquier oficina con cobertura V2.

Último cambio = último costo calculable DISTINTO anterior (no rn=2).

date_from NO elimina el costo vigente; limita cambios del periodo, historial
mostrado y conteos de actividad.
"""

from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import Any, Callable

from backend.schemas.cost_v2_company_read import (
    CALCULATION_VERSION_PIN,
    COMPANY_REVIEW_STATUSES,
    COMPANY_REVIEW_WARNINGS,
    COST_CONTROL_OFFICE_IDS_BY_COMPANY,
    CostV2ReadValidationError,
)
from backend.schemas.cost_v2_read import date_to_exclusive, escape_ilike
from backend.services.analytics.cost_audit_models import coerce_optional_decimal
from backend.services.analytics.validate_distribuidora_source import (
    assert_sql_is_read_only,
)

QueryExecutor = Callable[[str, tuple[Any, ...]], list[dict[str, Any]]]
logger = logging.getLogger(__name__)


class CostV2CompanyReadRepository:
    def __init__(self, executor: QueryExecutor) -> None:
        self._execute = executor

    def _run(self, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        assert_sql_is_read_only(sql)
        try:
            return self._execute(sql, params)
        except Exception as exc:
            # Sin SQL completo ni secrets; solo tipo y contexto de tamaño de params
            logger.exception(
                "cost_v2_company_repo_error exc_type=%s exc=%s param_count=%s",
                type(exc).__name__,
                str(exc),
                len(params),
            )
            raise

    def _money_keys(self, row: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
        out = dict(row)
        for key in keys:
            if key in out:
                out[key] = coerce_optional_decimal(out.get(key))
        return out

    def list_active_offices(self, *, company_id: int) -> list[dict[str, Any]]:
        """Oficinas operativas de control de costos (sin BAJAS)."""
        preferred = COST_CONTROL_OFFICE_IDS_BY_COMPANY.get(int(company_id))
        if preferred:
            rows = self._run(
                """
SELECT
    o.bsale_id AS office_id,
    COALESCE(NULLIF(TRIM(o.name), ''), 'Oficina ' || o.bsale_id::text) AS office_name
FROM bsale.offices o
WHERE o.company_id = %s
  AND o.state = 0
  AND o.bsale_id = ANY(%s)
ORDER BY o.bsale_id ASC
""".strip(),
                (int(company_id), list(preferred)),
            )
            if rows:
                return [
                    {
                        "office_id": int(r["office_id"]),
                        "office_name": r.get("office_name") or f"Oficina {r['office_id']}",
                    }
                    for r in rows
                ]
        # Fallback: activas reales excluyendo BAJAS
        rows = self._run(
            """
SELECT
    o.bsale_id AS office_id,
    COALESCE(NULLIF(TRIM(o.name), ''), 'Oficina ' || o.bsale_id::text) AS office_name
FROM bsale.offices o
WHERE o.company_id = %s
  AND o.state = 0
  AND COALESCE(o.name, '') NOT ILIKE '%%BAJA%%'
ORDER BY o.bsale_id ASC
""".strip(),
            (int(company_id),),
        )
        return [
            {
                "office_id": int(r["office_id"]),
                "office_name": r.get("office_name") or f"Oficina {r['office_id']}",
            }
            for r in rows
        ]

    def _company_products_cte(self) -> str:
        """CTE: current calculable + previous distinct + coverage + latest any."""
        return """
WITH active_offices AS (
    SELECT * FROM UNNEST(%s::int[], %s::text[]) AS t(office_id, office_name)
),
as_of_base AS (
    SELECT
        c.variant_id,
        c.history_id,
        c.company_id,
        c.office_id,
        COALESCE(
            NULLIF(TRIM(h.office_name), ''),
            ao.office_name,
            'Oficina ' || c.office_id::text
        ) AS office_name,
        h.admission_date::date AS admission_date,
        h.document_number,
        h.document,
        h.barcode,
        h.product_name,
        h.variant_name,
        c.stored_cost_net,
        c.stored_gross_cost,
        c.corrected_gross_cost,
        c.calculated_iva_amount,
        c.additional_tax_amount_total,
        c.additional_taxes_json,
        c.total_tax_rate,
        c.iva_rate,
        c.effective_quality_status,
        c.warnings_json,
        c.tax_ids_source,
        c.tax_rates_source,
        c.tax_context_source,
        c.calculation_version,
        c.calculation_batch_id,
        c.calculated_at,
        c.source_history_fingerprint,
        c.tax_context_fingerprint,
        c.calculation_result_fingerprint,
        c.resolved_tax_ids_json
    FROM analytics.cost_reception_calculated c
    INNER JOIN analytics.cost_reception_history h
        ON h.id = c.history_id
    LEFT JOIN active_offices ao
        ON ao.office_id = c.office_id
    WHERE c.calculation_version = %s
      AND c.company_id = %s
      AND h.admission_date < %s
),
as_of AS (
    SELECT
        b.*,
        ROW_NUMBER() OVER (
            PARTITION BY b.variant_id
            ORDER BY b.admission_date DESC, b.history_id DESC
        ) AS rn_any
    FROM as_of_base b
),
calc_ranked AS (
    SELECT
        b.*,
        ROW_NUMBER() OVER (
            PARTITION BY b.variant_id
            ORDER BY b.admission_date DESC, b.history_id DESC
        ) AS rn_calc
    FROM as_of_base b
    WHERE b.corrected_gross_cost IS NOT NULL
),
current_calc AS (
    SELECT * FROM calc_ranked WHERE rn_calc = 1
),
latest_any AS (
    SELECT * FROM as_of WHERE rn_any = 1
),
prev_distinct AS (
    SELECT
        a.variant_id,
        a.history_id AS previous_distinct_history_id,
        a.corrected_gross_cost AS previous_distinct_cost,
        a.admission_date AS last_change_date,
        a.office_id AS previous_distinct_office_id,
        ROW_NUMBER() OVER (
            PARTITION BY a.variant_id
            ORDER BY a.admission_date DESC, a.history_id DESC
        ) AS rn_prev
    FROM calc_ranked a
    INNER JOIN current_calc cur
        ON cur.variant_id = a.variant_id
    WHERE a.rn_calc > 1
      AND a.corrected_gross_cost IS DISTINCT FROM cur.corrected_gross_cost
),
office_latest_calc AS (
    SELECT
        c.variant_id,
        c.office_id,
        COALESCE(
            NULLIF(TRIM(h.office_name), ''),
            ao.office_name,
            'Oficina ' || c.office_id::text
        ) AS office_name,
        c.corrected_gross_cost,
        h.admission_date::date AS admission_date,
        c.history_id,
        ROW_NUMBER() OVER (
            PARTITION BY c.variant_id, c.office_id
            ORDER BY h.admission_date DESC, h.id DESC
        ) AS rn_off
    FROM analytics.cost_reception_calculated c
    INNER JOIN analytics.cost_reception_history h
        ON h.id = c.history_id
    LEFT JOIN active_offices ao
        ON ao.office_id = c.office_id
    WHERE c.calculation_version = %s
      AND c.company_id = %s
      AND h.admission_date < %s
      AND c.corrected_gross_cost IS NOT NULL
),
office_stats AS (
    SELECT
        ol.variant_id,
        COUNT(*)::int AS offices_with_current_cost,
        COUNT(DISTINCT ol.corrected_gross_cost)::int AS distinct_office_costs,
        BOOL_OR(
            cur.corrected_gross_cost IS NOT NULL
            AND ol.corrected_gross_cost IS DISTINCT FROM cur.corrected_gross_cost
        ) AS has_office_difference
    FROM office_latest_calc ol
    LEFT JOIN current_calc cur ON cur.variant_id = ol.variant_id
    WHERE ol.rn_off = 1
    GROUP BY ol.variant_id
),
v2_coverage AS (
    SELECT
        c.variant_id,
        COUNT(DISTINCT c.office_id)::int AS offices_with_v2_data
    FROM analytics.cost_reception_calculated c
    INNER JOIN analytics.cost_reception_history h
        ON h.id = c.history_id
    WHERE c.calculation_version = %s
      AND c.company_id = %s
      AND h.admission_date < %s
    GROUP BY c.variant_id
),
period_activity AS (
    SELECT
        c.variant_id,
        COUNT(*)::int AS receptions_in_period,
        MAX(h.admission_date)::date AS last_reception_in_period
    FROM analytics.cost_reception_calculated c
    INNER JOIN analytics.cost_reception_history h
        ON h.id = c.history_id
    WHERE c.calculation_version = %s
      AND c.company_id = %s
      AND h.admission_date >= %s
      AND h.admission_date < %s
    GROUP BY c.variant_id
),
active_count AS (
    SELECT COUNT(*)::int AS active_offices_count FROM active_offices
),
products AS (
    SELECT
        la.variant_id,
        la.company_id,
        COALESCE(cur.barcode, la.barcode) AS barcode,
        COALESCE(cur.product_name, la.product_name) AS product_name,
        COALESCE(cur.variant_name, la.variant_name) AS variant_name,
        cur.history_id AS current_history_id,
        cur.corrected_gross_cost AS current_cost,
        cur.corrected_gross_cost AS current_cost_raw,
        cur.admission_date AS current_admission_date,
        cur.office_id AS current_office_id,
        cur.office_name AS current_office_name,
        cur.document_number AS current_document_number,
        cur.effective_quality_status AS current_quality_status,
        cur.warnings_json AS current_warnings_json,
        cur.stored_cost_net AS current_stored_cost_net,
        cur.stored_gross_cost AS current_stored_gross_cost,
        cur.calculated_iva_amount AS current_calculated_iva_amount,
        cur.additional_tax_amount_total AS current_additional_tax_amount_total,
        cur.additional_taxes_json AS current_additional_taxes_json,
        cur.total_tax_rate AS current_total_tax_rate,
        cur.iva_rate AS current_iva_rate,
        cur.tax_ids_source,
        cur.tax_rates_source,
        cur.tax_context_source,
        cur.calculation_version,
        cur.calculation_batch_id,
        cur.calculated_at AS last_calculated_at,
        cur.source_history_fingerprint,
        cur.tax_context_fingerprint,
        cur.calculation_result_fingerprint,
        cur.resolved_tax_ids_json,
        la.history_id AS latest_history_id,
        la.admission_date AS last_reception_date,
        la.corrected_gross_cost AS latest_corrected_gross_cost,
        la.effective_quality_status AS latest_quality_status,
        pd.previous_distinct_history_id,
        pd.previous_distinct_cost,
        pd.last_change_date,
        CASE
            WHEN cur.corrected_gross_cost IS NOT NULL
             AND pd.previous_distinct_cost IS NOT NULL
            THEN cur.corrected_gross_cost - pd.previous_distinct_cost
            ELSE NULL
        END AS change_amount,
        CASE
            WHEN cur.corrected_gross_cost IS NOT NULL
             AND pd.previous_distinct_cost IS NOT NULL
             AND pd.previous_distinct_cost <> 0
            THEN (
                (cur.corrected_gross_cost - pd.previous_distinct_cost)
                / pd.previous_distinct_cost
            ) * 100
            ELSE NULL
        END AS change_percent,
        (pd.previous_distinct_cost IS NOT NULL) AS has_comparable_cost,
        COALESCE(os.offices_with_current_cost, 0) AS offices_with_current_cost,
        COALESCE(vc.offices_with_v2_data, 0) AS offices_with_v2_data,
        (SELECT active_offices_count FROM active_count) AS active_offices_count,
        COALESCE(os.has_office_difference, FALSE) AS has_office_difference,
        COALESCE(pa.receptions_in_period, 0) AS receptions_in_period,
        (
            cur.corrected_gross_cost IS NULL
            OR la.corrected_gross_cost IS NULL
            OR cur.effective_quality_status = ANY(%s)
            OR COALESCE(cur.warnings_json, '[]'::jsonb) ?| %s::text[]
            OR (
                la.history_id IS DISTINCT FROM cur.history_id
                AND la.corrected_gross_cost IS NULL
            )
        ) AS requires_review
    FROM latest_any la
    LEFT JOIN current_calc cur
        ON cur.variant_id = la.variant_id
    LEFT JOIN prev_distinct pd
        ON pd.variant_id = la.variant_id
       AND pd.rn_prev = 1
    LEFT JOIN office_stats os
        ON os.variant_id = la.variant_id
    LEFT JOIN v2_coverage vc
        ON vc.variant_id = la.variant_id
    LEFT JOIN period_activity pa
        ON pa.variant_id = la.variant_id
)
""".strip()

    def _cte_params(
        self,
        *,
        company_id: int,
        date_from: date,
        date_to: date,
        offices: list[dict[str, Any]],
    ) -> list[Any]:
        date_to_excl = date_to_exclusive(date_to)
        office_ids = [int(o["office_id"]) for o in offices]
        office_names = [str(o["office_name"]) for o in offices]
        review_statuses = list(COMPANY_REVIEW_STATUSES)
        review_warnings = list(COMPANY_REVIEW_WARNINGS)
        # Params order must match %s in CTE:
        # active_offices unnest, then as_of (ver, company, date_to_excl) x3 blocks,
        # period (ver, company, date_from, date_to_excl), review arrays
        return [
            office_ids,
            office_names,
            # as_of
            CALCULATION_VERSION_PIN,
            int(company_id),
            date_to_excl,
            # office_latest_calc
            CALCULATION_VERSION_PIN,
            int(company_id),
            date_to_excl,
            # v2_coverage
            CALCULATION_VERSION_PIN,
            int(company_id),
            date_to_excl,
            # period_activity
            CALCULATION_VERSION_PIN,
            int(company_id),
            date_from,
            date_to_excl,
            # requires_review
            review_statuses,
            review_warnings,
        ]

    def list_company_products(
        self,
        *,
        company_id: int,
        date_from: date,
        date_to: date,
        limit: int,
        sort: str = "latest_reception",
        cursor: dict[str, Any] | None = None,
        search: str | None = None,
        barcode: str | None = None,
        warning: str | None = None,
        movement: str | None = None,
        situation: str | None = None,
        only_relevant_changes: bool = False,
        min_abs_change_percent: Decimal | None = None,
        change_threshold_percent: Decimal | None = None,
    ) -> list[dict[str, Any]]:
        offices = self.list_active_offices(company_id=company_id)
        if not offices:
            return []
        params = self._cte_params(
            company_id=company_id,
            date_from=date_from,
            date_to=date_to,
            offices=offices,
        )
        filters: list[str] = ["p.variant_id IS NOT NULL"]
        if search:
            term = escape_ilike(search.strip())
            like = f"%{term}%"
            filters.append(
                """(
                    COALESCE(p.barcode, '') ILIKE %s ESCAPE '\\'
                 OR COALESCE(p.product_name, '') ILIKE %s ESCAPE '\\'
                 OR COALESCE(p.variant_name, '') ILIKE %s ESCAPE '\\'
                )"""
            )
            params.extend([like, like, like])
        if barcode:
            filters.append("TRIM(COALESCE(p.barcode, '')) = %s")
            params.append(barcode.strip())
        if warning:
            filters.append("COALESCE(p.current_warnings_json, '[]'::jsonb) ? %s")
            params.append(warning)
        if movement == "up":
            filters.append("p.change_amount IS NOT NULL AND p.change_amount > 0")
        elif movement == "down":
            filters.append("p.change_amount IS NOT NULL AND p.change_amount < 0")
        elif movement == "flat":
            filters.append(
                "(p.has_comparable_cost IS TRUE AND (p.change_amount IS NULL OR p.change_amount = 0))"
            )
        if situation == "requires_review":
            filters.append("p.requires_review IS TRUE")
        elif situation == "office_difference":
            filters.append("p.has_office_difference IS TRUE")
        elif situation == "partial_coverage":
            filters.append("p.offices_with_v2_data < p.active_offices_count")
        thr = change_threshold_percent
        if only_relevant_changes:
            thr = thr if thr is not None else Decimal("10")
            filters.append(
                """(
                    p.change_percent IS NOT NULL
                    AND ABS(p.change_percent) >= %s
                    AND p.last_change_date IS NOT NULL
                    AND p.last_change_date >= %s
                    AND p.last_change_date < %s
                )"""
            )
            params.extend([thr, date_from, date_to_exclusive(date_to)])
        elif min_abs_change_percent is not None:
            filters.append(
                "p.change_percent IS NOT NULL AND ABS(p.change_percent) >= %s"
            )
            params.append(min_abs_change_percent)

        if cursor:
            c_sort = cursor.get("sort") or sort
            if c_sort != sort:
                raise CostV2ReadValidationError(
                    "cursor sort mismatch",
                    error_type="invalid_cursor",
                )
            vid = int(cursor["variant_id"])
            if sort == "latest_reception":
                filters.append(
                    "(p.last_reception_date, p.variant_id) < (%s::date, %s::bigint)"
                )
                params.extend([cursor.get("admission_date"), vid])
            elif sort == "product":
                filters.append(
                    "(LOWER(COALESCE(p.product_name, '')), p.variant_id) > "
                    "(LOWER(%s), %s::bigint)"
                )
                params.extend([cursor.get("product_name") or "", vid])
            elif sort == "pct_increase":
                filters.append(
                    """(
                        (p.change_percent IS NOT NULL AND %s::numeric IS NOT NULL
                         AND (p.change_percent, p.variant_id) < (%s::numeric, %s::bigint))
                     OR (p.change_percent IS NULL AND %s::numeric IS NOT NULL)
                     OR (p.change_percent IS NULL AND %s::numeric IS NULL
                         AND p.variant_id < %s::bigint)
                    )"""
                )
                pct = cursor.get("change_percent")
                params.extend([pct, pct, vid, pct, pct, vid])
            elif sort == "pct_decrease":
                filters.append(
                    """(
                        (p.change_percent IS NOT NULL AND %s::numeric IS NOT NULL
                         AND (p.change_percent, p.variant_id) > (%s::numeric, %s::bigint))
                     OR (p.change_percent IS NULL AND %s::numeric IS NOT NULL)
                     OR (p.change_percent IS NULL AND %s::numeric IS NULL
                         AND p.variant_id < %s::bigint)
                    )"""
                )
                pct = cursor.get("change_percent")
                params.extend([pct, pct, vid, pct, pct, vid])
            elif sort == "abs_change":
                filters.append(
                    """(
                        (p.change_amount IS NOT NULL AND %s::numeric IS NOT NULL
                         AND (ABS(p.change_amount), p.variant_id)
                             < (%s::numeric, %s::bigint))
                     OR (p.change_amount IS NULL AND %s::numeric IS NOT NULL)
                     OR (p.change_amount IS NULL AND %s::numeric IS NULL
                         AND p.variant_id < %s::bigint)
                    )"""
                )
                abs_c = cursor.get("change_abs")
                params.extend([abs_c, abs_c, vid, abs_c, abs_c, vid])
            elif sort == "requires_review":
                filters.append(
                    """(
                        (p.requires_review IS TRUE AND %s IS FALSE)
                     OR (p.requires_review = %s AND p.variant_id < %s::bigint)
                     OR (p.requires_review IS FALSE AND %s IS FALSE
                         AND p.variant_id < %s::bigint)
                    )"""
                )
                rr = bool(cursor.get("requires_review"))
                params.extend([rr, rr, vid, rr, vid])
            elif sort == "office_difference":
                filters.append(
                    """(
                        (p.has_office_difference IS TRUE AND %s IS FALSE)
                     OR (p.has_office_difference = %s AND p.variant_id < %s::bigint)
                     OR (p.has_office_difference IS FALSE AND %s IS FALSE
                         AND p.variant_id < %s::bigint)
                    )"""
                )
                hd = bool(cursor.get("has_office_difference"))
                params.extend([hd, hd, vid, hd, vid])

        order = {
            "latest_reception": "p.last_reception_date DESC NULLS LAST, p.variant_id DESC",
            "pct_increase": "p.change_percent DESC NULLS LAST, p.variant_id DESC",
            "pct_decrease": "p.change_percent ASC NULLS LAST, p.variant_id DESC",
            "abs_change": "ABS(p.change_amount) DESC NULLS LAST, p.variant_id DESC",
            "product": "LOWER(COALESCE(p.product_name, '')) ASC, p.variant_id ASC",
            "requires_review": "p.requires_review DESC, p.variant_id DESC",
            "office_difference": "p.has_office_difference DESC, p.variant_id DESC",
        }.get(sort, "p.last_reception_date DESC NULLS LAST, p.variant_id DESC")

        where = " AND ".join(filters)
        sql = f"""
{self._company_products_cte()}
SELECT p.*
FROM products p
WHERE {where}
ORDER BY {order}
LIMIT %s
""".strip()
        params.append(int(limit) + 1)
        rows = self._run(sql, tuple(params))
        money = (
            "current_cost",
            "current_cost_raw",
            "previous_distinct_cost",
            "change_amount",
            "change_percent",
            "current_stored_cost_net",
            "current_stored_gross_cost",
            "current_calculated_iva_amount",
            "current_additional_tax_amount_total",
            "current_total_tax_rate",
            "current_iva_rate",
            "latest_corrected_gross_cost",
        )
        return [self._money_keys(r, money) for r in rows]

    def summarize_company_products(
        self,
        *,
        company_id: int,
        date_from: date,
        date_to: date,
        change_threshold_percent: Decimal,
    ) -> dict[str, Any]:
        offices = self.list_active_offices(company_id=company_id)
        if not offices:
            return {
                "total_products": 0,
                "products_with_current_cost": 0,
                "products_without_current_cost": 0,
                "relevant_changes": 0,
                "products_requiring_review": 0,
                "products_with_outlier": 0,
                "products_with_office_difference": 0,
                "active_offices_count": 0,
                "offices_with_v2_coverage": 0,
                "latest_reception_date": None,
                "latest_sync_or_calculation_at": None,
                "change_threshold_percent": change_threshold_percent,
            }
        params = self._cte_params(
            company_id=company_id,
            date_from=date_from,
            date_to=date_to,
            offices=offices,
        )
        sql = f"""
{self._company_products_cte()}
SELECT
    COUNT(*)::int AS total_products,
    COUNT(*) FILTER (WHERE p.current_cost IS NOT NULL)::int AS products_with_current_cost,
    COUNT(*) FILTER (WHERE p.current_cost IS NULL)::int AS products_without_current_cost,
    COUNT(*) FILTER (
        WHERE p.change_percent IS NOT NULL
          AND ABS(p.change_percent) >= %s
          AND p.last_change_date IS NOT NULL
          AND p.last_change_date >= %s
          AND p.last_change_date < %s
    )::int AS relevant_changes,
    COUNT(*) FILTER (WHERE p.requires_review IS TRUE)::int AS products_requiring_review,
    COUNT(*) FILTER (
        WHERE COALESCE(p.current_warnings_json, '[]'::jsonb) ? 'suspicious_outlier'
    )::int AS products_with_outlier,
    COUNT(*) FILTER (WHERE p.has_office_difference IS TRUE)::int
        AS products_with_office_difference,
    MAX(p.active_offices_count)::int AS active_offices_count,
    (
        SELECT COUNT(DISTINCT c.office_id)::int
        FROM analytics.cost_reception_calculated c
        WHERE c.calculation_version = %s
          AND c.company_id = %s
    ) AS offices_with_v2_coverage,
    MAX(p.last_reception_date) AS latest_reception_date,
    MAX(p.last_calculated_at) AS latest_sync_or_calculation_at
FROM products p
WHERE p.variant_id IS NOT NULL
""".strip()
        params.extend(
            [
                change_threshold_percent,
                date_from,
                date_to_exclusive(date_to),
                CALCULATION_VERSION_PIN,
                int(company_id),
            ]
        )
        rows = self._run(sql, tuple(params))
        row = rows[0] if rows else {}
        return {
            "total_products": int(row.get("total_products") or 0),
            "products_with_current_cost": int(row.get("products_with_current_cost") or 0),
            "products_without_current_cost": int(
                row.get("products_without_current_cost") or 0
            ),
            "relevant_changes": int(row.get("relevant_changes") or 0),
            "products_requiring_review": int(row.get("products_requiring_review") or 0),
            "products_with_outlier": int(row.get("products_with_outlier") or 0),
            "products_with_office_difference": int(
                row.get("products_with_office_difference") or 0
            ),
            "active_offices_count": int(row.get("active_offices_count") or len(offices)),
            "offices_with_v2_coverage": int(row.get("offices_with_v2_coverage") or 0),
            "latest_reception_date": row.get("latest_reception_date"),
            "latest_sync_or_calculation_at": row.get("latest_sync_or_calculation_at"),
            "change_threshold_percent": change_threshold_percent,
        }

    def get_company_product(
        self,
        *,
        company_id: int,
        variant_id: int,
        date_from: date,
        date_to: date,
    ) -> dict[str, Any] | None:
        offices = self.list_active_offices(company_id=company_id)
        if not offices:
            return None
        params = self._cte_params(
            company_id=company_id,
            date_from=date_from,
            date_to=date_to,
            offices=offices,
        )
        sql = f"""
{self._company_products_cte()}
SELECT p.*
FROM products p
WHERE p.variant_id = %s
LIMIT 1
""".strip()
        params.append(int(variant_id))
        rows = self._run(sql, tuple(params))
        if not rows:
            return None
        money = (
            "current_cost",
            "current_cost_raw",
            "previous_distinct_cost",
            "change_amount",
            "change_percent",
            "current_stored_cost_net",
            "current_stored_gross_cost",
            "current_calculated_iva_amount",
            "current_additional_tax_amount_total",
            "current_total_tax_rate",
            "current_iva_rate",
            "latest_corrected_gross_cost",
        )
        return self._money_keys(rows[0], money)

    def list_company_product_history(
        self,
        *,
        company_id: int,
        variant_id: int,
        date_from: date,
        date_to: date,
        office_id: int | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        date_to_excl = date_to_exclusive(date_to)
        clauses = [
            "c.calculation_version = %s",
            "c.company_id = %s",
            "c.variant_id = %s",
            "h.admission_date >= %s",
            "h.admission_date < %s",
        ]
        params: list[Any] = [
            CALCULATION_VERSION_PIN,
            int(company_id),
            int(variant_id),
            date_from,
            date_to_excl,
        ]
        if office_id is not None:
            clauses.append("c.office_id = %s")
            params.append(int(office_id))
        where = " AND ".join(clauses)
        sql = f"""
SELECT
    c.history_id,
    c.company_id,
    c.office_id,
    COALESCE(NULLIF(TRIM(h.office_name), ''), 'Oficina ' || c.office_id::text)
        AS office_name,
    h.admission_date::date AS admission_date,
    h.document_number,
    h.barcode,
    h.product_name,
    h.variant_name,
    c.stored_cost_net,
    c.stored_gross_cost,
    c.corrected_gross_cost,
    c.calculated_iva_amount,
    c.additional_tax_amount_total,
    c.additional_taxes_json,
    c.total_tax_rate,
    c.effective_quality_status,
    c.warnings_json,
    c.calculation_version,
    c.calculation_batch_id,
    c.calculated_at,
    c.tax_ids_source,
    c.tax_rates_source,
    c.source_history_fingerprint,
    c.tax_context_fingerprint,
    c.calculation_result_fingerprint,
    c.resolved_tax_ids_json,
    LAG(c.corrected_gross_cost) OVER (
        ORDER BY h.admission_date ASC, h.id ASC
    ) AS prev_cost_in_series
FROM analytics.cost_reception_calculated c
INNER JOIN analytics.cost_reception_history h
    ON h.id = c.history_id
WHERE {where}
ORDER BY h.admission_date ASC, h.id ASC
LIMIT %s
""".strip()
        params.append(int(limit))
        rows = self._run(sql, tuple(params))
        money = (
            "stored_cost_net",
            "stored_gross_cost",
            "corrected_gross_cost",
            "calculated_iva_amount",
            "additional_tax_amount_total",
            "total_tax_rate",
            "prev_cost_in_series",
        )
        return [self._money_keys(r, money) for r in rows]

    def list_company_product_offices(
        self,
        *,
        company_id: int,
        variant_id: int,
        date_to: date,
        company_current_cost: Decimal | None,
    ) -> list[dict[str, Any]]:
        offices = self.list_active_offices(company_id=company_id)
        date_to_excl = date_to_exclusive(date_to)
        rows = self._run(
            """
WITH latest AS (
    SELECT
        c.office_id,
        COALESCE(NULLIF(TRIM(h.office_name), ''), 'Oficina ' || c.office_id::text)
            AS office_name,
        c.corrected_gross_cost,
        h.admission_date::date AS admission_date,
        c.history_id,
        c.effective_quality_status,
        c.warnings_json,
        ROW_NUMBER() OVER (
            PARTITION BY c.office_id
            ORDER BY
                CASE WHEN c.corrected_gross_cost IS NOT NULL THEN 0 ELSE 1 END,
                h.admission_date DESC,
                h.id DESC
        ) AS rn
    FROM analytics.cost_reception_calculated c
    INNER JOIN analytics.cost_reception_history h
        ON h.id = c.history_id
    WHERE c.calculation_version = %s
      AND c.company_id = %s
      AND c.variant_id = %s
      AND h.admission_date < %s
)
SELECT * FROM latest WHERE rn = 1
""".strip(),
            (
                CALCULATION_VERSION_PIN,
                int(company_id),
                int(variant_id),
                date_to_excl,
            ),
        )
        by_id = {int(r["office_id"]): r for r in rows}
        out: list[dict[str, Any]] = []
        for o in offices:
            oid = int(o["office_id"])
            hit = by_id.get(oid)
            if not hit:
                out.append(
                    {
                        "office_id": oid,
                        "office_name": o["office_name"],
                        "current_cost": None,
                        "admission_date": None,
                        "history_id": None,
                        "quality_status": None,
                        "warnings": [],
                        "diff_vs_company": None,
                        "has_v2_data": False,
                        "situation": "coverage_pending",
                    }
                )
                continue
            cost = coerce_optional_decimal(hit.get("corrected_gross_cost"))
            diff = None
            if cost is not None and company_current_cost is not None:
                diff = cost - company_current_cost
            situation = "aligned"
            if cost is None:
                situation = "no_calculable"
            elif company_current_cost is None:
                situation = "has_cost"
            elif diff is not None and diff != 0:
                situation = "different"
            elif cost is not None and company_current_cost is not None and cost == company_current_cost:
                # Solo “alineada” si hay ≥2 oficinas con costo — caller ajusta label
                situation = "aligned"
            out.append(
                {
                    "office_id": oid,
                    "office_name": hit.get("office_name") or o["office_name"],
                    "current_cost": cost,
                    "admission_date": hit.get("admission_date"),
                    "history_id": hit.get("history_id"),
                    "quality_status": hit.get("effective_quality_status"),
                    "warnings": hit.get("warnings_json"),
                    "diff_vs_company": diff,
                    "has_v2_data": True,
                    "situation": situation,
                }
            )
        return out
