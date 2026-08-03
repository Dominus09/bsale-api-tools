"""Repositorio read-only Costos V2 (tabla calculada + history)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any, Callable

from backend.schemas.cost_v2_read import (
    CALCULATION_VERSION_PIN,
    date_to_exclusive,
    escape_ilike,
)
from backend.services.analytics.cost_audit_models import coerce_optional_decimal
from backend.services.analytics.validate_distribuidora_source import (
    assert_sql_is_read_only,
)

QueryExecutor = Callable[[str, tuple[Any, ...]], list[dict[str, Any]]]

# Columnas explícitas — nunca SELECT *
LIST_SELECT = """
SELECT
    c.history_id,
    c.company_id,
    c.office_id,
    c.variant_id,
    c.admission_date,
    c.calculation_version,
    c.calculation_batch_id,
    c.calculated_at,
    c.stored_cost_net,
    c.stored_quantity,
    c.stored_iva_amount,
    c.stored_other_taxes,
    c.stored_gross_cost,
    c.corrected_gross_cost,
    c.calculated_iva_amount,
    c.additional_tax_amount_total,
    c.additional_tax_rate_total,
    c.total_tax_rate,
    c.iva_tax_id,
    c.iva_rate,
    c.resolved_tax_ids_json,
    c.additional_taxes_json,
    c.reception_tax_ids_json,
    c.catalog_tax_ids_json,
    c.tax_ids_source,
    c.tax_rates_source,
    c.tax_resolution_quality,
    c.tax_context_source,
    c.tax_context_is_historical,
    c.tax_context_fingerprint,
    c.source_history_fingerprint,
    c.calculation_result_fingerprint,
    c.effective_quality_status,
    c.warnings_json,
    c.gross_difference_amount,
    h.document_number,
    h.document,
    h.reception_id,
    h.barcode,
    h.product_name,
    h.variant_name,
    h.created_at AS history_created_at
FROM analytics.cost_reception_calculated c
INNER JOIN analytics.cost_reception_history h
    ON h.id = c.history_id
""".strip()


class CostV2ReadRepository:
    """Solo SELECT. Fuente: cost_reception_calculated filtrada por versión."""

    def __init__(self, executor: QueryExecutor) -> None:
        self._execute = executor

    def _run(self, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        assert_sql_is_read_only(sql)
        return self._execute(sql, params)

    def _normalize_row(self, row: dict[str, Any]) -> dict[str, Any]:
        out = dict(row)
        for key in (
            "stored_cost_net",
            "stored_quantity",
            "stored_iva_amount",
            "stored_other_taxes",
            "stored_gross_cost",
            "corrected_gross_cost",
            "calculated_iva_amount",
            "additional_tax_amount_total",
            "additional_tax_rate_total",
            "total_tax_rate",
            "iva_rate",
            "gross_difference_amount",
        ):
            if key in out:
                out[key] = coerce_optional_decimal(out.get(key))
        adm = out.get("admission_date")
        if hasattr(adm, "date") and not isinstance(adm, date):
            out["admission_date"] = adm.date()  # type: ignore[union-attr]
        return out

    def _scope_clauses(
        self,
        *,
        company_id: int,
        office_id: int,
        date_from: date,
        date_to: date,
        statuses: list[str] | None = None,
        warnings: list[str] | None = None,
        barcode: str | None = None,
        variant_id: int | None = None,
        document_number: int | None = None,
        history_id: int | None = None,
        search: str | None = None,
    ) -> tuple[str, list[Any]]:
        date_to_excl = date_to_exclusive(date_to)
        clauses = [
            "c.calculation_version = %s",
            "c.company_id = %s",
            "c.office_id = %s",
            "h.admission_date >= %s",
            "h.admission_date < %s",
        ]
        params: list[Any] = [
            CALCULATION_VERSION_PIN,
            int(company_id),
            int(office_id),
            date_from,
            date_to_excl,
        ]
        if statuses:
            clauses.append("c.effective_quality_status = ANY(%s)")
            params.append(list(statuses))
        if warnings:
            # jsonb ?| text[] — elemento de array warnings_json
            clauses.append("c.warnings_json ?| %s::text[]")
            params.append(list(warnings))
        if barcode:
            clauses.append("TRIM(COALESCE(h.barcode, '')) = %s")
            params.append(barcode.strip())
        if variant_id is not None:
            clauses.append("c.variant_id = %s")
            params.append(int(variant_id))
        if document_number is not None:
            clauses.append(
                "(h.document_number = %s OR h.reception_id = %s OR h.document = %s)"
            )
            params.extend([int(document_number), int(document_number), str(document_number)])
        if history_id is not None:
            clauses.append("c.history_id = %s")
            params.append(int(history_id))
        if search:
            term = escape_ilike(search.strip())
            like = f"%{term}%"
            clauses.append(
                """(
                    h.barcode ILIKE %s ESCAPE '\\'
                 OR COALESCE(h.product_name, '') ILIKE %s ESCAPE '\\'
                 OR COALESCE(h.variant_name, '') ILIKE %s ESCAPE '\\'
                 OR CAST(h.document_number AS TEXT) ILIKE %s ESCAPE '\\'
                )"""
            )
            params.extend([like, like, like, like])
        return " AND ".join(clauses), params

    def list_receptions(
        self,
        *,
        company_id: int,
        office_id: int,
        date_from: date,
        date_to: date,
        limit: int,
        cursor_admission_date: date | None = None,
        cursor_history_id: int | None = None,
        statuses: list[str] | None = None,
        warnings: list[str] | None = None,
        barcode: str | None = None,
        variant_id: int | None = None,
        document_number: int | None = None,
        history_id: int | None = None,
        search: str | None = None,
    ) -> list[dict[str, Any]]:
        where, params = self._scope_clauses(
            company_id=company_id,
            office_id=office_id,
            date_from=date_from,
            date_to=date_to,
            statuses=statuses,
            warnings=warnings,
            barcode=barcode,
            variant_id=variant_id,
            document_number=document_number,
            history_id=history_id,
            search=search,
        )
        if cursor_admission_date is not None and cursor_history_id is not None:
            where += (
                " AND (h.admission_date, h.id) < (%s::date, %s::bigint)"
            )
            params.extend([cursor_admission_date, int(cursor_history_id)])
        sql = f"""
{LIST_SELECT}
WHERE {where}
ORDER BY h.admission_date DESC, h.id DESC
LIMIT %s
""".strip()
        params.append(int(limit) + 1)
        rows = self._run(sql, tuple(params))
        return [self._normalize_row(r) for r in rows]

    def get_reception(
        self,
        *,
        company_id: int,
        office_id: int,
        history_id: int,
    ) -> dict[str, Any] | None:
        sql = f"""
{LIST_SELECT}
WHERE c.calculation_version = %s
  AND c.company_id = %s
  AND c.office_id = %s
  AND c.history_id = %s
LIMIT 2
""".strip()
        rows = self._run(
            sql,
            (
                CALCULATION_VERSION_PIN,
                int(company_id),
                int(office_id),
                int(history_id),
            ),
        )
        if not rows:
            return None
        if len(rows) > 1:
            raise RuntimeError(
                "Invariante violada: más de una fila para history_id+calculation_version"
            )
        return self._normalize_row(rows[0])

    def summarize(
        self,
        *,
        company_id: int,
        office_id: int,
        date_from: date,
        date_to: date,
        statuses: list[str] | None = None,
        warnings: list[str] | None = None,
        barcode: str | None = None,
        variant_id: int | None = None,
        document_number: int | None = None,
        history_id: int | None = None,
        search: str | None = None,
    ) -> dict[str, Any]:
        where, params = self._scope_clauses(
            company_id=company_id,
            office_id=office_id,
            date_from=date_from,
            date_to=date_to,
            statuses=statuses,
            warnings=warnings,
            barcode=barcode,
            variant_id=variant_id,
            document_number=document_number,
            history_id=history_id,
            search=search,
        )
        sql = f"""
SELECT
    COUNT(*)::bigint AS total_rows,
    COUNT(DISTINCT c.variant_id)::bigint AS unique_variants,
    COUNT(DISTINCT COALESCE(h.document_number, h.reception_id))::bigint AS unique_documents,
    COUNT(*) FILTER (WHERE c.corrected_gross_cost IS NOT NULL)::bigint AS with_corrected_gross,
    COUNT(*) FILTER (WHERE c.corrected_gross_cost IS NULL)::bigint AS without_corrected_gross,
    MIN(h.admission_date::date) AS min_admission_date,
    MAX(h.admission_date::date) AS max_admission_date
FROM analytics.cost_reception_calculated c
INNER JOIN analytics.cost_reception_history h
    ON h.id = c.history_id
WHERE {where}
""".strip()
        rows = self._run(sql, tuple(params))
        base = rows[0] if rows else {}

        status_sql = f"""
SELECT c.effective_quality_status AS status, COUNT(*)::bigint AS n
FROM analytics.cost_reception_calculated c
INNER JOIN analytics.cost_reception_history h
    ON h.id = c.history_id
WHERE {where}
GROUP BY c.effective_quality_status
""".strip()
        status_rows = self._run(status_sql, tuple(params))

        # Expand warnings array → unnest counts (sin sumar montos)
        warn_sql = f"""
SELECT w.warning AS warning, COUNT(*)::bigint AS n
FROM analytics.cost_reception_calculated c
INNER JOIN analytics.cost_reception_history h
    ON h.id = c.history_id
CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(c.warnings_json, '[]'::jsonb)) AS w(warning)
WHERE {where}
GROUP BY w.warning
""".strip()
        warn_rows = self._run(warn_sql, tuple(params))

        return {
            "total_rows": int(base.get("total_rows") or 0),
            "unique_variants": int(base.get("unique_variants") or 0),
            "unique_documents": int(base.get("unique_documents") or 0),
            "with_corrected_gross": int(base.get("with_corrected_gross") or 0),
            "without_corrected_gross": int(base.get("without_corrected_gross") or 0),
            "min_admission_date": base.get("min_admission_date"),
            "max_admission_date": base.get("max_admission_date"),
            "by_status": {
                str(r["status"]): int(r["n"]) for r in status_rows if r.get("status")
            },
            "by_warning": {
                str(r["warning"]): int(r["n"]) for r in warn_rows if r.get("warning")
            },
        }

    def count_history_version_pairs(
        self,
        *,
        company_id: int,
        office_id: int,
        date_from: date,
        date_to: date,
    ) -> dict[str, int]:
        """Invariante: filas == DISTINCT (history_id, calculation_version)."""
        date_to_excl = date_to_exclusive(date_to)
        rows = self._run(
            """
SELECT
    COUNT(*)::bigint AS n,
    COUNT(DISTINCT (c.history_id, c.calculation_version))::bigint AS n_pairs
FROM analytics.cost_reception_calculated c
INNER JOIN analytics.cost_reception_history h
    ON h.id = c.history_id
WHERE c.calculation_version = %s
  AND c.company_id = %s
  AND c.office_id = %s
  AND h.admission_date >= %s
  AND h.admission_date < %s
""".strip(),
            (
                CALCULATION_VERSION_PIN,
                int(company_id),
                int(office_id),
                date_from,
                date_to_excl,
            ),
        )
        row = rows[0] if rows else {}
        return {
            "n": int(row.get("n") or 0),
            "n_pairs": int(row.get("n_pairs") or 0),
        }

    def _product_ranked_cte(
        self,
        where: str,
    ) -> str:
        """Última y penúltima recepción por variant_id (Decimal en SQL)."""
        return f"""
WITH ranked AS (
    SELECT
        c.variant_id,
        c.history_id,
        c.company_id,
        c.office_id,
        h.admission_date::date AS admission_date,
        h.document_number,
        h.document,
        h.barcode,
        h.product_name,
        h.variant_name,
        c.stored_cost_net,
        c.corrected_gross_cost,
        c.stored_gross_cost,
        c.calculated_iva_amount,
        c.additional_tax_amount_total,
        c.additional_taxes_json,
        c.total_tax_rate,
        c.iva_rate,
        c.iva_tax_id,
        c.effective_quality_status,
        c.warnings_json,
        c.tax_ids_source,
        c.tax_rates_source,
        c.tax_context_source,
        c.tax_resolution_quality,
        c.tax_context_is_historical,
        c.calculation_version,
        c.calculation_batch_id,
        c.calculated_at,
        c.source_history_fingerprint,
        c.tax_context_fingerprint,
        c.calculation_result_fingerprint,
        ROW_NUMBER() OVER (
            PARTITION BY c.variant_id
            ORDER BY h.admission_date DESC, h.id DESC
        ) AS rn,
        COUNT(*) OVER (PARTITION BY c.variant_id) AS receptions_count
    FROM analytics.cost_reception_calculated c
    INNER JOIN analytics.cost_reception_history h
        ON h.id = c.history_id
    WHERE {where}
),
products AS (
    SELECT
        cur.variant_id,
        cur.company_id,
        cur.office_id,
        cur.barcode,
        cur.product_name,
        cur.variant_name,
        cur.history_id AS latest_history_id,
        cur.admission_date AS latest_admission_date,
        cur.document_number AS latest_document_number,
        cur.document AS latest_document,
        cur.stored_cost_net AS current_stored_cost_net,
        cur.corrected_gross_cost AS current_corrected_gross_cost,
        cur.stored_gross_cost AS current_stored_gross_cost,
        cur.calculated_iva_amount AS current_calculated_iva_amount,
        cur.additional_tax_amount_total AS current_additional_tax_amount_total,
        cur.additional_taxes_json AS current_additional_taxes_json,
        cur.total_tax_rate AS current_total_tax_rate,
        cur.iva_rate AS current_iva_rate,
        cur.iva_tax_id AS current_iva_tax_id,
        cur.effective_quality_status AS current_quality_status,
        cur.warnings_json AS current_warnings_json,
        cur.tax_ids_source,
        cur.tax_rates_source,
        cur.tax_context_source,
        cur.tax_resolution_quality,
        cur.tax_context_is_historical,
        cur.calculation_version,
        cur.calculation_batch_id,
        cur.calculated_at AS last_calculated_at,
        cur.source_history_fingerprint,
        cur.tax_context_fingerprint,
        cur.calculation_result_fingerprint,
        cur.receptions_count,
        prev.history_id AS previous_history_id,
        prev.admission_date AS previous_admission_date,
        prev.corrected_gross_cost AS previous_corrected_gross_cost,
        CASE
            WHEN cur.corrected_gross_cost IS NOT NULL
             AND prev.corrected_gross_cost IS NOT NULL
            THEN cur.corrected_gross_cost - prev.corrected_gross_cost
            ELSE NULL
        END AS unit_change_amount,
        CASE
            WHEN cur.corrected_gross_cost IS NOT NULL
             AND prev.corrected_gross_cost IS NOT NULL
             AND prev.corrected_gross_cost <> 0
            THEN (
                (cur.corrected_gross_cost - prev.corrected_gross_cost)
                / prev.corrected_gross_cost
            ) * 100
            ELSE NULL
        END AS unit_change_percent
    FROM ranked cur
    LEFT JOIN ranked prev
        ON prev.variant_id = cur.variant_id
       AND prev.rn = 2
    WHERE cur.rn = 1
)
""".strip()

    def list_products(
        self,
        *,
        company_id: int,
        office_id: int,
        date_from: date,
        date_to: date,
        limit: int,
        sort: str = "latest_reception",
        cursor: dict[str, Any] | None = None,
        statuses: list[str] | None = None,
        warnings: list[str] | None = None,
        barcode: str | None = None,
        search: str | None = None,
        only_with_changes: bool = False,
        only_needs_review: bool = False,
        min_abs_change_percent: Decimal | None = None,
        needs_review_statuses: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        where, params = self._scope_clauses(
            company_id=company_id,
            office_id=office_id,
            date_from=date_from,
            date_to=date_to,
            statuses=None,  # status filtra el vigente (post-agg)
            warnings=None,
            barcode=barcode,
            search=search,
        )
        review = needs_review_statuses or [
            "incomplete_tax_context",
            "missing_cost",
        ]
        filters: list[str] = []
        if statuses:
            filters.append("p.current_quality_status = ANY(%s)")
            params.append(list(statuses))
        if warnings:
            filters.append("p.current_warnings_json ?| %s::text[]")
            params.append(list(warnings))
        if only_with_changes:
            filters.append("p.unit_change_amount IS NOT NULL AND p.unit_change_amount <> 0")
        if only_needs_review:
            filters.append(
                "(p.current_corrected_gross_cost IS NULL "
                "OR p.current_quality_status = ANY(%s))"
            )
            params.append(list(review))
        if min_abs_change_percent is not None:
            filters.append(
                "p.unit_change_percent IS NOT NULL "
                "AND ABS(p.unit_change_percent) >= %s"
            )
            params.append(min_abs_change_percent)

        # Keyset
        if cursor:
            c_sort = cursor.get("sort") or sort
            if c_sort != sort:
                from backend.schemas.cost_v2_read import CostV2ReadValidationError

                raise CostV2ReadValidationError(
                    "cursor sort mismatch",
                    error_type="invalid_cursor",
                )
            vid = int(cursor["variant_id"])
            if sort == "latest_reception":
                filters.append(
                    "(p.latest_admission_date, p.variant_id) < (%s::date, %s::bigint)"
                )
                params.extend([cursor["admission_date"], vid])
            elif sort == "product":
                filters.append(
                    "(LOWER(COALESCE(p.product_name, '')), p.variant_id) > "
                    "(LOWER(%s), %s::bigint)"
                )
                params.extend([cursor.get("product_name") or "", vid])
            elif sort == "current_cost":
                # DESC NULLS LAST
                filters.append(
                    """(
                        (p.current_corrected_gross_cost IS NOT NULL
                         AND %s::numeric IS NOT NULL
                         AND (p.current_corrected_gross_cost, p.variant_id)
                             < (%s::numeric, %s::bigint))
                     OR (p.current_corrected_gross_cost IS NULL AND %s::numeric IS NOT NULL)
                     OR (p.current_corrected_gross_cost IS NULL
                         AND %s::numeric IS NULL
                         AND p.variant_id < %s::bigint)
                    )"""
                )
                cc = cursor.get("current_cost")
                params.extend([cc, cc, vid, cc, cc, vid])
            elif sort == "pct_increase":
                filters.append(
                    """(
                        (p.unit_change_percent IS NOT NULL
                         AND %s::numeric IS NOT NULL
                         AND (p.unit_change_percent, p.variant_id)
                             < (%s::numeric, %s::bigint))
                     OR (p.unit_change_percent IS NULL AND %s::numeric IS NOT NULL)
                     OR (p.unit_change_percent IS NULL
                         AND %s::numeric IS NULL
                         AND p.variant_id < %s::bigint)
                    )"""
                )
                pct = cursor.get("unit_change_percent")
                params.extend([pct, pct, vid, pct, pct, vid])
            elif sort == "pct_decrease":
                # ASC (más negativo primero): continuar con valores mayores
                filters.append(
                    """(
                        (p.unit_change_percent IS NOT NULL
                         AND %s::numeric IS NOT NULL
                         AND (p.unit_change_percent, p.variant_id)
                             > (%s::numeric, %s::bigint))
                     OR (p.unit_change_percent IS NULL AND %s::numeric IS NOT NULL)
                     OR (p.unit_change_percent IS NULL
                         AND %s::numeric IS NULL
                         AND p.variant_id > %s::bigint)
                    )"""
                )
                pct = cursor.get("unit_change_percent")
                params.extend([pct, pct, vid, pct, pct, vid])
            elif sort == "status":
                filters.append(
                    "(COALESCE(p.current_quality_status, ''), p.variant_id) > (%s, %s::bigint)"
                )
                params.extend([cursor.get("status") or "", vid])

        filter_sql = (" AND " + " AND ".join(filters)) if filters else ""

        order_map = {
            "latest_reception": (
                "p.latest_admission_date DESC, p.variant_id DESC"
            ),
            "pct_increase": (
                "p.unit_change_percent DESC NULLS LAST, p.variant_id DESC"
            ),
            "pct_decrease": (
                "p.unit_change_percent ASC NULLS LAST, p.variant_id ASC"
            ),
            "product": "LOWER(COALESCE(p.product_name, '')) ASC, p.variant_id ASC",
            "current_cost": (
                "p.current_corrected_gross_cost DESC NULLS LAST, p.variant_id DESC"
            ),
            "status": (
                "COALESCE(p.current_quality_status, '') ASC, p.variant_id ASC"
            ),
        }
        order_sql = order_map.get(sort, order_map["latest_reception"])

        sql = f"""
{self._product_ranked_cte(where)}
SELECT * FROM products p
WHERE TRUE{filter_sql}
ORDER BY {order_sql}
LIMIT %s
""".strip()
        params.append(int(limit) + 1)
        rows = self._run(sql, tuple(params))
        out: list[dict[str, Any]] = []
        for r in rows:
            row = dict(r)
            for key in (
                "current_stored_cost_net",
                "current_corrected_gross_cost",
                "current_stored_gross_cost",
                "current_calculated_iva_amount",
                "current_additional_tax_amount_total",
                "current_total_tax_rate",
                "current_iva_rate",
                "previous_corrected_gross_cost",
                "unit_change_amount",
                "unit_change_percent",
            ):
                if key in row:
                    row[key] = coerce_optional_decimal(row.get(key))
            out.append(row)
        return out

    def get_product(
        self,
        *,
        company_id: int,
        office_id: int,
        variant_id: int,
        date_from: date,
        date_to: date,
        history_limit: int = 20,
    ) -> dict[str, Any] | None:
        where, params = self._scope_clauses(
            company_id=company_id,
            office_id=office_id,
            date_from=date_from,
            date_to=date_to,
            variant_id=variant_id,
        )
        sql = f"""
{self._product_ranked_cte(where)}
SELECT * FROM products p
LIMIT 1
""".strip()
        rows = self._run(sql, tuple(params))
        if not rows:
            return None
        product = dict(rows[0])
        for key in (
            "current_stored_cost_net",
            "current_corrected_gross_cost",
            "current_stored_gross_cost",
            "current_calculated_iva_amount",
            "current_additional_tax_amount_total",
            "current_total_tax_rate",
            "current_iva_rate",
            "previous_corrected_gross_cost",
            "unit_change_amount",
            "unit_change_percent",
        ):
            if key in product:
                product[key] = coerce_optional_decimal(product.get(key))

        hist = self.list_receptions(
            company_id=company_id,
            office_id=office_id,
            date_from=date_from,
            date_to=date_to,
            limit=history_limit,
            variant_id=variant_id,
        )
        product["receptions"] = hist
        return product

    def summarize_products(
        self,
        *,
        company_id: int,
        office_id: int,
        date_from: date,
        date_to: date,
        change_threshold_percent: Decimal,
        barcode: str | None = None,
        search: str | None = None,
        statuses: list[str] | None = None,
        warnings: list[str] | None = None,
        needs_review_statuses: list[str] | None = None,
    ) -> dict[str, Any]:
        where, cte_params = self._scope_clauses(
            company_id=company_id,
            office_id=office_id,
            date_from=date_from,
            date_to=date_to,
            barcode=barcode,
            search=search,
        )
        review = needs_review_statuses or [
            "incomplete_tax_context",
            "missing_cost",
        ]
        filters: list[str] = []
        filter_params: list[Any] = []
        if statuses:
            filters.append("p.current_quality_status = ANY(%s)")
            filter_params.append(list(statuses))
        if warnings:
            filters.append("p.current_warnings_json ?| %s::text[]")
            filter_params.append(list(warnings))
        filter_sql = (" AND " + " AND ".join(filters)) if filters else ""

        # Orden de %s en el SQL: CTE where → SELECT filters → outer WHERE
        sql = f"""
{self._product_ranked_cte(where)}
SELECT
    COUNT(*)::bigint AS total_products,
    COUNT(*) FILTER (
        WHERE p.current_corrected_gross_cost IS NOT NULL
    )::bigint AS products_with_current_cost,
    COUNT(*) FILTER (
        WHERE p.current_corrected_gross_cost IS NULL
    )::bigint AS products_without_calculable_cost,
    COUNT(*) FILTER (
        WHERE p.current_quality_status = 'incomplete_tax_context'
    )::bigint AS products_incomplete_tax_context,
    COUNT(*) FILTER (
        WHERE p.current_warnings_json ? 'suspicious_outlier'
    )::bigint AS products_with_outlier,
    COUNT(*) FILTER (
        WHERE p.unit_change_amount IS NOT NULL AND p.unit_change_amount > 0
    )::bigint AS products_with_increase,
    COUNT(*) FILTER (
        WHERE p.unit_change_amount IS NOT NULL AND p.unit_change_amount < 0
    )::bigint AS products_with_decrease,
    COUNT(*) FILTER (
        WHERE p.unit_change_percent IS NOT NULL
          AND ABS(p.unit_change_percent) >= %s
    )::bigint AS products_with_change_over_threshold,
    COUNT(*) FILTER (
        WHERE p.current_corrected_gross_cost IS NULL
           OR p.current_quality_status = ANY(%s)
    )::bigint AS products_needing_review,
    COUNT(*) FILTER (
        WHERE p.current_quality_status = 'missing_cost'
    )::bigint AS products_missing_cost,
    COUNT(*) FILTER (
        WHERE p.current_warnings_json ? 'stored_components_rounding'
    )::bigint AS products_rounding_warning,
    MAX(p.latest_admission_date) AS latest_reception_date,
    MAX(p.last_calculated_at) AS latest_calculation_at
FROM products p
WHERE TRUE{filter_sql}
""".strip()
        params = (
            list(cte_params)
            + [change_threshold_percent, list(review)]
            + filter_params
        )
        rows = self._run(sql, tuple(params))
        row = rows[0] if rows else {}
        return {
            "total_products": int(row.get("total_products") or 0),
            "products_with_current_cost": int(row.get("products_with_current_cost") or 0),
            "products_without_calculable_cost": int(
                row.get("products_without_calculable_cost") or 0
            ),
            "products_incomplete_tax_context": int(
                row.get("products_incomplete_tax_context") or 0
            ),
            "products_with_outlier": int(row.get("products_with_outlier") or 0),
            "products_with_increase": int(row.get("products_with_increase") or 0),
            "products_with_decrease": int(row.get("products_with_decrease") or 0),
            "products_with_change_over_threshold": int(
                row.get("products_with_change_over_threshold") or 0
            ),
            "products_needing_review": int(row.get("products_needing_review") or 0),
            "products_missing_cost": int(row.get("products_missing_cost") or 0),
            "products_rounding_warning": int(row.get("products_rounding_warning") or 0),
            "latest_reception_date": row.get("latest_reception_date"),
            "latest_calculation_at": row.get("latest_calculation_at"),
            "change_threshold_percent": change_threshold_percent,
        }
