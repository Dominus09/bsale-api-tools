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
