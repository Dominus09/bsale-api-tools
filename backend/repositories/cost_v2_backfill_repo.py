"""Consultas batch para backfill dry-run y persistencia canaria Costos V2.

Dry-run: sin N+1, sin SELECT FOR UPDATE, sin DML.
Apply canario: DML solo vía write_executor (UPSERT idempotente).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

from backend.services.analytics.cost_audit_models import TaxCatalogEntry, coerce_optional_decimal
from backend.services.analytics.cost_v2_models import CostReceptionCalculation
from backend.services.analytics.money import optional_decimal
from backend.services.analytics.validate_distribuidora_source import assert_sql_is_read_only

QueryExecutor = Callable[[str, tuple[Any, ...]], list[dict[str, Any]]]


def _parse_jsonish(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _tax_id_list(value: Any) -> list[int]:
    parsed = _parse_jsonish(value)
    if not isinstance(parsed, list):
        return []
    out: list[int] = []
    for item in parsed:
        try:
            out.append(int(item))
        except (TypeError, ValueError):
            continue
    return out


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


UPSERT_CALCULATION_SQL = """
INSERT INTO analytics.cost_reception_calculated (
    history_id,
    calculation_version,
    calculation_batch_id,
    company_id,
    office_id,
    variant_id,
    admission_date,
    stored_cost_net,
    stored_quantity,
    stored_iva_amount,
    stored_other_taxes,
    stored_gross_cost,
    reception_tax_ids_json,
    catalog_tax_ids_json,
    resolved_tax_ids_json,
    iva_tax_id,
    iva_rate,
    calculated_iva_amount,
    additional_taxes_json,
    additional_tax_rate_total,
    additional_tax_amount_total,
    total_tax_rate,
    corrected_gross_cost,
    gross_difference_amount,
    tax_rate_on_net_pct,
    gross_understatement_vs_corrected_pct,
    tax_context_source,
    tax_ids_source,
    tax_rates_source,
    tax_context_as_of,
    tax_context_is_historical,
    tax_context_fingerprint,
    tax_resolution_quality,
    effective_quality_status,
    warnings_json,
    source_history_created_at,
    source_history_fingerprint,
    calculation_result_fingerprint
) VALUES (
    %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s::jsonb, %s::jsonb, %s::jsonb,
    %s, %s, %s,
    %s::jsonb, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s,
    %s::jsonb, %s, %s, %s
)
ON CONFLICT (history_id, calculation_version)
DO UPDATE SET
    calculation_batch_id = EXCLUDED.calculation_batch_id,
    company_id = EXCLUDED.company_id,
    office_id = EXCLUDED.office_id,
    variant_id = EXCLUDED.variant_id,
    admission_date = EXCLUDED.admission_date,
    stored_cost_net = EXCLUDED.stored_cost_net,
    stored_quantity = EXCLUDED.stored_quantity,
    stored_iva_amount = EXCLUDED.stored_iva_amount,
    stored_other_taxes = EXCLUDED.stored_other_taxes,
    stored_gross_cost = EXCLUDED.stored_gross_cost,
    reception_tax_ids_json = EXCLUDED.reception_tax_ids_json,
    catalog_tax_ids_json = EXCLUDED.catalog_tax_ids_json,
    resolved_tax_ids_json = EXCLUDED.resolved_tax_ids_json,
    iva_tax_id = EXCLUDED.iva_tax_id,
    iva_rate = EXCLUDED.iva_rate,
    calculated_iva_amount = EXCLUDED.calculated_iva_amount,
    additional_taxes_json = EXCLUDED.additional_taxes_json,
    additional_tax_rate_total = EXCLUDED.additional_tax_rate_total,
    additional_tax_amount_total = EXCLUDED.additional_tax_amount_total,
    total_tax_rate = EXCLUDED.total_tax_rate,
    corrected_gross_cost = EXCLUDED.corrected_gross_cost,
    gross_difference_amount = EXCLUDED.gross_difference_amount,
    tax_rate_on_net_pct = EXCLUDED.tax_rate_on_net_pct,
    gross_understatement_vs_corrected_pct = EXCLUDED.gross_understatement_vs_corrected_pct,
    tax_context_source = EXCLUDED.tax_context_source,
    tax_ids_source = EXCLUDED.tax_ids_source,
    tax_rates_source = EXCLUDED.tax_rates_source,
    tax_context_as_of = EXCLUDED.tax_context_as_of,
    tax_context_is_historical = EXCLUDED.tax_context_is_historical,
    tax_context_fingerprint = EXCLUDED.tax_context_fingerprint,
    tax_resolution_quality = EXCLUDED.tax_resolution_quality,
    effective_quality_status = EXCLUDED.effective_quality_status,
    warnings_json = EXCLUDED.warnings_json,
    source_history_created_at = EXCLUDED.source_history_created_at,
    source_history_fingerprint = EXCLUDED.source_history_fingerprint,
    calculation_result_fingerprint = EXCLUDED.calculation_result_fingerprint,
    calculated_at = NOW()
RETURNING
    history_id,
    calculation_version,
    calculation_batch_id,
    calculated_at,
    (xmax = 0) AS was_inserted
""".strip()


class CostV2BackfillRepository:
    def __init__(
        self,
        executor: QueryExecutor,
        write_executor: QueryExecutor | None = None,
    ) -> None:
        self._executor = executor
        self._write_executor = write_executor
        self._schema_cache: dict[str, bool] = {}

    def _execute(self, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        assert_sql_is_read_only(sql)
        upper = sql.upper()
        if "FOR UPDATE" in upper:
            raise RuntimeError("SELECT FOR UPDATE no permitido en backfill dry-run")
        return self._executor(sql, params)

    def _execute_write(self, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        if self._write_executor is None:
            raise RuntimeError("write_executor no configurado (apply canario)")
        return self._write_executor(sql, params)

    def column_exists(self, schema: str, table: str, column: str) -> bool:
        key = f"{schema}.{table}.{column}"
        if key in self._schema_cache:
            return self._schema_cache[key]
        rows = self._execute(
            """
SELECT 1 AS ok
FROM information_schema.columns
WHERE table_schema = %s
  AND table_name = %s
  AND column_name = %s
LIMIT 1
""".strip(),
            (schema, table, column),
        )
        exists = bool(rows)
        self._schema_cache[key] = exists
        return exists

    def calculated_table_exists(self) -> bool:
        rows = self._execute(
            """
SELECT to_regclass('analytics.cost_reception_calculated') IS NOT NULL AS ok
""".strip(),
            (),
        )
        return bool(rows and rows[0].get("ok"))

    def calculated_latest_view_exists(self) -> bool:
        rows = self._execute(
            """
SELECT to_regclass('analytics.v_cost_reception_calculated_latest') IS NOT NULL AS ok
""".strip(),
            (),
        )
        return bool(rows and rows[0].get("ok"))

    def resolve_barcode_variant_ids(self, *, company_id: int, barcode: str) -> list[int]:
        normalized = (barcode or "").strip()
        if not normalized:
            return []
        like = f"%{normalized}%"
        rows = self._execute(
            """
SELECT DISTINCT h.variant_id
FROM analytics.cost_reception_history h
WHERE h.company_id = %s
  AND (
        TRIM(COALESCE(h.barcode, '')) = %s
     OR h.barcode ILIKE %s
  )
ORDER BY h.variant_id
LIMIT 500
""".strip(),
            (int(company_id), normalized, like),
        )
        return [int(r["variant_id"]) for r in rows]

    def count_population(
        self,
        *,
        company_id: int,
        office_id: int | None,
        date_from: date,
        date_to: date,
        variant_ids: list[int] | None = None,
        history_id: int | None = None,
        document_number: int | None = None,
    ) -> dict[str, Any]:
        date_to_exclusive = date_to + timedelta(days=1)
        sql = """
SELECT
    COUNT(*)::bigint AS rows_found,
    COUNT(DISTINCT h.variant_id)::bigint AS unique_variants,
    COUNT(DISTINCT COALESCE(h.document_number, h.reception_id))::bigint AS unique_documents,
    MIN(h.admission_date::date) AS min_admission_date,
    MAX(h.admission_date::date) AS max_admission_date
FROM analytics.cost_reception_history h
WHERE h.company_id = %s
  AND h.admission_date >= %s
  AND h.admission_date < %s
""".strip()
        params: list[Any] = [company_id, date_from, date_to_exclusive]
        if office_id is not None:
            sql += " AND h.office_id = %s"
            params.append(office_id)
        if history_id is not None:
            sql += " AND h.id = %s"
            params.append(history_id)
        if document_number is not None:
            sql += " AND (h.document_number = %s OR h.reception_id = %s)"
            params.extend([document_number, document_number])
        if variant_ids is not None:
            if not variant_ids:
                return {
                    "rows_found": 0,
                    "unique_variants": 0,
                    "unique_documents": 0,
                    "min_admission_date": None,
                    "max_admission_date": None,
                }
            sql += " AND h.variant_id = ANY(%s)"
            params.append(list(variant_ids))
        rows = self._execute(sql, tuple(params))
        row = rows[0] if rows else {}
        min_d = row.get("min_admission_date")
        max_d = row.get("max_admission_date")
        if isinstance(min_d, datetime):
            min_d = min_d.date()
        if isinstance(max_d, datetime):
            max_d = max_d.date()
        return {
            "rows_found": int(row.get("rows_found") or 0),
            "unique_variants": int(row.get("unique_variants") or 0),
            "unique_documents": int(row.get("unique_documents") or 0),
            "min_admission_date": min_d.isoformat() if min_d else None,
            "max_admission_date": max_d.isoformat() if max_d else None,
        }

    def fetch_population_scope_stats(
        self,
        *,
        company_id: int,
        office_id: int,
        date_from: date,
        date_to: date,
    ) -> dict[str, Any]:
        """Conteo + bounds + unicidad de history_id (sin OFFSET / sin cargar filas)."""
        date_to_exclusive = date_to + timedelta(days=1)
        rows = self._execute(
            """
SELECT
    COUNT(*)::bigint AS rows_found,
    COUNT(DISTINCT h.id)::bigint AS unique_history_ids,
    COUNT(DISTINCT h.variant_id)::bigint AS unique_variants,
    MIN(h.id)::bigint AS min_history_id,
    MAX(h.id)::bigint AS max_history_id
FROM analytics.cost_reception_history h
WHERE h.company_id = %s
  AND h.office_id = %s
  AND h.admission_date >= %s
  AND h.admission_date < %s
""".strip(),
            (int(company_id), int(office_id), date_from, date_to_exclusive),
        )
        row = rows[0] if rows else {}
        return {
            "rows_found": int(row.get("rows_found") or 0),
            "unique_history_ids": int(row.get("unique_history_ids") or 0),
            "unique_variants": int(row.get("unique_variants") or 0),
            "min_history_id": (
                int(row["min_history_id"])
                if row.get("min_history_id") is not None
                else None
            ),
            "max_history_id": (
                int(row["max_history_id"])
                if row.get("max_history_id") is not None
                else None
            ),
        }

    def count_calculated_for_scope(
        self,
        *,
        company_id: int,
        office_id: int,
        date_from: date,
        date_to: date,
        calculation_version: str,
    ) -> int:
        date_to_exclusive = date_to + timedelta(days=1)
        rows = self._execute(
            """
SELECT COUNT(*)::bigint AS n
FROM analytics.cost_reception_calculated c
JOIN analytics.cost_reception_history h ON h.id = c.history_id
WHERE c.calculation_version = %s
  AND h.company_id = %s
  AND h.office_id = %s
  AND h.admission_date >= %s
  AND h.admission_date < %s
""".strip(),
            (
                str(calculation_version),
                int(company_id),
                int(office_id),
                date_from,
                date_to_exclusive,
            ),
        )
        return int(rows[0]["n"]) if rows else 0

    def count_latest_for_scope(
        self,
        *,
        company_id: int,
        office_id: int,
        date_from: date,
        date_to: date,
        calculation_version: str,
    ) -> int:
        date_to_exclusive = date_to + timedelta(days=1)
        rows = self._execute(
            """
SELECT COUNT(*)::bigint AS n
FROM analytics.v_cost_reception_calculated_latest l
JOIN analytics.cost_reception_history h ON h.id = l.history_id
WHERE l.calculation_version = %s
  AND h.company_id = %s
  AND h.office_id = %s
  AND h.admission_date >= %s
  AND h.admission_date < %s
""".strip(),
            (
                str(calculation_version),
                int(company_id),
                int(office_id),
                date_from,
                date_to_exclusive,
            ),
        )
        return int(rows[0]["n"]) if rows else 0

    def fetch_history_batch(
        self,
        *,
        company_id: int,
        office_id: int | None,
        date_from: date,
        date_to: date,
        after_id: int,
        batch_size: int,
        variant_ids: list[int] | None = None,
        history_id: int | None = None,
        document_number: int | None = None,
    ) -> list[dict[str, Any]]:
        """Keyset: WHERE id > after_id ORDER BY id ASC LIMIT batch_size."""
        has_tax_ids = self.column_exists("bsale", "products", "tax_ids_json")
        tax_ids_expr = (
            "p.tax_ids_json AS catalog_tax_ids_json"
            if has_tax_ids
            else "NULL::jsonb AS catalog_tax_ids_json"
        )
        date_to_exclusive = date_to + timedelta(days=1)
        sql = f"""
SELECT
    h.id AS history_id,
    h.company_id,
    h.office_id,
    h.variant_id,
    h.admission_date,
    h.quantity,
    h.cost_net,
    h.iva_amount,
    h.other_taxes,
    h.cost_bruto_erp,
    h.created_at,
    h.barcode,
    h.product_name,
    h.variant_name,
    h.document_number,
    h.reception_id,
    h.product_id,
    {tax_ids_expr}
FROM analytics.cost_reception_history h
LEFT JOIN bsale.variants v
    ON v.company_id = h.company_id
   AND v.bsale_id = h.variant_id
LEFT JOIN bsale.products p
    ON p.company_id = h.company_id
   AND p.bsale_id = COALESCE(h.product_id, v.product_id)
WHERE h.company_id = %s
  AND h.admission_date >= %s
  AND h.admission_date < %s
  AND h.id > %s
""".strip()
        params: list[Any] = [company_id, date_from, date_to_exclusive, int(after_id)]
        if office_id is not None:
            sql += " AND h.office_id = %s"
            params.append(office_id)
        if history_id is not None:
            sql += " AND h.id = %s"
            params.append(history_id)
        if document_number is not None:
            sql += " AND (h.document_number = %s OR h.reception_id = %s)"
            params.extend([document_number, document_number])
        if variant_ids is not None:
            if not variant_ids:
                return []
            sql += " AND h.variant_id = ANY(%s)"
            params.append(list(variant_ids))
        sql += " ORDER BY h.id ASC LIMIT %s"
        params.append(int(batch_size))

        rows = self._execute(sql, tuple(params))
        out: list[dict[str, Any]] = []
        for row in rows:
            tax_ids = _tax_id_list(row.get("catalog_tax_ids_json"))
            out.append(
                {
                    "history_id": int(row["history_id"]),
                    "company_id": int(row["company_id"]),
                    "office_id": (
                        int(row["office_id"]) if row.get("office_id") is not None else None
                    ),
                    "variant_id": int(row["variant_id"]),
                    "admission_date": row.get("admission_date"),
                    "quantity": coerce_optional_decimal(row.get("quantity")),
                    "cost_net": coerce_optional_decimal(row.get("cost_net")),
                    "iva_amount": coerce_optional_decimal(row.get("iva_amount")),
                    "other_taxes": coerce_optional_decimal(row.get("other_taxes")),
                    "cost_bruto_erp": coerce_optional_decimal(row.get("cost_bruto_erp")),
                    "created_at": row.get("created_at"),
                    "barcode": row.get("barcode"),
                    "product_name": row.get("product_name"),
                    "variant_name": row.get("variant_name"),
                    "document_number": row.get("document_number"),
                    "reception_id": row.get("reception_id"),
                    "catalog_tax_ids": tax_ids,
                }
            )
        return out

    def fetch_scope_variant_ids(
        self,
        *,
        company_id: int,
        office_id: int | None,
        date_from: date,
        date_to: date,
        variant_ids: list[int] | None = None,
        history_id: int | None = None,
        document_number: int | None = None,
    ) -> list[int]:
        if variant_ids is not None:
            return sorted({int(v) for v in variant_ids})
        date_to_exclusive = date_to + timedelta(days=1)
        sql = """
SELECT DISTINCT h.variant_id AS variant_id
FROM analytics.cost_reception_history h
WHERE h.company_id = %s
  AND h.admission_date >= %s
  AND h.admission_date < %s
""".strip()
        params: list[Any] = [company_id, date_from, date_to_exclusive]
        if office_id is not None:
            sql += " AND h.office_id = %s"
            params.append(office_id)
        if history_id is not None:
            sql += " AND h.id = %s"
            params.append(history_id)
        if document_number is not None:
            sql += " AND (h.document_number = %s OR h.reception_id = %s)"
            params.extend([document_number, document_number])
        sql += " ORDER BY h.variant_id"
        rows = self._execute(sql, tuple(params))
        return [int(r["variant_id"]) for r in rows if r.get("variant_id") is not None]

    def fetch_outlier_baseline_cost_nets(
        self,
        *,
        company_id: int,
        office_id: int | None,
        variant_ids: list[int],
    ) -> list[dict[str, Any]]:
        ids = sorted({int(v) for v in variant_ids})
        if not ids:
            return []
        sql = """
SELECT
    h.variant_id,
    h.cost_net
FROM analytics.cost_reception_history h
WHERE h.company_id = %s
  AND h.cost_net IS NOT NULL
  AND h.cost_net > 0
  AND h.variant_id = ANY(%s)
""".strip()
        params: list[Any] = [company_id, ids]
        if office_id is not None:
            sql += " AND h.office_id = %s"
            params.append(office_id)

        rows = self._execute(sql, tuple(params))
        out: list[dict[str, Any]] = []
        for row in rows:
            net = coerce_optional_decimal(row.get("cost_net"))
            if net is None or net <= Decimal("0"):
                continue
            out.append({"variant_id": int(row["variant_id"]), "cost_net": net})
        return out

    def fetch_taxes_for_ids(
        self,
        *,
        company_id: int,
        tax_ids: list[int],
    ) -> dict[int, TaxCatalogEntry]:
        ids = sorted({int(i) for i in tax_ids if i is not None})
        if not ids:
            return {}
        if not self.column_exists("bsale", "taxes", "bsale_id"):
            return {}
        rows = self._execute(
            """
SELECT bsale_id, name, percentage
FROM bsale.taxes
WHERE company_id = %s
  AND bsale_id = ANY(%s)
""".strip(),
            (int(company_id), ids),
        )
        out: dict[int, TaxCatalogEntry] = {}
        for row in rows:
            tid = int(row["bsale_id"])
            out[tid] = TaxCatalogEntry(
                tax_id=tid,
                name=row.get("name"),
                percentage=(
                    optional_decimal(row.get("percentage"))
                    if row.get("percentage") is not None
                    else None
                ),
            )
        return out

    def get_existing_calculation(
        self,
        *,
        history_id: int,
        calculation_version: str,
    ) -> dict[str, Any] | None:
        rows = self._execute(
            """
SELECT
    history_id,
    calculation_version,
    calculation_batch_id,
    calculated_at,
    source_history_fingerprint,
    tax_context_fingerprint,
    calculation_result_fingerprint,
    stored_cost_net,
    calculated_iva_amount,
    additional_tax_amount_total,
    total_tax_rate,
    corrected_gross_cost,
    gross_difference_amount,
    tax_rate_on_net_pct,
    gross_understatement_vs_corrected_pct,
    tax_ids_source,
    tax_rates_source,
    tax_resolution_quality,
    effective_quality_status,
    warnings_json,
    resolved_tax_ids_json
FROM analytics.cost_reception_calculated
WHERE history_id = %s
  AND calculation_version = %s
LIMIT 1
""".strip(),
            (int(history_id), str(calculation_version)),
        )
        return self._normalize_calc_row(rows[0]) if rows else None

    def read_calculation(
        self,
        *,
        history_id: int,
        calculation_version: str,
    ) -> dict[str, Any] | None:
        rows = self._execute(
            """
SELECT *
FROM analytics.cost_reception_calculated
WHERE history_id = %s
  AND calculation_version = %s
LIMIT 1
""".strip(),
            (int(history_id), str(calculation_version)),
        )
        return self._normalize_calc_row(rows[0]) if rows else None

    def read_latest_view(self, *, history_id: int) -> list[dict[str, Any]]:
        rows = self._execute(
            """
SELECT *
FROM analytics.v_cost_reception_calculated_latest
WHERE history_id = %s
""".strip(),
            (int(history_id),),
        )
        return [self._normalize_calc_row(r) for r in rows]

    def verify_source_fingerprint_inputs(
        self,
        *,
        history_id: int,
    ) -> dict[str, Any] | None:
        has_tax_ids = self.column_exists("bsale", "products", "tax_ids_json")
        tax_ids_expr = (
            "p.tax_ids_json AS catalog_tax_ids_json"
            if has_tax_ids
            else "NULL::jsonb AS catalog_tax_ids_json"
        )
        rows = self._execute(
            f"""
SELECT
    h.id AS history_id,
    h.company_id,
    h.office_id,
    h.variant_id,
    h.admission_date,
    h.cost_net,
    h.quantity,
    h.iva_amount,
    h.other_taxes,
    h.cost_bruto_erp,
    h.created_at,
    {tax_ids_expr}
FROM analytics.cost_reception_history h
LEFT JOIN bsale.variants v
    ON v.company_id = h.company_id
   AND v.bsale_id = h.variant_id
LEFT JOIN bsale.products p
    ON p.company_id = h.company_id
   AND p.bsale_id = COALESCE(h.product_id, v.product_id)
WHERE h.id = %s
LIMIT 1
""".strip(),
            (int(history_id),),
        )
        if not rows:
            return None
        row = rows[0]
        return {
            "history_id": int(row["history_id"]),
            "company_id": int(row["company_id"]),
            "office_id": (
                int(row["office_id"]) if row.get("office_id") is not None else None
            ),
            "variant_id": int(row["variant_id"]),
            "admission_date": row.get("admission_date"),
            "cost_net": coerce_optional_decimal(row.get("cost_net")),
            "quantity": coerce_optional_decimal(row.get("quantity")),
            "iva_amount": coerce_optional_decimal(row.get("iva_amount")),
            "other_taxes": coerce_optional_decimal(row.get("other_taxes")),
            "cost_bruto_erp": coerce_optional_decimal(row.get("cost_bruto_erp")),
            "created_at": row.get("created_at"),
            "catalog_tax_ids": _tax_id_list(row.get("catalog_tax_ids_json")),
        }

    def persist_calculation(
        self,
        *,
        calc: CostReceptionCalculation,
        calculation_batch_id: UUID | str,
    ) -> dict[str, Any]:
        """UPSERT. Caller debe haber descartado el caso unchanged."""
        batch_id = str(calculation_batch_id)
        params = (
            int(calc.history_id),
            str(calc.calculation_version),
            batch_id,
            int(calc.company_id),
            calc.office_id,
            int(calc.variant_id),
            calc.admission_date,
            calc.stored_cost_net,
            calc.stored_quantity,
            calc.stored_iva_amount,
            calc.stored_other_taxes,
            calc.stored_gross_cost,
            _json_dumps(list(calc.reception_tax_ids)),
            _json_dumps(list(calc.catalog_tax_ids)),
            _json_dumps(list(calc.resolved_tax_ids)),
            calc.iva_tax_id,
            calc.iva_rate,
            calc.calculated_iva_amount,
            _json_dumps(calc.additional_taxes_json()),
            calc.additional_tax_rate_total,
            calc.additional_tax_amount_total,
            calc.total_tax_rate,
            calc.corrected_gross_cost,
            calc.gross_difference_amount,
            calc.tax_rate_on_net_pct,
            calc.gross_understatement_vs_corrected_pct,
            calc.tax_context_source,
            calc.tax_ids_source,
            calc.tax_rates_source,
            calc.tax_context_as_of,
            calc.tax_context_is_historical,
            calc.tax_context_fingerprint,
            calc.tax_resolution_quality,
            calc.effective_quality_status,
            _json_dumps(list(calc.warnings)),
            calc.source_history_created_at,
            calc.source_history_fingerprint,
            calc.calculation_result_fingerprint,
        )
        rows = self._execute_write(UPSERT_CALCULATION_SQL, params)
        if not rows:
            raise RuntimeError("UPSERT no retornó fila")
        return rows[0]

    def count_calculations_for_history(self, *, history_id: int) -> int:
        rows = self._execute(
            """
SELECT COUNT(*)::bigint AS n
FROM analytics.cost_reception_calculated
WHERE history_id = %s
""".strip(),
            (int(history_id),),
        )
        return int(rows[0]["n"]) if rows else 0

    @staticmethod
    def _normalize_calc_row(row: dict[str, Any]) -> dict[str, Any]:
        out = dict(row)
        for key in (
            "stored_cost_net",
            "stored_quantity",
            "stored_iva_amount",
            "stored_other_taxes",
            "stored_gross_cost",
            "iva_rate",
            "calculated_iva_amount",
            "additional_tax_rate_total",
            "additional_tax_amount_total",
            "total_tax_rate",
            "corrected_gross_cost",
            "gross_difference_amount",
            "tax_rate_on_net_pct",
            "gross_understatement_vs_corrected_pct",
        ):
            if key in out:
                out[key] = coerce_optional_decimal(out.get(key))
        for key in (
            "reception_tax_ids_json",
            "catalog_tax_ids_json",
            "resolved_tax_ids_json",
            "additional_taxes_json",
            "warnings_json",
        ):
            if key in out:
                out[key] = _parse_jsonish(out.get(key))
        if "calculation_batch_id" in out and out["calculation_batch_id"] is not None:
            out["calculation_batch_id"] = str(out["calculation_batch_id"])
        return out
