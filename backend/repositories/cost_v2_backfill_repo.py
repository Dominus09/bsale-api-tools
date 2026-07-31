"""Consultas batch read-only para backfill dry-run Costos V2.

Sin N+1: history+product tax_ids en un SELECT; taxes por ids en batch.
Sin SELECT FOR UPDATE. Sin DML.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from backend.services.analytics.cost_audit_models import TaxCatalogEntry, coerce_optional_decimal
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


class CostV2BackfillRepository:
    def __init__(self, executor: QueryExecutor) -> None:
        self._executor = executor
        self._schema_cache: dict[str, bool] = {}

    def _execute(self, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        assert_sql_is_read_only(sql)
        upper = sql.upper()
        if "FOR UPDATE" in upper:
            raise RuntimeError("SELECT FOR UPDATE no permitido en backfill dry-run")
        return self._executor(sql, params)

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
        """Variant IDs presentes en el scope de salida (con filtros del CLI)."""
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
        """Baseline de outliers: historial completo de la variante (sin filtros de salida).

        Ignora date_from/date_to, history_id, barcode y document_number.
        Solo company_id + office_id + variant_ids + cost_net > 0.
        Una sola consulta batch (sin N+1).
        """
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
