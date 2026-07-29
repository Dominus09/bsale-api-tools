"""Consultas batch read-only para auditoría de costos.

Sin N+1: una query de history+joins, una de taxes por ids, probes de schema.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import date, datetime, timedelta
from typing import Any

from backend.services.analytics.cost_audit_models import (
    BarcodeResolution,
    CostAuditArgs,
    CostAuditRawRow,
    TaxCatalogEntry,
    coerce_optional_decimal,
    normalize_barcode,
)
from backend.services.analytics.money import optional_decimal
from backend.services.analytics.validate_distribuidora_source import assert_sql_is_read_only

QueryExecutor = Callable[[str, tuple[Any, ...]], list[dict[str, Any]]]


def _as_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


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


def _tax_id_list(tax_ids_json: Any) -> list[int]:
    parsed = _parse_jsonish(tax_ids_json)
    if not isinstance(parsed, list):
        return []
    out: list[int] = []
    for item in parsed:
        try:
            out.append(int(item))
        except (TypeError, ValueError):
            continue
    return out


class CostDataAuditRepository:
    def __init__(self, executor: QueryExecutor) -> None:
        self._executor = executor
        self._schema_cache: dict[str, bool] = {}

    def _execute(self, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        assert_sql_is_read_only(sql)
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

    def resolve_barcode_to_variant_ids(
        self,
        *,
        company_id: int,
        barcode: str,
    ) -> BarcodeResolution:
        """Resuelve barcode → variant_id(s) como /costos.

        Fuente canónica de búsqueda en /costos:
          analytics.cost_reception_history.barcode ILIKE %term%
        (list_history_rows / search_variants en cost_analytics_repo).

        No usa variants.code (SKU) como si fuera barcode.
        Catálogo adicional: bsale.variants.bar_code (solo diagnóstico / fallback).
        No filtra por office ni por estado activo.
        """
        requested = barcode
        normalized = normalize_barcode(barcode)
        if not normalized:
            return BarcodeResolution(
                requested_barcode=requested,
                normalized_barcode=None,
                catalog_matches=0,
                resolved_variant_ids=(),
                resolution_source=None,
                duplicate_mapping=False,
                history_rows_found=0,
                barcode_not_found=True,
                warnings=("empty_barcode",),
            )

        # Misma semántica que /costos: ILIKE %term% sobre h.barcode + TRIM exacto
        like = f"%{normalized}%"
        history_rows = self._execute(
            """
SELECT DISTINCT
    h.variant_id,
    h.barcode,
    h.product_name,
    h.variant_name
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

        history_ids: list[int] = []
        details: list[dict[str, Any]] = []
        for row in history_rows:
            vid = int(row["variant_id"])
            if vid not in history_ids:
                history_ids.append(vid)
            details.append(
                {
                    "variant_id": vid,
                    "barcode": row.get("barcode"),
                    "product_name": row.get("product_name"),
                    "variant_name": row.get("variant_name"),
                    "source": "cost_reception_history.barcode",
                }
            )

        catalog_rows = self._execute(
            """
SELECT
    v.bsale_id AS variant_id,
    v.bar_code AS barcode,
    v.description AS variant_name,
    v.product_id,
    p.name AS product_name
FROM bsale.variants v
LEFT JOIN bsale.products p
    ON p.company_id = v.company_id
   AND p.bsale_id = v.product_id
WHERE v.company_id = %s
  AND (
        TRIM(COALESCE(v.bar_code, '')) = %s
     OR v.bar_code ILIKE %s
  )
ORDER BY v.bsale_id
LIMIT 500
""".strip(),
            (int(company_id), normalized, like),
        )

        catalog_ids: list[int] = []
        for row in catalog_rows:
            vid = int(row["variant_id"])
            if vid not in catalog_ids:
                catalog_ids.append(vid)
            details.append(
                {
                    "variant_id": vid,
                    "barcode": row.get("barcode"),
                    "product_name": row.get("product_name"),
                    "variant_name": row.get("variant_name"),
                    "source": "bsale.variants.bar_code",
                }
            )

        resolved: list[int] = []
        for vid in history_ids + catalog_ids:
            if vid not in resolved:
                resolved.append(vid)

        warnings: list[str] = []
        if len(resolved) > 1:
            warnings.append("duplicate_barcode_mapping")

        if not resolved:
            return BarcodeResolution(
                requested_barcode=requested,
                normalized_barcode=normalized,
                catalog_matches=len(catalog_ids),
                resolved_variant_ids=(),
                resolution_source=None,
                duplicate_mapping=False,
                history_rows_found=0,
                barcode_not_found=True,
                no_reception_history=False,
                warnings=tuple(warnings),
                match_details=(),
            )

        if history_ids and catalog_ids:
            source = "both"
        elif history_ids:
            source = "cost_reception_history.barcode"
        else:
            source = "bsale.variants.bar_code"
            warnings.append("no_reception_history")

        return BarcodeResolution(
            requested_barcode=requested,
            normalized_barcode=normalized,
            catalog_matches=len(catalog_ids),
            resolved_variant_ids=tuple(resolved),
            resolution_source=source,
            duplicate_mapping=len(resolved) > 1,
            history_rows_found=0,  # se completa tras fetch
            barcode_not_found=False,
            no_reception_history=not bool(history_ids),
            warnings=tuple(warnings),
            match_details=tuple(details),
        )

    def fetch_history_rows(
        self,
        args: CostAuditArgs,
        *,
        date_from: date,
        date_to: date,
        variant_ids: list[int] | None = None,
    ) -> list[CostAuditRawRow]:
        has_taxes = self.column_exists("bsale", "products", "taxes")
        has_tax_ids = self.column_exists("bsale", "products", "tax_ids_json")
        has_tax_factor = self.column_exists("bsale", "products", "tax_factor")
        has_vc_gross = self.column_exists("bsale", "variant_cost", "average_cost_gross")
        has_vc_tf = self.column_exists("bsale", "variant_cost", "tax_factor")
        has_vc_iva = self.column_exists("bsale", "variant_cost", "iva_rate")
        has_vc_specific = self.column_exists("bsale", "variant_cost", "specific_taxes")
        has_vc_source = self.column_exists("bsale", "variant_cost", "cost_source")

        taxes_expr = "p.taxes AS products_taxes" if has_taxes else "NULL::jsonb AS products_taxes"
        tax_ids_expr = (
            "p.tax_ids_json AS tax_ids_json" if has_tax_ids else "NULL::jsonb AS tax_ids_json"
        )
        tax_factor_expr = (
            "p.tax_factor AS product_tax_factor"
            if has_tax_factor
            else "NULL::numeric AS product_tax_factor"
        )
        vc_gross_expr = (
            "vc.average_cost_gross AS variant_cost_gross"
            if has_vc_gross
            else "NULL::numeric AS variant_cost_gross"
        )
        vc_tf_expr = (
            "vc.tax_factor AS vc_tax_factor" if has_vc_tf else "NULL::numeric AS vc_tax_factor"
        )
        vc_iva_expr = (
            "vc.iva_rate AS vc_iva_rate" if has_vc_iva else "NULL::numeric AS vc_iva_rate"
        )
        vc_spec_expr = (
            "vc.specific_taxes AS specific_taxes"
            if has_vc_specific
            else "NULL::jsonb AS specific_taxes"
        )
        vc_src_expr = (
            "vc.cost_source AS cost_source" if has_vc_source else "NULL::text AS cost_source"
        )

        # date_to inclusivo: admission_date < date_to + 1 day
        date_to_exclusive = date_to + timedelta(days=1)

        sql = f"""
SELECT
    h.id AS history_id,
    h.unique_key,
    h.reception_id,
    h.reception_detail_id,
    h.document_number,
    h.variant_id,
    h.product_id,
    h.product_name,
    h.variant_name,
    h.barcode,
    h.admission_date,
    h.quantity,
    h.cost_net,
    h.iva_amount,
    h.other_taxes,
    h.cost_bruto_erp,
    h.average_cost,
    h.reception_type,
    h.office_id,
    v.code AS variant_code,
    v.bar_code AS catalog_barcode,
    vc.average_cost_net AS variant_cost_net,
    vc.last_update,
    {vc_gross_expr},
    {vc_tf_expr},
    {vc_iva_expr},
    {vc_spec_expr},
    {vc_src_expr},
    {tax_factor_expr},
    {tax_ids_expr},
    {taxes_expr}
FROM analytics.cost_reception_history h
LEFT JOIN bsale.variants v
    ON v.company_id = h.company_id
   AND v.bsale_id = h.variant_id
LEFT JOIN bsale.products p
    ON p.company_id = h.company_id
   AND p.bsale_id = COALESCE(h.product_id, v.product_id)
LEFT JOIN bsale.variant_cost vc
    ON vc.company_id = h.company_id
   AND vc.variant_id = h.variant_id
WHERE h.company_id = %s
  AND h.admission_date >= %s
  AND h.admission_date < %s
""".strip()
        params: list[Any] = [args.company_id, date_from, date_to_exclusive]
        if args.office_id is not None:
            sql += " AND h.office_id = %s"
            params.append(args.office_id)
        if args.variant_id is not None:
            sql += " AND h.variant_id = %s"
            params.append(args.variant_id)
        # Filtro barcode: SOLO por variant_id(s) ya resueltos (misma fuente /costos).
        # Nunca filtrar por variants.code como barcode.
        if variant_ids is not None:
            if not variant_ids:
                return []
            sql += " AND h.variant_id = ANY(%s)"
            params.append(list(variant_ids))
        if args.source_document_id is not None:
            sql += " AND (h.document_number = %s OR h.reception_id = %s)"
            params.extend([args.source_document_id, args.source_document_id])
        sql += " ORDER BY h.admission_date DESC, h.id DESC LIMIT %s"
        params.append(args.limit)

        rows = self._execute(sql, tuple(params))
        out: list[CostAuditRawRow] = []
        for row in rows:
            doc_num = row.get("document_number")
            reception_id = row.get("reception_id")
            source_doc = (
                int(doc_num)
                if doc_num is not None
                else (int(reception_id) if reception_id is not None else None)
            )
            tax_ids = _parse_jsonish(row.get("tax_ids_json"))
            products_taxes = _parse_jsonish(row.get("products_taxes"))
            out.append(
                CostAuditRawRow(
                    history_id=int(row["history_id"]),
                    unique_key=row.get("unique_key"),
                    reception_id=int(reception_id) if reception_id is not None else None,
                    reception_detail_id=(
                        int(row["reception_detail_id"])
                        if row.get("reception_detail_id") is not None
                        else None
                    ),
                    source_document_id=source_doc,
                    variant_id=int(row["variant_id"]),
                    product_id=(
                        int(row["product_id"]) if row.get("product_id") is not None else None
                    ),
                    product_name=row.get("product_name"),
                    variant_name=row.get("variant_name"),
                    barcode=row.get("barcode"),
                    variant_code=row.get("variant_code"),
                    catalog_barcode=row.get("catalog_barcode"),
                    admission_date=row.get("admission_date"),
                    quantity=coerce_optional_decimal(row.get("quantity")),
                    cost_net=coerce_optional_decimal(row.get("cost_net")),
                    iva_amount=coerce_optional_decimal(row.get("iva_amount")),
                    other_taxes=coerce_optional_decimal(row.get("other_taxes")),
                    cost_bruto_erp=coerce_optional_decimal(row.get("cost_bruto_erp")),
                    average_cost=coerce_optional_decimal(row.get("average_cost")),
                    reception_type=row.get("reception_type"),
                    office_id=(
                        int(row["office_id"]) if row.get("office_id") is not None else None
                    ),
                    variant_cost_net=coerce_optional_decimal(row.get("variant_cost_net")),
                    variant_cost_gross=coerce_optional_decimal(row.get("variant_cost_gross")),
                    vc_iva_rate=coerce_optional_decimal(row.get("vc_iva_rate")),
                    vc_tax_factor=coerce_optional_decimal(row.get("vc_tax_factor")),
                    specific_taxes=_parse_jsonish(row.get("specific_taxes")),
                    cost_source=row.get("cost_source"),
                    last_update=row.get("last_update"),
                    product_tax_factor=coerce_optional_decimal(row.get("product_tax_factor")),
                    tax_ids_json=tax_ids,
                    products_taxes=products_taxes,
                    has_products_taxes_column=has_taxes,
                    has_tax_ids_json=has_tax_ids and tax_ids is not None,
                    has_product_tax_factor=has_tax_factor
                    and row.get("product_tax_factor") is not None,
                )
            )
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
                percentage=optional_decimal(row.get("percentage"))
                if row.get("percentage") is not None
                else None,
            )
        return out

    @staticmethod
    def collect_tax_ids(rows: list[CostAuditRawRow]) -> list[int]:
        ids: list[int] = []
        for r in rows:
            ids.extend(_tax_id_list(r.tax_ids_json))
        return ids
