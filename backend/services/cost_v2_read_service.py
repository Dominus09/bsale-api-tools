"""Servicio read-only Costos V2 para endpoints /cost-analytics/v2/*."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from typing import Any, Callable

from backend.db import get_connection
from backend.repositories.cost_v2_read_repo import CostV2ReadRepository
from backend.schemas.cost_v2_read import (
    CALCULATION_VERSION_PIN,
    DATA_SOURCE,
    DEFAULT_LIMIT,
    NEEDS_REVIEW_STATUSES,
    CostV2ReadValidationError,
    decode_cursor,
    decode_product_cursor,
    encode_cursor,
    encode_product_cursor,
    money_to_json,
    normalize_statuses,
    normalize_warnings,
    parse_iso_date,
    unit_difference,
    validate_change_threshold,
    validate_date_range,
    validate_limit,
    validate_product_sort,
    warnings_list,
)
from backend.services.analytics.validate_distribuidora_source import (
    make_psycopg_executor,
    open_readonly_connection,
)


def _meta() -> dict[str, str]:
    return {
        "data_source": DATA_SOURCE,
        "calculation_version": CALCULATION_VERSION_PIN,
        "latest_view_note": (
            "v_cost_reception_calculated_latest es latest temporal; "
            "API V2 pinnea calculation_version en la tabla (UNIQUE history_id+version). "
            "Ver backend/sql/048_v_cost_reception_calculated_latest_by_version.sql"
        ),
    }


def _json_list(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return []
    if isinstance(raw, list):
        return list(raw)
    return []


def _map_list_item(row: dict[str, Any]) -> dict[str, Any]:
    warns = warnings_list(row.get("warnings_json"))
    corrected = row.get("corrected_gross_cost")
    stored_gross = row.get("stored_gross_cost")
    unit_diff = unit_difference(corrected, stored_gross)
    adm = row.get("admission_date")
    if hasattr(adm, "isoformat"):
        adm_s = adm.isoformat()
    else:
        adm_s = str(adm) if adm is not None else None
    return {
        "history_id": int(row["history_id"]),
        "company_id": int(row["company_id"]) if row.get("company_id") is not None else None,
        "office_id": int(row["office_id"]) if row.get("office_id") is not None else None,
        "admission_date": adm_s,
        "document_number": row.get("document_number"),
        "document": row.get("document"),
        "reception_id": row.get("reception_id"),
        "variant_id": int(row["variant_id"]) if row.get("variant_id") is not None else None,
        "barcode": row.get("barcode"),
        "product_name": row.get("product_name"),
        "variant_name": row.get("variant_name"),
        "stored_cost_net": money_to_json(row.get("stored_cost_net")),
        "stored_cost_gross": money_to_json(stored_gross),
        "stored_iva_amount": money_to_json(row.get("stored_iva_amount")),
        "stored_other_taxes": money_to_json(row.get("stored_other_taxes")),
        "stored_quantity": money_to_json(row.get("stored_quantity")),
        "corrected_gross_cost": money_to_json(corrected),
        "calculated_iva_amount": money_to_json(row.get("calculated_iva_amount")),
        "additional_tax_amount_total": money_to_json(
            row.get("additional_tax_amount_total")
        ),
        "total_tax_rate": money_to_json(row.get("total_tax_rate")),
        "resolved_tax_ids": _json_list(row.get("resolved_tax_ids_json")),
        "additional_taxes": _json_list(row.get("additional_taxes_json")),
        "tax_ids_source": row.get("tax_ids_source"),
        "tax_rates_source": row.get("tax_rates_source"),
        "tax_context_quality": row.get("tax_resolution_quality"),
        "historical_tax_context_available": row.get("tax_context_is_historical"),
        "effective_quality_status": row.get("effective_quality_status"),
        "warnings": warns,
        "suspicious_outlier": "suspicious_outlier" in warns,
        "calculation_version": row.get("calculation_version"),
        "calculation_batch_id": (
            str(row["calculation_batch_id"])
            if row.get("calculation_batch_id") is not None
            else None
        ),
        "calculated_at": (
            row["calculated_at"].isoformat()
            if hasattr(row.get("calculated_at"), "isoformat")
            else row.get("calculated_at")
        ),
        "source_history_fingerprint": row.get("source_history_fingerprint"),
        "tax_context_fingerprint": row.get("tax_context_fingerprint"),
        "calculation_result_fingerprint": row.get("calculation_result_fingerprint"),
        "unit_difference": money_to_json(unit_diff),
    }


def _map_detail(row: dict[str, Any]) -> dict[str, Any]:
    base = _map_list_item(row)
    additional = _json_list(row.get("additional_taxes_json"))
    # Serializar montos internos de additional_taxes si vienen Decimal
    clean_additional: list[Any] = []
    for item in additional:
        if isinstance(item, dict):
            clean = dict(item)
            for k, v in list(clean.items()):
                if isinstance(v, Decimal):
                    clean[k] = money_to_json(v)
            clean_additional.append(clean)
        else:
            clean_additional.append(item)

    base["calculation"] = {
        "stored_cost_net": money_to_json(row.get("stored_cost_net")),
        "iva": {
            "tax_id": row.get("iva_tax_id"),
            "rate": money_to_json(row.get("iva_rate")),
            "amount": money_to_json(row.get("calculated_iva_amount")),
        },
        "additional_taxes": clean_additional,
        "corrected_gross_cost": money_to_json(row.get("corrected_gross_cost")),
        "formula": "net + iva + additional_taxes",
    }
    base["reception_tax_ids"] = _json_list(row.get("reception_tax_ids_json"))
    base["catalog_tax_ids"] = _json_list(row.get("catalog_tax_ids_json"))
    base["tax_context_source"] = row.get("tax_context_source")
    base["gross_difference_amount"] = money_to_json(row.get("gross_difference_amount"))
    return base


def _with_repo(
    fn: Callable[[CostV2ReadRepository], Any],
    *,
    get_conn: Callable[[], Any] = get_connection,
) -> Any:
    conn = open_readonly_connection(get_conn)
    sql_log: list[str] = []
    try:
        executor = make_psycopg_executor(
            conn,
            statement_timeout_seconds=20,
            lock_timeout="3s",
            sql_log=sql_log,
        )
        repo = CostV2ReadRepository(executor)
        return fn(repo)
    finally:
        try:
            conn.rollback()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def list_v2_receptions(
    *,
    company_id: int,
    office_id: int,
    date_from: date | str,
    date_to: date | str,
    limit: int = DEFAULT_LIMIT,
    cursor: str | None = None,
    status: list[str] | None = None,
    warning: list[str] | None = None,
    barcode: str | None = None,
    variant_id: int | None = None,
    document_number: int | None = None,
    history_id: int | None = None,
    search: str | None = None,
    get_conn: Callable[[], Any] = get_connection,
) -> dict[str, Any]:
    d_from = parse_iso_date(date_from)
    d_to = parse_iso_date(date_to)
    validate_date_range(date_from=d_from, date_to=d_to)
    lim = validate_limit(int(limit))
    statuses = normalize_statuses(status)
    warnings = normalize_warnings(warning)
    cur_adm: date | None = None
    cur_hid: int | None = None
    if cursor:
        cur_adm, cur_hid = decode_cursor(cursor)

    def _run(repo: CostV2ReadRepository) -> dict[str, Any]:
        rows = repo.list_receptions(
            company_id=company_id,
            office_id=office_id,
            date_from=d_from,
            date_to=d_to,
            limit=lim,
            cursor_admission_date=cur_adm,
            cursor_history_id=cur_hid,
            statuses=statuses,
            warnings=warnings,
            barcode=barcode,
            variant_id=variant_id,
            document_number=document_number,
            history_id=history_id,
            search=search,
        )
        has_more = len(rows) > lim
        page_rows = rows[:lim]
        items = [_map_list_item(r) for r in page_rows]
        next_cursor = None
        if has_more and page_rows:
            last = page_rows[-1]
            adm = last["admission_date"]
            if not isinstance(adm, date):
                adm = parse_iso_date(adm)
            next_cursor = encode_cursor(
                admission_date=adm, history_id=int(last["history_id"])
            )
        return {
            "items": items,
            "page": {
                "limit": lim,
                "has_more": has_more,
                "next_cursor": next_cursor,
            },
            "meta": _meta(),
        }

    return _with_repo(_run, get_conn=get_conn)


def get_v2_reception(
    *,
    company_id: int,
    office_id: int,
    history_id: int,
    get_conn: Callable[[], Any] = get_connection,
) -> dict[str, Any]:
    def _run(repo: CostV2ReadRepository) -> dict[str, Any]:
        row = repo.get_reception(
            company_id=company_id,
            office_id=office_id,
            history_id=history_id,
        )
        if row is None:
            raise LookupError("Recepción V2 no encontrada en el scope autorizado")
        return {
            "item": _map_detail(row),
            "meta": _meta(),
        }

    return _with_repo(_run, get_conn=get_conn)


def summarize_v2(
    *,
    company_id: int,
    office_id: int,
    date_from: date | str,
    date_to: date | str,
    status: list[str] | None = None,
    warning: list[str] | None = None,
    barcode: str | None = None,
    variant_id: int | None = None,
    document_number: int | None = None,
    history_id: int | None = None,
    search: str | None = None,
    get_conn: Callable[[], Any] = get_connection,
) -> dict[str, Any]:
    d_from = parse_iso_date(date_from)
    d_to = parse_iso_date(date_to)
    validate_date_range(date_from=d_from, date_to=d_to)
    statuses = normalize_statuses(status)
    warnings = normalize_warnings(warning)

    def _run(repo: CostV2ReadRepository) -> dict[str, Any]:
        summary = repo.summarize(
            company_id=company_id,
            office_id=office_id,
            date_from=d_from,
            date_to=d_to,
            statuses=statuses,
            warnings=warnings,
            barcode=barcode,
            variant_id=variant_id,
            document_number=document_number,
            history_id=history_id,
            search=search,
        )
        status_sum = sum(summary["by_status"].values())
        min_d = summary.get("min_admission_date")
        max_d = summary.get("max_admission_date")
        if hasattr(min_d, "isoformat"):
            min_d = min_d.isoformat()
        if hasattr(max_d, "isoformat"):
            max_d = max_d.isoformat()
        return {
            "summary": {
                "total_rows": summary["total_rows"],
                "unique_variants": summary["unique_variants"],
                "unique_documents": summary["unique_documents"],
                "by_status": summary["by_status"],
                "by_warning": summary["by_warning"],
                "with_corrected_gross": summary["with_corrected_gross"],
                "without_corrected_gross": summary["without_corrected_gross"],
                "min_admission_date": min_d,
                "max_admission_date": max_d,
                "status_sum_matches_total": status_sum == summary["total_rows"],
            },
            "meta": _meta(),
            # Explicitamente ausentes (no sumar costos unitarios):
            # corrected_gross_sum / unit_difference_sum / purchase_impact
        }

    return _with_repo(_run, get_conn=get_conn)


def _map_product_item(row: dict[str, Any]) -> dict[str, Any]:
    warns = warnings_list(row.get("current_warnings_json"))
    adm = row.get("latest_admission_date")
    prev_adm = row.get("previous_admission_date")
    calc_at = row.get("last_calculated_at")

    def _d(v: Any) -> str | None:
        if hasattr(v, "isoformat"):
            return v.isoformat()
        return str(v) if v is not None else None

    return {
        "variant_id": int(row["variant_id"]) if row.get("variant_id") is not None else None,
        "company_id": int(row["company_id"]) if row.get("company_id") is not None else None,
        "office_id": int(row["office_id"]) if row.get("office_id") is not None else None,
        "barcode": row.get("barcode"),
        "product_name": row.get("product_name"),
        "variant_name": row.get("variant_name"),
        "latest_history_id": (
            int(row["latest_history_id"]) if row.get("latest_history_id") is not None else None
        ),
        "latest_admission_date": _d(adm),
        "latest_document_number": row.get("latest_document_number"),
        "latest_document": row.get("latest_document"),
        "current_stored_cost_net": money_to_json(row.get("current_stored_cost_net")),
        "current_corrected_gross_cost": money_to_json(
            row.get("current_corrected_gross_cost")
        ),
        "current_stored_gross_cost": money_to_json(row.get("current_stored_gross_cost")),
        "current_calculated_iva_amount": money_to_json(
            row.get("current_calculated_iva_amount")
        ),
        "current_additional_tax_amount_total": money_to_json(
            row.get("current_additional_tax_amount_total")
        ),
        "current_additional_taxes": _json_list(row.get("current_additional_taxes_json")),
        "current_total_tax_rate": money_to_json(row.get("current_total_tax_rate")),
        "current_iva_rate": money_to_json(row.get("current_iva_rate")),
        "current_quality_status": row.get("current_quality_status"),
        "current_warnings": warns,
        "previous_history_id": (
            int(row["previous_history_id"])
            if row.get("previous_history_id") is not None
            else None
        ),
        "previous_admission_date": _d(prev_adm),
        "previous_corrected_gross_cost": money_to_json(
            row.get("previous_corrected_gross_cost")
        ),
        "unit_change_amount": money_to_json(row.get("unit_change_amount")),
        "unit_change_percent": money_to_json(row.get("unit_change_percent")),
        "receptions_count": int(row.get("receptions_count") or 0),
        "last_calculated_at": (
            calc_at.isoformat() if hasattr(calc_at, "isoformat") else calc_at
        ),
        "tax_ids_source": row.get("tax_ids_source"),
        "tax_rates_source": row.get("tax_rates_source"),
        "tax_context_source": row.get("tax_context_source"),
        "calculation_version": row.get("calculation_version"),
        "source_history_fingerprint": row.get("source_history_fingerprint"),
        "tax_context_fingerprint": row.get("tax_context_fingerprint"),
        "calculation_result_fingerprint": row.get("calculation_result_fingerprint"),
        "needs_review": (
            row.get("current_corrected_gross_cost") is None
            or str(row.get("current_quality_status") or "") in NEEDS_REVIEW_STATUSES
        ),
    }


def list_v2_products(
    *,
    company_id: int,
    office_id: int,
    date_from: date | str,
    date_to: date | str,
    limit: int = DEFAULT_LIMIT,
    cursor: str | None = None,
    sort: str | None = None,
    status: list[str] | None = None,
    warning: list[str] | None = None,
    barcode: str | None = None,
    search: str | None = None,
    only_with_changes: bool = False,
    only_needs_review: bool = False,
    min_abs_change_percent: Decimal | float | str | None = None,
    get_conn: Callable[[], Any] = get_connection,
) -> dict[str, Any]:
    d_from = parse_iso_date(date_from)
    d_to = parse_iso_date(date_to)
    validate_date_range(date_from=d_from, date_to=d_to)
    lim = validate_limit(int(limit))
    sort_key = validate_product_sort(sort)
    statuses = normalize_statuses(status)
    warnings = normalize_warnings(warning)
    cur: dict[str, Any] | None = None
    if cursor:
        cur = decode_product_cursor(cursor)
        if cur.get("sort") != sort_key:
            raise CostV2ReadValidationError(
                "cursor no corresponde al sort actual",
                error_type="invalid_cursor",
            )
    min_abs: Decimal | None = None
    if min_abs_change_percent is not None and str(min_abs_change_percent).strip() != "":
        min_abs = Decimal(str(min_abs_change_percent))

    def _run(repo: CostV2ReadRepository) -> dict[str, Any]:
        rows = repo.list_products(
            company_id=company_id,
            office_id=office_id,
            date_from=d_from,
            date_to=d_to,
            limit=lim,
            sort=sort_key,
            cursor=cur,
            statuses=statuses,
            warnings=warnings,
            barcode=barcode,
            search=search,
            only_with_changes=only_with_changes,
            only_needs_review=only_needs_review,
            min_abs_change_percent=min_abs,
            needs_review_statuses=list(NEEDS_REVIEW_STATUSES),
        )
        has_more = len(rows) > lim
        page_rows = rows[:lim]
        items = [_map_product_item(r) for r in page_rows]
        next_cursor = None
        if has_more and page_rows:
            last = page_rows[-1]
            adm = last.get("latest_admission_date")
            if adm is not None and not isinstance(adm, date):
                adm = parse_iso_date(adm)
            next_cursor = encode_product_cursor(
                sort=sort_key,
                variant_id=int(last["variant_id"]),
                admission_date=adm if isinstance(adm, date) else None,
                product_name=last.get("product_name"),
                current_cost=last.get("current_corrected_gross_cost"),
                unit_change_percent=last.get("unit_change_percent"),
                status=last.get("current_quality_status"),
            )
        return {
            "items": items,
            "page": {
                "limit": lim,
                "has_more": has_more,
                "next_cursor": next_cursor,
                "sort": sort_key,
            },
            "meta": _meta(),
        }

    return _with_repo(_run, get_conn=get_conn)


def get_v2_product(
    *,
    company_id: int,
    office_id: int,
    variant_id: int,
    date_from: date | str,
    date_to: date | str,
    history_limit: int = 20,
    get_conn: Callable[[], Any] = get_connection,
) -> dict[str, Any]:
    d_from = parse_iso_date(date_from)
    d_to = parse_iso_date(date_to)
    validate_date_range(date_from=d_from, date_to=d_to)
    hist_lim = validate_limit(int(history_limit))

    def _run(repo: CostV2ReadRepository) -> dict[str, Any]:
        row = repo.get_product(
            company_id=company_id,
            office_id=office_id,
            variant_id=variant_id,
            date_from=d_from,
            date_to=d_to,
            history_limit=hist_lim,
        )
        if row is None:
            raise LookupError("Producto V2 no encontrado en el scope autorizado")
        item = _map_product_item(row)
        receptions = [_map_list_item(r) for r in (row.get("receptions") or [])]
        item["receptions"] = receptions
        item["calculation"] = {
            "stored_cost_net": item["current_stored_cost_net"],
            "iva": {
                "tax_id": row.get("current_iva_tax_id"),
                "rate": item["current_iva_rate"],
                "amount": item["current_calculated_iva_amount"],
            },
            "additional_taxes": item["current_additional_taxes"],
            "corrected_gross_cost": item["current_corrected_gross_cost"],
            "formula": "net + iva + additional_taxes",
        }
        return {"item": item, "meta": _meta()}

    return _with_repo(_run, get_conn=get_conn)


def summarize_v2_products(
    *,
    company_id: int,
    office_id: int,
    date_from: date | str,
    date_to: date | str,
    change_threshold_percent: Decimal | float | str | None = None,
    status: list[str] | None = None,
    warning: list[str] | None = None,
    barcode: str | None = None,
    search: str | None = None,
    get_conn: Callable[[], Any] = get_connection,
) -> dict[str, Any]:
    d_from = parse_iso_date(date_from)
    d_to = parse_iso_date(date_to)
    validate_date_range(date_from=d_from, date_to=d_to)
    thr = validate_change_threshold(change_threshold_percent)
    statuses = normalize_statuses(status)
    warnings = normalize_warnings(warning)

    def _run(repo: CostV2ReadRepository) -> dict[str, Any]:
        summary = repo.summarize_products(
            company_id=company_id,
            office_id=office_id,
            date_from=d_from,
            date_to=d_to,
            change_threshold_percent=thr,
            barcode=barcode,
            search=search,
            statuses=statuses,
            warnings=warnings,
            needs_review_statuses=list(NEEDS_REVIEW_STATUSES),
        )
        latest = summary.get("latest_reception_date")
        calc_at = summary.get("latest_calculation_at")
        if hasattr(latest, "isoformat"):
            latest = latest.isoformat()
        if hasattr(calc_at, "isoformat"):
            calc_at = calc_at.isoformat()
        return {
            "summary": {
                "total_products": summary["total_products"],
                "products_with_current_cost": summary["products_with_current_cost"],
                "products_without_calculable_cost": summary[
                    "products_without_calculable_cost"
                ],
                "products_incomplete_tax_context": summary[
                    "products_incomplete_tax_context"
                ],
                "products_with_outlier": summary["products_with_outlier"],
                "products_with_increase": summary["products_with_increase"],
                "products_with_decrease": summary["products_with_decrease"],
                "products_with_change_over_threshold": summary[
                    "products_with_change_over_threshold"
                ],
                "products_needing_review": summary["products_needing_review"],
                "products_missing_cost": summary["products_missing_cost"],
                "products_rounding_warning": summary["products_rounding_warning"],
                "latest_reception_date": latest,
                "latest_calculation_at": calc_at,
                "change_threshold_percent": money_to_json(thr),
            },
            "meta": _meta(),
        }

    return _with_repo(_run, get_conn=get_conn)


__all__ = [
    "CostV2ReadValidationError",
    "get_v2_reception",
    "list_v2_receptions",
    "list_v2_products",
    "get_v2_product",
    "summarize_v2",
    "summarize_v2_products",
]
