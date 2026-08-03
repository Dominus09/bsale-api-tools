"""Servicio read-only Costos V2 consolidado por empresa (E.7.3)."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from typing import Any

from backend.repositories.cost_v2_company_read_repo import CostV2CompanyReadRepository
from backend.schemas.cost_v2_company_read import (
    CALCULATION_VERSION_PIN,
    DEFAULT_LIMIT,
    VISUAL_NO_CHANGE_ABS,
    coverage_label,
    decode_company_product_cursor,
    derive_business_statuses,
    derive_office_alignment_status,
    encode_company_product_cursor,
    validate_change_threshold,
    validate_company_id_for_v2_company,
    validate_company_product_sort,
    validate_date_range,
    validate_limit,
)
from backend.schemas.cost_v2_read import (
    DATA_SOURCE,
    CostV2ReadValidationError,
    money_to_json,
    normalize_warnings,
    parse_iso_date,
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
        "scope": "company",
        "latest_view_note": (
            "Consolidado por empresa: costo vigente = última recepción calculable "
            "hasta date_to; cambio vs último costo distinto. Sin office_id."
        ),
    }


def _iso(d: Any) -> str | None:
    if d is None:
        return None
    if hasattr(d, "isoformat"):
        return d.isoformat()
    return str(d)


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


def _with_repo(fn):
    conn = open_readonly_connection()
    try:
        executor = make_psycopg_executor(conn)
        repo = CostV2CompanyReadRepository(executor)
        return fn(repo)
    finally:
        try:
            conn.rollback()
        except Exception:
            pass
        conn.close()


def map_company_product_item(row: dict[str, Any]) -> dict[str, Any]:
    warns = warnings_list(row.get("current_warnings_json"))
    active = int(row.get("active_offices_count") or 0)
    with_v2 = int(row.get("offices_with_v2_data") or 0)
    with_cost = int(row.get("offices_with_current_cost") or 0)
    has_diff = bool(row.get("has_office_difference"))
    requires_review = bool(row.get("requires_review"))
    alignment = derive_office_alignment_status(
        offices_with_current_cost=with_cost,
        has_office_difference=has_diff,
    )
    # Una sola oficina nunca es “alineada”
    if with_cost < 2:
        has_diff = False
    business = derive_business_statuses(
        requires_review=requires_review,
        has_office_difference=has_diff and with_cost >= 2,
        offices_with_v2_data=with_v2,
        active_offices_count=active,
        offices_with_current_cost=with_cost,
    )
    change_amount = row.get("change_amount")
    change_percent = row.get("change_percent")
    has_comparable = bool(row.get("has_comparable_cost"))
    visual_flat = False
    if (
        has_comparable
        and change_amount is not None
        and abs(Decimal(str(change_amount))) < VISUAL_NO_CHANGE_ABS
    ):
        visual_flat = True

    return {
        "variant_id": int(row["variant_id"]),
        "barcode": row.get("barcode"),
        "product_name": row.get("product_name"),
        "variant_name": row.get("variant_name"),
        "current_history_id": (
            int(row["current_history_id"])
            if row.get("current_history_id") is not None
            else None
        ),
        "current_cost": money_to_json(row.get("current_cost")),
        "current_cost_raw": money_to_json(row.get("current_cost_raw")),
        "current_admission_date": _iso(row.get("current_admission_date")),
        "current_office_id": (
            int(row["current_office_id"])
            if row.get("current_office_id") is not None
            else None
        ),
        "current_office_name": row.get("current_office_name"),
        "current_document_number": row.get("current_document_number"),
        "current_quality_status": row.get("current_quality_status"),
        "current_warnings": warns,
        "current_stored_cost_net": money_to_json(row.get("current_stored_cost_net")),
        "current_stored_gross_cost": money_to_json(row.get("current_stored_gross_cost")),
        "current_calculated_iva_amount": money_to_json(
            row.get("current_calculated_iva_amount")
        ),
        "current_additional_tax_amount_total": money_to_json(
            row.get("current_additional_tax_amount_total")
        ),
        "current_additional_taxes": _json_list(
            row.get("current_additional_taxes_json")
        ),
        "current_total_tax_rate": money_to_json(row.get("current_total_tax_rate")),
        "previous_distinct_history_id": (
            int(row["previous_distinct_history_id"])
            if row.get("previous_distinct_history_id") is not None
            else None
        ),
        "previous_distinct_cost": money_to_json(row.get("previous_distinct_cost")),
        "change_amount": money_to_json(change_amount),
        "change_percent": money_to_json(change_percent),
        "last_change_date": _iso(row.get("last_change_date")),
        "has_comparable_cost": has_comparable,
        "visual_no_change": visual_flat,
        "active_offices_count": active,
        "offices_with_v2_data": with_v2,
        "offices_with_current_cost": with_cost,
        "coverage_label": coverage_label(with_v2=with_v2, active=active),
        "requires_review": requires_review,
        "has_office_difference": has_diff and with_cost >= 2,
        "office_alignment_status": alignment,
        "business_statuses": business,
        "last_reception_date": _iso(row.get("last_reception_date")),
        "receptions_in_period": int(row.get("receptions_in_period") or 0),
        "last_calculated_at": _iso(row.get("last_calculated_at")),
        "latest_history_id": (
            int(row["latest_history_id"])
            if row.get("latest_history_id") is not None
            else None
        ),
        "tax_ids_source": row.get("tax_ids_source"),
        "tax_rates_source": row.get("tax_rates_source"),
        "tax_context_source": row.get("tax_context_source"),
        "calculation_version": row.get("calculation_version"),
        "calculation_batch_id": row.get("calculation_batch_id"),
        "source_history_fingerprint": row.get("source_history_fingerprint"),
        "tax_context_fingerprint": row.get("tax_context_fingerprint"),
        "calculation_result_fingerprint": row.get("calculation_result_fingerprint"),
        "resolved_tax_ids": _json_list(row.get("resolved_tax_ids_json")),
        "company_id": (
            int(row["company_id"]) if row.get("company_id") is not None else None
        ),
    }


def list_company_products(
    *,
    company_id: int,
    date_from: date | str,
    date_to: date | str,
    limit: int = DEFAULT_LIMIT,
    cursor: str | None = None,
    sort: str | None = None,
    search: str | None = None,
    barcode: str | None = None,
    warning: str | None = None,
    movement: str | None = None,
    situation: str | None = None,
    only_relevant_changes: bool = False,
    min_abs_change_percent: Decimal | float | str | None = None,
    change_threshold_percent: Decimal | float | str | None = None,
) -> dict[str, Any]:
    cid = validate_company_id_for_v2_company(company_id)
    df = parse_iso_date(date_from)
    dt = parse_iso_date(date_to)
    validate_date_range(date_from=df, date_to=dt)
    lim = validate_limit(limit)
    sort_key = validate_company_product_sort(sort)
    cur = decode_company_product_cursor(cursor) if cursor else None
    thr = validate_change_threshold(change_threshold_percent)
    min_pct = None
    if min_abs_change_percent is not None and str(min_abs_change_percent) != "":
        min_pct = Decimal(str(min_abs_change_percent))
    warn = None
    if warning:
        warns = normalize_warnings([warning])
        warn = warns[0] if warns else None

    def _run(repo: CostV2CompanyReadRepository):
        rows = repo.list_company_products(
            company_id=cid,
            date_from=df,
            date_to=dt,
            limit=lim,
            sort=sort_key,
            cursor=cur,
            search=search,
            barcode=barcode,
            warning=warn,
            movement=movement,
            situation=situation,
            only_relevant_changes=only_relevant_changes,
            min_abs_change_percent=min_pct,
            change_threshold_percent=thr,
        )
        has_more = len(rows) > lim
        page_rows = rows[:lim]
        items = [map_company_product_item(r) for r in page_rows]
        next_cursor = None
        if has_more and page_rows:
            last = page_rows[-1]
            next_cursor = encode_company_product_cursor(
                sort=sort_key,
                variant_id=int(last["variant_id"]),
                admission_date=last.get("last_reception_date"),
                product_name=last.get("product_name"),
                current_cost=last.get("current_cost"),
                change_percent=last.get("change_percent"),
                change_abs=(
                    abs(last["change_amount"])
                    if last.get("change_amount") is not None
                    else None
                ),
                requires_review=bool(last.get("requires_review")),
                has_office_difference=bool(last.get("has_office_difference")),
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

    return _with_repo(_run)


def summarize_company_products(
    *,
    company_id: int,
    date_from: date | str,
    date_to: date | str,
    change_threshold_percent: Decimal | float | str | None = None,
) -> dict[str, Any]:
    cid = validate_company_id_for_v2_company(company_id)
    df = parse_iso_date(date_from)
    dt = parse_iso_date(date_to)
    validate_date_range(date_from=df, date_to=dt)
    thr = validate_change_threshold(change_threshold_percent)

    def _run(repo: CostV2CompanyReadRepository):
        offices = repo.list_active_offices(company_id=cid)
        s = repo.summarize_company_products(
            company_id=cid,
            date_from=df,
            date_to=dt,
            change_threshold_percent=thr,
        )
        active = int(s.get("active_offices_count") or len(offices))
        with_v2 = int(s.get("offices_with_v2_coverage") or 0)
        office_diff = int(s.get("products_with_office_difference") or 0)
        # Sin comparación real si <2 oficinas con V2
        office_diff_available = with_v2 >= 2
        return {
            "summary": {
                "total_products": int(s.get("total_products") or 0),
                "products_with_current_cost": int(
                    s.get("products_with_current_cost") or 0
                ),
                "products_without_current_cost": int(
                    s.get("products_without_current_cost") or 0
                ),
                "relevant_changes": int(s.get("relevant_changes") or 0),
                "products_requiring_review": int(
                    s.get("products_requiring_review") or 0
                ),
                "products_with_outlier": int(s.get("products_with_outlier") or 0),
                "products_with_office_difference": (
                    office_diff if office_diff_available else None
                ),
                "office_difference_comparable": office_diff_available,
                "active_offices_count": active,
                "offices_with_v2_coverage": with_v2,
                "coverage_label": coverage_label(with_v2=with_v2, active=active),
                "latest_reception_date": _iso(s.get("latest_reception_date")),
                "latest_sync_or_calculation_at": _iso(
                    s.get("latest_sync_or_calculation_at")
                ),
                "change_threshold_percent": money_to_json(thr),
                "active_offices": [
                    {"office_id": o["office_id"], "office_name": o["office_name"]}
                    for o in offices
                ],
            },
            "meta": _meta(),
        }

    return _with_repo(_run)


def get_company_product(
    *,
    company_id: int,
    variant_id: int,
    date_from: date | str,
    date_to: date | str,
) -> dict[str, Any]:
    cid = validate_company_id_for_v2_company(company_id)
    df = parse_iso_date(date_from)
    dt = parse_iso_date(date_to)
    validate_date_range(date_from=df, date_to=dt)

    def _run(repo: CostV2CompanyReadRepository):
        row = repo.get_company_product(
            company_id=cid,
            variant_id=int(variant_id),
            date_from=df,
            date_to=dt,
        )
        if not row:
            raise LookupError("Producto no encontrado en el scope autorizado")
        item = map_company_product_item(row)
        current = row.get("current_cost")
        offices = repo.list_company_product_offices(
            company_id=cid,
            variant_id=int(variant_id),
            date_to=dt,
            company_current_cost=current if isinstance(current, Decimal) else (
                Decimal(str(current)) if current is not None else None
            ),
        )
        with_cost = sum(1 for o in offices if o.get("current_cost") is not None)
        office_items = []
        for o in offices:
            sit = o.get("situation")
            if not o.get("has_v2_data"):
                label = "Cobertura V2 pendiente"
                sit = "coverage_pending"
            elif with_cost < 2 and o.get("current_cost") is not None:
                label = "Sin comparación entre oficinas"
                sit = "no_comparison"
            elif sit == "aligned":
                label = "Alineada"
            elif sit == "different":
                label = "Costo distinto"
            elif sit == "no_calculable":
                label = "Sin costo calculable"
            else:
                label = "Con datos"
            office_items.append(
                {
                    "office_id": o["office_id"],
                    "office_name": o["office_name"],
                    "current_cost": money_to_json(o.get("current_cost")),
                    "admission_date": _iso(o.get("admission_date")),
                    "history_id": (
                        int(o["history_id"]) if o.get("history_id") is not None else None
                    ),
                    "quality_status": o.get("quality_status"),
                    "warnings": warnings_list(o.get("warnings")),
                    "diff_vs_company": money_to_json(o.get("diff_vs_company")),
                    "has_v2_data": bool(o.get("has_v2_data")),
                    "situation": sit,
                    "situation_label": label,
                }
            )
        item["offices"] = office_items
        return {"item": item, "meta": _meta()}

    return _with_repo(_run)


def list_company_product_history(
    *,
    company_id: int,
    variant_id: int,
    date_from: date | str,
    date_to: date | str,
    office_id: int | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    cid = validate_company_id_for_v2_company(company_id)
    df = parse_iso_date(date_from)
    dt = parse_iso_date(date_to)
    validate_date_range(date_from=df, date_to=dt)
    lim = min(max(int(limit), 1), 500)

    def _run(repo: CostV2CompanyReadRepository):
        rows = repo.list_company_product_history(
            company_id=cid,
            variant_id=int(variant_id),
            date_from=df,
            date_to=dt,
            office_id=office_id,
            limit=lim,
        )
        items = []
        costs: list[Decimal] = []
        for r in rows:
            cost = r.get("corrected_gross_cost")
            prev = r.get("prev_cost_in_series")
            changed = (
                cost is not None
                and prev is not None
                and cost != prev
            )
            if isinstance(cost, Decimal):
                costs.append(cost)
            elif cost is not None:
                costs.append(Decimal(str(cost)))
            change_amt = None
            change_pct = None
            if cost is not None and prev is not None:
                change_amt = cost - prev if isinstance(cost, Decimal) else (
                    Decimal(str(cost)) - Decimal(str(prev))
                )
                if prev != 0 and isinstance(prev, Decimal) or prev is not None:
                    p = prev if isinstance(prev, Decimal) else Decimal(str(prev))
                    if p != 0:
                        change_pct = (change_amt / p) * Decimal("100")
            items.append(
                {
                    "history_id": int(r["history_id"]),
                    "company_id": (
                        int(r["company_id"]) if r.get("company_id") is not None else None
                    ),
                    "office_id": (
                        int(r["office_id"]) if r.get("office_id") is not None else None
                    ),
                    "office_name": r.get("office_name"),
                    "admission_date": _iso(r.get("admission_date")),
                    "document_number": r.get("document_number"),
                    "stored_cost_net": money_to_json(r.get("stored_cost_net")),
                    "stored_gross_cost": money_to_json(r.get("stored_gross_cost")),
                    "corrected_gross_cost": money_to_json(cost),
                    "calculated_iva_amount": money_to_json(
                        r.get("calculated_iva_amount")
                    ),
                    "additional_tax_amount_total": money_to_json(
                        r.get("additional_tax_amount_total")
                    ),
                    "total_tax_rate": money_to_json(r.get("total_tax_rate")),
                    "effective_quality_status": r.get("effective_quality_status"),
                    "warnings": warnings_list(r.get("warnings_json")),
                    "change_amount": money_to_json(change_amt),
                    "change_percent": money_to_json(change_pct),
                    "cost_changed": bool(changed),
                    "calculation_version": r.get("calculation_version"),
                    "calculation_batch_id": r.get("calculation_batch_id"),
                    "calculated_at": _iso(r.get("calculated_at")),
                    "tax_ids_source": r.get("tax_ids_source"),
                    "tax_rates_source": r.get("tax_rates_source"),
                    "source_history_fingerprint": r.get("source_history_fingerprint"),
                    "tax_context_fingerprint": r.get("tax_context_fingerprint"),
                    "calculation_result_fingerprint": r.get(
                        "calculation_result_fingerprint"
                    ),
                    "resolved_tax_ids": _json_list(r.get("resolved_tax_ids_json")),
                }
            )
        observed_min = min(costs) if costs else None
        observed_max = max(costs) if costs else None
        return {
            "items": items,
            "observed_range": {
                "min_cost": money_to_json(observed_min),
                "max_cost": money_to_json(observed_max),
            },
            "meta": _meta(),
        }

    return _with_repo(_run)
