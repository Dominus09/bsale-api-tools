"""Schemas y helpers para lectura consolidada Costos V2 por empresa (E.7.3)."""

from __future__ import annotations

import base64
import json
from datetime import date
from decimal import Decimal
from typing import Any

from backend.schemas.cost_v2_read import (
    CALCULATION_VERSION_PIN,
    CostV2ReadValidationError,
    DEFAULT_LIMIT,
    MAX_LIMIT,
    parse_iso_date,
    validate_change_threshold,
    validate_date_range,
    validate_limit,
)

# company_id permitidos en consolidado (no confiar ciegamente en query string)
ALLOWED_COMPANY_IDS_V2_COMPANY: frozenset[int] = frozenset({3})

# Oficinas operativas de control de costos por empresa (excluye BAJAS / auxiliares).
# No afirmar que siempre serán 4: si la empresa no está mapeada, se consultan activas.
COST_CONTROL_OFFICE_IDS_BY_COMPANY: dict[int, tuple[int, ...]] = {
    3: (1, 3, 4, 5),  # Bodega Central, Supermercado, Q1, Q2
}

COMPANY_PRODUCT_SORTS: frozenset[str] = frozenset(
    {
        "latest_reception",
        "pct_increase",
        "pct_decrease",
        "abs_change",
        "absolute_change",  # alias → abs_change
        "product",
        "product_name",  # alias → product
        "requires_review",
        "office_difference",
    }
)

COMPANY_PRODUCT_SORT_ALIASES: dict[str, str] = {
    "absolute_change": "abs_change",
    "product_name": "product",
}

# Umbral visual de “Sin cambio” en UI ($0,50). Backend conserva Decimal exacto.
VISUAL_NO_CHANGE_ABS: Decimal = Decimal("0.5")

# Estados tributarios que fuerzan revisión (no incluye missing_taxes_in_gross)
COMPANY_REVIEW_STATUSES: frozenset[str] = frozenset(
    {
        "incomplete_tax_context",
        "missing_cost",
        "duplicated_taxes_in_gross",
        "gross_component_mismatch",
    }
)

COMPANY_REVIEW_WARNINGS: frozenset[str] = frozenset(
    {
        "suspicious_outlier",
    }
)

BUSINESS_STATUS_REQUIRES_REVIEW = "requires_review"
BUSINESS_STATUS_OFFICE_DIFFERENCE = "office_difference"
BUSINESS_STATUS_PARTIAL_COVERAGE = "partial_coverage"
BUSINESS_STATUS_OFFICES_ALIGNED = "offices_aligned"
BUSINESS_STATUS_INSUFFICIENT_COVERAGE = "insufficient_coverage"
# Alias legacy (tests / clientes previos)
BUSINESS_STATUS_NO_OFFICE_COMPARE = BUSINESS_STATUS_INSUFFICIENT_COVERAGE


def validate_company_id_for_v2_company(company_id: int) -> int:
    cid = int(company_id)
    if cid not in ALLOWED_COMPANY_IDS_V2_COMPANY:
        raise CostV2ReadValidationError(
            "company_id no autorizado para Costos V2 consolidado",
            error_type="forbidden_company",
        )
    return cid


def validate_company_product_sort(sort: str | None) -> str:
    key = (sort or "latest_reception").strip()
    if key not in COMPANY_PRODUCT_SORTS:
        raise CostV2ReadValidationError(
            f"sort inválido: {key}",
            error_type="invalid_sort",
        )
    return COMPANY_PRODUCT_SORT_ALIASES.get(key, key)


def encode_company_product_cursor(
    *,
    sort: str,
    variant_id: int,
    admission_date: date | None = None,
    product_name: str | None = None,
    current_cost: Decimal | None = None,
    change_percent: Decimal | None = None,
    change_abs: Decimal | None = None,
    requires_review: bool | None = None,
    has_office_difference: bool | None = None,
) -> str:
    payload: dict[str, Any] = {"sort": sort, "variant_id": int(variant_id)}
    if admission_date is not None:
        payload["admission_date"] = admission_date.isoformat()
    if product_name is not None:
        payload["product_name"] = product_name
    if current_cost is not None:
        payload["current_cost"] = format(current_cost, "f")
    if change_percent is not None:
        payload["change_percent"] = format(change_percent, "f")
    if change_abs is not None:
        payload["change_abs"] = format(change_abs, "f")
    if requires_review is not None:
        payload["requires_review"] = bool(requires_review)
    if has_office_difference is not None:
        payload["has_office_difference"] = bool(has_office_difference)
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_company_product_cursor(token: str) -> dict[str, Any]:
    text = (token or "").strip()
    if not text:
        raise CostV2ReadValidationError("cursor vacío", error_type="invalid_cursor")
    pad = "=" * (-len(text) % 4)
    try:
        raw = base64.urlsafe_b64decode(text + pad)
        data = json.loads(raw.decode("utf-8"))
        vid = int(data["variant_id"])
        sort = str(data.get("sort") or "latest_reception")
    except Exception as exc:
        raise CostV2ReadValidationError(
            "cursor inválido",
            error_type="invalid_cursor",
        ) from exc
    if vid <= 0 or sort not in COMPANY_PRODUCT_SORTS:
        raise CostV2ReadValidationError("cursor inválido", error_type="invalid_cursor")
    out: dict[str, Any] = {"sort": sort, "variant_id": vid}
    if data.get("admission_date"):
        out["admission_date"] = parse_iso_date(data["admission_date"])
    if "product_name" in data:
        out["product_name"] = data.get("product_name") or ""
    if data.get("current_cost") is not None:
        out["current_cost"] = Decimal(str(data["current_cost"]))
    if data.get("change_percent") is not None:
        out["change_percent"] = Decimal(str(data["change_percent"]))
    if data.get("change_abs") is not None:
        out["change_abs"] = Decimal(str(data["change_abs"]))
    if "requires_review" in data:
        out["requires_review"] = bool(data["requires_review"])
    if "has_office_difference" in data:
        out["has_office_difference"] = bool(data["has_office_difference"])
    return out


def coverage_label(*, with_v2: int, active: int) -> str:
    return f"{int(with_v2)} de {int(active)} oficinas"


def derive_office_alignment_status(
    *,
    offices_with_current_cost: int,
    has_office_difference: bool,
) -> str:
    """Alineación solo con ≥2 oficinas con costo vigente calculable."""
    n = int(offices_with_current_cost)
    if n < 2:
        return BUSINESS_STATUS_INSUFFICIENT_COVERAGE
    if has_office_difference:
        return BUSINESS_STATUS_OFFICE_DIFFERENCE
    return BUSINESS_STATUS_OFFICES_ALIGNED


def derive_business_statuses(
    *,
    requires_review: bool,
    has_office_difference: bool,
    offices_with_v2_data: int,
    active_offices_count: int,
    offices_with_current_cost: int,
) -> list[str]:
    out: list[str] = []
    if requires_review:
        out.append(BUSINESS_STATUS_REQUIRES_REVIEW)
    alignment = derive_office_alignment_status(
        offices_with_current_cost=offices_with_current_cost,
        has_office_difference=has_office_difference,
    )
    out.append(alignment)
    if int(offices_with_v2_data) < int(active_offices_count):
        out.append(BUSINESS_STATUS_PARTIAL_COVERAGE)
    # Deduplicar preservando orden
    seen: set[str] = set()
    uniq: list[str] = []
    for s in out:
        if s not in seen:
            seen.add(s)
            uniq.append(s)
    return uniq


__all__ = [
    "ALLOWED_COMPANY_IDS_V2_COMPANY",
    "BUSINESS_STATUS_INSUFFICIENT_COVERAGE",
    "BUSINESS_STATUS_NO_OFFICE_COMPARE",
    "BUSINESS_STATUS_OFFICE_DIFFERENCE",
    "BUSINESS_STATUS_OFFICES_ALIGNED",
    "BUSINESS_STATUS_PARTIAL_COVERAGE",
    "BUSINESS_STATUS_REQUIRES_REVIEW",
    "CALCULATION_VERSION_PIN",
    "COMPANY_PRODUCT_SORTS",
    "COMPANY_PRODUCT_SORT_ALIASES",
    "COMPANY_REVIEW_STATUSES",
    "COMPANY_REVIEW_WARNINGS",
    "COST_CONTROL_OFFICE_IDS_BY_COMPANY",
    "CostV2ReadValidationError",
    "DEFAULT_LIMIT",
    "MAX_LIMIT",
    "VISUAL_NO_CHANGE_ABS",
    "coverage_label",
    "decode_company_product_cursor",
    "derive_business_statuses",
    "derive_office_alignment_status",
    "encode_company_product_cursor",
    "validate_change_threshold",
    "validate_company_id_for_v2_company",
    "validate_company_product_sort",
    "validate_date_range",
    "validate_limit",
]
