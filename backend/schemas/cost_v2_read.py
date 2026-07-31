"""Constantes, cursor opaco y validaciones para lectura Costos V2."""

from __future__ import annotations

import base64
import json
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from backend.services.analytics.cost_v2_models import (
    CALCULATION_VERSION,
    EffectiveQualityStatus,
)

DEFAULT_LIMIT = 50
MAX_LIMIT = 200
CALCULATION_VERSION_PIN = CALCULATION_VERSION

# Fuente real de la API E.5: tabla + filtro de versión (UNIQUE history_id+version).
# La vista v_cost_reception_calculated_latest es latest TEMPORAL y no es segura
# para pinnear cost-v2.0.0 cuando exista otra versión más reciente.
DATA_SOURCE = "analytics.cost_reception_calculated"

ALLOWED_QUALITY_STATUSES: frozenset[str] = frozenset(
    {
        "missing_cost",
        "gross_component_mismatch",
        "duplicated_taxes_in_gross",
        "missing_taxes_in_gross",
        "incomplete_tax_context",
        "valid_gross",
    }
)

# Warnings conocidos del motor V2 (filtro; no inventar SQL libre).
KNOWN_WARNING_FILTERS: frozenset[str] = frozenset(
    {
        "suspicious_outlier",
        "tax_ids_not_consumed",
        "variant_barcode_mismatch",
        "source_conflict",
        "reception_tax_context_unavailable",
        "stored_components_rounding",
    }
)


class CostV2ReadValidationError(ValueError):
    def __init__(self, message: str, *, error_type: str = "invalid_args") -> None:
        super().__init__(message)
        self.error_type = error_type


def parse_iso_date(value: date | datetime | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if "T" in text:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    return date.fromisoformat(text)


def validate_date_range(*, date_from: date, date_to: date) -> None:
    if date_to < date_from:
        raise CostV2ReadValidationError(
            "date_from debe ser <= date_to",
            error_type="invalid_date_range",
        )


def validate_limit(limit: int) -> int:
    if limit < 1:
        raise CostV2ReadValidationError("limit debe ser >= 1", error_type="invalid_limit")
    if limit > MAX_LIMIT:
        raise CostV2ReadValidationError(
            f"limit no puede superar {MAX_LIMIT}",
            error_type="invalid_limit",
        )
    return int(limit)


def normalize_statuses(raw: list[str] | None) -> list[str] | None:
    if not raw:
        return None
    out: list[str] = []
    for item in raw:
        for part in str(item).split(","):
            s = part.strip()
            if not s:
                continue
            if s not in ALLOWED_QUALITY_STATUSES:
                raise CostV2ReadValidationError(
                    f"status inválido: {s}",
                    error_type="invalid_status",
                )
            out.append(s)
    return out or None


def normalize_warnings(raw: list[str] | None) -> list[str] | None:
    if not raw:
        return None
    out: list[str] = []
    for item in raw:
        for part in str(item).split(","):
            w = part.strip()
            if not w:
                continue
            if w not in KNOWN_WARNING_FILTERS:
                raise CostV2ReadValidationError(
                    f"warning inválido: {w}",
                    error_type="invalid_warning",
                )
            out.append(w)
    return out or None


def encode_cursor(*, admission_date: date, history_id: int) -> str:
    payload = {
        "admission_date": admission_date.isoformat(),
        "history_id": int(history_id),
    }
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_cursor(token: str) -> tuple[date, int]:
    text = (token or "").strip()
    if not text:
        raise CostV2ReadValidationError("cursor vacío", error_type="invalid_cursor")
    pad = "=" * (-len(text) % 4)
    try:
        raw = base64.urlsafe_b64decode(text + pad)
        data = json.loads(raw.decode("utf-8"))
        adm = parse_iso_date(data["admission_date"])
        hid = int(data["history_id"])
    except Exception as exc:
        raise CostV2ReadValidationError(
            "cursor inválido",
            error_type="invalid_cursor",
        ) from exc
    if hid <= 0:
        raise CostV2ReadValidationError("cursor inválido", error_type="invalid_cursor")
    return adm, hid


def escape_ilike(term: str) -> str:
    return (
        term.replace("\\", "\\\\")
        .replace("%", "\\%")
        .replace("_", "\\_")
    )


def money_to_json(value: Decimal | None) -> str | None:
    """Montos V2 como string decimal (sin float binario)."""
    if value is None:
        return None
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    return format(value, "f")


def date_to_exclusive(date_to: date) -> date:
    return date_to + timedelta(days=1)


def unit_difference(
    corrected_gross: Decimal | None, stored_gross: Decimal | None
) -> Decimal | None:
    if corrected_gross is None or stored_gross is None:
        return None
    return corrected_gross - stored_gross


def warnings_list(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return []
    if isinstance(raw, list):
        return [str(x) for x in raw]
    return []


# Re-export type hint helper
QualityStatus = EffectiveQualityStatus
