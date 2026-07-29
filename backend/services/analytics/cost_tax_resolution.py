"""Resolución tributaria por identidad (tax_id), no por orden del array.

Mapeo canónico Quillotana / Bsale Chile documentado en el repo
(tests gross commercial + tasas conocidas):

| tax_id | Rol                         | Tasa fallback |
|--------|-----------------------------|---------------|
| 1      | IVA                         | 19%           |
| 2      | ILA vino / cerveza-vino     | 20.5%         |
| 3      | ILA cerveza                 | 20.5%         |
| 8      | ILA destilados              | 31.5%         |

Las tasas de ``bsale.taxes`` (catálogo) tienen prioridad sobre el fallback.
Nunca se asume que taxes[0] es IVA.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from backend.services.analytics.cost_audit_models import TaxCatalogEntry
from backend.services.analytics.money import ZERO
from backend.services.analytics.tax_models import TaxCategory, classify_tax_category

COMMERCIAL_QUANT = Decimal("0.01")

# IDs conocidos de IVA (Bsale company Quillotana / catálogo habitual)
IVA_TAX_IDS: frozenset[int] = frozenset({1})

# Fallback cuando bsale.taxes no trae percentage (solo auditoría; no escribe sync)
TAX_ID_FALLBACK: dict[int, tuple[str, Decimal, str]] = {
    1: ("IVA", Decimal("19"), "iva"),
    2: ("ILA vino", Decimal("20.5"), "ila_beer_wine"),
    3: ("ILA cerveza", Decimal("20.5"), "ila_beer_wine"),
    8: ("ILA destilados", Decimal("31.5"), "ila_spirits"),
}


def _commercial(value: Decimal) -> Decimal:
    return value.quantize(COMMERCIAL_QUANT, rounding=ROUND_HALF_UP)


def _parse_tax_ids(tax_ids_json: Any) -> list[int]:
    if tax_ids_json is None:
        return []
    raw = tax_ids_json
    if isinstance(raw, str):
        import json

        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return []
    if not isinstance(raw, list):
        return []
    out: list[int] = []
    for item in raw:
        try:
            out.append(int(item))
        except (TypeError, ValueError):
            continue
    return out


def _is_iva_entry(
    tax_id: int,
    *,
    name: str | None,
    rate: Decimal | None,
) -> bool:
    if tax_id in IVA_TAX_IDS:
        return True
    n = (name or "").strip().lower()
    if n in {"iva", "i.v.a", "i.v.a."} or n.startswith("iva "):
        return True
    # No clasificar como IVA solo por rate==19 (ILA podría coincidir en otros países)
    return False


def _category_for(tax_id: int, rate: Decimal | None, *, is_iva: bool) -> str:
    if is_iva:
        return "iva"
    if tax_id in TAX_ID_FALLBACK:
        return TAX_ID_FALLBACK[tax_id][2]
    if rate is not None:
        cat = classify_tax_category(iva_rate_pct=None, ila_rate_pct=rate)
        if cat != TaxCategory.UNKNOWN:
            return cat.value
    return "specific_other"


@dataclass(frozen=True, slots=True)
class ResolvedSpecificTax:
    tax_id: int
    name: str | None
    rate: Decimal
    category: str
    amount: Decimal | None = None  # se completa con cost_net

    def with_amount(self, cost_net: Decimal) -> ResolvedSpecificTax:
        return ResolvedSpecificTax(
            tax_id=self.tax_id,
            name=self.name,
            rate=self.rate,
            category=self.category,
            amount=_commercial(cost_net * self.rate / Decimal("100")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "tax_id": self.tax_id,
            "name": self.name,
            "rate": str(self.rate),
            "category": self.category,
            "amount": None if self.amount is None else str(self.amount),
        }


@dataclass(frozen=True, slots=True)
class TaxResolution:
    iva_tax_id: int | None
    iva_rate: Decimal | None
    specific_taxes: tuple[ResolvedSpecificTax, ...]
    specific_tax_total_rate: Decimal | None
    total_tax_rate: Decimal | None
    tax_resolution_source: str | None
    tax_resolution_quality: str  # resolved | partial | unavailable
    unresolved_tax_ids: tuple[int, ...] = ()

    def expected_iva_amount(self, cost_net: Decimal) -> Decimal | None:
        if self.iva_rate is None:
            return None
        return _commercial(cost_net * self.iva_rate / Decimal("100"))

    def expected_specific_amount(self, cost_net: Decimal) -> Decimal | None:
        if not self.specific_taxes:
            return ZERO if self.iva_rate is not None else None
        total = ZERO
        for t in self.specific_taxes:
            total += cost_net * t.rate / Decimal("100")
        return _commercial(total)

    def expected_gross(self, cost_net: Decimal) -> Decimal | None:
        if self.total_tax_rate is None:
            return None
        return _commercial(cost_net * (Decimal("1") + self.total_tax_rate / Decimal("100")))

    def specifics_with_amounts(self, cost_net: Decimal) -> tuple[ResolvedSpecificTax, ...]:
        return tuple(t.with_amount(cost_net) for t in self.specific_taxes)

    def to_dict(self) -> dict[str, Any]:
        return {
            "iva_tax_id": self.iva_tax_id,
            "iva_rate": None if self.iva_rate is None else str(self.iva_rate),
            "specific_taxes": [t.to_dict() for t in self.specific_taxes],
            "specific_tax_total_rate": (
                None if self.specific_tax_total_rate is None else str(self.specific_tax_total_rate)
            ),
            "total_tax_rate": None if self.total_tax_rate is None else str(self.total_tax_rate),
            "tax_resolution_source": self.tax_resolution_source,
            "tax_resolution_quality": self.tax_resolution_quality,
            "unresolved_tax_ids": list(self.unresolved_tax_ids),
        }


def _rate_and_name_for_id(
    tax_id: int,
    catalog: dict[int, TaxCatalogEntry],
) -> tuple[Decimal | None, str | None, str]:
    """Retorna (rate, name, source_tag)."""
    entry = catalog.get(tax_id)
    if entry is not None and entry.percentage is not None:
        return entry.percentage, entry.name, "bsale.taxes"
    if tax_id in TAX_ID_FALLBACK:
        name, rate, _cat = TAX_ID_FALLBACK[tax_id]
        return rate, name, "canonical_fallback"
    return None, None, "unresolved"


def resolve_taxes_from_ids(
    tax_ids_json: Any,
    *,
    tax_catalog: dict[int, TaxCatalogEntry] | None = None,
    cost_net: Decimal | None = None,
) -> TaxResolution:
    """Resuelve IVA + específicos por tax_id. Orden del array irrelevante."""
    catalog = tax_catalog or {}
    ids = _parse_tax_ids(tax_ids_json)
    if not ids:
        return TaxResolution(
            iva_tax_id=None,
            iva_rate=None,
            specific_taxes=(),
            specific_tax_total_rate=None,
            total_tax_rate=None,
            tax_resolution_source=None,
            tax_resolution_quality="unavailable",
        )

    iva_tax_id: int | None = None
    iva_rate: Decimal | None = None
    specifics: list[ResolvedSpecificTax] = []
    unresolved: list[int] = []
    sources: set[str] = set()

    # Orden estable por tax_id para determinismo (no el orden del JSON)
    for tax_id in sorted(set(ids)):
        rate, name, src = _rate_and_name_for_id(tax_id, catalog)
        if rate is None:
            unresolved.append(tax_id)
            continue
        sources.add(src)
        is_iva = _is_iva_entry(tax_id, name=name, rate=rate)
        if is_iva:
            # Si hay varios IVA, conservar el de mayor tasa / primero por id
            if iva_rate is None or rate > iva_rate:
                iva_tax_id = tax_id
                iva_rate = rate
        else:
            cat = _category_for(tax_id, rate, is_iva=False)
            amount = (
                _commercial(cost_net * rate / Decimal("100")) if cost_net is not None else None
            )
            specifics.append(
                ResolvedSpecificTax(
                    tax_id=tax_id,
                    name=name,
                    rate=rate,
                    category=cat,
                    amount=amount,
                )
            )

    if iva_rate is None and not specifics:
        return TaxResolution(
            iva_tax_id=None,
            iva_rate=None,
            specific_taxes=(),
            specific_tax_total_rate=None,
            total_tax_rate=None,
            tax_resolution_source=",".join(sorted(sources)) or None,
            tax_resolution_quality="unavailable",
            unresolved_tax_ids=tuple(unresolved),
        )

    spec_total = sum((t.rate for t in specifics), ZERO) if specifics else ZERO
    total = (iva_rate or ZERO) + spec_total
    quality = "resolved"
    if unresolved:
        quality = "partial"
    if iva_rate is None and specifics:
        quality = "partial"

    source = "bsale.taxes"
    if sources == {"canonical_fallback"}:
        source = "canonical_fallback"
    elif "canonical_fallback" in sources and "bsale.taxes" in sources:
        source = "bsale.taxes+canonical_fallback"
    elif sources:
        source = "+".join(sorted(sources))

    return TaxResolution(
        iva_tax_id=iva_tax_id,
        iva_rate=iva_rate,
        specific_taxes=tuple(specifics),
        specific_tax_total_rate=_commercial(spec_total) if specifics else ZERO,
        total_tax_rate=_commercial(total),
        tax_resolution_source=source,
        tax_resolution_quality=quality,
        unresolved_tax_ids=tuple(unresolved),
    )


def tax_ids_fingerprint(tax_ids_json: Any) -> str:
    ids = sorted(set(_parse_tax_ids(tax_ids_json)))
    return ",".join(str(i) for i in ids) if ids else ""
