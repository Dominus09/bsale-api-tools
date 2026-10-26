"""Alcance comercial V2: listas de precios evaluadas y validez de resultados.

Una lista se evalúa solo si está autorizada en ``COMMERCIAL_PRICE_LISTS`` y además vigente en
BSALE RAW (state = 0 y sin missing_since); el nombre nunca decide. Las listas retiradas (p. ej.
COMODITI) conservan sus reglas en bsale.margin_rules, pero quedan fuera de márgenes, dashboard y
alertas. Si una retirada reaparece vigente en RAW se informa como discrepancia administrativa.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from backend.services.analytics_v2.contracts import (
    COMMERCIAL_PRICE_LISTS,
    NET_COST_VERIFIED,
    POLICIES_REVIEWED,
    RETIRED_PRICE_LISTS,
    PolicyReviewStatus,
    PriceListScopeStatus,
    ResultValidity,
)
from backend.services.analytics_v2.display_labels import price_list_visible_name

PRICE_LISTS_SQL = """
SELECT pl.bsale_id AS price_list_id, pl.name, pl.state, pl.missing_since
FROM bsale_raw.price_lists pl
WHERE pl.company_id = %s
ORDER BY pl.bsale_id
"""

_DISCREPANCIES = frozenset(
    {
        PriceListScopeStatus.AUTORIZADA_NO_VIGENTE,
        PriceListScopeStatus.RETIRADA_REACTIVADA,
    }
)


class CommercialScopeNotConfigured(LookupError):
    pass


def is_price_list_active(state: Any, missing_since: Any) -> bool:
    return state is not None and int(state) == 0 and missing_since is None


@dataclass(frozen=True)
class PriceListEntry:
    price_list_id: int
    name: str | None
    commercial_label: str | None
    raw_state: Any
    raw_missing: bool
    in_raw: bool
    status: PriceListScopeStatus

    @property
    def is_discrepancy(self) -> bool:
        return self.status in _DISCREPANCIES

    @property
    def visible_name(self) -> str:
        """Etiqueta comercial para UI/Excel (sin ID técnico)."""
        return price_list_visible_name(
            name=self.name,
            commercial_label=self.commercial_label,
            price_list_id=self.price_list_id,
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "price_list_id": self.price_list_id,
            "name": self.name,
            "commercial_label": self.commercial_label,
            "visible_name": self.visible_name,
            "raw_state": self.raw_state,
            "raw_missing": self.raw_missing,
            "in_raw": self.in_raw,
            "status": self.status.value,
            "discrepancy": self.is_discrepancy,
        }


@dataclass(frozen=True)
class PriceListScope:
    company_id: int
    entries: tuple[PriceListEntry, ...]

    @property
    def evaluated_ids(self) -> frozenset[int]:
        return frozenset(e.price_list_id for e in self.entries if e.status is PriceListScopeStatus.EVALUADA)

    @property
    def discrepancies(self) -> tuple[PriceListEntry, ...]:
        return tuple(e for e in self.entries if e.is_discrepancy)

    def entry(self, price_list_id: int) -> PriceListEntry | None:
        return next((e for e in self.entries if e.price_list_id == price_list_id), None)

    def to_json(self) -> dict[str, Any]:
        return {
            "company_id": self.company_id,
            "evaluated_price_list_ids": sorted(self.evaluated_ids),
            "price_lists": [e.to_json() for e in self.entries],
            "discrepancies": [e.to_json() for e in self.discrepancies],
        }


def resolve_price_list_scope(
    company_id: int,
    rows: Iterable[Mapping[str, Any]],
    *,
    authorized: Mapping[int, str] | None = None,
    retired: Mapping[int, str] | None = None,
) -> PriceListScope:
    if authorized is None:
        if company_id not in COMMERCIAL_PRICE_LISTS:
            raise CommercialScopeNotConfigured(
                f"Alcance comercial de listas no configurado para company_id={company_id}"
            )
        authorized = COMMERCIAL_PRICE_LISTS[company_id]
    if retired is None:
        retired = RETIRED_PRICE_LISTS.get(company_id, {})

    raw = {int(r["price_list_id"]): r for r in rows}
    entries: list[PriceListEntry] = []
    for pid in sorted(set(raw) | set(authorized) | set(retired)):
        row = raw.get(pid)
        active = row is not None and is_price_list_active(row.get("state"), row.get("missing_since"))
        if pid in retired:
            status = PriceListScopeStatus.RETIRADA_REACTIVADA if active else PriceListScopeStatus.RETIRADA
        elif pid in authorized:
            status = PriceListScopeStatus.EVALUADA if active else PriceListScopeStatus.AUTORIZADA_NO_VIGENTE
        else:
            status = PriceListScopeStatus.VIGENTE_NO_AUTORIZADA if active else PriceListScopeStatus.NO_VIGENTE
        entries.append(
            PriceListEntry(
                price_list_id=pid,
                name=(row or {}).get("name") or retired.get(pid),
                commercial_label=authorized.get(pid),
                raw_state=(row or {}).get("state"),
                raw_missing=row is not None and row.get("missing_since") is not None,
                in_raw=row is not None,
                status=status,
            )
        )
    return PriceListScope(company_id, tuple(entries))


def rule_in_commercial_scope(price_list_id: int, scope: PriceListScope) -> bool:
    """Reglas de listas no evaluadas se conservan, pero no generan alertas ni entran en márgenes."""
    return int(price_list_id) in scope.evaluated_ids


def evaluated_price_list_names(scope: PriceListScope) -> dict[int, str]:
    """Solo listas autorizadas y vigentes (EVALUADA): id → nombre comercial visible."""
    return {
        e.price_list_id: e.visible_name
        for e in scope.entries
        if e.status is PriceListScopeStatus.EVALUADA
    }


def policy_review_status(reviewed: bool = POLICIES_REVIEWED) -> PolicyReviewStatus:
    return PolicyReviewStatus.VALIDADA if reviewed else PolicyReviewStatus.PENDIENTE_REVISION


def result_validity(
    policy_status: PolicyReviewStatus | None,
    cost_verified: bool = NET_COST_VERIFIED,
) -> tuple[ResultValidity, list[str]]:
    """Un resultado de margen solo es validado si la política y el costo base están validados."""
    reasons: list[str] = []
    if policy_status is not PolicyReviewStatus.VALIDADA:
        reasons.append("politica_pendiente_revision")
    if not cost_verified:
        reasons.append("costo_no_verificado")
    return (ResultValidity.VALIDADO if not reasons else ResultValidity.PROVISIONAL), reasons
