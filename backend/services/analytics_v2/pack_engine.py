"""Costo de packs Bsale (classification = 3) a partir de su composición original en RAW.

Fuente: ``bsale_raw.products.payload -> 'pack_details'``. El costo de un pack es la suma de sus
componentes: costo neto de cada variante componente × cantidad, y costo bruto comercial calculado
con los impuestos propios de cada componente (sus ``tax_ids``), nunca con los del pack. El costo
promedio del SKU del pack y sus impuestos propios no participan.

Un margen solo es calculable con una composición completa: pack cerrado (variante + cantidad por
detalle), todos los componentes existentes, con costo y con impuestos resueltos. Packs abiertos
(variante elegida al vender), packs anidados no resolubles y ciclos quedan pendientes. Puro: sin SQL.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from backend.services.analytics_v2.contracts import (
    INCREASE_ALERT_PCT,
    PACK_MAX_DEPTH,
    CostBasis,
    CostReliability,
    PackComponentStatus as C,
    PackCostStatus as P,
    money,
)
from backend.services.analytics_v2.tax_profile import GrossCost, TaxProfile, commercial_gross_cost

COMPOSITION_SOURCE = "bsale_raw.products.payload.pack_details"

ISSUE_ABSENT = "pack_details_ausente"
ISSUE_NOT_EXPANDED = "pack_details_no_expandido"
ISSUE_EMPTY = "pack_details_vacio"
ISSUE_UNPARSEABLE = "pack_details_no_interpretable"
ISSUE_DETAIL_UNPARSEABLE = "detalle_no_interpretable"
ISSUE_INVALID_QUANTITY = "cantidad_invalida"
ISSUE_INACTIVE_DETAIL = "detalle_inactivo_excluido"

WARN_INACTIVE_COMPONENT = "componente_inactivo"
WARN_STALE_COST = "componente_costo_desactualizado"
WARN_REVIEW_COST = "componente_costo_requiere_revision"
WARN_COST_INCREASE = "componente_alza_de_costo"
WARN_NESTED = "pack_anidado"

# Peor primero: la confiabilidad del pack es la peor de sus componentes.
_RELIABILITY_ORDER = (
    CostReliability.SIN_COSTO,
    CostReliability.REQUIERE_REVISION,
    CostReliability.DESACTUALIZADO,
    CostReliability.SOLO_PROMEDIO_BSALE,
    CostReliability.CONSISTENTE,
)

# Precedencia del estado del pack cuando varios componentes fallan.
_LINE_TO_PACK = (
    (C.CICLO, P.CICLO_DETECTADO),
    (C.VARIANTE_NO_DETERMINADA, P.PACK_ABIERTO),
    (C.INEXISTENTE, P.COMPONENTE_INEXISTENTE),
    (C.ANIDAMIENTO_EXCESIVO, P.COMPONENTE_PACK_NO_CALCULABLE),
    (C.PACK_NO_CALCULABLE, P.COMPONENTE_PACK_NO_CALCULABLE),
    (C.REQUIERE_REVISION_TRIBUTARIA, P.COMPONENTE_REQUIERE_REVISION_TRIBUTARIA),
    (C.SIN_COSTO, P.COMPONENTE_SIN_COSTO),
)


# ---------------------------------------------------------------- composición


@dataclass(frozen=True, slots=True)
class PackComponentRef:
    position: int
    variant_id: int | None
    product_id: int | None
    quantity: Decimal | None
    multiple_variant: bool
    detail_id: int | None = None

    @property
    def open(self) -> bool:
        return self.multiple_variant or self.variant_id is None

    def to_json(self) -> dict[str, Any]:
        return {
            "position": self.position,
            "detail_id": self.detail_id,
            "variant_id": self.variant_id,
            "product_id": self.product_id,
            "quantity": money(self.quantity),
            "multiple_variant": self.multiple_variant,
        }


@dataclass(frozen=True, slots=True)
class PackComposition:
    components: tuple[PackComponentRef, ...]
    issues: tuple[str, ...] = ()

    @property
    def status(self) -> P | None:
        """Bloqueo de la composición misma; None si se puede evaluar componente a componente."""
        if ISSUE_UNPARSEABLE in self.issues or ISSUE_DETAIL_UNPARSEABLE in self.issues or ISSUE_INVALID_QUANTITY in self.issues:
            return P.COMPOSICION_NO_INTERPRETABLE
        if not self.components:
            return P.SIN_COMPOSICION
        if any(c.open for c in self.components):
            return P.PACK_ABIERTO
        return None

    @property
    def open(self) -> bool:
        return any(c.open for c in self.components)

    def to_json(self) -> dict[str, Any]:
        return {
            "source": COMPOSITION_SOURCE,
            "components": [c.to_json() for c in self.components],
            "issues": list(self.issues),
            "open": self.open,
        }


def _int_id(value: Any) -> int | None:
    if isinstance(value, dict):
        value = value.get("id")
    if isinstance(value, bool) or value is None:
        return None
    try:
        out = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    # Bsale informa productId = 0 en detalles de pack cerrado (solo variante).
    return out if out > 0 else None


def _first_id(entry: dict[str, Any], keys: tuple[str, ...]) -> int | None:
    for key in keys:
        if key in entry:
            found = _int_id(entry[key])
            if found is not None:
                return found
    return None


def _quantity(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        out = Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None
    if not out.is_finite() or out <= 0:
        return None
    # Bsale puede enviar 1, "1" o 1.0: la misma cantidad debe producir los mismos montos.
    return out.quantize(Decimal(1)) if out == out.to_integral_value() else out.normalize()


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    try:
        return int(value) == 1
    except (TypeError, ValueError):
        return False


_VARIANT_KEYS = ("variant", "variantId", "variant_id", "variantPromoId")
_PRODUCT_KEYS = ("product", "productId", "product_id", "productPromoId")


def parse_pack_details(raw: Any) -> PackComposition:
    """Interpreta ``pack_details`` tal como lo guarda RAW (lista, o colección ``{items: [...]}``)."""
    if raw is None:
        return PackComposition((), (ISSUE_ABSENT,))
    if isinstance(raw, (str, bytes)):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            return PackComposition((), (ISSUE_UNPARSEABLE,))
    if isinstance(raw, dict):
        if "items" in raw:
            raw = raw["items"]
        elif "href" in raw:
            return PackComposition((), (ISSUE_NOT_EXPANDED,))
        else:
            return PackComposition((), (ISSUE_UNPARSEABLE,))
    if not isinstance(raw, list):
        return PackComposition((), (ISSUE_UNPARSEABLE,))
    components: list[PackComponentRef] = []
    issues: list[str] = []
    for position, entry in enumerate(raw, start=1):
        if not isinstance(entry, dict):
            issues.append(ISSUE_DETAIL_UNPARSEABLE)
            continue
        state = entry.get("state")
        if state not in (None, 0, "0"):
            issues.append(ISSUE_INACTIVE_DETAIL)
            continue
        variant_id = _first_id(entry, _VARIANT_KEYS)
        product_id = _first_id(entry, _PRODUCT_KEYS)
        if variant_id is None and product_id is None:
            issues.append(ISSUE_DETAIL_UNPARSEABLE)
            continue
        quantity = _quantity(entry.get("quantity"))
        if quantity is None:
            issues.append(ISSUE_INVALID_QUANTITY)
        components.append(
            PackComponentRef(
                position=position,
                variant_id=variant_id,
                product_id=product_id,
                quantity=quantity,
                multiple_variant=_truthy(entry.get("multipleVariant", entry.get("multiple_variant"))),
                detail_id=_int_id(entry.get("id")),
            )
        )
    if not raw:
        issues.append(ISSUE_EMPTY)
    elif not components and ISSUE_INACTIVE_DETAIL in issues:
        issues.append(ISSUE_EMPTY)
    return PackComposition(tuple(components), tuple(dict.fromkeys(issues)))


# ---------------------------------------------------------------- hechos de componentes


@dataclass(frozen=True, slots=True)
class ComponentFacts:
    """Lo que el motor necesita de cada variante (pack o no), ya leído desde RAW."""

    variant_id: int
    product_id: int | None
    code: str | None
    name: str
    active: bool
    average_net: Decimal | None
    last_comparable_net: Decimal | None
    tax_profile: TaxProfile
    reliability: CostReliability
    cost_change_pct: Decimal | None = None
    composition: PackComposition | None = None

    @property
    def is_pack(self) -> bool:
        return self.composition is not None

    def net_for(self, basis: CostBasis) -> Decimal | None:
        return self.average_net if basis is CostBasis.PROMEDIO_BSALE else self.last_comparable_net


# ---------------------------------------------------------------- resultado


@dataclass(frozen=True, slots=True)
class PackLine:
    ref: PackComponentRef
    status: C
    facts: ComponentFacts | None = None
    unit_net: Decimal | None = None
    unit_gross: Decimal | None = None
    gross_cost: GrossCost | None = None
    nested: "PackCost | None" = None

    @property
    def subtotal_net(self) -> Decimal | None:
        if self.unit_net is None or self.ref.quantity is None or self.status is not C.CALCULADO:
            return None
        return self.unit_net * self.ref.quantity

    @property
    def subtotal_gross(self) -> Decimal | None:
        if self.unit_gross is None or self.ref.quantity is None or self.status is not C.CALCULADO:
            return None
        return self.unit_gross * self.ref.quantity

    def to_json(self) -> dict[str, Any]:
        f = self.facts
        out: dict[str, Any] = {
            **self.ref.to_json(),
            "status": self.status.value,
            "code": f.code if f else None,
            "name": f.name if f else None,
            "active": f.active if f else None,
            "is_pack": f.is_pack if f else False,
            "reliability": f.reliability.value if f and not f.is_pack else (self.nested.reliability.value if self.nested and self.nested.reliability else None),
            "cost_change_pct": money(f.cost_change_pct) if f else None,
            "unit_net": money(self.unit_net),
            "unit_gross": money(self.unit_gross),
            "subtotal_net": money(self.subtotal_net),
            "subtotal_gross": money(self.subtotal_gross),
            "tax_ids": list(self.gross_cost.tax_ids) if self.gross_cost else [],
            # Base de márgenes (sin anticipo); el bruto pagadero no alimenta markup/pack.
            "taxes_applied": [
                {"tax_id": t.tax_id, "code": t.code, "percentage": money(t.percentage), "kind": t.role}
                for t in (self.gross_cost.margin_applied if self.gross_cost else ())
            ],
            "tax_factor": money(self.gross_cost.margin_factor) if self.gross_cost else None,
            "tax_status": f.tax_profile.status.value if f and not f.is_pack else None,
        }
        if self.nested is not None:
            out["nested_pack"] = self.nested.to_json(lines=True)
        return out


@dataclass(frozen=True, slots=True)
class PackCost:
    variant_id: int
    status: P
    basis: CostBasis
    lines: tuple[PackLine, ...] = ()
    composition_issues: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    reliability: CostReliability | None = None
    nested: bool = False
    depth_limited: bool = False
    _totals: tuple[Decimal | None, Decimal | None] = field(default=(None, None))

    @property
    def calculable(self) -> bool:
        return self.status is P.CALCULADO

    @property
    def net_total(self) -> Decimal | None:
        return self._totals[0]

    @property
    def gross_total(self) -> Decimal | None:
        return self._totals[1]

    @property
    def components_count(self) -> int:
        return len(self.lines)

    @property
    def missing_cost_variant_ids(self) -> list[int]:
        return [l.ref.variant_id for l in self.lines if l.status is C.SIN_COSTO and l.ref.variant_id is not None]

    @property
    def missing_variant_ids(self) -> list[int]:
        return [l.ref.variant_id for l in self.lines if l.status is C.INEXISTENTE and l.ref.variant_id is not None]

    def compact(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "calculable": self.calculable,
            "basis": self.basis.value,
            "components_count": self.components_count,
            "net_total": money(self.net_total),
            "gross_total": money(self.gross_total),
            "reliability": self.reliability.value if self.reliability else None,
            "nested": self.nested,
            "open": self.status is P.PACK_ABIERTO,
            "missing_cost_variant_ids": self.missing_cost_variant_ids,
            "missing_variant_ids": self.missing_variant_ids,
            "composition_issues": list(self.composition_issues),
            "warnings": list(self.warnings),
        }

    def to_json(self, *, lines: bool = True) -> dict[str, Any]:
        out = self.compact()
        out["source"] = COMPOSITION_SOURCE
        out["pack_own_cost_used"] = False
        out["pack_own_taxes_used"] = False
        if lines:
            out["lines"] = [l.to_json() for l in self.lines]
        return out


# ---------------------------------------------------------------- motor


def _worst(reliabilities: list[CostReliability]) -> CostReliability | None:
    if not reliabilities:
        return None
    return min(reliabilities, key=_RELIABILITY_ORDER.index)


class PackCostEngine:
    """Evalúa packs contra un índice de variantes de la misma empresa (nunca mezcla empresas)."""

    def __init__(
        self,
        components: Mapping[int, ComponentFacts],
        *,
        basis: CostBasis = CostBasis.PROMEDIO_BSALE,
        max_depth: int = PACK_MAX_DEPTH,
    ) -> None:
        self._facts = components
        self._basis = basis
        self._max_depth = max_depth
        self._memo: dict[int, PackCost] = {}

    def evaluate(self, variant_id: int) -> PackCost | None:
        facts = self._facts.get(variant_id)
        if facts is None or not facts.is_pack:
            return None
        return self._resolve(facts, (variant_id,))

    def _resolve(self, pack: ComponentFacts, path: tuple[int, ...]) -> PackCost:
        hit = self._memo.get(pack.variant_id)
        if hit is not None:
            return hit
        composition = pack.composition or PackComposition((), (ISSUE_ABSENT,))
        blocking = composition.status
        if blocking in (P.SIN_COMPOSICION, P.COMPOSICION_NO_INTERPRETABLE):
            result = PackCost(pack.variant_id, blocking, self._basis, composition_issues=composition.issues)
            self._memo[pack.variant_id] = result
            return result

        lines: list[PackLine] = []
        warnings: list[str] = []
        reliabilities: list[CostReliability] = []
        nested = False
        depth_limited = False
        for ref in composition.components:
            line, nested_here, limited = self._line(ref, path)
            nested = nested or nested_here
            depth_limited = depth_limited or limited
            lines.append(line)
            self._collect(line, warnings, reliabilities)

        statuses = {l.status for l in lines}
        status = P.CALCULADO
        for line_status, pack_status in _LINE_TO_PACK:
            if line_status in statuses:
                status = pack_status
                break
        totals: tuple[Decimal | None, Decimal | None] = (None, None)
        if status is P.CALCULADO:
            totals = (
                sum((l.subtotal_net for l in lines), Decimal(0)),  # type: ignore[misc]
                sum((l.subtotal_gross for l in lines), Decimal(0)),  # type: ignore[misc]
            )
        if nested:
            warnings.append(WARN_NESTED)
        if ISSUE_INACTIVE_DETAIL in composition.issues:
            warnings.append(ISSUE_INACTIVE_DETAIL)
        result = PackCost(
            pack.variant_id,
            status,
            self._basis,
            tuple(lines),
            composition.issues,
            tuple(dict.fromkeys(warnings)),
            _worst(reliabilities),
            nested,
            depth_limited,
            totals,
        )
        if not depth_limited:
            self._memo[pack.variant_id] = result
        return result

    def _line(self, ref: PackComponentRef, path: tuple[int, ...]) -> tuple[PackLine, bool, bool]:
        if ref.open:
            facts = self._facts.get(ref.variant_id) if ref.variant_id is not None else None
            return PackLine(ref, C.VARIANTE_NO_DETERMINADA, facts), False, False
        facts = self._facts.get(ref.variant_id)  # type: ignore[arg-type]
        if facts is None:
            return PackLine(ref, C.INEXISTENTE), False, False
        if facts.is_pack:
            if facts.variant_id in path:
                return PackLine(ref, C.CICLO, facts), True, False
            if len(path) >= self._max_depth:
                return PackLine(ref, C.ANIDAMIENTO_EXCESIVO, facts), True, True
            sub = self._resolve(facts, path + (facts.variant_id,))
            if sub.status is P.CICLO_DETECTADO:
                return PackLine(ref, C.CICLO, facts, nested=sub), True, sub.depth_limited
            if not sub.calculable:
                return PackLine(ref, C.PACK_NO_CALCULABLE, facts, nested=sub), True, sub.depth_limited
            return (
                PackLine(ref, C.CALCULADO, facts, sub.net_total, sub.gross_total, nested=sub),
                True,
                sub.depth_limited,
            )
        net = facts.net_for(self._basis)
        gross = commercial_gross_cost(net, facts.tax_profile)
        if net is None or net <= 0:
            return PackLine(ref, C.SIN_COSTO, facts, net, None, gross), False, False
        # Packs usan base de márgenes (sin anticipo) hasta aprobación metodológica.
        if gross.margin_gross is None:
            return PackLine(ref, C.REQUIERE_REVISION_TRIBUTARIA, facts, net, None, gross), False, False
        return PackLine(ref, C.CALCULADO, facts, net, gross.margin_gross, gross), False, False

    @staticmethod
    def _collect(line: PackLine, warnings: list[str], reliabilities: list[CostReliability]) -> None:
        f = line.facts
        if f is None:
            return
        if not f.active:
            warnings.append(WARN_INACTIVE_COMPONENT)
        if line.nested is not None:
            warnings.extend(w for w in line.nested.warnings if w != WARN_NESTED)
            if line.nested.reliability is not None:
                reliabilities.append(line.nested.reliability)
            return
        if f.is_pack:
            return
        reliabilities.append(f.reliability)
        if f.reliability is CostReliability.DESACTUALIZADO:
            warnings.append(WARN_STALE_COST)
        elif f.reliability is CostReliability.REQUIERE_REVISION:
            warnings.append(WARN_REVIEW_COST)
        if f.cost_change_pct is not None and f.cost_change_pct >= INCREASE_ALERT_PCT:
            warnings.append(WARN_COST_INCREASE)
