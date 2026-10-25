"""Motor puro de Márgenes V2: un SKU × una lista → estado, recargo y trazabilidad.

Precedencia del estado principal:
SIN_PRECIO → SIN_COSTO → SIN_REGLA → REQUIERE_REVISION_TRIBUTARIA (bruto no calculable por impuestos)
→ BAJO / DENTRO / SOBRE_POLITICA → (excepción verificada) PROMOCION_VIGENTE / REMATE_VIGENTE.
Packs Bsale: el costo de referencia es la suma de sus componentes (``pack_engine``); si la composición
no es completa el estado es PACK_NO_CALCULABLE (después de SIN_PRECIO) y no hay porcentajes.

Calificadores (no reemplazan el estado): COSTO_NO_VERIFICADO, POLITICA_PROVISIONAL y
PROMOCION_PENDIENTE_VERIFICACION. Un estado comparativo solo es "confirmado" sin calificadores.
Los riesgos (venta bajo costo, costo sospechoso, precio no verificado...) se informan siempre,
también cuando una excepción promocional verificada reemplaza la alerta de bajo margen.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

from backend.services.analytics_v2.commercial_scope import result_validity
from backend.services.analytics_v2.contracts import (
    NET_COST_VERIFIED,
    SYMBOLIC_PRICE_MAX,
    TAX_CONFIG_VERIFIED,
    CostBasis,
    CostReliability,
    MarginComplianceStatus as S,
    MarginRisk,
    PolicyReviewStatus,
    RuleIssue,
    StatusConfirmation,
    TaxProfileStatus,
    iso,
    money,
)
from backend.services.analytics_v2.margin_math import (
    ceiling_price,
    margin_on_sale_pct,
    markup_pct,
    round_pct,
    target_price,
)
from backend.services.analytics_v2.pack_engine import COMPOSITION_SOURCE, PackCost
from backend.services.analytics_v2.promotion_exceptions import ExceptionResolution
from backend.services.analytics_v2.tax_profile import TAX_DATA_SOURCE, TaxProfile, commercial_gross_cost

COMPARATIVE = frozenset({S.BAJO_POLITICA, S.DENTRO_POLITICA, S.SOBRE_POLITICA, S.PROMOCION_VIGENTE, S.REMATE_VIGENTE})
UNRELIABLE_COST = frozenset({CostReliability.REQUIERE_REVISION, CostReliability.DESACTUALIZADO})


@dataclass(frozen=True)
class PriceFact:
    gross: Decimal | None
    net: Decimal | None
    missing: bool = False
    fetched_at: datetime | None = None


@dataclass(frozen=True)
class RuleFact:
    rule_id: int
    price_list_id: int
    product_type_id: int | None
    min_markup: Decimal | None
    max_markup: Decimal | None
    active: bool
    rule_key: str | None = None
    price_list_name: str | None = None
    product_type_name: str | None = None
    review_status: PolicyReviewStatus = PolicyReviewStatus.PENDIENTE_REVISION
    issues: tuple[RuleIssue, ...] = field(default_factory=tuple)

    @property
    def usable(self) -> bool:
        return self.active and self.min_markup is not None

    @property
    def has_max(self) -> bool:
        return self.max_markup is not None and self.max_markup > 0


def rule_issues(min_markup: Decimal | None, max_markup: Decimal | None, active: bool | None) -> tuple[RuleIssue, ...]:
    issues: list[RuleIssue] = []
    if active is False:
        issues.append(RuleIssue.REGLA_INACTIVA)
    if min_markup is None:
        issues.append(RuleIssue.MINIMO_NULO)
    if max_markup is not None and max_markup == 0:
        issues.append(RuleIssue.MAXIMO_CERO)
    if min_markup is not None and max_markup is not None and max_markup > 0 and min_markup > max_markup:
        issues.append(RuleIssue.MINIMO_MAYOR_QUE_MAXIMO)
    return tuple(issues)


@dataclass(frozen=True)
class CostFacts:
    average_net: Decimal | None
    last_comparable_net: Decimal | None
    reliability: CostReliability
    tax_profile: TaxProfile
    anomaly_codes: tuple[str, ...] = ()
    # Solo packs Bsale: costo por componentes con la misma base de costo de la evaluación.
    pack: PackCost | None = None


def price_issue(price: PriceFact | None) -> str | None:
    if price is None:
        return "sin_precio_en_lista"
    if price.missing:
        return "precio_retirado_en_bsale"
    if price.gross is None:
        return "precio_bruto_nulo"
    if price.gross <= 0:
        return "precio_cero_o_negativo"
    if price.gross < SYMBOLIC_PRICE_MAX:
        return "precio_simbolico"
    return None


def evaluate(
    *,
    price: PriceFact | None,
    cost: CostFacts,
    rule: RuleFact | None,
    rule_match: str | None,
    cost_basis: CostBasis,
    promotion: ExceptionResolution,
    net_cost_verified: bool = NET_COST_VERIFIED,
    tax_config_verified: bool = TAX_CONFIG_VERIFIED,
) -> dict[str, Any]:
    reasons: list[str] = []
    issue = price_issue(price)
    gross_price = price.gross if price and issue is None else None

    pack = cost.pack
    if pack is not None and pack.basis is not cost_basis:
        raise ValueError("el costo del pack debe calcularse con la misma base de costo")
    if pack is not None:
        reference_net, gross_cost = pack.net_total, pack.gross_total
        reference = _pack_reference(pack, cost_basis)
    else:
        reference_net = cost.last_comparable_net if cost_basis is CostBasis.ULTIMO_COMPARABLE else cost.average_net
        gross = commercial_gross_cost(reference_net, cost.tax_profile)
        # Base de márgenes vigente: sin anticipo (margin_gross). El bruto pagadero va aparte.
        gross_cost = gross.margin_gross
        reference = _sku_reference(gross, cost, cost_basis, reference_net)
    net_verified = cost_basis is CostBasis.PROMEDIO_BSALE or net_cost_verified
    cost_verified = gross_cost is not None and tax_config_verified and net_verified

    markup = margin_sale = None
    if issue is not None:
        status = S.SIN_PRECIO
        reasons.append(issue)
    elif pack is not None and not pack.calculable:
        status = S.PACK_NO_CALCULABLE
        reasons.append(f"pack_{pack.status.value}")
    elif reference_net is None or reference_net <= 0:
        status = S.SIN_COSTO
        reasons.append(f"sin_costo_{cost_basis.value}")
    elif rule is None or not rule.usable:
        status = S.SIN_REGLA
        reasons.append("regla_inexistente" if rule is None else "regla_no_aplicable")
    elif gross_cost is None:
        status = S.REQUIERE_REVISION_TRIBUTARIA
        reasons.append(f"impuesto_{cost.tax_profile.status.value}")
    else:
        markup = markup_pct(gross_price, gross_cost)
        margin_sale = margin_on_sale_pct(gross_price, gross_cost)
        if markup < rule.min_markup:
            status = S.BAJO_POLITICA
        elif rule.has_max and markup > rule.max_markup:
            status = S.SOBRE_POLITICA
        else:
            status = S.DENTRO_POLITICA
        override = promotion.verified_status
        if override is not None and status is S.BAJO_POLITICA:
            status = override
        # Anticipo: visible en bruto pagadero; excluido de la base de márgenes vigente.
        if cost.tax_profile.status is TaxProfileStatus.IVA_ANTICIPADO:
            reasons.append("impuesto_iva_anticipado_excluido_de_base_margen")

    qualifiers: list[S] = []
    if status in COMPARATIVE or status is S.REQUIERE_REVISION_TRIBUTARIA:
        if (not cost_verified or cost.reliability in UNRELIABLE_COST) and status is not S.REQUIERE_REVISION_TRIBUTARIA:
            qualifiers.append(S.COSTO_NO_VERIFICADO)
        if rule is not None and rule.review_status is not PolicyReviewStatus.VALIDADA:
            qualifiers.append(S.POLITICA_PROVISIONAL)
    if status is S.BAJO_POLITICA and promotion.pending_verification:
        qualifiers.append(S.PROMOCION_PENDIENTE_VERIFICACION)

    risks: list[MarginRisk] = []
    if issue is not None:
        risks.append(MarginRisk.PRECIO_NO_VERIFICADO)
    if status in (S.SIN_COSTO, S.REQUIERE_REVISION_TRIBUTARIA, S.PACK_NO_CALCULABLE):
        risks.append(MarginRisk.SIN_COSTO_VALIDO)
    if markup is not None and markup < 0:
        risks.append(MarginRisk.VENTA_BAJO_COSTO)
    if cost.reliability is CostReliability.REQUIERE_REVISION or cost.anomaly_codes:
        risks.append(MarginRisk.COSTO_SOSPECHOSO)
    if cost.reliability is CostReliability.DESACTUALIZADO:
        risks.append(MarginRisk.COSTO_DESACTUALIZADO)
    if gross_cost is not None and not cost_verified:
        risks.append(MarginRisk.COSTO_NO_VERIFICADO)

    confirmed = status not in COMPARATIVE or not qualifiers
    validity, validity_reasons = result_validity(
        rule.review_status if rule else None,
        cost_verified and cost.reliability not in UNRELIABLE_COST,
    )

    usable_rule = rule if rule is not None and rule.usable else None
    target_min = target_price(gross_cost, usable_rule.min_markup) if usable_rule else None
    target_max = ceiling_price(gross_cost, usable_rule.max_markup) if usable_rule and usable_rule.has_max else None

    return {
        "status": status.value,
        "confirmation": (StatusConfirmation.CONFIRMADO if confirmed else StatusConfirmation.CALCULADO).value,
        "qualifiers": [q.value for q in qualifiers],
        "risks": [r.value for r in risks],
        "reasons": reasons,
        "validity": {"status": validity.value, "reasons": validity_reasons},
        "price": {
            "gross": money(price.gross) if price else None,
            "net": money(price.net) if price else None,
            "issue": issue,
            "fetched_at": iso(price.fetched_at) if price else None,
            "source": "bsale_raw.variant_prices.variant_value_with_taxes",
        },
        "costs": {
            "bsale_average_net": money(cost.average_net),
            "last_reception_net": money(cost.last_comparable_net),
            "reference": reference,
            "validated_gross": {"value": None, "status": "no_disponible"},
            "reliability": cost.reliability.value,
            "anomaly_codes": list(cost.anomaly_codes),
        },
        "markup_pct": money(round_pct(markup)),
        "margin_on_sale_pct": money(round_pct(margin_sale)),
        "gap_to_min_pct": money(round_pct(markup - rule.min_markup)) if markup is not None and rule else None,
        "rule": None
        if rule is None
        else {
            "rule_id": rule.rule_id,
            "rule_key": rule.rule_key,
            "price_list_id": rule.price_list_id,
            "product_type_id": rule.product_type_id,
            "match": rule_match,
            "min_markup_pct": money(rule.min_markup),
            "max_markup_pct": money(rule.max_markup),
            "active": rule.active,
            "review_status": rule.review_status.value,
            "issues": [i.value for i in rule.issues],
        },
        "target_price": {
            "min": money(target_min),
            "max": money(target_max),
            "provisional": validity.value != "validado",
        },
        "promotion": promotion.to_json(),
        "pack": pack.compact() if pack is not None else None,
        "_markup": markup,
    }


def _sku_reference(gross: Any, cost: CostFacts, basis: CostBasis, net: Decimal | None) -> dict[str, Any]:
    profile = cost.tax_profile
    return {
        "basis": basis.value,
        "net": money(net),
        # «gross» = base de márgenes (metodología vigente). Pagadero va en payable_*.
        "gross": money(gross.margin_gross),
        "payable_gross": money(gross.gross),
        "gross_status": gross.status.value,
        "gross_reason": gross.reason,
        "tax_status": profile.status.value,
        "tax_validation": profile.validation.value,
        "iva_rate_pct": money(gross.iva_rate_pct),
        "ila_rate_pct": money(gross.ila_rate_pct),
        "iva_anticipado_rate_pct": money(gross.iva_anticipado_rate_pct),
        "tax_rate_pct": money(profile.rate_for_margins),
        "payable_rate_pct": money(profile.total_rate_pct),
        "tax_factor": money(gross.margin_factor),
        "payable_factor": money(gross.factor),
        "tax_ids": list(gross.tax_ids),
        "taxes_applied": [
            {"tax_id": t.tax_id, "code": t.code, "percentage": money(t.percentage), "kind": t.role}
            for t in gross.margin_applied
        ],
        "tax_source": TAX_DATA_SOURCE,
    }


def _pack_reference(pack: PackCost, basis: CostBasis) -> dict[str, Any]:
    """Los impuestos van por componente (detalle del pack); los impuestos propios del pack no se aplican."""
    return {
        "basis": basis.value,
        "net": money(pack.net_total),
        "gross": money(pack.gross_total),
        "gross_status": "preliminar_no_verificado" if pack.calculable else "pack_no_calculable",
        "gross_reason": None if pack.calculable else pack.status.value,
        "tax_status": "por_componente",
        "tax_validation": "calculable" if pack.calculable else "pack_no_calculable",
        "iva_rate_pct": None,
        "ila_rate_pct": None,
        "tax_rate_pct": None,
        "tax_factor": None,
        "tax_ids": [],
        "taxes_applied": [],
        "tax_source": COMPOSITION_SOURCE,
    }
