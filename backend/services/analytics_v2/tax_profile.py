"""Motor tributario de Analítica V2: perfil por producto y costo bruto total desde BSALE RAW.

Fuentes: ``bsale_raw.product_taxes.tax_ids`` (ids Bsale de los impuestos asignados al producto) y
``bsale_raw.taxes`` de la misma empresa (código SII, tasa, estado). Cada impuesto se identifica por
su id y se clasifica por su código SII o por una asignación por id confirmada por contabilidad;
nunca por su nombre ni por la categoría o tipo del producto. La tasa usada siempre es la de RAW;
la tasa legal solo sirve para detectar configuraciones inconsistentes.

Costo bruto total pagadero = costo neto × (1 + Σ tasas que incrementan el total / 100).
Las tasas se aplican sobre la misma base neta (aditivo): nunca factor cascada (p. ej. 1,19 × 1,12).

Qué incrementa el total pagadero (cuando está configurado y activo en Bsale):
- IVA general (SII 14).
- Un ILA (bebidas/cervezas/vinos/licores).
- IVA anticipado (harina SII 19, carne SII 17/18) **junto con IVA**: ejemplo oficial SII harina
  neto + 19 % + 12 % = 131 % del neto. El anticipo NO es ILA.

Qué no se inventa: productos sin impuestos en Bsale no se asumen exentos; un anticipo solo
(sin IVA comercial) no define bruto de márgenes. El bruto calculado ≠ total confirmado en factura.
No modifica ``average_cost`` ni otros costos originales de Bsale.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Iterable

from backend.services.analytics_v2.contracts import (
    GrossCostStatus,
    TaxProfileStatus,
    TaxValidation,
    money,
)

TAX_DATA_SOURCE = "bsale_raw.product_taxes.tax_ids + bsale_raw.taxes"

ROLE_IVA = "iva"
ROLE_ILA = "ila"
ROLE_IVA_ANTICIPADO = "iva_anticipado"
ROLE_UNKNOWN = "desconocido"

USE_COMMERCIAL = "comparacion_comercial"
# Naturaleza contable del anticipo; cuando coexiste con IVA integra el bruto total pagadero.
USE_ANTICIPATED = "anticipado_contable"
USE_UNCLASSIFIED = "no_clasificado"
# Roles cuya tasa, validada, suma al factor bruto total (misma base neta).
PAYABLE_GROSS_ROLES = frozenset({ROLE_IVA, ROLE_ILA, ROLE_IVA_ANTICIPADO})

CLASSIFIED_BY_CODE = "codigo_sii"
CLASSIFIED_BY_CONFIRMED_ID = "id_confirmado"
UNCLASSIFIED = "sin_clasificar"
TAX_ROLES = frozenset({ROLE_IVA, ROLE_ILA, ROLE_IVA_ANTICIPADO})

_ROLE_USE = {
    ROLE_IVA: USE_COMMERCIAL,
    ROLE_ILA: USE_COMMERCIAL,
    ROLE_IVA_ANTICIPADO: USE_ANTICIPATED,
    ROLE_UNKNOWN: USE_UNCLASSIFIED,
}


@dataclass(frozen=True, slots=True)
class SiiTaxCode:
    role: str
    description: str
    legal_rate_pct: Decimal


# Códigos SII de impuestos (campo ``code`` del impuesto en Bsale). Cualquier otro código, incluidas
# las retenciones, queda sin clasificar y requiere revisión.
SII_TAX_CODES: dict[str, SiiTaxCode] = {
    "14": SiiTaxCode(ROLE_IVA, "IVA", Decimal("19")),
    "17": SiiTaxCode(ROLE_IVA_ANTICIPADO, "IVA anticipado faenamiento carne", Decimal("5")),
    "18": SiiTaxCode(ROLE_IVA_ANTICIPADO, "IVA anticipado carne", Decimal("5")),
    "19": SiiTaxCode(ROLE_IVA_ANTICIPADO, "IVA anticipado harina", Decimal("12")),
    "24": SiiTaxCode(ROLE_ILA, "ILA licores, piscos y destilados", Decimal("31.5")),
    "25": SiiTaxCode(ROLE_ILA, "ILA vinos", Decimal("20.5")),
    "26": SiiTaxCode(ROLE_ILA, "ILA cervezas y bebidas alcohólicas", Decimal("20.5")),
    "27": SiiTaxCode(ROLE_ILA, "ILA bebidas analcohólicas", Decimal("10")),
    "271": SiiTaxCode(ROLE_ILA, "ILA bebidas analcohólicas alto azúcar", Decimal("18")),
}

_LEGAL_RATES_BY_ROLE: dict[str, frozenset[Decimal]] = {}
for _c in SII_TAX_CODES.values():
    _LEGAL_RATES_BY_ROLE[_c.role] = _LEGAL_RATES_BY_ROLE.get(_c.role, frozenset()) | {_c.legal_rate_pct}

# Problemas de un impuesto que impiden usarlo en un costo bruto.
ENTRY_BLOCKING_ISSUES = frozenset(
    {
        "codigo_sii_no_reconocido",
        "codigo_sii_contradice_id_confirmado",
        "tasa_distinta_a_codigo_sii",
        "tasa_no_legal_para_tipo",
        "tasa_nula",
        "impuesto_inactivo",
        "impuesto_retirado_de_bsale",
    }
)

ONE = Decimal("1")
HUNDRED = Decimal("100")
ZERO = Decimal("0")


@dataclass(frozen=True, slots=True)
class TaxEntry:
    tax_id: int
    name: str
    percentage: Decimal | None
    role: str
    code: str | None = None
    state: int | None = None
    classified_by: str = UNCLASSIFIED
    issues: tuple[str, ...] = ()

    @property
    def use(self) -> str:
        return _ROLE_USE[self.role]

    @property
    def kind(self) -> str:
        return self.role

    @property
    def blocking(self) -> bool:
        return any(i in ENTRY_BLOCKING_ISSUES for i in self.issues)

    def to_json(self) -> dict[str, Any]:
        return {
            "tax_id": self.tax_id,
            "name": self.name,
            "code": self.code,
            "percentage": money(self.percentage),
            "kind": self.role,
            "use": self.use,
            "classified_by": self.classified_by,
            "issues": list(self.issues),
        }


@dataclass(frozen=True, slots=True)
class TaxProfile:
    status: TaxProfileStatus
    reason: str
    taxes: tuple[TaxEntry, ...] = field(default_factory=tuple)
    # Bruto total pagadero: IVA + ILA + anticipo (cuando aplica).
    total_rate_pct: Decimal | None = None
    iva_rate_pct: Decimal | None = None
    ila_rate_pct: Decimal | None = None
    issues: tuple[str, ...] = ()
    # Anticipo (harina/carne): integra total_rate_pct; NO entra a margin_rate_pct.
    iva_anticipado_rate_pct: Decimal | None = None
    # Base de márgenes vigente: solo IVA + ILA (metodología comercial previa, hasta nueva aprobación).
    margin_rate_pct: Decimal | None = None

    @property
    def validation(self) -> TaxValidation:
        if self.status in (TaxProfileStatus.NO_CONSULTADO, TaxProfileStatus.SIN_IMPUESTOS):
            return TaxValidation.SIN_CONFIGURACION
        if self.total_rate_pct is None:
            return TaxValidation.REQUIERE_REVISION_TRIBUTARIA
        # Con IVA + anticipo: bruto pagadero y base de margen (sin anticipo) calculables.
        return TaxValidation.CALCULABLE

    @property
    def rate_for_margins(self) -> Decimal | None:
        if self.margin_rate_pct is not None:
            return self.margin_rate_pct
        return self.total_rate_pct

    @property
    def identified(self) -> bool:
        """Todos los impuestos del producto tienen tipo reconocido (aunque requieran revisión)."""
        if self.status is TaxProfileStatus.EXENTO_VERIFICADO:
            return True
        return bool(self.taxes) and all(t.role != ROLE_UNKNOWN for t in self.taxes) and (
            self.status is not TaxProfileStatus.IMPUESTO_NO_RECONOCIDO
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "validation": self.validation.value,
            "reason": self.reason,
            "iva_rate_pct": money(self.iva_rate_pct),
            "ila_rate_pct": money(self.ila_rate_pct),
            "iva_anticipado_rate_pct": money(self.iva_anticipado_rate_pct),
            "total_rate_pct": money(self.total_rate_pct),
            "margin_rate_pct": money(self.rate_for_margins),
            "issues": list(self.issues),
            "taxes": [t.to_json() for t in self.taxes],
        }


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def classify_tax(row: dict[str, Any], confirmed_role: str | None = None) -> TaxEntry:
    """Clasifica un impuesto de ``bsale_raw.taxes`` por su código SII o por el rol confirmado para su id.

    El nombre solo se conserva para mostrarlo: nunca decide el tipo ni la tasa.
    """
    tax_id = int(row["tax_id"])
    name = str(row.get("name") or f"Impuesto {tax_id}")
    pct = _decimal(row.get("percentage"))
    code = str(row.get("code") or "").strip() or None
    state = row.get("state")
    issues: list[str] = []
    if confirmed_role is not None and confirmed_role not in TAX_ROLES:
        raise ValueError(f"rol tributario confirmado inválido para impuesto {tax_id}: {confirmed_role}")
    sii = SII_TAX_CODES.get(code) if code is not None else None
    if code is not None and sii is None:
        role, classified_by = ROLE_UNKNOWN, UNCLASSIFIED
        issues.append("codigo_sii_no_reconocido")
    elif sii is not None:
        role, classified_by = sii.role, CLASSIFIED_BY_CODE
        if pct is not None and pct != sii.legal_rate_pct:
            issues.append("tasa_distinta_a_codigo_sii")
        if confirmed_role is not None and confirmed_role != sii.role:
            issues.append("codigo_sii_contradice_id_confirmado")
    elif confirmed_role is not None:
        role, classified_by = confirmed_role, CLASSIFIED_BY_CONFIRMED_ID
        issues.append("codigo_sii_ausente")
        if pct is not None and pct not in _LEGAL_RATES_BY_ROLE[role]:
            issues.append("tasa_no_legal_para_tipo")
    else:
        role, classified_by = ROLE_UNKNOWN, UNCLASSIFIED
        issues.append("codigo_sii_ausente")
    if pct is None:
        issues.append("tasa_nula")
    if state not in (None, 0):
        issues.append("impuesto_inactivo")
    if row.get("missing_since") is not None:
        issues.append("impuesto_retirado_de_bsale")
    return TaxEntry(
        tax_id=tax_id,
        name=name,
        percentage=pct,
        role=role,
        code=code,
        state=int(state) if state is not None else None,
        classified_by=classified_by,
        issues=tuple(issues),
    )


def build_tax_catalog(
    rows: Iterable[dict[str, Any]], confirmed_roles: dict[int, str] | None = None
) -> dict[int, TaxEntry]:
    """Catálogo de una sola empresa indexado por id Bsale del impuesto."""
    roles = confirmed_roles or {}
    catalog: dict[int, TaxEntry] = {}
    for row in rows:
        entry = classify_tax(row, roles.get(int(row["tax_id"])))
        catalog[entry.tax_id] = entry
    return catalog


def resolve_tax_profile(
    tax_ids: list[int] | None,
    catalog: dict[int, TaxEntry],
    *,
    verified_exempt: bool = False,
    config_stale: bool = False,
) -> TaxProfile:
    S = TaxProfileStatus
    if tax_ids is None:
        return TaxProfile(S.NO_CONSULTADO, "Impuestos del producto aún no sincronizados desde Bsale")
    profile_issues: list[str] = []
    unique_ids = list(dict.fromkeys(int(t) for t in tax_ids))
    if len(unique_ids) != len(tax_ids):
        profile_issues.append("impuesto_repetido_en_bsale")
    if config_stale:
        return TaxProfile(
            S.CONFIGURACION_INCONSISTENTE,
            "La configuración tributaria del producto ya no aparece en Bsale",
            issues=(*profile_issues, "configuracion_tributaria_retirada"),
        )
    if not unique_ids:
        if verified_exempt:
            return TaxProfile(
                S.EXENTO_VERIFICADO, "Exento confirmado por contabilidad", (), ZERO, ZERO, ZERO, tuple(profile_issues)
            )
        return TaxProfile(
            S.SIN_IMPUESTOS,
            "Bsale no informa impuestos: no se asume exento",
            issues=(*profile_issues, "sin_impuestos_configurados"),
        )

    missing = [t for t in unique_ids if t not in catalog]
    taxes = tuple(catalog[t] for t in unique_ids if t in catalog)
    if missing:
        return TaxProfile(
            S.IMPUESTO_NO_RECONOCIDO,
            f"Impuesto {', '.join(map(str, missing))} no existe en el catálogo RAW",
            taxes,
            issues=(*profile_issues, "impuesto_fuera_de_catalogo"),
        )
    if verified_exempt:
        return TaxProfile(
            S.CONFIGURACION_INCONSISTENTE,
            "Marcado exento por contabilidad pero Bsale informa impuestos",
            taxes,
            issues=(*profile_issues, "exento_con_impuestos"),
        )
    if any(t.role == ROLE_UNKNOWN for t in taxes):
        return TaxProfile(
            S.IMPUESTO_NO_RECONOCIDO,
            "Impuesto sin código SII reconocido ni clasificación confirmada por id",
            taxes,
            issues=(*profile_issues, "impuesto_sin_clasificar"),
        )
    blocking = sorted({i for t in taxes for i in t.issues if i in ENTRY_BLOCKING_ISSUES})
    if blocking:
        return TaxProfile(
            S.CONFIGURACION_INCONSISTENTE,
            "Impuesto con tasa, estado o vigencia inconsistentes en Bsale",
            taxes,
            issues=(*profile_issues, *blocking),
        )
    roles = [t.role for t in taxes]
    if ROLE_IVA_ANTICIPADO in roles:
        anticipados = [t for t in taxes if t.role == ROLE_IVA_ANTICIPADO]
        iva_a = [t for t in taxes if t.role == ROLE_IVA]
        ila_a = [t for t in taxes if t.role == ROLE_ILA]
        anticipado_rate = sum((t.percentage or ZERO) for t in anticipados)
        # Solo anticipo (sin IVA comercial): no hay factor de comparación de márgenes.
        if not iva_a:
            return TaxProfile(
                S.IVA_ANTICIPADO,
                "Solo IVA anticipado: recuperable contablemente; sin IVA comercial no hay costo bruto de márgenes",
                taxes,
                issues=(*profile_issues, "iva_anticipado", "anticipo_sin_iva_comercial"),
                iva_anticipado_rate_pct=anticipado_rate,
            )
        if len(iva_a) != 1 or len(ila_a) >= 2:
            code = "iva_duplicado" if len(iva_a) > 1 else "ila_multiple"
            return TaxProfile(
                S.CONFIGURACION_INCONSISTENTE,
                "IVA anticipado con configuración de IVA/ILA incompatible",
                taxes,
                issues=(*profile_issues, "iva_anticipado", code),
                iva_anticipado_rate_pct=anticipado_rate,
            )
        # IVA (+ ILA opcional) + anticipo:
        # - Bruto pagadero = neto×(1+(IVA+ILA+anticipo)/100) — SII harina 131 %.
        # - Base de márgenes = neto×(1+(IVA+ILA)/100) — metodología vigente hasta nueva aprobación.
        iva_rate = iva_a[0].percentage
        ila_rate = ila_a[0].percentage if ila_a else ZERO
        assert iva_rate is not None and ila_rate is not None
        margin_rate = iva_rate + ila_rate
        payable_rate = margin_rate + anticipado_rate
        return TaxProfile(
            S.IVA_ANTICIPADO,
            "Bruto pagadero incluye IVA + anticipo (≠ ILA). "
            "La base de márgenes conserva solo IVA/ILA hasta aprobación metodológica.",
            taxes,
            payable_rate,
            iva_rate,
            ila_rate,
            (*profile_issues, "iva_anticipado"),
            iva_anticipado_rate_pct=anticipado_rate,
            margin_rate_pct=margin_rate,
        )
    ila = [t for t in taxes if t.role == ROLE_ILA]
    iva = [t for t in taxes if t.role == ROLE_IVA]
    if len(ila) >= 2:
        return TaxProfile(
            S.ILA_COMBINADO,
            "Más de un impuesto adicional (ILA): configuración incompatible",
            taxes,
            issues=(*profile_issues, "ila_multiple"),
        )
    if len(iva) != 1:
        code = "iva_duplicado" if iva else "ila_sin_iva"
        reason = "Más de un IVA en el producto" if iva else "Impuesto adicional sin IVA"
        return TaxProfile(S.CONFIGURACION_INCONSISTENTE, reason, taxes, issues=(*profile_issues, code))

    if any(t.classified_by == CLASSIFIED_BY_CONFIRMED_ID for t in taxes):
        profile_issues.append("clasificado_por_id_confirmado")
    iva_rate = iva[0].percentage
    ila_rate = ila[0].percentage if ila else ZERO
    assert iva_rate is not None and ila_rate is not None
    total = iva_rate + ila_rate
    if ila:
        return TaxProfile(
            S.IVA_ILA, "IVA + un ILA", taxes, total, iva_rate, ila_rate, tuple(profile_issues), margin_rate_pct=total
        )
    return TaxProfile(S.IVA, "IVA", taxes, total, iva_rate, ZERO, tuple(profile_issues), margin_rate_pct=total)


@dataclass(frozen=True, slots=True)
class GrossCost:
    """Bruto pagadero (gross) y base de márgenes (margin_gross). Neto Bsale intacto."""

    net: Decimal | None
    gross: Decimal | None  # bruto total pagadero estimado
    status: GrossCostStatus
    reason: str | None
    iva_rate_pct: Decimal | None = None
    ila_rate_pct: Decimal | None = None
    iva_anticipado_rate_pct: Decimal | None = None
    factor: Decimal | None = None  # factor bruto pagadero
    # Base márgenes = neto×(1+margin_rate); sin anticipo. Alias histórico net_plus_iva_gross.
    margin_gross: Decimal | None = None
    margin_factor: Decimal | None = None
    net_plus_iva_gross: Decimal | None = None
    tax_ids: tuple[int, ...] = ()
    applied: tuple[TaxEntry, ...] = ()  # impuestos del bruto pagadero
    margin_applied: tuple[TaxEntry, ...] = ()  # IVA/ILA de la base de márgenes

    def to_json(self) -> dict[str, Any]:
        return {
            "net": money(self.net),
            "iva_rate_pct": money(self.iva_rate_pct),
            "ila_rate_pct": money(self.ila_rate_pct),
            "iva_anticipado_rate_pct": money(self.iva_anticipado_rate_pct),
            "factor": money(self.factor),
            "gross": money(self.gross),
            "margin_factor": money(self.margin_factor),
            "margin_gross": money(self.margin_gross),
            "net_plus_iva_gross": money(self.net_plus_iva_gross if self.net_plus_iva_gross is not None else self.margin_gross),
            "status": self.status.value,
            "reason": self.reason,
            "tax_ids": list(self.tax_ids),
            "taxes_applied": [
                {"tax_id": t.tax_id, "code": t.code, "percentage": money(t.percentage), "kind": t.role}
                for t in self.applied
            ],
            "margin_taxes_applied": [
                {"tax_id": t.tax_id, "code": t.code, "percentage": money(t.percentage), "kind": t.role}
                for t in self.margin_applied
            ],
            "confirmed_by_document": False,
            "source": TAX_DATA_SOURCE,
        }


def commercial_gross_cost(net_cost: Decimal | None, profile: TaxProfile) -> GrossCost:
    """Calcula bruto pagadero y base de márgenes (separados cuando hay anticipo)."""
    ctx = {
        "iva_rate_pct": profile.iva_rate_pct,
        "ila_rate_pct": profile.ila_rate_pct,
        "iva_anticipado_rate_pct": profile.iva_anticipado_rate_pct,
        "tax_ids": tuple(t.tax_id for t in profile.taxes),
    }
    if net_cost is None:
        return GrossCost(None, None, GrossCostStatus.SIN_COSTO_BASE, "costo_nulo", **ctx)
    if net_cost <= 0:
        reason = "costo_cero" if net_cost == 0 else "costo_negativo"
        return GrossCost(net_cost, None, GrossCostStatus.SIN_COSTO_BASE, reason, **ctx)
    validation = profile.validation
    if validation is TaxValidation.SIN_CONFIGURACION:
        return GrossCost(net_cost, None, GrossCostStatus.SIN_CONFIGURACION_TRIBUTARIA, profile.status.value, **ctx)
    if validation is TaxValidation.REQUIERE_REVISION_TRIBUTARIA or profile.total_rate_pct is None:
        return GrossCost(net_cost, None, GrossCostStatus.REQUIERE_REVISION_TRIBUTARIA, profile.status.value, **ctx)

    margin_rate = profile.rate_for_margins
    assert margin_rate is not None
    payable_factor = ONE + profile.total_rate_pct / HUNDRED
    margin_factor = ONE + margin_rate / HUNDRED
    payable_gross = net_cost * payable_factor
    margin_gross = net_cost * margin_factor

    applied = tuple(
        t
        for t in profile.taxes
        if t.role in PAYABLE_GROSS_ROLES
        and not t.blocking
        and t.percentage is not None
        and (t.role != ROLE_IVA_ANTICIPADO or profile.iva_anticipado_rate_pct is not None)
    )
    margin_applied = tuple(t for t in applied if t.use == USE_COMMERCIAL)

    reason = None
    if profile.status is TaxProfileStatus.IVA_ANTICIPADO and profile.iva_anticipado_rate_pct:
        reason = "bruto_pagadero_incluye_anticipo_base_margen_sin_anticipo"

    return GrossCost(
        net_cost,
        payable_gross,
        GrossCostStatus.PRELIMINAR_NO_VERIFICADO,
        reason,
        factor=payable_factor,
        margin_gross=margin_gross,
        margin_factor=margin_factor,
        net_plus_iva_gross=margin_gross,
        applied=applied,
        margin_applied=margin_applied,
        **ctx,
    )


_ROLE_LABEL = {
    ROLE_IVA: "IVA",
    ROLE_ILA: "ILA (impuesto adicional)",
    ROLE_IVA_ANTICIPADO: "IVA anticipado (no es ILA)",
    ROLE_UNKNOWN: "Impuesto sin clasificar",
}


def tax_visual_breakdown(net_cost: Decimal | None, profile: TaxProfile) -> dict[str, Any]:
    """Desglose: neto, impuestos, bruto pagadero y base de márgenes (separados si hay anticipo)."""
    gross = commercial_gross_cost(net_cost, profile)
    lines: list[dict[str, Any]] = []
    payable_ids = {t.tax_id for t in gross.applied}
    margin_ids = {t.tax_id for t in gross.margin_applied}
    for t in profile.taxes:
        amount = None
        if net_cost is not None and net_cost > 0 and t.percentage is not None:
            amount = net_cost * t.percentage / HUNDRED
        label = t.name
        if t.percentage is not None:
            label = f"{_ROLE_LABEL.get(t.role, t.role)} {money(t.percentage)}%"
            if t.name:
                label = f"{label} · {t.name}"
        lines.append(
            {
                "tax_id": t.tax_id,
                "name": t.name,
                "label": label,
                "code": t.code,
                "role": t.role,
                "percentage": money(t.percentage),
                "amount_estimated": money(amount),
                "increases_payable_total": t.tax_id in payable_ids,
                "in_margin_base": t.tax_id in margin_ids,
                "in_commercial_factor": t.tax_id in margin_ids,  # alias histórico = base márgenes
                "use": t.use,
                "source": "configuracion_bsale",
                "confirmed_by_document": False,
            }
        )
    return {
        "net": money(net_cost),
        "lines": lines,
        "net_plus_iva_gross": money(gross.margin_gross),
        "margin_gross": money(gross.margin_gross),
        "margin_factor": money(gross.margin_factor),
        "commercial_gross": money(gross.gross),
        "payable_gross_total": money(gross.gross),
        "commercial_factor": money(gross.factor),
        "payable_factor": money(gross.factor),
        "documentary_total_referential": money(gross.gross),
        "documentary_total_confirmed": None,
        "disclaimers": [
            "Importes por impuesto: estimación sobre el costo neto Bsale según tasas configuradas.",
            "Costo bruto total pagadero = neto + gravámenes del total (incl. anticipo harina cuando aplica).",
            "Base de márgenes = neto + IVA/ILA; el anticipo no entra hasta nueva aprobación metodológica.",
            "El IVA anticipado no es ILA.",
            "El bruto total calculado no es el total confirmado de la factura.",
            "No modifica el costo promedio neto original de Bsale.",
        ],
    }
