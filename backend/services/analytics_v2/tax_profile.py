"""Motor tributario de Analítica V2: perfil por producto y costo bruto comercial desde BSALE RAW.

Fuentes: ``bsale_raw.product_taxes.tax_ids`` (ids Bsale de los impuestos asignados al producto) y
``bsale_raw.taxes`` de la misma empresa (código SII, tasa, estado). Cada impuesto se identifica por
su id y se clasifica por su código SII o por una asignación por id confirmada por contabilidad;
nunca por su nombre ni por la categoría o tipo del producto. La tasa usada siempre es la de RAW;
la tasa legal solo sirve para detectar configuraciones inconsistentes.

Costo bruto comercial = costo neto × (1 + (IVA + ILA) / 100). IVA e ILA se aplican sobre la
misma base neta (régimen de impuestos adicionales a bebidas): nunca 1,19 × 1,205.

Solo IVA general e ILA entran a la comparación comercial. El IVA anticipado (harina, carne) es un
anticipo recuperable contablemente: el producto queda en ``requiere_revision_tributaria`` hasta que
contabilidad defina su tratamiento. Un producto sin impuestos en Bsale no se asume exento.
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
USE_ANTICIPATED = "anticipado_contable"
USE_UNCLASSIFIED = "no_clasificado"

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
    total_rate_pct: Decimal | None = None
    iva_rate_pct: Decimal | None = None
    ila_rate_pct: Decimal | None = None
    issues: tuple[str, ...] = ()
    # Anticipo (harina/carne): informativo; NUNCA entra a total_rate_pct / factor comercial.
    iva_anticipado_rate_pct: Decimal | None = None

    @property
    def validation(self) -> TaxValidation:
        if self.status in (TaxProfileStatus.NO_CONSULTADO, TaxProfileStatus.SIN_IMPUESTOS):
            return TaxValidation.SIN_CONFIGURACION
        if self.total_rate_pct is None:
            return TaxValidation.REQUIERE_REVISION_TRIBUTARIA
        # Con IVA comercial calculable + anticipo: el bruto comercial es usable,
        # pero el estado sigue siendo iva_anticipado (revisión contable del anticipo).
        return TaxValidation.CALCULABLE

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
        # IVA (+ ILA opcional) + anticipo: factor comercial = solo IVA/ILA.
        # El anticipo se expone aparte y NUNCA se suma al costo comercial ni al inventario.
        iva_rate = iva_a[0].percentage
        ila_rate = ila_a[0].percentage if ila_a else ZERO
        assert iva_rate is not None and ila_rate is not None
        total = iva_rate + ila_rate
        return TaxProfile(
            S.IVA_ANTICIPADO,
            "IVA anticipado excluido del costo comercial; el factor usa solo IVA/ILA. "
            "Total documental (neto+IVA+anticipo) requiere respaldo de factura.",
            taxes,
            total,
            iva_rate,
            ila_rate,
            (*profile_issues, "iva_anticipado"),
            iva_anticipado_rate_pct=anticipado_rate,
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
        return TaxProfile(S.IVA_ILA, "IVA + un ILA", taxes, total, iva_rate, ila_rate, tuple(profile_issues))
    return TaxProfile(S.IVA, "IVA", taxes, total, iva_rate, ZERO, tuple(profile_issues))


@dataclass(frozen=True, slots=True)
class GrossCost:
    """Costo bruto comercial calculado sin redondeos intermedios; el neto original se conserva."""

    net: Decimal | None
    gross: Decimal | None
    status: GrossCostStatus
    reason: str | None
    iva_rate_pct: Decimal | None = None
    ila_rate_pct: Decimal | None = None
    factor: Decimal | None = None
    tax_ids: tuple[int, ...] = ()
    applied: tuple[TaxEntry, ...] = ()

    def to_json(self) -> dict[str, Any]:
        return {
            "net": money(self.net),
            "iva_rate_pct": money(self.iva_rate_pct),
            "ila_rate_pct": money(self.ila_rate_pct),
            "factor": money(self.factor),
            "gross": money(self.gross),
            "status": self.status.value,
            "reason": self.reason,
            "tax_ids": list(self.tax_ids),
            "taxes_applied": [
                {"tax_id": t.tax_id, "code": t.code, "percentage": money(t.percentage), "kind": t.role}
                for t in self.applied
            ],
            "source": TAX_DATA_SOURCE,
        }


def commercial_gross_cost(net_cost: Decimal | None, profile: TaxProfile) -> GrossCost:
    """Solo suma los impuestos del producto que son IVA o un único ILA; si no, no hay bruto."""
    ctx = {
        "iva_rate_pct": profile.iva_rate_pct,
        "ila_rate_pct": profile.ila_rate_pct,
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
    applied = tuple(t for t in profile.taxes if t.use == USE_COMMERCIAL)
    factor = ONE + profile.total_rate_pct / HUNDRED
    reason = None
    if profile.status is TaxProfileStatus.IVA_ANTICIPADO:
        reason = "anticipo_excluido_del_factor_comercial"
    return GrossCost(
        net_cost,
        net_cost * factor,
        GrossCostStatus.PRELIMINAR_NO_VERIFICADO,
        reason,
        factor=factor,
        applied=applied,
        **ctx,
    )
