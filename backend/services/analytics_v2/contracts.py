"""Contratos compartidos de Analítica V2 (costos, política, márgenes, promociones, dashboard).

Todos los módulos V2 usan estas constantes y estados: una sola definición por concepto.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from enum import StrEnum
from typing import Any

CONTRACT_VERSION = "analytics_v2.0"
DATA_SOURCE = "bsale_raw"

# Empresas cuya fuente RAW está completa (catálogo, impuestos, precios, costos y recepciones).
# Las demás responden 409 hasta completar su carga: nunca se mezclan ni se rellenan con legacy.
RAW_READY_COMPANY_IDS: frozenset[int] = frozenset({3})

# Alcance histórico de recepciones cargadas en RAW.
HISTORY_START = date(2025, 1, 1)

# El carácter neto de stock_reception_details.cost aún no se contrasta contra facturas.
NET_COST_VERIFIED = False

# Las reglas actuales de bsale.margin_rules se conservan, pero no son la política definitiva:
# serán reemplazadas por una importación controlada desde Excel. Hasta entonces todo resultado
# de margen que dependa de ellas es provisional.
POLICIES_REVIEWED = False

DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200
HISTORY_SERIES_MAX_POINTS = 1000

# Umbrales de anomalías (comparación contra el costo comparable anterior del mismo SKU).
INCREASE_ALERT_PCT = Decimal("30")
INCREASE_SEVERE_PCT = Decimal("100")
DECREASE_ALERT_PCT = Decimal("-30")
DECREASE_SEVERE_PCT = Decimal("-60")
EXTREME_QUANTITY_FACTOR = Decimal("10")
AVERAGE_DIVERGENCE_PCT = Decimal("50")
STALE_COST_DAYS = 180


class MovementClass(StrEnum):
    FACTURA = "factura"
    GUIA_DESPACHO = "guia_despacho"
    NOTA_CREDITO = "nota_credito"
    TRANSFERENCIA_INTERNA = "transferencia_interna"
    AJUSTE = "ajuste"
    SIN_DOCUMENTO = "sin_documento"
    NO_DETERMINADO = "no_determinado"


# Movimientos que pueden representar una compra real a proveedor.
COMPARABLE_MOVEMENTS: frozenset[MovementClass] = frozenset(
    {MovementClass.FACTURA, MovementClass.GUIA_DESPACHO}
)


class CostReliability(StrEnum):
    CONSISTENTE = "consistente"
    REQUIERE_REVISION = "requiere_revision"
    DESACTUALIZADO = "desactualizado"
    SOLO_PROMEDIO_BSALE = "solo_promedio_bsale"
    SIN_COSTO = "sin_costo"


class AnomalyCode(StrEnum):
    ALZA_ANORMAL = "alza_anormal"
    BAJA_SOSPECHOSA = "baja_sospechosa"
    COSTO_CERO = "costo_cero"
    COSTO_NULO = "costo_nulo"
    CANTIDAD_EXTREMA = "cantidad_extrema"
    CANTIDAD_NO_POSITIVA = "cantidad_no_positiva"
    RECEPCION_MODIFICADA = "recepcion_modificada"
    CAMBIO_VARIANTE = "cambio_variante"
    DETALLE_INCOMPLETO = "detalle_incompleto"
    MOVIMIENTO_NO_DETERMINADO = "movimiento_no_determinado"
    DIFERENCIA_PROMEDIO_BSALE = "diferencia_promedio_bsale"


class Severity(StrEnum):
    ALTA = "alta"
    MEDIA = "media"
    BAJA = "baja"


class TaxProfileStatus(StrEnum):
    NO_CONSULTADO = "no_consultado"
    # Bsale no informa impuestos para el producto. No equivale a exento.
    SIN_IMPUESTOS = "sin_impuestos"
    EXENTO_VERIFICADO = "exento_verificado"
    IVA = "iva"
    IVA_ILA = "iva_ila"
    IVA_ANTICIPADO = "iva_anticipado"
    ILA_COMBINADO = "ila_combinado"
    IMPUESTO_NO_RECONOCIDO = "impuesto_no_reconocido"
    CONFIGURACION_INCONSISTENTE = "configuracion_inconsistente"


class TaxValidation(StrEnum):
    CALCULABLE = "calculable"
    REQUIERE_REVISION_TRIBUTARIA = "requiere_revision_tributaria"
    SIN_CONFIGURACION = "sin_configuracion"


class GrossCostStatus(StrEnum):
    """Estado del costo bruto comercial: provisional mientras la configuración tributaria no se contraste con documentos."""

    PRELIMINAR_NO_VERIFICADO = "preliminar_no_verificado"
    REQUIERE_REVISION_TRIBUTARIA = "requiere_revision_tributaria"
    SIN_CONFIGURACION_TRIBUTARIA = "sin_configuracion_tributaria"
    SIN_COSTO_BASE = "sin_costo_base"


# La configuración tributaria de Bsale aún no se contrasta con facturas reales: todo bruto calculado
# es provisional aunque la configuración sea consistente.
TAX_CONFIG_VERIFIED = False

# Productos sin impuestos confirmados como exentos por contabilidad, por empresa (product_id de Bsale).
# Vacío hasta contar con esa confirmación: un producto sin impuestos en Bsale NO se asume exento.
VERIFIED_EXEMPT_PRODUCT_IDS: dict[int, frozenset[int]] = {}

# Tipo tributario ("iva", "ila", "iva_anticipado") confirmado por contabilidad para un id de impuesto
# Bsale sin código SII, por empresa ({company_id: {tax_id: tipo}}). Vacío: sin código SII ni
# confirmación, el impuesto queda sin clasificar y el producto requiere revisión tributaria.
CONFIRMED_TAX_ROLES: dict[int, dict[int, str]] = {}


class PolicyReviewStatus(StrEnum):
    PENDIENTE_REVISION = "pendiente_revision"
    VALIDADA = "validada"


class ResultValidity(StrEnum):
    PROVISIONAL = "provisional"
    VALIDADO = "validado"


class MarginComplianceStatus(StrEnum):
    """Un estado principal por SKU × lista; los calificadores usan los mismos valores."""

    BAJO_POLITICA = "bajo_politica"
    DENTRO_POLITICA = "dentro_politica"
    SOBRE_POLITICA = "sobre_politica"
    SIN_COSTO = "sin_costo"
    SIN_PRECIO = "sin_precio"
    SIN_REGLA = "sin_regla"
    COSTO_NO_VERIFICADO = "costo_no_verificado"
    REQUIERE_REVISION_TRIBUTARIA = "requiere_revision_tributaria"
    POLITICA_PROVISIONAL = "politica_provisional"
    PROMOCION_VIGENTE = "promocion_vigente"
    REMATE_VIGENTE = "remate_vigente"
    PROMOCION_PENDIENTE_VERIFICACION = "promocion_pendiente_verificacion"


class StatusConfirmation(StrEnum):
    CALCULADO = "calculado"
    CONFIRMADO = "confirmado"


class CostBasis(StrEnum):
    """Base de costo elegida explícitamente; nunca se sustituye una por otra en silencio."""

    ULTIMO_COMPARABLE = "ultimo_comparable"
    PROMEDIO_BSALE = "promedio_bsale"


DEFAULT_COST_BASIS = CostBasis.PROMEDIO_BSALE


class RuleIssue(StrEnum):
    MAXIMO_CERO = "maximo_cero_requiere_revision"
    MINIMO_MAYOR_QUE_MAXIMO = "minimo_mayor_que_maximo"
    REGLA_INACTIVA = "regla_inactiva"
    MINIMO_NULO = "minimo_nulo"


class PriceListScopeStatus(StrEnum):
    EVALUADA = "evaluada"
    AUTORIZADA_NO_VIGENTE = "autorizada_no_vigente"
    RETIRADA = "retirada"
    RETIRADA_REACTIVADA = "retirada_reactivada"
    VIGENTE_NO_AUTORIZADA = "vigente_no_autorizada"
    NO_VIGENTE = "no_vigente"


class PromotionCheck(StrEnum):
    FUENTE_NO_INTEGRADA = "fuente_no_integrada"
    SIN_EXCEPCION = "sin_excepcion"
    EXCEPCION_VERIFICADA = "excepcion_verificada"
    EXCEPCION_NO_VERIFICADA = "excepcion_no_verificada"
    EXCEPCION_PENDIENTE_APROBACION = "excepcion_pendiente_aprobacion"
    CONFLICTO_PROMOCIONES = "conflicto_promociones"
    PRECIO_NO_VERIFICABLE = "precio_no_verificable"
    PROMOCION_LEGACY_NO_VERIFICABLE = "promocion_legacy_no_verificable"


class MarginRisk(StrEnum):
    """Riesgos que una excepción promocional nunca oculta."""

    VENTA_BAJO_COSTO = "venta_bajo_costo"
    COSTO_SOSPECHOSO = "costo_sospechoso"
    COSTO_DESACTUALIZADO = "costo_desactualizado"
    COSTO_NO_VERIFICADO = "costo_no_verificado"
    SIN_COSTO_VALIDO = "sin_costo_valido"
    PRECIO_NO_VERIFICADO = "precio_no_verificado"


# Riesgos que se destacan siempre, también sobre una promoción o remate verificado. "costo_no_verificado"
# queda fuera porque hoy aplica a todo costo (costo neto aún no contrastado con facturas).
SEVERE_MARGIN_RISKS = frozenset(
    {
        MarginRisk.VENTA_BAJO_COSTO,
        MarginRisk.COSTO_SOSPECHOSO,
        MarginRisk.COSTO_DESACTUALIZADO,
        MarginRisk.SIN_COSTO_VALIDO,
    }
)


class CampaignKind(StrEnum):
    OFERTA = "oferta"
    REMATE = "remate"


class CampaignApproval(StrEnum):
    BORRADOR = "borrador"
    PENDIENTE_APROBACION = "pendiente_aprobacion"
    APROBADA = "aprobada"
    RECHAZADA = "rechazada"
    CANCELADA = "cancelada"


class CampaignTimeline(StrEnum):
    """Estado temporal derivado de las fechas al consultar: vence solo, sin cron."""

    PROGRAMADA = "programada"
    VIGENTE = "vigente"
    FINALIZADA = "finalizada"
    CANCELADA = "cancelada"


class ClearanceReason(StrEnum):
    VENCIMIENTO = "vencimiento"
    DESCONTINUADO = "descontinuado"
    DANO = "dano"
    SOBRESTOCK = "sobrestock"
    OTRO = "otro"


class PriceVerification(StrEnum):
    """Precio promocional declarado vs precio bruto vigente en BSALE RAW."""

    COINCIDE_BSALE = "coincide_bsale"
    NO_COINCIDE_BSALE = "no_coincide_bsale"
    SIN_PRECIO_BSALE = "sin_precio_bsale"
    PRECIO_BSALE_DESACTUALIZADO = "precio_bsale_desactualizado"
    VENCIDA_SIN_REVERTIR = "vencida_sin_revertir"
    NO_APLICA = "no_aplica"


# Un precio RAW sincronizado hace más de estos días no permite verificar una excepción.
PROMO_PRICE_MAX_AGE_DAYS = 3

BUSINESS_TIMEZONE = "America/Santiago"


class DataClass(StrEnum):
    """Clasificación de un resultado para el Dashboard V2."""

    CONFIRMADO = "confirmado"
    PROVISIONAL = "provisional"
    NO_DISPONIBLE = "no_disponible"


class FreshnessStatus(StrEnum):
    AL_DIA = "al_dia"
    ATRASADA = "atrasada"
    CON_ERROR = "con_error"
    SIN_DATOS = "sin_datos"


# Recursos RAW de los que dependen costos, márgenes y promociones.
DASHBOARD_RAW_RESOURCES = (
    "price_lists",
    "variant_prices",
    "variant_costs",
    "stock_receptions",
    "variants",
    "products",
    "product_taxes",
    "stocks",
)
# La sincronización RAW es nocturna: más de 36 h sin éxito se considera atrasada.
RAW_FRESHNESS_STALE_HOURS = 36
DASHBOARD_ALERT_DAYS = 30
DASHBOARD_TOP_N = 10
DASHBOARD_CACHE_TTL_SECONDS = 60


def local_today() -> date:
    """Fecha comercial en Chile: la vigencia de campañas se evalúa contra este día."""
    from datetime import datetime, timezone

    try:
        from zoneinfo import ZoneInfo

        return datetime.now(ZoneInfo(BUSINESS_TIMEZONE)).date()
    except Exception:
        return datetime.now(timezone.utc).date()


def business_date(ts: Any) -> date:
    """Día comercial en Chile de un timestamp (sin zona = UTC, como guarda PostgreSQL ``timestamptz``)."""
    from datetime import datetime, timezone

    if not isinstance(ts, datetime):
        return ts
    aware = ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    try:
        from zoneinfo import ZoneInfo

        return aware.astimezone(ZoneInfo(BUSINESS_TIMEZONE)).date()
    except Exception:
        return aware.astimezone(timezone.utc).date()


# Precios brutos > 0 pero por debajo de este valor (CLP) se tratan como simbólicos/ficticios.
SYMBOLIC_PRICE_MAX = Decimal("10")

# Alcance comercial por empresa. Una lista solo se evalúa si está autorizada aquí Y vigente en
# RAW (state = 0, sin missing_since). Las retiradas conservan sus reglas pero nunca se evalúan;
# si reaparecen vigentes en RAW se informa la discrepancia en vez de incorporarlas.
COMMERCIAL_PRICE_LISTS: dict[int, dict[int, str]] = {
    3: {12: "Supermercado", 13: "Ruta/Web", 16: "Melinka"},
}
RETIRED_PRICE_LISTS: dict[int, dict[int, str]] = {
    3: {14: "COMODITI ALCOHOLES"},
}


def money(value: Decimal | None) -> str | None:
    """Montos como string decimal exacto en JSON (sin float)."""
    if value is None:
        return None
    return format(value, "f")


def iso(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def paginate(items: list[Any], *, page: int, page_size: int) -> dict[str, Any]:
    total = len(items)
    start = (page - 1) * page_size
    return {
        "items": items[start : start + page_size],
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size if total else 0,
    }
