"""
Precios autoritativos para pedidos del catálogo web.

Fuente: ``bsale.catalog_view`` (la misma que ``GET /api/catalog``):
``factura`` → ``price_13`` (Ruta/Web), ``melinka`` → ``price_16``.
La lista 14 (COMODITI) no se consulta nunca para pedidos nuevos.

No hay descuentos ni redondeos en el flujo del catálogo: el precio de línea es el
precio bruto de la lista y el total es la suma exacta de ``precio × cantidad``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Iterable

_PRICE_COLUMN_BY_LIST: dict[str, str] = {
    "factura": "price_13",
    "melinka": "price_16",
}

# Los precios de lista tienen como máximo 2 decimales; el navegador los transporta como
# float, así que la comparación se hace al centavo para no rechazar por ruido binario.
_CENT = Decimal("0.01")


@dataclass(frozen=True)
class CatalogPrice:
    variant_id: int
    name: str
    barcode: str | None
    price: Decimal | None


@dataclass(frozen=True)
class PricedLine:
    variant_id: int
    name: str
    barcode: str | None
    quantity: int
    price: Decimal
    subtotal: Decimal


@dataclass
class OrderPricing:
    lines: list[PricedLine] = field(default_factory=list)
    total: Decimal = Decimal("0")
    price_issues: list[dict[str, Any]] = field(default_factory=list)
    total_matches: bool = True

    @property
    def ok(self) -> bool:
        return not self.price_issues and self.total_matches


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _to_cents(value: Decimal) -> Decimal:
    return value.quantize(_CENT, rounding=ROUND_HALF_UP)


def _clean_name(name: str | None) -> str:
    return " ".join((name or "").split())


def _has_valid_price(price: Decimal | None) -> bool:
    return price is not None and price.is_finite() and price > 0


def fetch_catalog_prices(
    cur,
    variant_ids: Iterable[int],
    price_list: str,
) -> dict[int, CatalogPrice]:
    column = _PRICE_COLUMN_BY_LIST.get(price_list)
    if column is None:
        raise ValueError(f"Lista de precios no permitida: {price_list!r}")
    ids = sorted({int(v) for v in variant_ids})
    if not ids:
        return {}
    cur.execute(
        f"""
        SELECT
            cv.variant_id,
            TRIM(cv.product || ' ' || COALESCE(cv.variant, '')) AS name,
            cv.bar_code,
            cv.{column} AS price
        FROM bsale.catalog_view cv
        WHERE cv.variant_id = ANY(%s)
        """,
        (ids,),
    )
    out: dict[int, CatalogPrice] = {}
    for vid, name, barcode, price in cur.fetchall():
        bc = (str(barcode).strip() if barcode is not None else "") or None
        out[int(vid)] = CatalogPrice(
            variant_id=int(vid),
            name=_clean_name(name),
            barcode=bc,
            price=_to_decimal(price),
        )
    return out


def price_order_items(
    items: list[dict[str, Any]],
    catalog: dict[int, CatalogPrice],
    *,
    sent_total: Any,
) -> OrderPricing:
    """
    Calcula las líneas y el total con los precios vigentes de ``catalog``.

    Cada item: ``id`` (variant_id), ``name``, ``quantity``, ``price`` (enviado por el
    navegador, solo se usa para detectar diferencias; nunca se guarda).
    """
    result = OrderPricing()
    total = Decimal("0")

    for it in items:
        vid = int(it["id"])
        qty = int(it["quantity"])
        sent_price = _to_decimal(it.get("price"))
        current = catalog.get(vid)
        sent_name = _clean_name(it.get("name")) or None

        if current is None or not _has_valid_price(current.price):
            result.price_issues.append(
                {
                    "id": vid,
                    "name": current.name if current else sent_name,
                    "reason": "sin_precio",
                    "sent_price": float(sent_price) if sent_price is not None else None,
                    "current_price": None,
                }
            )
            continue

        price = current.price
        if sent_price is None or _to_cents(sent_price) != _to_cents(price):
            result.price_issues.append(
                {
                    "id": vid,
                    "name": current.name,
                    "reason": "precio_distinto",
                    "sent_price": float(sent_price) if sent_price is not None else None,
                    "current_price": float(price),
                }
            )

        subtotal = price * qty
        total += subtotal
        result.lines.append(
            PricedLine(
                variant_id=vid,
                name=current.name,
                barcode=current.barcode,
                quantity=qty,
                price=price,
                subtotal=subtotal,
            )
        )

    result.total = total
    sent_total_dec = _to_decimal(sent_total)
    result.total_matches = sent_total_dec is not None and _to_cents(
        sent_total_dec
    ) == _to_cents(total)
    return result
