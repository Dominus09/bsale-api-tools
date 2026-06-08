"""Reglas comerciales de cantidad para catálogo web (SEC / sale_type / quantity_step)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Literal

SaleType = Literal["ENTERA", "PARCIAL", "UNITARIO"]

_VALID_SALE_TYPES = frozenset({"ENTERA", "PARCIAL", "UNITARIO"})
_SEC_RE = re.compile(r"SEC\s*(\d+)", re.IGNORECASE)
_CATALOG_COMPANY_ID = 3

_VARIANT_RULES_SQL = """
SELECT
    v.bsale_id AS variant_id,
    v.bar_code AS barcode,
    v.description AS variant_description,
    p.name AS product_name,
    COALESCE(
        NULLIF(v.units_per_box, 0),
        NULLIF(pm.units_per_box, 0),
        (regexp_match(UPPER(COALESCE(v.description, '')),
                      'SEC[[:space:]]*([0-9]+)'))[1]::integer
    ) AS units_per_box,
    pm.sale_type AS pm_sale_type,
    pm.quantity_step AS pm_quantity_step
FROM bsale.variants v
LEFT JOIN bsale.products p
    ON p.company_id = v.company_id
   AND p.bsale_id = v.product_id
LEFT JOIN bsale.products_master pm
    ON pm.variant_id = v.bsale_id
    OR (
        NULLIF(BTRIM(v.bar_code), '') IS NOT NULL
        AND pm.barcode = BTRIM(v.bar_code)
    )
WHERE v.company_id = %s
  AND v.bsale_id = ANY(%s)
"""


@dataclass(frozen=True)
class CommercialRules:
    variant_id: int
    product_name: str
    barcode: str | None
    units_per_box: int | None
    sale_type: SaleType
    quantity_step: int
    auto_unitario_no_sec: bool = False
    missing_sale_type: bool = False
    missing_quantity_step: bool = False


@dataclass(frozen=True)
class QuantityValidationResult:
    ok: bool
    message: str | None = None


def extract_sec_from_text(*texts: str | None) -> int | None:
    for t in texts:
        if not t:
            continue
        m = _SEC_RE.search(t)
        if not m:
            continue
        try:
            n = int(m.group(1))
            if n > 0:
                return n
        except (TypeError, ValueError):
            continue
    return None


def resolve_units_per_box(
    *,
    column_value: Any = None,
    description: str | None = None,
    pm_units: Any = None,
) -> int | None:
    for src in (column_value, pm_units):
        if src is None:
            continue
        try:
            n = int(src)
            if n > 0:
                return n
        except (TypeError, ValueError):
            pass
    return extract_sec_from_text(description)


def _normalize_sale_type(raw: str | None) -> SaleType | None:
    if not raw or not str(raw).strip():
        return None
    key = str(raw).strip().upper()
    if key in _VALID_SALE_TYPES:
        return key  # type: ignore[return-value]
    return None


def resolve_sale_type(
    *,
    units_per_box: int | None,
    pm_sale_type: str | None = None,
) -> SaleType:
    """Sin SEC → UNITARIO (no bloquea ventas). Con SEC → PM o ENTERA por defecto."""
    if not units_per_box or units_per_box <= 0:
        return "UNITARIO"
    configured = _normalize_sale_type(pm_sale_type)
    return configured or "ENTERA"


def resolve_quantity_step(
    *,
    sale_type: SaleType,
    units_per_box: int | None,
    pm_quantity_step: int | None = None,
) -> tuple[int, bool]:
    """
    Retorna (step, missing_config).
    missing_config=True cuando PARCIAL/ENTERA requiere dato en PM y no está.
    """
    if sale_type == "UNITARIO":
        return 1, False
    sec = units_per_box if units_per_box and units_per_box > 0 else None
    if sec is None:
        return 1, False

    if pm_quantity_step is not None:
        try:
            step = int(pm_quantity_step)
            if step > 0:
                return step, False
        except (TypeError, ValueError):
            pass

    if sale_type == "ENTERA":
        return sec, False
    # PARCIAL sin step configurado: fallback a caja completa hasta que admin configure.
    return sec, True


def build_commercial_rules(
    *,
    variant_id: int,
    product_name: str | None = None,
    barcode: str | None = None,
    units_per_box: int | None = None,
    pm_sale_type: str | None = None,
    pm_quantity_step: int | None = None,
    variant_description: str | None = None,
) -> CommercialRules:
    sec = resolve_units_per_box(
        column_value=units_per_box,
        description=variant_description,
    )
    auto_unitario = sec is None
    sale_type = resolve_sale_type(units_per_box=sec, pm_sale_type=pm_sale_type)
    step, missing_step = resolve_quantity_step(
        sale_type=sale_type,
        units_per_box=sec,
        pm_quantity_step=pm_quantity_step,
    )
    missing_sale_type = bool(sec and not _normalize_sale_type(pm_sale_type))

    return CommercialRules(
        variant_id=variant_id,
        product_name=(product_name or "").strip() or f"Producto {variant_id}",
        barcode=(barcode or "").strip() or None,
        units_per_box=sec,
        sale_type=sale_type,
        quantity_step=step,
        auto_unitario_no_sec=auto_unitario,
        missing_sale_type=missing_sale_type,
        missing_quantity_step=missing_step,
    )


def validate_quantity(
    quantity: int,
    *,
    rules: CommercialRules,
) -> QuantityValidationResult:
    if quantity < 1:
        return QuantityValidationResult(ok=False, message="La cantidad debe ser al menos 1")

    step = rules.quantity_step
    if step < 1:
        step = 1

    if quantity % step == 0:
        return QuantityValidationResult(ok=True)

    label = rules.product_name
    return QuantityValidationResult(
        ok=False,
        message=f"{label} se vende en múltiplos de {step} unidades",
    )


def fetch_commercial_rules_batch(
    cur,
    variant_ids: list[int],
    *,
    company_id: int = _CATALOG_COMPANY_ID,
) -> dict[int, CommercialRules]:
    if not variant_ids:
        return {}

    cur.execute(_VARIANT_RULES_SQL, (company_id, variant_ids))
    columns = [d[0] for d in cur.description]
    out: dict[int, CommercialRules] = {}
    for row in cur.fetchall():
        data = dict(zip(columns, row))
        vid = int(data["variant_id"])
        out[vid] = build_commercial_rules(
            variant_id=vid,
            product_name=data.get("product_name"),
            barcode=data.get("barcode"),
            units_per_box=data.get("units_per_box"),
            pm_sale_type=data.get("pm_sale_type"),
            pm_quantity_step=data.get("pm_quantity_step"),
            variant_description=data.get("variant_description"),
        )
    return out


def validate_order_items(
    cur,
    items: list[dict[str, Any]],
    *,
    company_id: int = _CATALOG_COMPANY_ID,
) -> list[dict[str, Any]]:
    """
    Valida líneas de pedido. Cada item debe tener keys: id (variant_id), name, quantity.
    Retorna lista de errores (vacía = OK).
    """
    variant_ids = [int(it["id"]) for it in items]
    rules_map = fetch_commercial_rules_batch(cur, variant_ids, company_id=company_id)
    errors: list[dict[str, Any]] = []

    for idx, it in enumerate(items):
        vid = int(it["id"])
        qty = int(it["quantity"])
        rules = rules_map.get(vid)
        if rules is None:
            errors.append(
                {
                    "line": idx,
                    "variant_id": vid,
                    "product": (it.get("name") or "").strip() or None,
                    "quantity": qty,
                    "error": "Producto no encontrado en catálogo",
                    "message": "Producto no disponible",
                }
            )
            continue

        result = validate_quantity(qty, rules=rules)
        if not result.ok:
            errors.append(
                {
                    "line": idx,
                    "variant_id": vid,
                    "product": rules.product_name,
                    "quantity": qty,
                    "required_step": rules.quantity_step,
                    "sale_type": rules.sale_type,
                    "sec": rules.units_per_box,
                    "message": result.message,
                }
            )

    return errors
