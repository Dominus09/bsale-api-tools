"""Proyección de lectura Catálogo V2 (bsale_raw + products_master).

No escribe tablas. No altera GET /api/catalog productivo.
Identidad estable: company_id + variant bsale_id (nunca por nombre).
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from backend.db import get_connection
from backend.utils.sale_quantity import build_commercial_rules

logger = logging.getLogger(__name__)

COMPANY_ID = 3
PRICE_LIST_ALIASES = {
    "factura": 13,  # RUTA/WEB
    "melinka": 16,
}
DEFAULT_STOCK_OFFICE_ID = 1  # BODEGA CENTRAL (paridad con catálogo ruta)


class CatalogV2ProjectionError(ValueError):
    def __init__(self, message: str, *, code: str = "catalog_v2_error"):
        super().__init__(message)
        self.code = code


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clean_name(name: str | None) -> str:
    return " ".join((name or "").split())


def _resolve_price_list_id(price_list: str) -> int:
    key = (price_list or "").strip().lower()
    if key in ("comoditi", "14"):
        raise CatalogV2ProjectionError(
            "La lista COMODITI está descontinuada", code="price_list_discontinued"
        )
    if key in PRICE_LIST_ALIASES:
        return PRICE_LIST_ALIASES[key]
    if key.isdigit():
        return int(key)
    raise CatalogV2ProjectionError(
        "price_list debe ser factura, melinka o id numérico de lista",
        code="price_list_invalid",
    )


_CATALOG_V2_SQL = """
SELECT
    v.bsale_id AS variant_id,
    v.product_id,
    v.code AS sku,
    NULLIF(BTRIM(v.bar_code), '') AS barcode,
    v.description AS variant_name,
    v.state AS variant_state,
    p.name AS product_name,
    p.state AS product_state,
    pt.name AS product_type,
    p.product_type_id,
    vp.variant_value_with_taxes AS price_gross,
    vp.variant_value AS price_net,
    vp.api_fetched_at AS price_fetched_at,
    st_off.quantity AS stock_office_quantity,
    st_off.quantity_available AS stock_office_available,
    st_off.quantity_reserved AS stock_office_reserved,
    st_off.api_fetched_at AS stock_office_fetched_at,
    st_off.payload AS stock_office_payload,
    st_all.stock_total AS stock_all_offices,
    NULLIF(BTRIM(COALESCE(v.payload->>'image', v.payload->>'imageUrl', '')), '') AS image_raw,
    COALESCE(lv.units_per_box, pm.units_per_box) AS units_per_box_source,
    v.description AS variant_description,
    pm.units_per_box AS pm_units_per_box,
    pm.weight_box_kg AS peso_caja_kg,
    pm.height_cm AS alto_caja_cm,
    pm.width_cm AS ancho_caja_cm,
    pm.length_cm AS largo_caja_cm,
    pm.sale_type AS pm_sale_type,
    pm.quantity_step AS pm_quantity_step,
    pm.id AS pm_id,
    pm.logistics_completed,
    pm.updated_at AS pm_updated_at,
    (v.missing_since IS NOT NULL) AS raw_missing,
    v.api_fetched_at AS variant_fetched_at
FROM bsale_raw.variants v
LEFT JOIN bsale_raw.products p
    ON p.company_id = v.company_id AND p.bsale_id = v.product_id
LEFT JOIN bsale_raw.product_types pt
    ON pt.company_id = p.company_id AND pt.bsale_id = p.product_type_id
LEFT JOIN bsale_raw.variant_prices vp
    ON vp.company_id = v.company_id
   AND vp.variant_id = v.bsale_id
   AND vp.price_list_id = %s
   AND vp.missing_since IS NULL
LEFT JOIN bsale_raw.stocks st_off
    ON st_off.company_id = v.company_id
   AND st_off.variant_id = v.bsale_id
   AND st_off.office_id = %s
LEFT JOIN (
    SELECT company_id, variant_id, SUM(quantity) AS stock_total
    FROM bsale_raw.stocks
    WHERE company_id = %s
    GROUP BY company_id, variant_id
) st_all
    ON st_all.company_id = v.company_id AND st_all.variant_id = v.bsale_id
LEFT JOIN bsale.variants lv
    ON lv.company_id = v.company_id AND lv.bsale_id = v.bsale_id
LEFT JOIN LATERAL (
    SELECT pm_inner.*
    FROM bsale.products_master pm_inner
    WHERE pm_inner.variant_id = v.bsale_id
       OR (
            NULLIF(BTRIM(v.bar_code), '') IS NOT NULL
            AND BTRIM(pm_inner.barcode) = BTRIM(v.bar_code)
       )
    ORDER BY
        CASE WHEN pm_inner.variant_id = v.bsale_id THEN 0 ELSE 1 END,
        pm_inner.updated_at DESC NULLS LAST,
        pm_inner.id DESC
    LIMIT 1
) pm ON TRUE
WHERE v.company_id = %s
  AND v.missing_since IS NULL
  AND COALESCE(v.state, 0) = 0
ORDER BY
    lower(COALESCE(pt.name, '')),
    lower(COALESCE(p.name, '')),
    lower(COALESCE(v.description, '')),
    v.bsale_id
"""


def _upb_provenance(row: dict[str, Any], resolved_upb: int | None) -> dict[str, Any]:
    """Clasifica UPB: verified (PM/legacy col), inherited_sec, o missing.

    No deduce desde 'x18'/'x12' en el nombre de presentación.
    """
    pm_upb = row.get("pm_units_per_box")
    try:
        if pm_upb is not None and int(pm_upb) > 0:
            return {
                "upb_status": "verified",
                "upb_source": "products_master",
                "missing_upb": False,
            }
    except (TypeError, ValueError):
        pass
    col = row.get("units_per_box_source")
    # units_per_box_source = COALESCE(legacy, pm); si pm vacío y col>0 → legacy col
    try:
        if col is not None and int(col) > 0 and (
            pm_upb is None or int(pm_upb or 0) <= 0
        ):
            return {
                "upb_status": "verified",
                "upb_source": "bsale.variants.units_per_box",
                "missing_upb": False,
            }
    except (TypeError, ValueError):
        pass
    if resolved_upb is not None and int(resolved_upb) > 0:
        return {
            "upb_status": "inherited_sec",
            "upb_source": "variant_description_SEC",
            "missing_upb": False,
            "note_es": "UPB heredado de SEC en descripción; no verificado en PM.",
        }
    return {
        "upb_status": "missing",
        "upb_source": None,
        "missing_upb": True,
    }


def _row_to_public_item(row: dict[str, Any], *, price_list_key: str) -> dict[str, Any]:
    name = _clean_name(
        f"{row.get('product_name') or ''} {row.get('variant_name') or ''}".strip()
    )
    upb_raw = row.get("units_per_box_source")
    rules = build_commercial_rules(
        variant_id=int(row["variant_id"]),
        product_name=name,
        barcode=row.get("barcode"),
        units_per_box=upb_raw,
        pm_sale_type=row.get("pm_sale_type"),
        pm_quantity_step=row.get("pm_quantity_step"),
        variant_description=row.get("variant_description"),
    )
    # Paridad con catalog_view: stock público = quantity_available (oficina 1).
    stock_avail = row.get("stock_office_available")
    stock_qty = row.get("stock_office_quantity")
    stock_reserved = row.get("stock_office_reserved")
    price = _to_float(row.get("price_gross"))
    upb_meta = _upb_provenance(row, rules.units_per_box)
    payload = row.get("stock_office_payload") or {}
    if not isinstance(payload, dict):
        payload = {}
    return {
        # Contrato compatible con GET /api/catalog
        "id": int(row["variant_id"]),
        "name": name,
        "type": (row.get("product_type") or "").strip(),
        "barcode": (row.get("barcode") or "").strip(),
        "price": price if price is not None else 0.0,
        "stock": int(stock_avail) if stock_avail is not None else 0,
        "image": row.get("image_raw"),
        "units_per_box": rules.units_per_box,
        "sale_type": rules.sale_type,
        "quantity_step": rules.quantity_step,
        # Metadatos V2 (preview / compare; strip en cutover público)
        "v2": {
            "source": "bsale_raw+products_master",
            "price_list_key": price_list_key,
            "price_list_id": PRICE_LIST_ALIASES.get(price_list_key)
            if price_list_key in PRICE_LIST_ALIASES
            else None,
            "price_net": _to_float(row.get("price_net")),
            "price_fetched_at": row["price_fetched_at"].isoformat()
            if row.get("price_fetched_at")
            else None,
            "sku": row.get("sku"),
            "product_name": row.get("product_name"),
            "variant_name": row.get("variant_name"),
            "stock_office_id": DEFAULT_STOCK_OFFICE_ID,
            "stock_field_public": "quantity_available",
            "stock_quantity": _to_float(stock_qty),
            "stock_reserved": _to_float(stock_reserved),
            "stock_office_available": _to_float(stock_avail),
            "stock_all_offices": _to_float(row.get("stock_all_offices")),
            "stock_office_fetched_at": row["stock_office_fetched_at"].isoformat()
            if row.get("stock_office_fetched_at")
            else None,
            "bsale_api_snapshot": {
                "quantity": payload.get("quantity"),
                "quantityAvailable": payload.get("quantityAvailable"),
                "quantityReserved": payload.get("quantityReserved"),
            },
            "pm_id": row.get("pm_id"),
            "pm_units_per_box": row.get("pm_units_per_box"),
            "peso_caja_kg": _to_float(row.get("peso_caja_kg")),
            "alto_caja_cm": _to_float(row.get("alto_caja_cm")),
            "ancho_caja_cm": _to_float(row.get("ancho_caja_cm")),
            "largo_caja_cm": _to_float(row.get("largo_caja_cm")),
            "logistics_completed": bool(row.get("logistics_completed"))
            if row.get("logistics_completed") is not None
            else None,
            **upb_meta,
            "active": int(row.get("variant_state") or 0) == 0,
            "variant_fetched_at": row["variant_fetched_at"].isoformat()
            if row.get("variant_fetched_at")
            else None,
        },
    }


def list_catalog_v2(
    *,
    price_list: str,
    in_stock: bool | None = None,
    office_id: int = DEFAULT_STOCK_OFFICE_ID,
    limit: int | None = None,
    q: str | None = None,
) -> dict[str, Any]:
    """Lista proyección V2 con contrato público + metadatos v2."""
    price_list_id = _resolve_price_list_id(price_list)
    key = (price_list or "").strip().lower()
    if key.isdigit():
        key = "custom"

    conn = get_connection()
    try:
        cur = conn.cursor()
        sql = _CATALOG_V2_SQL
        params: list[Any] = [
            price_list_id,
            int(office_id),
            COMPANY_ID,
            COMPANY_ID,
        ]
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()
    finally:
        conn.close()

    items = [_row_to_public_item(r, price_list_key=key) for r in rows]

    if q:
        ql = q.strip().lower()
        items = [
            it
            for it in items
            if ql in (it.get("name") or "").lower()
            or ql in (it.get("barcode") or "").lower()
            or ql in str(it.get("id"))
        ]

    if in_stock is True:
        items = [it for it in items if int(it.get("stock") or 0) > 0]
    elif in_stock is False:
        items = [it for it in items if int(it.get("stock") or 0) <= 0]

    total = len(items)
    if limit is not None and limit > 0:
        items = items[: int(limit)]

    return {
        "company_id": COMPANY_ID,
        "price_list": key,
        "price_list_id": price_list_id,
        "stock_office_id": int(office_id),
        "mode": "preview_v2",
        "count": len(items),
        "total_before_limit": total,
        "items": items,
        "note_es": (
            "Preview de lectura. No reemplaza GET /api/catalog. "
            "Sin costos ni márgenes. UPB/cubicación desde Products Master / SEC."
        ),
    }


_LEGACY_BY_IDS_SQL = """
SELECT
    cv.variant_id AS id,
    TRIM(cv.product || ' ' || COALESCE(cv.variant, '')) AS name,
    cv.product_type AS type,
    cv.bar_code AS barcode,
    COALESCE(cv.stock, 0) AS stock,
    CASE
        WHEN %s = 'factura' THEN cv.price_13
        WHEN %s = 'melinka' THEN cv.price_16
        ELSE NULL
    END AS price,
    COALESCE(
        NULLIF(v.units_per_box, 0),
        NULLIF(pm.units_per_box, 0),
        (regexp_match(UPPER(COALESCE(v.description, '')),
                      'SEC[[:space:]]*([0-9]+)'))[1]::integer
    ) AS units_per_box
FROM bsale.catalog_view cv
LEFT JOIN bsale.variants v
    ON v.company_id = %s AND v.bsale_id = cv.variant_id
LEFT JOIN bsale.products_master pm
    ON pm.variant_id = cv.variant_id
    OR (
        NULLIF(BTRIM(cv.bar_code), '') IS NOT NULL
        AND BTRIM(pm.barcode) = BTRIM(cv.bar_code)
    )
WHERE cv.variant_id = ANY(%s)
"""


def compare_catalog_sample(
    *,
    price_list: str = "factura",
    variant_ids: list[int] | None = None,
    search_terms: list[str] | None = None,
    limit: int = 40,
) -> dict[str, Any]:
    """Compara proyección V2 vs legacy catalog_view para una muestra."""
    key = (price_list or "factura").strip().lower()
    if key not in PRICE_LIST_ALIASES:
        raise CatalogV2ProjectionError(
            "compare solo soporta factura|melinka", code="price_list_invalid"
        )

    v2 = list_catalog_v2(price_list=key, in_stock=None, limit=None)
    by_id = {int(it["id"]): it for it in v2["items"]}

    selected: list[int] = []
    if variant_ids:
        selected.extend(int(x) for x in variant_ids)
    if search_terms:
        for term in search_terms:
            t = term.strip().lower()
            if not t:
                continue
            for it in v2["items"]:
                blob = f"{it.get('name','')} {it.get('barcode','')}".lower()
                if t in blob and int(it["id"]) not in selected:
                    selected.append(int(it["id"]))
                    if len(selected) >= limit:
                        break
            if len(selected) >= limit:
                break

    if not selected:
        # Muestra diversa: con/sin stock, con/sin UPB
        with_stock = [it for it in v2["items"] if int(it.get("stock") or 0) > 0][:8]
        zero_stock = [it for it in v2["items"] if int(it.get("stock") or 0) <= 0][:4]
        missing_upb = [it for it in v2["items"] if (it.get("v2") or {}).get("missing_upb")][:4]
        with_upb = [it for it in v2["items"] if not (it.get("v2") or {}).get("missing_upb")][:4]
        for bucket in (with_stock, zero_stock, missing_upb, with_upb):
            for it in bucket:
                if int(it["id"]) not in selected:
                    selected.append(int(it["id"]))
        selected = selected[:limit]

    selected = selected[:limit]
    if not selected:
        return {
            "price_list": key,
            "compared": 0,
            "diffs": [],
            "rows": [],
            "note_es": "Sin variantes V2 para comparar",
        }

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            _LEGACY_BY_IDS_SQL,
            (key, key, COMPANY_ID, selected),
        )
        cols = [d[0] for d in cur.description]
        legacy_rows = {int(r[0]): dict(zip(cols, r)) for r in cur.fetchall() if r[0] is not None}
        cur.close()
    finally:
        conn.close()

    rows_out: list[dict[str, Any]] = []
    diff_count = 0
    for vid in selected:
        v2_it = by_id.get(vid)
        leg = legacy_rows.get(vid)
        if not v2_it:
            continue
        causes: list[str] = []
        leg_price = _to_float(leg.get("price")) if leg else None
        v2_price = _to_float(v2_it.get("price"))
        leg_stock = int(leg["stock"]) if leg and leg.get("stock") is not None else None
        v2_stock = int(v2_it.get("stock") or 0)
        leg_upb = leg.get("units_per_box") if leg else None
        v2_upb = v2_it.get("units_per_box")
        leg_bc = (str(leg.get("barcode") or "").strip() if leg else "")
        v2_bc = (v2_it.get("barcode") or "").strip()

        if leg is None:
            causes.append("solo_en_v2_no_en_catalog_view")
        else:
            if leg_bc != v2_bc:
                causes.append("barcode_difiere")
            if leg_price is None and v2_price not in (None, 0.0):
                causes.append("precio_solo_v2")
            elif v2_price is None and leg_price not in (None, 0.0):
                causes.append("precio_solo_legacy")
            elif leg_price is not None and v2_price is not None and abs(leg_price - v2_price) > 0.009:
                causes.append("precio_difiere")
            if leg_stock is not None and leg_stock != v2_stock:
                causes.append("stock_difiere_office1_vs_catalog_view")
            if (leg_upb or None) != (v2_upb or None):
                causes.append("upb_difiere")

        if causes:
            diff_count += 1
        rows_out.append(
            {
                "variant_id": vid,
                "name_v2": v2_it.get("name"),
                "barcode_v2": v2_bc,
                "barcode_legacy": leg_bc or None,
                "price_v2": v2_price,
                "price_legacy": leg_price,
                "stock_v2_office1_available": v2_stock,
                "stock_legacy_available": leg_stock,
                "upb_v2": v2_upb,
                "upb_legacy": leg_upb,
                "causes": causes,
                "match": len(causes) == 0,
            }
        )

    return {
        "price_list": key,
        "price_list_id": PRICE_LIST_ALIASES[key],
        "compared": len(rows_out),
        "diff_count": diff_count,
        "rows": rows_out,
        "note_es": (
            "Stock público V2 = quantity_available office_id=1 (paridad catalog_view). "
            "Legacy stocks.updated_at puede estar atrasado vs RAW payload Bsale. "
            "Diferencia quantity vs available = reservas (quantityReserved)."
        ),
    }


def public_items_from_v2(price_list: str, *, in_stock: bool | None = None) -> list[dict[str, Any]]:
    """Lista con contrato público puro (sin bloque v2) para GET /api/catalog si flag=v2."""
    payload = list_catalog_v2(price_list=price_list, in_stock=in_stock, limit=None)
    out: list[dict[str, Any]] = []
    for it in payload["items"]:
        out.append(
            {
                "id": it["id"],
                "name": it["name"],
                "type": it["type"],
                "barcode": it["barcode"],
                "price": it["price"],
                "stock": it["stock"],
                "image": it.get("image"),
                "units_per_box": it.get("units_per_box"),
                "sale_type": it.get("sale_type"),
                "quantity_step": it.get("quantity_step"),
            }
        )
    return out


def upb_missing_priority_report(*, limit: int = 100) -> dict[str, Any]:
    """Faltantes UPB prioritarios: activos, con stock RAW, sin deducir xN del nombre."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            WITH base AS (
                SELECT
                    v.bsale_id AS variant_id,
                    NULLIF(BTRIM(v.bar_code), '') AS barcode,
                    v.description AS variant_description,
                    p.name AS product_name,
                    pt.name AS product_type,
                    COALESCE(st.quantity_available, 0) AS stock_raw_o1,
                    pm.id AS pm_id,
                    pm.units_per_box AS pm_upb,
                    lv.units_per_box AS legacy_upb,
                    CASE
                        WHEN COALESCE(pm.units_per_box, 0) > 0 THEN 'verified_pm'
                        WHEN COALESCE(lv.units_per_box, 0) > 0 THEN 'verified_legacy'
                        WHEN (regexp_match(UPPER(COALESCE(v.description, '')),
                              'SEC[[:space:]]*([0-9]+)'))[1] IS NOT NULL
                            THEN 'inherited_sec'
                        ELSE 'missing'
                    END AS upb_status
                FROM bsale_raw.variants v
                LEFT JOIN bsale_raw.products p
                    ON p.company_id = v.company_id AND p.bsale_id = v.product_id
                LEFT JOIN bsale_raw.product_types pt
                    ON pt.company_id = p.company_id AND pt.bsale_id = p.product_type_id
                LEFT JOIN bsale_raw.stocks st
                    ON st.company_id = v.company_id
                   AND st.variant_id = v.bsale_id
                   AND st.office_id = 1
                LEFT JOIN bsale.variants lv
                    ON lv.company_id = v.company_id AND lv.bsale_id = v.bsale_id
                LEFT JOIN LATERAL (
                    SELECT id, units_per_box
                    FROM bsale.products_master pm
                    WHERE pm.variant_id = v.bsale_id
                       OR (
                            NULLIF(BTRIM(v.bar_code), '') IS NOT NULL
                            AND BTRIM(pm.barcode) = BTRIM(v.bar_code)
                       )
                    ORDER BY
                        CASE WHEN pm.variant_id = v.bsale_id THEN 0 ELSE 1 END,
                        pm.updated_at DESC NULLS LAST
                    LIMIT 1
                ) pm ON TRUE
                WHERE v.company_id = %s
                  AND v.missing_since IS NULL
                  AND COALESCE(v.state, 0) = 0
            )
            SELECT
                variant_id, barcode, product_name, variant_description, product_type,
                stock_raw_o1, pm_id, pm_upb, legacy_upb, upb_status
            FROM base
            WHERE upb_status IN ('missing', 'inherited_sec')
            ORDER BY
                CASE WHEN stock_raw_o1 > 0 THEN 0 ELSE 1 END,
                stock_raw_o1 DESC,
                variant_id
            LIMIT %s
            """,
            (COMPANY_ID, int(limit)),
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE COALESCE(pm.units_per_box, lv.units_per_box, 0) <= 0
                    AND (regexp_match(UPPER(COALESCE(v.description,'')),
                         'SEC[[:space:]]*([0-9]+)'))[1] IS NULL)::bigint AS missing_strict,
                COUNT(*) FILTER (WHERE COALESCE(pm.units_per_box, lv.units_per_box, 0) <= 0)::bigint
                    AS missing_or_only_sec_eligible,
                COUNT(*) FILTER (
                    WHERE COALESCE(pm.units_per_box, lv.units_per_box, 0) <= 0
                      AND COALESCE(st.quantity_available, 0) > 0
                )::bigint AS missing_with_stock
            FROM bsale_raw.variants v
            LEFT JOIN bsale.variants lv
                ON lv.company_id = v.company_id AND lv.bsale_id = v.bsale_id
            LEFT JOIN bsale_raw.stocks st
                ON st.company_id = v.company_id AND st.variant_id = v.bsale_id
               AND st.office_id = 1
            LEFT JOIN LATERAL (
                SELECT units_per_box FROM bsale.products_master pm
                WHERE pm.variant_id = v.bsale_id
                   OR (NULLIF(BTRIM(v.bar_code),'') IS NOT NULL
                       AND BTRIM(pm.barcode)=BTRIM(v.bar_code))
                ORDER BY CASE WHEN pm.variant_id = v.bsale_id THEN 0 ELSE 1 END
                LIMIT 1
            ) pm ON TRUE
            WHERE v.company_id = %s
              AND v.missing_since IS NULL
              AND COALESCE(v.state, 0) = 0
            """,
            (COMPANY_ID,),
        )
        summary = dict(zip(
            ("missing_strict", "missing_or_only_sec_eligible", "missing_with_stock"),
            cur.fetchone() or (0, 0, 0),
        ))
        cur.close()
    finally:
        conn.close()

    for r in rows:
        r["stock_raw_o1"] = _to_float(r.get("stock_raw_o1")) or 0.0
        # Hint de formato SOLO informativo — no se usa como UPB.
        blob = f"{r.get('product_name') or ''} {r.get('variant_description') or ''}".lower()
        hint = None
        for label, pat in (("x24", "x24"), ("x18", "x18"), ("x12", "x12")):
            if pat in blob.replace(" ", ""):
                hint = label
                break
        r["format_name_hint_not_authoritative"] = hint

    return {
        "company_id": COMPANY_ID,
        "summary": {k: int(summary.get(k) or 0) for k in summary},
        "priority_rows": rows,
        "note_es": (
            "No se asigna UPB desde x12/x18/x24 del nombre. "
            "Sol x18 (31199) sin fila PM: SEC 1 en descripción es sospechoso para pack x18; "
            "corregir manualmente en ERP/PM a 18."
        ),
    }


def upb_coverage_report() -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                COUNT(*)::bigint AS raw_variants_active,
                COUNT(*) FILTER (
                    WHERE COALESCE(lv.units_per_box, pm.units_per_box, 0) > 0
                )::bigint AS with_upb,
                COUNT(*) FILTER (
                    WHERE COALESCE(lv.units_per_box, pm.units_per_box, 0) <= 0
                )::bigint AS missing_upb,
                COUNT(*) FILTER (WHERE pm.id IS NOT NULL)::bigint AS with_pm_row,
                COUNT(*) FILTER (
                    WHERE pm.weight_box_kg IS NOT NULL AND pm.weight_box_kg > 0
                )::bigint AS with_peso
            FROM bsale_raw.variants v
            LEFT JOIN bsale.variants lv
                ON lv.company_id = v.company_id AND lv.bsale_id = v.bsale_id
            LEFT JOIN LATERAL (
                SELECT *
                FROM bsale.products_master pm
                WHERE pm.variant_id = v.bsale_id
                   OR (
                        NULLIF(BTRIM(v.bar_code), '') IS NOT NULL
                        AND BTRIM(pm.barcode) = BTRIM(v.bar_code)
                   )
                ORDER BY
                    CASE WHEN pm.variant_id = v.bsale_id THEN 0 ELSE 1 END,
                    pm.updated_at DESC NULLS LAST
                LIMIT 1
            ) pm ON TRUE
            WHERE v.company_id = %s
              AND v.missing_since IS NULL
              AND COALESCE(v.state, 0) = 0
            """,
            (COMPANY_ID,),
        )
        row = cur.fetchone()
        cur.close()
    finally:
        conn.close()

    keys = (
        "raw_variants_active",
        "with_upb",
        "missing_upb",
        "with_pm_row",
        "with_peso",
    )
    data = dict(zip(keys, row)) if row else {}
    return {
        "company_id": COMPANY_ID,
        **{k: int(data.get(k) or 0) for k in keys},
        "note_es": (
            "UPB = COALESCE(legacy variants.units_per_box, products_master.units_per_box). "
            "SEC en descripción se aplica al armar ítems del catálogo, no en este conteo SQL."
        ),
    }


def raw_sync_integrity_report() -> dict[str, Any]:
    """Detecta SUCCESS con last_error residual / atraso por recurso."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT resource, scope, status, last_success_at, last_attempt_at,
                   last_error_at, rows_received,
                   left(coalesce(last_error, ''), 160) AS last_error
            FROM bsale_raw.sync_state
            WHERE company_id = %s
            ORDER BY resource, scope
            """,
            (COMPANY_ID,),
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()
    finally:
        conn.close()

    flagged: list[dict[str, Any]] = []
    residual_stale = 0
    residual_current = 0
    for r in rows:
        err = (r.get("last_error") or "").strip()
        status = (r.get("status") or "").upper()
        issues: list[str] = []
        severity = "info"
        err_at = r.get("last_error_at")
        ok_at = r.get("last_success_at")
        if status == "RUNNING":
            issues.append("status_running")
            severity = "warn"
        if status == "SUCCESS" and err:
            if err_at and ok_at and err_at < ok_at:
                issues.append("residual_error_stale_after_success")
                residual_stale += 1
                severity = "info"
            else:
                issues.append("residual_error_current_or_uncleared")
                residual_current += 1
                severity = "warn"
        elif err_at and ok_at and err_at >= ok_at and status != "SUCCESS":
            issues.append("error_at_gte_success_at")
            severity = "warn"
            residual_current += 1
        if issues:
            flagged.append(
                {
                    "resource": r["resource"],
                    "scope": r["scope"],
                    "status": r["status"],
                    "severity": severity,
                    "last_success_at": ok_at.isoformat() if ok_at else None,
                    "last_error_at": err_at.isoformat() if err_at else None,
                    "rows_received": r.get("rows_received"),
                    "last_error": err or None,
                    "issues": issues,
                }
            )

    return {
        "company_id": COMPANY_ID,
        "resources": len(rows),
        "flagged_partial_or_noisy": len(flagged),
        "residual_stale_count": residual_stale,
        "residual_current_count": residual_current,
        "flagged": flagged,
        "recommendation_es": (
            "SUCCESS + last_error con last_error_at < last_success_at = residual histórico "
            "(sync posterior OK; limpiar last_error en próximo ciclo). "
            "No marcar VALIDATED si residual_current > 0, status=RUNNING, o rows_received "
            "inestable entre reintentos. No usar solo timestamp reciente como prueba de integridad."
        ),
    }
