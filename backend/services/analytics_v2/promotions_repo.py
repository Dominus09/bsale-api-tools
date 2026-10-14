"""SQL de Promociones V2.

Lecturas: tablas app.commercial_*_v2, BSALE RAW (variantes, precios, stock, listas) y las tablas
legacy app.promotion* en solo lectura. Escrituras: exclusivamente app.commercial_*_v2.
Todas las sentencias filtran por empresa. Nunca se lee ``payload`` ni se escribe en RAW o legacy.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable, Mapping, Sequence

from backend.services.analytics_v2.commercial_scope import PRICE_LISTS_SQL

QueryExecutor = Callable[[str, tuple[Any, ...]], list[dict[str, Any]]]

TABLES_READY_SQL = """
SELECT to_regclass('app.commercial_campaigns_v2') IS NOT NULL
   AND to_regclass('app.commercial_campaign_items_v2') IS NOT NULL
   AND to_regclass('app.commercial_campaign_history_v2') IS NOT NULL AS ready
"""

CAMPAIGN_COLUMNS = """
c.id, c.company_id, c.kind, c.name, c.starts_on, c.ends_on, c.approval_status, c.responsible, c.notes,
c.created_by, c.created_at, c.updated_by, c.updated_at, c.submitted_by, c.submitted_at,
c.approved_by, c.approved_at, c.rejected_by, c.rejected_at, c.cancelled_by, c.cancelled_at,
c.status_reason, c.legacy_promotion_id, c.version
"""

CAMPAIGNS_RANGE_SQL = f"""
SELECT {CAMPAIGN_COLUMNS}
FROM app.commercial_campaigns_v2 c
WHERE c.company_id = %s
  AND c.starts_on <= %s
  AND c.ends_on >= %s
ORDER BY c.starts_on DESC, c.id DESC
"""

CAMPAIGN_SQL = f"""
SELECT {CAMPAIGN_COLUMNS}
FROM app.commercial_campaigns_v2 c
WHERE c.company_id = %s AND c.id = %s
"""

ITEM_COLUMNS = """
i.id, i.campaign_id, i.company_id, i.variant_id, i.product_id, i.price_list_id, i.sku_code, i.barcode,
i.product_name, i.variant_name, i.normal_gross_price, i.normal_price_fetched_at, i.promo_gross_price,
i.clearance_reason, i.reference_stock, i.reference_stock_fetched_at, i.notes, i.item_status,
i.legacy_snapshot_id, i.created_by, i.created_at, i.updated_by, i.updated_at, i.removed_by, i.removed_at,
i.version
"""

ITEMS_SQL = f"""
SELECT {ITEM_COLUMNS}
FROM app.commercial_campaign_items_v2 i
WHERE i.company_id = %s AND i.campaign_id = ANY(%s)
ORDER BY i.campaign_id, i.product_name NULLS LAST, i.id
"""

ITEM_SQL = f"""
SELECT {ITEM_COLUMNS}
FROM app.commercial_campaign_items_v2 i
WHERE i.company_id = %s AND i.campaign_id = %s AND i.id = %s
"""

# Ítems activos de campañas aprobadas o pendientes que cubren un rango: excepciones y conflictos.
COVERING_ITEMS_SQL = """
SELECT i.id AS item_id, i.campaign_id, c.company_id, c.name AS campaign_name, c.kind, c.approval_status,
       c.starts_on, c.ends_on, c.approved_by, c.approved_at, i.variant_id, i.price_list_id,
       i.promo_gross_price, i.item_status
FROM app.commercial_campaign_items_v2 i
JOIN app.commercial_campaigns_v2 c ON c.id = i.campaign_id AND c.company_id = i.company_id
WHERE c.company_id = %s
  AND i.item_status = 'activo'
  AND c.approval_status IN ('aprobada', 'pendiente_aprobacion')
  AND c.starts_on <= %s
  AND c.ends_on >= %s
"""

CAMPAIGN_HISTORY_SQL = """
SELECT h.id, h.campaign_id, h.item_id, h.event, h.previous_status, h.new_status, h.previous_values,
       h.new_values, h.reason, h.actor, h.actor_role, h.occurred_at
FROM app.commercial_campaign_history_v2 h
WHERE h.company_id = %s AND h.campaign_id = %s
ORDER BY h.occurred_at, h.id
"""

COMPANY_HISTORY_SQL = """
SELECT h.id, h.campaign_id, c.name AS campaign_name, c.kind, h.item_id, i.product_name, i.price_list_id,
       h.event, h.previous_status, h.new_status, h.previous_values, h.new_values, h.reason, h.actor,
       h.actor_role, h.occurred_at, count(*) OVER () AS total_rows
FROM app.commercial_campaign_history_v2 h
JOIN app.commercial_campaigns_v2 c ON c.id = h.campaign_id AND c.company_id = h.company_id
LEFT JOIN app.commercial_campaign_items_v2 i ON i.id = h.item_id
WHERE h.company_id = %s
  AND h.occurred_at >= %s
  AND h.occurred_at < %s
ORDER BY h.occurred_at DESC, h.id DESC
LIMIT %s OFFSET %s
"""

RAW_PRICES_SQL = """
SELECT vp.price_list_id, vp.variant_id, vp.variant_value_with_taxes, vp.missing_since, vp.api_fetched_at
FROM bsale_raw.variant_prices vp
WHERE vp.company_id = %s
  AND vp.variant_id = ANY(%s)
  AND vp.price_list_id = ANY(%s)
"""

RAW_VARIANT_SQL = """
SELECT v.bsale_id AS variant_id, v.product_id, v.code, v.bar_code, v.description, v.state, v.missing_since,
       p.name AS product_name
FROM bsale_raw.variants v
LEFT JOIN bsale_raw.products p ON p.company_id = v.company_id AND p.bsale_id = v.product_id
WHERE v.company_id = %s AND v.bsale_id = %s
"""

RAW_STOCK_SQL = """
SELECT sum(s.quantity_available) AS quantity_available, max(s.api_fetched_at) AS fetched_at, count(*) AS offices
FROM bsale_raw.stocks s
WHERE s.company_id = %s AND s.variant_id = %s
"""

PRODUCT_SEARCH_SQL = """
SELECT v.bsale_id AS variant_id, v.product_id, v.code, v.bar_code, v.description, v.state,
       p.name AS product_name
FROM bsale_raw.variants v
LEFT JOIN bsale_raw.products p ON p.company_id = v.company_id AND p.bsale_id = v.product_id
WHERE v.company_id = %s
  AND v.missing_since IS NULL
  AND (v.code ILIKE %s OR v.bar_code ILIKE %s OR p.name ILIKE %s OR v.description ILIKE %s)
ORDER BY (v.state = 0) DESC, p.name NULLS LAST, v.bsale_id
LIMIT %s
"""

LEGACY_SNAPSHOTS_SQL = """
SELECT ps.id AS snapshot_id, ps.promotion_id, p.tipo, p.canal, p.fecha_inicio, p.fecha_fin, p.activa,
       p.created_at, ps.barcode, ps.price_list,
       COALESCE(ps.regular_price, ps.precio_normal) AS regular_price,
       COALESCE(ps.sale_price, ps.precio_oferta) AS sale_price,
       ps.fecha_generado, vm.variant_ids, plm.price_list_ids
FROM app.promotion_price_snapshot ps
JOIN app.promotions p ON p.id = ps.promotion_id
LEFT JOIN LATERAL (
    SELECT array_agg(v.bsale_id ORDER BY v.bsale_id) AS variant_ids
    FROM bsale_raw.variants v
    WHERE v.company_id = ps.company_id
      AND btrim(v.bar_code) = btrim(ps.barcode)
      AND v.missing_since IS NULL
) vm ON TRUE
LEFT JOIN LATERAL (
    SELECT array_agg(pl.bsale_id ORDER BY pl.bsale_id) AS price_list_ids
    FROM bsale_raw.price_lists pl
    WHERE pl.company_id = ps.company_id
      AND lower(btrim(pl.name)) = lower(btrim(ps.price_list))
) plm ON TRUE
WHERE ps.company_id = %s
ORDER BY p.fecha_inicio DESC, ps.promotion_id DESC, ps.id
"""

LEGACY_PROMOTIONS_SQL = """
SELECT DISTINCT p.id AS promotion_id, p.tipo, p.canal, p.fecha_inicio, p.fecha_fin, p.activa, p.created_at,
       (SELECT count(*) FROM app.promotion_items pi WHERE pi.promotion_id = p.id) AS items,
       (SELECT count(*) FROM app.promotion_price_snapshot s
         WHERE s.promotion_id = p.id AND s.company_id = pc.company_id) AS snapshots
FROM app.promotions p
JOIN app.promotion_companies pc ON pc.promotion_id = p.id
WHERE pc.company_id = %s
ORDER BY p.fecha_inicio DESC, p.id DESC
"""

LEGACY_IMPORTED_SQL = """
SELECT c.legacy_promotion_id, c.id AS campaign_id
FROM app.commercial_campaigns_v2 c
WHERE c.company_id = %s AND c.legacy_promotion_id IS NOT NULL
"""

# ---------------------------------------------------------------- escrituras (solo app.commercial_*_v2)

INSERT_CAMPAIGN_SQL = f"""
INSERT INTO app.commercial_campaigns_v2 AS c
    (company_id, kind, name, starts_on, ends_on, approval_status, responsible, notes, created_by, legacy_promotion_id)
VALUES (%s, %s, %s, %s, %s, 'borrador', %s, %s, %s, %s)
RETURNING {CAMPAIGN_COLUMNS}
"""

SAVE_CAMPAIGN_SQL = f"""
UPDATE app.commercial_campaigns_v2 AS c
SET name = %s, starts_on = %s, ends_on = %s, responsible = %s, notes = %s, approval_status = %s,
    submitted_by = %s, submitted_at = %s, approved_by = %s, approved_at = %s,
    rejected_by = %s, rejected_at = %s, cancelled_by = %s, cancelled_at = %s, status_reason = %s,
    updated_by = %s, updated_at = now(), version = c.version + 1
WHERE c.company_id = %s AND c.id = %s AND c.version = %s
RETURNING {CAMPAIGN_COLUMNS}
"""

INSERT_ITEM_SQL = f"""
INSERT INTO app.commercial_campaign_items_v2 AS i
    (campaign_id, company_id, variant_id, product_id, price_list_id, sku_code, barcode, product_name,
     variant_name, normal_gross_price, normal_price_fetched_at, promo_gross_price, clearance_reason,
     reference_stock, reference_stock_fetched_at, notes, legacy_snapshot_id, created_by)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
RETURNING {ITEM_COLUMNS}
"""

SAVE_ITEM_SQL = f"""
UPDATE app.commercial_campaign_items_v2 AS i
SET promo_gross_price = %s, clearance_reason = %s, notes = %s, item_status = %s,
    removed_by = %s, removed_at = %s, updated_by = %s, updated_at = now(), version = i.version + 1
WHERE i.company_id = %s AND i.campaign_id = %s AND i.id = %s AND i.version = %s
RETURNING {ITEM_COLUMNS}
"""

INSERT_HISTORY_SQL = """
INSERT INTO app.commercial_campaign_history_v2
    (campaign_id, item_id, company_id, event, previous_status, new_status, previous_values, new_values,
     reason, actor, actor_role)
VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s)
RETURNING id, occurred_at
"""

READ_SQL = {
    "TABLES_READY_SQL": TABLES_READY_SQL,
    "CAMPAIGNS_RANGE_SQL": CAMPAIGNS_RANGE_SQL,
    "CAMPAIGN_SQL": CAMPAIGN_SQL,
    "ITEMS_SQL": ITEMS_SQL,
    "ITEM_SQL": ITEM_SQL,
    "COVERING_ITEMS_SQL": COVERING_ITEMS_SQL,
    "CAMPAIGN_HISTORY_SQL": CAMPAIGN_HISTORY_SQL,
    "COMPANY_HISTORY_SQL": COMPANY_HISTORY_SQL,
    "RAW_PRICES_SQL": RAW_PRICES_SQL,
    "RAW_VARIANT_SQL": RAW_VARIANT_SQL,
    "RAW_STOCK_SQL": RAW_STOCK_SQL,
    "PRODUCT_SEARCH_SQL": PRODUCT_SEARCH_SQL,
    "LEGACY_SNAPSHOTS_SQL": LEGACY_SNAPSHOTS_SQL,
    "LEGACY_PROMOTIONS_SQL": LEGACY_PROMOTIONS_SQL,
    "LEGACY_IMPORTED_SQL": LEGACY_IMPORTED_SQL,
}

WRITE_SQL = {
    "INSERT_CAMPAIGN_SQL": INSERT_CAMPAIGN_SQL,
    "SAVE_CAMPAIGN_SQL": SAVE_CAMPAIGN_SQL,
    "INSERT_ITEM_SQL": INSERT_ITEM_SQL,
    "SAVE_ITEM_SQL": SAVE_ITEM_SQL,
    "INSERT_HISTORY_SQL": INSERT_HISTORY_SQL,
}


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"no serializable: {type(value).__name__}")


def to_json_text(values: Mapping[str, Any] | None) -> str | None:
    if values is None:
        return None
    return json.dumps(dict(values), default=_json_default, ensure_ascii=False, sort_keys=True)


def _one(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    return rows[0] if rows else None


class PromotionsV2Repository:
    def __init__(self, execute: QueryExecutor) -> None:
        self._execute = execute

    # ---------- lecturas

    def tables_ready(self) -> bool:
        row = _one(self._execute(TABLES_READY_SQL, ()))
        return bool(row and row.get("ready"))

    def price_lists(self, *, company_id: int) -> list[dict[str, Any]]:
        return self._execute(PRICE_LISTS_SQL, (company_id,))

    def campaigns_in_range(self, *, company_id: int, date_from: date, date_to: date) -> list[dict[str, Any]]:
        return self._execute(CAMPAIGNS_RANGE_SQL, (company_id, date_to, date_from))

    def campaign(self, *, company_id: int, campaign_id: int) -> dict[str, Any] | None:
        return _one(self._execute(CAMPAIGN_SQL, (company_id, campaign_id)))

    def items(self, *, company_id: int, campaign_ids: Sequence[int]) -> list[dict[str, Any]]:
        if not campaign_ids:
            return []
        return self._execute(ITEMS_SQL, (company_id, list(campaign_ids)))

    def item(self, *, company_id: int, campaign_id: int, item_id: int) -> dict[str, Any] | None:
        return _one(self._execute(ITEM_SQL, (company_id, campaign_id, item_id)))

    def covering_items(self, *, company_id: int, date_from: date, date_to: date) -> list[dict[str, Any]]:
        return self._execute(COVERING_ITEMS_SQL, (company_id, date_to, date_from))

    def campaign_history(self, *, company_id: int, campaign_id: int) -> list[dict[str, Any]]:
        return self._execute(CAMPAIGN_HISTORY_SQL, (company_id, campaign_id))

    def company_history(
        self, *, company_id: int, since: datetime, until: datetime, limit: int, offset: int
    ) -> list[dict[str, Any]]:
        return self._execute(COMPANY_HISTORY_SQL, (company_id, since, until, limit, offset))

    def raw_prices(
        self, *, company_id: int, variant_ids: Sequence[int], price_list_ids: Sequence[int]
    ) -> list[dict[str, Any]]:
        if not variant_ids or not price_list_ids:
            return []
        return self._execute(RAW_PRICES_SQL, (company_id, list(variant_ids), list(price_list_ids)))

    def raw_variant(self, *, company_id: int, variant_id: int) -> dict[str, Any] | None:
        return _one(self._execute(RAW_VARIANT_SQL, (company_id, variant_id)))

    def raw_stock(self, *, company_id: int, variant_id: int) -> dict[str, Any] | None:
        return _one(self._execute(RAW_STOCK_SQL, (company_id, variant_id)))

    def search_products(self, *, company_id: int, q: str, limit: int) -> list[dict[str, Any]]:
        like = f"%{q}%"
        return self._execute(PRODUCT_SEARCH_SQL, (company_id, like, like, like, like, limit))

    def legacy_snapshots(self, *, company_id: int) -> list[dict[str, Any]]:
        return self._execute(LEGACY_SNAPSHOTS_SQL, (company_id,))

    def legacy_promotions(self, *, company_id: int) -> list[dict[str, Any]]:
        return self._execute(LEGACY_PROMOTIONS_SQL, (company_id,))

    def legacy_imported(self, *, company_id: int) -> dict[int, int]:
        return {
            int(r["legacy_promotion_id"]): int(r["campaign_id"])
            for r in self._execute(LEGACY_IMPORTED_SQL, (company_id,))
        }

    # ---------- escrituras

    def insert_campaign(self, *, company_id: int, values: Mapping[str, Any], actor: str) -> dict[str, Any]:
        row = _one(
            self._execute(
                INSERT_CAMPAIGN_SQL,
                (
                    company_id,
                    values["kind"],
                    values["name"],
                    values["starts_on"],
                    values["ends_on"],
                    values.get("responsible"),
                    values.get("notes"),
                    actor,
                    values.get("legacy_promotion_id"),
                ),
            )
        )
        assert row is not None
        return row

    def save_campaign(
        self, *, company_id: int, campaign_id: int, expected_version: int, values: Mapping[str, Any], actor: str
    ) -> dict[str, Any] | None:
        return _one(
            self._execute(
                SAVE_CAMPAIGN_SQL,
                (
                    values["name"],
                    values["starts_on"],
                    values["ends_on"],
                    values.get("responsible"),
                    values.get("notes"),
                    values["approval_status"],
                    values.get("submitted_by"),
                    values.get("submitted_at"),
                    values.get("approved_by"),
                    values.get("approved_at"),
                    values.get("rejected_by"),
                    values.get("rejected_at"),
                    values.get("cancelled_by"),
                    values.get("cancelled_at"),
                    values.get("status_reason"),
                    actor,
                    company_id,
                    campaign_id,
                    expected_version,
                ),
            )
        )

    def insert_item(self, *, company_id: int, campaign_id: int, values: Mapping[str, Any], actor: str) -> dict[str, Any]:
        row = _one(
            self._execute(
                INSERT_ITEM_SQL,
                (
                    campaign_id,
                    company_id,
                    values["variant_id"],
                    values.get("product_id"),
                    values["price_list_id"],
                    values.get("sku_code"),
                    values.get("barcode"),
                    values.get("product_name"),
                    values.get("variant_name"),
                    values.get("normal_gross_price"),
                    values.get("normal_price_fetched_at"),
                    values["promo_gross_price"],
                    values.get("clearance_reason"),
                    values.get("reference_stock"),
                    values.get("reference_stock_fetched_at"),
                    values.get("notes"),
                    values.get("legacy_snapshot_id"),
                    actor,
                ),
            )
        )
        assert row is not None
        return row

    def save_item(
        self,
        *,
        company_id: int,
        campaign_id: int,
        item_id: int,
        expected_version: int,
        values: Mapping[str, Any],
        actor: str,
    ) -> dict[str, Any] | None:
        return _one(
            self._execute(
                SAVE_ITEM_SQL,
                (
                    values["promo_gross_price"],
                    values.get("clearance_reason"),
                    values.get("notes"),
                    values["item_status"],
                    values.get("removed_by"),
                    values.get("removed_at"),
                    actor,
                    company_id,
                    campaign_id,
                    item_id,
                    expected_version,
                ),
            )
        )

    def insert_history(
        self,
        *,
        company_id: int,
        campaign_id: int,
        item_id: int | None,
        event: str,
        previous_status: str | None,
        new_status: str | None,
        previous_values: Mapping[str, Any] | None,
        new_values: Mapping[str, Any] | None,
        reason: str | None,
        actor: str,
        actor_role: str | None,
    ) -> dict[str, Any]:
        row = _one(
            self._execute(
                INSERT_HISTORY_SQL,
                (
                    campaign_id,
                    item_id,
                    company_id,
                    event,
                    previous_status,
                    new_status,
                    to_json_text(previous_values),
                    to_json_text(new_values),
                    reason,
                    actor,
                    actor_role,
                ),
            )
        )
        assert row is not None
        return row
