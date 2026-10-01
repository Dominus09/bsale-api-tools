"""
Sync de catálogo Bsale por empresa (ex ``sync_catalog.py``).

Fase HTTP completa (taxes, product_types, price_lists, offices, products, product_taxes,
variants) sin conexión DB abierta; luego una única transacción por empresa.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from psycopg2.extras import execute_batch

from backend.services.bsale.companies import BsaleCompany
from backend.services.bsale.http_client import BsaleHttpClient, BsaleHttpError
from backend.services.bsale.sync_common import (
    ClientFactory,
    CompanySyncError,
    ConnectionFactory,
    count_company_rows,
    default_client_factory,
    default_connection_factory,
    run_company_phase,
)

PHASE = "catalog"


@dataclass
class CatalogSnapshot:
    company_id: int
    taxes: list[tuple] = field(default_factory=list)
    product_types: list[tuple] = field(default_factory=list)
    price_lists: list[tuple] = field(default_factory=list)
    offices: list[tuple] = field(default_factory=list)
    products: list[tuple] = field(default_factory=list)
    variants: list[tuple] = field(default_factory=list)
    products_without_tax_href: int = 0

    def counts(self) -> dict[str, int]:
        return {
            "taxes": len(self.taxes),
            "product_types": len(self.product_types),
            "price_lists": len(self.price_lists),
            "offices": len(self.offices),
            "products": len(self.products),
            "variants": len(self.variants),
        }


def _int_id(obj: Any, *, what: str) -> int:
    try:
        return int(obj["id"])
    except (KeyError, TypeError, ValueError) as exc:
        raise CompanySyncError(f"{what} sin id válido") from exc


def resolve_product_taxes(
    client: BsaleHttpClient,
    product: dict[str, Any],
    tax_map: dict[int, dict[str, Any]],
) -> tuple[list[int], list[str], float] | None:
    """
    Devuelve (tax_ids, tax_names, tax_factor) o ``None`` si el producto no expone ``product_taxes``.

    Cualquier error HTTP (tras retries) o tax_id fuera de ``tax_map`` lanza ``CompanySyncError``:
    nunca se inventa ``tax_factor=1.0`` ni se aceptan impuestos parcialmente resueltos.
    """
    product_id = _int_id(product, what="product")
    ref = product.get("product_taxes")
    href = ref.get("href") if isinstance(ref, dict) else None
    if not href:
        return None
    try:
        items = client.fetch_all_items(href)
    except BsaleHttpError as exc:
        raise CompanySyncError(
            f"product_taxes no resuelto product_id={product_id} endpoint={exc.endpoint}: {exc}"
        ) from exc

    tax_ids: list[int] = []
    tax_names: list[str] = []
    total = 0.0
    for item in items:
        try:
            tax_id = int(item["tax"]["id"])
        except (KeyError, TypeError, ValueError) as exc:
            raise CompanySyncError(
                f"product_taxes sin tax.id product_id={product_id}"
            ) from exc
        tax = tax_map.get(tax_id)
        if tax is None:
            raise CompanySyncError(
                f"tax_id={tax_id} de product_id={product_id} no existe en taxes de la empresa"
            )
        tax_ids.append(tax_id)
        tax_names.append(tax["name"])
        total += tax["percentage"]
    return tax_ids, tax_names, round(1 + total / 100, 3)


def fetch_company_catalog(client: BsaleHttpClient, company_id: int) -> CatalogSnapshot:
    """Sólo HTTP + validación. No toca la base de datos."""
    snap = CatalogSnapshot(company_id=company_id)

    tax_map: dict[int, dict[str, Any]] = {}
    for t in client.fetch_all_items("taxes.json"):
        tax_id = _int_id(t, what="tax")
        try:
            pct = float(t["percentage"])
        except (KeyError, TypeError, ValueError) as exc:
            raise CompanySyncError(f"tax_id={tax_id} sin percentage válido") from exc
        tax_map[tax_id] = {"name": t.get("name"), "percentage": pct}
        snap.taxes.append((company_id, tax_id, t.get("name"), pct))

    for pt in client.fetch_all_items("product_types.json"):
        snap.product_types.append(
            (company_id, _int_id(pt, what="product_type"), pt.get("name"), pt.get("state"))
        )

    for pl in client.fetch_all_items("price_lists.json"):
        snap.price_lists.append(
            (company_id, _int_id(pl, what="price_list"), pl.get("name"), pl.get("state"))
        )

    for o in client.fetch_all_items("offices.json"):
        snap.offices.append(
            (company_id, _int_id(o, what="office"), o.get("name"), o.get("state"))
        )

    for p in client.fetch_all_items("products.json"):
        product_id = _int_id(p, what="product")
        resolved = resolve_product_taxes(client, p, tax_map)
        if resolved is None:
            snap.products_without_tax_href += 1
            tax_ids, tax_names, tax_factor = [], [], 1.0
        else:
            tax_ids, tax_names, tax_factor = resolved
        ptype = p.get("product_type")
        ptype_id = int(ptype["id"]) if isinstance(ptype, dict) and ptype.get("id") is not None else None
        snap.products.append(
            (
                company_id,
                product_id,
                p.get("name"),
                ptype_id,
                json.dumps(tax_ids),
                json.dumps(tax_names),
                tax_factor,
            )
        )

    for v in client.fetch_all_items("variants.json"):
        variant_id = _int_id(v, what="variant")
        product = v.get("product")
        if not isinstance(product, dict):
            raise CompanySyncError(f"variant_id={variant_id} sin product")
        snap.variants.append(
            (
                company_id,
                variant_id,
                _int_id(product, what=f"product de variant_id={variant_id}"),
                v.get("code"),
                v.get("barCode"),
                v.get("description"),
            )
        )

    return snap


_UPSERT_TAXES = """
INSERT INTO bsale.taxes (company_id, bsale_id, name, percentage)
VALUES (%s,%s,%s,%s)
ON CONFLICT (company_id, bsale_id) DO UPDATE SET
    name = EXCLUDED.name,
    percentage = EXCLUDED.percentage
"""

_UPSERT_PRODUCT_TYPES = """
INSERT INTO bsale.product_types (company_id, bsale_id, name, state)
VALUES (%s,%s,%s,%s)
ON CONFLICT (company_id, bsale_id) DO UPDATE SET
    name = EXCLUDED.name,
    state = EXCLUDED.state
"""

_UPSERT_PRICE_LISTS = """
INSERT INTO bsale.price_lists (company_id, bsale_id, name, state)
VALUES (%s,%s,%s,%s)
ON CONFLICT (company_id, bsale_id) DO UPDATE SET
    name = EXCLUDED.name,
    state = EXCLUDED.state
"""

_UPSERT_OFFICES = """
INSERT INTO bsale.offices (company_id, bsale_id, name, state)
VALUES (%s,%s,%s,%s)
ON CONFLICT (company_id, bsale_id) DO UPDATE SET
    name = EXCLUDED.name,
    state = EXCLUDED.state
"""

_UPSERT_PRODUCTS = """
INSERT INTO bsale.products
    (company_id, bsale_id, name, product_type_id, tax_ids_json, tax_names_json, tax_factor)
VALUES (%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (company_id, bsale_id) DO UPDATE SET
    name = EXCLUDED.name,
    product_type_id = EXCLUDED.product_type_id,
    tax_ids_json = EXCLUDED.tax_ids_json,
    tax_names_json = EXCLUDED.tax_names_json,
    tax_factor = EXCLUDED.tax_factor
"""

_UPSERT_VARIANTS = """
INSERT INTO bsale.variants (company_id, bsale_id, product_id, code, bar_code, description)
VALUES (%s,%s,%s,%s,%s,%s)
ON CONFLICT (company_id, bsale_id) DO UPDATE SET
    product_id = EXCLUDED.product_id,
    code = EXCLUDED.code,
    bar_code = EXCLUDED.bar_code,
    description = EXCLUDED.description
"""


def persist_company_catalog(conn: Any, snap: CatalogSnapshot) -> dict[str, int]:
    """Una transacción: COMMIT sólo si todas las entidades se persistieron; si no, ROLLBACK."""
    try:
        existing_products = count_company_rows(conn, "bsale.products", snap.company_id)
        existing_variants = count_company_rows(conn, "bsale.variants", snap.company_id)
        if not snap.products and existing_products > 0:
            raise CompanySyncError(
                f"products=0 desde Bsale pero hay {existing_products} en BD (company_id={snap.company_id})"
            )
        if not snap.variants and existing_variants > 0:
            raise CompanySyncError(
                f"variants=0 desde Bsale pero hay {existing_variants} en BD (company_id={snap.company_id})"
            )
        if any(row[0] != snap.company_id for row in snap.variants):
            raise CompanySyncError("variant con company_id distinto al de la empresa sincronizada")

        cur = conn.cursor()
        for sql, rows in (
            (_UPSERT_TAXES, snap.taxes),
            (_UPSERT_PRODUCT_TYPES, snap.product_types),
            (_UPSERT_PRICE_LISTS, snap.price_lists),
            (_UPSERT_OFFICES, snap.offices),
            (_UPSERT_PRODUCTS, snap.products),
            (_UPSERT_VARIANTS, snap.variants),
        ):
            if rows:
                execute_batch(cur, sql, rows, page_size=500)
        cur.close()
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return snap.counts()


def sync_company_catalog(
    company: BsaleCompany,
    *,
    client_factory: ClientFactory = default_client_factory,
    connection_factory: ConnectionFactory = default_connection_factory,
) -> dict[str, Any]:
    def _run() -> dict[str, Any]:
        snap = fetch_company_catalog(client_factory(company), company.company_id)
        conn = connection_factory()
        try:
            upserted = persist_company_catalog(conn, snap)
        finally:
            conn.close()
        return {
            "fetched": snap.counts(),
            "upserted": upserted,
            "products_without_tax_href": snap.products_without_tax_href,
        }

    return run_company_phase(PHASE, company, _run)
