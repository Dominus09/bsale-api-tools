"""
Sync de costos y precios Bsale por empresa (ex ``sync_prices_costs.py``).

Descarga completa (costos por variante + detalle de todas las listas) sin transacción abierta;
luego una transacción: UPSERT costos, UPSERT precios y reconciliación de precios obsoletos.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from psycopg2.extras import execute_batch

from backend.services.bsale.companies import BsaleCompany
from backend.services.bsale.http_client import BsaleHttpClient
from backend.services.bsale.snapshot_reconcile import (
    VARIANT_PRICES_SPEC,
    upsert_and_reconcile_snapshot,
)
from backend.services.bsale.sync_common import (
    ClientFactory,
    CompanySyncError,
    ConnectionFactory,
    default_client_factory,
    default_connection_factory,
    run_company_phase,
)

PHASE = "prices_costs"

_UPSERT_COSTS = """
INSERT INTO bsale.variant_cost (company_id, variant_id, average_cost_net, last_update)
VALUES (%s,%s,%s,%s)
ON CONFLICT (company_id, variant_id) DO UPDATE SET
    average_cost_net = EXCLUDED.average_cost_net,
    last_update = EXCLUDED.last_update
"""


def load_company_variant_ids(conn: Any, company_id: int) -> list[int]:
    cur = conn.cursor()
    cur.execute(
        "SELECT bsale_id FROM bsale.variants WHERE company_id = %s ORDER BY bsale_id",
        (company_id,),
    )
    ids = [int(r[0]) for r in cur.fetchall()]
    cur.close()
    return ids


def fetch_company_costs(
    client: BsaleHttpClient, company_id: int, variant_ids: list[int]
) -> list[tuple]:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows: list[tuple] = []
    for variant_id in variant_ids:
        data = client.get_json(f"variants/{variant_id}/costs.json")
        rows.append((company_id, variant_id, data.get("averageCost"), now))
    return rows


def fetch_company_prices(client: BsaleHttpClient, company_id: int) -> list[tuple]:
    rows: list[tuple] = []
    for pl in client.fetch_all_items("price_lists.json"):
        try:
            price_list_id = int(pl["id"])
        except (KeyError, TypeError, ValueError) as exc:
            raise CompanySyncError("price_list sin id válido") from exc
        for d in client.fetch_all_items(f"price_lists/{price_list_id}/details.json"):
            try:
                variant_id = int(d["variant"]["id"])
            except (KeyError, TypeError, ValueError) as exc:
                raise CompanySyncError(
                    f"detalle de price_list_id={price_list_id} sin variant.id"
                ) from exc
            rows.append(
                (
                    company_id,
                    variant_id,
                    price_list_id,
                    d.get("variantValue"),
                    d.get("variantValueWithTaxes"),
                )
            )
    return rows


def persist_company_prices_costs(
    conn: Any, company_id: int, cost_rows: list[tuple], price_rows: list[tuple]
) -> dict[str, int]:
    try:
        cur = conn.cursor()
        if cost_rows:
            execute_batch(cur, _UPSERT_COSTS, cost_rows, page_size=500)
        cur.close()
        prices = upsert_and_reconcile_snapshot(conn, VARIANT_PRICES_SPEC, company_id, price_rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {
        "costs_upserted": len(cost_rows),
        "prices_upserted": prices["upserted"],
        "prices_deleted": prices["deleted"],
        "prices_reconcile": {k: v for k, v in prices.items() if k not in ("upserted", "deleted")},
    }


def sync_company_prices_costs(
    company: BsaleCompany,
    *,
    client_factory: ClientFactory = default_client_factory,
    connection_factory: ConnectionFactory = default_connection_factory,
) -> dict[str, Any]:
    def _run() -> dict[str, Any]:
        conn = connection_factory()
        try:
            variant_ids = load_company_variant_ids(conn, company.company_id)
            conn.rollback()
        finally:
            conn.close()

        client = client_factory(company)
        cost_rows = fetch_company_costs(client, company.company_id, variant_ids)
        price_rows = fetch_company_prices(client, company.company_id)

        conn = connection_factory()
        try:
            res = persist_company_prices_costs(conn, company.company_id, cost_rows, price_rows)
        finally:
            conn.close()
        return {
            "fetched": {"costs": len(cost_rows), "prices": len(price_rows)},
            "upserted": {"costs": res["costs_upserted"], "prices": res["prices_upserted"]},
            "deleted": {"prices": res["prices_deleted"]},
            "reconcile": {"prices": res["prices_reconcile"]},
        }

    return run_company_phase(PHASE, company, _run)
