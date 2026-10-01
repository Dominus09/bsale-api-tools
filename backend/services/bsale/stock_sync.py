"""Sync de stock Bsale por empresa (ex ``sync_stock.py``): snapshot completo → transacción → reconciliación."""

from __future__ import annotations

from typing import Any

from backend.services.bsale.companies import BsaleCompany
from backend.services.bsale.http_client import BsaleHttpClient
from backend.services.bsale.snapshot_reconcile import STOCKS_SPEC, upsert_and_reconcile_snapshot
from backend.services.bsale.sync_common import (
    ClientFactory,
    CompanySyncError,
    ConnectionFactory,
    default_client_factory,
    default_connection_factory,
    run_company_phase,
)

PHASE = "stock"


def fetch_company_stock(client: BsaleHttpClient, company_id: int) -> list[tuple]:
    """Descarga completa; cualquier error HTTP se propaga y no se toca la BD."""
    rows: list[tuple] = []
    for s in client.fetch_all_items("stocks.json"):
        try:
            rows.append(
                (
                    company_id,
                    int(s["variant"]["id"]),
                    int(s["office"]["id"]),
                    s.get("quantityAvailable"),
                    s.get("quantityReserved"),
                )
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise CompanySyncError(f"stock sin variant/office válido (company_id={company_id})") from exc
    return rows


def persist_company_stock(conn: Any, company_id: int, rows: list[tuple]) -> dict[str, int]:
    try:
        result = upsert_and_reconcile_snapshot(conn, STOCKS_SPEC, company_id, rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return result


def sync_company_stock(
    company: BsaleCompany,
    *,
    client_factory: ClientFactory = default_client_factory,
    connection_factory: ConnectionFactory = default_connection_factory,
) -> dict[str, Any]:
    def _run() -> dict[str, Any]:
        rows = fetch_company_stock(client_factory(company), company.company_id)
        conn = connection_factory()
        try:
            res = persist_company_stock(conn, company.company_id, rows)
        finally:
            conn.close()
        return {
            "fetched": len(rows),
            "upserted": res["upserted"],
            "deleted": res["deleted"],
            "reconcile": {k: v for k, v in res.items() if k not in ("upserted", "deleted")},
        }

    return run_company_phase(PHASE, company, _run)
