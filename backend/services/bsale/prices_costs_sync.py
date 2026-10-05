"""
Sync de costos y precios Bsale por empresa (ex ``sync_prices_costs.py``).

Dos fases independientes, cada una con su propia transacción y COMMIT:

1. Precios: sólo las listas administradas por la ERP (``managed_price_lists.py``). Se descargan
   sin transacción abierta, se valida cada lista por separado y, en una transacción, se hace
   UPSERT + reconciliación SÓLO de las listas administradas confirmadas completas
   (``reported_count == fetched``, sin errores). Listas administradas degradadas conservan sus
   precios. Listas no administradas no se descargan, no se cuentan y nunca se borran.
2. Costos: descarga por variante y UPSERT. Un fallo aquí no revierte los precios ya confirmados.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from psycopg2.extras import execute_batch

from backend.services.bsale.companies import BsaleCompany
from backend.services.bsale.http_client import BsaleHttpClient
from backend.services.bsale.managed_price_lists import managed_price_lists
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

logger = logging.getLogger(__name__)

PRICE_LIST_STATE_ACTIVE = 0

LIST_OK = "OK"
LIST_ERROR = "ERROR"
LIST_MISSING = "MISSING"
LIST_INACTIVE = "INACTIVE"
LIST_INCONSISTENT = "INCONSISTENT"
LIST_DEGRADED = "DEGRADED"

_UPSERT_COSTS = """
INSERT INTO bsale.variant_cost (company_id, variant_id, average_cost_net, last_update)
VALUES (%s,%s,%s,%s)
ON CONFLICT (company_id, variant_id) DO UPDATE SET
    average_cost_net = EXCLUDED.average_cost_net,
    last_update = EXCLUDED.last_update
"""

_EXISTING_PRICES_BY_LIST = """
SELECT price_list_id, COUNT(*)
FROM bsale.variant_prices
WHERE company_id = %s AND price_list_id = ANY(%s)
GROUP BY price_list_id
"""


def _ids(values: Sequence[int]) -> str:
    return "[" + ",".join(str(v) for v in values) + "]"


@dataclass
class PriceListFetch:
    price_list_id: int
    name: Any = None
    state: Any = None
    rows: list[tuple] = field(default_factory=list)
    fetched: int = 0
    reported_count: int | None = None
    status: str = LIST_OK
    reason: str | None = None


@dataclass
class PricesSnapshot:
    company_id: int
    managed: tuple[int, ...]
    lists: dict[int, PriceListFetch]
    ignored: list[int] = field(default_factory=list)

    @property
    def total_rows(self) -> int:
        return sum(len(pl.rows) for pl in self.lists.values())


def load_company_variant_ids(conn: Any, company_id: int) -> list[int]:
    cur = conn.cursor()
    cur.execute(
        "SELECT bsale_id FROM bsale.variants WHERE company_id = %s ORDER BY bsale_id",
        (company_id,),
    )
    ids = [int(r[0]) for r in cur.fetchall()]
    cur.close()
    return ids


def load_existing_prices_by_list(
    conn: Any, company_id: int, price_list_ids: Sequence[int]
) -> dict[int, int]:
    cur = conn.cursor()
    cur.execute(_EXISTING_PRICES_BY_LIST, (company_id, list(price_list_ids)))
    out = {int(r[0]): int(r[1] or 0) for r in cur.fetchall()}
    cur.close()
    return out


def fetch_company_costs(
    client: BsaleHttpClient, company_id: int, variant_ids: list[int]
) -> list[tuple]:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows: list[tuple] = []
    for variant_id in variant_ids:
        data = client.get_json(f"variants/{variant_id}/costs.json")
        rows.append((company_id, variant_id, data.get("averageCost"), now))
    return rows


def _reported_count(client: Any) -> int | None:
    stats = getattr(client, "last_pagination", None)
    if not isinstance(stats, dict):
        return None
    count = stats.get("reported_count")
    return count if isinstance(count, int) else None


def _pagination_diag(client: Any) -> str:
    stats = getattr(client, "last_pagination", None)
    if not isinstance(stats, dict):
        return ""
    return f" pages={stats.get('pages')} stop={stats.get('stop_reason')}"


def _log_list(company_id: int, pl: PriceListFetch, extra: str = "") -> None:
    ok = pl.status == LIST_OK
    logger.log(
        logging.INFO if ok else logging.WARNING,
        "company_id=%s price_list_id=%s status=%s fetched=%s name=%s state=%s reported_count=%s%s%s",
        company_id,
        pl.price_list_id,
        "OK" if ok else "ERROR",
        pl.fetched,
        pl.name,
        pl.state,
        pl.reported_count,
        extra,
        f" reason={pl.reason}" if pl.reason else "",
    )


def _fetch_price_list(client: BsaleHttpClient, company_id: int, pl: PriceListFetch) -> None:
    """Descarga y valida una lista; nunca lanza: deja el resultado en ``pl.status``/``pl.reason``."""
    try:
        details = client.fetch_all_items(f"price_lists/{pl.price_list_id}/details.json")
        pl.reported_count = _reported_count(client)
        rows: list[tuple] = []
        for d in details:
            try:
                variant_id = int(d["variant"]["id"])
            except (KeyError, TypeError, ValueError) as exc:
                raise CompanySyncError(
                    f"detalle de price_list_id={pl.price_list_id} sin variant.id"
                ) from exc
            rows.append(
                (
                    company_id,
                    variant_id,
                    pl.price_list_id,
                    d.get("variantValue"),
                    d.get("variantValueWithTaxes"),
                )
            )
        pl.fetched = len(rows)
    except Exception as exc:
        pl.status = LIST_ERROR
        pl.reason = f"{type(exc).__name__}: {exc}"
        _log_list(company_id, pl)
        return

    if pl.reported_count is None:
        pl.status, pl.reason = LIST_INCONSISTENT, "Bsale no informó count"
    elif pl.reported_count != pl.fetched:
        pl.status = LIST_INCONSISTENT
        pl.reason = f"reported_count={pl.reported_count} != fetched={pl.fetched}"
    elif len({r[1] for r in rows}) != len(rows):
        pl.status, pl.reason = LIST_INCONSISTENT, "variant_id duplicado en la lista"
    if pl.status == LIST_OK:
        pl.rows = rows
    _log_list(company_id, pl, _pagination_diag(client))


def fetch_company_prices(
    client: BsaleHttpClient,
    company_id: int,
    managed: Sequence[int] | None = None,
) -> PricesSnapshot:
    """
    Descarga sólo las listas administradas de la empresa.

    Falla la empresa sólo si ``price_lists.json`` no se puede obtener o es inválido. Una lista
    administrada ausente, inactiva o con error queda registrada en esa lista; las listas no
    administradas se ignoran sin descargarlas.
    """
    managed_ids = tuple(managed) if managed is not None else managed_price_lists(company_id)
    logger.info("company_id=%s managed_price_lists=%s", company_id, _ids(managed_ids))

    endpoint: dict[int, dict[str, Any]] = {}
    for raw in client.fetch_all_items("price_lists.json"):
        try:
            price_list_id = int(raw["id"])
        except (KeyError, TypeError, ValueError) as exc:
            raise CompanySyncError("price_list sin id válido") from exc
        if price_list_id in endpoint:
            raise CompanySyncError(f"price_list_id={price_list_id} duplicado en price_lists.json")
        endpoint[price_list_id] = raw

    ignored = sorted(pid for pid in endpoint if pid not in managed_ids)
    if ignored:
        logger.info("company_id=%s ignored_price_lists=%s (no administradas)", company_id, _ids(ignored))

    lists: dict[int, PriceListFetch] = {}
    for price_list_id in managed_ids:
        raw = endpoint.get(price_list_id)
        if raw is None:
            pl = PriceListFetch(
                price_list_id=price_list_id,
                status=LIST_MISSING,
                reason="lista administrada ausente de price_lists.json",
            )
            _log_list(company_id, pl)
        else:
            pl = PriceListFetch(price_list_id=price_list_id, name=raw.get("name"), state=raw.get("state"))
            if pl.state != PRICE_LIST_STATE_ACTIVE:
                pl.status = LIST_INACTIVE
                pl.reason = f"lista administrada inactiva en Bsale (state={pl.state})"
                _log_list(company_id, pl)
            else:
                _fetch_price_list(client, company_id, pl)
        lists[price_list_id] = pl
    return PricesSnapshot(company_id=company_id, managed=managed_ids, lists=lists, ignored=ignored)


def classify_price_lists(
    snapshot: PricesSnapshot, existing_by_list: Mapping[int, int]
) -> tuple[list[int], dict[int, str]]:
    """
    Devuelve (listas administradas confirmadas completas, {lista administrada degradada: motivo}).

    Degradadas (sin UPSERT ni DELETE): ausente o inactiva en Bsale, error/inconsistencia de
    descarga, o ``fetched=0`` con precios existentes. Sólo se evalúan listas administradas.
    """
    complete: list[int] = []
    degraded: dict[int, str] = {}
    for pl in snapshot.lists.values():
        existing = existing_by_list.get(pl.price_list_id, 0)
        if pl.status != LIST_OK:
            degraded[pl.price_list_id] = pl.reason or pl.status
            continue
        if pl.fetched == 0 and existing > 0:
            pl.status = LIST_DEGRADED
            pl.reason = f"lista administrada vacía con {existing} precios existentes"
            pl.rows = []
            degraded[pl.price_list_id] = pl.reason
            continue
        complete.append(pl.price_list_id)
    for price_list_id, reason in degraded.items():
        logger.warning(
            "company_id=%s price_list_id=%s status=DEGRADED existing=%s reason=%s",
            snapshot.company_id,
            price_list_id,
            existing_by_list.get(price_list_id, 0),
            reason,
        )
    return complete, degraded


def persist_company_prices(conn: Any, snapshot: PricesSnapshot) -> dict[str, Any]:
    company_id = snapshot.company_id
    try:
        existing_by_list = load_existing_prices_by_list(conn, company_id, snapshot.managed)
        complete, degraded = classify_price_lists(snapshot, existing_by_list)
        rows = [r for pid in complete for r in snapshot.lists[pid].rows]
        if complete:
            res = upsert_and_reconcile_snapshot(
                conn,
                VARIANT_PRICES_SPEC,
                company_id,
                rows,
                scope_column="price_list_id",
                scope_values=complete,
            )
        else:
            res = {"upserted": 0, "deleted": 0, "skipped": "sin listas confirmadas completas"}
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    logger.info(
        "company_id=%s lists_expected=%s lists_fetched=%s prices_by_list=%s snapshot_total=%s "
        "lists_degraded=%s",
        company_id,
        _ids(snapshot.managed),
        _ids(complete),
        {pid: snapshot.lists[pid].fetched for pid in complete},
        len(rows),
        degraded,
    )
    return {
        "upserted": res["upserted"],
        "deleted": res["deleted"],
        "complete_lists": complete,
        "degraded_lists": degraded,
        "reconcile": {k: v for k, v in res.items() if k not in ("upserted", "deleted")},
    }


def persist_company_costs(conn: Any, cost_rows: list[tuple]) -> int:
    try:
        cur = conn.cursor()
        if cost_rows:
            execute_batch(cur, _UPSERT_COSTS, cost_rows, page_size=500)
        cur.close()
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return len(cost_rows)


def _run_prices(
    company: BsaleCompany,
    client: BsaleHttpClient,
    connection_factory: ConnectionFactory,
    managed: Sequence[int] | None,
) -> tuple[PricesSnapshot, dict[str, Any]]:
    snapshot = fetch_company_prices(client, company.company_id, managed)
    conn = connection_factory()
    try:
        return snapshot, persist_company_prices(conn, snapshot)
    finally:
        conn.close()


def _run_costs(
    company: BsaleCompany, client: BsaleHttpClient, connection_factory: ConnectionFactory
) -> tuple[int, int]:
    conn = connection_factory()
    try:
        variant_ids = load_company_variant_ids(conn, company.company_id)
        conn.rollback()
    finally:
        conn.close()
    cost_rows = fetch_company_costs(client, company.company_id, variant_ids)
    conn = connection_factory()
    try:
        return len(cost_rows), persist_company_costs(conn, cost_rows)
    finally:
        conn.close()


def sync_company_prices_costs(
    company: BsaleCompany,
    *,
    client_factory: ClientFactory = default_client_factory,
    connection_factory: ConnectionFactory = default_connection_factory,
    managed_lists: Mapping[int, Sequence[int]] | None = None,
) -> dict[str, Any]:
    """``managed_lists`` sólo para tests; en producción se usa ``MANAGED_PRICE_LISTS``."""

    def _run() -> dict[str, Any]:
        client = client_factory(company)
        errors: list[str] = []
        out: dict[str, Any] = {
            "fetched": {"costs": 0, "prices": 0},
            "upserted": {"costs": 0, "prices": 0},
            "deleted": {"prices": 0},
            "reconcile": {"prices": {}},
            "price_lists": {},
        }

        try:
            managed = None
            if managed_lists is not None:
                managed = managed_price_lists(company.company_id, managed_lists)
            snapshot, prices = _run_prices(company, client, connection_factory, managed)
            out["managed_price_lists"] = list(snapshot.managed)
            out["ignored_price_lists"] = snapshot.ignored
            out["fetched"]["prices"] = snapshot.total_rows
            out["upserted"]["prices"] = prices["upserted"]
            out["deleted"]["prices"] = prices["deleted"]
            out["reconcile"]["prices"] = prices["reconcile"]
            out["price_lists"] = {
                pid: {
                    "name": pl.name,
                    "state": pl.state,
                    "fetched": pl.fetched,
                    "reported_count": pl.reported_count,
                    "status": pl.status,
                    "reason": pl.reason,
                }
                for pid, pl in snapshot.lists.items()
            }
            out["degraded_price_lists"] = prices["degraded_lists"]
            if prices["degraded_lists"]:
                errors.append(
                    "prices: listas administradas degradadas (precios conservados) "
                    + ", ".join(f"{pid}: {r}" for pid, r in prices["degraded_lists"].items())
                )
        except Exception as exc:
            out["prices_error"] = f"{type(exc).__name__}: {exc}"
            errors.append(f"prices: {out['prices_error']}")
            logger.error(
                "[BSALE_SYNC] phase=%s company_id=%s prices FAILED error=%s",
                PHASE,
                company.company_id,
                out["prices_error"],
            )

        try:
            fetched_costs, upserted_costs = _run_costs(company, client, connection_factory)
            out["fetched"]["costs"] = fetched_costs
            out["upserted"]["costs"] = upserted_costs
        except Exception as exc:
            out["costs_error"] = f"{type(exc).__name__}: {exc}"
            errors.append(f"costs: {out['costs_error']}")
            logger.error(
                "[BSALE_SYNC] phase=%s company_id=%s costs FAILED error=%s",
                PHASE,
                company.company_id,
                out["costs_error"],
            )

        if errors:
            out["error"] = "; ".join(errors)
        return out

    return run_company_phase(PHASE, company, _run)
