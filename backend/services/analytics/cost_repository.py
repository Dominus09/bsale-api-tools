"""SQL puro + fetch de candidatos de costo (executor inyectable)."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import date, datetime
from typing import Any

from backend.services.analytics.cost_models import ReceptionCostCandidate, VariantCostSnapshot
from backend.services.analytics.money import optional_decimal

QueryExecutor = Callable[[str, tuple[Any, ...]], list[dict[str, Any]]]


def build_reception_costs_query(
    *,
    company_id: int,
    variant_ids: Sequence[int],
    on_or_before: date,
    limit: int = 5000,
) -> tuple[str, tuple[Any, ...]]:
    """Recepciones con admission_date::date <= on_or_before para el set de variantes."""
    if int(company_id) <= 0:
        raise ValueError("company_id is required")
    ids = [int(v) for v in variant_ids if v is not None]
    if not ids:
        return (
            """
SELECT h.id, h.variant_id, h.cost_net, h.admission_date, h.reception_id,
       h.reception_detail_id, h.document_number, h.office_id,
       h.iva_amount, h.other_taxes, h.cost_bruto_erp
FROM analytics.cost_reception_history h
WHERE FALSE
LIMIT 0
""".strip(),
            (),
        )
    lim = max(1, min(int(limit), 20000))
    sql = """
SELECT
    h.id,
    h.variant_id,
    h.cost_net,
    h.admission_date,
    h.reception_id,
    h.reception_detail_id,
    h.document_number,
    h.office_id,
    h.iva_amount,
    h.other_taxes,
    h.cost_bruto_erp
FROM analytics.cost_reception_history h
WHERE h.company_id = %s
  AND h.variant_id = ANY(%s)
  AND h.admission_date::date <= %s
  AND h.cost_net IS NOT NULL
ORDER BY h.variant_id ASC, h.admission_date DESC, h.id DESC
LIMIT %s
""".strip()
    return sql, (int(company_id), ids, on_or_before, lim)


def build_variant_cost_snapshots_query(
    *,
    company_id: int,
    variant_ids: Sequence[int],
) -> tuple[str, tuple[Any, ...]]:
    if int(company_id) <= 0:
        raise ValueError("company_id is required")
    ids = [int(v) for v in variant_ids if v is not None]
    if not ids:
        return (
            """
SELECT v.variant_id, v.average_cost_net, v.last_update, v.cost_source
FROM bsale.variant_cost v
WHERE FALSE
LIMIT 0
""".strip(),
            (),
        )
    sql = """
SELECT
    v.variant_id,
    v.average_cost_net,
    v.last_update,
    v.cost_source
FROM bsale.variant_cost v
WHERE v.company_id = %s
  AND v.variant_id = ANY(%s)
""".strip()
    return sql, (int(company_id), ids)


def _as_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def row_to_reception_candidate(row: dict[str, Any]) -> ReceptionCostCandidate:
    cost = optional_decimal(row.get("cost_net"))
    if cost is None:
        raise ValueError("reception candidate without cost_net")
    cost_date = _as_date(row.get("admission_date"))
    if cost_date is None:
        raise ValueError("reception candidate without admission_date")
    return ReceptionCostCandidate(
        id=int(row["id"]),
        variant_id=int(row["variant_id"]),
        cost_net=cost,
        cost_date=cost_date,
        reception_id=int(row["reception_id"]) if row.get("reception_id") is not None else None,
        reception_detail_id=(
            int(row["reception_detail_id"])
            if row.get("reception_detail_id") is not None
            else None
        ),
        document_number=(
            int(row["document_number"]) if row.get("document_number") is not None else None
        ),
        office_id=int(row["office_id"]) if row.get("office_id") is not None else None,
        iva_amount=optional_decimal(row.get("iva_amount")),
        other_taxes=optional_decimal(row.get("other_taxes")),
        cost_bruto_erp=optional_decimal(row.get("cost_bruto_erp")),
    )


def row_to_variant_snapshot(row: dict[str, Any]) -> VariantCostSnapshot:
    return VariantCostSnapshot(
        variant_id=int(row["variant_id"]),
        average_cost_net=optional_decimal(row.get("average_cost_net")),
        last_update=_as_date(row.get("last_update")),
        cost_source=str(row["cost_source"]) if row.get("cost_source") is not None else None,
    )


class CostCandidateRepository:
    """Carga candidatos vía executor inyectable (sin abrir conexiones)."""

    def __init__(self, executor: QueryExecutor) -> None:
        self._executor = executor

    def fetch_reception_candidates(
        self,
        *,
        company_id: int,
        variant_ids: Sequence[int],
        on_or_before: date,
        limit: int = 5000,
    ) -> list[ReceptionCostCandidate]:
        sql, params = build_reception_costs_query(
            company_id=company_id,
            variant_ids=variant_ids,
            on_or_before=on_or_before,
            limit=limit,
        )
        rows = self._executor(sql, params)
        return [row_to_reception_candidate(r) for r in rows]

    def fetch_variant_snapshots(
        self,
        *,
        company_id: int,
        variant_ids: Sequence[int],
    ) -> dict[int, VariantCostSnapshot]:
        sql, params = build_variant_cost_snapshots_query(
            company_id=company_id,
            variant_ids=variant_ids,
        )
        rows = self._executor(sql, params)
        out: dict[int, VariantCostSnapshot] = {}
        for row in rows:
            snap = row_to_variant_snapshot(row)
            out[snap.variant_id] = snap
        return out
