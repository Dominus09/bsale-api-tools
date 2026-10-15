"""Consultas read-only de Control de Costos V2 sobre ``bsale_raw``.

Reglas: toda consulta filtra por company_id; nunca se leen tablas legacy; nunca se seleccionan
``payload``, ``note`` ni ``user`` de las recepciones (contienen texto libre y datos personales).
Única excepción: ``PACK_DETAILS_SQL`` extrae solo la clave ``pack_details`` del payload de productos
pack (classification = 3), sin texto libre ni datos personales.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Callable

from backend.services.analytics.validate_distribuidora_source import assert_sql_is_read_only
from backend.services.analytics_v2.contracts import MovementClass
from backend.services.analytics_v2.movement_classification import (
    comparable_sql_list,
    movement_class_sql,
)

QueryExecutor = Callable[[str, tuple[Any, ...]], list[dict[str, Any]]]
logger = logging.getLogger(__name__)

_MOVEMENT_CASE = movement_class_sql("r.document", "r.internal_dispatch_id")
_COMPARABLE = comparable_sql_list()
_TRANSFER = MovementClass.TRANSFERENCIA_INTERNA.value
_UNDETERMINED = MovementClass.NO_DETERMINADO.value

# Líneas vigentes de recepciones vigentes desde el inicio del historial, ya clasificadas.
_LINES_CTE = f"""
recs AS (
    SELECT
        r.company_id,
        r.bsale_id,
        r.admission_date,
        r.office_id,
        r.document,
        r.document_number,
        r.internal_dispatch_id,
        r.details_complete,
        {_MOVEMENT_CASE} AS movement_class
    FROM bsale_raw.stock_receptions r
    WHERE r.company_id = %s
      AND r.missing_since IS NULL
      AND r.admission_date >= %s
),
lines AS (
    SELECT
        d.variant_id,
        d.reception_id,
        d.bsale_id AS detail_id,
        d.quantity,
        d.cost,
        r.admission_date,
        r.office_id,
        r.document,
        r.document_number,
        r.internal_dispatch_id,
        r.details_complete,
        r.movement_class
    FROM recs r
    JOIN bsale_raw.stock_reception_details d
      ON d.company_id = r.company_id AND d.reception_id = r.bsale_id
    WHERE d.missing_since IS NULL
      {{variant_filter}}
),
flagged AS (
    SELECT
        l.*,
        (l.movement_class IN ({_COMPARABLE})
            AND l.cost > 0 AND l.quantity > 0 AND l.details_complete IS TRUE) AS comparable
    FROM lines l
)"""

# Modificaciones de líneas detectadas por la bitácora RAW (event MODIFIED).
_MODIFIED_CTE = """
modified AS (
    SELECT
        cl.reception_id,
        x.detail_id,
        bool_or(x.kind = 'cost') AS cost_modified,
        bool_or(x.kind = 'quantity') AS quantity_modified,
        bool_or(x.kind = 'variant') AS variant_modified
    FROM bsale_raw.stock_reception_change_log cl
    CROSS JOIN LATERAL (
        SELECT unnest(cl.cost_changed_detail_ids) AS detail_id, 'cost' AS kind
        UNION ALL
        SELECT unnest(cl.quantity_changed_detail_ids), 'quantity'
        UNION ALL
        SELECT unnest(cl.variant_changed_detail_ids), 'variant'
    ) x
    WHERE cl.company_id = %s AND cl.event_kind = 'MODIFIED'
    GROUP BY cl.reception_id, x.detail_id
)"""

_LINE_COLUMNS = (
    "reception_id",
    "detail_id",
    "admission_date",
    "office_id",
    "document",
    "document_number",
    "movement_class",
    "cost",
    "quantity",
    "details_complete",
)


def _prefixed(alias: str, prefix: str) -> str:
    return ",\n    ".join(f"{alias}.{col} AS {prefix}{col}" for col in _LINE_COLUMNS)


VARIANT_FACTS_SQL = f"""
WITH {_LINES_CTE.replace("{variant_filter}", "")},
ranked AS (
    SELECT
        f.*,
        row_number() OVER (
            PARTITION BY f.variant_id
            ORDER BY f.admission_date DESC, f.reception_id DESC, f.detail_id DESC
        ) AS rn_any,
        row_number() OVER (
            PARTITION BY f.variant_id, f.comparable
            ORDER BY f.admission_date DESC, f.reception_id DESC, f.detail_id DESC
        ) AS rn_part
    FROM flagged f
),
agg AS (
    SELECT
        variant_id,
        count(*) AS lines_total,
        count(*) FILTER (WHERE comparable) AS lines_comparable,
        count(DISTINCT reception_id) AS receptions_total,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY quantity::double precision)
            FILTER (WHERE comparable) AS median_comparable_quantity,
        min(admission_date) AS first_admission_date
    FROM flagged
    GROUP BY variant_id
),
{_MODIFIED_CTE.strip()}
SELECT
    v.bsale_id AS variant_id,
    v.code,
    v.bar_code,
    v.description AS variant_name,
    v.state AS variant_state,
    p.bsale_id AS product_id,
    p.name AS product_name,
    p.state AS product_state,
    p.classification AS product_classification,
    p.product_type_id,
    pt.name AS product_type_name,
    ptx.tax_ids,
    (ptx.missing_since IS NOT NULL) AS tax_config_stale,
    vc.average_cost,
    vc.api_fetched_at AS average_cost_fetched_at,
    {_prefixed("la", "last_")},
    {_prefixed("lc", "lc_")},
    {_prefixed("pc", "pc_")},
    coalesce(lm.cost_modified, false) AS lc_cost_modified,
    coalesce(lm.quantity_modified, false) AS lc_quantity_modified,
    coalesce(lm.variant_modified, false) AS lc_variant_modified,
    coalesce(a.lines_total, 0) AS lines_total,
    coalesce(a.lines_comparable, 0) AS lines_comparable,
    coalesce(a.receptions_total, 0) AS receptions_total,
    a.median_comparable_quantity,
    a.first_admission_date
FROM bsale_raw.variants v
LEFT JOIN bsale_raw.products p
  ON p.company_id = v.company_id AND p.bsale_id = v.product_id
LEFT JOIN bsale_raw.product_types pt
  ON pt.company_id = p.company_id AND pt.bsale_id = p.product_type_id
LEFT JOIN bsale_raw.product_taxes ptx
  ON ptx.company_id = p.company_id AND ptx.product_id = p.bsale_id
LEFT JOIN bsale_raw.variant_costs vc
  ON vc.company_id = v.company_id AND vc.variant_id = v.bsale_id
LEFT JOIN ranked la ON la.variant_id = v.bsale_id AND la.rn_any = 1
LEFT JOIN ranked lc ON lc.variant_id = v.bsale_id AND lc.comparable AND lc.rn_part = 1
LEFT JOIN ranked pc ON pc.variant_id = v.bsale_id AND pc.comparable AND pc.rn_part = 2
LEFT JOIN modified lm ON lm.reception_id = lc.reception_id AND lm.detail_id = lc.detail_id
LEFT JOIN agg a ON a.variant_id = v.bsale_id
WHERE v.company_id = %s
  AND v.missing_since IS NULL
""".strip()

VARIANT_LINES_SQL = f"""
WITH {_LINES_CTE.replace("{variant_filter}", "AND d.variant_id = %s")},
{_MODIFIED_CTE.strip()}
SELECT
    f.reception_id,
    f.detail_id,
    f.admission_date,
    f.office_id,
    f.document,
    f.document_number,
    f.internal_dispatch_id,
    f.movement_class,
    f.cost,
    f.quantity,
    f.details_complete,
    f.comparable,
    coalesce(m.cost_modified, false) AS cost_modified,
    coalesce(m.quantity_modified, false) AS quantity_modified,
    coalesce(m.variant_modified, false) AS variant_modified
FROM flagged f
LEFT JOIN modified m ON m.reception_id = f.reception_id AND m.detail_id = f.detail_id
ORDER BY f.admission_date ASC, f.reception_id ASC, f.detail_id ASC
""".strip()

ALERT_CANDIDATES_SQL = f"""
WITH {_LINES_CTE.replace("{variant_filter}", "")},
sequenced AS (
    SELECT
        f.*,
        CASE WHEN f.comparable THEN lag(f.cost) OVER (
            PARTITION BY f.variant_id, f.comparable
            ORDER BY f.admission_date, f.reception_id, f.detail_id
        ) END AS previous_comparable_cost
    FROM flagged f
),
agg AS (
    SELECT
        variant_id,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY quantity::double precision)
            FILTER (WHERE comparable) AS median_comparable_quantity
    FROM flagged
    GROUP BY variant_id
),
{_MODIFIED_CTE.strip()}
SELECT
    s.variant_id,
    v.code,
    v.bar_code,
    v.description AS variant_name,
    p.name AS product_name,
    s.reception_id,
    s.detail_id,
    s.admission_date,
    s.office_id,
    s.document,
    s.document_number,
    s.movement_class,
    s.cost,
    s.quantity,
    s.details_complete,
    s.previous_comparable_cost,
    a.median_comparable_quantity,
    coalesce(m.cost_modified, false) AS cost_modified,
    coalesce(m.quantity_modified, false) AS quantity_modified,
    coalesce(m.variant_modified, false) AS variant_modified
FROM sequenced s
LEFT JOIN agg a ON a.variant_id = s.variant_id
LEFT JOIN modified m ON m.reception_id = s.reception_id AND m.detail_id = s.detail_id
LEFT JOIN bsale_raw.variants v ON v.company_id = %s AND v.bsale_id = s.variant_id
LEFT JOIN bsale_raw.products p ON p.company_id = v.company_id AND p.bsale_id = v.product_id
WHERE s.admission_date BETWEEN %s AND %s
  AND (
        s.cost IS NULL
     OR (s.cost = 0 AND s.movement_class <> '{_TRANSFER}')
     OR s.quantity IS NULL OR s.quantity <= 0
     OR (a.median_comparable_quantity > 0
         AND s.quantity::double precision > a.median_comparable_quantity * %s::double precision)
     OR (s.previous_comparable_cost > 0
         AND ((s.cost - s.previous_comparable_cost) / s.previous_comparable_cost * 100 >= %s
           OR (s.cost - s.previous_comparable_cost) / s.previous_comparable_cost * 100 <= %s))
     OR m.reception_id IS NOT NULL
     OR s.details_complete IS NOT TRUE
     OR s.movement_class = '{_UNDETERMINED}'
  )
ORDER BY s.admission_date DESC, s.reception_id DESC, s.detail_id DESC
""".strip()

MOVEMENT_SUMMARY_SQL = f"""
WITH {_LINES_CTE.replace("{variant_filter}", "")}
SELECT
    movement_class,
    count(*) AS lines,
    count(DISTINCT reception_id) AS receptions,
    count(*) FILTER (WHERE comparable) AS comparable_lines,
    count(*) FILTER (WHERE cost = 0) AS zero_cost_lines,
    count(*) FILTER (WHERE cost IS NULL) AS null_cost_lines,
    min(admission_date) AS first_admission_date,
    max(admission_date) AS last_admission_date
FROM flagged
GROUP BY movement_class
ORDER BY lines DESC
""".strip()

RECEPTION_HEADER_SQL = f"""
SELECT
    r.bsale_id AS reception_id,
    r.admission_date,
    r.office_id,
    r.document,
    r.document_number,
    r.internal_dispatch_id,
    r.details_complete,
    r.details_count,
    r.missing_since,
    r.first_seen_at,
    r.last_changed_at,
    r.api_fetched_at,
    {_MOVEMENT_CASE} AS movement_class
FROM bsale_raw.stock_receptions r
WHERE r.company_id = %s AND r.bsale_id = %s
""".strip()

RECEPTION_LINES_SQL = """
SELECT
    d.bsale_id AS detail_id,
    d.variant_id,
    v.code,
    v.bar_code,
    v.description AS variant_name,
    p.name AS product_name,
    d.quantity,
    d.cost,
    d.missing_since,
    d.last_changed_at
FROM bsale_raw.stock_reception_details d
LEFT JOIN bsale_raw.variants v ON v.company_id = d.company_id AND v.bsale_id = d.variant_id
LEFT JOIN bsale_raw.products p ON p.company_id = v.company_id AND p.bsale_id = v.product_id
WHERE d.company_id = %s AND d.reception_id = %s
ORDER BY d.bsale_id
""".strip()

RECEPTION_EVENTS_SQL = """
SELECT
    event_kind,
    detected_at,
    detected_by,
    header_changed,
    details_changed,
    cardinality(added_detail_ids) AS added_lines,
    cardinality(removed_detail_ids) AS removed_lines,
    cost_changed_detail_ids,
    quantity_changed_detail_ids,
    variant_changed_detail_ids
FROM bsale_raw.stock_reception_change_log
WHERE company_id = %s AND reception_id = %s
ORDER BY detected_at ASC, id ASC
""".strip()

TAX_CATALOG_SQL = """
SELECT bsale_id AS tax_id, name, code, percentage, state, missing_since
FROM bsale_raw.taxes
WHERE company_id = %s
""".strip()

OFFICES_SQL = """
SELECT bsale_id AS office_id, name, state, is_virtual
FROM bsale_raw.offices
WHERE company_id = %s
ORDER BY bsale_id
""".strip()

PRODUCT_TYPES_SQL = """
SELECT bsale_id AS product_type_id, name, state
FROM bsale_raw.product_types
WHERE company_id = %s AND missing_since IS NULL
ORDER BY name
""".strip()

PACK_DETAILS_SQL = """
SELECT
    p.bsale_id AS product_id,
    p.payload -> 'pack_details' AS pack_details,
    p.api_fetched_at AS composition_fetched_at
FROM bsale_raw.products p
WHERE p.company_id = %s
  AND p.classification = 3
  AND p.missing_since IS NULL
""".strip()

SYNC_STATE_SQL = """
SELECT resource, scope, status, last_success_at, last_attempt_at, last_full_reconcile_at
FROM bsale_raw.sync_state
WHERE company_id = %s
ORDER BY resource, scope
""".strip()


class CostControlV2Repository:
    def __init__(self, executor: QueryExecutor) -> None:
        self._execute = executor

    def _run(self, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        assert_sql_is_read_only(sql)
        return self._execute(sql, params)

    def variant_facts(self, *, company_id: int, history_start: date) -> list[dict[str, Any]]:
        return self._run(VARIANT_FACTS_SQL, (company_id, history_start, company_id, company_id))

    def variant_lines(
        self, *, company_id: int, variant_id: int, history_start: date
    ) -> list[dict[str, Any]]:
        return self._run(VARIANT_LINES_SQL, (company_id, history_start, variant_id, company_id))

    def alert_candidates(
        self,
        *,
        company_id: int,
        history_start: date,
        date_from: date,
        date_to: date,
        extreme_quantity_factor: Any,
        increase_pct: Any,
        decrease_pct: Any,
    ) -> list[dict[str, Any]]:
        return self._run(
            ALERT_CANDIDATES_SQL,
            (
                company_id,
                history_start,
                company_id,
                company_id,
                date_from,
                date_to,
                extreme_quantity_factor,
                increase_pct,
                decrease_pct,
            ),
        )

    def movement_summary(self, *, company_id: int, history_start: date) -> list[dict[str, Any]]:
        return self._run(MOVEMENT_SUMMARY_SQL, (company_id, history_start))

    def reception_header(self, *, company_id: int, reception_id: int) -> dict[str, Any] | None:
        rows = self._run(RECEPTION_HEADER_SQL, (company_id, reception_id))
        return rows[0] if rows else None

    def reception_lines(self, *, company_id: int, reception_id: int) -> list[dict[str, Any]]:
        return self._run(RECEPTION_LINES_SQL, (company_id, reception_id))

    def reception_events(self, *, company_id: int, reception_id: int) -> list[dict[str, Any]]:
        return self._run(RECEPTION_EVENTS_SQL, (company_id, reception_id))

    def tax_catalog(self, *, company_id: int) -> list[dict[str, Any]]:
        return self._run(TAX_CATALOG_SQL, (company_id,))

    def offices(self, *, company_id: int) -> list[dict[str, Any]]:
        return self._run(OFFICES_SQL, (company_id,))

    def product_types(self, *, company_id: int) -> list[dict[str, Any]]:
        return self._run(PRODUCT_TYPES_SQL, (company_id,))

    def pack_details(self, *, company_id: int) -> list[dict[str, Any]]:
        return self._run(PACK_DETAILS_SQL, (company_id,))

    def sync_state(self, *, company_id: int) -> list[dict[str, Any]]:
        return self._run(SYNC_STATE_SQL, (company_id,))
