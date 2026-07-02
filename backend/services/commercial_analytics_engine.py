"""Motor analítico comercial: una sesión readonly, sales_base compartido, sin consultas duplicadas."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Callable, TypeVar

from backend.db import get_connection
from backend.services.distribuidora.orders_service import _row_to_dict, _serialize_row

logger = logging.getLogger(__name__)

COMMERCIAL_DEBUG_SQL = os.getenv("COMMERCIAL_DEBUG_SQL", "").lower() in ("true", "1", "yes")

T = TypeVar("T")

COMPANY_ID = 3
DOC_BOLETA = 1
DOC_FACTURA = 6
SALE_DOC_TYPES = (DOC_BOLETA, DOC_FACTURA)

CROSS_SELL_RULES: list[tuple[str, str, str, str]] = [
    ("cerveza", "hielo", "Compra cerveza pero no hielo", "alta"),
    ("whisky", "energetica", "Compra whisky pero no energética", "media"),
    ("bebida", "snack", "Compra bebidas pero no snacks", "media"),
    ("aseo", "papel", "Compra aseo pero no papel", "baja"),
]


def _float(v: Any) -> float:
    from decimal import Decimal
    if v is None:
        return 0.0
    if isinstance(v, Decimal):
        return float(v)
    return float(v)


def _int(v: Any) -> int:
    if v is None:
        return 0
    return int(v)


def _delta(current: float, previous: float) -> dict[str, Any]:
    diff = current - previous
    if previous == 0:
        pct = 100.0 if current > 0 else 0.0
    else:
        pct = (diff / abs(previous)) * 100.0
    if abs(diff) < 0.01:
        trend = "flat"
    elif diff > 0:
        trend = "up"
    else:
        trend = "down"
    return {
        "current": current,
        "previous": previous,
        "delta_abs": round(diff, 2),
        "delta_pct": round(pct, 1),
        "trend": trend,
    }



from backend.services.commercial_analytics_service import CommercialFilters  # noqa: E402 — sin import circular (engine no importado al cargar service)


@dataclass
class BundleLimits:
    seller: int = 50
    unique_clients: int = 300
    lost_clients: int = 100
    cross_selling: int = 100
    products: int = 50


@dataclass
class SalesScope:
    """Filtros y ventana histórica compartida para todas las métricas del bundle."""

    filters: CommercialFilters
    prev_from: date
    prev_to: date
    hist_from: date
    date_to: date
    doc_filter: str
    doc_params: list[Any]
    extra_sql: str
    extra_params: list[Any]

    @classmethod
    def from_filters(cls, filters: CommercialFilters) -> SalesScope:
        prev_from, prev_to = filters.compare_period()
        hist_from = min(prev_from, filters.date_from - timedelta(days=180))
        doc = (filters.document_type or "all").lower().strip()
        if doc == "factura":
            doc_filter = "sb.document_type_id = %s"
            doc_params: list[Any] = [DOC_FACTURA]
        elif doc == "boleta":
            doc_filter = "sb.document_type_id = %s"
            doc_params = [DOC_BOLETA]
        else:
            doc_filter = "sb.document_type_id IN %s"
            doc_params = [SALE_DOC_TYPES]

        extra: list[str] = []
        extra_params: list[Any] = []
        if filters.seller and str(filters.seller).strip():
            extra.append("sb.seller_name = %s")
            extra_params.append(filters.seller.strip())
        if filters.city and str(filters.city).strip():
            extra.append("sb.municipality = %s")
            extra_params.append(filters.city.strip())
        if filters.client_id is not None:
            extra.append("sb.client_id = %s")
            extra_params.append(int(filters.client_id))
        extra_sql = (" AND " + " AND ".join(extra)) if extra else ""

        return cls(
            filters=filters,
            prev_from=prev_from,
            prev_to=prev_to,
            hist_from=hist_from,
            date_to=filters.date_to,
            doc_filter=doc_filter,
            doc_params=doc_params,
            extra_sql=extra_sql,
            extra_params=extra_params,
        )

    def sales_base_cte(self) -> tuple[str, list[Any]]:
        """CTE sales_base: única lectura acotada de v_sales para el bundle."""
        params: list[Any] = list(self.doc_params)
        params.extend([self.hist_from, self.date_to])
        params.extend(self.extra_params)
        sql = f"""
        sales_base AS (
            SELECT
                sb.document_id,
                sb.emission_date,
                (sb.emission_date AT TIME ZONE 'UTC')::date AS sale_day,
                sb.document_type_id,
                sb.client_id,
                sb.client_name,
                sb.municipality,
                sb.seller_name,
                sb.seller_id,
                sb.total_amount_net,
                sb.total_amount_sales,
                sb.is_sale
            FROM distribuidora.v_sales sb
            WHERE sb.is_sale = 1
              AND {self.doc_filter}
              AND (sb.emission_date AT TIME ZONE 'UTC')::date >= %s
              AND (sb.emission_date AT TIME ZONE 'UTC')::date <= %s
              {self.extra_sql}
        )"""
        return sql, params


class CommercialReadSession:
    """Una conexión, autocommit, solo lectura — evita transacciones implícitas largas."""

    def __init__(self, endpoint: str) -> None:
        self.endpoint = endpoint
        self._conn: Any = None
        self._cache: dict[str, Any] = {}
        self.rows_analyzed = 0
        self.sql_metrics: list[dict[str, Any]] = []
        self.queries_ok = 0
        self.queries_failed = 0
        self._failed_query_names: list[str] = []

    def record_bundle_failure(self, query_name: str, exc: Exception) -> None:
        self.queries_failed += 1
        self._failed_query_names.append(query_name)
        logger.error(
            "[COMMERCIAL_BUNDLE] secondary_query_failed query=%s error=%s",
            query_name,
            exc,
        )

    def health_payload(self, *, bundle_complete: bool) -> dict[str, Any]:
        status = "ok"
        if self.queries_failed > 0:
            status = "degraded"
        if not bundle_complete and self.queries_failed == 0:
            status = "ok"
        return {
            "status": status,
            "queries_ok": self.queries_ok,
            "queries_failed": self.queries_failed,
            "bundle_complete": bundle_complete,
            "failed_queries": list(self._failed_query_names),
        }

    def __enter__(self) -> CommercialReadSession:
        self._conn = get_connection()
        self._conn.autocommit = True
        cur = self._conn.cursor()
        try:
            cur.execute("SET default_transaction_read_only = on")
        finally:
            cur.close()
        logger.info(
            "[COMMERCIAL_LOCK] transaction_start endpoint=%s readonly=1 autocommit=1",
            self.endpoint,
        )
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        logger.info(
            "[COMMERCIAL_LOCK] transaction_end endpoint=%s error=%s",
            self.endpoint,
            bool(exc),
        )
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def query_all(self, label: str, sql: str, params: tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
        assert self._conn is not None
        bound_params = tuple(params or ())
        placeholder_count = sql.count("%s")
        param_count = len(bound_params)

        if placeholder_count != param_count:
            logger.error(
                "[COMMERCIAL_SQL_DEBUG]\nPARAMETER_MISMATCH\nquery=%s\nplaceholders=%s\nparams=%s",
                label,
                placeholder_count,
                param_count,
            )
            raise RuntimeError(
                f"SQL parameter mismatch: placeholders={placeholder_count}, params={param_count}"
            )

        if COMMERCIAL_DEBUG_SQL:
            logger.info(
                "[COMMERCIAL_SQL_DEBUG] query=%s placeholders=%s params=%s",
                label,
                placeholder_count,
                param_count,
            )
            logger.info("[COMMERCIAL_SQL_DEBUG] params_list=%s", list(bound_params))

        t0 = time.perf_counter()
        cur = self._conn.cursor()
        try:
            cur.execute(sql, bound_params)
            rows = cur.fetchall()
            out = [_serialize_row(_row_to_dict(cur, r)) for r in rows]
        finally:
            cur.close()
        ms = (time.perf_counter() - t0) * 1000
        self.rows_analyzed += len(out)
        self.queries_ok += 1
        self.sql_metrics.append({
            "name": label,
            "execution_ms": round(ms, 1),
            "rows": len(out),
            "placeholders": placeholder_count,
            "params": param_count,
        })

        logger.info(
            "[COMMERCIAL_SQL] query_name=%s execution_ms=%.1f rows=%s placeholders=%s params=%s",
            label,
            ms,
            len(out),
            placeholder_count,
            param_count,
        )
        if ms > 1000:
            logger.warning(
                "[COMMERCIAL_PERFORMANCE_WARNING] query_name=%s execution_ms=%.1f rows=%s",
                label,
                ms,
                len(out),
            )

        if COMMERCIAL_DEBUG_SQL:
            logger.info(
                "[COMMERCIAL_SQL_DEBUG] query=%s execution_ms=%.1f rows=%s placeholders=%s params=%s",
                label,
                ms,
                len(out),
                placeholder_count,
                param_count,
            )
            self._log_explain(label, sql, bound_params)

        return out

    def _log_explain(self, label: str, sql: str, params: tuple[Any, ...]) -> None:
        assert self._conn is not None
        explain_cur = self._conn.cursor()
        try:
            explain_cur.execute(f"EXPLAIN {sql}", params)
            plan = "\n".join(str(row[0]) for row in explain_cur.fetchall())
            logger.info("[COMMERCIAL_SQL_DEBUG] EXPLAIN query=%s\n%s", label, plan)
        except Exception as exc:
            logger.warning("[COMMERCIAL_SQL_DEBUG] EXPLAIN failed query=%s error=%s", label, exc)
        finally:
            explain_cur.close()

    def query_one(self, label: str, sql: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
        rows = self.query_all(label, sql, params)
        return rows[0] if rows else None


def _client_classification_merged(session: CommercialReadSession, scope: SalesScope) -> dict[str, int]:
    cache_key = "client_classification"
    if cache_key in session._cache:
        return session._cache[cache_key]

    base_cte, base_params = scope.sales_base_cte()
    f = scope.filters

    # Parámetros del CTE per_client (9 placeholders, en orden de aparición en SQL)
    per_client_params: list[Any] = [
        f.date_from,
        f.date_to,
        scope.prev_from,
        scope.prev_to,
        f.date_from,
        f.date_to,
        f.date_to,
        f.date_to,
        f.date_to,
    ]

    # Parámetros del bloque tagged (2 placeholders)
    tagged_params: list[Any] = [
        f.date_from,
        f.date_from,
    ]

    # Parámetro del bloque risk (1 placeholder)
    risk_params: list[Any] = [f.date_to]

    params: list[Any] = (
        list(base_params) + per_client_params + tagged_params + risk_params
    )

    sql = f"""
        WITH {base_cte},
        per_client AS (
            SELECT
                sb.client_id,
                BOOL_OR(sb.sale_day BETWEEN %s AND %s) AS in_curr,
                BOOL_OR(sb.sale_day BETWEEN %s AND %s) AS in_prev,
                MAX(sb.sale_day) FILTER (WHERE sb.sale_day < %s) AS last_before,
                COUNT(DISTINCT sb.sale_day) FILTER (
                    WHERE sb.sale_day >= (%s::date - INTERVAL '180 days')::date
                ) AS visit_days_180,
                MAX(sb.sale_day) FILTER (
                    WHERE sb.sale_day >= (%s::date - INTERVAL '180 days')::date
                ) AS last_d_180,
                MIN(sb.sale_day) FILTER (
                    WHERE sb.sale_day >= (%s::date - INTERVAL '180 days')::date
                ) AS first_d_180,
                COUNT(*) FILTER (
                    WHERE sb.sale_day >= (%s::date - INTERVAL '180 days')::date
                ) AS cnt_180
            FROM sales_base sb
            GROUP BY sb.client_id
        ),
        tagged AS (
            SELECT
                client_id,
                CASE
                    WHEN in_curr AND in_prev THEN 'activo'
                    WHEN in_curr AND NOT in_prev
                         AND (last_before IS NULL OR last_before < (%s::date - INTERVAL '90 days')::date)
                        THEN 'nuevo'
                    WHEN in_curr AND NOT in_prev
                         AND last_before < (%s::date - INTERVAL '60 days')::date
                        THEN 'recuperado'
                    WHEN in_prev AND NOT in_curr THEN 'perdido'
                    ELSE NULL
                END AS bucket
            FROM per_client
        ),
        risk AS (
            SELECT COUNT(*)::bigint AS n
            FROM per_client pc
            WHERE NOT pc.in_curr
              AND pc.cnt_180 >= 3
              AND (%s::date - pc.last_d_180) > GREATEST(
                  14,
                  ((pc.last_d_180 - pc.first_d_180)::numeric / NULLIF(pc.visit_days_180 - 1, 0)) * 1.5
              )
        ),
        counts AS (
            SELECT bucket AS status, COUNT(*)::bigint AS n
            FROM tagged
            WHERE bucket IS NOT NULL
            GROUP BY bucket
        )
        SELECT status, n FROM counts
        UNION ALL
        SELECT 'en_riesgo', n FROM risk
    """
    rows = session.query_all("client_classification_merged", sql, tuple(params))
    result = {
        "activos": 0,
        "nuevos": 0,
        "recuperados": 0,
        "perdidos": 0,
        "en_riesgo": 0,
    }
    key_map = {
        "activo": "activos",
        "nuevo": "nuevos",
        "recuperado": "recuperados",
        "perdido": "perdidos",
        "en_riesgo": "en_riesgo",
    }
    for r in rows:
        k = key_map.get(str(r.get("status")), "")
        if k:
            result[k] = _int(r.get("n"))
    session._cache[cache_key] = result
    return result


def _period_kpis_merged(
    session: CommercialReadSession,
    scope: SalesScope,
    d_from: date,
    d_to: date,
    label: str,
) -> dict[str, Any]:
    base_cte, base_params = scope.sales_base_cte()
    params: list[Any] = list(base_params)
    params.extend([d_from, d_to])

    sql_sales = f"""
        WITH {base_cte}
        SELECT
            COALESCE(SUM(sb.total_amount_net) FILTER (
                WHERE sb.sale_day BETWEEN %s AND %s
            ), 0) AS venta_neta,
            COUNT(DISTINCT sb.client_id) FILTER (
                WHERE sb.sale_day BETWEEN %s AND %s
            )::bigint AS clientes_unicos,
            COALESCE(SUM(sb.is_sale) FILTER (
                WHERE sb.sale_day BETWEEN %s AND %s
            ), 0)::bigint AS documentos_emitidos,
            COALESCE(
                SUM(sb.total_amount_sales) FILTER (WHERE sb.sale_day BETWEEN %s AND %s)
                / NULLIF(SUM(sb.is_sale) FILTER (WHERE sb.sale_day BETWEEN %s AND %s)::numeric, 0),
                0
            ) AS ticket_promedio
        FROM sales_base sb
    """
    params.extend([d_from, d_to] * 4)
    row = session.query_one(f"{label}_sales", sql_sales, tuple(params)) or {}

    sql_lines = f"""
        WITH {base_cte},
        sale_docs AS (
            SELECT DISTINCT sb.document_id
            FROM sales_base sb
            WHERE sb.sale_day BETWEEN %s AND %s
        )
        SELECT
            COALESCE(SUM(dd.quantity), 0) AS unidades_vendidas,
            COUNT(DISTINCT dd.variant_id)::bigint AS productos_distintos
        FROM distribuidora.document_details dd
        INNER JOIN sale_docs sd ON sd.document_id = dd.document_id
    """
    line_params = list(base_params) + [d_from, d_to]
    lines = session.query_one(f"{label}_lines", sql_lines, tuple(line_params)) or {}

    return {
        "venta_neta": _float(row.get("venta_neta")),
        "clientes_unicos": _int(row.get("clientes_unicos")),
        "documentos_emitidos": _int(row.get("documentos_emitidos")),
        "ticket_promedio": _float(row.get("ticket_promedio")),
        "unidades_vendidas": _float(lines.get("unidades_vendidas")),
        "productos_distintos": _int(lines.get("productos_distintos")),
    }


def _today_sales(session: CommercialReadSession, scope: SalesScope) -> dict[str, Any] | None:
    from datetime import datetime, timezone

    today = datetime.now(timezone.utc).date()
    if today < scope.filters.date_from or today > scope.filters.date_to:
        return None
    base_cte, base_params = scope.sales_base_cte()
    params = list(base_params) + [today, today]
    sql = f"""
        WITH {base_cte}
        SELECT
            COALESCE(SUM(sb.total_amount_net), 0) AS venta_neta,
            COUNT(DISTINCT sb.client_id)::bigint AS clientes
        FROM sales_base sb
        WHERE sb.sale_day BETWEEN %s AND %s
    """
    return session.query_one("today_sales", sql, tuple(params))


def _monthly_timeline(session: CommercialReadSession, scope: SalesScope) -> list[dict[str, Any]]:
    from datetime import timedelta

    end = scope.filters.date_to
    start = end - timedelta(days=365)
    base_cte, base_params = scope.sales_base_cte()
    params = list(base_params) + [start, end]
    sql = f"""
        WITH {base_cte}
        SELECT
            TO_CHAR(sb.sale_day, 'YYYY-MM') AS mes,
            COALESCE(SUM(sb.total_amount_net), 0) AS venta,
            COUNT(DISTINCT sb.client_id)::bigint AS clientes,
            COALESCE(SUM(sb.is_sale), 0)::bigint AS documentos,
            COALESCE(
                SUM(sb.total_amount_sales) / NULLIF(SUM(sb.is_sale)::numeric, 0),
                0
            ) AS ticket_promedio
        FROM sales_base sb
        WHERE sb.sale_day BETWEEN %s AND %s
        GROUP BY 1
        ORDER BY 1
    """
    return session.query_all("monthly_timeline", sql, tuple(params))


def _daily_sales(session: CommercialReadSession, scope: SalesScope) -> list[dict[str, Any]]:
    base_cte, base_params = scope.sales_base_cte()
    f = scope.filters
    params = list(base_params) + [f.date_from, f.date_to]
    sql = f"""
        WITH {base_cte}
        SELECT
            sb.sale_day AS day,
            COALESCE(SUM(sb.total_amount_net), 0) AS venta_neta,
            COUNT(DISTINCT sb.client_id)::bigint AS clientes
        FROM sales_base sb
        WHERE sb.sale_day BETWEEN %s AND %s
        GROUP BY sb.sale_day
        ORDER BY sb.sale_day
    """
    rows = session.query_all("daily_sales", sql, tuple(params))
    return [
        {"day": str(r["day"]), "venta_neta": _float(r["venta_neta"]), "clientes": _int(r["clientes"])}
        for r in rows
    ]


def _seller_performance(session: CommercialReadSession, scope: SalesScope, limit: int) -> dict[str, Any]:
    base_cte, base_params = scope.sales_base_cte()
    f = scope.filters
    params = list(base_params) + [
        f.date_from, f.date_to,
        scope.prev_from, scope.prev_to,
        f.date_from, f.date_to,
        scope.prev_from, scope.prev_to,
        f.date_from,
        f.date_from, f.date_from,
        f.date_from, f.date_to,
        limit,
    ]

    sql = f"""
        WITH {base_cte},
        seller_agg AS (
            SELECT
                sb.seller_name,
                MAX(sb.seller_id) AS seller_id,
                COALESCE(SUM(sb.total_amount_net) FILTER (
                    WHERE sb.sale_day BETWEEN %s AND %s
                ), 0) AS venta_actual,
                COALESCE(SUM(sb.total_amount_net) FILTER (
                    WHERE sb.sale_day BETWEEN %s AND %s
                ), 0) AS venta_anterior,
                COUNT(DISTINCT sb.client_id) FILTER (
                    WHERE sb.sale_day BETWEEN %s AND %s
                )::bigint AS clientes_curr,
                COUNT(DISTINCT sb.client_id) FILTER (
                    WHERE sb.sale_day BETWEEN %s AND %s
                )::bigint AS clientes_prev,
                COALESCE(
                    SUM(sb.total_amount_sales) FILTER (WHERE sb.sale_day BETWEEN %s AND %s)
                    / NULLIF(SUM(sb.is_sale) FILTER (WHERE sb.sale_day BETWEEN %s AND %s)::numeric, 0),
                    0
                ) AS ticket_promedio
            FROM sales_base sb
            GROUP BY sb.seller_name
        ),
        seller_clients AS (
            SELECT
                sb.seller_name,
                sb.client_id,
                BOOL_OR(sb.sale_day BETWEEN %s AND %s) AS in_curr,
                BOOL_OR(sb.sale_day BETWEEN %s AND %s) AS in_prev,
                MAX(sb.sale_day) FILTER (WHERE sb.sale_day < %s) AS last_before
            FROM sales_base sb
            GROUP BY sb.seller_name, sb.client_id
        ),
        seller_client_stats AS (
            SELECT
                sc.seller_name,
                COUNT(*) FILTER (WHERE sc.in_curr AND NOT sc.in_prev
                    AND (sc.last_before IS NULL OR sc.last_before < (%s::date - INTERVAL '90 days')::date)
                ) AS nuevos,
                COUNT(*) FILTER (WHERE sc.in_curr AND NOT sc.in_prev
                    AND sc.last_before IS NOT NULL AND sc.last_before < (%s::date - INTERVAL '60 days')::date
                ) AS recuperados,
                COUNT(*) FILTER (WHERE sc.in_prev AND NOT sc.in_curr) AS perdidos
            FROM seller_clients sc
            GROUP BY sc.seller_name
        ),
        sale_docs AS (
            SELECT DISTINCT sb.document_id, sb.seller_name
            FROM sales_base sb
            WHERE sb.sale_day BETWEEN %s AND %s
        ),
        prod AS (
            SELECT sd.seller_name, COUNT(DISTINCT dd.variant_id)::bigint AS productos
            FROM sale_docs sd
            INNER JOIN distribuidora.document_details dd ON dd.document_id = sd.document_id
            GROUP BY sd.seller_name
        ),
        cats AS (
            SELECT
                sd.seller_name,
                COUNT(DISTINCT COALESCE(pt.name, pm.product_type, 'Sin categoría'))::bigint AS categorias
            FROM sale_docs sd
            INNER JOIN distribuidora.document_details dd ON dd.document_id = sd.document_id
            LEFT JOIN bsale.variants v2 ON v2.company_id = {COMPANY_ID} AND v2.bsale_id = dd.variant_id
            LEFT JOIN bsale.products p ON p.company_id = v2.company_id AND p.bsale_id = v2.product_id
            LEFT JOIN bsale.product_types pt ON pt.company_id = p.company_id AND pt.bsale_id = p.product_type_id
            LEFT JOIN bsale.products_master pm ON pm.company_id = {COMPANY_ID} AND pm.variant_id = dd.variant_id
            GROUP BY sd.seller_name
        )
        SELECT
            sa.seller_name,
            sa.seller_id,
            sa.venta_actual,
            sa.venta_anterior,
            sa.clientes_curr,
            sa.clientes_prev,
            COALESCE(scs.nuevos, 0) AS nuevos,
            COALESCE(scs.perdidos, 0) AS perdidos,
            COALESCE(scs.recuperados, 0) AS recuperados,
            sa.ticket_promedio,
            COALESCE(pr.productos, 0) AS productos,
            COALESCE(ca.categorias, 0) AS categorias
        FROM seller_agg sa
        LEFT JOIN seller_client_stats scs ON scs.seller_name = sa.seller_name
        LEFT JOIN prod pr ON pr.seller_name = sa.seller_name
        LEFT JOIN cats ca ON ca.seller_name = sa.seller_name
        ORDER BY sa.venta_actual DESC NULLS LAST
        LIMIT %s
    """
    rows = session.query_all("seller_performance", sql, tuple(params))
    items = [
        {
            "seller_name": r.get("seller_name"),
            "seller_id": r.get("seller_id"),
            "venta_actual": _float(r.get("venta_actual")),
            "venta_anterior": _float(r.get("venta_anterior")),
            "variacion_pct": round(
                _delta(_float(r.get("venta_actual")), _float(r.get("venta_anterior")))["delta_pct"], 1
            ),
            "clientes_unicos_actual": _int(r.get("clientes_curr")),
            "clientes_unicos_anterior": _int(r.get("clientes_prev")),
            "clientes_nuevos": _int(r.get("nuevos")),
            "clientes_perdidos": _int(r.get("perdidos")),
            "clientes_recuperados": _int(r.get("recuperados")),
            "ticket_promedio": _float(r.get("ticket_promedio")),
            "productos_distintos": _int(r.get("productos")),
            "categorias_vendidas": _int(r.get("categorias")),
        }
        for r in rows
    ]
    rankings = {
        "mayor_venta": [x["seller_name"] for x in sorted(items, key=lambda x: x["venta_actual"], reverse=True)[:5]],
        "mayor_crecimiento": [x["seller_name"] for x in sorted(items, key=lambda x: x["variacion_pct"], reverse=True)[:5]],
        "mayor_recuperacion": [x["seller_name"] for x in sorted(items, key=lambda x: x["clientes_recuperados"], reverse=True)[:5]],
        "mayor_perdida": [x["seller_name"] for x in sorted(items, key=lambda x: x["clientes_perdidos"], reverse=True)[:5]],
        "mejor_cobertura": [x["seller_name"] for x in sorted(items, key=lambda x: x["clientes_unicos_actual"], reverse=True)[:5]],
    }
    return {"items": items, "rankings": rankings}


def _unique_clients(session: CommercialReadSession, scope: SalesScope, limit: int) -> dict[str, Any]:
    from backend.services.commercial_analytics_intelligence import enrich_client_row

    base_cte, base_params = scope.sales_base_cte()
    f = scope.filters
    params = list(base_params) + [
        f.date_from, f.date_to,
        f.date_from, f.date_to,
        scope.prev_from, scope.prev_to,
        f.date_from, f.date_to,
        scope.prev_from, scope.prev_to,
        f.date_from,
        f.date_from, f.date_from,
        limit,
    ]
    sql = f"""
        WITH {base_cte},
        client_period AS (
            SELECT
                sb.client_id,
                MAX(sb.client_name) AS client_name,
                MAX(sb.municipality) AS municipality,
                (
                    ARRAY_AGG(sb.seller_name ORDER BY sb.emission_date DESC)
                    FILTER (WHERE sb.sale_day BETWEEN %s AND %s)
                )[1] AS seller_name,
                COALESCE(SUM(sb.total_amount_net) FILTER (
                    WHERE sb.sale_day BETWEEN %s AND %s
                ), 0) AS venta_actual,
                COALESCE(SUM(sb.total_amount_net) FILTER (
                    WHERE sb.sale_day BETWEEN %s AND %s
                ), 0) AS venta_anterior,
                BOOL_OR(sb.sale_day BETWEEN %s AND %s) AS in_curr,
                BOOL_OR(sb.sale_day BETWEEN %s AND %s) AS in_prev,
                MAX(sb.sale_day) FILTER (WHERE sb.sale_day < %s) AS last_before,
                MAX(sb.sale_day) AS ultima_compra,
                (CURRENT_DATE - MAX(sb.sale_day))::int AS dias_sin_comprar,
                COUNT(*) FILTER (
                    WHERE sb.sale_day >= (%s::date - INTERVAL '90 days')::date
                )::bigint AS compras_90d,
                COALESCE(
                    SUM(sb.total_amount_sales) FILTER (WHERE sb.sale_day BETWEEN %s AND %s)
                    / NULLIF(SUM(sb.is_sale) FILTER (WHERE sb.sale_day BETWEEN %s AND %s)::numeric, 0),
                    0
                ) AS ticket_promedio
            FROM sales_base sb
            GROUP BY sb.client_id
        )
        SELECT
            client_id, client_name, municipality, seller_name,
            venta_actual, venta_anterior, dias_sin_comprar, compras_90d, ticket_promedio,
            CASE
                WHEN in_curr AND in_prev THEN 'activo'
                WHEN in_curr AND NOT in_prev AND (
                    last_before IS NULL OR last_before < (%s::date - INTERVAL '90 days')::date
                ) THEN 'nuevo'
                WHEN in_curr AND NOT in_prev AND last_before < (%s::date - INTERVAL '60 days')::date
                    THEN 'recuperado'
                WHEN in_prev AND NOT in_curr THEN 'perdido'
                ELSE 'en_riesgo'
            END AS status
        FROM client_period
        WHERE in_curr OR in_prev
        ORDER BY venta_actual DESC NULLS LAST
        LIMIT %s
    """
    rows = session.query_all("unique_clients", sql, tuple(params))
    summary: dict[str, int] = {}
    items = []
    for r in rows:
        enriched = enrich_client_row({
            "client_id": _int(r["client_id"]),
            "client_name": r.get("client_name"),
            "municipality": r.get("municipality"),
            "seller_name": r.get("seller_name"),
            "venta_actual": _float(r.get("venta_actual")),
            "venta_anterior": _float(r.get("venta_anterior")),
            "dias_sin_comprar": _int(r.get("dias_sin_comprar")),
            "compras_90d": _int(r.get("compras_90d")),
            "ticket_promedio": _float(r.get("ticket_promedio")),
            "status": r.get("status"),
            "ultima_compra": str(r.get("ultima_compra")) if r.get("ultima_compra") else None,
        })
        st = str(enriched.get("status") or "otro")
        summary[st] = summary.get(st, 0) + 1
        items.append(enriched)
    return {"items": items, "summary": summary}


def get_commercial_map_data(
    filters: CommercialFilters,
    *,
    limit: int = 500,
) -> dict[str, Any]:
    """Datos de clientes con georef para mapa comercial — on-demand."""
    from backend.services.commercial_predictive_intelligence import (
        MAP_ESTADO,
        _cross_lookup,
        _peer_medians_by_comuna,
        enrich_client_predictive,
    )
    from backend.services.commercial_analytics_intelligence import enrich_client_row

    scope = SalesScope.from_filters(filters)
    lim = BundleLimits(unique_clients=limit, lost_clients=min(limit, 200))

    with CommercialReadSession("commercial-map") as session:
        unique = _unique_clients(session, scope, lim.unique_clients)
        cross = _cross_selling(session, scope, lim.cross_selling)

        base_cte, base_params = scope.sales_base_cte()
        f = scope.filters
        params = list(base_params) + [f.date_from, f.date_to, limit]
        sql = f"""
            WITH {base_cte},
            active_clients AS (
                SELECT DISTINCT sb.client_id
                FROM sales_base sb
                WHERE sb.sale_day BETWEEN %s AND %s
            )
            SELECT
                ac.client_id,
                c.lat,
                c.lon AS lng
            FROM active_clients ac
            INNER JOIN bsale.clients c
                ON c.company_id = {COMPANY_ID} AND c.bsale_id = ac.client_id
            WHERE c.lat IS NOT NULL AND c.lon IS NOT NULL
            LIMIT %s
        """
        geo_rows = session.query_all("commercial_map_geo", sql, tuple(params))
        geo_by_id = {_int(r["client_id"]): r for r in geo_rows}

    peer_medians = _peer_medians_by_comuna(unique["items"])
    cross_lookup = _cross_lookup(cross.get("items", []))

    points: list[dict[str, Any]] = []
    for raw in unique["items"]:
        cid = _int(raw.get("client_id"))
        geo = geo_by_id.get(cid)
        if not geo:
            continue
        enriched = enrich_client_predictive(
            enrich_client_row(raw),
            peer_medians=peer_medians,
            cross_by_client=cross_lookup,
        )
        status = str(enriched.get("status") or "activo")
        points.append({
            "client_id": cid,
            "lat": _float(geo.get("lat")),
            "lng": _float(geo.get("lng")),
            "vendedor": enriched.get("seller_name"),
            "nombre": enriched.get("client_name"),
            "score": _int(enriched.get("client_score")),
            "estado": MAP_ESTADO.get(status, "saludable"),
            "prioridad": "alta" if enriched.get("segmento") in ("VIP", "Perdido", "En Riesgo") else "media",
            "potencial": _float(enriched.get("potential_monthly")),
            "ticket_promedio": _float(enriched.get("ticket_promedio")),
            "frecuencia": _float(enriched.get("frecuencia_dias")),
            "ultima_compra": enriched.get("ultima_compra"),
            "cliente_vip": bool(enriched.get("cliente_vip")),
            "purchase_probability": _int(enriched.get("purchase_probability")),
            "segmento": enriched.get("segmento"),
            "comuna": enriched.get("municipality"),
        })

    return {
        "items": points,
        "total": len(points),
        "with_georef": len(points),
        "period": {"from": filters.date_from.isoformat(), "to": filters.date_to.isoformat()},
    }


def _lost_clients(session: CommercialReadSession, scope: SalesScope, limit: int) -> dict[str, Any]:
    base_cte, base_params = scope.sales_base_cte()
    f = scope.filters
    params = list(base_params) + [
        scope.prev_from, scope.prev_to,
        f.date_from, f.date_to,
        limit,
    ]
    sql = f"""
        WITH {base_cte},
        prev_clients AS (
            SELECT DISTINCT sb.client_id
            FROM sales_base sb
            WHERE sb.sale_day BETWEEN %s AND %s
        ),
        curr_clients AS (
            SELECT DISTINCT sb.client_id
            FROM sales_base sb
            WHERE sb.sale_day BETWEEN %s AND %s
        ),
        lost AS (
            SELECT p.client_id FROM prev_clients p
            LEFT JOIN curr_clients c ON c.client_id = p.client_id
            WHERE c.client_id IS NULL
        ),
        stats AS (
            SELECT
                sb.client_id,
                MAX(sb.client_name) AS client_name,
                MAX(sb.municipality) AS municipality,
                (ARRAY_AGG(sb.seller_name ORDER BY sb.emission_date DESC))[1] AS seller_name,
                MAX(sb.sale_day) AS ultima_compra,
                (CURRENT_DATE - MAX(sb.sale_day))::int AS dias_sin_comprar,
                COALESCE(SUM(sb.total_amount_net), 0) AS valor_historico,
                COALESCE(SUM(sb.is_sale), 0)::bigint AS total_compras,
                COALESCE(
                    SUM(sb.total_amount_sales) / NULLIF(SUM(sb.is_sale)::numeric, 0),
                    0
                ) AS ticket_promedio
            FROM sales_base sb
            INNER JOIN lost l ON l.client_id = sb.client_id
            GROUP BY sb.client_id
        ),
        top_products AS (
            SELECT
                sb.client_id,
                ARRAY_AGG(DISTINCT COALESCE(dd.variant_description, dd.variant_code, 'Producto')
                    ORDER BY COALESCE(dd.variant_description, dd.variant_code)) AS productos
            FROM sales_base sb
            INNER JOIN lost l ON l.client_id = sb.client_id
            INNER JOIN distribuidora.document_details dd ON dd.document_id = sb.document_id
            GROUP BY sb.client_id
        )
        SELECT
            s.*,
            tp.productos[1:5] AS productos_habituales,
            CASE
                WHEN s.valor_historico >= 500000 AND s.dias_sin_comprar >= 30 THEN 'alta'
                WHEN s.total_compras >= 5 AND s.dias_sin_comprar >= 21 THEN 'media'
                ELSE 'baja'
            END AS prioridad
        FROM stats s
        LEFT JOIN top_products tp ON tp.client_id = s.client_id
        ORDER BY
            CASE
                WHEN s.valor_historico >= 500000 THEN 1
                WHEN s.total_compras >= 5 THEN 2
                ELSE 3
            END,
            s.valor_historico DESC NULLS LAST
        LIMIT %s
    """
    rows = session.query_all("lost_clients", sql, tuple(params))
    action_map = {"alta": "Visitar", "media": "Llamar", "baja": "Ofrecer productos habituales"}
    return {
        "items": [
            {
                "client_id": _int(r["client_id"]),
                "client_name": r.get("client_name"),
                "seller_name": r.get("seller_name"),
                "municipality": r.get("municipality"),
                "ultima_compra": str(r.get("ultima_compra")) if r.get("ultima_compra") else None,
                "dias_sin_comprar": _int(r.get("dias_sin_comprar")),
                "promedio_compra_mensual": round(
                    _float(r.get("valor_historico")) / max(1, _int(r.get("total_compras"))), 0
                ),
                "ticket_promedio": _float(r.get("ticket_promedio")),
                "productos_habituales": list(r.get("productos_habituales") or []),
                "prioridad": r.get("prioridad"),
                "accion_sugerida": action_map.get(str(r.get("prioridad")), "Revisar si cambió de proveedor"),
            }
            for r in rows
        ],
    }


def _cross_selling(session: CommercialReadSession, scope: SalesScope, limit: int) -> dict[str, Any]:
    base_cte, base_params = scope.sales_base_cte()
    f = scope.filters
    params = list(base_params) + [f.date_from, f.date_to, limit * 3]
    sql = f"""
        WITH {base_cte},
        client_cats AS (
            SELECT
                sb.client_id,
                MAX(sb.client_name) AS client_name,
                (ARRAY_AGG(sb.seller_name ORDER BY sb.emission_date DESC))[1] AS seller_name,
                LOWER(COALESCE(pt.name, pm.product_type, '')) AS categoria
            FROM sales_base sb
            INNER JOIN distribuidora.document_details dd ON dd.document_id = sb.document_id
            LEFT JOIN bsale.variants v2 ON v2.company_id = {COMPANY_ID} AND v2.bsale_id = dd.variant_id
            LEFT JOIN bsale.products p ON p.company_id = v2.company_id AND p.bsale_id = v2.product_id
            LEFT JOIN bsale.product_types pt ON pt.company_id = p.company_id AND pt.bsale_id = p.product_type_id
            LEFT JOIN bsale.products_master pm ON pm.company_id = {COMPANY_ID} AND pm.variant_id = dd.variant_id
            WHERE sb.sale_day BETWEEN %s AND %s
            GROUP BY sb.client_id, LOWER(COALESCE(pt.name, pm.product_type, ''))
        ),
        client_cat_set AS (
            SELECT client_id, MAX(client_name) AS client_name, MAX(seller_name) AS seller_name,
                   ARRAY_AGG(DISTINCT categoria) AS cats
            FROM client_cats
            GROUP BY client_id
        )
        SELECT client_id, client_name, seller_name, cats
        FROM client_cat_set
        LIMIT %s
    """
    rows = session.query_all("cross_selling_base", sql, tuple(params))
    opportunities: list[dict[str, Any]] = []
    for row in rows:
        cats = [str(c).lower() for c in (row.get("cats") or []) if c]
        cats_joined = " ".join(cats)
        for bought_kw, rec_kw, motivo, prioridad in CROSS_SELL_RULES:
            if bought_kw in cats_joined and rec_kw not in cats_joined:
                opportunities.append({
                    "client_id": _int(row["client_id"]),
                    "client_name": row.get("client_name"),
                    "seller_name": row.get("seller_name"),
                    "producto_comprado": bought_kw,
                    "producto_recomendado": rec_kw,
                    "motivo": motivo,
                    "prioridad": prioridad,
                })
                break
        if len(opportunities) >= limit:
            break
    return {"items": opportunities, "total": len(opportunities)}


def _product_performance(session: CommercialReadSession, scope: SalesScope, limit: int) -> dict[str, Any]:
    base_cte, base_params = scope.sales_base_cte()
    f = scope.filters
    seller_extra = ""
    seller_params: list[Any] = []
    if f.seller and str(f.seller).strip():
        seller_extra = "AND sb.seller_name = %s"
        seller_params.append(f.seller.strip())

    top_params = list(base_params) + [f.date_from, f.date_to] + seller_params + [limit]
    sql_top = f"""
        WITH {base_cte},
        sale_docs AS (
            SELECT sb.document_id, sb.client_id
            FROM sales_base sb
            WHERE sb.sale_day BETWEEN %s AND %s
              {seller_extra}
        )
        SELECT
            COALESCE(dd.variant_description, dd.variant_code, 'Sin nombre') AS producto,
            dd.variant_id,
            COALESCE(SUM(dd.quantity), 0) AS unidades,
            COUNT(DISTINCT sd.client_id)::bigint AS clientes,
            COALESCE(SUM(dd.total_amount), 0) AS venta
        FROM sale_docs sd
        INNER JOIN distribuidora.document_details dd ON dd.document_id = sd.document_id
        GROUP BY dd.variant_id, dd.variant_description, dd.variant_code
        ORDER BY venta DESC NULLS LAST
        LIMIT %s
    """
    top = session.query_all("product_top", sql_top, tuple(top_params))

    gap_params = (
        list(base_params) + [f.date_from, f.date_to]
        + list(base_params) + [f.date_from, f.date_to]
        + seller_params + [min(50, limit)]
    )
    gap_sql = f"""
        WITH {base_cte},
        company_prod AS (
            SELECT
                dd.variant_id,
                COALESCE(dd.variant_description, dd.variant_code, 'Sin nombre') AS producto,
                COUNT(DISTINCT sb.client_id)::bigint AS clientes_empresa
            FROM sales_base sb
            INNER JOIN distribuidora.document_details dd ON dd.document_id = sb.document_id
            WHERE sb.sale_day BETWEEN %s AND %s
            GROUP BY dd.variant_id, dd.variant_description, dd.variant_code
            HAVING COUNT(DISTINCT sb.client_id) >= 10
        ),
        seller_prod AS (
            SELECT
                dd.variant_id,
                COUNT(DISTINCT sb.client_id)::bigint AS clientes_vendedor
            FROM sales_base sb
            INNER JOIN distribuidora.document_details dd ON dd.document_id = sb.document_id
            WHERE sb.sale_day BETWEEN %s AND %s
              {seller_extra}
            GROUP BY dd.variant_id
        )
        SELECT
            cp.producto,
            cp.variant_id,
            cp.clientes_empresa,
            COALESCE(sp.clientes_vendedor, 0)::bigint AS clientes_vendedor,
            cp.clientes_empresa - COALESCE(sp.clientes_vendedor, 0) AS brecha
        FROM company_prod cp
        LEFT JOIN seller_prod sp ON sp.variant_id = cp.variant_id
        WHERE COALESCE(sp.clientes_vendedor, 0) < cp.clientes_empresa * 0.3
        ORDER BY brecha DESC NULLS LAST
        LIMIT %s
    """
    gaps = session.query_all("product_gaps", gap_sql, tuple(gap_params))

    cmp_params = (
        list(base_params) + [f.date_from, f.date_to] + seller_params
        + list(base_params) + [scope.prev_from, scope.prev_to] + seller_params
        + [30]
    )
    cmp_sql = f"""
        WITH {base_cte},
        curr AS (
            SELECT
                dd.variant_id,
                MAX(COALESCE(dd.variant_description, dd.variant_code, 'Producto')) AS producto,
                COALESCE(SUM(dd.total_amount), 0) AS venta
            FROM sales_base sb
            INNER JOIN distribuidora.document_details dd ON dd.document_id = sb.document_id
            WHERE sb.sale_day BETWEEN %s AND %s
              {seller_extra}
            GROUP BY dd.variant_id
        ),
        prev AS (
            SELECT
                dd.variant_id,
                MAX(COALESCE(dd.variant_description, dd.variant_code, 'Producto')) AS producto,
                COALESCE(SUM(dd.total_amount), 0) AS venta
            FROM sales_base sb
            INNER JOIN distribuidora.document_details dd ON dd.document_id = sb.document_id
            WHERE sb.sale_day BETWEEN %s AND %s
              {seller_extra}
            GROUP BY dd.variant_id
        )
        SELECT
            COALESCE(c.variant_id, p.variant_id) AS variant_id,
            COALESCE(c.producto, p.producto) AS producto,
            COALESCE(c.venta, 0) AS venta_actual,
            COALESCE(p.venta, 0) AS venta_anterior
        FROM curr c
        FULL OUTER JOIN prev p ON p.variant_id = c.variant_id
        WHERE COALESCE(p.venta, 0) > 0
        ORDER BY (COALESCE(c.venta, 0) - COALESCE(p.venta, 0)) ASC
        LIMIT %s
    """
    cmp_rows = session.query_all("product_compare", cmp_sql, tuple(cmp_params))

    return {
        "seller": f.seller,
        "top_products": [
            {
                "producto": r.get("producto"),
                "variant_id": _int(r.get("variant_id")) if r.get("variant_id") else None,
                "unidades": _float(r.get("unidades")),
                "clientes": _int(r.get("clientes")),
                "venta": _float(r.get("venta")),
            }
            for r in top
        ],
        "oportunidades": [
            {
                "producto": r.get("producto"),
                "variant_id": _int(r.get("variant_id")) if r.get("variant_id") else None,
                "clientes_empresa": _int(r.get("clientes_empresa")),
                "clientes_vendedor": _int(r.get("clientes_vendedor")),
                "brecha": _int(r.get("brecha")),
            }
            for r in gaps
        ],
        "caida_fuerte": [
            {
                "variant_id": _int(r.get("variant_id")) if r.get("variant_id") else None,
                "producto": r.get("producto"),
                "venta_actual": _float(r.get("venta_actual")),
                "venta_anterior": _float(r.get("venta_anterior")),
                "variacion_pct": round(
                    _delta(_float(r.get("venta_actual")), _float(r.get("venta_anterior")))["delta_pct"], 1
                ),
            }
            for r in cmp_rows
            if _float(r.get("venta_actual")) < _float(r.get("venta_anterior")) * 0.5
        ],
    }


def _build_dashboard(
    scope: SalesScope,
    classification: dict[str, int],
    curr_kpi: dict[str, Any],
    prev_kpi: dict[str, Any],
    daily: list[dict[str, Any]],
) -> dict[str, Any]:
    f = scope.filters
    kpis = {
        "venta_neta": _delta(curr_kpi["venta_neta"], prev_kpi["venta_neta"]),
        "clientes_unicos": _delta(curr_kpi["clientes_unicos"], prev_kpi["clientes_unicos"]),
        "clientes_nuevos": {
            "current": classification["nuevos"],
            "previous": 0,
            "delta_abs": classification["nuevos"],
            "delta_pct": 0.0,
            "trend": "up" if classification["nuevos"] else "flat",
        },
        "clientes_recuperados": {
            "current": classification["recuperados"],
            "previous": 0,
            "delta_abs": classification["recuperados"],
            "delta_pct": 0.0,
            "trend": "up" if classification["recuperados"] else "flat",
        },
        "clientes_perdidos": {
            "current": classification["perdidos"],
            "previous": 0,
            "delta_abs": classification["perdidos"],
            "delta_pct": 0.0,
            "trend": "down" if classification["perdidos"] else "flat",
        },
        "ticket_promedio": _delta(curr_kpi["ticket_promedio"], prev_kpi["ticket_promedio"]),
        "documentos_emitidos": _delta(curr_kpi["documentos_emitidos"], prev_kpi["documentos_emitidos"]),
        "unidades_vendidas": _delta(curr_kpi["unidades_vendidas"], prev_kpi["unidades_vendidas"]),
        "productos_distintos": _delta(curr_kpi["productos_distintos"], prev_kpi["productos_distintos"]),
    }
    return {
        "period": {"from": f.date_from.isoformat(), "to": f.date_to.isoformat()},
        "compare_period": {"from": scope.prev_from.isoformat(), "to": scope.prev_to.isoformat()},
        "document_types": {"boleta": DOC_BOLETA, "factura": DOC_FACTURA, "nota_credito_excluded": 9},
        "kpis": kpis,
        "client_classification": classification,
        "daily_sales": daily,
    }


def _mark_section_available(result: dict[str, Any]) -> dict[str, Any]:
    return {**result, "available": True}


def _section_error_payload(fallback: dict[str, Any], exc: Exception) -> dict[str, Any]:
    return {**fallback, "available": False, "error": str(exc)}


def _run_secondary_bundle_section(
    session: CommercialReadSession,
    query_name: str,
    fn: Callable[[], dict[str, Any]],
    *,
    fallback: dict[str, Any],
) -> dict[str, Any]:
    """Ejecuta consulta secundaria; degradación graceful si falla (no HTTP 500)."""
    try:
        return _mark_section_available(fn())
    except Exception as exc:
        session.record_bundle_failure(query_name, exc)
        logger.exception("[COMMERCIAL_BUNDLE] secondary_query_failed query=%s", query_name)
        return _section_error_payload(fallback, exc)


def _run_secondary_bundle_value(
    session: CommercialReadSession,
    query_name: str,
    fn: Callable[[], T],
    *,
    fallback: T,
) -> T:
    try:
        return fn()
    except Exception as exc:
        session.record_bundle_failure(query_name, exc)
        logger.exception("[COMMERCIAL_BUNDLE] secondary_query_failed query=%s", query_name)
        return fallback


def build_commercial_bundle(
    filters: CommercialFilters,
    limits: BundleLimits | None = None,
) -> dict[str, Any]:
    """Una sesión, sales_base compartido, clasificación calculada una sola vez."""
    from backend.services.commercial_analytics_intelligence import (
        build_actionable_summary,
        build_attack_plan,
        build_meta,
        build_opportunities,
        compute_seller_scores,
    )

    lim = limits or BundleLimits()
    scope = SalesScope.from_filters(filters)
    t0 = time.perf_counter()
    bundle_complete = True

    seller_fallback: dict[str, Any] = {
        "items": [],
        "rankings": {
            "mayor_venta": [],
            "mayor_crecimiento": [],
            "mayor_recuperacion": [],
            "mayor_perdida": [],
            "mejor_cobertura": [],
        },
    }
    unique_fallback: dict[str, Any] = {"items": [], "summary": {}}
    lost_fallback: dict[str, Any] = {"items": []}
    cross_fallback: dict[str, Any] = {"items": [], "total": 0}
    products_fallback: dict[str, Any] = {"top_products": [], "oportunidades": []}

    with CommercialReadSession("bundle") as session:
        # Consultas críticas (sales_base): fallo → excepción → HTTP 500
        try:
            classification = _client_classification_merged(session, scope)
            curr_kpi = _period_kpis_merged(session, scope, filters.date_from, filters.date_to, "curr")
            prev_kpi = _period_kpis_merged(session, scope, scope.prev_from, scope.prev_to, "prev")
            daily = _daily_sales(session, scope)
        except Exception as exc:
            session.record_bundle_failure("sales_base", exc)
            logger.exception("[COMMERCIAL_BUNDLE] critical_query_failed query=sales_base")
            raise

        dashboard = _build_dashboard(scope, classification, curr_kpi, prev_kpi, daily)

        seller_data = _run_secondary_bundle_section(
            session,
            "seller_performance",
            lambda: _seller_performance(session, scope, lim.seller),
            fallback=seller_fallback,
        )
        scored_sellers = compute_seller_scores(seller_data.get("items") or [])
        seller_payload = {
            **seller_data,
            "items": scored_sellers,
            "client_classification_total": classification,
            "period": dashboard["period"],
            "compare_period": dashboard["compare_period"],
        }

        unique = _run_secondary_bundle_section(
            session,
            "unique_clients",
            lambda: _unique_clients(session, scope, lim.unique_clients),
            fallback=unique_fallback,
        )
        lost = _run_secondary_bundle_section(
            session,
            "lost_clients",
            lambda: _lost_clients(session, scope, lim.lost_clients),
            fallback=lost_fallback,
        )
        cross = _run_secondary_bundle_section(
            session,
            "cross_selling",
            lambda: _cross_selling(session, scope, lim.cross_selling),
            fallback=cross_fallback,
        )
        products = _run_secondary_bundle_section(
            session,
            "product_performance",
            lambda: _product_performance(session, scope, lim.products),
            fallback=products_fallback,
        )

        if session.queries_failed > 0:
            bundle_complete = False

        month_label = filters.date_to.strftime("%B %Y")
        lost_items = lost.get("items") or []
        unique_items = unique.get("items") or []
        cross_items = cross.get("items") or []

        summary = build_actionable_summary(
            f"Resumen Comercial — {month_label}",
            scored_sellers,
            lost_items,
            unique_items,
            products,
            cross_items,
            classification,
            dashboard["kpis"],
        )
        summary["period"] = dashboard["period"]
        summary["compare_period"] = dashboard["compare_period"]

        attack_plan = build_attack_plan(
            scored_sellers,
            lost_items,
            unique_items,
            cross_items,
            products,
        )
        opportunities = build_opportunities(
            lost_items,
            unique_items,
            cross_items,
            products,
            scored_sellers,
        )

        today_row = _run_secondary_bundle_value(
            session,
            "today_sales",
            lambda: _today_sales(session, scope),
            fallback=None,
        )
        monthly_timeline = _run_secondary_bundle_value(
            session,
            "monthly_timeline",
            lambda: _monthly_timeline(session, scope),
            fallback=[],
        )
        recovered_items = [x for x in unique_items if x.get("status") == "recuperado"][: lim.lost_clients]
        rows_analyzed = session.rows_analyzed
        sql_metrics_public = [
            {"name": m["name"], "execution_ms": m["execution_ms"]}
            for m in session.sql_metrics
        ]
        health = session.health_payload(bundle_complete=bundle_complete)

    execution_ms = (time.perf_counter() - t0) * 1000
    meta = build_meta(
        filters=filters,
        scope=scope,
        execution_ms=execution_ms,
        rows_analyzed=rows_analyzed,
        documents_analyzed=_int(curr_kpi.get("documentos_emitidos")),
        clients_analyzed=len(unique_items),
        products_analyzed=_int(curr_kpi.get("productos_distintos")),
    )
    meta["sql_metrics"] = sql_metrics_public
    meta["health"] = health

    from backend.services.commercial_crm_intelligence import build_crm_layer

    crm = build_crm_layer(
        filters=filters,
        scope=scope,
        curr_kpi=curr_kpi,
        prev_kpi=prev_kpi,
        daily=daily,
        classification=classification,
        dashboard_kpis=dashboard["kpis"],
        summary=summary,
        attack_plan=attack_plan,
        opportunities=opportunities,
        sellers=scored_sellers,
        unique=unique_items,
        lost=lost_items,
        cross=cross_items,
        products=products,
        today_row=today_row,
        monthly_timeline=monthly_timeline,
    )
    opportunities = crm.get("opportunities", opportunities)
    alerts = crm.get("alerts", [])

    return {
        "meta": meta,
        "crm": crm,
        "alerts": alerts,
        "dashboard": dashboard,
        "summary": summary,
        "attack_plan": attack_plan,
        "opportunities": opportunities,
        "seller_performance": seller_payload,
        "unique_clients": unique,
        "lost_clients": lost,
        "recovered_clients": {"items": recovered_items, "total": len(recovered_items)},
        "cross_selling": cross,
        "product_performance": products,
    }
