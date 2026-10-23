#!/usr/bin/env python3
"""
Auditoría Comercial 8.0 — censo + Fase 1.3 (solo lectura).

Usa la misma conexión PostgreSQL que FastAPI (``backend.db.get_connection``).
No escribe datos, no abre endpoints, no imprime credenciales.
No ejecuta sync de Bsale (solo documenta el procedimiento).

Coolify (WORKDIR=/app):

  cd /app
  python -m backend.scripts.audit_commercial_cost_census \\
    --company-id 3 --timeout-sec 180 --top-critical 30 --extended --json

Opciones:
  --timeout-sec 180   statement_timeout (default 180 en análisis extendido)
  --top-critical 30   ranking de anomalías / críticos
  --extended          activos costo 0 + divergencias costo>0 (default: on)
  --no-extended       sólo censo A–D / huérfanas
  --json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from psycopg2.extensions import ISOLATION_LEVEL_REPEATABLE_READ

from backend.db import get_connection
from backend.services.analytics.validate_distribuidora_source import assert_sql_is_read_only
from backend.services.analytics_v2.movement_classification import (
    comparable_sql_list,
    movement_class_sql,
)

DEFAULT_TIMEOUT_SEC = 180
DEFAULT_TOP = 30
DEFAULT_PRICE_LIST = 12
COMPANY_DEFAULT = 3
PRICE_LISTS_COMMERCIAL = (12, 13, 16)

# Umbrales de alerta (investigación; no reglas comerciales definitivas).
DIFF_RELEVANT_PCT = Decimal("15")
DIFF_IMPORTANT_PCT = Decimal("50")  # AVERAGE_DIVERGENCE_PCT del motor
DIFF_EXTREME_UP_PCT = Decimal("100")
DIFF_EXTREME_DOWN_PCT = Decimal("-60")

REQUIRED_COST_TABLE = "bsale_raw.variant_costs"
OPTIONAL_TABLES = (
    "bsale_raw.products",
    "bsale_raw.product_types",
    "bsale_raw.variant_prices",
    "bsale_raw.stock_receptions",
    "bsale_raw.stock_reception_details",
)

ORPHAN_RESOLUTION_PROCEDURE = {
    "summary": (
        "Las huérfanas 31352–31366 (y 31358) aparecieron en stocks después del "
        "último FULL de variants (max bsale_id=31351). Resolver con el sync de "
        "catálogo existente; no inventar filas ni borrar stocks."
    ),
    "recommended_coolify_commands": [
        "# 1) Dry-run catálogo diario (products → variants) — sin escritura",
        "cd /app && python -m backend.jobs.bsale_raw sync-catalog --company 3 --dry-run",
        "# 2) Aplicar (autorización explícita del operador)",
        "cd /app && python -m backend.jobs.bsale_raw sync-catalog --company 3",
        "# Alternativa mínima: sólo variants (exige products SUCCESS reciente)",
        "cd /app && python -m backend.jobs.bsale_raw sync --company 3 --resource variants --mode full-reconcile --dry-run",
        "cd /app && python -m backend.jobs.bsale_raw sync --company 3 --resource variants --mode full-reconcile",
        "# 3) Tras aparecer en variants: refrescar costos de esos IDs (POINT)",
        (
            "cd /app && python -m backend.jobs.bsale_raw refresh-costs --company 3 "
            "--variant 31352 --variant 31353 --variant 31354 --variant 31355 "
            "--variant 31356 --variant 31357 --variant 31358 --variant 31359 "
            "--variant 31360 --variant 31361 --variant 31362 --variant 31363 "
            "--variant 31364 --variant 31365 --variant 31366 --dry-run"
        ),
        (
            "cd /app && python -m backend.jobs.bsale_raw refresh-costs --company 3 "
            "--variant 31352 --variant 31353 --variant 31354 --variant 31355 "
            "--variant 31356 --variant 31357 --variant 31358 --variant 31359 "
            "--variant 31360 --variant 31361 --variant 31362 --variant 31363 "
            "--variant 31364 --variant 31365 --variant 31366"
        ),
        "# 4) Verificar: re-ejecutar esta auditoría y confirmar stock_pos_sin_variante=0",
    ],
    "notes": [
        "sync-catalog / sync variants SÍ escriben bsale_raw (no es esta auditoría).",
        "El scanner scan-costs solo itera filas ya presentes en bsale_raw.variants; "
        "por eso hay que sincronizar catálogo antes de que el scanner las cubra.",
        "No ejecutar sync automáticamente desde este script.",
        "Cron habitual: sync-catalog / sync-nightly en Coolify (ver jobs/bsale_raw/README).",
    ],
}

EXCEL_FIELD_MAP_1_3 = {
    "base_exportadores": (
        "backend.services.analytics_v2.excel_export "
        "(catalog_xlsx / costs_xlsx / margins_xlsx / receptions_xlsx) — "
        "ampliar hojas, no crear exporter paralelo."
    ),
    "columnas_comunes": [
        "company_id",
        "product_id",
        "product_name",
        "product_type",
        "variant_id",
        "variant_name",
        "sku",
        "barcode",
        "average_cost_net",
        "last_comparable_cost",
        "last_comparable_date",
        "last_comparable_document",
        "last_comparable_document_number",
        "stock_total",
        "price_list_12_gross",
        "price_list_13_gross",
        "price_list_16_gross",
        "evidence_quality",
        "review_priority / priority_band",
        "priority_score",
        "error_confidence",
        "monetary_exposure_potential (orientativa; no valorización)",
        "unit_presentation_verified",
        "suggested_admin_action",
        "admin_status",
        "admin_owner",
        "admin_notes",
    ],
    "hoja_1_resumen_ejecutivo": [
        "census.*",
        "answers.*",
        "cost_classes",
        "zero_cost_actives.summary",
        "positive_cost_divergence.buckets",
        "sync_state",
        "cut_at_utc",
    ],
    "hoja_2_criticos_anomalias": [
        "positive_cost_divergence.top_anomalies (+ admin_priority)",
        "top_critical",
        "ordenar por priority_score DESC",
    ],
    "hoja_3_activos_costo_cero": [
        "zero_cost_actives.by_price_stock (A/B)",
        "zero_cost_actives.by_reception (C/D)",
        "detalle: stock_cero_explicito vs sin_fila_stock",
        "zero_cost_actives.samples",
    ],
    "hoja_4_promedio_vs_recepciones": [
        "positive_cost_divergence.buckets",
        "bandas + extrema_indicador",
        "top_anomalies con documento/fecha/oficina",
    ],
    "hoja_5_huerfanas_sync": [
        "orphan_stock_variants",
        "orphan_resolution_procedure",
        "census.stock_pos_sin_variante",
        "sync_state variants/stocks/costs",
    ],
    "hoja_6_revision_administrativa": [
        "plantilla: variant_id, sku, producto, prioridad, exposición_potencial, "
        "estado, responsable, fecha, observación, respaldo, solución",
        "alimentar desde top_anomalies + zero_cost samples + huérfanas",
    ],
}


def _json_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return str(obj)
    # datetime antes que date: datetime es subclase de date.
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, (bytes, bytearray)):
        return None
    raise TypeError(type(obj).__name__)


def _row_get(row: dict[str, Any], key: str, default: Any = None) -> Any:
    """Lectura tolerante a case-folding de aliases PostgreSQL (sin comillas → minúsculas)."""
    if key in row:
        return row[key]
    lk = key.lower()
    for k, v in row.items():
        if str(k).lower() == lk:
            return v
    return default


def _row_int(row: dict[str, Any], key: str, default: int = 0) -> int:
    val = _row_get(row, key)
    if val is None:
        return default
    return int(val)


def _open_readonly_tx(conn: Any, *, timeout_sec: int) -> Any:
    """TX READ ONLY + REPEATABLE READ + timeouts locales. Caller debe ROLLBACK."""
    conn.set_session(
        readonly=True,
        isolation_level=ISOLATION_LEVEL_REPEATABLE_READ,
        autocommit=False,
    )
    cur = conn.cursor()
    # SET LOCAL solo aplica dentro de la TX abierta (autocommit=False).
    cur.execute(f"SET LOCAL statement_timeout = '{int(timeout_sec) * 1000}'")
    cur.execute("SET LOCAL lock_timeout = '5s'")
    cur.execute("SET LOCAL idle_in_transaction_session_timeout = '180000'")
    return cur


def _fetch_dicts(cur: Any, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    assert_sql_is_read_only(sql)
    cur.execute(sql, params)
    if not cur.description:
        return []
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _fetch_one(cur: Any, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any]:
    rows = _fetch_dicts(cur, sql, params)
    return rows[0] if rows else {}


def _can_select(cur: Any, regclass: str) -> bool:
    row = _fetch_one(
        cur,
        "SELECT has_table_privilege(current_user, %s::regclass, 'SELECT') AS ok",
        (regclass,),
    )
    return bool(row.get("ok"))


def probe_privileges(cur: Any) -> dict[str, Any]:
    out: dict[str, Any] = {
        "current_user": _fetch_one(cur, "SELECT current_user AS u")["u"],
        "session_user": _fetch_one(cur, "SELECT session_user AS u")["u"],
        "tables": {},
    }
    for tbl in (
        "bsale_raw.variants",
        "bsale_raw.stocks",
        "bsale_raw.sync_state",
        REQUIRED_COST_TABLE,
        *OPTIONAL_TABLES,
    ):
        try:
            out["tables"][tbl] = _can_select(cur, tbl)
        except Exception as exc:
            out["tables"][tbl] = False
            out.setdefault("privilege_errors", []).append(f"{tbl}: {exc}")
    return out


def sync_freshness(cur: Any, company_id: int) -> list[dict[str, Any]]:
    return _fetch_dicts(
        cur,
        """
        SELECT resource, scope, status, last_success_at, last_attempt_at,
               last_error_at, left(coalesce(last_error, ''), 160) AS last_error,
               rows_received, updated_at
        FROM bsale_raw.sync_state
        WHERE company_id = %s
          AND resource IN (
            'variants', 'variant_costs', 'variant_prices', 'stocks',
            'stock_receptions', 'products', 'taxes'
          )
        ORDER BY resource, scope
        """,
        (company_id,),
    )


def census_variants_stocks(cur: Any, company_id: int) -> dict[str, Any]:
    """Censo único: cierra matemáticamente activas/inactivas/huérfanas de stock."""
    return _fetch_one(
        cur,
        """
        WITH st AS (
          SELECT company_id, variant_id,
                 sum(quantity) AS stock_total,
                 count(*) AS oficinas
          FROM bsale_raw.stocks
          WHERE company_id = %s
          GROUP BY company_id, variant_id
        ),
        v AS (
          SELECT company_id, bsale_id AS variant_id, COALESCE(state, 0) AS state
          FROM bsale_raw.variants
          WHERE company_id = %s
            AND missing_since IS NULL
        )
        SELECT
          (SELECT count(*) FROM v) AS variantes,
          (SELECT count(*) FROM v WHERE state = 0) AS activas,
          (SELECT count(*) FROM v WHERE state <> 0) AS inactivas,
          (SELECT count(*) FROM st) AS con_fila_stock,
          (SELECT count(*) FROM st WHERE stock_total > 0) AS stock_pos,
          (SELECT count(*) FROM st WHERE stock_total = 0) AS stock_cero,
          (SELECT count(*) FROM st WHERE stock_total < 0) AS stock_neg,
          (SELECT count(*) FROM v
             INNER JOIN st USING (company_id, variant_id)
           WHERE v.state = 0 AND st.stock_total > 0) AS activas_stock_pos,
          (SELECT count(*) FROM v
             INNER JOIN st USING (company_id, variant_id)
           WHERE v.state <> 0 AND st.stock_total > 0) AS inactivas_stock_pos,
          (SELECT count(*) FROM st
             LEFT JOIN v USING (company_id, variant_id)
           WHERE st.stock_total > 0 AND v.variant_id IS NULL) AS stock_pos_sin_variante,
          (SELECT count(*) FROM v
             LEFT JOIN st USING (company_id, variant_id)
           WHERE v.state = 0 AND st.variant_id IS NULL) AS activas_sin_fila_stock,
          (SELECT coalesce(sum(stock_total), 0) FROM st WHERE stock_total > 0)
            AS unidades_stock_pos,
          (SELECT coalesce(sum(st.stock_total), 0)
             FROM st LEFT JOIN v USING (company_id, variant_id)
             WHERE st.stock_total > 0 AND v.variant_id IS NULL)
            AS unidades_stock_pos_sin_variante
        """,
        (company_id, company_id),
    )


def orphan_stock_variants(cur: Any, company_id: int) -> list[dict[str, Any]]:
    return _fetch_dicts(
        cur,
        """
        WITH st AS (
          SELECT variant_id, sum(quantity) AS stock_total, count(*) AS oficinas
          FROM bsale_raw.stocks
          WHERE company_id = %s
          GROUP BY variant_id
          HAVING sum(quantity) > 0
        )
        SELECT st.variant_id, st.stock_total, st.oficinas
        FROM st
        WHERE NOT EXISTS (
          SELECT 1 FROM bsale_raw.variants v
          WHERE v.company_id = %s AND v.bsale_id = st.variant_id
        )
        ORDER BY st.stock_total DESC, st.variant_id
        """,
        (company_id, company_id),
    )


def cost_coverage(cur: Any, company_id: int) -> dict[str, Any]:
    """Cobertura del scanner: filas en variant_costs vs universo de variantes."""
    return _fetch_one(
        cur,
        """
        SELECT
          (SELECT count(*) FROM bsale_raw.variants
            WHERE company_id = %s AND missing_since IS NULL) AS variantes,
          (SELECT count(*) FROM bsale_raw.variant_costs
            WHERE company_id = %s) AS filas_variant_costs,
          (SELECT count(*) FROM bsale_raw.variant_costs
            WHERE company_id = %s AND average_cost IS NULL) AS costs_average_null,
          (SELECT count(*) FROM bsale_raw.variant_costs
            WHERE company_id = %s AND average_cost = 0) AS costs_average_zero,
          (SELECT count(*) FROM bsale_raw.variant_costs
            WHERE company_id = %s AND average_cost > 0) AS costs_average_pos,
          (SELECT count(*) FROM bsale_raw.variant_costs
            WHERE company_id = %s AND average_cost < 0) AS costs_average_neg,
          (SELECT min(api_fetched_at) FROM bsale_raw.variant_costs
            WHERE company_id = %s) AS cost_fetched_min,
          (SELECT max(api_fetched_at) FROM bsale_raw.variant_costs
            WHERE company_id = %s) AS cost_fetched_max
        """,
        (company_id,) * 8,
    )


def cost_class_cross_stock(cur: Any, company_id: int) -> list[dict[str, Any]]:
    """Clases A / B1 / B2 / C / D cruzadas con Stock Total (sin duplicar por oficina)."""
    return _fetch_dicts(
        cur,
        """
        WITH st AS (
          SELECT variant_id, sum(quantity) AS stock_total
          FROM bsale_raw.stocks
          WHERE company_id = %s
          GROUP BY variant_id
        ),
        base AS (
          SELECT
            v.bsale_id AS variant_id,
            COALESCE(v.state, 0) AS variant_state,
            vc.variant_id AS cost_row_id,
            vc.average_cost,
            CASE
              WHEN vc.variant_id IS NULL THEN 'B1_sin_registro'
              WHEN vc.average_cost IS NULL THEN 'B2_null'
              WHEN vc.average_cost > 0 THEN 'A_positivo'
              WHEN vc.average_cost = 0 THEN 'C_cero'
              WHEN vc.average_cost < 0 THEN 'D_negativo'
            END AS clase_costo,
            st.stock_total
          FROM bsale_raw.variants v
          LEFT JOIN bsale_raw.variant_costs vc
            ON vc.company_id = v.company_id AND vc.variant_id = v.bsale_id
          LEFT JOIN st ON st.variant_id = v.bsale_id
          WHERE v.company_id = %s
            AND v.missing_since IS NULL
        )
        SELECT
          clase_costo,
          count(*)::bigint AS variantes,
          count(*) FILTER (WHERE variant_state = 0)::bigint AS activas,
          count(*) FILTER (WHERE variant_state <> 0)::bigint AS inactivas,
          count(*) FILTER (WHERE COALESCE(stock_total, 0) > 0)::bigint AS stock_pos,
          count(*) FILTER (WHERE stock_total = 0)::bigint AS stock_cero,
          count(*) FILTER (WHERE stock_total IS NULL)::bigint AS sin_fila_stock,
          coalesce(sum(stock_total) FILTER (WHERE stock_total > 0), 0) AS unidades_stock_pos,
          count(*) FILTER (
            WHERE variant_state = 0
              AND COALESCE(stock_total, 0) > 0
              AND clase_costo IN ('B1_sin_registro', 'B2_null', 'C_cero', 'D_negativo')
          )::bigint AS critico_activo_stock_sin_costo_valido
        FROM base
        GROUP BY clase_costo
        ORDER BY clase_costo
        """,
        (company_id, company_id),
    )


def critical_active_stock_no_valid_cost(
    cur: Any,
    company_id: int,
    *,
    limit: int,
    price_list_id: int,
    can_products: bool,
    can_product_types: bool,
    can_prices: bool,
    can_receptions: bool,
) -> list[dict[str, Any]]:
    """Top N: activas, Stock Total > 0, sin costo válido (B1/B2/C/D)."""
    product_join = ""
    product_cols = "NULL::text AS product_name, NULL::text AS product_type_name,"
    if can_products:
        product_join = """
          LEFT JOIN bsale_raw.products p
            ON p.company_id = v.company_id AND p.bsale_id = v.product_id
        """
        product_cols = "p.name AS product_name, NULL::text AS product_type_name,"
        if can_product_types:
            product_join += """
          LEFT JOIN bsale_raw.product_types pt
            ON pt.company_id = p.company_id AND pt.bsale_id = p.product_type_id
            """
            product_cols = "p.name AS product_name, pt.name AS product_type_name,"

    price_join = ""
    price_cols = "NULL::numeric AS precio_bruto_lista,"
    if can_prices:
        price_join = """
          LEFT JOIN bsale_raw.variant_prices vp
            ON vp.company_id = v.company_id
           AND vp.variant_id = v.bsale_id
           AND vp.price_list_id = %s
           AND vp.missing_since IS NULL
        """
        price_cols = "vp.variant_value_with_taxes AS precio_bruto_lista,"

    reception_join = ""
    reception_cols = (
        "NULL::date AS ultima_recepcion_comparable, NULL::numeric AS costo_ultima_comparable,"
    )
    if can_receptions:
        # Última línea comparable: costo>0, cantidad>0, details_complete, no transferencia
        # (misma idea del motor; simplificada para auditoría).
        reception_join = """
          LEFT JOIN LATERAL (
            SELECT d.cost, r.admission_date
            FROM bsale_raw.stock_reception_details d
            INNER JOIN bsale_raw.stock_receptions r
              ON r.company_id = d.company_id AND r.bsale_id = d.reception_id
            WHERE d.company_id = v.company_id
              AND d.variant_id = v.bsale_id
              AND d.missing_since IS NULL
              AND r.missing_since IS NULL
              AND r.details_complete IS TRUE
              AND d.cost > 0 AND d.quantity > 0
              AND coalesce(r.internal_dispatch_id, 0) = 0
            ORDER BY r.admission_date DESC, r.bsale_id DESC, d.bsale_id DESC
            LIMIT 1
          ) lc ON TRUE
        """
        reception_cols = (
            "lc.admission_date AS ultima_recepcion_comparable, "
            "lc.cost AS costo_ultima_comparable,"
        )

    # Parámetros: company (st), price_list si aplica, company (where), limit
    params: list[Any] = [company_id]
    if can_prices:
        params.append(price_list_id)
    params.extend([company_id, limit])

    sql = f"""
        WITH st AS (
          SELECT variant_id, sum(quantity) AS stock_total
          FROM bsale_raw.stocks
          WHERE company_id = %s
          GROUP BY variant_id
        )
        SELECT
          v.bsale_id AS variant_id,
          {product_cols}
          v.description AS variant_name,
          v.code AS sku,
          v.bar_code AS barcode,
          COALESCE(v.state, 0) AS state,
          st.stock_total,
          vc.average_cost,
          CASE
            WHEN vc.variant_id IS NULL THEN 'B1_sin_registro'
            WHEN vc.average_cost IS NULL THEN 'B2_null'
            WHEN vc.average_cost = 0 THEN 'C_cero'
            WHEN vc.average_cost < 0 THEN 'D_negativo'
            ELSE 'A_positivo'
          END AS clase_costo,
          {price_cols}
          {reception_cols}
          vc.api_fetched_at AS costo_sincronizado_at,
          CASE
            WHEN vc.variant_id IS NULL THEN
              'Activa con stock > 0 y sin fila en variant_costs (cobertura scanner / sync; no equivale automáticamente a ausencia en Bsale)'
            WHEN vc.average_cost IS NULL THEN
              'Activa con stock > 0 y average_cost NULL en fila sincronizada'
            WHEN vc.average_cost = 0 THEN
              'Activa con stock > 0 y average_cost explícito = 0'
            WHEN vc.average_cost < 0 THEN
              'Activa con stock > 0 y average_cost negativo'
            ELSE 'N/A'
          END AS motivo_alerta
        FROM bsale_raw.variants v
        INNER JOIN st ON st.variant_id = v.bsale_id AND st.stock_total > 0
        LEFT JOIN bsale_raw.variant_costs vc
          ON vc.company_id = v.company_id AND vc.variant_id = v.bsale_id
        {product_join}
        {price_join}
        {reception_join}
        WHERE v.company_id = %s
          AND v.missing_since IS NULL
          AND COALESCE(v.state, 0) = 0
          AND (
            vc.variant_id IS NULL
            OR vc.average_cost IS NULL
            OR vc.average_cost <= 0
          )
        ORDER BY st.stock_total DESC, v.bsale_id
        LIMIT %s
    """
    return _fetch_dicts(cur, sql, tuple(params))


def grant_instructions(role: str) -> list[str]:
    """GRANT sugerido para rol de auditoría MCP — NUNCA hacia postgres/app."""
    target = "ai_reader"
    warn = []
    if str(role).lower() in {"postgres", "root", "supabase_admin"}:
        warn = [
            f"-- ADVERTENCIA: current_user={role} es rol de aplicación/superusuario.",
            "-- NO ejecutar GRANT hacia ese rol. Destino de auditoría = ai_reader (u otro RO).",
            "-- En producción preferir ejecutar este script en el contenedor FastAPI (ya lee costos).",
        ]
    return [
        *warn,
        "-- Ejecutar como propietario del esquema / DBA.",
        f"-- Destino SELECT mínimo: {target} (no {role}).",
        "-- Solo SELECT; sin GRANT OPTION; sin escritura.",
        f"GRANT SELECT ON TABLE bsale_raw.variant_costs TO {target};",
        f"GRANT SELECT ON TABLE bsale_raw.products TO {target};",
        f"GRANT SELECT ON TABLE bsale_raw.product_types TO {target};",
        f"GRANT SELECT ON TABLE bsale_raw.variant_prices TO {target};",
        f"GRANT SELECT ON TABLE bsale_raw.stock_receptions TO {target};",
        f"GRANT SELECT ON TABLE bsale_raw.stock_reception_details TO {target};",
    ]


def classify_divergence_bucket(
    pct: Decimal | None,
    *,
    has_comparable: bool,
    requires_verification: bool = False,
) -> str:
    """
    Banda principal mutuamente excluyente (para sumatorias).

    ``extrema`` NO es banda de cierre: usar ``is_extreme_divergence``.
    """
    if has_comparable and pct is not None:
        ap = abs(pct)
        if ap <= DIFF_RELEVANT_PCT:
            return "hasta_15"
        if ap <= DIFF_IMPORTANT_PCT:
            return "entre_15_y_50"
        return "sobre_50"
    if requires_verification:
        return "requiere_verificacion"
    return "sin_recepcion_comparable"


def is_extreme_divergence(pct: Decimal | None) -> bool:
    """Indicador adicional (subconjunto de sobre_50): no entra a la suma de cierre."""
    if pct is None:
        return False
    return pct >= DIFF_EXTREME_UP_PCT or pct <= DIFF_EXTREME_DOWN_PCT


def monetary_exposure_potential(
    *,
    average_cost: Decimal | None,
    last_comparable_cost: Decimal | None,
    stock_total: Decimal | None,
) -> Decimal | None:
    """
    Exposición monetaria *potencial* = |última_comparable − promedio| × stock.

    Métrica orientativa de cola de revisión. No es pérdida real ni valorización
    contable definitiva (sobre todo si la unidad de la recepción no está comprobada).
    """
    if average_cost is None or last_comparable_cost is None or stock_total is None:
        return None
    if stock_total <= 0:
        return Decimal("0")
    return abs(last_comparable_cost - average_cost) * stock_total


def score_admin_priority(
    *,
    diff_pct: Decimal | None,
    stock_total: Decimal | None,
    average_cost: Decimal | None,
    last_comparable_cost: Decimal | None,
    last_comparable_date: date | None,
    cut_at: date | None = None,
    has_document: bool = True,
    possible_admin_correction: bool = False,
    unit_presentation_verified: bool = False,
    presentation_risk: bool = False,
) -> dict[str, Any]:
    """
    Ranking administrativo (alerta, no veredicto).

    Separan dos conceptos:
    - ``review_priority`` / ``priority_score``: urgencia de revisar el caso.
    - ``error_confidence``: confianza de que exista un error real de costo.

    Una posible corrección administrativa **no reduce** la prioridad de revisión;
    solo baja la confianza de error.
    """
    abs_pct = abs(diff_pct) if diff_pct is not None else Decimal("0")
    stock = stock_total if stock_total is not None else Decimal("0")
    exposure = monetary_exposure_potential(
        average_cost=average_cost,
        last_comparable_cost=last_comparable_cost,
        stock_total=stock_total,
    )
    age_days: int | None = None
    if last_comparable_date is not None:
        ref = cut_at or date.today()
        age_days = max(0, (ref - last_comparable_date).days)

    # --- Prioridad de revisión (no se reduce por corrección admin) ---
    # Volumen pesa más que un extremo de pocas unidades al ordenar la cola.
    score = Decimal("0")
    score += min(abs_pct, Decimal("200")) * Decimal("0.25")
    score += min(stock, Decimal("5000")) / Decimal("8")
    if exposure is not None and unit_presentation_verified:
        score += min(exposure / Decimal("1000"), Decimal("40"))
    elif exposure is not None:
        # Unidad no comprobada: la exposición orienta poco; no valorizar.
        score += min(exposure / Decimal("1000"), Decimal("15"))
    if age_days is not None:
        if age_days > 365:
            score += Decimal("15")
        elif age_days > 180:
            score += Decimal("10")
    if not has_document:
        score += Decimal("5")
    if presentation_risk:
        score += Decimal("12")  # bidón vs recarga, pack vs unidad, etc.

    if abs_pct >= 100 or stock >= 1000 or (
        exposure is not None and unit_presentation_verified and exposure >= 500_000
    ):
        band = "P1_critico"
    elif abs_pct > 50 or stock >= 500 or (
        exposure is not None and unit_presentation_verified and exposure >= 100_000
    ):
        band = "P2_alto"
    elif abs_pct > 15 or stock >= 100 or presentation_risk:
        band = "P3_medio"
    else:
        band = "P4_bajo"

    evidence_quality = "alta" if has_document and diff_pct is not None else "media"
    if not has_document:
        evidence_quality = "baja"
    if presentation_risk:
        evidence_quality = "riesgo_presentacion_unidad"

    # --- Confianza de error real (independiente de la prioridad de revisión) ---
    if possible_admin_correction or presentation_risk or not unit_presentation_verified:
        error_confidence = "baja"
    elif has_document and abs_pct > DIFF_IMPORTANT_PCT and unit_presentation_verified:
        error_confidence = "media"
    elif has_document and abs_pct > DIFF_RELEVANT_PCT:
        error_confidence = "media_baja"
    else:
        error_confidence = "indeterminada"

    exposure_disclaimer = (
        "Exposición potencial orientativa: no es pérdida financiera ni valorización "
        "contable definitiva."
    )
    if not unit_presentation_verified:
        exposure_disclaimer += (
            " No usar como valorización mientras la unidad/presentación de la "
            "recepción no esté suficientemente comprobada."
        )

    return {
        "review_priority": band,
        "priority_band": band,  # alias estable para Excel / consumidores previos
        "priority_score": round(float(score), 2),
        "error_confidence": error_confidence,
        "abs_diff_pct": float(abs_pct) if diff_pct is not None else None,
        "stock_total": float(stock) if stock_total is not None else None,
        "monetary_exposure_potential": (
            str(exposure.quantize(Decimal("0.01"))) if exposure is not None else None
        ),
        "monetary_exposure_disclaimer": exposure_disclaimer,
        "unit_presentation_verified": unit_presentation_verified,
        "reception_age_days": age_days,
        "evidence_quality": evidence_quality,
        "possible_admin_correction": possible_admin_correction,
        "presentation_risk": presentation_risk,
        "suggested_action": (
            "contrastar_factura_unidad_presentacion"
            if abs_pct >= 50 or presentation_risk
            else "revisar_recepcion_y_promedio"
        ),
    }


def _last_comparable_lateral_sql() -> str:
    """
    Última línea comparable al criterio del motor de costos:

    - misma variante (``d.variant_id = v.bsale_id``)
    - ``details_complete``, costo > 0, cantidad > 0
    - movimiento FACTURA o GUÍA (no NC, ajuste, transferencia, sin documento)
    - sin despacho interno

    El costo de línea se toma tal cual Bsale (unitario); no se recalculan
    impuestos ni descuentos aquí. Sin fila comparable → no hay Δ%.
    """
    mov = movement_class_sql("r.document", "r.internal_dispatch_id")
    comp = comparable_sql_list()
    return f"""
          LEFT JOIN LATERAL (
            SELECT d.cost AS lc_cost,
                   r.admission_date AS lc_date,
                   r.bsale_id AS lc_reception_id,
                   r.document AS lc_document,
                   r.document_number AS lc_document_number,
                   r.office_id AS lc_office_id,
                   ({mov}) AS lc_movement_class
            FROM bsale_raw.stock_reception_details d
            INNER JOIN bsale_raw.stock_receptions r
              ON r.company_id = d.company_id AND r.bsale_id = d.reception_id
            WHERE d.company_id = v.company_id
              AND d.variant_id = v.bsale_id
              AND d.missing_since IS NULL
              AND r.missing_since IS NULL
              AND r.details_complete IS TRUE
              AND d.cost > 0 AND d.quantity > 0
              AND ({mov}) IN ({comp})
            ORDER BY r.admission_date DESC, r.bsale_id DESC, d.bsale_id DESC
            LIMIT 1
          ) lc ON TRUE
    """


def _any_reception_evidence_lateral_sql() -> str:
    """Recepción con costo>0 que no califica como comparable (requiere verificación)."""
    mov = movement_class_sql("r.document", "r.internal_dispatch_id")
    comp = comparable_sql_list()
    return f"""
          LEFT JOIN LATERAL (
            SELECT true AS has_noncomparable_evidence
            FROM bsale_raw.stock_reception_details d
            INNER JOIN bsale_raw.stock_receptions r
              ON r.company_id = d.company_id AND r.bsale_id = d.reception_id
            WHERE d.company_id = v.company_id
              AND d.variant_id = v.bsale_id
              AND d.missing_since IS NULL
              AND r.missing_since IS NULL
              AND d.cost > 0 AND d.quantity > 0
              AND (
                r.details_complete IS NOT TRUE
                OR ({mov}) NOT IN ({comp})
              )
            LIMIT 1
          ) ev ON TRUE
    """


def analyze_zero_cost_actives(
    cur: Any,
    company_id: int,
    *,
    can_prices: bool,
    can_receptions: bool,
    sample_limit: int = 20,
) -> dict[str, Any]:
    """2.xxx activas con average_cost = 0: cruce precio 12/13/16, stock, recepción."""
    price_expr = "FALSE"
    price_join = ""
    if can_prices:
        # Precio vigente = fila no missing + valor bruto con impuestos > 0.
        # Una fila a 0 / NULL no cuenta como «con precio».
        price_join = """
          LEFT JOIN bsale_raw.variant_prices vp12
            ON vp12.company_id = v.company_id AND vp12.variant_id = v.bsale_id
           AND vp12.price_list_id = 12 AND vp12.missing_since IS NULL
          LEFT JOIN bsale_raw.variant_prices vp13
            ON vp13.company_id = v.company_id AND vp13.variant_id = v.bsale_id
           AND vp13.price_list_id = 13 AND vp13.missing_since IS NULL
          LEFT JOIN bsale_raw.variant_prices vp16
            ON vp16.company_id = v.company_id AND vp16.variant_id = v.bsale_id
           AND vp16.price_list_id = 16 AND vp16.missing_since IS NULL
        """
        price_expr = """
          (COALESCE(vp12.variant_value_with_taxes, 0) > 0
            OR COALESCE(vp13.variant_value_with_taxes, 0) > 0
            OR COALESCE(vp16.variant_value_with_taxes, 0) > 0)
        """

    rec_join = _last_comparable_lateral_sql() if can_receptions else ""
    has_rec = "lc.lc_cost IS NOT NULL" if can_receptions else "FALSE"

    sql = f"""
        WITH st AS (
          SELECT variant_id, sum(quantity) AS stock_total
          FROM bsale_raw.stocks WHERE company_id = %s GROUP BY variant_id
        ),
        base AS (
          SELECT
            v.bsale_id AS variant_id,
            v.code AS sku,
            COALESCE(v.state, 0) AS variant_state,
            st.stock_total AS stock_total_raw,
            COALESCE(st.stock_total, 0) AS stock_total,
            (st.variant_id IS NULL) AS sin_fila_stock,
            ({price_expr}) AS has_price,
            ({has_rec}) AS has_comparable_reception
          FROM bsale_raw.variants v
          INNER JOIN bsale_raw.variant_costs vc
            ON vc.company_id = v.company_id AND vc.variant_id = v.bsale_id
           AND vc.average_cost = 0
          LEFT JOIN st ON st.variant_id = v.bsale_id
          {price_join}
          {rec_join}
          WHERE v.company_id = %s
            AND v.missing_since IS NULL
            AND COALESCE(v.state, 0) = 0
        )
        SELECT
          count(*)::bigint AS total_activas_costo_cero,
          count(*) FILTER (WHERE stock_total_raw IS NOT NULL AND stock_total_raw > 0)
            ::bigint AS con_stock_pos,
          count(*) FILTER (
            WHERE stock_total_raw IS NOT NULL AND stock_total_raw = 0
          )::bigint AS stock_cero_explicito,
          count(*) FILTER (WHERE sin_fila_stock)::bigint AS sin_fila_stock,
          count(*) FILTER (
            WHERE sin_fila_stock OR COALESCE(stock_total_raw, 0) = 0
          )::bigint AS sin_stock_positivo,
          count(*) FILTER (
            WHERE has_price AND NOT (COALESCE(stock_total_raw, 0) > 0)
          )::bigint AS grupo_a_precio_sin_stock_pos,
          count(*) FILTER (
            WHERE NOT has_price AND NOT (COALESCE(stock_total_raw, 0) > 0)
          )::bigint AS grupo_b_sin_precio_sin_stock_pos,
          count(*) FILTER (WHERE has_comparable_reception)::bigint AS grupo_c_con_recepcion,
          count(*) FILTER (WHERE NOT has_comparable_reception)::bigint AS grupo_d_sin_recepcion
        FROM base
    """
    summary_raw = _fetch_one(cur, sql, (company_id, company_id))
    total = _row_int(summary_raw, "total_activas_costo_cero")
    con_stock = _row_int(summary_raw, "con_stock_pos")
    stock_cero_exp = _row_int(summary_raw, "stock_cero_explicito")
    sin_fila = _row_int(summary_raw, "sin_fila_stock")
    sin_stock_pos = _row_int(summary_raw, "sin_stock_positivo")
    grupo_a = _row_int(summary_raw, "grupo_a_precio_sin_stock_pos")
    grupo_b = _row_int(summary_raw, "grupo_b_sin_precio_sin_stock_pos")
    grupo_c = _row_int(summary_raw, "grupo_c_con_recepcion")
    grupo_d = _row_int(summary_raw, "grupo_d_sin_recepcion")

    summary = {
        "total_activas_costo_cero": total,
        "con_stock_pos": con_stock,
        "stock_cero_explicito": stock_cero_exp,
        "sin_fila_stock": sin_fila,
        "sin_stock_positivo": sin_stock_pos,
        # Compatibilidad: stock_cero ya NO mezcla sin_fila (era el bug de 2278).
        "stock_cero": stock_cero_exp,
        "grupo_a_precio_sin_stock_pos": grupo_a,
        "grupo_b_sin_precio_sin_stock_pos": grupo_b,
        "grupo_c_con_recepcion": grupo_c,
        "grupo_d_sin_recepcion": grupo_d,
        "stock_identity": {
            "formula": "total = con_stock_pos + stock_cero_explicito + sin_fila_stock",
            "closes": total == con_stock + stock_cero_exp + sin_fila,
            "sin_stock_positivo_closes": sin_stock_pos == stock_cero_exp + sin_fila,
        },
        "price_stock_identity": {
            "formula": "sin_stock_positivo = grupo_a + grupo_b",
            "closes": sin_stock_pos == grupo_a + grupo_b,
        },
        "reception_identity": {
            "formula": "total = grupo_c + grupo_d",
            "closes": total == grupo_c + grupo_d,
        },
    }

    samples_sql = f"""
        WITH st AS (
          SELECT variant_id, sum(quantity) AS stock_total
          FROM bsale_raw.stocks WHERE company_id = %s GROUP BY variant_id
        )
        SELECT
          v.bsale_id AS variant_id,
          v.code AS sku,
          v.description AS variant_name,
          st.stock_total AS stock_total_raw,
          COALESCE(st.stock_total, 0) AS stock_total,
          (st.variant_id IS NULL) AS sin_fila_stock,
          ({price_expr}) AS has_price,
          ({has_rec}) AS has_comparable_reception
        FROM bsale_raw.variants v
        INNER JOIN bsale_raw.variant_costs vc
          ON vc.company_id = v.company_id AND vc.variant_id = v.bsale_id
         AND vc.average_cost = 0
        LEFT JOIN st ON st.variant_id = v.bsale_id
        {price_join}
        {rec_join}
        WHERE v.company_id = %s
          AND v.missing_since IS NULL
          AND COALESCE(v.state, 0) = 0
        ORDER BY
          CASE WHEN ({has_rec}) THEN 0 ELSE 1 END,
          CASE WHEN ({price_expr}) THEN 0 ELSE 1 END,
          COALESCE(st.stock_total, 0) DESC,
          v.bsale_id
        LIMIT %s
    """
    samples = _fetch_dicts(cur, samples_sql, (company_id, company_id, sample_limit))
    return {
        "summary": summary,
        "axes_note": (
            "Ejes independientes: by_price_stock (A/B) y by_reception (C/D) "
            "no son particiones excluyentes entre sí; una variante puede estar "
            "en A y C a la vez."
        ),
        "by_price_stock": {
            "A_con_precio_sin_stock_positivo": grupo_a,
            "B_sin_precio_sin_stock_positivo": grupo_b,
            "con_stock_positivo": con_stock,
            "nota": (
                "A+B = sin_stock_positivo. has_price exige precio vigente "
                "(missing_since IS NULL) y variant_value_with_taxes > 0 en listas 12/13/16."
            ),
            "closes": sin_stock_pos == grupo_a + grupo_b,
        },
        "by_reception": {
            "C_con_recepcion_comparable_promedio_cero": grupo_c,
            "D_sin_recepcion_comparable": grupo_d,
            "nota": "C usa sólo FACTURA/GUÍA comparable (mismo criterio del motor).",
            "closes": total == grupo_c + grupo_d,
        },
        "prices_available": can_prices,
        "receptions_available": can_receptions,
        "samples": samples,
        "sales_recent": None,
        "sales_note": "Sin fuente confiable de ventas 90d; no se declara.",
    }


def analyze_positive_cost_divergence(
    cur: Any,
    company_id: int,
    *,
    can_receptions: bool,
    top_n: int,
) -> dict[str, Any]:
    """Activas + stock>0 + average_cost>0 vs última recepción comparable."""
    if not can_receptions:
        return {
            "blocked": True,
            "reason": "sin SELECT a stock_receptions / details",
            "buckets": {},
            "top_anomalies": [],
        }

    rec_join = _last_comparable_lateral_sql()
    ev_join = _any_reception_evidence_lateral_sql()
    # Bandas principales (mutuamente excluyentes):
    #   sin_recepcion | requiere_verificacion | hasta_15 | entre_15_y_50 | sobre_50
    # Extrema = indicador adicional ⊆ sobre_50 (no suma al cierre).
    sql_buckets = f"""
        WITH st AS (
          SELECT variant_id, sum(quantity) AS stock_total
          FROM bsale_raw.stocks WHERE company_id = %s GROUP BY variant_id
        ),
        base AS (
          SELECT
            v.bsale_id AS variant_id,
            vc.average_cost,
            st.stock_total,
            lc.lc_cost,
            (ev.has_noncomparable_evidence IS TRUE) AS requires_verification,
            CASE
              WHEN lc.lc_cost IS NULL OR lc.lc_cost <= 0 OR vc.average_cost IS NULL
                   OR vc.average_cost <= 0
                THEN NULL
              ELSE round(((lc.lc_cost - vc.average_cost) / vc.average_cost) * 100, 2)
            END AS diff_pct
          FROM bsale_raw.variants v
          INNER JOIN st ON st.variant_id = v.bsale_id AND st.stock_total > 0
          INNER JOIN bsale_raw.variant_costs vc
            ON vc.company_id = v.company_id AND vc.variant_id = v.bsale_id
           AND vc.average_cost > 0
          {rec_join}
          {ev_join}
          WHERE v.company_id = %s
            AND v.missing_since IS NULL
            AND COALESCE(v.state, 0) = 0
        )
        SELECT
          count(*)::bigint AS universo,
          count(*) FILTER (
            WHERE diff_pct IS NULL AND NOT requires_verification
          )::bigint AS sin_recepcion_comparable,
          count(*) FILTER (
            WHERE diff_pct IS NULL AND requires_verification
          )::bigint AS requiere_verificacion,
          count(*) FILTER (
            WHERE diff_pct IS NOT NULL AND abs(diff_pct) <= 15
          )::bigint AS hasta_15,
          count(*) FILTER (
            WHERE diff_pct IS NOT NULL
              AND abs(diff_pct) > 15 AND abs(diff_pct) <= 50
          )::bigint AS entre_15_y_50,
          count(*) FILTER (
            WHERE diff_pct IS NOT NULL AND abs(diff_pct) > 50
          )::bigint AS sobre_50,
          count(*) FILTER (
            WHERE diff_pct IS NOT NULL
              AND (diff_pct >= 100 OR diff_pct <= -60)
          )::bigint AS extrema_indicador
        FROM base
    """
    buckets = _fetch_one(cur, sql_buckets, (company_id, company_id))

    sql_top = f"""
        WITH st AS (
          SELECT variant_id, sum(quantity) AS stock_total
          FROM bsale_raw.stocks WHERE company_id = %s GROUP BY variant_id
        ),
        base AS (
          SELECT
            v.bsale_id AS variant_id,
            v.code AS sku,
            v.description AS variant_name,
            vc.average_cost,
            st.stock_total,
            lc.lc_cost AS last_comparable_cost,
            lc.lc_date AS last_comparable_date,
            lc.lc_reception_id,
            lc.lc_document,
            lc.lc_document_number,
            lc.lc_office_id,
            lc.lc_movement_class,
            CASE
              WHEN lc.lc_cost IS NULL OR vc.average_cost <= 0 THEN NULL
              ELSE round(((lc.lc_cost - vc.average_cost) / vc.average_cost) * 100, 2)
            END AS diff_pct
          FROM bsale_raw.variants v
          INNER JOIN st ON st.variant_id = v.bsale_id AND st.stock_total > 0
          INNER JOIN bsale_raw.variant_costs vc
            ON vc.company_id = v.company_id AND vc.variant_id = v.bsale_id
           AND vc.average_cost > 0
          {rec_join}
          WHERE v.company_id = %s
            AND v.missing_since IS NULL
            AND COALESCE(v.state, 0) = 0
            AND lc.lc_cost IS NOT NULL
        )
        SELECT *,
          CASE
            WHEN abs(diff_pct) <= 15 THEN 'hasta_15'
            WHEN abs(diff_pct) <= 50 THEN 'entre_15_y_50'
            ELSE 'sobre_50'
          END AS banda_principal,
          (diff_pct >= 100 OR diff_pct <= -60) AS extrema_indicador,
          'alerta_investigacion_no_prueba_error' AS interpretacion
        FROM base
        WHERE abs(diff_pct) > 15
        ORDER BY abs(diff_pct) DESC, stock_total DESC, variant_id
        LIMIT %s
    """
    top = _fetch_dicts(cur, sql_top, (company_id, company_id, top_n))
    cut_day = datetime.now(timezone.utc).date()
    enriched: list[dict[str, Any]] = []
    for row in top:
        item = dict(row)
        diff = _row_get(item, "diff_pct")
        avg = _row_get(item, "average_cost")
        lc = _row_get(item, "last_comparable_cost")
        stock = _row_get(item, "stock_total")
        lc_date = _row_get(item, "last_comparable_date")
        if isinstance(diff, (int, float, str)):
            diff = Decimal(str(diff))
        if isinstance(avg, (int, float, str)):
            avg = Decimal(str(avg))
        if isinstance(lc, (int, float, str)):
            lc = Decimal(str(lc))
        if isinstance(stock, (int, float, str)):
            stock = Decimal(str(stock))
        if isinstance(lc_date, datetime):
            lc_date = lc_date.date()
        # Heurística suave: caídas ≥60 % o alzas ≥100 % pueden ser corrección admin
        # (Glenfiddich §5.7); no se declara error. No reduce prioridad de revisión.
        maybe_admin = bool(
            diff is not None
            and isinstance(diff, Decimal)
            and (diff >= DIFF_EXTREME_UP_PCT or diff <= DIFF_EXTREME_DOWN_PCT)
        )
        name = str(_row_get(item, "variant_name") or "").upper()
        presentation_risk = any(
            token in name
            for token in (
                "BIDON RETORNABLE",
                "RECARGA AGUA",
                "10 UND",
                "10 UNID",
            )
        )
        prio = score_admin_priority(
            diff_pct=diff if isinstance(diff, Decimal) else None,
            stock_total=stock if isinstance(stock, Decimal) else None,
            average_cost=avg if isinstance(avg, Decimal) else None,
            last_comparable_cost=lc if isinstance(lc, Decimal) else None,
            last_comparable_date=lc_date if isinstance(lc_date, date) else None,
            cut_at=cut_day,
            has_document=bool(
                _row_get(item, "lc_document") or _row_get(item, "lc_document_number")
            ),
            possible_admin_correction=maybe_admin,
            unit_presentation_verified=False,  # solo tras contraste documental
            presentation_risk=presentation_risk,
        )
        item["admin_priority"] = prio
        enriched.append(item)
    enriched.sort(
        key=lambda r: (
            -float((r.get("admin_priority") or {}).get("priority_score") or 0),
            -abs(float(r.get("diff_pct") or 0)),
            -float(r.get("stock_total") or 0),
        )
    )

    u = _row_int(buckets, "universo")
    primary_keys = (
        "sin_recepcion_comparable",
        "requiere_verificacion",
        "hasta_15",
        "entre_15_y_50",
        "sobre_50",
    )
    s = sum(_row_int(buckets, k) for k in primary_keys)
    extrema = _row_int(buckets, "extrema_indicador")
    sobre = _row_int(buckets, "sobre_50")
    return {
        "blocked": False,
        "diff_definition": (
            "diff_pct = (ultima_comparable - average_cost) / average_cost * 100. "
            "Sólo FACTURA/GUÍA con details_complete, costo>0 y cantidad>0. "
            "Umbrales = alertas de investigación, nunca error confirmado. "
            "Impuestos/descuentos no se recalculan aquí (valores netos Bsale)."
        ),
        "thresholds": {
            "hasta_15": float(DIFF_RELEVANT_PCT),
            "importante_sobre": float(DIFF_IMPORTANT_PCT),
            "extrema_alza_indicador": float(DIFF_EXTREME_UP_PCT),
            "extrema_baja_indicador": float(DIFF_EXTREME_DOWN_PCT),
        },
        "buckets": buckets,
        "buckets_identity": {
            "formula": (
                "universo = sin_recepcion_comparable + requiere_verificacion "
                "+ hasta_15 + entre_15_y_50 + sobre_50"
            ),
            "universo": u,
            "sum_buckets_principales": s,
            "closes": u == s,
            "extrema_indicador": extrema,
            "extrema_es_subconjunto_de_sobre_50": extrema <= sobre,
            "nota": (
                "extrema_indicador no entra en la suma de cierre; es subconjunto de sobre_50."
            ),
        },
        "top_anomalies": enriched,
        "priority_note": (
            "admin_priority ordena revisión; monetary_exposure_potential no es pérdida "
            "ni valorización contable."
        ),
        "possible_admin_correction": (
            "No auto-clasificado como error. Casos extremos / correcciones documentales "
            "(referencia Glenfiddich §5.7) quedan para revisión administrativa manual."
        ),
    }


def build_report(
    *,
    company_id: int,
    timeout_sec: int,
    top_n: int,
    price_list_id: int,
    extended: bool = True,
) -> dict[str, Any]:
    cut = datetime.now(timezone.utc)
    report: dict[str, Any] = {
        "audit": "comercial_8_0_fase_1_3_cost_integrity",
        "company_id": company_id,
        "cut_at_utc": cut.isoformat(),
        "readonly": True,
        "isolation": "REPEATABLE READ",
        "statement_timeout_sec": timeout_sec,
        "extended": extended,
        "ok": False,
        "blocked_on_costs": True,
        "orphan_resolution_procedure": ORPHAN_RESOLUTION_PROCEDURE,
        "excel_field_map": EXCEL_FIELD_MAP_1_3,
        "notes": [],
        "excel_implications": [
            "Separar A / C (cero ≠ desconocido).",
            "No valorizar inventario con costo cero.",
            "Huérfanas: hoja sync; no inventar catálogo.",
            "Divergencias: alertas de investigación, no prueba de error.",
            "No sustituir average_cost por última recepción.",
        ],
    }

    # Solo presencia booleana de env — nunca valores (host, user, password, DSN).
    report["pg_env_present"] = {
        "PG_HOST": bool(os.getenv("PG_HOST")),
        "PG_DB": bool(os.getenv("PG_DB")),
        "PG_USER": bool(os.getenv("PG_USER")),
        "PG_PASSWORD": bool(os.getenv("PG_PASSWORD")),
        "PG_PORT": bool(os.getenv("PG_PORT")),
    }
    if not all(
        report["pg_env_present"][k] for k in ("PG_HOST", "PG_DB", "PG_USER", "PG_PASSWORD")
    ):
        report["error"] = (
            "Faltan variables PG_* en el entorno. Ejecutar dentro del contenedor "
            "backend (Coolify) donde FastAPI ya tiene la conexión configurada."
        )
        report["coolify_command"] = (
            "python -m backend.scripts.audit_commercial_cost_census "
            f"--company-id {company_id} --timeout-sec {timeout_sec} "
            f"--top-critical {top_n} --extended --json"
        )
        return report

    conn = get_connection()
    cur = None
    try:
        cur = _open_readonly_tx(conn, timeout_sec=timeout_sec)
        report["privileges"] = probe_privileges(cur)
        role = str(report["privileges"].get("current_user") or "ai_reader")
        report["suggested_grants"] = grant_instructions(role)
        report["sync_state"] = sync_freshness(cur, company_id)

        census = census_variants_stocks(cur, company_id)
        orphans = orphan_stock_variants(cur, company_id)
        report["census"] = census
        report["orphan_stock_variants"] = orphans

        # Identidad del censo: stock_pos = activas_stock_pos + inactivas + huérfanas
        a = int(census.get("activas_stock_pos") or 0)
        i = int(census.get("inactivas_stock_pos") or 0)
        o = int(census.get("stock_pos_sin_variante") or 0)
        sp = int(census.get("stock_pos") or 0)
        report["census_identity"] = {
            "formula": "stock_pos = activas_stock_pos + inactivas_stock_pos + stock_pos_sin_variante",
            "activas_stock_pos": a,
            "inactivas_stock_pos": i,
            "stock_pos_sin_variante": o,
            "stock_pos": sp,
            "closes": (a + i + o) == sp,
            "delta": sp - (a + i + o),
        }
        if report["census_identity"]["closes"]:
            report["notes"].append(
                f"Diferencia de {o} entre stock_pos y activas_stock_pos explicada: "
                f"variant_id con stock > 0 sin fila en bsale_raw.variants "
                f"(huérfanos de catálogo; {census.get('unidades_stock_pos_sin_variante')} uds)."
            )
        else:
            report["notes"].append(
                "Censo NO cierra: investigar joins company_id/variant_id antes de declarar cifras."
            )

        can_costs = bool(report["privileges"]["tables"].get(REQUIRED_COST_TABLE))
        if not can_costs:
            report["blocked_on_costs"] = True
            report["error_costs"] = (
                f"permission denied / sin SELECT en {REQUIRED_COST_TABLE}. "
                "Aplicar suggested_grants (manual) o reejecutar este script con el rol de la app."
            )
            report["ok"] = report["census_identity"]["closes"]
            return report

        report["blocked_on_costs"] = False
        report["cost_coverage"] = cost_coverage(cur, company_id)
        classes = cost_class_cross_stock(cur, company_id)
        report["cost_classes"] = classes

        # Totales y % sobre universo de variantes
        universo = int(census.get("variantes") or 0) or 1
        by_class = {r["clase_costo"]: r for r in classes}
        report["answers"] = {
            "A_costo_promedio_valido_positivo": int(
                (by_class.get("A_positivo") or {}).get("variantes") or 0
            ),
            "B1_sin_registro_costo": int(
                (by_class.get("B1_sin_registro") or {}).get("variantes") or 0
            ),
            "B2_costo_null": int((by_class.get("B2_null") or {}).get("variantes") or 0),
            "C_costo_cero_explicito": int(
                (by_class.get("C_cero") or {}).get("variantes") or 0
            ),
            "D_costo_negativo": int(
                (by_class.get("D_negativo") or {}).get("variantes") or 0
            ),
        }
        report["answers"]["sin_costo_valido_B1_B2_C_D"] = (
            report["answers"]["B1_sin_registro_costo"]
            + report["answers"]["B2_costo_null"]
            + report["answers"]["C_costo_cero_explicito"]
            + report["answers"]["D_costo_negativo"]
        )
        for key, val in list(report["answers"].items()):
            report["answers"][f"pct_{key}"] = round(100.0 * int(val) / universo, 2)

        critico = sum(
            int(r.get("critico_activo_stock_sin_costo_valido") or 0) for r in classes
        )

        crit_units_row = _fetch_one(
            cur,
            """
            WITH st AS (
              SELECT variant_id, sum(quantity) AS stock_total
              FROM bsale_raw.stocks WHERE company_id = %s GROUP BY variant_id
            )
            SELECT
              count(*)::bigint AS n,
              coalesce(sum(st.stock_total), 0) AS unidades
            FROM bsale_raw.variants v
            INNER JOIN st ON st.variant_id = v.bsale_id AND st.stock_total > 0
            LEFT JOIN bsale_raw.variant_costs vc
              ON vc.company_id = v.company_id AND vc.variant_id = v.bsale_id
            WHERE v.company_id = %s
              AND v.missing_since IS NULL
              AND COALESCE(v.state, 0) = 0
              AND (
                vc.variant_id IS NULL
                OR vc.average_cost IS NULL
                OR vc.average_cost <= 0
              )
            """,
            (company_id, company_id),
        )
        report["answers"]["activas_stock_pos_sin_costo_valido"] = int(
            crit_units_row.get("n") or 0
        )
        report["answers"]["unidades_activas_stock_pos_sin_costo_valido"] = crit_units_row.get(
            "unidades"
        )
        report["answers"]["critico_desde_clases_sum"] = critico

        # Cierre de clases = universo
        sum_classes = sum(int(r.get("variantes") or 0) for r in classes)
        report["class_identity"] = {
            "sum_clases": sum_classes,
            "universo": int(census.get("variantes") or 0),
            "closes": sum_classes == int(census.get("variantes") or 0),
        }

        cov = report["cost_coverage"]
        filas_c = int(cov.get("filas_variant_costs") or 0)
        vars_n = int(cov.get("variantes") or 0)
        report["sync_cost_interpretation"] = {
            "filas_variant_costs": filas_c,
            "variantes": vars_n,
            "sin_fila_en_pg": vars_n - filas_c,
            "nota": (
                "B1 (sin registro en PG) NO se declara automáticamente como "
                "'Bsale no tiene costo': puede ser cobertura incompleta del scanner. "
                "Contrastar con sync_state.variant_costs y muestreo en Bsale."
            ),
        }

        can_products = bool(report["privileges"]["tables"].get("bsale_raw.products"))
        can_pt = bool(report["privileges"]["tables"].get("bsale_raw.product_types"))
        can_prices = bool(report["privileges"]["tables"].get("bsale_raw.variant_prices"))
        can_rec = bool(
            report["privileges"]["tables"].get("bsale_raw.stock_receptions")
        ) and bool(
            report["privileges"]["tables"].get("bsale_raw.stock_reception_details")
        )
        report["top_critical"] = critical_active_stock_no_valid_cost(
            cur,
            company_id,
            limit=top_n,
            price_list_id=price_list_id,
            can_products=can_products,
            can_product_types=can_pt,
            can_prices=can_prices,
            can_receptions=can_rec,
        )
        report["top_critical_columns_available"] = {
            "products": can_products,
            "price_list": can_prices,
            "price_list_id": price_list_id,
            "receptions": can_rec,
        }

        if extended:
            report["zero_cost_actives"] = analyze_zero_cost_actives(
                cur,
                company_id,
                can_prices=can_prices,
                can_receptions=can_rec,
                sample_limit=min(30, top_n),
            )
            report["positive_cost_divergence"] = analyze_positive_cost_divergence(
                cur,
                company_id,
                can_receptions=can_rec,
                top_n=top_n,
            )
            div = report["positive_cost_divergence"]
            if div.get("buckets_identity") and not div["buckets_identity"].get("closes"):
                report["notes"].append(
                    "Divergencias: suma de buckets no cierra con universo; revisar SQL."
                )

        report["ok"] = bool(
            report["census_identity"]["closes"] and report["class_identity"]["closes"]
        )
        if extended and report.get("positive_cost_divergence", {}).get("buckets_identity"):
            report["ok"] = report["ok"] and bool(
                report["positive_cost_divergence"]["buckets_identity"].get("closes")
            )
        return report
    finally:
        try:
            if cur is not None:
                cur.close()
        except Exception:
            pass
        try:
            conn.rollback()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def _print_human(report: dict[str, Any]) -> None:
    print("=" * 72)
    print("Auditoría Comercial 8.0 — Fase 1.3 (solo lectura)")
    print(f"company_id={report.get('company_id')}  corte UTC={report.get('cut_at_utc')}")
    print("=" * 72)
    if report.get("error"):
        print("ERROR:", report["error"])
        if report.get("coolify_command"):
            print("Comando Coolify:", report["coolify_command"])
        return
    priv = report.get("privileges") or {}
    print(f"Rol DB: {priv.get('current_user')}")
    print("SELECT variant_costs:", (priv.get("tables") or {}).get(REQUIRED_COST_TABLE))
    cens = report.get("census") or {}
    print("\n--- Censo ---")
    for k in (
        "variantes",
        "activas",
        "inactivas",
        "stock_pos",
        "activas_stock_pos",
        "inactivas_stock_pos",
        "stock_pos_sin_variante",
        "activas_sin_fila_stock",
        "unidades_stock_pos",
        "unidades_stock_pos_sin_variante",
    ):
        if k in cens:
            print(f"  {k}: {cens[k]}")
    ident = report.get("census_identity") or {}
    print(f"  cierra: {ident.get('closes')}  delta={ident.get('delta')}")
    for n in report.get("notes") or []:
        print("  NOTE:", n)
    if report.get("blocked_on_costs"):
        print("\nBLOQUEADO en costos:", report.get("error_costs"))
        print("GRANT sugerido:")
        for line in report.get("suggested_grants") or []:
            print(" ", line)
        return
    print("\n--- Respuestas ---")
    for k, v in (report.get("answers") or {}).items():
        print(f"  {k}: {v}")
    print("\n--- Clases ---")
    for row in report.get("cost_classes") or []:
        print(" ", row)
    print(f"\n--- Top críticos ({len(report.get('top_critical') or [])}) ---")
    for row in report.get("top_critical") or []:
        print(
            f"  vid={row.get('variant_id')} stock={row.get('stock_total')} "
            f"clase={row.get('clase_costo')} sku={row.get('sku')}"
        )
    zc = report.get("zero_cost_actives") or {}
    if zc:
        print("\n--- Activos costo cero ---")
        print(" ", zc.get("summary"))
        print(" ", zc.get("by_price_stock"))
        print(" ", zc.get("by_reception"))
    div = report.get("positive_cost_divergence") or {}
    if div and not div.get("blocked"):
        print("\n--- Divergencias costo positivo ---")
        print(" ", div.get("buckets"))
        print(f"  top_anomalies={len(div.get('top_anomalies') or [])}")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Auditoría costos Comercial 8.0 (READ ONLY)")
    p.add_argument("--company-id", type=int, default=COMPANY_DEFAULT)
    p.add_argument("--timeout-sec", type=int, default=DEFAULT_TIMEOUT_SEC)
    p.add_argument("--top-critical", type=int, default=DEFAULT_TOP)
    p.add_argument("--price-list-id", type=int, default=DEFAULT_PRICE_LIST)
    p.add_argument("--extended", dest="extended", action="store_true", default=True)
    p.add_argument("--no-extended", dest="extended", action="store_false")
    p.add_argument("--json", action="store_true")
    args = p.parse_args(argv)

    report = build_report(
        company_id=int(args.company_id),
        timeout_sec=max(5, min(int(args.timeout_sec), 600)),
        top_n=max(1, min(int(args.top_critical), 200)),
        price_list_id=int(args.price_list_id),
        extended=bool(args.extended),
    )
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2, default=_json_default))
    else:
        _print_human(report)
        print("\n(JSON completo: añadir --json)")
    return 0 if report.get("ok") or report.get("census_identity", {}).get("closes") else 2


if __name__ == "__main__":
    sys.exit(main())
