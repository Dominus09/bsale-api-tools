#!/usr/bin/env python3
"""
Auditoría Comercial 8.0 — censo de costos (solo lectura).

Usa la misma conexión PostgreSQL que FastAPI (``backend.db.get_connection``).
No escribe datos, no abre endpoints, no imprime credenciales.

Uso (desde la raíz del repo, con env PG_* del backend):

  python -m backend.scripts.audit_commercial_cost_census --company-id 3

Coolify (terminal del contenedor backend; Dockerfile WORKDIR=/app, COPY . .):

  cd /app
  python -m backend.scripts.audit_commercial_cost_census \\
    --company-id 3 --timeout-sec 120 --top-critical 30 \\
    --price-list-id 12 --json

Opciones:
  --timeout-sec 120   statement_timeout por consulta (default 120)
  --top-critical 30   listado administrativo (default 30)
  --price-list-id 12  lista comercial para precio bruto (default 12)
  --json              imprime el informe completo en JSON
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from psycopg2.extensions import ISOLATION_LEVEL_REPEATABLE_READ

from backend.db import get_connection
from backend.services.analytics.validate_distribuidora_source import assert_sql_is_read_only

DEFAULT_TIMEOUT_SEC = 120
DEFAULT_TOP = 30
DEFAULT_PRICE_LIST = 12
COMPANY_DEFAULT = 3

# Tablas mínimas para Fase 1.1 (SELECT). GRANT sugerido al final del informe.
REQUIRED_COST_TABLE = "bsale_raw.variant_costs"
OPTIONAL_TABLES = (
    "bsale_raw.products",
    "bsale_raw.product_types",
    "bsale_raw.variant_prices",
    "bsale_raw.stock_receptions",
    "bsale_raw.stock_reception_details",
)


def _json_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, (bytes, bytearray)):
        return None
    raise TypeError(type(obj).__name__)


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
    return [
        f"-- Ejecutar como propietario del esquema / DBA (NO como superusuario de auditoría).",
        f"-- Rol de auditoría MCP actual: {role}",
        f"-- Solo SELECT; sin GRANT OPTION; sin escritura.",
        f"GRANT SELECT ON TABLE bsale_raw.variant_costs TO {role};",
        f"GRANT SELECT ON TABLE bsale_raw.products TO {role};",
        f"GRANT SELECT ON TABLE bsale_raw.product_types TO {role};",
        f"GRANT SELECT ON TABLE bsale_raw.variant_prices TO {role};",
        f"GRANT SELECT ON TABLE bsale_raw.stock_receptions TO {role};",
        f"GRANT SELECT ON TABLE bsale_raw.stock_reception_details TO {role};",
        f"-- Verificar:",
        f"SELECT table_name, has_table_privilege('{role}', format('bsale_raw.%s', table_name), 'SELECT')",
        f"FROM (VALUES ('variant_costs'),('products'),('variant_prices'),",
        f"             ('stock_receptions'),('stock_reception_details')) t(table_name);",
    ]


def build_report(
    *,
    company_id: int,
    timeout_sec: int,
    top_n: int,
    price_list_id: int,
) -> dict[str, Any]:
    cut = datetime.now(timezone.utc)
    report: dict[str, Any] = {
        "audit": "comercial_8_0_fase_1_1_cost_census",
        "company_id": company_id,
        "cut_at_utc": cut.isoformat(),
        "readonly": True,
        "isolation": "REPEATABLE READ",
        "statement_timeout_sec": timeout_sec,
        "ok": False,
        "blocked_on_costs": True,
        "notes": [],
        "excel_implications": [
            "Separar en resumen A / B1 / B2 / C / D (nunca mezclar NULL con cero).",
            "Marcar explícitamente 'stock>0 sin costo válido' como prioritario.",
            "Documentar hora de corte y estado del scanner de variant_costs.",
            "No valorizar inventario cuando el costo es B1/B2/C/D.",
            "Incluir advertencia de variant_id con stock sin fila en variants (huérfanos).",
            "No sustituir average_cost por última recepción en columnas de margen.",
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
            f"--top-critical {top_n} --price-list-id {price_list_id} --json"
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

        report["ok"] = bool(
            report["census_identity"]["closes"] and report["class_identity"]["closes"]
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
    print("Auditoría Comercial 8.0 — Fase 1.1 (solo lectura)")
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


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Censo costos Comercial 8.0 (READ ONLY)")
    p.add_argument("--company-id", type=int, default=COMPANY_DEFAULT)
    p.add_argument("--timeout-sec", type=int, default=DEFAULT_TIMEOUT_SEC)
    p.add_argument("--top-critical", type=int, default=DEFAULT_TOP)
    p.add_argument("--price-list-id", type=int, default=DEFAULT_PRICE_LIST)
    p.add_argument("--json", action="store_true")
    args = p.parse_args(argv)

    report = build_report(
        company_id=int(args.company_id),
        timeout_sec=max(5, min(int(args.timeout_sec), 300)),
        top_n=max(1, min(int(args.top_critical), 200)),
        price_list_id=int(args.price_list_id),
    )
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2, default=_json_default))
    else:
        _print_human(report)
        print("\n(JSON completo: añadir --json)")
    return 0 if report.get("ok") or report.get("census_identity", {}).get("closes") else 2


if __name__ == "__main__":
    sys.exit(main())
