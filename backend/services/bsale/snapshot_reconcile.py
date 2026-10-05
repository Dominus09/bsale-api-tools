"""
UPSERT + reconciliación de un snapshot COMPLETO de una empresa vía TEMP TABLE.

Sólo debe llamarse cuando la descarga HTTP terminó sin errores. No hace COMMIT:
el llamador controla la transacción (COMMIT/ROLLBACK).

Fusible de reconciliación masiva (antes de escribir nada):
- snapshot vacío con filas existentes → FAIL;
- stale_percentage > umbral → FAIL (el llamador hace ROLLBACK; no hay DELETE).
Umbral: ``BSALE_RECONCILE_MAX_STALE_PCT_<TABLA>`` o ``BSALE_RECONCILE_MAX_STALE_PCT`` (default 20).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Callable, Sequence

from psycopg2.extras import execute_values

from backend.services.bsale.sync_common import CompanySyncError, count_company_rows

logger = logging.getLogger(__name__)

DEFAULT_MAX_STALE_PCT = 20.0


@dataclass(frozen=True)
class SnapshotTableSpec:
    table: str
    temp_table: str
    columns: tuple[tuple[str, str], ...]
    key_columns: tuple[str, ...]
    threshold_env: str
    touch_updated_at: bool = False

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c for c, _ in self.columns)

    @property
    def value_columns(self) -> tuple[str, ...]:
        return tuple(c for c in self.column_names if c not in self.key_columns)


STOCKS_SPEC = SnapshotTableSpec(
    table="bsale.stocks",
    temp_table="_sync_stocks_snapshot",
    columns=(
        ("company_id", "bigint"),
        ("variant_id", "bigint"),
        ("office_id", "bigint"),
        ("quantity_available", "numeric"),
        ("quantity_reserved", "numeric"),
    ),
    key_columns=("company_id", "variant_id", "office_id"),
    threshold_env="BSALE_RECONCILE_MAX_STALE_PCT_STOCKS",
    touch_updated_at=True,
)

VARIANT_PRICES_SPEC = SnapshotTableSpec(
    table="bsale.variant_prices",
    temp_table="_sync_variant_prices_snapshot",
    columns=(
        ("company_id", "bigint"),
        ("variant_id", "bigint"),
        ("price_list_id", "bigint"),
        ("price_net", "numeric"),
        ("price_gross", "numeric"),
    ),
    key_columns=("company_id", "variant_id", "price_list_id"),
    threshold_env="BSALE_RECONCILE_MAX_STALE_PCT_VARIANT_PRICES",
)


def max_stale_pct(spec: SnapshotTableSpec, getenv: Callable[[str], str | None] = os.getenv) -> float:
    for name in (spec.threshold_env, "BSALE_RECONCILE_MAX_STALE_PCT"):
        raw = (getenv(name) or "").strip()
        if raw:
            value = float(raw)
            if not 0 <= value <= 100:
                raise ValueError(f"{name} debe estar entre 0 y 100")
            return value
    return DEFAULT_MAX_STALE_PCT


def dedupe_rows(spec: SnapshotTableSpec, rows: Sequence[tuple]) -> list[tuple]:
    """Una fila por clave (gana la última vista) para no golpear la misma fila dos veces en ON CONFLICT."""
    key_idx = [spec.column_names.index(k) for k in spec.key_columns]
    out: dict[tuple, tuple] = {}
    for row in rows:
        out[tuple(row[i] for i in key_idx)] = tuple(row)
    return list(out.values())


def _count_scoped_rows(
    conn: Any, spec: SnapshotTableSpec, company_id: int, scope_column: str, scope_values: list[int]
) -> int:
    cur = conn.cursor()
    cur.execute(
        f"SELECT COUNT(*) FROM {spec.table} WHERE company_id = %s AND {scope_column} = ANY(%s)",
        (company_id, scope_values),
    )
    row = cur.fetchone()
    cur.close()
    return int(row[0] or 0) if row else 0


def upsert_and_reconcile_snapshot(
    conn: Any,
    spec: SnapshotTableSpec,
    company_id: int,
    rows: Sequence[tuple],
    *,
    max_stale_percentage: float | None = None,
    scope_column: str | None = None,
    scope_values: Sequence[int] | None = None,
) -> dict[str, Any]:
    """
    Con ``scope_column``/``scope_values`` el conteo existente, el fusible y el DELETE de obsoletos
    se limitan a las filas de la empresa cuyo ``scope_column`` está en ``scope_values``; filas fuera
    del alcance no se cuentan ni se borran.
    """
    company_idx = spec.column_names.index("company_id")
    if any(int(r[company_idx]) != company_id for r in rows):
        raise CompanySyncError(f"{spec.table}: snapshot contiene filas de otra empresa")

    scope: list[int] | None = None
    if scope_column is not None:
        if scope_column not in spec.key_columns or scope_column == "company_id":
            raise ValueError(f"{spec.table}: scope_column inválida {scope_column!r}")
        if not scope_values:
            raise ValueError(f"{spec.table}: scope_values vacío")
        scope = sorted({int(v) for v in scope_values})
        scope_idx = spec.column_names.index(scope_column)
        if any(int(r[scope_idx]) not in scope for r in rows):
            raise CompanySyncError(f"{spec.table}: snapshot contiene filas fuera del alcance {scope}")

    threshold = max_stale_pct(spec) if max_stale_percentage is None else max_stale_percentage
    if scope is None:
        existing = count_company_rows(conn, spec.table, company_id)
    else:
        existing = _count_scoped_rows(conn, spec, company_id, scope_column, scope)
    if not rows and existing > 0:
        raise CompanySyncError(
            f"{spec.table}: snapshot vacío desde Bsale pero hay {existing} filas en BD "
            f"(company_id={company_id}); no se reconcilia"
        )

    unique_rows = dedupe_rows(spec, rows)
    cols = ", ".join(spec.column_names)
    col_defs = ", ".join(f"{c} {t}" for c, t in spec.columns)
    keys = ", ".join(spec.key_columns)
    set_parts = [f"{c} = EXCLUDED.{c}" for c in spec.value_columns]
    insert_cols = cols
    select_cols = cols
    if spec.touch_updated_at:
        insert_cols += ", updated_at"
        select_cols += ", NOW()"
        set_parts.append("updated_at = NOW()")
    key_match = " AND ".join(f"s.{k} = t.{k}" for k in spec.key_columns)
    scope_sql = f"AND t.{scope_column} = ANY(%s)" if scope is not None else ""
    stale_params: tuple = (company_id,) if scope is None else (company_id, scope)
    stale_where = f"""
        FROM {spec.table} t
        WHERE t.company_id = %s
          {scope_sql}
          AND NOT EXISTS (SELECT 1 FROM {spec.temp_table} s WHERE {key_match})
    """

    cur = conn.cursor()
    cur.execute(f"CREATE TEMP TABLE {spec.temp_table} ({col_defs}) ON COMMIT DROP")
    if unique_rows:
        execute_values(
            cur,
            f"INSERT INTO {spec.temp_table} ({cols}) VALUES %s",
            unique_rows,
            page_size=1000,
        )

    cur.execute(f"SELECT COUNT(*) {stale_where}", stale_params)
    row = cur.fetchone()
    stale = int(row[0] or 0) if row else 0
    stale_pct = round(stale * 100.0 / existing, 2) if existing else 0.0
    metrics: dict[str, Any] = {
        "existing_count": existing,
        "snapshot_count": len(unique_rows),
        "stale_count": stale,
        "stale_percentage": stale_pct,
        "max_stale_percentage": threshold,
    }
    if scope is not None:
        metrics["scope"] = {scope_column: scope}
    logger.info("[BSALE_SYNC] reconcile table=%s company_id=%s %s", spec.table, company_id, metrics)
    if stale_pct > threshold:
        cur.close()
        raise CompanySyncError(
            f"{spec.table}: reconciliación masiva bloqueada company_id={company_id} "
            f"stale={stale}/{existing} ({stale_pct}%) > umbral {threshold}% "
            f"(ajustable con {spec.threshold_env})"
        )

    cur.execute(
        f"""
        INSERT INTO {spec.table} ({insert_cols})
        SELECT {select_cols} FROM {spec.temp_table}
        ON CONFLICT ({keys}) DO UPDATE SET {", ".join(set_parts)}
        """
    )
    upserted = int(cur.rowcount or 0)
    cur.execute(f"DELETE {stale_where}", stale_params)
    deleted = int(cur.rowcount or 0)
    cur.close()
    return {"upserted": upserted, "deleted": deleted, **metrics}
