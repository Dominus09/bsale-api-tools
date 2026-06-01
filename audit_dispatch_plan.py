#!/usr/bin/env python3
"""
Auditoría PostgreSQL — rendimiento de distribuidora.v_dispatch_plan_invoiced_documents.

Uso en contenedor Coolify (WORKDIR /app):

    python audit_dispatch_plan.py
    python audit_dispatch_plan.py --plan-id 3 --output /tmp/dispatch_plan_audit.txt

Conexión (en orden):
    1. DATABASE_URL / POSTGRES_URL
    2. PG_HOST, PG_DB, PG_USER, PG_PASSWORD, PG_PORT
    3. POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_PORT

Solo lectura (EXPLAIN ANALYZE ejecuta la consulta; puede tardar minutos).
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any, TextIO
from urllib.parse import urlparse

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection as PgConnection

DEFAULT_OUTPUT = "/tmp/dispatch_plan_audit.txt"
DEFAULT_PLAN_ID = 3

# Objetos pedidos (distribuidora + bsale por si el deploy usa otro esquema).
OBJECTS_TO_CHECK: list[tuple[str, str, str]] = [
    ("distribuidora", "v_dispatch_plan_invoiced_documents", "view"),
    ("distribuidora", "v_orders_purchase_status", "view"),
    ("distribuidora", "dispatch_plan_orders", "table"),
    ("distribuidora", "documents", "table"),
    ("distribuidora", "document_details", "table"),
    ("distribuidora", "document_related", "table"),
    ("bsale", "documents", "table"),
    ("bsale", "document_details", "table"),
    ("bsale", "document_related", "table"),
]

INDEX_TABLES = ("dispatch_plan_orders", "documents", "document_details", "document_related")
COUNT_TABLES = ("documents", "document_details", "document_related")

MIGRATION_TABLE_CANDIDATES = (
    ("distribuidora", "schema_migrations"),
    ("distribuidora", "applied_migrations"),
    ("distribuidora", "sql_migrations"),
    ("public", "schema_migrations"),
    ("public", "alembic_version"),
)


def _env(*keys: str, default: str | None = None) -> str | None:
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default


def resolve_connection_params() -> dict[str, Any]:
    """Parámetros para psycopg2.connect."""
    url = _env("DATABASE_URL", "POSTGRES_URL", "POSTGRESQL_URL")
    if url:
        parsed = urlparse(url)
        if parsed.scheme and parsed.scheme.startswith("postgres"):
            return {
                "host": parsed.hostname or "localhost",
                "port": parsed.port or 5432,
                "database": (parsed.path or "/").lstrip("/") or "postgres",
                "user": parsed.username,
                "password": parsed.password,
            }
        raise ValueError(f"DATABASE_URL con esquema no soportado: {parsed.scheme!r}")

    host = _env("PG_HOST", "POSTGRES_HOST", default="localhost")
    database = _env("PG_DB", "POSTGRES_DB", "PGDATABASE", default="postgres")
    user = _env("PG_USER", "POSTGRES_USER", "PGUSER")
    password = _env("PG_PASSWORD", "POSTGRES_PASSWORD", "PGPASSWORD")
    port = int(_env("PG_PORT", "POSTGRES_PORT", default="5432") or "5432")

    if not user:
        raise ValueError(
            "Falta usuario: defina DATABASE_URL o PG_USER / POSTGRES_USER."
        )

    return {
        "host": host,
        "database": database,
        "user": user,
        "password": password,
        "port": port,
    }


class AuditLog:
    def __init__(self, path: str) -> None:
        self.path = path
        self._file: TextIO | None = None
        self.lines: list[str] = []

    def __enter__(self) -> AuditLog:
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        self._file = open(self.path, "w", encoding="utf-8")
        return self

    def __exit__(self, *args: object) -> None:
        if self._file:
            self._file.close()

    def write(self, text: str = "") -> None:
        self.lines.append(text)
        print(text, flush=True)
        if self._file:
            self._file.write(text + "\n")
            self._file.flush()

    def section(self, title: str) -> None:
        bar = "=" * 72
        self.write("")
        self.write(bar)
        self.write(title)
        self.write(bar)


def fetch_one(cur, query: str, params: tuple[Any, ...] | None = None) -> Any:
    cur.execute(query, params)
    row = cur.fetchone()
    return row[0] if row else None


def object_exists(cur, schema: str, name: str, kind: str) -> bool:
    if kind == "view":
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.views
                WHERE table_schema = %s AND table_name = %s
            )
            """,
            (schema, name),
        )
    else:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
                  AND table_type = 'BASE TABLE'
            )
            """,
            (schema, name),
        )
    return bool(cur.fetchone()[0])


def resolve_documents_schema(cur, log: AuditLog) -> str | None:
    """Esquema donde viven documents / details / related (distribuidora o bsale)."""
    for schema in ("distribuidora", "bsale"):
        if object_exists(cur, schema, "documents", "table"):
            log.write(f"  → documents encontrado en: {schema}.documents")
            return schema
    return None


def find_migration_table(cur) -> tuple[str, str] | None:
    for schema, table in MIGRATION_TABLE_CANDIDATES:
        if object_exists(cur, schema, table, "table"):
            return schema, table
    # Cualquier tabla con 'migration' en el nombre
    cur.execute(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_name ILIKE '%migration%'
        ORDER BY table_schema, table_name
        LIMIT 1
        """
    )
    row = cur.fetchone()
    return (row[0], row[1]) if row else None


def list_recent_migrations(cur, schema: str, table: str, log: AuditLog) -> list[str]:
    """Últimas 20 filas; adapta columnas comunes."""
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table),
    )
    cols = [r[0] for r in cur.fetchall()]
    if not cols:
        log.write("  (tabla sin columnas legibles)")
        return []

    order_col = None
    for candidate in ("applied_at", "created_at", "executed_at", "installed_on", "version"):
        if candidate in cols:
            order_col = candidate
            break
    name_col = "name" if "name" in cols else ("version" if "version" in cols else cols[0])

    fq = sql.Identifier(schema, table)
    if order_col:
        q = sql.SQL("SELECT * FROM {} ORDER BY {} DESC NULLS LAST LIMIT 20").format(
            fq, sql.Identifier(order_col)
        )
    else:
        q = sql.SQL("SELECT * FROM {} LIMIT 20").format(fq)

    cur.execute(q)
    rows = cur.fetchall()
    headers = [d[0] for d in cur.description]
    out: list[str] = []
    name_idx = headers.index(name_col) if name_col in headers else None
    for row in rows:
        parts = [f"{h}={v!r}" for h, v in zip(headers, row)]
        line = " | ".join(parts)
        log.write(f"  {line}")
        if name_idx is not None and row[name_idx] is not None:
            out.append(str(row[name_idx]))
    return out


def detect_migration_026(viewdef: str | None) -> bool:
    if not viewdef:
        return False
    low = viewdef.lower()
    # Vista optimizada (026): LATERAL por OC, sin join a vista global de status.
    has_lateral = "join lateral" in low or "lateral (" in low
    no_old_join = "v_orders_purchase_status" not in low
    has_plan = "dispatch_plan_orders" in low
    return has_plan and has_lateral and no_old_join


def parse_explain_execution_ms(explain_text: str) -> float | None:
    m = re.search(r"Execution Time:\s*([\d.]+)\s*ms", explain_text)
    if m:
        return float(m.group(1))
    m = re.search(r"Tiempo de ejecución:\s*([\d.]+)\s*ms", explain_text, re.I)
    if m:
        return float(m.group(1))
    return None


def run_explain_analyze(cur, plan_id: int, log: AuditLog) -> tuple[str, float | None]:
    log.write(f"Ejecutando EXPLAIN ANALYZE (plan_id={plan_id}) — puede tardar varios minutos…")
    t0 = time.perf_counter()
    cur.execute(
        """
        EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
        SELECT *
        FROM distribuidora.v_dispatch_plan_invoiced_documents
        WHERE dispatch_plan_id = %s
        """,
        (plan_id,),
    )
    rows = cur.fetchall()
    wall_s = time.perf_counter() - t0
    text = "\n".join(r[0] for r in rows)
    exec_ms = parse_explain_execution_ms(text)
    log.write(f"Wall clock: {wall_s:.2f} s")
    if exec_ms is not None:
        log.write(f"Execution Time (plan): {exec_ms:.2f} ms")
    log.write("")
    log.write(text)
    return text, exec_ms if exec_ms is not None else wall_s * 1000.0


def list_indexes(cur, log: AuditLog) -> None:
    cur.execute(
        """
        SELECT schemaname, tablename, indexname, indexdef
        FROM pg_indexes
        WHERE schemaname IN ('distribuidora', 'bsale')
          AND tablename = ANY(%s)
        ORDER BY schemaname, tablename, indexname
        """,
        (list(INDEX_TABLES),),
    )
    rows = cur.fetchall()
    if not rows:
        log.write("  (ningún índice en tablas objetivo)")
        return
    for schema, table, iname, idef in rows:
        log.write(f"  [{schema}.{table}] {iname}")
        log.write(f"    {idef}")


def table_row_count(cur, schema: str, table: str) -> int | None:
    try:
        q = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
            sql.Identifier(schema), sql.Identifier(table)
        )
        cur.execute(q)
        return int(cur.fetchone()[0])
    except Exception:
        return None


def run_audit(plan_id: int, output_path: str, skip_explain: bool) -> int:
    params = resolve_connection_params()
    summary: dict[str, Any] = {
        "view_found": False,
        "migration_026_detected": False,
        "explain_ms": None,
        "table_counts": {},
        "documents_schema": None,
    }

    with AuditLog(output_path) as log:
        log.section(f"AUDITORÍA dispatch_plan — {datetime.now(timezone.utc).isoformat()}")
        log.write(f"Salida: {output_path}")
        log.write(f"plan_id: {plan_id}")
        safe_params = {**params, "password": "***" if params.get("password") else None}
        log.write(f"Conexión: {safe_params}")

        conn: PgConnection | None = None
        try:
            conn = psycopg2.connect(**params)
            conn.autocommit = True
            cur = conn.cursor()

            log.section("1. Servidor PostgreSQL")
            log.write(f"version: {fetch_one(cur, 'SELECT version()')}")
            log.write(f"database: {fetch_one(cur, 'SELECT current_database()')}")
            log.write(f"user: {fetch_one(cur, 'SELECT current_user')}")
            log.write(f"search_path: {fetch_one(cur, 'SHOW search_path')}")

            log.section("2. Existencia de objetos")
            existence: dict[str, bool] = {}
            for schema, name, kind in OBJECTS_TO_CHECK:
                key = f"{schema}.{name}"
                ok = object_exists(cur, schema, name, kind)
                existence[key] = ok
                log.write(f"  {'OK' if ok else 'NO'}  {key} ({kind})")
            summary["view_found"] = existence.get(
                "distribuidora.v_dispatch_plan_invoiced_documents", False
            )

            log.section("3. Definición vista (pg_get_viewdef)")
            viewdef: str | None = None
            if summary["view_found"]:
                viewdef = fetch_one(
                    cur,
                    "SELECT pg_get_viewdef('distribuidora.v_dispatch_plan_invoiced_documents'::regclass, true)",
                )
                log.write(viewdef or "(vacío)")
                summary["migration_026_detected"] = detect_migration_026(viewdef)
                log.write("")
                log.write(
                    f"Firma migración 026 (LATERAL plan-first, sin v_orders_purchase_status): "
                    f"{'SÍ' if summary['migration_026_detected'] else 'NO'}"
                )
            else:
                log.write("Vista no existe — omitiendo pg_get_viewdef.")

            log.section("4. Migraciones aplicadas")
            mig = find_migration_table(cur)
            migration_rows: list[str] = []
            if mig:
                schema, table = mig
                log.write(f"Tabla detectada: {schema}.{table}")
                migration_rows = list_recent_migrations(cur, schema, table, log)
                joined = " ".join(migration_rows).lower()
                if "026" in joined or "dispatch_plan_invoiced_view_perf" in joined:
                    summary["migration_026_detected"] = True
            else:
                log.write(
                    "No hay tabla de migraciones conocida. "
                    "Distribuidora aplica SQL vía ensure_distribuidora_schema (archivos 001…025 en código)."
                )
                log.write(
                    "Compruebe si existe backend/sql/distribuidora/026_dispatch_plan_invoiced_view_perf.sql "
                    "y ejecútelo manualmente si la firma de la vista es antigua."
                )

            log.section("5. EXPLAIN ANALYZE")
            if skip_explain:
                log.write("Omitido (--skip-explain).")
            elif not summary["view_found"]:
                log.write("Omitido: vista no encontrada.")
            else:
                _, summary["explain_ms"] = run_explain_analyze(cur, plan_id, log)

            log.section("6. Índices")
            list_indexes(cur, log)

            log.section("7. Conteo de registros (documents / details / related)")
            doc_schema = resolve_documents_schema(cur, log)
            summary["documents_schema"] = doc_schema
            for table in COUNT_TABLES:
                counted = False
                for schema in (doc_schema, "distribuidora", "bsale"):
                    if not schema:
                        continue
                    if not object_exists(cur, schema, table, "table"):
                        continue
                    n = table_row_count(cur, schema, table)
                    if n is not None:
                        key = f"{schema}.{table}"
                        summary["table_counts"][key] = n
                        log.write(f"  COUNT(*) {key} = {n:,}")
                        counted = True
                        break
                if not counted:
                    log.write(f"  {table}: no encontrada en distribuidora ni bsale")

            log.section("8. Órdenes del plan")
            if object_exists(cur, "distribuidora", "dispatch_plan_orders", "table"):
                n_plan = fetch_one(
                    cur,
                    "SELECT COUNT(*) FROM distribuidora.dispatch_plan_orders WHERE dispatch_plan_id = %s",
                    (plan_id,),
                )
                log.write(f"  dispatch_plan_orders para plan_id={plan_id}: {n_plan}")
            else:
                log.write("  dispatch_plan_orders: no existe")

            log.section("RESUMEN FINAL")
            log.write(
                f"  Vista v_dispatch_plan_invoiced_documents: "
                f"{'ENCONTRADA' if summary['view_found'] else 'NO ENCONTRADA'}"
            )
            log.write(
                f"  Migración 026 / vista optimizada: "
                f"{'DETECTADA' if summary['migration_026_detected'] else 'NO DETECTADA (sigue vista lenta probable)'}"
            )
            if summary["explain_ms"] is not None:
                log.write(f"  Tiempo EXPLAIN ANALYZE: {summary['explain_ms']:.2f} ms")
            else:
                log.write("  Tiempo EXPLAIN ANALYZE: (no ejecutado o no parseado)")
            if summary["table_counts"]:
                largest = max(summary["table_counts"].items(), key=lambda x: x[1])
                log.write(f"  Tabla más grande: {largest[0]} = {largest[1]:,} filas")
                log.write("  Top tablas:")
                for k, v in sorted(
                    summary["table_counts"].items(), key=lambda x: -x[1]
                )[:5]:
                    log.write(f"    - {k}: {v:,}")
            if summary["view_found"] and not summary["migration_026_detected"]:
                log.write("")
                log.write(
                    "  ACCIÓN: aplicar backend/sql/distribuidora/026_dispatch_plan_invoiced_view_perf.sql "
                    "y volver a ejecutar este script."
                )

            cur.close()
        except Exception as e:
            log.write(f"ERROR: {type(e).__name__}: {e}")
            import traceback

            log.write(traceback.format_exc())
            return 1
        finally:
            if conn is not None:
                conn.close()

    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Auditoría PostgreSQL — v_dispatch_plan_invoiced_documents"
    )
    parser.add_argument(
        "--plan-id",
        type=int,
        default=int(_env("DISPATCH_PLAN_AUDIT_ID", default=str(DEFAULT_PLAN_ID)) or DEFAULT_PLAN_ID),
        help=f"dispatch_plan_id para EXPLAIN (default {DEFAULT_PLAN_ID})",
    )
    parser.add_argument(
        "--output",
        default=_env("DISPATCH_PLAN_AUDIT_OUTPUT", default=DEFAULT_OUTPUT) or DEFAULT_OUTPUT,
        help=f"Archivo de salida (default {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--skip-explain",
        action="store_true",
        help="No ejecutar EXPLAIN ANALYZE (solo metadatos)",
    )
    args = parser.parse_args()
    try:
        code = run_audit(args.plan_id, args.output, args.skip_explain)
    except ValueError as e:
        print(f"Configuración: {e}", file=sys.stderr)
        sys.exit(2)
    sys.exit(code)


if __name__ == "__main__":
    main()
