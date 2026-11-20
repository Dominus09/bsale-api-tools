"""Aplica 061 SOLO a PostgreSQL de staging/lab tras asserts de aislamiento.

NO producción. No imprime passwords. Requiere --i-confirm-staging.
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[2]
SQL_061 = ROOT / "backend" / "sql" / "distribuidora" / "061_delivery_offline_ops_AWAITING_APPROVAL.sql"
SQL_BASE = [
    ROOT / "backend" / "sql" / "distribuidora" / "056_delivery_visits_evidence.sql",
    ROOT / "backend" / "sql" / "distribuidora" / "057_delivery_visit_closure_guards.sql",
    ROOT / "backend" / "sql" / "distribuidora" / "058_delivery_visit_admin_void.sql",
]


def _strip_tx(sql: str) -> str:
    return "\n".join(
        ln
        for ln in sql.splitlines()
        if ln.strip().upper() not in ("BEGIN;", "COMMIT;", "BEGIN", "COMMIT")
    )


def _split_sql(raw: str) -> list[str]:
    parts: list[str] = []
    buf: list[str] = []
    for line in raw.splitlines():
        if line.strip() == "-- +go":
            chunk = "\n".join(buf).strip()
            if chunk:
                parts.append(chunk)
            buf = []
        else:
            buf.append(line)
    chunk = "\n".join(buf).strip()
    if chunk:
        parts.append(chunk)
    return parts


def _load_isolation():
    path = ROOT / "deploy" / "staging-offline-v2" / "isolation_assert.py"
    spec = importlib.util.spec_from_file_location("isolation_assert", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main() -> int:
    if "--i-confirm-staging" not in sys.argv:
        print(
            "Uso: python deploy/staging-offline-v2/apply_061_staging_only.py "
            "--i-confirm-staging\n"
            "Requiere STAGING_DATABASE_URL apuntando a PG de lab.",
            flush=True,
        )
        return 2

    iso = _load_isolation()
    os.environ.setdefault("OFFLINE_SYNC_WRITES_ENABLED", "false")
    iso.assert_staging_isolation(require_writes_off=True)

    dsn = (
        os.getenv("STAGING_DATABASE_URL")
        or os.getenv("DATABASE_URL")
        or os.getenv("SIMPLE_PLANNING_TEST_DSN")
        or ""
    ).strip()

    u = urlparse(dsn)
    print(
        f"[061] host={u.hostname!r} db={(u.path or '').lstrip('/')!r} user={u.username!r}",
        flush=True,
    )

    import psycopg2

    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    cur.execute("SHOW data_directory")
    datadir = str(cur.fetchone()[0])
    print(f"[061] data_directory={datadir}", flush=True)
    low = datadir.lower()
    if "analytics" in low or "quillotana" in low:
        raise SystemExit("data_directory parece productivo — abort")

    cur.execute("SELECT current_database(), current_user, inet_server_addr()")
    db, user, addr = cur.fetchone()
    print(f"[061] database={db!r} user={user!r} server_addr={addr!r}", flush=True)

    cur.execute("SELECT to_regclass('distribuidora.delivery_visits')")
    if cur.fetchone()[0] is None:
        print("[061] delivery_visits ausente — aplicando 056→058 en este PG lab", flush=True)
        cur.execute("CREATE SCHEMA IF NOT EXISTS distribuidora")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS distribuidora.dispatch_plan (
                id BIGSERIAL PRIMARY KEY,
                status TEXT NOT NULL DEFAULT 'confirmed',
                planning_code TEXT,
                driver_user_id INTEGER
            )
            """
        )
        conn.commit()
        for path in SQL_BASE:
            raw = _strip_tx(path.read_text(encoding="utf-8"))
            for stmt in _split_sql(raw):
                if stmt.strip():
                    cur.execute(stmt)
            conn.commit()
            print(f"[061] applied base {path.name}", flush=True)

    cur.execute("SELECT to_regclass('distribuidora.delivery_offline_ops')")
    if cur.fetchone()[0] is not None:
        print("[061] delivery_offline_ops ya existe — skip DDL", flush=True)
    else:
        raw = _strip_tx(SQL_061.read_text(encoding="utf-8"))
        for stmt in _split_sql(raw):
            if stmt.strip():
                cur.execute(stmt)
        conn.commit()
        print("[061] delivery_offline_ops CREADA en staging/lab", flush=True)

    cur.execute(
        """
        SELECT indexname FROM pg_indexes
        WHERE schemaname='distribuidora' AND tablename='delivery_offline_ops'
        ORDER BY 1
        """
    )
    indexes = [r[0] for r in cur.fetchall()]
    cur.execute(
        """
        SELECT conname FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname='distribuidora' AND t.relname='delivery_offline_ops'
        ORDER BY 1
        """
    )
    cons = [r[0] for r in cur.fetchall()]
    print(f"[061] indexes={indexes}", flush=True)
    print(f"[061] constraints={cons}", flush=True)
    if "uq_delivery_offline_ops_driver_idem" not in cons:
        raise SystemExit("falta UNIQUE driver+idempotency_key")
    cur.close()
    conn.close()
    print("[061] OK — solo staging/lab", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
