"""Arranca Postgres temporal (pgserver) y ejecuta tests de organización de carga.

No usa analytics ni producción. Solo esquema mínimo sintético del propio test.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _assert_isolated_dsn(uri: str) -> None:
    low = uri.lower()
    banned = (
        "analytics",
        "quillotana",
        "prod",
        "production",
        "coolify",
        "neon.tech",
        "supabase",
        "amazonaws.com",
        "rds.amazonaws",
    )
    for b in banned:
        if b in low:
            raise SystemExit(f"DSN sospechoso (contiene {b!r}): abortando")
    if not any(
        x in low
        for x in ("localhost", "127.0.0.1", "host=/", "@/", "host=127.")
    ):
        if "postgresql://" not in low and "postgres://" not in low:
            raise SystemExit(f"DSN no parece local: {uri}")


def main() -> int:
    import pgserver
    import psycopg2

    base = Path(tempfile.mkdtemp(prefix="fase_f_load_org_"))
    pgdata = base / "pgdata"
    print(f"[isolated] pgdata={pgdata}", flush=True)
    srv = pgserver.get_server(pgdata, cleanup_mode="stop")
    uri = srv.get_uri() if hasattr(srv, "get_uri") else str(srv)
    if hasattr(srv, "get_uri") and callable(srv.get_uri):
        uri = srv.get_uri()
    elif hasattr(srv, "uri"):
        uri = srv.uri
    print(f"[isolated] uri={uri}", flush=True)
    _assert_isolated_dsn(uri)

    conn = psycopg2.connect(uri)
    cur = conn.cursor()
    cur.execute(
        "SELECT current_database(), inet_server_addr()::text, "
        "inet_server_port(), version()"
    )
    identity = cur.fetchone()
    print(f"[isolated] identity={identity}", flush=True)
    cur.execute("SHOW data_directory")
    datadir = cur.fetchone()[0]
    print(f"[isolated] data_directory={datadir}", flush=True)
    if "analytics" in str(datadir).lower() or "quillotana" in str(datadir).lower():
        raise SystemExit("data_directory apunta a entorno productivo — abort")
    conn.close()

    env = os.environ.copy()
    env["SIMPLE_PLANNING_TEST_DSN"] = uri
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")

    cmd = [
        sys.executable,
        "-m",
        "pytest",
        "backend/tests/test_dispatch_plan_load_organization.py",
        "-vv",
        "--tb=short",
    ]
    print(f"[isolated] running: {' '.join(cmd)}", flush=True)
    proc = subprocess.run(cmd, cwd=str(ROOT), env=env)
    _ = srv
    print(f"[isolated] pytest_exit={proc.returncode}", flush=True)
    return int(proc.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
