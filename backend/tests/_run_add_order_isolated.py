"""Arranca Postgres temporal y ejecuta test_add_order_isolated_pg."""

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


def main() -> int:
    import pgserver
    import psycopg2

    base = Path(tempfile.mkdtemp(prefix="fase_f_add_order_"))
    pgdata = base / "pgdata"
    srv = pgserver.get_server(pgdata, cleanup_mode="stop")
    uri = srv.get_uri() if callable(getattr(srv, "get_uri", None)) else getattr(srv, "uri", str(srv))
    if callable(getattr(srv, "get_uri", None)):
        uri = srv.get_uri()
    _assert_isolated_dsn(uri)
    conn = psycopg2.connect(uri)
    cur = conn.cursor()
    cur.execute("SHOW data_directory")
    datadir = cur.fetchone()[0]
    if "analytics" in str(datadir).lower() or "quillotana" in str(datadir).lower():
        raise SystemExit("data_directory productivo — abort")
    conn.close()

    env = os.environ.copy()
    env["SIMPLE_PLANNING_TEST_DSN"] = uri
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    cmd = [
        sys.executable,
        "-m",
        "pytest",
        "backend/tests/test_add_order_isolated_pg.py",
        "backend/tests/test_add_order_to_plan.py",
        "-vv",
        "--tb=short",
    ]
    proc = subprocess.run(cmd, cwd=str(ROOT), env=env)
    _ = srv
    return int(proc.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
