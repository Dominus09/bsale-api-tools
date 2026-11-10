"""Arranca Postgres temporal y ejecuta tests P4 de visitas aislados."""

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

    base = Path(tempfile.mkdtemp(prefix="p4_delivery_visit_"))
    pgdata = base / "pgdata"
    srv = pgserver.get_server(pgdata, cleanup_mode="stop")
    uri = srv.get_uri() if callable(getattr(srv, "get_uri", None)) else getattr(srv, "uri", str(srv))
    if callable(getattr(srv, "get_uri", None)):
        uri = srv.get_uri()
    print(f"[isolated] uri={uri}", flush=True)
    _assert_isolated_dsn(uri)

    conn = psycopg2.connect(uri)
    cur = conn.cursor()
    cur.execute("SHOW data_directory")
    datadir = cur.fetchone()[0]
    print(f"[isolated] data_directory={datadir}", flush=True)
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
        "backend/tests/test_delivery_visit_p4.py",
        "backend/tests/test_delivery_visit_p4_isolated_pg.py",
        "backend/tests/test_delivery_whatsapp_report.py",
        "backend/tests/test_pwa_driver_p2_navigation.py",
        "backend/tests/test_schema_bootstrap.py",
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
