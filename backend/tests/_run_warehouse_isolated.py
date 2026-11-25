"""Arranca Postgres temporal (pgserver) y ejecuta tests warehouse slices 1.1–1.2.

Nunca analytics/prod.
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
    for b in (
        "analytics",
        "quillotana",
        "prod",
        "production",
        "coolify",
        "neon.tech",
        "supabase",
        "amazonaws.com",
    ):
        if b in low:
            raise SystemExit(f"DSN sospechoso (contiene {b!r}): abortando")


def main() -> int:
    import pgserver
    import psycopg2

    base = Path(tempfile.mkdtemp(prefix="warehouse_1_1_"))
    pgdata = base / "pgdata"
    print(f"[isolated-warehouse] pgdata={pgdata}", flush=True)
    srv = pgserver.get_server(pgdata, cleanup_mode="stop")
    uri = srv.get_uri() if callable(getattr(srv, "get_uri", None)) else getattr(srv, "uri", str(srv))
    if callable(getattr(srv, "get_uri", None)):
        uri = srv.get_uri()
    print(f"[isolated-warehouse] uri={uri}", flush=True)
    _assert_isolated_dsn(uri)

    conn = psycopg2.connect(uri)
    cur = conn.cursor()
    cur.execute("SHOW data_directory")
    datadir = cur.fetchone()[0]
    print(f"[isolated-warehouse] data_directory={datadir}", flush=True)
    if "analytics" in str(datadir).lower() or "quillotana" in str(datadir).lower():
        raise SystemExit("data_directory productivo — abort")
    conn.close()

    env = os.environ.copy()
    env["WAREHOUSE_TEST_DSN"] = uri
    env["SIMPLE_PLANNING_TEST_DSN"] = uri
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    (ROOT / "_tmp_isolated_pg_dsn.txt").write_text(uri + "\n", encoding="utf-8")

    cmd = [
        sys.executable,
        "-m",
        "pytest",
        "backend/tests/test_warehouse_layout_isolated_pg.py",
        "backend/tests/test_warehouse_layout_unit.py",
        "backend/tests/test_warehouse_generator_unit.py",
        "backend/tests/test_warehouse_publish_isolated_pg.py",
        "-vv",
        "--tb=short",
    ]
    print(f"[isolated-warehouse] running: {' '.join(cmd)}", flush=True)
    code = 1
    try:
        proc = subprocess.run(cmd, cwd=str(ROOT), env=env)
        code = int(proc.returncode)
        print(f"[isolated-warehouse] pytest_exit={code}", flush=True)
    finally:
        # Evitar exit code ambiguo por fallos de cleanup de pgserver.
        try:
            closer = getattr(srv, "cleanup", None) or getattr(srv, "close", None)
            if callable(closer):
                closer()
        except Exception as exc:  # noqa: BLE001
            print(f"[isolated-warehouse] pgserver_cleanup_warn={exc!r}", flush=True)
        print(f"[isolated-warehouse] returning_pytest_exit={code}", flush=True)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
