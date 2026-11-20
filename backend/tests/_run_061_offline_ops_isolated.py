"""Arranca Postgres temporal (pgserver) y valida Offline V2 2A.1.

Nunca analytics/prod. Esquema sintético 056→061.
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


def main() -> int:
    import pgserver
    import psycopg2

    base = Path(tempfile.mkdtemp(prefix="p4_061_offline_ops_"))
    pgdata = base / "pgdata"
    print(f"[isolated-061] pgdata={pgdata}", flush=True)
    srv = pgserver.get_server(pgdata, cleanup_mode="stop")
    uri = srv.get_uri() if callable(getattr(srv, "get_uri", None)) else getattr(srv, "uri", str(srv))
    if callable(getattr(srv, "get_uri", None)):
        uri = srv.get_uri()
    print(f"[isolated-061] uri={uri}", flush=True)
    _assert_isolated_dsn(uri)

    conn = psycopg2.connect(uri)
    cur = conn.cursor()
    cur.execute("SHOW data_directory")
    datadir = cur.fetchone()[0]
    print(f"[isolated-061] data_directory={datadir}", flush=True)
    if "analytics" in str(datadir).lower() or "quillotana" in str(datadir).lower():
        raise SystemExit("data_directory productivo — abort")
    conn.close()

    env = os.environ.copy()
    env["SIMPLE_PLANNING_TEST_DSN"] = uri
    # Laboratorio: permite ejercicio del servicio; el router HTTP sigue exigiendo flag.
    env["OFFLINE_SYNC_WRITES_ENABLED"] = "1"
    env["OFFLINE_SYNC_PILOT_ALLOWLIST"] = "42"
    env["OFFLINE_SYNC_PILOT_PLAN_ALLOWLIST"] = "910,920"
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    Path(ROOT / "_tmp_isolated_pg_dsn.txt").write_text(uri + "\n", encoding="utf-8")

    cmd = [
        sys.executable,
        "-m",
        "pytest",
        "backend/tests/test_delivery_offline_ops_2a1_isolated_pg.py",
        "-vv",
        "--tb=short",
    ]
    print(f"[isolated-061] running: {' '.join(cmd)}", flush=True)
    proc = subprocess.run(cmd, cwd=str(ROOT), env=env)
    _ = srv
    print(f"[isolated-061] pytest_exit={proc.returncode}", flush=True)
    return int(proc.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
