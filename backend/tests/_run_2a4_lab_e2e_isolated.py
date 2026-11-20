"""Arranca Postgres temporal y ejecuta suite Offline V2 2A.4 lab E2E.

Nunca analytics/prod. Esquema sintético 056→061.
"""

from __future__ import annotations

import json
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

    base = Path(tempfile.mkdtemp(prefix="p4_2a4_lab_e2e_"))
    pgdata = base / "pgdata"
    print(f"[2a4-lab] pgdata={pgdata}", flush=True)
    srv = pgserver.get_server(pgdata, cleanup_mode="stop")
    uri = srv.get_uri() if callable(getattr(srv, "get_uri", None)) else getattr(srv, "uri", str(srv))
    if callable(getattr(srv, "get_uri", None)):
        uri = srv.get_uri()
    print(f"[2a4-lab] uri={uri}", flush=True)
    _assert_isolated_dsn(uri)

    conn = psycopg2.connect(uri)
    cur = conn.cursor()
    cur.execute("SHOW data_directory")
    datadir = cur.fetchone()[0]
    print(f"[2a4-lab] data_directory={datadir}", flush=True)
    if "analytics" in str(datadir).lower() or "quillotana" in str(datadir).lower():
        raise SystemExit("data_directory productivo — abort")
    conn.close()

    env = os.environ.copy()
    env["SIMPLE_PLANNING_TEST_DSN"] = uri
    env["OFFLINE_SYNC_WRITES_ENABLED"] = "1"
    env["OFFLINE_SYNC_PILOT_ALLOWLIST"] = "42"
    env["OFFLINE_SYNC_PILOT_PLAN_ALLOWLIST"] = "920"
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")

    cmd = [
        sys.executable,
        "-m",
        "pytest",
        "backend/tests/test_offline_v2_2a4_lab_e2e.py",
        "backend/tests/test_offline_v2_2a5_concurrency_plan_customer.py",
        "-vv",
        "--tb=short",
    ]
    print(f"[2a4-lab] running: {' '.join(cmd)}", flush=True)
    proc = subprocess.run(cmd, cwd=str(ROOT), env=env)
    report = {
        "suite": "offline_v2_2a4_lab_e2e",
        "pytest_exit": int(proc.returncode),
        "dsn_host": "127.0.0.1 (pgserver temp)",
        "offline_sync_writes_enabled": "1 (lab runner only)",
        "production_sql": False,
        "push": False,
        "safari_ios_physical": False,
        "note_es": (
            "E2E laboratorio = contrato sync + PostgreSQL real aislado + R2 mock. "
            "No navegador Safari iOS físico en esta corrida."
        ),
    }
    out = ROOT / "_tmp_2a4_lab_e2e_report.json"
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"[2a4-lab] report={out}", flush=True)
    print(f"[2a4-lab] pytest_exit={proc.returncode}", flush=True)
    # Mantener srv vivo hasta el final; evitar atexit de pgserver alterando el exit code.
    code = int(proc.returncode)
    _ = srv
    os._exit(code)


if __name__ == "__main__":
    raise SystemExit(main())
