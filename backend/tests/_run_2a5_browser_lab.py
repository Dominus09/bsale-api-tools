"""Orquesta laboratorio 2A.5: PG + FastAPI lab + Next + Playwright IndexedDB.

NO prod. NO analytics. NO push.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PWA = ROOT / "apps" / "pwa"
API_PORT = int(os.getenv("LAB_2A5_PORT", "18765"))
PWA_PORT = int(os.getenv("LAB_PWA_PORT", "3015"))


def _wait_http(url: str, timeout_s: float = 90.0) -> None:
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as r:
                if 200 <= r.status < 500:
                    return
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(0.5)
    raise SystemExit(f"Timeout esperando {url}: {last}")


def main() -> int:
    import pgserver

    os.environ["OFFLINE_SYNC_WRITES_ENABLED"] = "1"
    os.environ["OFFLINE_SYNC_PILOT_ALLOWLIST"] = "42"
    os.environ["OFFLINE_SYNC_PILOT_PLAN_ALLOWLIST"] = "920"
    os.environ["LAB_2A5_PORT"] = str(API_PORT)
    os.environ["PYTHONPATH"] = str(ROOT) + os.pathsep + os.environ.get("PYTHONPATH", "")

    base = Path(tempfile.mkdtemp(prefix="p4_2a5_browser_"))
    srv = pgserver.get_server(base / "pgdata", cleanup_mode="stop")
    uri = srv.get_uri() if callable(getattr(srv, "get_uri", None)) else str(srv)
    if callable(getattr(srv, "get_uri", None)):
        uri = srv.get_uri()
    print(f"[2a5] pg={uri}", flush=True)

    # Import lab bootstrap + app
    sys.path.insert(0, str(ROOT))
    from backend.tests._lab_2a5_offline_api import bootstrap, build_app

    bootstrap(uri)

    import uvicorn
    from threading import Thread

    app = build_app()

    def run_api():
        uvicorn.run(app, host="127.0.0.1", port=API_PORT, log_level="warning")

    t = Thread(target=run_api, daemon=True)
    t.start()
    _wait_http(f"http://127.0.0.1:{API_PORT}/health")
    print(f"[2a5] api ready :{API_PORT}", flush=True)

    env = os.environ.copy()
    env["API_PROXY_TARGET"] = f"http://127.0.0.1:{API_PORT}"
    env["NEXT_PUBLIC_API_URL"] = f"http://127.0.0.1:{API_PORT}"
    # Sync browser → API lab directo (CORS); evita multipart colgado en proxy Next.
    env["NEXT_PUBLIC_LAB_API_DIRECT"] = f"http://127.0.0.1:{API_PORT}"
    env["NEXT_PUBLIC_OFFLINE_SYNC_WRITES"] = "1"
    # Middleware: habilita /pruebas/offline-v2-lab solo en este proceso lab.
    # Solo env de servidor (middleware NO acepta NEXT_PUBLIC_*).
    env["OFFLINE_V2_LAB_ENABLED"] = "1"
    env.pop("NEXT_PUBLIC_OFFLINE_V2_LAB_ENABLED", None)
    env["PORT"] = str(PWA_PORT)

    # Prefer already-built standalone if present; else next dev
    npm = "npm.cmd" if os.name == "nt" else "npm"
    npx = "npx.cmd" if os.name == "nt" else "npx"
    next_cmd = [
        npx,
        "next",
        "dev",
        "-p",
        str(PWA_PORT),
        "-H",
        "127.0.0.1",
    ]
    next_log = ROOT / "_tmp_2a5_next_dev.log"
    print(f"[2a5] starting Next: {' '.join(next_cmd)} log={next_log}", flush=True)
    next_log_f = open(next_log, "w", encoding="utf-8", errors="replace")
    next_proc = subprocess.Popen(
        next_cmd,
        cwd=str(PWA),
        env=env,
        stdout=next_log_f,
        stderr=subprocess.STDOUT,
        shell=False,
    )
    try:
        _wait_http(f"http://127.0.0.1:{PWA_PORT}/pruebas/offline-v2-lab", timeout_s=180)
        # Exigir 200 real (middleware no debe devolver 404 en lab).
        with urllib.request.urlopen(
            f"http://127.0.0.1:{PWA_PORT}/pruebas/offline-v2-lab", timeout=5
        ) as r:
            if r.status != 200:
                raise SystemExit(f"Lab page status={r.status} (middleware?)")
        print("[2a5] PWA lab page ready", flush=True)

        pw_env = env.copy()
        pw_env["LAB_PWA_URL"] = f"http://127.0.0.1:{PWA_PORT}"
        pw_env["LAB_API_URL"] = f"http://127.0.0.1:{API_PORT}"
        pw = subprocess.run(
            [
                npx,
                "playwright",
                "test",
                "e2e/offline-v2-2a5-idb.spec.ts",
                "e2e/offline-v2-driver-flow.spec.ts",
                "--config=playwright.config.ts",
            ],
            cwd=str(PWA),
            env=pw_env,
        )
        _ = npm
        report = {
            "suite": "offline_v2_2a5_browser_idb",
            "playwright_exit": int(pw.returncode),
            "api": f"127.0.0.1:{API_PORT}",
            "pwa": f"127.0.0.1:{PWA_PORT}",
            "indexeddb": "quillotana-ops-outbox-v2 (persistent context)",
            "memory_store": False,
            "production_sql": False,
            "push": False,
            "safari_ios_physical": False,
        }
        out = ROOT / "_tmp_2a5_browser_lab_report.json"
        out.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"[2a5] report={out} exit={pw.returncode}", flush=True)
        code = int(pw.returncode)
    finally:
        next_proc.terminate()
        try:
            next_proc.wait(timeout=10)
        except Exception:
            next_proc.kill()
        try:
            next_log_f.close()
        except Exception:
            pass
        _ = srv
    os._exit(code)


if __name__ == "__main__":
    raise SystemExit(main())
