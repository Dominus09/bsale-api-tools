"""Arranca stack lab local: PG temporal + FastAPI lab + Next (HTTPS vía cloudflared si existe).

NO Coolify prod. NO analytics. NO push.
"""

from __future__ import annotations

import json
import os
import secrets
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
API_PORT = int(os.getenv("LAB_API_PORT", "18080"))
PWA_PORT = int(os.getenv("LAB_PWA_PORT", "13015"))


def _wait_http(url: str, timeout_s: float = 120.0) -> None:
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as r:
                if 200 <= r.status < 500:
                    return
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(0.4)
    raise SystemExit(f"Timeout {url}: {last}")


def main() -> int:
    import pgserver

    os.environ["OFFLINE_V2_ENV_PROFILE"] = "local-lab"
    os.environ["OFFLINE_SYNC_WRITES_ENABLED"] = "false"  # boot OFF
    os.environ["JWT_SECRET_KEY"] = os.environ.get(
        "JWT_SECRET_KEY", "lab-" + secrets.token_hex(16)
    )
    # Gate lab route
    gate = os.environ.get("OFFLINE_V2_LAB_GATE_TOKEN") or secrets.token_urlsafe(18)
    os.environ["OFFLINE_V2_LAB_GATE_TOKEN"] = gate
    os.environ["OFFLINE_V2_LAB_ENABLED"] = "1"

    base = Path(tempfile.mkdtemp(prefix="offline_v2_staging_lab_"))
    srv = pgserver.get_server(base / "pgdata", cleanup_mode="stop")
    uri = srv.get_uri() if callable(getattr(srv, "get_uri", None)) else str(srv)
    if callable(getattr(srv, "get_uri", None)):
        uri = srv.get_uri()
    os.environ["STAGING_DATABASE_URL"] = uri
    os.environ["DATABASE_URL"] = uri
    os.environ["SIMPLE_PLANNING_TEST_DSN"] = uri
    os.environ["PYTHONPATH"] = str(ROOT) + os.pathsep + os.environ.get("PYTHONPATH", "")

    # Isolation
    sys.path.insert(0, str(ROOT / "deploy" / "staging-offline-v2"))
    import isolation_assert as iso

    iso.assert_staging_isolation(require_writes_off=True)

    # Apply 061
    rc = subprocess.call(
        [
            sys.executable,
            str(ROOT / "deploy" / "staging-offline-v2" / "apply_061_staging_only.py"),
            "--i-confirm-staging",
        ],
        cwd=str(ROOT),
        env=os.environ.copy(),
    )
    if rc != 0:
        return rc

    # FastAPI lab (datos ficticios) — writes siguen OFF hasta toggle manual
    from backend.tests._lab_2a5_offline_api import bootstrap, build_app
    import uvicorn

    bootstrap(uri)
    app = build_app()

    def run_api():
        uvicorn.run(app, host="127.0.0.1", port=API_PORT, log_level="warning")

    threading.Thread(target=run_api, daemon=True).start()
    _wait_http(f"http://127.0.0.1:{API_PORT}/health")

    env = os.environ.copy()
    env["API_PROXY_TARGET"] = f"http://127.0.0.1:{API_PORT}"
    env["NEXT_PUBLIC_API_URL"] = f"http://127.0.0.1:{API_PORT}"
    env["NEXT_PUBLIC_LAB_API_DIRECT"] = f"http://127.0.0.1:{API_PORT}"
    env["NEXT_PUBLIC_OFFLINE_SYNC_WRITES"] = "0"
    env["OFFLINE_V2_LAB_ENABLED"] = "1"
    env["OFFLINE_V2_LAB_GATE_TOKEN"] = gate
    env["PORT"] = str(PWA_PORT)

    npx = "npx.cmd" if os.name == "nt" else "npx"
    next_log = ROOT / "_tmp_staging_next.log"
    next_log_f = open(next_log, "w", encoding="utf-8", errors="replace")
    next_proc = subprocess.Popen(
        [npx, "next", "dev", "-p", str(PWA_PORT), "-H", "127.0.0.1"],
        cwd=str(ROOT / "apps" / "pwa"),
        env=env,
        stdout=next_log_f,
        stderr=subprocess.STDOUT,
    )
    try:
        _wait_http(f"http://127.0.0.1:{PWA_PORT}/login", timeout_s=180)

        report = {
            "profile": "local-lab",
            "pwa_local": f"http://127.0.0.1:{PWA_PORT}",
            "api_local": f"http://127.0.0.1:{API_PORT}",
            "lab_gate_required": True,
            "lab_gate_cookie": "lq_offline_v2_lab_gate",
            "writes_enabled": False,
            "061_applied": True,
            "production_untouched": True,
            "domain_lq_ops_dev": "NXDOMAIN — usar túnel HTTPS",
            "note_es": (
                "Para iPhone: exponer PWA+API con cloudflared/ngrok a estos puertos. "
                "Luego set OFFLINE_SYNC_WRITES_ENABLED=1 + allowlist solo en este proceso."
            ),
            # gate token en archivo local gitignored, no en stdout completo si posible
        }
        out = ROOT / "_tmp_staging_lab_status.json"
        out.write_text(json.dumps(report, indent=2), encoding="utf-8")
        gate_file = ROOT / "deploy" / "staging-offline-v2" / ".lab_gate_token"
        gate_file.write_text(gate + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2), flush=True)
        print(f"[lab] gate token file={gate_file} (no commitear)", flush=True)
        print("[lab] stack running — Ctrl+C para detener", flush=True)

        # Opcional: cloudflared si está instalado
        cf = None
        for cand in ("cloudflared", "cloudflared.exe"):
            try:
                subprocess.run([cand, "--version"], capture_output=True, check=True)
                cf = cand
                break
            except Exception:
                continue
        if cf:
            print(f"[lab] iniciando {cf} tunnel → PWA :{PWA_PORT}", flush=True)
            subprocess.Popen(
                [cf, "tunnel", "--url", f"http://127.0.0.1:{PWA_PORT}"],
                cwd=str(ROOT),
            )
        else:
            print(
                "[lab] cloudflared no instalado — admin debe exponer HTTPS "
                f"a 127.0.0.1:{PWA_PORT} (y API :{API_PORT} o proxy)",
                flush=True,
            )

        next_proc.wait()
        return int(next_proc.returncode or 0)
    except KeyboardInterrupt:
        return 0
    finally:
        next_proc.terminate()
        try:
            next_proc.wait(timeout=8)
        except Exception:
            next_proc.kill()
        try:
            next_log_f.close()
        except Exception:
            pass
        _ = srv


if __name__ == "__main__":
    raise SystemExit(main())
