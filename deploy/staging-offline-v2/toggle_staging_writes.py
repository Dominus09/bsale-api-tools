"""Activa/desactiva writes offline en proceso staging (kill switch).

Uso:
  python deploy/staging-offline-v2/toggle_staging_writes.py --off
  python deploy/staging-offline-v2/toggle_staging_writes.py --on --allowlist 42,77 --i-confirm-staging

No toca producción. Solo imprime el export sugerido / archivo env local.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = ROOT / "deploy" / "staging-offline-v2" / ".env.staging"


def main() -> int:
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--off", action="store_true", help="Kill switch: writes OFF + vacía allowlist")
    g.add_argument("--on", action="store_true", help="Activa writes solo con allowlist")
    p.add_argument("--allowlist", default="", help="user_ids separados por coma")
    p.add_argument("--i-confirm-staging", action="store_true")
    args = p.parse_args()

    if args.on and not args.i_confirm_staging:
        print("Requiere --i-confirm-staging para --on", flush=True)
        return 2
    if args.on and not args.allowlist.strip():
        print("--on requiere --allowlist no vacío", flush=True)
        return 2

    lines = []
    if ENV_FILE.exists():
        lines = ENV_FILE.read_text(encoding="utf-8").splitlines()

    def upsert(key: str, value: str) -> None:
        nonlocal lines
        found = False
        out = []
        for ln in lines:
            if ln.startswith(key + "="):
                out.append(f"{key}={value}")
                found = True
            else:
                out.append(ln)
        if not found:
            out.append(f"{key}={value}")
        lines = out

    if args.off:
        upsert("OFFLINE_SYNC_WRITES_ENABLED", "false")
        upsert("OFFLINE_SYNC_PILOT_ALLOWLIST", "")
        upsert("OFFLINE_V2_ALLOW_WRITES_AT_BOOT", "0")
        print("[kill-switch] writes OFF + allowlist vacía", flush=True)
    else:
        upsert("OFFLINE_SYNC_WRITES_ENABLED", "true")
        upsert("OFFLINE_SYNC_PILOT_ALLOWLIST", args.allowlist.replace(" ", ""))
        upsert("OFFLINE_V2_ALLOW_WRITES_AT_BOOT", "1")
        print(
            f"[staging] writes ON allowlist={args.allowlist!r} "
            "(solo proceso staging; reiniciar API)",
            flush=True,
        )

    ENV_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[staging] actualizado {ENV_FILE} (gitignored)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
