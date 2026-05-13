"""Shim de compatibilidad: use ``python -m backend.debug.export_oc_bs_only``."""

from __future__ import annotations

from backend.debug.export_oc_bs_only import run

if __name__ == "__main__":
    raise SystemExit(run())
