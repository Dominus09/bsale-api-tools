"""Shim de compatibilidad (typo histórico): reenvía a ``backend.debug.debug_full_bsale_relationships``."""

from __future__ import annotations

from backend.debug.debug_full_bsale_relationships import main

if __name__ == "__main__":
    raise SystemExit(main())
