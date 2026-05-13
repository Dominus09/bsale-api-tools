"""Shim de compatibilidad: use ``python -m backend.debug.analyze_related_patterns``."""

from __future__ import annotations

from backend.debug.analyze_related_patterns import main

if __name__ == "__main__":
    raise SystemExit(main())
