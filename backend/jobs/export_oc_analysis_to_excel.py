"""Shim de compatibilidad: use ``python -m backend.debug.export_oc_analysis_to_excel``."""

from __future__ import annotations

from backend.debug.export_oc_analysis_to_excel import run_export

if __name__ == "__main__":
    raise SystemExit(run_export())
