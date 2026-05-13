"""Shim: use ``python -m backend.maintenance.cleanup_document_related_invalid_types``."""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.maintenance.cleanup_document_related_invalid_types import main

if __name__ == "__main__":
    raise SystemExit(main())
