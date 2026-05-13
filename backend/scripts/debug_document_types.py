"""Shim: use ``python -m backend.debug.debug_document_types``."""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.debug.debug_document_types import main

if __name__ == "__main__":
    main()
