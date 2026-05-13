"""Shim: use ``python -m backend.debug.analyze_purchase_orders_relationships``."""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.debug.analyze_purchase_orders_relationships import main

if __name__ == "__main__":
    main()
