"""Shim: el módulo está en ``backend.audits``. Preferir::

    python -m backend.audits.audit_document_related
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.audits.audit_document_related import main

if __name__ == "__main__":
    raise SystemExit(main())
