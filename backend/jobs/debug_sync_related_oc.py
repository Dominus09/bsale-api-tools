"""
Depuración: sync solo ``document_related`` para una OC por número (Bsale ``number``).

Uso:
  python -m backend.jobs.debug_sync_related_oc [66080]
"""

from __future__ import annotations

import json
import sys

from backend.services.distribuidora.sync_related_service import debug_sync_related_for_document


def main() -> None:
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 66080
    out = debug_sync_related_for_document(n)
    print(json.dumps(out, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
