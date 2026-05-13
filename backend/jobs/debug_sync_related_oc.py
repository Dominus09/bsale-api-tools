"""Shim de compatibilidad: el módulo vive en ``backend.debug``.

Ejecute preferentemente::

    python -m backend.debug.debug_sync_related_oc [número]
"""

from __future__ import annotations

from backend.debug.debug_sync_related_oc import main

if __name__ == "__main__":
    main()
