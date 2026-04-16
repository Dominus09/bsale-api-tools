"""
Job opcional: relaciones OC ↔ documentos (``relateddetailid``).

No compite con el lock del sync principal (usa otro advisory lock).
Ejecución manual / cron, p. ej.:

  python -m backend.jobs.sync_distribuidora_related
"""

from __future__ import annotations

from backend.services.distribuidora.sync_related_service import (
    sync_distribuidora_related_documents,
)


def main() -> None:
    sync_distribuidora_related_documents(strict_token=True)


if __name__ == "__main__":
    main()
