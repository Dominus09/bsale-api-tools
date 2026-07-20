"""Reservado para runners Python de DDL; el DDL versionado sigue en ``backend/sql/``.

Aplicar schema de distribuidora:

    python -m backend.jobs.apply_distribuidora_schema

No ejecutar DDL desde syncs, endpoints HTTP ni crons.
"""

from backend.repositories.distribuidora.sync_repo import (
    DISTRIBUIDORA_SCHEMA_FILES,
    apply_distribuidora_migrations,
)

__all__ = [
    "DISTRIBUIDORA_SCHEMA_FILES",
    "apply_distribuidora_migrations",
]
