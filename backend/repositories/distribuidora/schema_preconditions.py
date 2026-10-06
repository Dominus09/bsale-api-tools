"""Objetos de schema que el código de reemisiones (F1A/F1B) exige; solo lectura de catálogo.

Sin ``document_detail_history`` el reemplazo de líneas volvería a borrar relaciones y sin las
vistas de linaje la facturación de OC reemitidas se calcularía mal. Nunca hace DDL: el schema
se aplica con ``python -m backend.jobs.apply_distribuidora_schema`` (048).
"""

from __future__ import annotations

import logging
import sys

logger = logging.getLogger(__name__)

SCHEMA_MISSING_EXIT_CODE = 3

REISSUE_SCHEMA_OBJECTS: tuple[str, ...] = (
    "distribuidora.document_detail_history",
    "distribuidora.v_document_detail_lineage",
    "distribuidora.v_document_related_resolved",
)

_REISSUE_SCHEMA_OK = False


class SchemaPreconditionError(RuntimeError):
    """Falta schema requerido por el código desplegado."""


def missing_reissue_schema_objects(cur) -> list[str]:
    cur.execute(
        "SELECT name FROM unnest(%s::text[]) AS name WHERE to_regclass(name) IS NULL ORDER BY name",
        (list(REISSUE_SCHEMA_OBJECTS),),
    )
    return [str(r[0]) for r in (cur.fetchall() or [])]


def require_reissue_schema(cur) -> None:
    """Falla si falta 048. Solo cachea el positivo (aplicar el schema no exige reinicio)."""
    global _REISSUE_SCHEMA_OK
    if _REISSUE_SCHEMA_OK:
        return
    missing = missing_reissue_schema_objects(cur)
    if missing:
        raise SchemaPreconditionError(
            "Schema distribuidora incompleto (falta 048_document_reissue_lineage.sql): "
            f"{', '.join(missing)}. Ejecutar: python -m backend.jobs.apply_distribuidora_schema"
        )
    _REISSUE_SCHEMA_OK = True


def missing_reissue_schema_objects_new_connection() -> list[str] | None:
    """``None`` si no se pudo consultar (la caída de BD la reporta el propio job/endpoint)."""
    from backend.db import get_connection

    try:
        conn = get_connection()
    except Exception as exc:
        logger.warning("schema_preconditions: sin conexión para verificar 048: %s", exc)
        return None
    try:
        cur = conn.cursor()
        try:
            return missing_reissue_schema_objects(cur)
        finally:
            cur.close()
    except Exception as exc:
        logger.warning("schema_preconditions: verificación 048 falló: %s", exc)
        return None
    finally:
        conn.close()


def reissue_schema_exit_code(job: str) -> int:
    """Para jobs que escriben distribuidora: 0 = seguir; distinto de 0 = abortar antes de Bsale."""
    missing = missing_reissue_schema_objects_new_connection()
    if not missing:
        return 0
    msg = (
        f"[{job}] ABORTADO: schema distribuidora incompleto (falta 048): {', '.join(missing)}. "
        "Ejecutar: python -m backend.jobs.apply_distribuidora_schema"
    )
    logger.critical(msg)
    print(msg, file=sys.stderr, flush=True)
    return SCHEMA_MISSING_EXIT_CODE
