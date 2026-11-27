"""OWNER-managed operational switches, not a generic environment editor.

Only this explicit registry can be controlled. Existing env defaults apply
before migration and until the first override; a DB error never silently
reenables a feature. No per-worker cache, no write from read paths.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from importlib.util import find_spec
import logging
import os
from pathlib import Path

from fastapi import HTTPException
from backend.db import get_connection
from backend.services.catalog_v2.source_flag import resolve_catalog_source

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Feature:
    env: str
    title: str
    description: str


FEATURES = {
    "product_master_pallet_writes": Feature(
        "PRODUCT_MASTER_PALLET_WRITES_ENABLED", "Cajas por pallet",
        "Permite registrar y editar las cajas por pallet en Productos Logísticos.",
    ),
    "catalog_photo_writes": Feature(
        "CATALOG_PHOTO_WRITES_ENABLED", "Publicación de fotografías",
        "Permite guardar fotografías WebP confirmadas en GitHub desde el ERP.",
    ),
    "catalog_photo_ai": Feature(
        "CATALOG_PHOTO_AI_ENABLED", "Eliminar fondo con IA",
        "Permite preparar fotografías con recorte automático y fondo transparente o blanco.",
    ),
}


def env_default(key: str) -> bool:
    return os.getenv(FEATURES[key].env, "").strip().lower() in {"1", "true", "yes", "on"}


def _schema_ready(cur) -> bool:
    cur.execute("""
        SELECT to_regclass('app.system_feature_controls') IS NOT NULL,
               to_regclass('app.system_feature_control_audit') IS NOT NULL
    """)
    settings, audit = cur.fetchone()
    if settings != audit:
        raise HTTPException(503, "El esquema de configuración está incompleto.")
    return bool(settings and audit)


def _read_override(cur, key: str):
    cur.execute("""
        SELECT enabled_override, row_version, updated_by, updated_at
        FROM app.system_feature_controls WHERE feature_key=%s
    """, (key,))
    return cur.fetchone() or (None, 0, None, None)


def _configured(cur, key: str) -> bool:
    inherited = env_default(key)
    if not _schema_ready(cur):
        return inherited
    override, _, _, _ = _read_override(cur, key)
    return inherited if override is None else bool(override)


def enabled(key: str, *, cur=None) -> bool:
    """Uncached gate for existing feature consumers. Fail closed on read errors."""
    if key not in FEATURES:
        raise ValueError("Unknown feature key")
    try:
        if cur is not None:
            return _configured(cur, key)
        with closing(get_connection()) as conn, conn.cursor() as cursor:
            return _configured(cursor, key)
    except Exception as exc:
        # Do not log connection strings, credentials or exception payloads.
        logger.warning("system_feature_read_failed key=%s error_type=%s", key, type(exc).__name__)
        return False


def photo_credentials_ready() -> bool:
    return bool(os.getenv("CATALOG_PHOTO_GITHUB_TOKEN", "").strip()
                and len(os.getenv("CATALOG_PHOTO_PREVIEW_SECRET", "")) >= 32)


def ai_model_ready() -> bool:
    """No downloads/inference here. The model must already be baked into the image."""
    try:
        home = os.getenv("U2NET_HOME") or os.path.join(os.getenv("XDG_DATA_HOME", "~"), ".u2net")
        model = Path(home).expanduser() / "u2netp.onnx"
        return bool(find_spec("rembg") and find_spec("onnxruntime")
                    and model.is_file() and model.stat().st_size > 0)
    except (ImportError, ValueError, OSError):
        return False


def _requirements(cur, key: str) -> str | None:
    if key == "product_master_pallet_writes":
        cur.execute("""
            SELECT EXISTS (SELECT 1 FROM information_schema.columns
                         WHERE table_schema='bsale' AND table_name='products_master'
                           AND column_name='boxes_per_pallet'),
                   to_regclass('app.product_master_pallet_audit') IS NOT NULL
        """)
        col, audit = cur.fetchone()
        if not (col and audit):
            return "Falta aplicar la migración 070 de cajas por pallet."
    elif key == "catalog_photo_writes":
        if not photo_credentials_ready():
            return "Faltan las credenciales privadas de publicación en el servidor."
        cur.execute("SELECT to_regclass('app.catalog_photo_audit') IS NOT NULL")
        if not cur.fetchone()[0]:
            return "Falta la auditoría de fotografías de la migración 070."
        if resolve_catalog_source(consumer="public_web") != "legacy":
            return "La publicación requiere validar primero la asociación del catálogo V2."
    elif key == "catalog_photo_ai":
        if not ai_model_ready():
            return "Falta instalar el procesador de imágenes y su modelo en la imagen del backend."
    return None


def _item(cur, key: str, *, ready: bool) -> dict:
    spec = FEATURES[key]
    override, version, actor, updated = _read_override(cur, key) if ready else (None, 0, None, None)
    inherited = env_default(key)
    configured = inherited if override is None else bool(override)
    reason = _requirements(cur, key)
    return {
        "key": key, "title": spec.title, "description": spec.description,
        "enabled_override": override, "environment_default": inherited,
        "configured_enabled": configured, "effective_enabled": bool(configured and reason is None),
        "source": "erp" if override is not None else "environment",
        "requirements_ready": reason is None, "blocked_reason": reason,
        "row_version": int(version), "updated_by": actor,
        "updated_at": updated.isoformat() if updated is not None else None,
    }


def _state(cur) -> dict:
    ready = _schema_ready(cur)
    items = [_item(cur, k, ready=ready) for k in FEATURES]
    history = []
    if ready:
        cur.execute("""
            SELECT id, feature_key, actor_user_id, before_override, after_override,
                   before_configured, after_configured, row_version, created_at
            FROM app.system_feature_control_audit
            WHERE feature_key = ANY(%s)
            ORDER BY id DESC LIMIT 30
        """, (list(FEATURES),))
        for r in cur.fetchall():
            history.append({
                "id": int(r[0]), "key": r[1], "actor_user_id": int(r[2]),
                "before_override": r[3], "after_override": r[4],
                "before_configured": r[5], "after_configured": r[6],
                "row_version": int(r[7]), "created_at": r[8].isoformat(),
            })
    return {"schema_ready": ready, "items": items, "history": history}


def get_state() -> dict:
    try:
        with closing(get_connection()) as conn:
            # All switch state and recent history share a single committed snapshot.
            conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
            with conn.cursor() as cur:
                return _state(cur)
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("system_feature_state_failed error_type=%s", type(exc).__name__)
        raise HTTPException(503, "No se pudo consultar la configuración del sistema.") from None


def set_override(key: str, *, override: bool | None, expected_version: int, actor: int) -> dict:
    if key not in FEATURES:
        raise HTTPException(404, "Funcionalidad no administrable desde este panel.")
    if type(override) not in (bool, type(None)) or type(expected_version) is not int or expected_version < 0:
        raise HTTPException(422, "Valor de configuración no válido.")
    if type(actor) is not int or actor <= 0:
        raise HTTPException(403, "Identidad OWNER no válida.")
    try:
        with closing(get_connection()) as conn, conn:
            with conn.cursor() as cur:
                if not _schema_ready(cur):
                    raise HTTPException(503, "Aplica primero la migración 071 de configuración.")
                cur.execute("SET LOCAL lock_timeout = '5s'")
                cur.execute("""
                    INSERT INTO app.system_feature_controls (feature_key)
                    VALUES (%s) ON CONFLICT (feature_key) DO NOTHING
                """, (key,))
                cur.execute("""
                    SELECT enabled_override, row_version
                    FROM app.system_feature_controls WHERE feature_key=%s FOR UPDATE
                """, (key,))
                previous, version = cur.fetchone()
                if version != expected_version:
                    raise HTTPException(409, "La configuración cambió. Actualiza antes de volver a guardar.")
                inherited = env_default(key)
                before = inherited if previous is None else bool(previous)
                after = inherited if override is None else override
                if after and (reason := _requirements(cur, key)):
                    raise HTTPException(409, reason)
                if previous != override:
                    cur.execute("""
                        UPDATE app.system_feature_controls
                        SET enabled_override=%s, row_version=row_version+1,
                            updated_by=%s, updated_at=now()
                        WHERE feature_key=%s
                        RETURNING row_version
                    """, (override, actor, key))
                    new_version = cur.fetchone()[0]
                    cur.execute("""
                        INSERT INTO app.system_feature_control_audit
                            (feature_key, actor_user_id, before_override, after_override,
                             before_configured, after_configured, row_version)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                    """, (key, actor, previous, override, before, after, new_version))
                # Commit is performed by connection context before the response escapes.
                return _state(cur)
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("system_feature_write_failed key=%s error_type=%s", key, type(exc).__name__)
        raise HTTPException(503, "No se pudo guardar la configuración. Actualiza el estado antes de reintentar.") from None
