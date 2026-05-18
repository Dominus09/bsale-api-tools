"""
Persistencia de fotos de visitas (incidencias) en filesystem.

- No guarda binarios en PostgreSQL: en BD queda clave relativa ``visitas/{id}.ext`` o URL http(s).
- Compatible con app móvil: sigue enviando ``foto_url`` en POST /visitas (string).
- Si llega ``data:image/...;base64,...`` se materializa en disco en el sync.
"""

from __future__ import annotations

import base64
import binascii
import logging
import os
import re
from pathlib import Path

logger = logging.getLogger(__name__)

_UPLOAD_ROOT = Path(
    os.getenv("VISITA_FOTOS_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "data", "uploads", "visitas")),
).resolve()
_REL_PREFIX = "visitas/"
_DATA_URL_RE = re.compile(r"^data:image/(?P<fmt>[a-zA-Z0-9+.-]+);base64,(?P<data>.+)$", re.DOTALL)
_EXT_BY_FMT = {"jpeg": "jpg", "jpg": "jpg", "png": "png", "webp": "webp", "gif": "gif"}


def upload_root() -> Path:
    _UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
    return _UPLOAD_ROOT


def storage_key(visita_id: int, ext: str = "jpg") -> str:
    return f"{_REL_PREFIX}{visita_id}.{ext.lstrip('.')}"


def path_for_key(key: str) -> Path | None:
    if not key or not str(key).startswith(_REL_PREFIX):
        return None
    name = key.split("/", 1)[-1]
    if ".." in name or "/" in name:
        return None
    p = upload_root() / name
    return p if p.is_file() else None


def path_for_visita_id(visita_id: int) -> Path | None:
    root = upload_root()
    for ext in ("jpg", "jpeg", "png", "webp", "gif"):
        p = root / f"{visita_id}.{ext}"
        if p.is_file():
            return p
    return None


def tiene_archivo_foto(visita_id: int, stored: str | None) -> bool:
    if stored and str(stored).strip().startswith("http"):
        return True
    if stored and str(stored).strip().startswith("data:image"):
        return True
    if stored and path_for_key(str(stored).strip()):
        return True
    return path_for_visita_id(visita_id) is not None


def normalize_and_persist_foto_url(visita_id: int, raw: str | None) -> str | None:
    """
    Normaliza ``foto_url`` para guardar en BD.

    - ``http(s)://`` → se conserva.
    - ``data:image/...`` → archivo en disco + clave ``visitas/{id}.ext``.
    - clave ``visitas/...`` existente → se conserva.
    - ``file://`` u otros → se ignoran (log) y no se pisa la BD.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None

    if s.startswith("http://") or s.startswith("https://"):
        logger.debug("foto visita_id=%s URL externa conservada", visita_id)
        return s

    if s.startswith(_REL_PREFIX):
        return s

    m = _DATA_URL_RE.match(s)
    if m:
        fmt = (m.group("fmt") or "jpeg").lower()
        ext = _EXT_BY_FMT.get(fmt, "jpg")
        try:
            data = base64.b64decode(m.group("data"), validate=True)
        except (ValueError, binascii.Error) as e:
            logger.warning("foto base64 inválida visita_id=%s: %s", visita_id, e)
            return None
        return _write_bytes(visita_id, data, ext)

    if s.startswith("file://"):
        logger.warning("foto file:// no persistible visita_id=%s", visita_id)
        return None

    # Base64 crudo (sin prefijo data:) — heurística móvil legacy
    if len(s) > 200 and re.fullmatch(r"[A-Za-z0-9+/=\s]+", s):
        try:
            data = base64.b64decode(s, validate=True)
            if len(data) > 100:
                return _write_bytes(visita_id, data, "jpg")
        except (ValueError, Exception):
            pass

    # Path local legacy: intentar conservar texto si parece clave
    if "/" not in s and len(s) < 256:
        return s

    logger.debug("foto visita_id=%s formato no persistido len=%s", visita_id, len(s))
    return s


def resolve_foto_display(stored: str | None, visita_id: int) -> tuple[str | None, bool]:
    """
  Retorna (url_para_cliente, tiene_foto).

  - data: o http(s) → url directa.
  - clave ``visitas/`` o archivo en disco → path API relativo ``/operaciones/foto/{id}``.
    """
    if not stored or not str(stored).strip():
        has = path_for_visita_id(visita_id) is not None
        return (f"/operaciones/foto/{visita_id}" if has else None, has)

    s = str(stored).strip()
    if s.startswith("data:image") or s.startswith("http://") or s.startswith("https://"):
        return s, True

    has_file = path_for_key(s) is not None or path_for_visita_id(visita_id) is not None
    if has_file or s.startswith(_REL_PREFIX):
        return f"/operaciones/foto/{visita_id}", True

    return None, False


def _write_bytes(visita_id: int, data: bytes, ext: str) -> str:
    root = upload_root()
    key = storage_key(visita_id, ext)
    dest = root / key.split("/", 1)[-1]
    dest.write_bytes(data)
    logger.info("foto persistida visita_id=%s bytes=%s path=%s", visita_id, len(data), dest)
    return key
