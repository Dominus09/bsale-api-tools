"""Validated image processing and a narrowly scoped GitHub Contents client.

No temporary server files, no client-selected repository or path, no write on preview.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import io
import json
import os
import re
import threading
import time
import warnings
from functools import lru_cache

import requests
from PIL import Image, ImageOps, UnidentifiedImageError
from fastapi import HTTPException

REPO = "Dominus09/cat-quillotana"
BRANCH = "main"
PREFIX = "public/products"
CDN = "https://cat.quillotana.cl/products"
MAX_UPLOAD = 15 * 1024 * 1024
MAX_PIXELS = 32_000_000
MAX_WEBP = 900_000
SIZE = 1000
TTL = 900
_AI_LOCK = threading.Lock()
_SHA = re.compile(r"[0-9a-f]{40}")


def flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def writes_enabled() -> bool:
    return bool(flag("CATALOG_PHOTO_WRITES_ENABLED")
                and os.getenv("CATALOG_PHOTO_GITHUB_TOKEN", "").strip()
                and len(os.getenv("CATALOG_PHOTO_PREVIEW_SECRET", "")) >= 32)


def safe_barcode(value: str) -> str:
    value = value.strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", value):
        raise HTTPException(422, "Código de barras no válido para el nombre de archivo.")
    return value


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def ai_enabled() -> bool:
    return flag("CATALOG_PHOTO_AI_ENABLED")


@lru_cache(maxsize=1)
def _ai_session():
    from rembg import new_session
    return new_session("u2netp")


def _remove_background(image: Image.Image) -> Image.Image:
    if not ai_enabled():
        raise HTTPException(503, "La eliminación automática de fondo aún no está habilitada.")
    try:
        from rembg import remove
        # One inference per worker; bound concurrent CPU/RAM use.
        if not _AI_LOCK.acquire(blocking=False):
            raise HTTPException(429, "Procesador de imágenes ocupado. Intenta nuevamente.")
        try:
            return remove(image, session=_ai_session()).convert("RGBA")
        finally:
            _AI_LOCK.release()
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(503, "No se pudo quitar el fondo. Prueba conservar el original.") from None


def decode_image(raw: bytes) -> Image.Image:
    if not raw or len(raw) > MAX_UPLOAD:
        raise HTTPException(413, "La imagen debe pesar menos de 15 MB.")
    try:
        import pillow_heif
        pillow_heif.register_heif_opener()
    except ImportError:
        pass
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(raw)) as source:
                if source.format not in {"JPEG", "PNG", "WEBP", "HEIF", "HEIC"}:
                    raise HTTPException(415, "Usa JPG, PNG, WebP o HEIC.")
                if source.width * source.height > MAX_PIXELS:
                    raise HTTPException(413, "La imagen excede 32 megapíxeles.")
                if getattr(source, "n_frames", 1) != 1:
                    raise HTTPException(415, "Selecciona una fotografía estática.")
                source.load()
                return ImageOps.exif_transpose(source).convert("RGBA")
    except HTTPException:
        raise
    except (UnidentifiedImageError, Image.DecompressionBombError, Image.DecompressionBombWarning, OSError, ValueError):
        raise HTTPException(415, "No se pudo leer la imagen. Comprueba el formato.") from None


def prepare_photo(raw: bytes, mode: str) -> bytes:
    if mode not in {"original", "white", "remove", "remove_white"}:
        raise HTTPException(422, "Modo de fondo no válido.")
    image = decode_image(raw)
    # Bound model input, preserve proportions; no invented product pixels.
    image.thumbnail((1600, 1600), Image.Resampling.LANCZOS)
    if mode.startswith("remove"):
        image = _remove_background(image)
        box = image.getchannel("A").getbbox()
        if not box:
            raise HTTPException(422, "No se detectó el producto. Conserva el fondo original.")
        image = image.crop(box)
    image.thumbnail((900, 900), Image.Resampling.LANCZOS)
    canvas = Image.new("RGBA", (SIZE, SIZE), (255, 255, 255, 255) if mode in {"white", "remove_white"} else (0, 0, 0, 0))
    canvas.alpha_composite(image, ((SIZE - image.width) // 2, (SIZE - image.height) // 2))
    for quality in (88, 80, 70, 55, 40):
        out = io.BytesIO()
        canvas.save(out, "WEBP", quality=quality, method=4)
        if out.tell() <= MAX_WEBP:
            return out.getvalue()
    raise HTTPException(422, "No se pudo optimizar la fotografía al tamaño permitido.")


def validate_prepared(raw: bytes) -> None:
    if not raw or len(raw) > MAX_WEBP:
        raise HTTPException(422, "La previsualización no es válida.")
    try:
        with Image.open(io.BytesIO(raw)) as img:
            if img.format != "WEBP" or img.size != (SIZE, SIZE) or getattr(img, "n_frames", 1) != 1:
                raise HTTPException(422, "Se requiere WebP de 1000 × 1000.")
            img.verify()
    except HTTPException:
        raise
    except (OSError, ValueError):
        raise HTTPException(422, "WebP no válido.") from None


def _headers() -> dict[str, str]:
    token = os.getenv("CATALOG_PHOTO_GITHUB_TOKEN", "").strip()
    headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _url(barcode: str) -> str:
    return f"https://api.github.com/repos/{REPO}/contents/{PREFIX}/{safe_barcode(barcode)}.webp"


def _request(method: str, barcode: str, **kwargs):
    try:
        return requests.request(method, _url(barcode), headers=_headers(),
                                timeout=(5, 30), allow_redirects=False, **kwargs)
    except requests.RequestException:
        raise HTTPException(503, "GitHub no respondió. Revisa el estado antes de volver a intentar.") from None


def _json(response) -> dict:
    try:
        value = response.json()
        if isinstance(value, dict):
            return value
    except ValueError:
        pass
    raise HTTPException(502, "Respuesta inesperada de GitHub.")


def photo_sha(barcode: str) -> str | None:
    response = _request("GET", barcode, params={"ref": BRANCH})
    if response.status_code == 404:
        # A missing token permission can also yield 404. Verify repo/branch independently.
        try:
            check = requests.get(f"https://api.github.com/repos/{REPO}/branches/{BRANCH}",
                                 headers=_headers(), timeout=(5, 15), allow_redirects=False)
        except requests.RequestException:
            raise HTTPException(503, "No se pudo comprobar la rama de catálogo.") from None
        if check.status_code == 200:
            return None
    if response.status_code != 200:
        raise HTTPException(503, "No se pudo verificar la fotografía en GitHub.")
    value = _json(response)
    sha = value.get("sha")
    if value.get("type") != "file" or not isinstance(sha, str) or not _SHA.fullmatch(sha):
        raise HTTPException(409, "La ruta de fotografía requiere revisión.")
    return sha


def _sign(payload: str) -> str:
    key = os.getenv("CATALOG_PHOTO_PREVIEW_SECRET", "")
    if len(key) < 32:
        raise HTTPException(503, "Confirmación de fotos aún no configurada.")
    return hmac.new(key.encode(), payload.encode(), hashlib.sha256).hexdigest()


def sign_preview(*, user_id: int, row_id: int, barcode: str, old_sha: str | None, raw: bytes) -> str:
    body = {"u": user_id, "r": row_id, "b": safe_barcode(barcode), "old": old_sha,
            "sha256": digest(raw), "exp": int(time.time()) + TTL}
    payload = base64.urlsafe_b64encode(json.dumps(body, separators=(",", ":")).encode()).decode()
    return payload + "." + _sign(payload)


def verify_preview(token: str, *, user_id: int, row_id: int, barcode: str, raw: bytes) -> str | None:
    try:
        payload, signature = token.split(".")
        if len(token) > 2048 or not hmac.compare_digest(signature, _sign(payload)):
            raise ValueError()
        body = json.loads(base64.urlsafe_b64decode(payload))
        if body["u"] != user_id or body["r"] != row_id or body["b"] != safe_barcode(barcode):
            raise ValueError()
        if body["sha256"] != digest(raw) or body["exp"] < int(time.time()):
            raise ValueError()
        old = body["old"]
        if old is not None and (not isinstance(old, str) or not _SHA.fullmatch(old)):
            raise ValueError()
        return old
    except (ValueError, KeyError, TypeError):
        raise HTTPException(409, "La previsualización expiró o cambió. Prepárala nuevamente.") from None


def publish_photo(*, barcode: str, raw: bytes, expected_sha: str | None, user_id: int, row_id: int) -> dict:
    if not writes_enabled():
        raise HTTPException(503, "La publicación de fotos está deshabilitada.")
    validate_prepared(raw)
    if photo_sha(barcode) != expected_sha:
        raise HTTPException(409, "Otra persona cambió la fotografía. Actualiza la previsualización.")
    # Audit is part of the durable Git commit itself; no token/email in commit.
    body = {"message": f"catalog photo {safe_barcode(barcode)} | ERP user={user_id} product={row_id} previous={expected_sha or 'none'} sha256={digest(raw)}",
            "content": base64.b64encode(raw).decode(), "branch": BRANCH}
    if expected_sha is not None:
        body["sha"] = expected_sha
    response = _request("PUT", barcode, json=body)
    if response.status_code in {409, 422}:
        raise HTTPException(409, "La fotografía cambió o GitHub rechazó la escritura. Actualiza el estado.")
    if response.status_code not in {200, 201}:
        raise HTTPException(503, "GitHub no confirmó la publicación. Comprueba el estado antes de reintentar.")
    result = _json(response)
    commit = (result.get("commit") or {}).get("sha")
    new_sha = (result.get("content") or {}).get("sha")
    if not isinstance(commit, str) or not _SHA.fullmatch(commit) or not isinstance(new_sha, str) or not _SHA.fullmatch(new_sha):
        raise HTTPException(502, "GitHub no devolvió una confirmación verificable. Revisa el estado.")
    return {"commit_sha": commit, "old_sha": expected_sha, "new_sha": new_sha,
            "commit_url": f"https://github.com/{REPO}/commit/{commit}",
            "published_to_repository": True, "catalog_verified": False}
