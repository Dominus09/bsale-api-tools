"""Validated image processing and a narrowly scoped GitHub Contents client.

No persistent preview files, no client-selected repository or path, no write on preview.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import io
import json
import math
import os
import re
import threading
import time
import warnings
from functools import lru_cache
from urllib.parse import urljoin, urlsplit

import requests
from PIL import Image, ImageOps, UnidentifiedImageError
from fastapi import HTTPException
from backend.services import system_feature_controls as controls

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
_PLACEHOLDER = re.compile(r"(?:icon\.svg|logo-seal\.png|placeholder(?:2)?\.(?:png|webp)|(?:sin[-_]?foto|no[-_]?image|no[-_]?foto|default|generico|imagen[-_]?default|sin[-_]?imagen)\.(?:png|jpg|jpeg|webp|svg))", re.IGNORECASE)


def writes_enabled() -> bool:
    return controls.enabled("catalog_photo_writes") and controls.photo_credentials_ready()


def safe_barcode(value: str) -> str:
    value = value.strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", value):
        raise HTTPException(422, "Código de barras no válido para el nombre de archivo.")
    return value


def catalog_url_is_compatible(raw: str | None, barcode: str) -> bool:
    """Same policy as catalog static fallback: do not overwrite unrelated API images."""
    value = (raw or "").strip()
    if not value:
        return True
    try:
        parsed = urlsplit(urljoin("https://cat.quillotana.cl/", value))
        same = parsed.scheme == "https" and parsed.netloc == "cat.quillotana.cl" and parsed.path == f"/products/{safe_barcode(barcode)}.webp"
        return bool(same or _PLACEHOLDER.fullmatch(parsed.path.rsplit("/", 1)[-1]))
    except ValueError:
        return False


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def ai_enabled() -> bool:
    return controls.enabled("catalog_photo_ai") and controls.ai_model_ready()


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


def source_preview(raw: bytes) -> bytes:
    """Display the entire EXIF-oriented image so crop coordinates match server pixels."""
    image = decode_image(raw)
    image.thumbnail((1200, 1200), Image.Resampling.LANCZOS)
    for quality in (85, 70, 50, 35):
        out = io.BytesIO()
        image.save(out, "WEBP", quality=quality, method=4)
        if out.tell() <= MAX_WEBP:
            return out.getvalue()
    raise HTTPException(422, "No se pudo preparar la imagen original para recortarla.")


def _crop_image(image: Image.Image, crop: str | None) -> Image.Image:
    if crop is None:
        return image
    try:
        if len(crop) > 512:
            raise ValueError()
        rect = json.loads(crop)
        keys = ("left", "top", "right", "bottom")
        if not isinstance(rect, dict) or set(rect) != set(keys):
            raise ValueError()
        vals = [rect[k] for k in keys]
        if any(type(v) not in (int, float) or not math.isfinite(v) for v in vals):
            raise ValueError()
        left, top, right, bottom = vals
        if not (0 <= left < right <= 1 and 0 <= top < bottom <= 1):
            raise ValueError()
        if right - left < 0.019999 or bottom - top < 0.019999:
            raise ValueError()
        # Round inward so pixels excluded by the rectangle are never brought back.
        box = (math.ceil(left * image.width), math.ceil(top * image.height),
               math.floor(right * image.width), math.floor(bottom * image.height))
        if box[2] <= box[0] or box[3] <= box[1]:
            raise ValueError()
        return image.crop(box)
    except (ValueError, TypeError, OverflowError):
        raise HTTPException(422, "Recorte no válido. Selecciona un área de al menos 2% de ancho y alto.") from None


def prepare_photo(raw: bytes, mode: str, crop: str | None = None) -> bytes:
    if mode not in {"original", "white", "remove", "remove_white"}:
        raise HTTPException(422, "Modo de fondo no válido.")
    image = _crop_image(decode_image(raw), crop)
    # Crop at original resolution before segmentation, preserve proportions.
    image.thumbnail((1600, 1600), Image.Resampling.LANCZOS)
    if mode.startswith("remove"):
        image = _remove_background(image)
        box = image.getchannel("A").getbbox()
        if not box:
            raise HTTPException(422, "No se detectó el producto. Conserva el fondo original.")
        image = image.crop(box)
    # thumbnail() never enlarges a small source, which previously left tiny products.
    # Fit the selected image to 90% of the canvas even when the source is small.
    scale = 900 / max(image.size)
    image = image.resize(tuple(max(1, min(900, round(n * scale))) for n in image.size), Image.Resampling.LANCZOS)
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
    except (OSError, ValueError, SyntaxError):
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


def sign_preview(*, user_id: int, row_id: int, barcode: str, variant_id: int, old_sha: str | None, raw: bytes) -> str:
    body = {"u": user_id, "r": row_id, "b": safe_barcode(barcode), "v": variant_id, "old": old_sha,
            "sha256": digest(raw), "exp": int(time.time()) + TTL}
    payload = base64.urlsafe_b64encode(json.dumps(body, separators=(",", ":")).encode()).decode()
    return payload + "." + _sign(payload)


def verify_preview(token: str, *, user_id: int, row_id: int, barcode: str, variant_id: int, raw: bytes) -> str | None:
    try:
        payload, signature = token.split(".")
        if len(token) > 2048 or not hmac.compare_digest(signature, _sign(payload)):
            raise ValueError()
        body = json.loads(base64.urlsafe_b64decode(payload))
        if (body["u"] != user_id or body["r"] != row_id or body["b"] != safe_barcode(barcode)
                or body["v"] != variant_id):
            raise ValueError()
        if body["sha256"] != digest(raw) or body["exp"] < int(time.time()):
            raise ValueError()
        old = body["old"]
        if old is not None and (not isinstance(old, str) or not _SHA.fullmatch(old)):
            raise ValueError()
        return old
    except (ValueError, KeyError, TypeError, UnicodeError):
        raise HTTPException(409, "La previsualización expiró o cambió. Prepárala nuevamente.") from None


def publish_photo(*, barcode: str, raw: bytes, expected_sha: str | None, audit_id: str) -> dict:
    if not writes_enabled():
        raise HTTPException(503, "La publicación de fotos está deshabilitada.")
    validate_prepared(raw)
    if photo_sha(barcode) != expected_sha:
        raise HTTPException(409, "Otra persona cambió la fotografía. Actualiza la previsualización.")
    # Only an opaque reference is public; the ERP retains the actual actor and product.
    if not re.fullmatch(r"[0-9a-f]{32}", audit_id):
        raise HTTPException(500, "Referencia de auditoría no válida.")
    blob_sha = hashlib.sha1(f"blob {len(raw)}\0".encode() + raw).hexdigest()
    if blob_sha == expected_sha:
        raise HTTPException(409, "La fotografía es idéntica a la existente.")
    body = {"message": f"catalog photo {safe_barcode(barcode)} | audit={audit_id}",
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
    if new_sha != blob_sha:
        raise HTTPException(502, "El contenido confirmado no coincide con la previsualización. Revisa el estado.")
    return {"commit_sha": commit, "old_sha": expected_sha, "new_sha": new_sha,
            "commit_url": f"https://github.com/{REPO}/commit/{commit}",
            "published_to_repository": True, "catalog_verified": False}
