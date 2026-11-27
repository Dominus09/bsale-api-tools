"""Read-only GitHub inventory: one complete tree, no request per product."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import re
import threading
import time

from fastapi import HTTPException
import requests
from backend.services import product_photo_service as photos

CACHE_SECONDS = 300
ERROR_BACKOFF_SECONDS = 30
_FILE = re.compile(r"public/products/([A-Za-z0-9_-]{1,80})\.webp")
_LOCK = threading.Lock()


@dataclass(frozen=True)
class PhotoInventory:
    barcodes: frozenset[str]
    tree_sha: str
    checked_at: str

    def status(self) -> dict:
        return {"available": True, "checked_at": self.checked_at,
                "file_count": len(self.barcodes), "source": "github_webp",
                "tree_sha": self.tree_sha}


_cached: PhotoInventory | None = None
_valid_until = 0.0
_failed_until = 0.0


def parse_tree(payload: dict) -> PhotoInventory:
    if (payload.get("truncated") is not False
            or not isinstance(payload.get("tree"), list)
            or not isinstance(payload.get("sha"), str)
            or not re.fullmatch(r"[a-f0-9]{40}", payload["sha"])):
        raise HTTPException(503, "Inventario de fotografías incompleto. No se puede filtrar con seguridad.")
    entries = payload["tree"]
    if not all(isinstance(entry, dict) for entry in entries):
        raise HTTPException(503, "Inventario de fotografías no válido.")
    if not any(e.get("path") == photos.PREFIX and e.get("type") == "tree" for e in entries):
        raise HTTPException(503, "No se encontró la carpeta de fotografías del catálogo.")
    barcodes = set()
    for entry in entries:
        path = entry.get("path")
        match = _FILE.fullmatch(path) if isinstance(path, str) else None
        if match and entry.get("type") == "blob" and entry.get("mode") in {"100644", "100755"}:
            if not isinstance(entry.get("size"), int) or entry["size"] <= 0:
                raise HTTPException(503, "Inventario contiene un archivo de foto vacío o no verificable.")
            barcodes.add(match.group(1))
    return PhotoInventory(frozenset(barcodes), payload["sha"],
                          datetime.now(timezone.utc).isoformat())


def _fetch() -> PhotoInventory:
    try:
        response = requests.get(
            f"https://api.github.com/repos/{photos.REPO}/git/trees/{photos.BRANCH}",
            params={"recursive": "1"},
            headers=photos._headers(), timeout=(4, 12), allow_redirects=False,
        )
        if response.status_code != 200:
            raise HTTPException(503, "No se pudo comprobar el inventario de fotos en GitHub.")
        return parse_tree(photos._json(response))
    except requests.RequestException:
        raise HTTPException(503, "No se pudo conectar con GitHub para verificar las fotos.") from None


def get_inventory() -> PhotoInventory:
    global _cached, _valid_until, _failed_until
    with _LOCK:
        now = time.monotonic()
        if _cached is not None and now < _valid_until:
            return _cached
        if now < _failed_until:
            raise HTTPException(503, "Inventario de fotos temporalmente no disponible.")
        try:
            inventory = _fetch()
        except HTTPException:
            _failed_until = time.monotonic() + ERROR_BACKOFF_SECONDS
            raise
        _cached = inventory
        _valid_until = time.monotonic() + CACHE_SECONDS
        _failed_until = 0
        return inventory


def invalidate_inventory() -> None:
    global _cached, _valid_until, _failed_until
    with _LOCK:
        _cached = None
        _valid_until = 0
        _failed_until = 0
