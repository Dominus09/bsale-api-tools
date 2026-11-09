"""Almacenamiento de objetos privado (Cloudflare R2 / S3-compatible).

Deshabilitado si faltan variables. Sin secretos en el repo.
No escribe a disco permanente del contenedor.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from typing import Any


class ObjectStorageError(RuntimeError):
    pass


class ObjectStorageNotConfigured(ObjectStorageError):
    pass


@dataclass(frozen=True)
class ObjectStorageConfig:
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str
    region: str = "auto"
    public_base_url: str | None = None
    presign_ttl_sec: int = 600


def load_r2_config() -> ObjectStorageConfig | None:
    endpoint = (os.getenv("R2_ENDPOINT") or "").strip()
    access = (os.getenv("R2_ACCESS_KEY_ID") or "").strip()
    secret = (os.getenv("R2_SECRET_ACCESS_KEY") or "").strip()
    bucket = (os.getenv("R2_BUCKET") or "").strip()
    if not (endpoint and access and secret and bucket):
        return None
    ttl = int(os.getenv("R2_PRESIGN_TTL_SEC") or "600")
    region = (os.getenv("R2_REGION") or "auto").strip() or "auto"
    public = (os.getenv("R2_PUBLIC_BASE_URL") or "").strip() or None
    return ObjectStorageConfig(
        endpoint=endpoint.rstrip("/"),
        access_key=access,
        secret_key=secret,
        bucket=bucket,
        region=region,
        public_base_url=public,
        presign_ttl_sec=max(60, min(ttl, 3600)),
    )


def is_r2_configured() -> bool:
    return load_r2_config() is not None


def _client(cfg: ObjectStorageConfig):
    try:
        import boto3
        from botocore.config import Config
    except ImportError as exc:
        raise ObjectStorageNotConfigured(
            "boto3 no instalado; agregue boto3 al entorno del API."
        ) from exc
    return boto3.client(
        "s3",
        endpoint_url=cfg.endpoint,
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
        region_name=cfg.region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def build_facade_object_key(*, company_id: int, client_id: int, ext: str = "jpg") -> str:
    safe_ext = "".join(c for c in (ext or "jpg").lower() if c.isalnum())[:8] or "jpg"
    return (
        f"operaciones/clientes/{int(company_id)}/{int(client_id)}/"
        f"fachadas/{uuid.uuid4().hex}.{safe_ext}"
    )


def put_bytes(
    *,
    object_key: str,
    body: bytes,
    content_type: str,
) -> dict[str, Any]:
    cfg = load_r2_config()
    if not cfg:
        raise ObjectStorageNotConfigured(
            "Cloudflare R2 no configurado (faltan R2_ENDPOINT / R2_ACCESS_KEY_ID / "
            "R2_SECRET_ACCESS_KEY / R2_BUCKET)."
        )
    if not object_key or ".." in object_key or object_key.startswith("/"):
        raise ObjectStorageError("object_key inválido")
    client = _client(cfg)
    client.put_object(
        Bucket=cfg.bucket,
        Key=object_key,
        Body=body,
        ContentType=content_type or "application/octet-stream",
    )
    return {"bucket": cfg.bucket, "object_key": object_key, "size": len(body)}


def object_exists(object_key: str) -> bool:
    cfg = load_r2_config()
    if not cfg:
        raise ObjectStorageNotConfigured("R2 no configurado")
    client = _client(cfg)
    try:
        client.head_object(Bucket=cfg.bucket, Key=object_key)
        return True
    except Exception:
        return False


def presign_put(
    *,
    object_key: str,
    content_type: str,
    expires_in: int | None = None,
) -> dict[str, Any]:
    cfg = load_r2_config()
    if not cfg:
        raise ObjectStorageNotConfigured("R2 no configurado")
    client = _client(cfg)
    ttl = int(expires_in or cfg.presign_ttl_sec)
    url = client.generate_presigned_url(
        "put_object",
        Params={
            "Bucket": cfg.bucket,
            "Key": object_key,
            "ContentType": content_type or "image/jpeg",
        },
        ExpiresIn=ttl,
        HttpMethod="PUT",
    )
    return {
        "upload_url": url,
        "object_key": object_key,
        "content_type": content_type,
        "expires_in": ttl,
        "method": "PUT",
    }


def presign_get(*, object_key: str, expires_in: int | None = None) -> str:
    cfg = load_r2_config()
    if not cfg:
        raise ObjectStorageNotConfigured("R2 no configurado")
    client = _client(cfg)
    ttl = int(expires_in or cfg.presign_ttl_sec)
    return client.generate_presigned_url(
        "get_object",
        Params={"Bucket": cfg.bucket, "Key": object_key},
        ExpiresIn=ttl,
        HttpMethod="GET",
    )


def status_payload() -> dict[str, Any]:
    cfg = load_r2_config()
    return {
        "configured": bool(cfg),
        "bucket": cfg.bucket if cfg else None,
        "endpoint_configured": bool(cfg.endpoint) if cfg else False,
        "presign_ttl_sec": cfg.presign_ttl_sec if cfg else None,
        "note_es": (
            None
            if cfg
            else "R2 deshabilitado: defina R2_ENDPOINT, R2_ACCESS_KEY_ID, "
            "R2_SECRET_ACCESS_KEY y R2_BUCKET en Coolify."
        ),
    }
