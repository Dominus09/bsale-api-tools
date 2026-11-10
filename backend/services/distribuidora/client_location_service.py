"""P3: propuestas GPS y fotos de fachada (chofer) + revisión ERP."""

from __future__ import annotations

import hashlib
import math
import os
import re
from datetime import datetime, timezone
from typing import Any

from fastapi import HTTPException

from backend.db import get_connection
from backend.repositories.distribuidora import client_location_repo as loc_repo
from backend.repositories.distribuidora import dispatch_plan_load_batch_repo as events_repo
from backend.services import object_storage_service as storage
from backend.services.distribuidora import dispatch_plan_pwa_driver_read_service as read_svc
from backend.utils.json_safe import serialize_value

ALLOWED_IMAGE_TYPES = {
    "image/jpeg": "jpg",
    "image/jpg": "jpg",
    "image/png": "png",
    "image/webp": "webp",
}
MAX_PHOTO_BYTES = int(os.getenv("CLIENT_FACADE_MAX_BYTES") or str(4 * 1024 * 1024))
GPS_WARN_ACCURACY_M = float(os.getenv("CLIENT_GPS_WARN_ACCURACY_M") or "50")
# Umbral duro alto: en rural la precisión del navegador suele ser 80–400 m.
# Por encima solo se bloquea; entre WARN y HARD se exige confirmación explícita.
GPS_HARD_ACCURACY_M = float(os.getenv("CLIENT_GPS_HARD_ACCURACY_M") or "500")
SIGNIFICANT_MOVE_M = float(os.getenv("CLIENT_GPS_SIGNIFICANT_MOVE_M") or "80")


def _detect_image_mime(data: bytes) -> str | None:
    if len(data) >= 3 and data[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if len(data) >= 8 and data[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    return None


def _normalize_facade_image(data: bytes, claimed_mime: str | None) -> tuple[bytes, str, dict[str, Any]]:
    """Valida magia real, corrige orientación y elimina EXIF al re-encodear JPEG."""
    if not data:
        raise HTTPException(status_code=400, detail="Archivo vacío")
    if len(data) > MAX_PHOTO_BYTES:
        raise HTTPException(
            status_code=400,
            detail=f"Imagen demasiado grande (máx {MAX_PHOTO_BYTES // (1024 * 1024)} MB)",
        )
    detected = _detect_image_mime(data)
    if not detected:
        raise HTTPException(
            status_code=400,
            detail="El archivo no es una imagen JPEG/PNG/WebP válida",
        )
    claimed = (claimed_mime or "").lower().strip()
    if claimed in ALLOWED_IMAGE_TYPES and claimed not in ("image/jpg", detected) and claimed != detected:
        # Permitir image/jpg vs image/jpeg; rechazar PNG declarado como JPEG, etc.
        if not (claimed in ("image/jpeg", "image/jpg") and detected == "image/jpeg"):
            raise HTTPException(
                status_code=400,
                detail=f"Content-Type declarado ({claimed}) no coincide con el contenido ({detected})",
            )
    meta: dict[str, Any] = {"detected_mime": detected, "exif_stripped": False}
    try:
        from io import BytesIO

        from PIL import Image, ImageOps
    except ImportError:
        return data, detected, meta

    try:
        img = Image.open(BytesIO(data))
        img = ImageOps.exif_transpose(img)
        width, height = img.size
        meta["width"] = int(width)
        meta["height"] = int(height)
        # Re-encode JPEG operativo: legible y sin metadatos EXIF/GPS.
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        elif img.mode == "L":
            img = img.convert("RGB")
        max_side = 1600
        w, h = img.size
        scale = min(1.0, max_side / float(max(w, h)))
        if scale < 1.0:
            img = img.resize(
                (max(1, int(w * scale)), max(1, int(h * scale))),
                Image.Resampling.LANCZOS,
            )
        out = BytesIO()
        img.save(out, format="JPEG", quality=82, optimize=True)
        normalized = out.getvalue()
        if len(normalized) > MAX_PHOTO_BYTES:
            raise HTTPException(status_code=400, detail="Imagen demasiado grande tras normalizar")
        meta["exif_stripped"] = True
        meta["normalized_mime"] = "image/jpeg"
        meta["width"], meta["height"] = img.size
        return normalized, "image/jpeg", meta
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"No se pudo procesar la imagen: {exc}",
        ) from exc


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _parse_client_id(customer_key: str, client: dict[str, Any]) -> int:
    cid = client.get("client_id")
    if cid is not None:
        try:
            return int(cid)
        except (TypeError, ValueError):
            pass
    m = re.match(r"^id:(\d+)$", (customer_key or "").strip())
    if m:
        return int(m.group(1))
    raise HTTPException(
        status_code=422,
        detail="Cliente sin id Bsale estable; no se puede registrar ubicación/foto.",
    )


def _resolve_client(plan_id: int, customer_key: str) -> dict[str, Any]:
    bundle = read_svc._build_client_index(int(plan_id))  # noqa: SLF001 — index plan-scoped
    client = next(
        (c for c in bundle["clients"] if c.get("customer_key") == customer_key.strip()),
        None,
    )
    if not client:
        raise HTTPException(status_code=404, detail="Cliente no pertenece a este despacho")
    return client


def _staff_label(user: dict[str, Any] | None) -> str | None:
    if not user:
        return None
    return str(user.get("email") or user.get("name") or user.get("sub") or "").strip() or None


def schema_status() -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        ready = loc_repo.tables_ready(cur)
        cur.close()
    finally:
        conn.close()
    return {
        "tables_ready": ready,
        "migration": "055_client_location_photos.sql",
        "r2": storage.status_payload(),
        "gps_warn_accuracy_m": GPS_WARN_ACCURACY_M,
        "gps_hard_accuracy_m": GPS_HARD_ACCURACY_M,
        "significant_move_m": SIGNIFICANT_MOVE_M,
        "max_photo_bytes": MAX_PHOTO_BYTES,
    }


def enrich_client_location_fields(client: dict[str, Any]) -> dict[str, Any]:
    """Añade estado de ubicación verificada / pendiente a un dict de ficha."""
    out = dict(client)
    cid = out.get("client_id")
    if cid is None:
        out["location_registration"] = {
            "status": "unavailable",
            "label_es": "Sin id Bsale",
            "can_register": False,
        }
        out["facade"] = {"has_photo": False, "url": None, "status": None}
        return out

    conn = get_connection()
    try:
        cur = conn.cursor()
        if not loc_repo.tables_ready(cur):
            out["location_registration"] = {
                "status": "schema_pending",
                "label_es": "Migración 055 pendiente",
                "can_register": False,
            }
            out["facade"] = {"has_photo": False, "url": None, "status": None}
            cur.close()
            return out

        verified = loc_repo.get_verified_location(cur, int(cid))
        pending = loc_repo.get_pending_proposal(cur, int(cid))
        facade = loc_repo.get_current_facade(cur, int(cid))
        cur.close()
    finally:
        conn.close()

    if verified:
        out["latitude"] = float(verified["latitude"])
        out["longitude"] = float(verified["longitude"])
        out["has_coordinates"] = True
        out["coordinates_source"] = "client_verified_locations"
        status = "verified"
        label = "Ubicación verificada"
    elif pending:
        status = "pending_review"
        label = "Corrección pendiente de aprobación"
        pend_acc = pending.get("accuracy_m")
        if pend_acc is not None and float(pend_acc) > GPS_WARN_ACCURACY_M:
            label = (
                f"Corrección pendiente (GPS ±{float(pend_acc):.0f} m, baja precisión)"
            )
    elif out.get("has_coordinates"):
        status = "unverified_existing"
        label = "Coordenadas existentes (sin verificar por chofer)"
    else:
        status = "missing"
        label = "Sin registrar"

    pend_acc_out = (
        float(pending["accuracy_m"])
        if pending and pending.get("accuracy_m") is not None
        else None
    )
    out["location_registration"] = {
        "status": status,
        "label_es": label,
        "can_register": True,
        "pending_proposal_id": int(pending["id"]) if pending else None,
        "verified": bool(verified),
        "low_accuracy": bool(
            pend_acc_out is not None and pend_acc_out > GPS_WARN_ACCURACY_M
        ),
        "accuracy_m": pend_acc_out,
        "previous_lat": float(pending["previous_lat"]) if pending and pending.get("previous_lat") is not None else out.get("latitude"),
        "previous_lng": float(pending["previous_lng"]) if pending and pending.get("previous_lng") is not None else out.get("longitude"),
        "proposed_lat": float(pending["proposed_lat"]) if pending else None,
        "proposed_lng": float(pending["proposed_lng"]) if pending else None,
    }

    facade_url = None
    if facade and facade.get("upload_confirmed") and facade.get("object_key"):
        try:
            facade_url = storage.presign_get(object_key=str(facade["object_key"]))
        except storage.ObjectStorageNotConfigured:
            facade_url = None
        except Exception:
            facade_url = None
    out["facade"] = {
        "has_photo": bool(facade and facade.get("upload_confirmed")),
        "photo_id": int(facade["id"]) if facade else None,
        "status": facade.get("validation_status") if facade else None,
        "url": facade_url,
        "is_current_approved": bool(
            facade and facade.get("is_current") and facade.get("validation_status") == "approved"
        ),
    }
    out["facade_photo"] = facade_url
    out["facade_photo_placeholder"] = not bool(facade_url)
    return out


def propose_location(
    *,
    user: dict[str, Any],
    plan_id: int,
    customer_key: str,
    latitude: float,
    longitude: float,
    accuracy_m: float | None = None,
    captured_at: str | None = None,
    confirm_low_accuracy: bool = False,
    confirm_significant_move: bool = False,
    facade_photo_id: int | None = None,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    read_svc.enforce_driver_plan_read_access(user, int(plan_id))
    client = _resolve_client(int(plan_id), customer_key)
    client_id = _parse_client_id(customer_key, client)
    uid = int(user.get("id") or user.get("uid"))

    lat = float(latitude)
    lng = float(longitude)
    if not (-90 <= lat <= 90 and -180 <= lng <= 180) or (lat == 0 and lng == 0):
        raise HTTPException(status_code=400, detail="Coordenadas inválidas")

    acc = float(accuracy_m) if accuracy_m is not None else None
    warnings: list[str] = []
    if acc is not None and acc > GPS_HARD_ACCURACY_M:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Precisión GPS insuficiente ({acc:.0f} m). "
                f"Reintente al aire libre (límite {GPS_HARD_ACCURACY_M:.0f} m)."
            ),
        )
    if acc is not None and acc > GPS_WARN_ACCURACY_M and not confirm_low_accuracy:
        return serialize_value(
            {
                "requires_confirmation": True,
                "reason": "low_accuracy",
                "accuracy_m": acc,
                "warn_threshold_m": GPS_WARN_ACCURACY_M,
                "message_es": (
                    f"La precisión estimada es {acc:.0f} m (aviso sobre "
                    f"{GPS_WARN_ACCURACY_M:.0f} m). Puede reintentar o confirmar igual."
                ),
                "proposed": {"latitude": lat, "longitude": lng},
            }
        )

    prev_lat = float(client["latitude"]) if client.get("latitude") is not None else None
    prev_lng = float(client["longitude"]) if client.get("longitude") is not None else None
    prev_source = client.get("coordinates_source")
    distance_m = None
    if prev_lat is not None and prev_lng is not None:
        distance_m = _haversine_m(prev_lat, prev_lng, lat, lng)
        if distance_m >= SIGNIFICANT_MOVE_M and not confirm_significant_move:
            return serialize_value(
                {
                    "requires_confirmation": True,
                    "reason": "significant_move",
                    "distance_m": round(distance_m, 1),
                    "threshold_m": SIGNIFICANT_MOVE_M,
                    "previous": {"latitude": prev_lat, "longitude": prev_lng, "source": prev_source},
                    "proposed": {"latitude": lat, "longitude": lng, "accuracy_m": acc},
                    "message_es": (
                        f"La nueva ubicación está a {distance_m:.0f} m de la anterior. "
                        "Confirme si desea proponer la corrección."
                    ),
                }
            )

    captured_dt = None
    if captured_at:
        try:
            captured_dt = datetime.fromisoformat(captured_at.replace("Z", "+00:00"))
        except ValueError:
            captured_dt = None

    conn = get_connection()
    try:
        cur = conn.cursor()
        if not loc_repo.tables_ready(cur):
            raise HTTPException(
                status_code=503,
                detail="Migración 055 pendiente: no se puede registrar ubicación.",
            )
        if idempotency_key:
            existing = loc_repo.find_proposal_by_idem(
                cur, user_id=uid, idempotency_key=idempotency_key.strip()
            )
            if existing:
                conn.rollback()
                cur.close()
                return serialize_value(
                    {
                        "idempotent": True,
                        "proposal": existing,
                        "message_es": "Propuesta ya registrada (idempotente).",
                    }
                )

        loc_repo.supersede_pending(cur, client_id)
        proposal = loc_repo.insert_proposal(
            cur,
            client_id=client_id,
            company_id=3,
            dispatch_plan_id=int(plan_id),
            customer_key=customer_key.strip(),
            proposed_lat=lat,
            proposed_lng=lng,
            accuracy_m=acc,
            captured_at=captured_dt,
            previous_lat=prev_lat,
            previous_lng=prev_lng,
            previous_source=prev_source,
            distance_from_previous_m=distance_m,
            proposed_by_user_id=uid,
            facade_photo_id=int(facade_photo_id) if facade_photo_id else None,
            idempotency_key=(idempotency_key or "").strip() or None,
        )
        loc_repo.insert_audit(
            cur,
            client_id=client_id,
            entity_type="location_proposal",
            entity_id=int(proposal["id"]),
            action="proposed",
            actor_user_id=uid,
            dispatch_plan_id=int(plan_id),
            payload={
                "proposed_lat": lat,
                "proposed_lng": lng,
                "accuracy_m": acc,
                "low_accuracy": bool(acc is not None and acc > GPS_WARN_ACCURACY_M),
                "previous_lat": prev_lat,
                "previous_lng": prev_lng,
                "distance_m": distance_m,
                "customer_key": customer_key,
            },
        )
        events_repo.insert_order_event(
            cur,
            plan_id=int(plan_id),
            action="client_gps_proposed",
            user_name=_staff_label(user),
            reason="Propuesta GPS chofer",
            payload={
                "client_id": client_id,
                "customer_key": customer_key,
                "proposal_id": int(proposal["id"]),
                "proposed_lat": lat,
                "proposed_lng": lng,
            },
        )
        if acc is not None and acc > GPS_WARN_ACCURACY_M:
            warnings.append(f"Precisión {acc:.0f} m (confirmada por el chofer).")
        conn.commit()
        cur.close()
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return serialize_value(
        {
            "idempotent": False,
            "proposal": proposal,
            "warnings_es": warnings,
            "message_es": "Ubicación propuesta. Queda pendiente de aprobación en ERP.",
        }
    )


def prepare_facade_upload(
    *,
    user: dict[str, Any],
    plan_id: int,
    customer_key: str,
    content_type: str,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    read_svc.enforce_driver_plan_read_access(user, int(plan_id))
    if not storage.is_r2_configured():
        raise HTTPException(
            status_code=503,
            detail="Almacenamiento R2 no configurado. No se pueden subir fotos todavía.",
        )
    client = _resolve_client(int(plan_id), customer_key)
    client_id = _parse_client_id(customer_key, client)
    uid = int(user.get("id") or user.get("uid"))
    mime = (content_type or "").lower().strip()
    if mime not in ALLOWED_IMAGE_TYPES:
        raise HTTPException(status_code=400, detail="Tipo de imagen no permitido")
    ext = ALLOWED_IMAGE_TYPES[mime]

    conn = get_connection()
    try:
        cur = conn.cursor()
        if not loc_repo.tables_ready(cur):
            raise HTTPException(status_code=503, detail="Migración 055 pendiente")
        if idempotency_key:
            existing = loc_repo.find_facade_by_idem(
                cur, user_id=uid, idempotency_key=idempotency_key.strip()
            )
            if existing:
                conn.rollback()
                cur.close()
                return serialize_value(
                    {
                        "idempotent": True,
                        "photo": existing,
                        "r2": storage.status_payload(),
                    }
                )
        object_key = storage.build_facade_object_key(
            company_id=3, client_id=client_id, ext=ext
        )
        photo = loc_repo.insert_facade_photo(
            cur,
            client_id=client_id,
            company_id=3,
            dispatch_plan_id=int(plan_id),
            customer_key=customer_key.strip(),
            object_key=object_key,
            content_type=mime,
            size_bytes=None,
            sha256=None,
            width=None,
            height=None,
            captured_by_user_id=uid,
            validation_status="upload_pending",
            upload_confirmed=False,
            idempotency_key=(idempotency_key or "").strip() or None,
        )
        presign = storage.presign_put(object_key=object_key, content_type=mime)
        loc_repo.insert_audit(
            cur,
            client_id=client_id,
            entity_type="facade_photo",
            entity_id=int(photo["id"]),
            action="upload_prepared",
            actor_user_id=uid,
            dispatch_plan_id=int(plan_id),
            payload={"object_key": object_key, "content_type": mime},
        )
        conn.commit()
        cur.close()
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return serialize_value(
        {
            "idempotent": False,
            "photo_id": int(photo["id"]),
            "object_key": object_key,
            "upload": presign,
            "max_bytes": MAX_PHOTO_BYTES,
            "message_es": "Suba la imagen con PUT a la URL firmada y luego confirme.",
        }
    )


def upload_facade_bytes(
    *,
    user: dict[str, Any],
    plan_id: int,
    customer_key: str,
    content_type: str,
    data: bytes,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    """Carga vía API (útil si CORS R2 aún no está listo)."""
    read_svc.enforce_driver_plan_read_access(user, int(plan_id))
    if not storage.is_r2_configured():
        raise HTTPException(
            status_code=503,
            detail="Almacenamiento R2 no configurado. No se pueden subir fotos todavía.",
        )
    claimed = (content_type or "").lower().strip() or "image/jpeg"
    if claimed == "application/octet-stream":
        claimed = "image/jpeg"
    if claimed not in ALLOWED_IMAGE_TYPES and claimed != "application/octet-stream":
        raise HTTPException(status_code=400, detail="Tipo de imagen no permitido")
    data, mime, img_meta = _normalize_facade_image(data, claimed)
    client = _resolve_client(int(plan_id), customer_key)
    client_id = _parse_client_id(customer_key, client)
    uid = int(user.get("id") or user.get("uid"))
    ext = ALLOWED_IMAGE_TYPES[mime]
    digest = hashlib.sha256(data).hexdigest()

    conn = get_connection()
    try:
        cur = conn.cursor()
        if not loc_repo.tables_ready(cur):
            raise HTTPException(status_code=503, detail="Migración 055 pendiente")
        if idempotency_key:
            existing = loc_repo.find_facade_by_idem(
                cur, user_id=uid, idempotency_key=idempotency_key.strip()
            )
            if existing and existing.get("upload_confirmed"):
                conn.rollback()
                cur.close()
                return serialize_value(
                    {"idempotent": True, "photo": existing, "message_es": "Foto ya registrada."}
                )
        object_key = storage.build_facade_object_key(
            company_id=3, client_id=client_id, ext=ext
        )
        try:
            storage.put_bytes(object_key=object_key, body=data, content_type=mime)
        except storage.ObjectStorageError as exc:
            raise HTTPException(status_code=502, detail=f"Error al subir a R2: {exc}") from exc
        if not storage.object_exists(object_key):
            # No insertar metadatos de un objeto inexistente.
            raise HTTPException(status_code=502, detail="No se confirmó el objeto en R2")
        photo = loc_repo.insert_facade_photo(
            cur,
            client_id=client_id,
            company_id=3,
            dispatch_plan_id=int(plan_id),
            customer_key=customer_key.strip(),
            object_key=object_key,
            content_type=mime,
            size_bytes=len(data),
            sha256=digest,
            width=img_meta.get("width"),
            height=img_meta.get("height"),
            captured_by_user_id=uid,
            validation_status="pending",
            upload_confirmed=True,
            idempotency_key=(idempotency_key or "").strip() or None,
        )
        loc_repo.insert_audit(
            cur,
            client_id=client_id,
            entity_type="facade_photo",
            entity_id=int(photo["id"]),
            action="uploaded",
            actor_user_id=uid,
            dispatch_plan_id=int(plan_id),
            payload={
                "object_key": object_key,
                "size_bytes": len(data),
                "sha256": digest,
                "exif_stripped": bool(img_meta.get("exif_stripped")),
            },
        )
        events_repo.insert_order_event(
            cur,
            plan_id=int(plan_id),
            action="client_facade_photo_uploaded",
            user_name=_staff_label(user),
            reason="Foto fachada chofer",
            payload={
                "client_id": client_id,
                "customer_key": customer_key,
                "photo_id": int(photo["id"]),
            },
        )
        conn.commit()
        cur.close()
    except HTTPException:
        conn.rollback()
        raise
    except storage.ObjectStorageNotConfigured as exc:
        conn.rollback()
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return serialize_value(
        {
            "idempotent": False,
            "photo": photo,
            "message_es": "Fotografía enviada. Queda pendiente de validación en ERP.",
        }
    )


def confirm_facade_upload(
    *,
    user: dict[str, Any],
    plan_id: int,
    customer_key: str,
    photo_id: int,
) -> dict[str, Any]:
    read_svc.enforce_driver_plan_read_access(user, int(plan_id))
    client = _resolve_client(int(plan_id), customer_key)
    client_id = _parse_client_id(customer_key, client)
    uid = int(user.get("id") or user.get("uid"))

    conn = get_connection()
    try:
        cur = conn.cursor()
        photo = loc_repo.get_facade_photo(cur, int(photo_id))
        if not photo or int(photo["client_id"]) != client_id:
            raise HTTPException(status_code=404, detail="Foto no encontrada")
        if int(photo["captured_by_user_id"]) != uid:
            raise HTTPException(status_code=403, detail="Foto de otro usuario")
        if int(photo["dispatch_plan_id"]) != int(plan_id):
            raise HTTPException(status_code=403, detail="Foto de otro despacho")
        if not storage.object_exists(str(photo["object_key"])):
            raise HTTPException(
                status_code=400,
                detail="El archivo aún no existe en R2. Complete la subida y reintente.",
            )
        updated = loc_repo.confirm_facade_upload(cur, int(photo_id))
        loc_repo.insert_audit(
            cur,
            client_id=client_id,
            entity_type="facade_photo",
            entity_id=int(photo_id),
            action="upload_confirmed",
            actor_user_id=uid,
            dispatch_plan_id=int(plan_id),
            payload={"object_key": photo["object_key"]},
        )
        conn.commit()
        cur.close()
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return serialize_value(
        {
            "photo": updated,
            "message_es": "Subida confirmada. Pendiente de validación administrativa.",
        }
    )


def list_pending_for_erp(*, limit: int = 100) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        if not loc_repo.tables_ready(cur):
            cur.close()
            return serialize_value(
                {
                    "items": [],
                    "tables_ready": False,
                    "message_es": "Migración 055 pendiente",
                }
            )
        items = loc_repo.list_pending_proposals(cur, limit=limit)
        for it in items:
            if it.get("facade_object_key"):
                try:
                    it["facade_url"] = storage.presign_get(
                        object_key=str(it["facade_object_key"])
                    )
                except Exception:
                    it["facade_url"] = None
            else:
                it["facade_url"] = None
        cur.close()
    finally:
        conn.close()
    return serialize_value({"items": items, "tables_ready": True, "count": len(items)})


def review_proposal_erp(
    *,
    user: dict[str, Any],
    proposal_id: int,
    approve: bool,
    note: str | None = None,
) -> dict[str, Any]:
    uid = int(user.get("id") or user.get("uid"))
    conn = get_connection()
    try:
        cur = conn.cursor()
        if not loc_repo.tables_ready(cur):
            raise HTTPException(status_code=503, detail="Migración 055 pendiente")
        prop = loc_repo.get_proposal(cur, int(proposal_id))
        if not prop:
            raise HTTPException(status_code=404, detail="Propuesta no encontrada")
        if prop.get("status") != "pending":
            raise HTTPException(status_code=409, detail="La propuesta ya fue revisada")
        if int(prop["proposed_by_user_id"]) == uid:
            # Bloqueo duro: no aprobar, no rechazar, no mutar estado.
            loc_repo.insert_audit(
                cur,
                client_id=int(prop["client_id"]),
                entity_type="location_proposal",
                entity_id=int(prop["id"]),
                action="self_review_denied",
                actor_user_id=uid,
                dispatch_plan_id=int(prop["dispatch_plan_id"]),
                payload={
                    "attempted_approve": bool(approve),
                    "status_before": prop.get("status"),
                    "status_unchanged": True,
                },
            )
            conn.commit()
            raise HTTPException(
                status_code=403,
                detail="No puede aprobar ni rechazar su propia propuesta",
            )
        status = "approved" if approve else "rejected"
        updated = loc_repo.review_proposal(
            cur,
            int(proposal_id),
            status=status,
            reviewer_user_id=uid,
            note=note,
        )
        if not updated:
            raise HTTPException(status_code=409, detail="Conflicto de revisión concurrente")
        if approve:
            loc_repo.upsert_verified_location(
                cur,
                client_id=int(updated["client_id"]),
                company_id=int(updated.get("company_id") or 3),
                latitude=float(updated["proposed_lat"]),
                longitude=float(updated["proposed_lng"]),
                accuracy_m=updated.get("accuracy_m"),
                source="driver_approved",
                approved_proposal_id=int(updated["id"]),
                approved_by_user_id=uid,
            )
            if updated.get("facade_photo_id"):
                loc_repo.review_facade(
                    cur,
                    int(updated["facade_photo_id"]),
                    status="approved",
                    reviewer_user_id=uid,
                    note=note,
                    make_current=True,
                )
        loc_repo.insert_audit(
            cur,
            client_id=int(updated["client_id"]),
            entity_type="location_proposal",
            entity_id=int(updated["id"]),
            action=status,
            actor_user_id=uid,
            dispatch_plan_id=int(updated["dispatch_plan_id"]),
            payload={"note": note, "approve": approve},
        )
        events_repo.insert_order_event(
            cur,
            plan_id=int(updated["dispatch_plan_id"]),
            action=f"client_gps_{status}",
            user_name=_staff_label(user),
            reason=note,
            payload={
                "proposal_id": int(updated["id"]),
                "client_id": int(updated["client_id"]),
            },
        )
        conn.commit()
        cur.close()
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return serialize_value(
        {
            "proposal": updated,
            "message_es": (
                "Ubicación aprobada y publicada como fuente verificada."
                if approve
                else "Propuesta rechazada."
            ),
        }
    )


def history_for_client(client_id: int, *, limit: int = 50) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        if not loc_repo.tables_ready(cur):
            cur.close()
            return serialize_value({"items": [], "tables_ready": False})
        items = loc_repo.list_history(cur, int(client_id), limit=limit)
        verified = loc_repo.get_verified_location(cur, int(client_id))
        pending = loc_repo.get_pending_proposal(cur, int(client_id))
        cur.close()
    finally:
        conn.close()
    return serialize_value(
        {
            "tables_ready": True,
            "verified": verified,
            "pending": pending,
            "items": items,
        }
    )
