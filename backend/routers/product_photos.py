"""Private ERP management API. No mutation of Bsale or catalog database."""
from __future__ import annotations

import base64
import logging
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from backend.auth.permissions import require_management_access
from backend.db import get_connection
from backend.services import product_photo_service as photos
from backend.services import product_master_admin_service as admin

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/products-master/id", tags=["product-photos"])


def _actor(user: dict) -> int:
    return admin.actor_id(user)


def _writes_enabled() -> bool:
    return photos.writes_enabled() and admin.photo_audit_ready()


def _product(row_id: int) -> dict:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, barcode, variant_id, product_name, variant_name, sku FROM bsale.products_master WHERE id=%s",
                (row_id,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(404, "Producto no encontrado.")
            barcode = photos.safe_barcode(str(row[1] or ""))
            cur.execute("SELECT COUNT(*) FROM bsale.products_master WHERE BTRIM(barcode)=%s", (barcode,))
            master_count = int(cur.fetchone()[0])
            cur.execute(
                "SELECT DISTINCT bsale_id FROM bsale.variants WHERE company_id=3 AND BTRIM(bar_code)=%s ORDER BY bsale_id",
                (barcode,),
            )
            variants = [int(r[0]) for r in cur.fetchall()]
            cur.execute(
                "SELECT DISTINCT variant_id, image_url FROM bsale.catalog_view WHERE BTRIM(bar_code)=%s",
                (barcode,),
            )
            catalog_rows = cur.fetchall()
            catalog_ids = {int(r[0]) for r in catalog_rows if r[0] is not None}
            reason = None
            if master_count != 1 or len(variants) > 1 or len(catalog_ids) > 1:
                reason = "Código de barras compartido. Revisa las variantes antes de publicar."
            elif len(variants) != 1 or catalog_ids != set(variants):
                reason = "No se pudo verificar una variante única en el catálogo vigente."
            image_urls = sorted({str(r[1]) for r in catalog_rows if r[1]})
            return {"row_id": int(row[0]), "barcode": barcode, "variant_id": row[2],
                    "product_name": row[3], "variant_name": row[4], "sku": row[5],
                    "catalog_variant_ids": sorted(catalog_ids), "catalog_image_urls": image_urls,
                    "blocked_reason": reason, "filename": f"{barcode}.webp",
                    "catalog_asset_url": f"{photos.CDN}/{barcode}.webp"}
    finally:
        conn.close()


@router.get("/{row_id}/photo")
def get_photo(row_id: int, user: dict = Depends(require_management_access)):
    _actor(user)
    p = _product(row_id)
    sha = photos.photo_sha(p["barcode"])
    return {**p, "github_sha": sha, "has_photo": sha is not None,
            "writes_enabled": _writes_enabled(), "ai_enabled": photos.ai_enabled()}


@router.post("/{row_id}/photo/prepare")
def prepare(row_id: int, mode: str = Form("original"), image: UploadFile = File(...),
            user: dict = Depends(require_management_access)):
    actor = _actor(user)
    p = _product(row_id)
    if p["blocked_reason"]:
        raise HTTPException(409, p["blocked_reason"])
    try:
        raw = image.file.read(photos.MAX_UPLOAD + 1)
    finally:
        image.file.close()
    data = photos.prepare_photo(raw, mode)
    sha = photos.photo_sha(p["barcode"])
    enabled = _writes_enabled()
    return {"data_base64": base64.b64encode(data).decode(), "bytes": len(data),
            "width": photos.SIZE, "height": photos.SIZE, "format": "WEBP", "mode": mode,
            "filename": p["filename"], "expected_sha": sha, "writes_enabled": enabled,
            "preview_token": photos.sign_preview(user_id=actor, row_id=row_id, barcode=p["barcode"],
                                                 old_sha=sha, raw=data) if enabled else None}


@router.post("/{row_id}/photo/commit")
def commit(row_id: int, preview_token: str = Form(..., max_length=2048),
           image: UploadFile = File(...), user: dict = Depends(require_management_access)):
    actor = _actor(user)
    if not _writes_enabled():
        raise HTTPException(503, "La publicación de fotos está deshabilitada.")
    p = _product(row_id)
    if p["blocked_reason"]:
        raise HTTPException(409, p["blocked_reason"])
    try:
        raw = image.file.read(photos.MAX_WEBP + 1)
    finally:
        image.file.close()
    photos.validate_prepared(raw)
    expected = photos.verify_preview(preview_token, user_id=actor, row_id=row_id,
                                     barcode=p["barcode"], raw=raw)
    audit_id = admin.start_photo_audit(actor=actor, product=p, expected_sha=expected,
                                       prepared_sha256=photos.digest(raw))
    try:
        result = photos.publish_photo(barcode=p["barcode"], raw=raw, expected_sha=expected,
                                      audit_id=audit_id)
    except Exception as exc:
        try:
            admin.finish_photo_audit(audit_id, None, getattr(exc, "status_code", 500))
        except Exception:
            logger.error("catalog_photo_audit_finalize_pending audit=%s", audit_id)
        raise
    audit_pending = False
    try:
        admin.finish_photo_audit(audit_id, result)
    except Exception:
        # The GitHub commit exists. Never misreport it as a failed publication.
        audit_pending = True
        logger.error("catalog_photo_audit_finalize_pending audit=%s", audit_id)
    return {**result, "audit_id": audit_id, "audit_pending": audit_pending,
            "filename": p["filename"], "catalog_asset_url": p["catalog_asset_url"]}
