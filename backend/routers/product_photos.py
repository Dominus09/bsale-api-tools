"""Private ERP management API. No mutation of Bsale or catalog database."""
from __future__ import annotations

import base64
import logging
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from backend.auth.permissions import require_management_access
from backend.db import get_connection
from backend.services import product_photo_service as photos
from backend.services import catalog_photo_inventory
from backend.services import product_master_admin_service as admin
from backend.services.catalog_v2.source_flag import resolve_catalog_source

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
                "SELECT id, barcode, variant_id, product_name, variant_name, sku, is_active FROM bsale.products_master WHERE id=%s",
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
            if not row[6]:
                reason = "El producto está inactivo. No se permite publicar fotografías."
            elif resolve_catalog_source(consumer="public_web") != "legacy":
                reason = "El catálogo público usa V2. Verifica la asociación de imágenes antes de habilitar publicaciones."
            elif master_count != 1 or len(variants) > 1 or len(catalog_ids) > 1:
                reason = "Código de barras compartido. Revisa las variantes antes de publicar."
            elif len(variants) != 1 or catalog_ids != set(variants):
                reason = "No se pudo verificar una variante única en el catálogo vigente."
            # products_master.variant_id is a legacy representative across companies.
            # Validate the canonical company-3 mapping, never assume that representative is the catalog variant.
            if reason is None:
                cur.execute("SELECT to_regclass('bsale.product_master_variants') IS NOT NULL")
                if not cur.fetchone()[0]:
                    reason = "Falta verificar el mapa canónico de variantes por empresa."
                else:
                    cur.execute("""
                        SELECT mapping_status, variant_id, BTRIM(barcode)
                        FROM bsale.product_master_variants
                        WHERE product_master_id=%s AND company_id=3
                    """, (row_id,))
                    mappings = cur.fetchall()
                    if (len(mappings) != 1 or mappings[0][0] not in {"AUTO_EXACT", "MANUAL"}
                            or mappings[0][1] not in catalog_ids or mappings[0][2] != barcode):
                        reason = "La relación canónica de este producto con la variante del catálogo requiere revisión."
            image_urls = sorted({str(r[1]) for r in catalog_rows if r[1]})
            if reason is None and any(not photos.catalog_url_is_compatible(url, barcode) for url in image_urls):
                reason = "El catálogo apunta a otra imagen. Revisa la asociación antes de reemplazarla."
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
    # A remote read outage must not prevent local image preparation.
    sha = None
    github_checked = False
    github_error = None
    try:
        sha = photos.photo_sha(p["barcode"])
        github_checked = True
    except HTTPException as exc:
        github_error = str(exc.detail)
    return {**p, "github_sha": sha, "has_photo": (sha is not None) if github_checked else None,
            "github_checked": github_checked, "github_error": github_error,
            "writes_enabled": bool(github_checked and not p["blocked_reason"] and _writes_enabled()),
            "ai_enabled": photos.ai_enabled()}


@router.post("/{row_id}/photo/prepare")
def prepare(row_id: int, mode: str = Form("original"), image: UploadFile = File(...),
            user: dict = Depends(require_management_access)):
    actor = _actor(user)
    p = _product(row_id)
    # Preparing pixels is safe even when the product cannot be published.
    try:
        raw = image.file.read(photos.MAX_UPLOAD + 1)
    finally:
        image.file.close()
    data = photos.prepare_photo(raw, mode)
    sha = None
    enabled = not p["blocked_reason"] and _writes_enabled()
    publication_reason = p["blocked_reason"]
    if enabled:
        try:
            sha = photos.photo_sha(p["barcode"])
        except HTTPException as exc:
            enabled = False
            publication_reason = str(exc.detail)
    return {"data_base64": base64.b64encode(data).decode(), "bytes": len(data),
            "width": photos.SIZE, "height": photos.SIZE, "format": "WEBP", "mode": mode,
            "filename": p["filename"], "expected_sha": sha, "writes_enabled": bool(enabled),
            "publication_reason": publication_reason,
            "preview_token": photos.sign_preview(user_id=actor, row_id=row_id, barcode=p["barcode"],
                                                 variant_id=p["catalog_variant_ids"][0],
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
                                     barcode=p["barcode"], variant_id=p["catalog_variant_ids"][0], raw=raw)
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
    catalog_photo_inventory.invalidate_inventory()
    audit_pending = False
    try:
        admin.finish_photo_audit(audit_id, result)
    except Exception:
        # The GitHub commit exists. Never misreport it as a failed publication.
        audit_pending = True
        logger.error("catalog_photo_audit_finalize_pending audit=%s", audit_id)
    return {**result, "audit_id": audit_id, "audit_pending": audit_pending,
            "filename": p["filename"], "catalog_asset_url": p["catalog_asset_url"]}
