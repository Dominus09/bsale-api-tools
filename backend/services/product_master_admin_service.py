"""New ERP administrative data; schema readiness is checked before any mutation."""
from __future__ import annotations

import uuid
from fastapi import HTTPException
from backend.db import get_connection
from backend.services import system_feature_controls as controls


def actor_id(user: dict) -> int:
    try:
        actor = int(user.get("id") or user.get("uid"))
        if actor <= 0:
            raise ValueError()
        return actor
    except (TypeError, ValueError):
        raise HTTPException(403, "No se pudo identificar al usuario de gerencia.") from None


def pallet_ready(cur) -> bool:
    cur.execute("""
        SELECT EXISTS (SELECT 1 FROM information_schema.columns
            WHERE table_schema='bsale' AND table_name='products_master'
              AND column_name='boxes_per_pallet'),
               to_regclass('app.product_master_pallet_audit') IS NOT NULL
    """)
    column, audit = cur.fetchone()
    return bool(column and audit)


def photo_audit_ready() -> bool:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('app.catalog_photo_audit') IS NOT NULL")
            return bool(cur.fetchone()[0])
    finally:
        conn.close()


def pallet_data(ids: list[int]) -> dict:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            ready = pallet_ready(cur)
            if not ready:
                return {"schema_ready": False, "writes_enabled": False, "items": []}
            cur.execute("""
                SELECT id, boxes_per_pallet, units_per_box, weight_box_kg
                FROM bsale.products_master WHERE id = ANY(%s)
            """, (ids,))
            # Materialize the product result before the feature lookup executes
            # its own SQL on this cursor. Otherwise that query replaces the rows.
            items = [_pallet_row(row) for row in cur.fetchall()]
            writes_enabled = controls.enabled("product_master_pallet_writes", cur=cur)
            return {"schema_ready": True,
                    "writes_enabled": writes_enabled,
                    "items": items}
    finally:
        conn.close()


def _pallet_row(row) -> dict:
    row_id, boxes, units, weight = row
    return {"id": int(row_id), "boxes_per_pallet": boxes,
            "units_per_pallet": boxes * units if boxes and units and units > 0 else None,
            "load_weight_kg": round(float(boxes * weight), 4) if boxes and weight and weight > 0 else None}


def pallet_stats() -> dict:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if not pallet_ready(cur):
                return {"schema_ready": False, "total": None, "with_boxes_per_pallet": None}
            cur.execute("""
                SELECT COUNT(*)::bigint, COUNT(*) FILTER (WHERE boxes_per_pallet > 0)::bigint
                FROM bsale.products_master WHERE is_active = TRUE
            """)
            total, covered = cur.fetchone()
            return {"schema_ready": True, "total": int(total), "with_boxes_per_pallet": int(covered)}
    finally:
        conn.close()


def set_pallet_boxes(row_id: int, value: int | None, actor: int) -> dict:
    if not controls.enabled("product_master_pallet_writes"):
        raise HTTPException(503, "La edición de cajas por pallet está deshabilitada.")
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if not pallet_ready(cur):
                raise HTTPException(503, "La migración de cajas por pallet no está aplicada.")
            cur.execute("""
                SELECT id, barcode, variant_id, boxes_per_pallet, units_per_box, weight_box_kg
                FROM bsale.products_master WHERE id=%s FOR UPDATE
            """, (row_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(404, "Producto no encontrado.")
            _, barcode, variant_id, old, units, weight = row
            # The existing PM is barcode-consolidated; don't assign data to ambiguous catalog variants.
            cur.execute("""
                SELECT COUNT(DISTINCT bsale_id) FROM bsale.variants
                WHERE company_id=3 AND BTRIM(bar_code)=BTRIM(%s)
            """, (barcode,))
            if int(cur.fetchone()[0]) > 1:
                raise HTTPException(409, "Código de barras con varias variantes. Requiere revisión logística.")
            if old != value:
                cur.execute("""
                    UPDATE bsale.products_master SET boxes_per_pallet=%s, updated_at=NOW()
                    WHERE id=%s
                """, (value, row_id))
                cur.execute("""
                    INSERT INTO app.product_master_pallet_audit
                    (actor_user_id, products_master_id, variant_id, barcode, before_boxes, after_boxes)
                    VALUES (%s,%s,%s,%s,%s,%s)
                """, (actor, row_id, variant_id, barcode, old, value))
        conn.commit()
        return _pallet_row((row_id, value, units, weight))
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def start_photo_audit(*, actor: int, product: dict, expected_sha: str | None, prepared_sha256: str) -> str:
    audit_id = uuid.uuid4().hex
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO app.catalog_photo_audit
                (audit_id, actor_user_id, products_master_id, barcode, catalog_variant_id,
                 previous_sha, prepared_sha256, status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,'started')
            """, (audit_id, actor, product["row_id"], product["barcode"],
                  product["catalog_variant_ids"][0], expected_sha, prepared_sha256))
        conn.commit()
        return audit_id
    except Exception:
        conn.rollback()
        raise HTTPException(503, "No se pudo registrar la auditoría. No se publicó la fotografía.") from None
    finally:
        conn.close()


def finish_photo_audit(audit_id: str, result: dict | None, error_status: int | None = None) -> None:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE app.catalog_photo_audit
                SET status=%s, new_sha=%s, commit_sha=%s, error_status=%s, updated_at=NOW()
                WHERE audit_id=%s AND status='started'
            """, ("published" if result else "needs_review",
                  result.get("new_sha") if result else None,
                  result.get("commit_sha") if result else None, error_status, audit_id))
            if cur.rowcount != 1:
                raise RuntimeError("Photo audit record not updated")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
