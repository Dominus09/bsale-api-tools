from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backend.db import get_connection

router = APIRouter()


class OfferCreateBody(BaseModel):
    barcode: str
    offer_type: str
    status: str
    start_date: date | None = None
    end_date: date | None = None
    reason: str | None = None
    notes: str | None = None


class OfferPatchBody(BaseModel):
    status: str | None = None
    start_date: date | None = None
    end_date: date | None = None
    notes: str | None = None


@router.get("/offers")
def list_offers(
    status: Optional[str] = Query(None),
    offer_type: Optional[str] = Query(None),
    active_only: bool = Query(False),
) -> List[Dict[str, Any]]:
    sql = """
        SELECT
            id,
            barcode,
            offer_type,
            status,
            start_date,
            end_date,
            reason,
            notes,
            created_at,
            updated_at
        FROM bsale.product_offers
    """
    where_parts: list[str] = []
    params: list[Any] = []

    if status is not None and status.strip():
        where_parts.append("status = %s")
        params.append(status.strip())

    if offer_type is not None and offer_type.strip():
        where_parts.append("offer_type = %s")
        params.append(offer_type.strip())

    if active_only:
        where_parts.append("CURRENT_DATE BETWEEN start_date AND end_date")

    if where_parts:
        sql += " WHERE " + " AND ".join(where_parts)

    sql += " ORDER BY created_at DESC, id DESC"

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        result = [dict(zip(columns, row)) for row in rows]
        cur.close()
    finally:
        conn.close()

    return result


@router.post("/offers")
def create_offer(body: OfferCreateBody):
    barcode = (body.barcode or "").strip()
    offer_type = (body.offer_type or "").strip()
    status = (body.status or "").strip()

    if not barcode:
        raise HTTPException(status_code=400, detail="barcode es obligatorio")
    if not offer_type:
        raise HTTPException(status_code=400, detail="offer_type es obligatorio")
    if not status:
        raise HTTPException(status_code=400, detail="status es obligatorio")
    if body.start_date and body.end_date and body.start_date > body.end_date:
        raise HTTPException(status_code=400, detail="start_date no puede ser mayor a end_date")

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT 1
            FROM bsale.products_master
            WHERE barcode = %s
            LIMIT 1
            """,
            (barcode,),
        )
        exists = cur.fetchone()
        if not exists:
            raise HTTPException(status_code=400, detail="barcode no existe en products_master")

        cur.execute(
            """
            INSERT INTO bsale.product_offers (
                barcode,
                offer_type,
                status,
                start_date,
                end_date,
                reason,
                notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id, barcode, offer_type, status, start_date, end_date, reason, notes, created_at, updated_at
            """,
            (
                barcode,
                offer_type,
                status,
                body.start_date,
                body.end_date,
                (body.reason or "").strip() or None,
                (body.notes or "").strip() or None,
            ),
        )
        row = cur.fetchone()
        conn.commit()
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="Error creando oferta")
    finally:
        cur.close()
        conn.close()

    return {
        "id": row[0],
        "barcode": row[1],
        "offer_type": row[2],
        "status": row[3],
        "start_date": row[4],
        "end_date": row[5],
        "reason": row[6],
        "notes": row[7],
        "created_at": row[8],
        "updated_at": row[9],
    }


@router.patch("/offers/{offer_id}")
def patch_offer(offer_id: int, body: OfferPatchBody):
    updates: list[str] = []
    params: list[Any] = []

    if body.status is not None:
        status = body.status.strip()
        if not status:
            raise HTTPException(status_code=400, detail="status no puede ser vacío")
        updates.append("status = %s")
        params.append(status)

    if body.start_date is not None:
        updates.append("start_date = %s")
        params.append(body.start_date)

    if body.end_date is not None:
        updates.append("end_date = %s")
        params.append(body.end_date)

    if body.notes is not None:
        updates.append("notes = %s")
        params.append(body.notes.strip() or None)

    if not updates:
        raise HTTPException(status_code=400, detail="No hay campos para actualizar")

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT start_date, end_date
            FROM bsale.product_offers
            WHERE id = %s
            """,
            (offer_id,),
        )
        current = cur.fetchone()
        if not current:
            raise HTTPException(status_code=404, detail="Oferta no encontrada")

        final_start = body.start_date if body.start_date is not None else current[0]
        final_end = body.end_date if body.end_date is not None else current[1]
        if final_start and final_end and final_start > final_end:
            raise HTTPException(status_code=400, detail="start_date no puede ser mayor a end_date")

        updates.append("updated_at = NOW()")
        params.append(offer_id)

        sql = f"""
            UPDATE bsale.product_offers
            SET {", ".join(updates)}
            WHERE id = %s
            RETURNING id, barcode, offer_type, status, start_date, end_date, reason, notes, created_at, updated_at
        """
        cur.execute(sql, tuple(params))
        row = cur.fetchone()
        conn.commit()
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="Error actualizando oferta")
    finally:
        cur.close()
        conn.close()

    return {
        "id": row[0],
        "barcode": row[1],
        "offer_type": row[2],
        "status": row[3],
        "start_date": row[4],
        "end_date": row[5],
        "reason": row[6],
        "notes": row[7],
        "created_at": row[8],
        "updated_at": row[9],
    }
