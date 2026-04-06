from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backend.db import get_connection

router = APIRouter()

_ALLOWED_PAYMENT_METHODS = frozenset(
    {
        "transferencia",
        "efectivo",
        "cheque_dia",
        "cheque_30",
        "cheque_45",
        "cheque_60",
    }
)

_ALLOWED_VISIT_DAYS = frozenset(
    {
        "lunes",
        "martes",
        "miércoles",
        "jueves",
        "viernes",
        "sábado",
        "domingo",
    }
)


def _normalize_payment_method(raw: str | None) -> str | None:
    if raw is None:
        return None
    s = raw.strip().lower()
    if not s:
        return None
    if s not in _ALLOWED_PAYMENT_METHODS:
        raise HTTPException(
            status_code=400,
            detail=(
                "payment_method debe ser: transferencia, efectivo, cheque_dia, "
                "cheque_30, cheque_45 o cheque_60"
            ),
        )
    return s


def _normalize_visit_day(raw: str | None) -> str | None:
    if raw is None:
        return None
    s = raw.strip().lower()
    if not s:
        return None
    aliases = {"miercoles": "miércoles", "sabado": "sábado"}
    s = aliases.get(s, s)
    if s not in _ALLOWED_VISIT_DAYS:
        raise HTTPException(
            status_code=400,
            detail="visit_day debe ser un día de lunes a domingo",
        )
    return s


class SupplierCreateBody(BaseModel):
    name: str
    contact_name: str | None = None
    phone: str | None = None
    email: str | None = None
    notes: str | None = None
    payment_method: str | None = None
    visit_day: str | None = None


class SupplierPatchBody(BaseModel):
    name: str | None = None
    contact_name: str | None = None
    phone: str | None = None
    email: str | None = None
    notes: str | None = None
    payment_method: str | None = None
    visit_day: str | None = None
    is_active: bool | None = None


@router.get("/suppliers")
def list_suppliers(
    name: Optional[str] = Query(None),
) -> List[Dict[str, Any]]:
    sql = """
        SELECT
            id,
            name,
            contact_name,
            phone,
            email,
            notes,
            payment_method,
            visit_day,
            is_active,
            created_at,
            updated_at
        FROM bsale.suppliers
    """
    params: list[Any] = []

    if name is not None and name.strip():
        sql += " WHERE name ILIKE %s"
        params.append(f"%{name.strip()}%")

    sql += " ORDER BY name ASC, id ASC"

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


@router.post("/suppliers")
def create_supplier(body: SupplierCreateBody):
    clean_name = (body.name or "").strip()
    if not clean_name:
        raise HTTPException(status_code=400, detail="name es obligatorio")

    payment_method = _normalize_payment_method(body.payment_method)
    visit_day = _normalize_visit_day(body.visit_day)

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO bsale.suppliers (
                name,
                contact_name,
                phone,
                email,
                notes,
                payment_method,
                visit_day
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING
                id,
                name,
                contact_name,
                phone,
                email,
                notes,
                payment_method,
                visit_day,
                is_active,
                created_at,
                updated_at
            """,
            (
                clean_name,
                (body.contact_name or "").strip() or None,
                (body.phone or "").strip() or None,
                (body.email or "").strip() or None,
                (body.notes or "").strip() or None,
                payment_method,
                visit_day,
            ),
        )
        row = cur.fetchone()
        conn.commit()
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="Error creando proveedor")
    finally:
        cur.close()
        conn.close()

    return {
        "id": row[0],
        "name": row[1],
        "contact_name": row[2],
        "phone": row[3],
        "email": row[4],
        "notes": row[5],
        "payment_method": row[6],
        "visit_day": row[7],
        "is_active": row[8],
        "created_at": row[9],
        "updated_at": row[10],
    }


@router.patch("/suppliers/{supplier_id}")
def patch_supplier(supplier_id: int, body: SupplierPatchBody):
    updates: list[str] = []
    params: list[Any] = []

    if body.name is not None:
        clean_name = body.name.strip()
        if not clean_name:
            raise HTTPException(status_code=400, detail="name no puede ser vacío")
        updates.append("name = %s")
        params.append(clean_name)

    if body.contact_name is not None:
        updates.append("contact_name = %s")
        params.append(body.contact_name.strip() or None)

    if body.phone is not None:
        updates.append("phone = %s")
        params.append(body.phone.strip() or None)

    if body.email is not None:
        updates.append("email = %s")
        params.append(body.email.strip() or None)

    if body.notes is not None:
        updates.append("notes = %s")
        params.append(body.notes.strip() or None)

    if body.payment_method is not None:
        updates.append("payment_method = %s")
        params.append(_normalize_payment_method(body.payment_method))

    if body.visit_day is not None:
        updates.append("visit_day = %s")
        params.append(_normalize_visit_day(body.visit_day))

    if body.is_active is not None:
        updates.append("is_active = %s")
        params.append(body.is_active)

    if not updates:
        raise HTTPException(status_code=400, detail="No hay campos para actualizar")

    updates.append("updated_at = NOW()")
    params.append(supplier_id)

    sql = f"""
        UPDATE bsale.suppliers
        SET {", ".join(updates)}
        WHERE id = %s
        RETURNING
            id,
            name,
            contact_name,
            phone,
            email,
            notes,
            payment_method,
            visit_day,
            is_active,
            created_at,
            updated_at
    """

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql, tuple(params))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Proveedor no encontrado")
        conn.commit()
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail="Error actualizando proveedor")
    finally:
        cur.close()
        conn.close()

    return {
        "id": row[0],
        "name": row[1],
        "contact_name": row[2],
        "phone": row[3],
        "email": row[4],
        "notes": row[5],
        "payment_method": row[6],
        "visit_day": row[7],
        "is_active": row[8],
        "created_at": row[9],
        "updated_at": row[10],
    }
