"""Administración de reglas de margen (bsale.margin_rules)."""

from __future__ import annotations

from io import BytesIO
from typing import Any

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from backend.db import get_connection
from backend.utils.auth_staff import require_staff_user
from backend.utils.margin_rules_validation import margin_rule_key, validate_margin_rule_patch

router = APIRouter(tags=["margin-rules"])


class MarginRulePatchBody(BaseModel):
    min_margin: float | int
    max_margin: float | int
    active: bool
    notes: str | None = Field(None, max_length=2000)


def _row_to_dict(cur, row) -> dict[str, Any]:
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


def _fetch_margin_rules(
    *,
    company_id: int | None = None,
    price_list_id: int | None = None,
    product_type_id: int | None = None,
    active: bool | None = None,
) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        sql = """
            SELECT
                mr.id,
                mr.company_id,
                c.name AS company_name,
                mr.price_list_id,
                pl.name AS price_list_name,
                mr.product_type_id,
                COALESCE(pt.name, '(Todos los tipos)') AS product_type_name,
                mr.min_margin,
                COALESCE(mr.max_margin, 0) AS max_margin,
                mr.active,
                COALESCE(mr.notes, '') AS notes
            FROM bsale.margin_rules mr
            LEFT JOIN bsale.companies c
                ON c.company_id = mr.company_id
            LEFT JOIN bsale.price_lists pl
                ON pl.company_id = mr.company_id
               AND pl.bsale_id = mr.price_list_id
            LEFT JOIN bsale.product_types pt
                ON pt.company_id = mr.company_id
               AND mr.product_type_id IS NOT NULL
               AND pt.bsale_id = mr.product_type_id
            WHERE 1=1
        """
        params: list[Any] = []
        if company_id is not None:
            sql += " AND mr.company_id = %s"
            params.append(company_id)
        if price_list_id is not None:
            sql += " AND mr.price_list_id = %s"
            params.append(price_list_id)
        if product_type_id is not None:
            sql += " AND mr.product_type_id IS NOT DISTINCT FROM %s"
            params.append(product_type_id)
        if active is not None:
            sql += " AND mr.active = %s"
            params.append(active)
        sql += """
            ORDER BY c.name NULLS LAST, pl.name NULLS LAST, pt.name NULLS LAST, mr.id
        """
        cur.execute(sql, params)
        rows = [_row_to_dict(cur, r) for r in cur.fetchall()]
        for r in rows:
            r["rule_key"] = margin_rule_key(
                int(r["company_id"]),
                int(r["price_list_id"]),
                int(r["product_type_id"]) if r.get("product_type_id") is not None else None,
            )
            r["min_margin"] = float(r["min_margin"]) if r.get("min_margin") is not None else 0.0
            r["max_margin"] = float(r["max_margin"]) if r.get("max_margin") is not None else 0.0
        cur.close()
    finally:
        conn.close()
    return rows


@router.get("/margin-rules")
def list_margin_rules(
    company_id: int | None = Query(None, ge=1),
    price_list_id: int | None = Query(None, ge=1),
    product_type_id: int | None = Query(None, ge=1),
    active: str | None = Query(
        None,
        description="true | false | all (omitir = todos)",
    ),
    _user: dict = Depends(require_staff_user),
):
    active_filter: bool | None
    if active is None or active.strip().lower() in ("all", "todos", ""):
        active_filter = None
    elif active.strip().lower() in ("true", "1", "activo", "active"):
        active_filter = True
    elif active.strip().lower() in ("false", "0", "inactivo", "inactive"):
        active_filter = False
    else:
        raise HTTPException(status_code=400, detail="Filtro active inválido.")
    items = _fetch_margin_rules(
        company_id=company_id,
        price_list_id=price_list_id,
        product_type_id=product_type_id,
        active=active_filter,
    )
    return {"items": items, "count": len(items)}


@router.patch("/margin-rules/{rule_id}")
def patch_margin_rule(
    rule_id: int,
    body: MarginRulePatchBody,
    _user: dict = Depends(require_staff_user),
):
    try:
        min_v, max_v, warnings = validate_margin_rule_patch(
            min_margin=body.min_margin,
            max_margin=body.max_margin,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    notes = (body.notes or "").strip() or None
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE bsale.margin_rules
            SET min_margin = %s,
                max_margin = %s,
                active = %s,
                notes = %s
            WHERE id = %s
            RETURNING id
            """,
            (min_v, max_v, body.active, notes, rule_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Regla no encontrada.")
        conn.commit()
        cur.close()
    except HTTPException:
        conn.rollback()
        raise
    except Exception as exc:
        conn.rollback()
        err = str(exc).lower()
        if "max_margin" in err or "notes" in err or "column" in err:
            raise HTTPException(
                status_code=500,
                detail=(
                    "Faltan columnas max_margin/notes en bsale.margin_rules. "
                    "Ejecute backend/sql/margin_rules_extend.sql"
                ),
            ) from exc
        raise HTTPException(status_code=500, detail="Error al guardar regla.") from exc
    finally:
        conn.close()

    items = _fetch_margin_rules()
    updated = next((r for r in items if int(r["id"]) == rule_id), None)
    if not updated:
        raise HTTPException(status_code=404, detail="Regla no encontrada tras actualizar.")
    return {"item": updated, "warnings": warnings}


@router.get("/margin-rules/export")
def export_margin_rules(
    company_id: int | None = Query(None, ge=1),
    price_list_id: int | None = Query(None, ge=1),
    product_type_id: int | None = Query(None, ge=1),
    active: str | None = Query(None),
    _user: dict = Depends(require_staff_user),
):
    active_filter: bool | None
    if active is None or str(active).strip().lower() in ("all", "todos", ""):
        active_filter = None
    elif str(active).strip().lower() in ("true", "1", "activo", "active"):
        active_filter = True
    elif str(active).strip().lower() in ("false", "0", "inactivo", "inactive"):
        active_filter = False
    else:
        active_filter = None

    items = _fetch_margin_rules(
        company_id=company_id,
        price_list_id=price_list_id,
        product_type_id=product_type_id,
        active=active_filter,
    )
    export_rows = [
        {
            "Empresa": r.get("company_name") or r.get("company_id"),
            "Lista de precios": r.get("price_list_name") or r.get("price_list_id"),
            "Tipo producto": r.get("product_type_name"),
            "Margen mínimo": r.get("min_margin"),
            "Margen máximo": r.get("max_margin"),
            "Activo": "Sí" if r.get("active") else "No",
            "Notas": r.get("notes") or "",
        }
        for r in items
    ]
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        pd.DataFrame(export_rows).to_excel(writer, sheet_name="Política márgenes", index=False)
    buf.seek(0)
    fname = "politica_margenes.xlsx"
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )
