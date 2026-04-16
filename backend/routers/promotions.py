"""
Promociones (app.promotions): cabecera, ítems, empresas y snapshot de precios.
SQL raw + psycopg2. No modifica routers legacy (/offers).
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from backend.db import get_connection

router = APIRouter(tags=["promotions"])

CANAL_RUTA = "ruta"
COMPANY_ID_RUTA_FIJO = 3


class PromotionItemIn(BaseModel):
    barcode: str = Field(..., min_length=1, max_length=50)
    tipo_descuento: str
    valor: Decimal = Field(..., ge=0)
    observacion: str | None = None


class PromotionCompanyIn(BaseModel):
    company_id: int
    price_list: str | None = Field(None, max_length=50)


class PromotionCreateBody(BaseModel):
    tipo: str
    canal: str
    fecha_inicio: date
    fecha_fin: date
    activa: bool = True
    items: list[PromotionItemIn]
    companies: list[PromotionCompanyIn] = Field(default_factory=list)


def _norm(s: str) -> str:
    return (s or "").strip().lower()


def _fetch_variant_price(
    cur: Any,
    barcode: str,
    company_id: int,
) -> tuple[Decimal | None, str | None]:
    """
    Precio lista para barcode + company.
    Convención bsale: variant_prices.variant_id = variants.bsale_id (mismo company_id).
    """
    cur.execute(
        """
        SELECT
            COALESCE(vp.price_gross, vp.price_net)::numeric AS price,
            pl.name::text AS price_list_name
        FROM bsale.variant_prices vp
        INNER JOIN bsale.variants v
            ON v.company_id = vp.company_id
           AND v.bsale_id = vp.variant_id
        LEFT JOIN bsale.price_lists pl
            ON pl.company_id = vp.company_id
           AND pl.bsale_id = vp.price_list_id
        WHERE btrim(v.bar_code) = btrim(%s)
          AND vp.company_id = %s
        ORDER BY vp.price_list_id NULLS LAST
        LIMIT 1
        """,
        (barcode, company_id),
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        return None, None
    return Decimal(str(row[0])), (row[1] if row[1] is not None else None)


def _calc_precio_oferta(precio_normal: Decimal, tipo_descuento: str, valor: Decimal) -> Decimal:
    td = _norm(tipo_descuento)
    if td == "porcentaje":
        return (precio_normal * (Decimal(1) - valor / Decimal(100))).quantize(Decimal("0.01"))
    if td == "precio_fijo":
        return valor.quantize(Decimal("0.01"))
    raise ValueError("tipo_descuento inválido")


@router.post("/promotions")
def create_promotion(body: PromotionCreateBody) -> dict[str, Any]:
    tipo = _norm(body.tipo)
    canal = _norm(body.canal)
    if tipo not in ("oferta", "remate"):
        raise HTTPException(status_code=400, detail="tipo debe ser oferta o remate")
    if canal not in ("ruta", "detalle"):
        raise HTTPException(status_code=400, detail="canal debe ser ruta o detalle")
    if body.fecha_inicio > body.fecha_fin:
        raise HTTPException(status_code=400, detail="fecha_inicio no puede ser mayor que fecha_fin")
    if not body.items:
        raise HTTPException(status_code=400, detail="items no puede estar vacío")

    for it in body.items:
        if _norm(it.tipo_descuento) not in ("porcentaje", "precio_fijo"):
            raise HTTPException(
                status_code=400,
                detail=f"tipo_descuento inválido para barcode {it.barcode}",
            )

    conn = get_connection()
    cur = conn.cursor()
    snapshots = 0
    try:
        cur.execute(
            """
            INSERT INTO app.promotions (tipo, canal, fecha_inicio, fecha_fin, activa)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
            """,
            (tipo, canal, body.fecha_inicio, body.fecha_fin, body.activa),
        )
        pid_row = cur.fetchone()
        if not pid_row:
            raise HTTPException(status_code=500, detail="No se pudo crear la promoción")
        promotion_id = int(pid_row[0])

        for it in body.items:
            bc = (it.barcode or "").strip()[:50]
            cur.execute(
                """
                INSERT INTO app.promotion_items
                    (promotion_id, barcode, tipo_descuento, valor, observacion)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    promotion_id,
                    bc,
                    _norm(it.tipo_descuento),
                    it.valor,
                    (it.observacion or "").strip() or None,
                ),
            )

        if canal == CANAL_RUTA:
            pl0: str | None = None
            if body.companies:
                pl0 = body.companies[0].price_list
            cur.execute(
                """
                INSERT INTO app.promotion_companies (promotion_id, company_id, price_list)
                VALUES (%s, %s, %s)
                """,
                (promotion_id, COMPANY_ID_RUTA_FIJO, pl0),
            )
            company_rows: list[tuple[int, str | None]] = [(COMPANY_ID_RUTA_FIJO, pl0)]
        else:
            if not body.companies:
                raise HTTPException(
                    status_code=400,
                    detail="canal detalle requiere al menos una empresa en companies",
                )
            company_rows = []
            for c in body.companies:
                cid = int(c.company_id)
                pl = (c.price_list or "").strip()[:50] or None
                cur.execute(
                    """
                    INSERT INTO app.promotion_companies (promotion_id, company_id, price_list)
                    VALUES (%s, %s, %s)
                    """,
                    (promotion_id, cid, pl),
                )
                company_rows.append((cid, pl))

        for it in body.items:
            bc = (it.barcode or "").strip()[:50]
            td = _norm(it.tipo_descuento)
            val = it.valor
            for company_id, price_list in company_rows:
                price_normal, pl_name = _fetch_variant_price(cur, bc, company_id)
                if price_normal is None:
                    continue
                pl_snap = price_list or pl_name
                try:
                    precio_oferta = _calc_precio_oferta(price_normal, td, val)
                except ValueError:
                    continue
                cur.execute(
                    """
                    INSERT INTO app.promotion_price_snapshot (
                        promotion_id, barcode, company_id, price_list,
                        precio_normal, precio_oferta, canal
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        promotion_id,
                        bc,
                        company_id,
                        pl_snap,
                        price_normal,
                        precio_oferta,
                        canal,
                    ),
                )
                snapshots += 1

        conn.commit()
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        cur.close()
        conn.close()

    return {
        "id": promotion_id,
        "items_processed": len(body.items),
        "snapshots_generated": snapshots,
    }


@router.get("/promotions")
def list_promotions(activa: bool | None = None) -> list[dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor()
    try:
        if activa is None:
            cur.execute(
                """
                SELECT id, tipo, canal, fecha_inicio, fecha_fin, activa, created_at
                FROM app.promotions
                ORDER BY id DESC
                """
            )
        else:
            cur.execute(
                """
                SELECT id, tipo, canal, fecha_inicio, fecha_fin, activa, created_at
                FROM app.promotions
                WHERE activa = %s
                ORDER BY id DESC
                """,
                (activa,),
            )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        cur.close()
        conn.close()


@router.get("/promotions/grid")
def promotions_grid(
    canal: str | None = Query(None, description="Filtrar por canal (ruta | detalle)"),
    tipo: str | None = Query(None, description="Filtrar por tipo (oferta | remate)"),
    activa: bool | None = Query(None, description="Filtrar por flag activa en cabecera"),
    estado: str | None = Query(
        None,
        description="Filtrar por estado derivado: Activa | Inactiva | Programada | Vencida",
    ),
    company_id: int | None = Query(None, description="Filtrar por empresa en snapshot"),
) -> list[dict[str, Any]]:
    """
    Grilla ERP: una fila por registro en promotion_price_snapshot, con catálogo
    desde bsale.products_master y respaldo bsale.variants (mismo barcode + company).
    """
    where: list[str] = ["1=1"]
    params: list[Any] = []

    if canal is not None and str(canal).strip():
        where.append("p.canal = %s")
        params.append(_norm(str(canal)))
    if tipo is not None and str(tipo).strip():
        where.append("p.tipo = %s")
        params.append(_norm(str(tipo)))
    if activa is not None:
        where.append("p.activa = %s")
        params.append(activa)
    if company_id is not None:
        where.append("ps.company_id = %s")
        params.append(int(company_id))
    if estado is not None and str(estado).strip():
        estado_param = str(estado).strip().capitalize()
        print("Estado recibido:", estado_param)
        where.append(
            """CASE
WHEN NOT p.activa THEN 'Inactiva'
WHEN CURRENT_DATE < p.fecha_inicio THEN 'Programada'
WHEN CURRENT_DATE > p.fecha_fin THEN 'Vencida'
ELSE 'Activa'
END = %s"""
        )
        params.append(estado_param)

    # Columnas validadas contra el repo (no sustituyen \d en tu BD):
    # - bsale.products_master: barcode, product_name, variant_name, product_type (products_master_schema.sql)
    # - bsale.variants: company_id, bsale_id, product_id, code, bar_code, description (margin_analysis_view.sql)
    # - bsale.product_types: company_id, bsale_id, name, state — no usado en este SELECT
    sql = f"""
        SELECT
            ps.promotion_id AS promotion_id,
            p.activa AS activa,
            COALESCE(pm.product_type, '') AS tipo_producto,
            COALESCE(pm.product_name, '') AS producto,
            COALESCE(NULLIF(pm.variant_name, ''), vv.description, '') AS variante,
            ps.barcode AS codigo_barras,
            ROUND(
                CASE
                    WHEN ps.precio_normal > 0
                    THEN ((ps.precio_normal - ps.precio_oferta) / ps.precio_normal) * 100
                    ELSE 0
                END,
                2
            ) AS descuento_porcentaje,
            CASE
                WHEN pi.tipo_descuento = 'porcentaje' THEN CONCAT(pi.valor::text, '%')
                ELSE 'precio fijo'
            END AS descuento_texto,
            p.fecha_inicio AS fecha_inicio,
            p.fecha_fin AS fecha_fin,
            p.tipo AS tipo,
            pi.observacion AS observacion,
            p.canal AS canal,
            CASE
                WHEN NOT p.activa THEN 'Inactiva'
                WHEN CURRENT_DATE < p.fecha_inicio THEN 'Programada'
                WHEN CURRENT_DATE > p.fecha_fin THEN 'Vencida'
                ELSE 'Activa'
            END AS estado,
            ps.company_id AS company_id,
            COALESCE(ps.price_list, pc.price_list) AS price_list,
            ps.precio_normal AS precio_normal,
            ps.precio_oferta AS precio_oferta
        FROM app.promotion_price_snapshot ps
        INNER JOIN app.promotions p
            ON p.id = ps.promotion_id
        INNER JOIN app.promotion_items pi
            ON pi.promotion_id = p.id
           AND pi.barcode = ps.barcode
        INNER JOIN app.promotion_companies pc
            ON pc.promotion_id = p.id
           AND pc.company_id = ps.company_id
        LEFT JOIN bsale.products_master pm
            ON pm.barcode = ps.barcode
        LEFT JOIN (
            SELECT DISTINCT ON (v.company_id, v.bar_code)
                v.company_id,
                v.bar_code,
                v.description
            FROM bsale.variants v
            WHERE v.bar_code IS NOT NULL
            ORDER BY v.company_id, v.bar_code, v.bsale_id
        ) vv
            ON vv.company_id = ps.company_id
           AND vv.bar_code = ps.barcode
        WHERE {" AND ".join(where)}
        ORDER BY p.fecha_inicio DESC,
                 COALESCE(pm.product_name, '') ASC
    """

    conn = get_connection()
    cur = conn.cursor()
    try:
        print("[promotions/grid] SQL:\n", sql.strip())
        print("[promotions/grid] params:", repr(params))
        if params:
            print(cur.mogrify(sql, tuple(params)))
        else:
            print(sql)

        try:
            cur.execute(sql, tuple(params))
            cols = [d[0] for d in cur.description]
            rows_out = [dict(zip(cols, row)) for row in cur.fetchall()]
        except Exception as e:
            print("ERROR GRID:", e)
            raise HTTPException(status_code=500, detail=str(e)) from e

        return rows_out
    finally:
        cur.close()
        conn.close()


@router.get("/promotions/{promotion_id}")
def get_promotion(promotion_id: int) -> dict[str, Any]:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, tipo, canal, fecha_inicio, fecha_fin, activa, created_at
            FROM app.promotions
            WHERE id = %s
            """,
            (promotion_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Promoción no encontrada")
        keys = [d[0] for d in cur.description]
        out: dict[str, Any] = dict(zip(keys, row))

        cur.execute(
            """
            SELECT id, barcode, tipo_descuento, valor, observacion
            FROM app.promotion_items
            WHERE promotion_id = %s
            ORDER BY id
            """,
            (promotion_id,),
        )
        ic = [d[0] for d in cur.description]
        out["items"] = [dict(zip(ic, r)) for r in cur.fetchall()]

        cur.execute(
            """
            SELECT id, company_id, price_list
            FROM app.promotion_companies
            WHERE promotion_id = %s
            ORDER BY id
            """,
            (promotion_id,),
        )
        cc = [d[0] for d in cur.description]
        out["companies"] = [dict(zip(cc, r)) for r in cur.fetchall()]

        cur.execute(
            """
            SELECT COUNT(*)::int FROM app.promotion_price_snapshot WHERE promotion_id = %s
            """,
            (promotion_id,),
        )
        cnt = cur.fetchone()
        out["snapshots_count"] = int(cnt[0]) if cnt else 0

        return out
    finally:
        cur.close()
        conn.close()


@router.put("/promotions/{promotion_id}/toggle")
def toggle_promotion(promotion_id: int) -> dict[str, Any]:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE app.promotions
            SET activa = NOT activa
            WHERE id = %s
            RETURNING id, tipo, canal, fecha_inicio, fecha_fin, activa, created_at
            """,
            (promotion_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Promoción no encontrada")
        conn.commit()
        keys = [d[0] for d in cur.description]
        return dict(zip(keys, row))
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        cur.close()
        conn.close()
