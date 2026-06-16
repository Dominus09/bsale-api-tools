"""
Promociones (app.promotions): cabecera, ítems, empresas y snapshot de precios congelados.
regular_price / sale_price no se recalculan tras la creación (salvo edición manual de sale_price).
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from backend.db import get_connection
from backend.utils.promotion_price_list_map import mapped_price_list_for_company
from backend.utils.promotion_prices import calc_sale_price

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


class SnapshotSalePricePatch(BaseModel):
    sale_price: Decimal = Field(..., ge=0)


def _norm(s: str) -> str:
    return (s or "").strip().lower()


def _get_company_name(cur: Any, company_id: int) -> str | None:
    cur.execute(
        "SELECT name::text FROM bsale.companies WHERE company_id = %s LIMIT 1",
        (company_id,),
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return str(row[0]).strip() or None


def _resolve_price_list_name(
    cur: Any,
    company_id: int,
    explicit: str | None,
) -> str | None:
    if explicit and str(explicit).strip():
        return str(explicit).strip()[:50]
    company_name = _get_company_name(cur, company_id)
    if not company_name:
        return None
    return mapped_price_list_for_company(company_name)


def _fetch_variant_price(
    cur: Any,
    barcode: str,
    company_id: int,
    price_list_name: str | None,
) -> tuple[Decimal | None, str | None]:
    """
    Precio lista para barcode + company + lista (por nombre).
    Convención bsale: variant_prices.variant_id = variants.bsale_id (mismo company_id).
    """
    params: list[Any] = [barcode, company_id]
    list_filter = ""
    if price_list_name and str(price_list_name).strip():
        list_filter = "AND lower(btrim(pl.name)) = lower(btrim(%s))"
        params.append(str(price_list_name).strip())

    cur.execute(
        f"""
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
          {list_filter}
        ORDER BY vp.price_list_id NULLS LAST
        LIMIT 1
        """,
        tuple(params),
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        return None, None
    return Decimal(str(row[0])), (row[1] if row[1] is not None else None)


def _insert_frozen_snapshot(
    cur: Any,
    *,
    promotion_id: int,
    barcode: str,
    company_id: int,
    price_list: str | None,
    regular_price: Decimal,
    sale_price: Decimal,
    canal: str,
) -> None:
    """Persiste snapshot; regular_price queda inmutable salvo nueva promoción."""
    cur.execute(
        f"""
        INSERT INTO app.promotion_price_snapshot (
            promotion_id, barcode, company_id, price_list,
            regular_price, sale_price, precio_normal, precio_oferta, canal
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            promotion_id,
            barcode,
            company_id,
            price_list,
            regular_price,
            sale_price,
            regular_price,
            sale_price,
            canal,
        ),
    )


def _active_snapshot_sql_extra() -> str:
    return """
        AND p.activa = TRUE
        AND CURRENT_DATE BETWEEN p.fecha_inicio AND p.fecha_fin
    """


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
    warnings: list[str] = []
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
            pl_explicit: str | None = None
            if body.companies:
                pl_explicit = body.companies[0].price_list
            pl_resolved = _resolve_price_list_name(cur, COMPANY_ID_RUTA_FIJO, pl_explicit)
            cur.execute(
                """
                INSERT INTO app.promotion_companies (promotion_id, company_id, price_list)
                VALUES (%s, %s, %s)
                """,
                (promotion_id, COMPANY_ID_RUTA_FIJO, pl_resolved),
            )
            company_rows: list[tuple[int, str | None]] = [(COMPANY_ID_RUTA_FIJO, pl_resolved)]
        else:
            if not body.companies:
                raise HTTPException(
                    status_code=400,
                    detail="canal detalle requiere al menos una empresa en companies",
                )
            company_rows = []
            for c in body.companies:
                cid = int(c.company_id)
                pl_resolved = _resolve_price_list_name(cur, cid, c.price_list)
                cur.execute(
                    """
                    INSERT INTO app.promotion_companies (promotion_id, company_id, price_list)
                    VALUES (%s, %s, %s)
                    """,
                    (promotion_id, cid, pl_resolved),
                )
                company_rows.append((cid, pl_resolved))

        for it in body.items:
            bc = (it.barcode or "").strip()[:50]
            td = _norm(it.tipo_descuento)
            val = it.valor
            for company_id, price_list in company_rows:
                if not price_list:
                    warnings.append(
                        f"Sin lista de precios para empresa {company_id}, barcode {bc}"
                    )
                    continue
                regular_price, pl_name = _fetch_variant_price(cur, bc, company_id, price_list)
                if regular_price is None:
                    warnings.append(
                        f"Sin precio en lista '{price_list}' para empresa {company_id}, barcode {bc}"
                    )
                    continue
                pl_snap = price_list or pl_name
                try:
                    sale_price = calc_sale_price(regular_price, td, val)
                except ValueError:
                    warnings.append(f"Descuento inválido para barcode {bc}")
                    continue
                _insert_frozen_snapshot(
                    cur,
                    promotion_id=promotion_id,
                    barcode=bc,
                    company_id=company_id,
                    price_list=pl_snap,
                    regular_price=regular_price,
                    sale_price=sale_price,
                    canal=canal,
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
        "warnings": warnings,
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


def _grid_select_sql(where_clause: str) -> str:
    return f"""
        SELECT
            ps.id AS snapshot_id,
            ps.promotion_id AS promotion_id,
            p.activa AS activa,
            COALESCE(pm.product_type, '') AS tipo_producto,
            COALESCE(pm.product_name, '') AS producto,
            COALESCE(NULLIF(pm.variant_name, ''), vv.description, '') AS variante,
            ps.barcode AS codigo_barras,
            ROUND(
                CASE
                    WHEN COALESCE(ps.regular_price, ps.precio_normal) > 0
                    THEN (
                        (COALESCE(ps.regular_price, ps.precio_normal)
                         - COALESCE(ps.sale_price, ps.precio_oferta))
                        / COALESCE(ps.regular_price, ps.precio_normal)
                    ) * 100
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
            COALESCE(ps.regular_price, ps.precio_normal) AS regular_price,
            COALESCE(ps.sale_price, ps.precio_oferta) AS sale_price,
            COALESCE(ps.regular_price, ps.precio_normal) AS precio_normal,
            COALESCE(ps.sale_price, ps.precio_oferta) AS precio_oferta
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
        WHERE {where_clause}
        ORDER BY p.fecha_inicio DESC,
                 COALESCE(pm.product_name, '') ASC
    """


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
    """Grilla ERP: una fila por snapshot con precios congelados regular_price / sale_price."""
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
        where.append(
            """CASE
WHEN NOT p.activa THEN 'Inactiva'
WHEN CURRENT_DATE < p.fecha_inicio THEN 'Programada'
WHEN CURRENT_DATE > p.fecha_fin THEN 'Vencida'
ELSE 'Activa'
END = %s"""
        )
        params.append(estado_param)

    sql = _grid_select_sql(" AND ".join(where))

    conn = get_connection()
    cur = conn.cursor()
    try:
        if params:
            cur.execute(sql, tuple(params))
        else:
            cur.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        cur.close()
        conn.close()


@router.get("/promotions/active-snapshot")
def get_active_promotion_snapshot(
    company_id: int = Query(..., ge=1),
    barcode: str = Query(..., min_length=1, max_length=50),
) -> dict[str, Any]:
    """
    Snapshot vigente para etiquetas / consulta puntual.
    Fuente de verdad: precios congelados en BD, no lista Bsale actual.
    """
    bc = (barcode or "").strip()
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT
                ps.id AS snapshot_id,
                ps.promotion_id,
                ps.barcode,
                ps.company_id,
                COALESCE(ps.price_list, pc.price_list) AS price_list,
                COALESCE(ps.regular_price, ps.precio_normal) AS regular_price,
                COALESCE(ps.sale_price, ps.precio_oferta) AS sale_price,
                p.tipo,
                p.canal,
                p.fecha_inicio,
                p.fecha_fin,
                CASE
                    WHEN NOT p.activa THEN 'Inactiva'
                    WHEN CURRENT_DATE < p.fecha_inicio THEN 'Programada'
                    WHEN CURRENT_DATE > p.fecha_fin THEN 'Vencida'
                    ELSE 'Activa'
                END AS estado
            FROM app.promotion_price_snapshot ps
            INNER JOIN app.promotions p ON p.id = ps.promotion_id
            INNER JOIN app.promotion_companies pc
                ON pc.promotion_id = p.id AND pc.company_id = ps.company_id
            WHERE ps.company_id = %s
              AND btrim(ps.barcode) = btrim(%s)
              {_active_snapshot_sql_extra()}
            ORDER BY p.fecha_inicio DESC, ps.id DESC
            LIMIT 1
            """,
            (company_id, bc),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(
                status_code=404,
                detail="No hay promoción activa con snapshot para este producto",
            )
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))
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
            SELECT
                id,
                barcode,
                company_id,
                price_list,
                COALESCE(regular_price, precio_normal) AS regular_price,
                COALESCE(sale_price, precio_oferta) AS sale_price,
                canal,
                fecha_generado
            FROM app.promotion_price_snapshot
            WHERE promotion_id = %s
            ORDER BY id
            """,
            (promotion_id,),
        )
        sc = [d[0] for d in cur.description]
        out["snapshots"] = [dict(zip(sc, r)) for r in cur.fetchall()]
        out["snapshots_count"] = len(out["snapshots"])

        return out
    finally:
        cur.close()
        conn.close()


@router.patch("/promotions/snapshots/{snapshot_id}/sale-price")
def patch_snapshot_sale_price(
    snapshot_id: int,
    body: SnapshotSalePricePatch,
) -> dict[str, Any]:
    """
    Edita solo sale_price (AHORA). regular_price (ANTES) permanece congelado.
    """
    sale = body.sale_price.quantize(Decimal("0.01"))
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE app.promotion_price_snapshot
            SET
                sale_price = %s,
                precio_oferta = %s
            WHERE id = %s
            RETURNING
                id,
                promotion_id,
                barcode,
                company_id,
                price_list,
                COALESCE(regular_price, precio_normal) AS regular_price,
                sale_price,
                canal,
                fecha_generado
            """,
            (sale, sale, snapshot_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Snapshot no encontrado")
        conn.commit()
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e)) from e
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
