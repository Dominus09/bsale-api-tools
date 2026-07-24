from typing import Any, Dict, List, Optional

from fastapi import APIRouter

from backend.db import get_connection
from backend.services.price_list_control_service import (
    list_price_list_control_rows,
    summarize_price_list_control,
)

router = APIRouter()


def _dict_executor(conn):
    cur = conn.cursor()

    def execute(sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        cur.execute(sql, params)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in rows]

    execute._cursor = cur  # type: ignore[attr-defined]
    return execute


def _fetch_margin_analysis_view_rows(
    company_id: int,
    price_list_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Legacy: lee bsale.margin_analysis_view (costo actual × tax_factor).

    Preferir GET /price-list-control para semántica canónica de /margins.
    """
    conn = get_connection()
    cur = conn.cursor()

    sql = """
        SELECT
            company_id,
            product_type_id,
            product_type_name,
            product_name,
            variant_id,
            variant_name,
            barcode,
            sku,
            price_list_id,
            price_list_name,
            stock_quantity,
            price,
            cost,
            margin_value,
            margin_percent,
            min_margin_percent,
            margin_diff,
            status
        FROM bsale.margin_analysis_view
        WHERE company_id = %s
    """
    params: list = [company_id]
    if price_list_id is not None:
        sql += " AND price_list_id = %s"
        params.append(price_list_id)
    sql += """
        ORDER BY margin_diff ASC NULLS LAST, margin_percent ASC NULLS LAST,
                 price_list_id, variant_id
    """

    cur.execute(sql, params)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    result = [dict(zip(columns, row)) for row in rows]

    cur.close()
    conn.close()

    return result


@router.get("/price-list-control")
def price_list_control(
    company_id: int,
    price_list_id: Optional[int] = None,
):
    """Control actual de precios por lista (variante × lista).

    Costo de referencia = máximo bruto válido de recepción (fallback variant_cost).
    Métrica de cumplimiento = recargo % sobre costo (actual_markup_pct).
    Sin ventas ni unidades vendidas.
    """
    conn = get_connection()
    execute = _dict_executor(conn)
    try:
        rows = list_price_list_control_rows(
            execute,
            company_id=company_id,
            price_list_id=price_list_id,
        )
        summary = summarize_price_list_control(rows)
        return {"items": rows, "summary": summary}
    finally:
        execute._cursor.close()  # type: ignore[attr-defined]
        conn.close()


@router.get("/margin-analysis-view")
def margin_analysis_view(
    company_id: int,
    price_list_id: Optional[int] = None,
):
    """
    Vista bsale.margin_analysis_view (definición en backend/sql/margin_analysis_view.sql).
    Sin price_list_id devuelve todas las listas de la empresa.

    Nota: la vista legacy usa costo actual × tax_factor. Para /margins usar
    GET /price-list-control.
    """
    return _fetch_margin_analysis_view_rows(company_id, price_list_id)


@router.get("/margin-analysis")
def margin_analysis(
    company_id: int,
    price_list_id: Optional[int] = None,
):
    """
    Por defecto: bsale.erp_margin_dashboard (compatibilidad con el frontend).
    Con price_list_id: misma filas que la vista SQL (análisis por lista).
    """
    if price_list_id is not None:
        return _fetch_margin_analysis_view_rows(company_id, price_list_id)

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            product_name,
            bar_code,
            price_list_name,
            product_type_name,
            cost_gross,
            price_gross,
            margin_percent,
            min_margin,
            max_margin,
            margin_status,
            suggested_price_min,
            suggested_price_max
        FROM bsale.erp_margin_dashboard
        WHERE company_id = %s
        ORDER BY margin_percent ASC
        """,
        (company_id,),
    )

    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    result = [dict(zip(columns, row)) for row in rows]

    cur.close()
    conn.close()

    return result
