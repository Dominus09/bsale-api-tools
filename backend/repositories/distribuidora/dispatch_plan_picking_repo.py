"""Persistencia versionada de picking por plan de despacho."""

from __future__ import annotations

import json
from typing import Any

from psycopg2.extras import Json


def _cols(cur) -> list[str]:
    return [d[0] for d in cur.description]


def _row_dict(cur, row) -> dict[str, Any]:
    return dict(zip(_cols(cur), row))


def get_next_picking_version(cur, plan_id: int) -> int:
    cur.execute(
        """
        SELECT COALESCE(MAX(version), 0) + 1
        FROM distribuidora.dispatch_plan_pickings
        WHERE dispatch_plan_id = %s
        """,
        (plan_id,),
    )
    return int(cur.fetchone()[0])


def supersede_current_pickings(cur, plan_id: int) -> None:
    cur.execute(
        """
        UPDATE distribuidora.dispatch_plan_pickings
        SET is_current = FALSE,
            superseded_at = NOW()
        WHERE dispatch_plan_id = %s
          AND is_current = TRUE
        """,
        (plan_id,),
    )


def insert_picking(
    cur,
    *,
    plan_id: int,
    version: int,
    include_probable: bool,
    header: dict[str, Any],
    warnings: list[str],
    stops_count: int,
    product_lines_count: int,
    document_total_clp: float,
    product_total_monto_clp: float,
) -> int:
    cur.execute(
        """
        INSERT INTO distribuidora.dispatch_plan_pickings (
            dispatch_plan_id, version, is_current, include_probable,
            header, warnings, stops_count, product_lines_count,
            document_total_clp, product_total_monto_clp
        )
        VALUES (%s, %s, TRUE, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            plan_id,
            version,
            include_probable,
            Json(header),
            Json(warnings),
            stops_count,
            product_lines_count,
            document_total_clp,
            product_total_monto_clp,
        ),
    )
    return int(cur.fetchone()[0])


def insert_picking_clients(
    cur,
    *,
    picking_id: int,
    plan_id: int,
    clients: list[dict[str, Any]],
) -> None:
    for c in clients:
        cur.execute(
            """
            INSERT INTO distribuidora.dispatch_plan_picking_clients (
                picking_id, dispatch_plan_id, route_order, oc_document_id,
                related_document_id, client_id, client_name, fantasy_name,
                address, city, lat, lng, phone, document_number,
                document_type_label, payment_method, seller_name, observations,
                delivery_notes, stop_status,
                document_total, relation_source, inclusion,
                is_probable_included, probable_score
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                picking_id,
                plan_id,
                int(c.get("route_order") or 0),
                c.get("oc_document_id"),
                int(c["related_document_id"]),
                c.get("client_id"),
                c.get("client_name"),
                c.get("fantasy_name"),
                c.get("address"),
                c.get("city"),
                c.get("lat"),
                c.get("lng"),
                c.get("phone"),
                c.get("document_number"),
                c.get("document_type") or c.get("document_type_label"),
                c.get("payment_method"),
                c.get("seller_name"),
                c.get("observations"),
                c.get("delivery_notes"),
                c.get("stop_status") or "pending",
                c.get("document_total"),
                c.get("relation_source"),
                c.get("inclusion"),
                bool(c.get("is_probable_included")),
                c.get("probable_score"),
            ),
        )


def insert_picking_products(
    cur,
    *,
    picking_id: int,
    plan_id: int,
    items: list[dict[str, Any]],
) -> None:
    for idx, item in enumerate(items):
        producto = (item.get("producto") or "").strip()
        variante = (item.get("variante") or "").strip()
        prod_var = item.get("producto_variante") or ""
        if not prod_var:
            prod_var = (
                producto
                if producto == variante
                else f"{producto} — {variante}".strip(" —")
            )
        cur.execute(
            """
            INSERT INTO distribuidora.dispatch_plan_picking_products (
                picking_id, dispatch_plan_id, sort_order, sucursal_bodega,
                product_id, variant_id,
                tipo_producto, producto, variante, producto_variante,
                codigo_barras, unidades, cajas, units_per_box,
                sin_unidad_caja, total_monto
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                picking_id,
                plan_id,
                idx,
                item.get("sucursal_bodega"),
                item.get("product_id"),
                item.get("variant_id"),
                item.get("tipo_producto"),
                producto or prod_var,
                variante or producto,
                prod_var,
                item.get("codigo_barras"),
                item.get("unidades") or 0,
                item.get("cajas"),
                item.get("units_per_box"),
                bool(item.get("sin_unidad_caja")),
                item.get("total_monto") or 0,
            ),
        )


def get_picking_row(
    cur,
    plan_id: int,
    *,
    version: int | None = None,
    picking_id: int | None = None,
    current_only: bool = True,
) -> dict[str, Any] | None:
    if picking_id is not None:
        cur.execute(
            """
            SELECT *
            FROM distribuidora.dispatch_plan_pickings
            WHERE id = %s AND dispatch_plan_id = %s
            """,
            (picking_id, plan_id),
        )
    elif version is not None:
        cur.execute(
            """
            SELECT *
            FROM distribuidora.dispatch_plan_pickings
            WHERE dispatch_plan_id = %s AND version = %s
            """,
            (plan_id, version),
        )
    elif current_only:
        cur.execute(
            """
            SELECT *
            FROM distribuidora.dispatch_plan_pickings
            WHERE dispatch_plan_id = %s AND is_current = TRUE
            ORDER BY version DESC
            LIMIT 1
            """,
            (plan_id,),
        )
    else:
        return None
    row = cur.fetchone()
    if not row:
        return None
    return _row_dict(cur, row)


def list_picking_versions(cur, plan_id: int, *, limit: int = 30) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT
            id, dispatch_plan_id, version, is_current, include_probable,
            generated_at, superseded_at, stops_count, product_lines_count,
            document_total_clp, product_total_monto_clp
        FROM distribuidora.dispatch_plan_pickings
        WHERE dispatch_plan_id = %s
        ORDER BY version DESC
        LIMIT %s
        """,
        (plan_id, limit),
    )
    return [_row_dict(cur, r) for r in cur.fetchall()]


def list_picking_clients(cur, picking_id: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT *
        FROM distribuidora.dispatch_plan_picking_clients
        WHERE picking_id = %s
        ORDER BY route_order ASC, id ASC
        """,
        (picking_id,),
    )
    return [_row_dict(cur, r) for r in cur.fetchall()]


def list_picking_products(cur, picking_id: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT *
        FROM distribuidora.dispatch_plan_picking_products
        WHERE picking_id = %s
        ORDER BY sort_order ASC, id ASC
        """,
        (picking_id,),
    )
    return [_row_dict(cur, r) for r in cur.fetchall()]


def client_row_to_api(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row.get("id"),
        "route_order": row.get("route_order"),
        "oc_document_id": row.get("oc_document_id"),
        "city": row.get("city") or "",
        "client_id": row.get("client_id"),
        "client_name": row.get("client_name") or "",
        "fantasy_name": row.get("fantasy_name") or "",
        "address": row.get("address") or "",
        "lat": row.get("lat"),
        "lng": row.get("lng"),
        "phone": row.get("phone") or "",
        "document_number": row.get("document_number"),
        "document_type": row.get("document_type_label") or "",
        "payment_method": row.get("payment_method") or "",
        "seller_name": row.get("seller_name") or "",
        "observations": row.get("observations") or "",
        "delivery_notes": row.get("delivery_notes") or "",
        "stop_status": row.get("stop_status") or "pending",
        "document_total": row.get("document_total"),
        "related_document_id": row.get("related_document_id"),
        "relation_source": row.get("relation_source"),
        "inclusion": row.get("inclusion"),
        "is_probable_included": row.get("is_probable_included"),
        "probable_score": row.get("probable_score"),
    }


def product_row_to_api(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row.get("id"),
        "product_id": row.get("product_id"),
        "variant_id": row.get("variant_id"),
        "sucursal_bodega": row.get("sucursal_bodega") or "",
        "unidades": row.get("unidades"),
        "tipo_producto": row.get("tipo_producto") or "",
        "producto": row.get("producto"),
        "variante": row.get("variante"),
        "producto_variante": row.get("producto_variante") or "",
        "codigo_barras": row.get("codigo_barras"),
        "cajas": row.get("cajas"),
        "units_per_box": row.get("units_per_box"),
        "sin_unidad_caja": row.get("sin_unidad_caja"),
        "total_monto": row.get("total_monto"),
    }


def picking_meta_to_api(row: dict[str, Any]) -> dict[str, Any]:
    header = row.get("header")
    if isinstance(header, str):
        header = json.loads(header)
    warnings = row.get("warnings")
    if isinstance(warnings, str):
        warnings = json.loads(warnings)
    return {
        "picking_id": row["id"],
        "dispatch_plan_id": row["dispatch_plan_id"],
        "version": row["version"],
        "is_current": row.get("is_current"),
        "include_probable": row.get("include_probable"),
        "generated_at": row.get("generated_at"),
        "superseded_at": row.get("superseded_at"),
        "header": header if isinstance(header, dict) else {},
        "warnings": warnings if isinstance(warnings, list) else [],
        "stops_count": row.get("stops_count"),
        "product_lines_count": row.get("product_lines_count"),
        "document_total_clp": float(row.get("document_total_clp") or 0),
        "product_total_monto_clp": float(row.get("product_total_monto_clp") or 0),
    }
