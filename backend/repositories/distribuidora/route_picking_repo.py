"""Picking por cliente a partir de planificación + OC enriquecida + atributos (vía vista)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from backend.db import get_connection


def refresh_and_list_route_picking(
    planning_date: date,
    truck: str,
) -> tuple[list[dict[str, Any]], int, Decimal]:
    """Regenera filas de picking para el camión/fecha y devuelve lista ordenada + agregados."""
    t = truck.strip()
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            DELETE FROM distribuidora.route_picking pk
            WHERE pk.planning_id IN (
                SELECT rp.id
                FROM distribuidora.route_planning rp
                WHERE rp.planning_date = %s
                  AND rp.truck = %s
            )
            """,
            (planning_date, t),
        )
        cur.execute(
            """
            INSERT INTO distribuidora.route_picking (
                planning_id,
                document_id,
                oc_number,
                client_name,
                address,
                city,
                phone,
                document_number,
                payment_method,
                observations,
                seller,
                total_amount,
                created_at
            )
            SELECT
                rp.id,
                rp.document_id,
                COALESCE(rp.oc_number, v.number),
                COALESCE(NULLIF(BTRIM(rp.client_name), ''), v.nombre_fantasia),
                COALESCE(NULLIF(BTRIM(rp.address), ''), v.address),
                COALESCE(
                    NULLIF(BTRIM(v.city), ''),
                    NULLIF(BTRIM(v.municipality), ''),
                    rp.municipality
                ),
                NULLIF(BTRIM(cl.phone), ''),
                v.tipo_documento_a_generar,
                v.forma_pago,
                v.observaciones,
                COALESCE(
                    NULLIF(BTRIM(v.seller_name), ''),
                    NULLIF(
                        BTRIM(
                            COALESCE(u.first_name, '')
                            || ' '
                            || COALESCE(u.last_name, '')
                        ),
                        ''
                    ),
                    CASE
                        WHEN v.user_id IS NULL THEN NULL
                        ELSE 'Usuario ' || v.user_id::text
                    END
                ),
                COALESCE(rp.total_amount, v.total_amount),
                NOW()
            FROM distribuidora.route_planning rp
            INNER JOIN distribuidora.v_orders_purchase_enriched v
                ON v.document_id = rp.document_id
            LEFT JOIN bsale.clients cl
                ON cl.company_id = 3
               AND cl.bsale_id = v.client_id
            LEFT JOIN bsale.bsale_users u
                ON u.company_id = 3
               AND u.bsale_id = v.user_id
            WHERE rp.planning_date = %s
              AND rp.truck = %s
            """,
            (planning_date, t),
        )
        cur.execute(
            """
            SELECT
                pk.id,
                pk.planning_id,
                pk.document_id,
                pk.oc_number,
                pk.client_name,
                pk.address,
                pk.city,
                pk.phone,
                pk.document_number,
                pk.payment_method,
                pk.observations,
                pk.seller,
                pk.total_amount,
                pk.created_at
            FROM distribuidora.route_picking pk
            INNER JOIN distribuidora.route_planning rp ON rp.id = pk.planning_id
            WHERE rp.planning_date = %s
              AND rp.truck = %s
            ORDER BY
                COALESCE(pk.client_name, '') ASC,
                pk.document_id ASC
            """,
            (planning_date, t),
        )
        cols = [d[0] for d in cur.description]
        items = [dict(zip(cols, row)) for row in cur.fetchall()]

        cur.execute(
            """
            SELECT
                COUNT(DISTINCT rp.client_id) FILTER (WHERE rp.client_id IS NOT NULL),
                COALESCE(SUM(pk.total_amount), 0)
            FROM distribuidora.route_picking pk
            INNER JOIN distribuidora.route_planning rp ON rp.id = pk.planning_id
            WHERE rp.planning_date = %s
              AND rp.truck = %s
            """,
            (planning_date, t),
        )
        n_clients, total_amt = cur.fetchone()
        conn.commit()
        return (
            items,
            int(n_clients or 0),
            total_amt if total_amt is not None else Decimal("0"),
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
