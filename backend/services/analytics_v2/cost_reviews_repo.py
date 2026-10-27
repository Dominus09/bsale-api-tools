"""Persistencia de revisiones humanas de costos (esquema app)."""

from __future__ import annotations

from typing import Any

from backend.db import get_connection


OPEN_STATUSES = ("pendiente", "en_revision")

SORT_SQL = {
    "updated_at_desc": "updated_at DESC, id DESC",
    "updated_at_asc": "updated_at ASC, id ASC",
    "started_at_desc": "started_at DESC NULLS LAST, id DESC",
    "priority_desc": (
        "CASE priority WHEN 'alta' THEN 0 WHEN 'media' THEN 1 ELSE 2 END, updated_at DESC"
    ),
    "status": (
        "CASE status WHEN 'en_revision' THEN 0 WHEN 'pendiente' THEN 1 "
        "WHEN 'resuelto' THEN 2 ELSE 3 END, updated_at DESC"
    ),
    "product_name": "COALESCE(product_name, '') ASC, id ASC",
}


class CostReviewsRepo:
    def table_ready(self) -> bool:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'app' AND table_name = 'cost_reviews'
                LIMIT 1
                """
            )
            ok = cur.fetchone() is not None
            cur.close()
            return ok
        finally:
            conn.close()

    def get(self, *, company_id: int, review_id: int) -> dict[str, Any] | None:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT * FROM app.cost_reviews
                WHERE id = %s AND company_id = %s
                LIMIT 1
                """,
                (review_id, company_id),
            )
            row = cur.fetchone()
            if not row:
                cur.close()
                return None
            cols = [d[0] for d in cur.description]
            cur.close()
            return dict(zip(cols, row))
        finally:
            conn.close()

    def list_reviews(
        self,
        *,
        company_id: int,
        status: str | None,
        q: str | None,
        assignee_user_id: int | None,
        priority: str | None,
        sort: str,
        page: int,
        page_size: int,
    ) -> tuple[list[dict[str, Any]], int]:
        clauses = ["company_id = %s"]
        params: list[Any] = [company_id]
        if status:
            clauses.append("status = %s")
            params.append(status)
        if priority:
            clauses.append("priority = %s")
            params.append(priority)
        if assignee_user_id is not None:
            clauses.append("assignee_user_id = %s")
            params.append(assignee_user_id)
        if q and q.strip():
            clauses.append(
                "(COALESCE(product_name,'') ILIKE %s OR COALESCE(sku_code,'') ILIKE %s "
                "OR COALESCE(barcode,'') ILIKE %s OR CAST(variant_id AS text) = %s "
                "OR COALESCE(reason_label,'') ILIKE %s)"
            )
            like = f"%{q.strip()}%"
            params.extend([like, like, like, q.strip(), like])
        where = " AND ".join(clauses)
        order = SORT_SQL.get(sort, SORT_SQL["status"])
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM app.cost_reviews WHERE {where}", params)
            total = int((cur.fetchone() or [0])[0] or 0)
            offset = max(0, (page - 1) * page_size)
            cur.execute(
                f"""
                SELECT * FROM app.cost_reviews
                WHERE {where}
                ORDER BY {order}
                LIMIT %s OFFSET %s
                """,
                [*params, page_size, offset],
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            cur.close()
            return rows, total
        finally:
            conn.close()

    def find_open(
        self, *, company_id: int, variant_id: int, anomaly_code: str
    ) -> dict[str, Any] | None:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT * FROM app.cost_reviews
                WHERE company_id = %s AND variant_id = %s AND anomaly_code = %s
                  AND status = ANY(%s)
                LIMIT 1
                """,
                (company_id, variant_id, anomaly_code, list(OPEN_STATUSES)),
            )
            row = cur.fetchone()
            if not row:
                cur.close()
                return None
            cols = [d[0] for d in cur.description]
            cur.close()
            return dict(zip(cols, row))
        finally:
            conn.close()

    def insert_review_with_event(
        self, values: dict[str, Any], event: dict[str, Any]
    ) -> dict[str, Any]:
        """Inserta caso + evento en una sola transacción."""
        cols = list(values.keys())
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                f"""
                INSERT INTO app.cost_reviews ({', '.join(cols)})
                VALUES ({', '.join(['%s'] * len(cols))})
                RETURNING *
                """,
                [values[c] for c in cols],
            )
            row = cur.fetchone()
            colnames = [d[0] for d in cur.description]
            review = dict(zip(colnames, row))
            event_payload = {**event, "review_id": int(review["id"])}
            ev_cols = list(event_payload.keys())
            cur.execute(
                f"""
                INSERT INTO app.cost_review_events ({', '.join(ev_cols)})
                VALUES ({', '.join(['%s'] * len(ev_cols))})
                """,
                [event_payload[c] for c in ev_cols],
            )
            conn.commit()
            cur.close()
            return review
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def update_review_with_event(
        self,
        *,
        company_id: int,
        review_id: int,
        sets: dict[str, Any],
        event: dict[str, Any],
        expected_status: str | None = None,
        expected_assignee_null: bool = False,
    ) -> dict[str, Any] | None:
        """Actualiza caso + inserta evento en la misma transacción (o rollback total)."""
        if not sets:
            return self.get(company_id=company_id, review_id=review_id)
        set_clauses = [f"{k} = %s" for k in sets]
        params: list[Any] = list(sets.values())
        where = ["id = %s", "company_id = %s"]
        params.extend([review_id, company_id])
        if expected_status is not None:
            where.append("status = %s")
            params.append(expected_status)
        if expected_assignee_null:
            where.append("assignee_user_id IS NULL")
        set_clauses.append("updated_at = now()")
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                f"""
                UPDATE app.cost_reviews
                SET {', '.join(set_clauses)}
                WHERE {' AND '.join(where)}
                RETURNING *
                """,
                params,
            )
            row = cur.fetchone()
            if not row:
                conn.rollback()
                cur.close()
                return None
            colnames = [d[0] for d in cur.description]
            review = dict(zip(colnames, row))
            event_payload = {**event, "review_id": int(review["id"])}
            ev_cols = list(event_payload.keys())
            cur.execute(
                f"""
                INSERT INTO app.cost_review_events ({', '.join(ev_cols)})
                VALUES ({', '.join(['%s'] * len(ev_cols))})
                """,
                [event_payload[c] for c in ev_cols],
            )
            conn.commit()
            cur.close()
            return review
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def list_events(self, *, review_id: int, company_id: int) -> list[dict[str, Any]]:
        """Eventos solo si el caso pertenece a la empresa (aislamiento)."""
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT e.*
                FROM app.cost_review_events e
                INNER JOIN app.cost_reviews r ON r.id = e.review_id
                WHERE e.review_id = %s AND r.company_id = %s
                ORDER BY e.created_at ASC, e.id ASC
                """,
                (review_id, company_id),
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            cur.close()
            return rows
        finally:
            conn.close()
