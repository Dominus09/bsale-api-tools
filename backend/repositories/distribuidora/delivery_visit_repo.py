"""Persistencia P4: visitas de entrega y eventos."""

from __future__ import annotations

from typing import Any

from psycopg2.extras import Json


def _row(cur, r) -> dict[str, Any]:
    cols = [d[0] for d in cur.description]
    if isinstance(r, dict) or hasattr(r, "keys"):
        return dict(r)
    return dict(zip(cols, r))


def tables_ready(cur) -> bool:
    cur.execute(
        """
        SELECT (
            to_regclass('distribuidora.delivery_visits') IS NOT NULL
            AND to_regclass('distribuidora.delivery_visit_events') IS NOT NULL
        ) AS tables_ready
        """
    )
    row = cur.fetchone()
    if row is None:
        return False
    if isinstance(row, dict) or hasattr(row, "keys"):
        return bool(row["tables_ready"])
    return bool(row[0])


def get_active_visit(
    cur, *, plan_id: int, customer_key: str
) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT *
        FROM distribuidora.delivery_visits
        WHERE dispatch_plan_id = %s
          AND customer_key = %s
          AND status = 'in_progress'
        ORDER BY id DESC
        LIMIT 1
        """,
        (int(plan_id), customer_key),
    )
    r = cur.fetchone()
    return _row(cur, r) if r else None


def get_visit(cur, visit_id: int) -> dict[str, Any] | None:
    cur.execute(
        "SELECT * FROM distribuidora.delivery_visits WHERE id = %s",
        (int(visit_id),),
    )
    r = cur.fetchone()
    return _row(cur, r) if r else None


def get_visit_for_update(cur, visit_id: int) -> dict[str, Any] | None:
    """Bloqueo de fila para control de versión en mutaciones concurrentes."""
    cur.execute(
        """
        SELECT * FROM distribuidora.delivery_visits
        WHERE id = %s
        FOR UPDATE
        """,
        (int(visit_id),),
    )
    r = cur.fetchone()
    return _row(cur, r) if r else None


def find_by_idem(
    cur, *, driver_user_id: int, idempotency_key: str
) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT * FROM distribuidora.delivery_visits
        WHERE driver_user_id = %s AND idempotency_key = %s
        ORDER BY id DESC LIMIT 1
        """,
        (int(driver_user_id), idempotency_key),
    )
    r = cur.fetchone()
    return _row(cur, r) if r else None


def insert_visit(cur, **kw) -> dict[str, Any]:
    cur.execute(
        """
        INSERT INTO distribuidora.delivery_visits (
            company_id, dispatch_plan_id, customer_key, client_id, driver_user_id,
            status, start_lat, start_lng, start_accuracy_m, start_gps_captured_at,
            start_gps_source, gps_exception_code, gps_exception_note, idempotency_key
        ) VALUES (
            %(company_id)s, %(dispatch_plan_id)s, %(customer_key)s, %(client_id)s,
            %(driver_user_id)s, 'in_progress', %(start_lat)s, %(start_lng)s,
            %(start_accuracy_m)s, %(start_gps_captured_at)s, %(start_gps_source)s,
            %(gps_exception_code)s, %(gps_exception_note)s, %(idempotency_key)s
        )
        RETURNING *
        """,
        kw,
    )
    return _row(cur, cur.fetchone())


def finalize_visit(
    cur,
    visit_id: int,
    *,
    expected_version: int,
    status: str,
    result_reason: str | None,
    result_note: str | None,
    end_lat: float | None,
    end_lng: float | None,
    end_accuracy_m: float | None,
    end_gps_captured_at,
    end_gps_source: str | None,
    closure_review_status: str | None = "verified",
) -> dict[str, Any] | None:
    cur.execute(
        """
        UPDATE distribuidora.delivery_visits
        SET status = %s,
            result_reason = %s,
            result_note = %s,
            ended_at = NOW(),
            end_lat = %s,
            end_lng = %s,
            end_accuracy_m = %s,
            end_gps_captured_at = %s,
            end_gps_source = %s,
            closure_review_status = %s,
            version = version + 1,
            updated_at = NOW()
        WHERE id = %s
          AND status = 'in_progress'
          AND version = %s
        RETURNING *
        """,
        (
            status,
            result_reason,
            result_note,
            end_lat,
            end_lng,
            end_accuracy_m,
            end_gps_captured_at,
            end_gps_source,
            closure_review_status,
            int(visit_id),
            int(expected_version),
        ),
    )
    r = cur.fetchone()
    return _row(cur, r) if r else None


def insert_event(
    cur,
    *,
    visit_id: int,
    action: str,
    actor_user_id: int | None,
    payload: dict[str, Any] | None = None,
) -> int:
    cur.execute(
        """
        INSERT INTO distribuidora.delivery_visit_events (
            visit_id, action, actor_user_id, payload
        ) VALUES (%s, %s, %s, %s)
        RETURNING id
        """,
        (int(visit_id), action, actor_user_id, Json(payload or {})),
    )
    row = cur.fetchone()
    if isinstance(row, dict) or hasattr(row, "keys"):
        return int(row["id"])
    return int(row[0])


def upsert_document_result(cur, **kw) -> dict[str, Any]:
    cur.execute(
        """
        INSERT INTO distribuidora.delivery_document_results (
            visit_id, related_document_id, oc_document_id, document_number,
            document_type, document_total, result_status, notes
        ) VALUES (
            %(visit_id)s, %(related_document_id)s, %(oc_document_id)s,
            %(document_number)s, %(document_type)s, %(document_total)s,
            %(result_status)s, %(notes)s
        )
        ON CONFLICT (visit_id, related_document_id)
            WHERE related_document_id IS NOT NULL
        DO UPDATE SET
            oc_document_id = EXCLUDED.oc_document_id,
            document_number = EXCLUDED.document_number,
            document_type = EXCLUDED.document_type,
            document_total = EXCLUDED.document_total,
            result_status = EXCLUDED.result_status,
            notes = EXCLUDED.notes,
            updated_at = NOW()
        RETURNING *
        """,
        kw,
    )
    return _row(cur, cur.fetchone())


def replace_line_results(
    cur,
    *,
    visit_id: int,
    document_result_id: int,
    lines: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Upsert por source_detail_id. No borra líneas ya persistidas de otras details.

    Permite registro progresivo (1..N líneas) sin perder avances previos.
    """
    for line in lines:
        cur.execute(
            """
            INSERT INTO distribuidora.delivery_line_results (
                visit_id, document_result_id, source_detail_id, line_number,
                variant_id, sku_code, product_name,
                planned_units, delivered_units, undelivered_units,
                undelivered_reason, undelivered_note
            ) VALUES (
                %(visit_id)s, %(document_result_id)s, %(source_detail_id)s,
                %(line_number)s, %(variant_id)s, %(sku_code)s,
                %(product_name)s, %(planned_units)s, %(delivered_units)s,
                %(undelivered_units)s, %(undelivered_reason)s, %(undelivered_note)s
            )
            ON CONFLICT (document_result_id, source_detail_id)
                WHERE source_detail_id IS NOT NULL
            DO UPDATE SET
                line_number = EXCLUDED.line_number,
                variant_id = EXCLUDED.variant_id,
                sku_code = EXCLUDED.sku_code,
                product_name = EXCLUDED.product_name,
                planned_units = EXCLUDED.planned_units,
                delivered_units = EXCLUDED.delivered_units,
                undelivered_units = EXCLUDED.undelivered_units,
                undelivered_reason = EXCLUDED.undelivered_reason,
                undelivered_note = EXCLUDED.undelivered_note
            """,
            {
                "visit_id": int(visit_id),
                "document_result_id": int(document_result_id),
                "source_detail_id": (
                    int(line["source_detail_id"])
                    if line.get("source_detail_id") is not None
                    else None
                ),
                "line_number": (
                    int(line["line_number"])
                    if line.get("line_number") is not None
                    else None
                ),
                "variant_id": line.get("variant_id"),
                "sku_code": line.get("sku_code"),
                "product_name": line.get("product_name"),
                "planned_units": line.get("planned_units"),
                "delivered_units": line.get("delivered_units"),
                "undelivered_units": line.get("undelivered_units"),
                "undelivered_reason": line.get("undelivered_reason"),
                "undelivered_note": line.get("undelivered_note"),
            },
        )
    return list_line_results_for_document(cur, int(document_result_id))


def list_line_results_for_document(cur, document_result_id: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT * FROM distribuidora.delivery_line_results
        WHERE document_result_id = %s
        ORDER BY id
        """,
        (int(document_result_id),),
    )
    rows = cur.fetchall() or []
    return [_row(cur, r) for r in rows]


def update_document_result_status(
    cur, *, document_result_id: int, result_status: str, notes: str | None = None
) -> dict[str, Any]:
    cur.execute(
        """
        UPDATE distribuidora.delivery_document_results
        SET result_status = %s,
            notes = COALESCE(%s, notes),
            updated_at = NOW()
        WHERE id = %s
        RETURNING *
        """,
        (result_status, notes, int(document_result_id)),
    )
    return _row(cur, cur.fetchone())


def list_document_results(cur, visit_id: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT * FROM distribuidora.delivery_document_results
        WHERE visit_id = %s
        ORDER BY id
        """,
        (int(visit_id),),
    )
    rows = cur.fetchall() or []
    return [_row(cur, r) for r in rows]


def list_line_results(cur, visit_id: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT * FROM distribuidora.delivery_line_results
        WHERE visit_id = %s
        ORDER BY id
        """,
        (int(visit_id),),
    )
    rows = cur.fetchall() or []
    return [_row(cur, r) for r in rows]


def insert_collection(cur, **kw) -> dict[str, Any]:
    cur.execute(
        """
        INSERT INTO distribuidora.delivery_collections (
            visit_id, document_result_id, method, document_amount,
            declared_amount, verified_amount, pending_balance, note,
            created_by_user_id
        ) VALUES (
            %(visit_id)s, %(document_result_id)s, %(method)s, %(document_amount)s,
            %(declared_amount)s, %(verified_amount)s, %(pending_balance)s,
            %(note)s, %(created_by_user_id)s
        )
        RETURNING *
        """,
        kw,
    )
    return _row(cur, cur.fetchone())


def delete_collections_for_document(
    cur, *, visit_id: int, document_result_id: int
) -> int:
    cur.execute(
        """
        DELETE FROM distribuidora.delivery_collections
        WHERE visit_id = %s AND document_result_id = %s
        """,
        (int(visit_id), int(document_result_id)),
    )
    return int(cur.rowcount or 0)


def list_collections(cur, visit_id: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT * FROM distribuidora.delivery_collections
        WHERE visit_id = %s
        ORDER BY id
        """,
        (int(visit_id),),
    )
    rows = cur.fetchall() or []
    return [_row(cur, r) for r in rows]


def insert_report_snapshot(cur, **kw) -> dict[str, Any]:
    cur.execute(
        """
        INSERT INTO distribuidora.delivery_report_snapshots (
            visit_id, report_text, payload, generated_by_user_id
        ) VALUES (
            %(visit_id)s, %(report_text)s, %(payload)s, %(generated_by_user_id)s
        )
        RETURNING *
        """,
        {
            **kw,
            "payload": Json(kw.get("payload") or {}),
        },
    )
    return _row(cur, cur.fetchone())


def count_evidence(cur, visit_id: int) -> int:
    cur.execute(
        """
        SELECT COUNT(*) AS n
        FROM distribuidora.delivery_evidence
        WHERE visit_id = %s AND upload_confirmed = TRUE
        """,
        (int(visit_id),),
    )
    row = cur.fetchone()
    if row is None:
        return 0
    if isinstance(row, dict) or hasattr(row, "keys"):
        return int(row["n"] or 0)
    return int(row[0] or 0)


def get_conformity(cur, visit_id: int) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT * FROM distribuidora.delivery_conformity
        WHERE visit_id = %s
        """,
        (int(visit_id),),
    )
    r = cur.fetchone()
    return _row(cur, r) if r else None


def list_recent_visits(
    cur, *, limit: int = 100, company_id: int | None = None
) -> list[dict[str, Any]]:
    if company_id is not None:
        cur.execute(
            """
            SELECT v.*,
                   (
                     SELECT COALESCE(SUM(c.declared_amount), 0)
                     FROM distribuidora.delivery_collections c
                     WHERE c.visit_id = v.id
                   ) AS declared_collected,
                   (
                     SELECT COALESCE(SUM(c.pending_balance), 0)
                     FROM distribuidora.delivery_collections c
                     WHERE c.visit_id = v.id
                   ) AS pending_balance_sum
            FROM distribuidora.delivery_visits v
            WHERE v.company_id = %s
            ORDER BY v.started_at DESC, v.id DESC
            LIMIT %s
            """,
            (int(company_id), int(limit)),
        )
    else:
        cur.execute(
            """
            SELECT v.*,
                   (
                     SELECT COALESCE(SUM(c.declared_amount), 0)
                     FROM distribuidora.delivery_collections c
                     WHERE c.visit_id = v.id
                   ) AS declared_collected,
                   (
                     SELECT COALESCE(SUM(c.pending_balance), 0)
                     FROM distribuidora.delivery_collections c
                     WHERE c.visit_id = v.id
                   ) AS pending_balance_sum
            FROM distribuidora.delivery_visits v
            ORDER BY v.started_at DESC, v.id DESC
            LIMIT %s
            """,
            (int(limit),),
        )
    rows = cur.fetchall() or []
    return [_row(cur, r) for r in rows]


def insert_evidence(cur, **kw) -> dict[str, Any]:
    cur.execute(
        """
        INSERT INTO distribuidora.delivery_evidence (
            visit_id, company_id, evidence_kind, object_key, content_type,
            size_bytes, sha256, captured_by_user_id, capture_lat, capture_lng,
            capture_accuracy_m, upload_confirmed, validation_status,
            exception_note, idempotency_key, visit_event_id
        ) VALUES (
            %(visit_id)s, %(company_id)s, %(evidence_kind)s, %(object_key)s,
            %(content_type)s, %(size_bytes)s, %(sha256)s, %(captured_by_user_id)s,
            %(capture_lat)s, %(capture_lng)s, %(capture_accuracy_m)s,
            %(upload_confirmed)s, %(validation_status)s, %(exception_note)s,
            %(idempotency_key)s, %(visit_event_id)s
        )
        RETURNING *
        """,
        kw,
    )
    return _row(cur, cur.fetchone())


def find_evidence_by_idem(
    cur, *, user_id: int, idempotency_key: str
) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT * FROM distribuidora.delivery_evidence
        WHERE captured_by_user_id = %s AND idempotency_key = %s
        ORDER BY id DESC LIMIT 1
        """,
        (int(user_id), idempotency_key),
    )
    r = cur.fetchone()
    return _row(cur, r) if r else None


def list_evidence(cur, visit_id: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT * FROM distribuidora.delivery_evidence
        WHERE visit_id = %s
        ORDER BY id
        """,
        (int(visit_id),),
    )
    return [_row(cur, r) for r in (cur.fetchall() or [])]


def confirm_evidence(cur, evidence_id: int, **kw) -> dict[str, Any] | None:
    cur.execute(
        """
        UPDATE distribuidora.delivery_evidence
        SET upload_confirmed = TRUE,
            size_bytes = COALESCE(%(size_bytes)s, size_bytes),
            sha256 = COALESCE(%(sha256)s, sha256),
            content_type = COALESCE(%(content_type)s, content_type),
            validation_status = %(validation_status)s,
            visit_event_id = COALESCE(%(visit_event_id)s, visit_event_id),
            updated_at = NOW()
        WHERE id = %(evidence_id)s
        RETURNING *
        """,
        {"evidence_id": int(evidence_id), **kw},
    )
    r = cur.fetchone()
    return _row(cur, r) if r else None


def upsert_conformity(cur, **kw) -> dict[str, Any]:
    cur.execute(
        """
        INSERT INTO distribuidora.delivery_conformity (
            visit_id, receptor_name, signature_object_key, signature_content_type,
            accepted_summary_sha256, summary_payload, signed_at, refused,
            refuse_reason, gps_lat, gps_lng, gps_accuracy_m, visit_version_at_sign
        ) VALUES (
            %(visit_id)s, %(receptor_name)s, %(signature_object_key)s,
            %(signature_content_type)s, %(accepted_summary_sha256)s,
            %(summary_payload)s, %(signed_at)s, %(refused)s, %(refuse_reason)s,
            %(gps_lat)s, %(gps_lng)s, %(gps_accuracy_m)s, %(visit_version_at_sign)s
        )
        ON CONFLICT (visit_id) DO UPDATE SET
            receptor_name = EXCLUDED.receptor_name,
            signature_object_key = EXCLUDED.signature_object_key,
            signature_content_type = EXCLUDED.signature_content_type,
            accepted_summary_sha256 = EXCLUDED.accepted_summary_sha256,
            summary_payload = EXCLUDED.summary_payload,
            signed_at = EXCLUDED.signed_at,
            refused = EXCLUDED.refused,
            refuse_reason = EXCLUDED.refuse_reason,
            gps_lat = EXCLUDED.gps_lat,
            gps_lng = EXCLUDED.gps_lng,
            gps_accuracy_m = EXCLUDED.gps_accuracy_m,
            visit_version_at_sign = EXCLUDED.visit_version_at_sign,
            invalidated_at = NULL,
            invalidate_reason = NULL,
            updated_at = NOW()
        RETURNING *
        """,
        {
            **kw,
            "summary_payload": Json(kw.get("summary_payload") or {}),
        },
    )
    return _row(cur, cur.fetchone())


def invalidate_conformity(
    cur, visit_id: int, *, reason: str
) -> dict[str, Any] | None:
    cur.execute(
        """
        UPDATE distribuidora.delivery_conformity
        SET invalidated_at = NOW(),
            invalidate_reason = %s,
            updated_at = NOW()
        WHERE visit_id = %s
          AND invalidated_at IS NULL
        RETURNING *
        """,
        (reason, int(visit_id)),
    )
    r = cur.fetchone()
    return _row(cur, r) if r else None


def bump_visit_version(cur, visit_id: int) -> dict[str, Any] | None:
    cur.execute(
        """
        UPDATE distribuidora.delivery_visits
        SET version = version + 1, updated_at = NOW()
        WHERE id = %s AND status = 'in_progress'
        RETURNING *
        """,
        (int(visit_id),),
    )
    r = cur.fetchone()
    return _row(cur, r) if r else None
