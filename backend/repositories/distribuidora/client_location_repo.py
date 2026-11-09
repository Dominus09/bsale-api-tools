"""Persistencia P3: propuestas GPS, fotos fachada, ubicación verificada y auditoría."""

from __future__ import annotations

import json
from typing import Any

from psycopg2.extras import Json


def _row(cur, r) -> dict[str, Any]:
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, r))


def tables_ready(cur) -> bool:
    cur.execute(
        """
        SELECT to_regclass('distribuidora.client_location_proposals') IS NOT NULL
           AND to_regclass('distribuidora.client_facade_photos') IS NOT NULL
           AND to_regclass('distribuidora.client_verified_locations') IS NOT NULL
        """
    )
    return bool(cur.fetchone()[0])


def get_verified_location(cur, client_id: int) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT client_id, company_id, latitude, longitude, accuracy_m, source,
               approved_proposal_id, approved_by_user_id, approved_at, updated_at
        FROM distribuidora.client_verified_locations
        WHERE client_id = %s
        """,
        (int(client_id),),
    )
    r = cur.fetchone()
    return _row(cur, r) if r else None


def get_pending_proposal(cur, client_id: int) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT *
        FROM distribuidora.client_location_proposals
        WHERE client_id = %s AND status = 'pending'
        ORDER BY id DESC
        LIMIT 1
        """,
        (int(client_id),),
    )
    r = cur.fetchone()
    return _row(cur, r) if r else None


def get_proposal(cur, proposal_id: int) -> dict[str, Any] | None:
    cur.execute(
        "SELECT * FROM distribuidora.client_location_proposals WHERE id = %s",
        (int(proposal_id),),
    )
    r = cur.fetchone()
    return _row(cur, r) if r else None


def find_proposal_by_idem(
    cur, *, user_id: int, idempotency_key: str
) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT * FROM distribuidora.client_location_proposals
        WHERE proposed_by_user_id = %s AND idempotency_key = %s
        ORDER BY id DESC LIMIT 1
        """,
        (int(user_id), idempotency_key),
    )
    r = cur.fetchone()
    return _row(cur, r) if r else None


def insert_proposal(cur, **kw) -> dict[str, Any]:
    cur.execute(
        """
        INSERT INTO distribuidora.client_location_proposals (
            client_id, company_id, dispatch_plan_id, customer_key,
            proposed_lat, proposed_lng, accuracy_m, captured_at,
            previous_lat, previous_lng, previous_source, distance_from_previous_m,
            proposed_by_user_id, status, facade_photo_id, idempotency_key
        ) VALUES (
            %(client_id)s, %(company_id)s, %(dispatch_plan_id)s, %(customer_key)s,
            %(proposed_lat)s, %(proposed_lng)s, %(accuracy_m)s, COALESCE(%(captured_at)s, NOW()),
            %(previous_lat)s, %(previous_lng)s, %(previous_source)s, %(distance_from_previous_m)s,
            %(proposed_by_user_id)s, 'pending', %(facade_photo_id)s, %(idempotency_key)s
        )
        RETURNING *
        """,
        kw,
    )
    return _row(cur, cur.fetchone())


def supersede_pending(cur, client_id: int, *, except_id: int | None = None) -> int:
    cur.execute(
        """
        UPDATE distribuidora.client_location_proposals
        SET status = 'superseded', updated_at = NOW()
        WHERE client_id = %s AND status = 'pending'
          AND (%s::bigint IS NULL OR id <> %s)
        """,
        (int(client_id), except_id, except_id),
    )
    return int(cur.rowcount or 0)


def review_proposal(
    cur,
    proposal_id: int,
    *,
    status: str,
    reviewer_user_id: int,
    note: str | None,
) -> dict[str, Any] | None:
    cur.execute(
        """
        UPDATE distribuidora.client_location_proposals
        SET status = %s,
            reviewed_by_user_id = %s,
            reviewed_at = NOW(),
            review_note = %s,
            updated_at = NOW()
        WHERE id = %s AND status = 'pending'
        RETURNING *
        """,
        (status, int(reviewer_user_id), (note or "").strip() or None, int(proposal_id)),
    )
    r = cur.fetchone()
    return _row(cur, r) if r else None


def upsert_verified_location(cur, **kw) -> dict[str, Any]:
    cur.execute(
        """
        INSERT INTO distribuidora.client_verified_locations (
            client_id, company_id, latitude, longitude, accuracy_m, source,
            approved_proposal_id, approved_by_user_id, approved_at, updated_at
        ) VALUES (
            %(client_id)s, %(company_id)s, %(latitude)s, %(longitude)s, %(accuracy_m)s,
            %(source)s, %(approved_proposal_id)s, %(approved_by_user_id)s, NOW(), NOW()
        )
        ON CONFLICT (client_id) DO UPDATE SET
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            accuracy_m = EXCLUDED.accuracy_m,
            source = EXCLUDED.source,
            approved_proposal_id = EXCLUDED.approved_proposal_id,
            approved_by_user_id = EXCLUDED.approved_by_user_id,
            approved_at = NOW(),
            updated_at = NOW()
        RETURNING *
        """,
        kw,
    )
    return _row(cur, cur.fetchone())


def list_pending_proposals(cur, *, limit: int = 100) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT p.*,
               ph.object_key AS facade_object_key,
               ph.validation_status AS facade_validation_status
        FROM distribuidora.client_location_proposals p
        LEFT JOIN distribuidora.client_facade_photos ph ON ph.id = p.facade_photo_id
        WHERE p.status = 'pending'
        ORDER BY p.created_at ASC, p.id ASC
        LIMIT %s
        """,
        (int(limit),),
    )
    return [_row(cur, r) for r in cur.fetchall()]


def list_history(cur, client_id: int, *, limit: int = 50) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT * FROM distribuidora.client_location_audit
        WHERE client_id = %s
        ORDER BY created_at DESC, id DESC
        LIMIT %s
        """,
        (int(client_id), int(limit)),
    )
    rows = []
    for r in cur.fetchall():
        d = _row(cur, r)
        if isinstance(d.get("payload"), str):
            d["payload"] = json.loads(d["payload"])
        rows.append(d)
    return rows


def insert_audit(cur, **kw) -> int:
    cur.execute(
        """
        INSERT INTO distribuidora.client_location_audit (
            client_id, entity_type, entity_id, action, actor_user_id,
            dispatch_plan_id, payload
        ) VALUES (
            %(client_id)s, %(entity_type)s, %(entity_id)s, %(action)s, %(actor_user_id)s,
            %(dispatch_plan_id)s, %(payload)s
        )
        RETURNING id
        """,
        {
            **kw,
            "payload": Json(kw.get("payload") or {}),
        },
    )
    return int(cur.fetchone()[0])


def get_current_facade(cur, client_id: int) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT * FROM distribuidora.client_facade_photos
        WHERE client_id = %s
          AND (
            (is_current AND validation_status = 'approved')
            OR validation_status IN ('pending', 'upload_pending')
          )
        ORDER BY
          CASE WHEN is_current AND validation_status = 'approved' THEN 0
               WHEN validation_status = 'pending' THEN 1
               ELSE 2 END,
          id DESC
        LIMIT 1
        """,
        (int(client_id),),
    )
    r = cur.fetchone()
    return _row(cur, r) if r else None


def get_facade_photo(cur, photo_id: int) -> dict[str, Any] | None:
    cur.execute(
        "SELECT * FROM distribuidora.client_facade_photos WHERE id = %s",
        (int(photo_id),),
    )
    r = cur.fetchone()
    return _row(cur, r) if r else None


def find_facade_by_idem(
    cur, *, user_id: int, idempotency_key: str
) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT * FROM distribuidora.client_facade_photos
        WHERE captured_by_user_id = %s AND idempotency_key = %s
        ORDER BY id DESC LIMIT 1
        """,
        (int(user_id), idempotency_key),
    )
    r = cur.fetchone()
    return _row(cur, r) if r else None


def insert_facade_photo(cur, **kw) -> dict[str, Any]:
    cur.execute(
        """
        INSERT INTO distribuidora.client_facade_photos (
            client_id, company_id, dispatch_plan_id, customer_key,
            object_key, content_type, size_bytes, sha256, width, height,
            captured_by_user_id, validation_status, upload_confirmed, idempotency_key
        ) VALUES (
            %(client_id)s, %(company_id)s, %(dispatch_plan_id)s, %(customer_key)s,
            %(object_key)s, %(content_type)s, %(size_bytes)s, %(sha256)s, %(width)s, %(height)s,
            %(captured_by_user_id)s, %(validation_status)s, %(upload_confirmed)s, %(idempotency_key)s
        )
        RETURNING *
        """,
        kw,
    )
    return _row(cur, cur.fetchone())


def confirm_facade_upload(cur, photo_id: int, *, size_bytes: int | None = None) -> dict[str, Any] | None:
    cur.execute(
        """
        UPDATE distribuidora.client_facade_photos
        SET upload_confirmed = TRUE,
            validation_status = CASE
                WHEN validation_status = 'upload_pending' THEN 'pending'
                ELSE validation_status
            END,
            size_bytes = COALESCE(%s, size_bytes),
            updated_at = NOW()
        WHERE id = %s
        RETURNING *
        """,
        (size_bytes, int(photo_id)),
    )
    r = cur.fetchone()
    return _row(cur, r) if r else None


def review_facade(
    cur,
    photo_id: int,
    *,
    status: str,
    reviewer_user_id: int,
    note: str | None,
    make_current: bool,
) -> dict[str, Any] | None:
    if make_current and status == "approved":
        cur.execute(
            """
            SELECT client_id FROM distribuidora.client_facade_photos WHERE id = %s
            """,
            (int(photo_id),),
        )
        row = cur.fetchone()
        if not row:
            return None
        client_id = int(row[0])
        cur.execute(
            """
            UPDATE distribuidora.client_facade_photos
            SET is_current = FALSE, updated_at = NOW()
            WHERE client_id = %s AND is_current = TRUE
            """,
            (client_id,),
        )
    cur.execute(
        """
        UPDATE distribuidora.client_facade_photos
        SET validation_status = %s,
            reviewed_by_user_id = %s,
            reviewed_at = NOW(),
            review_note = %s,
            is_current = CASE WHEN %s THEN TRUE ELSE is_current END,
            updated_at = NOW()
        WHERE id = %s AND validation_status IN ('pending', 'upload_pending')
        RETURNING *
        """,
        (
            status,
            int(reviewer_user_id),
            (note or "").strip() or None,
            bool(make_current and status == "approved"),
            int(photo_id),
        ),
    )
    r = cur.fetchone()
    return _row(cur, r) if r else None
