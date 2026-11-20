"""Persistencia Offline V2 — delivery_offline_ops (idempotencia / etapas)."""

from __future__ import annotations

from typing import Any

from psycopg2.extras import Json


def _row(cur, r) -> dict[str, Any]:
    cols = [d[0] for d in cur.description]
    if isinstance(r, dict) or hasattr(r, "keys"):
        return dict(r)
    return dict(zip(cols, r))


def table_ready(cur) -> bool:
    cur.execute(
        """
        SELECT to_regclass('distribuidora.delivery_offline_ops') IS NOT NULL AS ready
        """
    )
    row = cur.fetchone()
    if row is None:
        return False
    if isinstance(row, dict) or hasattr(row, "keys"):
        return bool(row["ready"])
    return bool(row[0])


def find_by_idem(
    cur, *, driver_user_id: int, idempotency_key: str
) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT *
        FROM distribuidora.delivery_offline_ops
        WHERE driver_user_id = %s AND idempotency_key = %s
        FOR UPDATE
        """,
        (int(driver_user_id), str(idempotency_key)),
    )
    r = cur.fetchone()
    return _row(cur, r) if r else None


def find_completed_for_customer(
    cur,
    *,
    driver_user_id: int,
    dispatch_plan_id: int,
    customer_key: str,
    exclude_idempotency_key: str | None = None,
) -> dict[str, Any] | None:
    """Otra op completada del mismo chofer/plan/cliente (conflicto C)."""
    cur.execute(
        """
        SELECT *
        FROM distribuidora.delivery_offline_ops
        WHERE driver_user_id = %s
          AND dispatch_plan_id = %s
          AND customer_key = %s
          AND status = 'completed'
          AND (%s::text IS NULL OR idempotency_key <> %s)
        ORDER BY completed_at DESC NULLS LAST, id DESC
        LIMIT 1
        FOR UPDATE
        """,
        (
            int(driver_user_id),
            int(dispatch_plan_id),
            str(customer_key),
            exclude_idempotency_key,
            exclude_idempotency_key,
        ),
    )
    r = cur.fetchone()
    return _row(cur, r) if r else None


def insert_received(
    cur,
    *,
    company_id: int,
    driver_user_id: int,
    dispatch_plan_id: int,
    customer_key: str,
    client_id: int | None,
    op_type: str,
    idempotency_key: str,
    client_mutation_id: str | None,
    request_hash: str,
    request_json: dict[str, Any],
    picking_id: int | None,
    picking_version: str | None,
    gps_capture_mode: str | None,
    captured_at,
    device_id: str | None,
    created_by_user_id: int,
) -> dict[str, Any]:
    cur.execute(
        """
        INSERT INTO distribuidora.delivery_offline_ops (
            company_id, driver_user_id, dispatch_plan_id, customer_key, client_id,
            op_type, idempotency_key, client_mutation_id, request_hash, request_json,
            status, processing_stage, picking_id, picking_version,
            gps_capture_mode, captured_at, device_id, created_by_user_id,
            attempt_count, locked_at, locked_by
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            'processing', 'claimed', %s, %s,
            %s, %s, %s, %s,
            1, NOW(), 'sync'
        )
        RETURNING *
        """,
        (
            int(company_id),
            int(driver_user_id),
            int(dispatch_plan_id),
            str(customer_key),
            int(client_id) if client_id is not None else None,
            str(op_type),
            str(idempotency_key),
            client_mutation_id,
            str(request_hash),
            Json(request_json),
            int(picking_id) if picking_id is not None else None,
            picking_version,
            gps_capture_mode,
            captured_at,
            device_id,
            int(created_by_user_id),
        ),
    )
    return _row(cur, cur.fetchone())


def mark_stage(
    cur,
    op_id: int,
    *,
    status: str | None = None,
    processing_stage: str | None = None,
    visit_id: int | None = None,
    evidence_object_key: str | None = None,
    evidence_sha256: str | None = None,
    evidence_upload_confirmed: bool | None = None,
    response_json: dict[str, Any] | None = None,
    error_code: str | None = None,
    error_message: str | None = None,
    bump_attempt: bool = False,
) -> dict[str, Any]:
    sets = ["updated_at = NOW()"]
    params: list[Any] = []
    if status is not None:
        sets.append("status = %s")
        params.append(status)
        if status == "completed":
            sets.append("completed_at = COALESCE(completed_at, NOW())")
    if processing_stage is not None:
        sets.append("processing_stage = %s")
        params.append(processing_stage)
    if visit_id is not None:
        sets.append("visit_id = %s")
        params.append(int(visit_id))
    if evidence_object_key is not None:
        sets.append("evidence_object_key = %s")
        params.append(evidence_object_key)
    if evidence_sha256 is not None:
        sets.append("evidence_sha256 = %s")
        params.append(evidence_sha256)
    if evidence_upload_confirmed is not None:
        sets.append("evidence_upload_confirmed = %s")
        params.append(bool(evidence_upload_confirmed))
    if response_json is not None:
        sets.append("response_json = %s")
        params.append(Json(response_json))
    if error_code is not None:
        sets.append("error_code = %s")
        params.append(error_code)
    if error_message is not None:
        sets.append("error_message = %s")
        params.append(error_message)
    if bump_attempt:
        sets.append("attempt_count = attempt_count + 1")
        sets.append("locked_at = NOW()")
    params.append(int(op_id))
    cur.execute(
        f"""
        UPDATE distribuidora.delivery_offline_ops
        SET {", ".join(sets)}
        WHERE id = %s
        RETURNING *
        """,
        params,
    )
    return _row(cur, cur.fetchone())


def get_by_id(cur, op_id: int) -> dict[str, Any] | None:
    cur.execute(
        "SELECT * FROM distribuidora.delivery_offline_ops WHERE id = %s",
        (int(op_id),),
    )
    r = cur.fetchone()
    return _row(cur, r) if r else None
