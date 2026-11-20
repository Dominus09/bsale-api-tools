"""Offline V2 Fase 2A.1 — sync idempotente de entrega completa / local cerrado.

Diseño aprobado:
- Outbox server-side en delivery_offline_ops (UNIQUE driver+idempotency_key).
- R2 fuera de transacciones SQL largas (etapas recuperables).
- GPS diferido: 300 m + pending_review; no usa horario de sync como visita.
- No borra operaciones automáticamente (retención: ver módulo retention).

No muta Bsale / picking / stock / cuadratura.
"""

from __future__ import annotations

import hashlib
import json
import os
import zlib
from datetime import datetime, timezone
from typing import Any, Callable

from fastapi import HTTPException
from psycopg2 import IntegrityError

from backend.db import get_connection
from backend.repositories.distribuidora import delivery_offline_ops_repo as ops_repo
from backend.repositories.distribuidora import delivery_visit_repo as visit_repo
from backend.services import object_storage_service as storage
from backend.services.distribuidora import delivery_semaphore as sem
from backend.services.distribuidora import delivery_visit_service as visit_svc
from backend.services.distribuidora import dispatch_plan_pwa_driver_read_service as read_svc
from backend.services.distribuidora import client_location_service as loc_svc
from backend.services.distribuidora import delivery_offline_validate as offline_validate
from backend.utils.json_safe import serialize_value

OP_COMPLETE = "complete_delivery"
OP_CLOSED = "closed_store"
ALLOWED_OPS = frozenset({OP_COMPLETE, OP_CLOSED})

ERR_IDEMPOTENCY_PAYLOAD_MISMATCH = "IDEMPOTENCY_PAYLOAD_MISMATCH"
ERR_DUPLICATE_VISIT_OP = "DUPLICATE_VISIT_OPERATION"
ERR_PICKING_CHANGED = offline_validate.ERR_PICKING_CHANGED
ERR_UNAUTHORIZED = "OFFLINE_OP_UNAUTHORIZED"
ERR_VISIT_CLOSED = "VISIT_ALREADY_CLOSED"
ERR_EVIDENCE_REQUIRED = "EVIDENCE_REQUIRED"
ERR_INVALID_PAYMENT = "INVALID_PAYMENT_METHOD"
ERR_SYNC_WRITES_DISABLED = "OFFLINE_SYNC_WRITES_DISABLED"

# Inyectable en tests (simula R2 sin red).
R2PutFn = Callable[[str, bytes, str], str]
LiveContextFetcher = offline_validate.LiveContextFetcher


def offline_sync_writes_enabled() -> bool:
    """Feature flag backend: OFF por defecto. Solo laboratorio explícito."""
    return os.getenv("OFFLINE_SYNC_WRITES_ENABLED", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def compute_request_hash(payload: dict[str, Any]) -> str:
    """Hash del contenido lógico (sin bytes de imagen)."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def stable_evidence_object_key(
    *,
    company_id: int,
    plan_id: int,
    idempotency_key: str,
    local_evidence_id: str,
) -> str:
    safe_idem = "".join(c if c.isalnum() or c in "-_" else "_" for c in idempotency_key)[:80]
    safe_local = "".join(c if c.isalnum() or c in "-_" else "_" for c in local_evidence_id)[:40]
    return (
        f"operaciones/entregas/{int(company_id)}/{int(plan_id)}/"
        f"offline/{safe_idem}/{safe_local}.jpg"
    )


def evaluate_deferred_gps(
    *,
    driver_lat: float | None,
    driver_lng: float | None,
    accuracy_m: float | None,
    gps_captured_at: str | None,
    event_captured_at: str | None,
    photo_captured_at: str | None,
    client_lat: float | None,
    client_lng: float | None,
    require_proximity: bool,
) -> dict[str, Any]:
    """GPS diferido: distancia 300 m; NUNCA verified automático.

    No compara edad contra 'ahora del sync'. Coherencia temporal vs evento/foto.
    """
    out: dict[str, Any] = {
        "mode": "deferred_offline",
        "distance_m": None,
        "within_300m": None,
        "closure_review_status": "pending_review",
        "verified": False,
        "exception_code": None,
        "notes": [],
    }
    if driver_lat is None or driver_lng is None:
        out["exception_code"] = "gps_unavailable"
        out["notes"].append("Sin coordenadas en el evento offline")
        return out

    if gps_captured_at:
        try:
            gps_ts = visit_svc._parse_iso_ts(gps_captured_at)  # noqa: SLF001
        except HTTPException:
            out["exception_code"] = "gps_invalid_timestamp"
            out["notes"].append("Timestamp GPS inválido")
            return out
    else:
        gps_ts = None
        out["notes"].append("Falta gps_captured_at")

    # Coherencia temporal: GPS / foto / resultado dentro de ventana amplia (2 h).
    def _ts(v: str | None):
        if not v:
            return None
        try:
            return visit_svc._parse_iso_ts(v)  # noqa: SLF001
        except HTTPException:
            return None

    event_ts = _ts(event_captured_at)
    photo_ts = _ts(photo_captured_at)
    for label, other in (("event", event_ts), ("photo", photo_ts)):
        if gps_ts and other:
            delta = abs((gps_ts - other).total_seconds())
            if delta > 7200:
                out["notes"].append(f"Desfase temporal GPS vs {label}: {int(delta)}s")
                out["closure_review_status"] = "pending_review"

    if accuracy_m is not None and float(accuracy_m) > sem.GPS_WARN_ACCURACY_M:
        out["notes"].append(f"Precisión GPS pobre: {accuracy_m}m")
        out["closure_review_status"] = "pending_review"

    if require_proximity:
        if client_lat is None or client_lng is None:
            out["exception_code"] = "client_gps_missing"
            out["within_300m"] = False
            out["notes"].append("Cliente sin coordenadas referenciales")
            return out
        dist = sem.haversine_m(
            float(driver_lat), float(driver_lng), float(client_lat), float(client_lng)
        )
        out["distance_m"] = dist
        out["within_300m"] = dist <= sem.INCIDENT_MAX_DISTANCE_M
        if not out["within_300m"]:
            out["exception_code"] = "gps_out_of_range"
            out["notes"].append(
                f"Fuera de 300 m (dist={dist:.1f}m) — requiere revisión"
            )
        # Incluso dentro de 300 m: diferido → pending_review (no verified).
        out["closure_review_status"] = "pending_review"
        out["verified"] = False
        out["notes"].append(
            "GPS diferido offline: no se marca verified automáticamente"
        )
    return out


def _logical_payload(body: dict[str, Any]) -> dict[str, Any]:
    """Campos que definen el contenido inmutable (sin binarios)."""
    keys = (
        "op_type",
        "customer_key",
        "client_mutation_id",
        "picking_id",
        "picking_version",
        "captured_at",
        "result",
        "documents",
        "payment",
        "gps",
        "gps_exception",
        "evidence_meta",
        "device_id",
        "note",
    )
    return {k: body.get(k) for k in keys}


def _http_conflict(code: str, message_es: str, **extra: Any) -> HTTPException:
    return HTTPException(
        status_code=409,
        detail={
            "code": code,
            "message": message_es,
            "message_es": message_es,
            **extra,
        },
    )


def sync_offline_operation(
    *,
    user: dict[str, Any],
    plan_id: int,
    body: dict[str, Any],
    evidence_bytes: bytes | None = None,
    evidence_content_type: str = "image/jpeg",
    r2_put: R2PutFn | None = None,
    context_fetcher: LiveContextFetcher | None = None,
    require_writes_flag: bool = False,
    picking_validator: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Sync offline idempotente + validador vivo. Flag HTTP: require_writes_flag."""
    if require_writes_flag and not offline_sync_writes_enabled():
        raise HTTPException(
            status_code=503,
            detail={
                "code": ERR_SYNC_WRITES_DISABLED,
                "message_es": (
                    "Sincronización de escrituras offline deshabilitada "
                    "(feature flag). No se aceptan envíos."
                ),
                "message": "offline sync writes disabled",
            },
        )
    uid = read_svc.enforce_driver_plan_read_access(user, int(plan_id))
    idem = str(body.get("idempotency_key") or "").strip()
    if not idem:
        raise HTTPException(status_code=400, detail="idempotency_key requerido")

    op_type = str(body.get("op_type") or "").strip()
    if op_type not in ALLOWED_OPS:
        raise HTTPException(status_code=400, detail="op_type inválido")

    customer_key = str(body.get("customer_key") or "").strip()
    if not customer_key:
        raise HTTPException(status_code=400, detail="customer_key requerido")

    logical = _logical_payload(body)
    request_hash = compute_request_hash(logical)

    client = loc_svc._resolve_client(int(plan_id), customer_key)  # noqa: SLF001
    try:
        client_id = loc_svc._parse_client_id(customer_key, client)  # noqa: SLF001
    except HTTPException as exc:
        raise HTTPException(
            status_code=422,
            detail="Cliente sin identidad comprobada para escritura offline",
        ) from exc
    company_id = loc_svc._authorized_company_id_for_plan(user, int(plan_id))  # noqa: SLF001

    lock_key = _advisory_lock_key(uid, idem)
    lock_conn = get_connection()
    try:
        lock_cur = lock_conn.cursor()
        lock_cur.execute("SELECT pg_advisory_lock(%s)", (lock_key,))
        lock_conn.commit()
        lock_cur.close()
        return _sync_offline_operation_locked(
            user=user,
            plan_id=int(plan_id),
            uid=uid,
            idem=idem,
            op_type=op_type,
            customer_key=customer_key,
            client_id=client_id,
            company_id=int(company_id),
            body=body,
            logical=logical,
            request_hash=request_hash,
            evidence_bytes=evidence_bytes,
            evidence_content_type=evidence_content_type,
            r2_put=r2_put,
            context_fetcher=context_fetcher,
            picking_validator=picking_validator,
        )
    finally:
        try:
            unlock_cur = lock_conn.cursor()
            unlock_cur.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))
            lock_conn.commit()
            unlock_cur.close()
        except Exception:
            pass
        lock_conn.close()


def _advisory_lock_key(driver_user_id: int, idempotency_key: str) -> int:
    raw = f"offline_op:{int(driver_user_id)}:{idempotency_key}".encode("utf-8")
    return zlib.crc32(raw) & 0x7FFFFFFF


def _sync_offline_operation_locked(
    *,
    user: dict[str, Any],
    plan_id: int,
    uid: int,
    idem: str,
    op_type: str,
    customer_key: str,
    client_id: Any,
    company_id: int,
    body: dict[str, Any],
    logical: dict[str, Any],
    request_hash: str,
    evidence_bytes: bytes | None,
    evidence_content_type: str,
    r2_put: R2PutFn | None,
    context_fetcher: LiveContextFetcher | None,
    picking_validator: Callable[[dict[str, Any]], dict[str, Any]] | None,
) -> dict[str, Any]:
    # --- Fase A: claim idempotente (TX corta) ---
    conn = get_connection()
    op_row: dict[str, Any]
    try:
        cur = conn.cursor()
        if not visit_repo.tables_ready(cur) or not ops_repo.table_ready(cur):
            raise HTTPException(
                status_code=503,
                detail="Migraciones 056/061 pendientes para offline sync",
            )

        existing = ops_repo.find_by_idem(
            cur, driver_user_id=uid, idempotency_key=idem
        )
        if existing:
            if str(existing.get("request_hash")) != request_hash:
                raise _http_conflict(
                    ERR_IDEMPOTENCY_PAYLOAD_MISMATCH,
                    "Misma idempotency_key con contenido distinto",
                    op_id=int(existing["id"]),
                )
            if existing.get("status") == "completed" and existing.get("response_json"):
                conn.commit()
                cur.close()
                replay = dict(existing["response_json"])
                replay["idempotent_replay"] = True
                replay["offline_op_id"] = int(existing["id"])
                return serialize_value(replay)
            op_row = ops_repo.mark_stage(
                cur,
                int(existing["id"]),
                status="processing",
                processing_stage=str(existing.get("processing_stage") or "claimed"),
                bump_attempt=True,
            )
        else:
            other = ops_repo.find_completed_for_customer(
                cur,
                driver_user_id=uid,
                dispatch_plan_id=int(plan_id),
                customer_key=customer_key,
                exclude_idempotency_key=idem,
            )
            if other:
                raise _http_conflict(
                    ERR_DUPLICATE_VISIT_OP,
                    "Ya existe una operación offline completada para este cliente",
                    other_op_id=int(other["id"]),
                    other_visit_id=other.get("visit_id"),
                )

            closed = visit_repo.get_valid_closed_visit(
                cur, plan_id=int(plan_id), customer_key=customer_key
            )
            if visit_svc.is_valid_closed_visit(closed):
                raise _http_conflict(
                    ERR_VISIT_CLOSED,
                    "Visita ya cerrada en servidor; no se acepta operación offline",
                    closed_visit_id=int(closed["id"]),
                )

            gps = body.get("gps") if isinstance(body.get("gps"), dict) else {}
            captured_at = None
            if body.get("captured_at"):
                captured_at = visit_svc._parse_iso_ts(str(body.get("captured_at")))  # noqa: SLF001
            try:
                op_row = ops_repo.insert_received(
                    cur,
                    company_id=int(company_id),
                    driver_user_id=uid,
                    dispatch_plan_id=int(plan_id),
                    customer_key=customer_key,
                    client_id=int(client_id) if client_id is not None else None,
                    op_type=op_type,
                    idempotency_key=idem,
                    client_mutation_id=(
                        str(body.get("client_mutation_id") or "").strip() or None
                    ),
                    request_hash=request_hash,
                    request_json=logical,
                    picking_id=(
                        int(body["picking_id"])
                        if body.get("picking_id") is not None
                        else None
                    ),
                    picking_version=(
                        str(body.get("picking_version"))
                        if body.get("picking_version") is not None
                        else None
                    ),
                    gps_capture_mode="deferred_offline" if gps else "exception",
                    captured_at=captured_at,
                    device_id=(str(body.get("device_id") or "").strip() or None),
                    created_by_user_id=uid,
                )
            except IntegrityError:
                conn.rollback()
                existing2 = ops_repo.find_by_idem(
                    cur, driver_user_id=uid, idempotency_key=idem
                )
                if not existing2:
                    raise
                if str(existing2.get("request_hash")) != request_hash:
                    raise _http_conflict(
                        ERR_IDEMPOTENCY_PAYLOAD_MISMATCH,
                        "Misma idempotency_key con contenido distinto",
                        op_id=int(existing2["id"]),
                    )
                if existing2.get("status") == "completed" and existing2.get(
                    "response_json"
                ):
                    conn.commit()
                    cur.close()
                    replay = dict(existing2["response_json"])
                    replay["idempotent_replay"] = True
                    replay["offline_op_id"] = int(existing2["id"])
                    return serialize_value(replay)
                op_row = ops_repo.mark_stage(
                    cur,
                    int(existing2["id"]),
                    status="processing",
                    processing_stage=str(
                        existing2.get("processing_stage") or "claimed"
                    ),
                    bump_attempt=True,
                )
        conn.commit()
        cur.close()
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    # --- Validación real picking/docs (oficial; fetcher solo en lab) ---
    try:
        if picking_validator is not None:
            # Compat tests legados: se ejecuta además del real si hay fetcher.
            picking_validator(body)
        allow_vid = op_row.get("visit_id")
        offline_validate.validate_offline_operation(
            user=user,
            plan_id=int(plan_id),
            body=body,
            context_fetcher=context_fetcher,
            allow_closed_visit_id=(
                int(allow_vid) if allow_vid is not None else None
            ),
        )
    except HTTPException as exc:
        return _fail_op(int(op_row["id"]), exc)

    # --- Fase B: evidencia R2 (fuera de TX) ---
    evidence_key = op_row.get("evidence_object_key")
    evidence_sha = op_row.get("evidence_sha256")
    if op_type == OP_CLOSED:
        meta = body.get("evidence_meta") if isinstance(body.get("evidence_meta"), dict) else {}
        local_eid = str(meta.get("local_id") or "closed_store").strip() or "closed_store"
        sha_declared = str(meta.get("content_sha256") or "").strip().lower()
        if not evidence_bytes:
            # Reintento sin bytes: si ya hay upload confirmado, continuar
            if not op_row.get("evidence_upload_confirmed"):
                return _fail_op(
                    int(op_row["id"]),
                    HTTPException(
                        status_code=422,
                        detail={
                            "code": ERR_EVIDENCE_REQUIRED,
                            "message": "Local cerrado requiere fotografía",
                            "message_es": "Local cerrado requiere fotografía",
                        },
                    ),
                )
        else:
            digest = hashlib.sha256(evidence_bytes).hexdigest()
            if sha_declared and sha_declared != digest:
                return _fail_op(
                    int(op_row["id"]),
                    HTTPException(
                        status_code=422,
                        detail="content_sha256 no coincide con la fotografía",
                    ),
                )
            evidence_sha = digest
            evidence_key = stable_evidence_object_key(
                company_id=int(company_id),
                plan_id=int(plan_id),
                idempotency_key=idem,
                local_evidence_id=local_eid,
            )
            put = r2_put or _default_r2_put
            try:
                put(evidence_key, evidence_bytes, evidence_content_type)
            except Exception as exc:
                _mark(
                    int(op_row["id"]),
                    status="awaiting_evidence",
                    processing_stage="evidence_upload",
                    evidence_object_key=evidence_key,
                    evidence_sha256=evidence_sha,
                    error_code="R2_UPLOAD_FAILED",
                    error_message=str(exc)[:500],
                )
                raise HTTPException(
                    status_code=503,
                    detail={
                        "code": "R2_UPLOAD_FAILED",
                        "message": "Fallo al subir evidencia; reintente con misma clave",
                        "message_es": "Fallo al subir evidencia; reintente",
                        "offline_op_id": int(op_row["id"]),
                        "evidence_object_key": evidence_key,
                    },
                ) from exc
            _mark(
                int(op_row["id"]),
                status="evidence_ready",
                processing_stage="evidence_confirmed",
                evidence_object_key=evidence_key,
                evidence_sha256=evidence_sha,
                evidence_upload_confirmed=True,
            )

    # --- Fase C: aplicar visita (TX de negocio vía servicios existentes) ---
    try:
        result = _apply_business(
            user=user,
            plan_id=int(plan_id),
            uid=uid,
            op_type=op_type,
            body=body,
            customer_key=customer_key,
            evidence_object_key=evidence_key,
            evidence_sha256=evidence_sha,
            offline_op_id=int(op_row["id"]),
            idempotency_key=idem,
        )
    except HTTPException as exc:
        return _fail_op(int(op_row["id"]), exc)

    response = serialize_value(
        {
            **result,
            "idempotent_replay": False,
            "offline_op_id": int(op_row["id"]),
            "mode": "offline_sync_v2",
            "retention_note_es": (
                "Operación confirmada en servidor. La outbox del dispositivo "
                "solo debe marcarse synced tras recibir esta respuesta."
            ),
        }
    )
    _mark(
        int(op_row["id"]),
        status="completed",
        processing_stage="completed",
        visit_id=int(result["visit"]["id"]),
        response_json=response,
        evidence_upload_confirmed=bool(
            op_type != OP_CLOSED or evidence_key
        ),
    )
    return response


def _default_r2_put(object_key: str, data: bytes, content_type: str) -> str:
    storage.put_bytes(object_key, data, content_type=content_type)
    if not storage.object_exists(object_key):
        raise RuntimeError("R2 object_exists=false tras put_bytes")
    return object_key


def _mark(op_id: int, **kwargs: Any) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        ops_repo.mark_stage(cur, op_id, **kwargs)
        conn.commit()
        cur.close()
    finally:
        conn.close()


def _fail_op(op_id: int, exc: HTTPException) -> dict[str, Any]:
    detail = exc.detail
    if isinstance(detail, dict):
        code = str(detail.get("code") or "FAILED")
        msg = str(detail.get("message_es") or detail.get("message") or code)
    else:
        code = "FAILED"
        msg = str(detail)
    status = "conflict" if exc.status_code == 409 else "failed"
    _mark(
        op_id,
        status=status,
        processing_stage=status,
        error_code=code,
        error_message=msg[:800],
    )
    raise exc


def _apply_business(
    *,
    user: dict[str, Any],
    plan_id: int,
    uid: int,
    op_type: str,
    body: dict[str, Any],
    customer_key: str,
    evidence_object_key: str | None,
    evidence_sha256: str | None,
    offline_op_id: int,
    idempotency_key: str,
) -> dict[str, Any]:
    gps = body.get("gps") if isinstance(body.get("gps"), dict) else {}
    gps_exc = body.get("gps_exception") if isinstance(body.get("gps_exception"), dict) else {}
    result = body.get("result") if isinstance(body.get("result"), dict) else {}
    note = (result.get("note") or body.get("note") or "").strip() or None
    captured_at = str(body.get("captured_at") or gps.get("captured_at") or "")

    client_lat, client_lng = visit_svc._client_ref_coords(plan_id, customer_key)  # noqa: SLF001
    deferred = evaluate_deferred_gps(
        driver_lat=gps.get("lat"),
        driver_lng=gps.get("lng"),
        accuracy_m=gps.get("accuracy_m"),
        gps_captured_at=str(gps.get("captured_at") or "") or None,
        event_captured_at=captured_at or None,
        photo_captured_at=(
            str((body.get("evidence_meta") or {}).get("captured_at") or "") or None
        ),
        client_lat=client_lat,
        client_lng=client_lng,
        require_proximity=(op_type == OP_CLOSED),
    )

    start_kwargs: dict[str, Any] = {
        "user": user,
        "plan_id": plan_id,
        "customer_key": customer_key,
        "idempotency_key": f"offline-start:{idempotency_key}",
    }
    if gps.get("lat") is not None and gps.get("lng") is not None:
        start_kwargs.update(
            {
                "latitude": float(gps["lat"]),
                "longitude": float(gps["lng"]),
                "accuracy_m": gps.get("accuracy_m"),
                "gps_captured_at": gps.get("captured_at") or captured_at or None,
                "gps_source": "deferred_offline",
            }
        )
    else:
        start_kwargs.update(
            {
                "gps_exception_code": (
                    str(gps_exc.get("code") or deferred.get("exception_code") or "gps_unavailable")
                ),
                "gps_exception_note": (
                    str(gps_exc.get("note") or note or "GPS no disponible en captura offline")
                ),
            }
        )

    started = visit_svc.start_visit(**start_kwargs)
    visit = started["visit"]
    visit_id = int(visit["id"])
    version = int(visit["version"])
    # Reintento tras COMMIT perdido / carrera: visita ya cerrada por misma clave.
    if str(visit.get("status") or "") in {"completed", "not_delivered", "voided"}:
        return {
            "op_type": op_type,
            "visit": visit,
            "finalized": False,
            "idempotent_business_replay": True,
            "gps_deferred": deferred,
        }

    if op_type == OP_COMPLETE:
        docs = body.get("documents") if isinstance(body.get("documents"), list) else []
        if not docs:
            raise HTTPException(status_code=422, detail="documents requeridos")
        payment = body.get("payment") if isinstance(body.get("payment"), dict) else {}
        method = str(payment.get("method") or "").strip()
        if method not in visit_svc.DRIVER_PAYMENT_METHODS:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": ERR_INVALID_PAYMENT,
                    "message": "Método de pago inválido",
                    "message_es": "Método de pago inválido",
                },
            )
        for doc in docs:
            related_id = doc.get("related_document_id")
            lines_in = doc.get("lines") if isinstance(doc.get("lines"), list) else []
            # Entrega completa: cantidades planificadas vigentes (validadas por caller).
            lines_payload = []
            for ln in lines_in:
                planned = float(ln.get("planned_units") or ln.get("delivered_units") or 0)
                lines_payload.append(
                    {
                        "source_detail_id": int(ln["source_detail_id"]),
                        "variant_id": ln.get("variant_id"),
                        "sku": ln.get("sku"),
                        "description": ln.get("description"),
                        "planned_units": planned,
                        "delivered_units": planned,
                        "undelivered_units": 0,
                        "undelivered_reason": None,
                    }
                )
            saved = visit_svc.save_document_lines(
                user=user,
                plan_id=plan_id,
                visit_id=visit_id,
                related_document_id=int(related_id) if related_id is not None else None,
                oc_document_id=(
                    int(doc["oc_document_id"])
                    if doc.get("oc_document_id") is not None
                    else None
                ),
                document_number=(
                    int(doc["document_number"])
                    if doc.get("document_number") is not None
                    else None
                ),
                document_type=(str(doc.get("document_type") or "") or None),
                document_total=(
                    float(doc["document_total"])
                    if doc.get("document_total") is not None
                    else None
                ),
                result_status="full",
                notes=note,
                lines=[
                    {
                        "source_detail_id": ln["source_detail_id"],
                        "variant_id": ln.get("variant_id"),
                        "sku_code": ln.get("sku") or ln.get("sku_code"),
                        "product_name": ln.get("description") or ln.get("product_name"),
                        "planned_units": ln["planned_units"],
                        "delivered_units": ln["delivered_units"],
                        "undelivered_units": 0,
                        "undelivered_reason": None,
                    }
                    for ln in lines_payload
                ],
                expected_version=version,
            )
            version = int(saved.get("visit_version") or version)
            doc_result_id = int(saved["document_result"]["id"])
            pay = visit_svc.declare_payment_method(
                user=user,
                plan_id=plan_id,
                visit_id=visit_id,
                document_result_id=doc_result_id,
                method=method,
                note=str(payment.get("note") or "") or None,
            )
            version = int(pay.get("visit_version") or version)

        # GPS diferido: no pasar coords como "frescas" (evita GPS_STALE online).
        # Persistimos lat/lng/precisión/timestamps en result_note auditable + deferred meta.
        exc_code = "gps_deferred_offline"
        if deferred.get("exception_code") in (
            "gps_out_of_range",
            "client_gps_missing",
            "gps_unavailable",
        ):
            exc_code = str(deferred["exception_code"])
        if gps_exc.get("code"):
            exc_code = str(gps_exc["code"])
        exc_note = (
            f"GPS diferido offline complete. dist_m={deferred.get('distance_m')} "
            f"within_300m={deferred.get('within_300m')}. "
            f"lat={gps.get('lat')} lng={gps.get('lng')} acc={gps.get('accuracy_m')} "
            f"gps_at={gps.get('captured_at') or captured_at}. "
            f"{note or ''} "
            + "; ".join(deferred.get("notes") or [])
        ).strip()[:500]
        if len(exc_note) < 8:
            exc_note = (note or "Entrega completa offline con GPS diferido")[:500]

        fin = visit_svc.finalize_visit(
            user=user,
            plan_id=plan_id,
            visit_id=visit_id,
            expected_version=version,
            status="completed",
            result_note=note,
            gps_source="exception",
            gps_exception_code=exc_code,
            gps_exception_note=exc_note,
        )
        return {
            "op_type": op_type,
            "visit": fin["visit"],
            "finalized": fin.get("finalized"),
            "gps_deferred": deferred,
            "payment_method": method,
        }

    # Local cerrado
    if not note or len(note) < 3:
        raise HTTPException(
            status_code=422,
            detail="Observación obligatoria para local cerrado",
        )
    if not evidence_object_key:
        raise HTTPException(
            status_code=422,
            detail={
                "code": ERR_EVIDENCE_REQUIRED,
                "message": "Fotografía requerida",
                "message_es": "Fotografía requerida",
            },
        )

    # Registrar evidencia ya subida (sin re-put R2)
    conn = get_connection()
    try:
        cur = conn.cursor()
        visit_row = visit_repo.get_visit_for_update(cur, visit_id)
        if not visit_row or int(visit_row["driver_user_id"]) != uid:
            raise HTTPException(status_code=403, detail="Visita no autorizada")
        # Idempotencia evidencia
        evid_idem = f"offline-ev:{idempotency_key}"
        prior = visit_repo.find_evidence_by_idem(
            cur, user_id=uid, idempotency_key=evid_idem
        )
        if not prior:
            event_id = visit_repo.insert_event(
                cur,
                visit_id=visit_id,
                action="evidence_uploaded",
                actor_user_id=uid,
                payload={
                    "kind": "closed_store",
                    "object_key": evidence_object_key,
                    "sha256": evidence_sha256,
                    "offline_op_id": offline_op_id,
                    "deferred": True,
                },
            )
            visit_repo.insert_evidence(
                cur,
                visit_id=visit_id,
                company_id=int(visit_row["company_id"]),
                evidence_kind="closed_store",
                object_key=evidence_object_key,
                content_type="image/jpeg",
                size_bytes=None,
                sha256=evidence_sha256,
                captured_by_user_id=uid,
                capture_lat=gps.get("lat"),
                capture_lng=gps.get("lng"),
                capture_accuracy_m=gps.get("accuracy_m"),
                upload_confirmed=True,
                validation_status="pending",
                exception_note=None,
                idempotency_key=evid_idem,
                visit_event_id=int(event_id),
            )
            # Docs none (sin productos entregados / sin cobro)
            expected_ids = visit_svc._expected_related_document_ids(  # noqa: SLF001
                plan_id, customer_key
            )
            for rid in expected_ids or []:
                visit_repo.upsert_document_result(
                    cur,
                    visit_id=visit_id,
                    related_document_id=int(rid),
                    oc_document_id=None,
                    document_number=None,
                    document_type=None,
                    document_total=None,
                    result_status="none",
                    notes=note,
                )
            bumped = visit_repo.bump_visit_version(cur, visit_id)
            version = int((bumped or visit_row).get("version") or version)
        else:
            version = int(visit_row.get("version") or version)
        conn.commit()
        cur.close()
    finally:
        conn.close()

    # Cierre not_delivered: NO pasar GPS "fresco" (evitar GPS_STALE 120s).
    # Usar excepción auditable gps_deferred_offline → pending_review + 300m info en notas.
    exc_code = "gps_deferred_offline"
    if deferred.get("exception_code") in (
        "gps_out_of_range",
        "client_gps_missing",
        "gps_unavailable",
    ):
        exc_code = str(deferred["exception_code"])
    if gps_exc.get("code"):
        exc_code = str(gps_exc["code"])
    exc_note = (
        f"GPS diferido offline. dist_m={deferred.get('distance_m')} "
        f"within_300m={deferred.get('within_300m')}. "
        f"lat={gps.get('lat')} lng={gps.get('lng')} acc={gps.get('accuracy_m')} "
        f"gps_at={gps.get('captured_at') or captured_at}. "
        f"{note or ''} "
        + "; ".join(deferred.get("notes") or [])
    ).strip()[:500]
    if len(exc_note) < 8:
        exc_note = (note or "Local cerrado offline con GPS diferido")[:500]

    fin = visit_svc.finalize_visit(
        user=user,
        plan_id=plan_id,
        visit_id=visit_id,
        expected_version=version,
        status="not_delivered",
        result_reason="closed_store",
        result_note=note,
        gps_source="exception",
        gps_exception_code=exc_code,
        gps_exception_note=exc_note,
    )
    return {
        "op_type": op_type,
        "visit": fin["visit"],
        "finalized": fin.get("finalized"),
        "gps_deferred": deferred,
        "evidence_object_key": evidence_object_key,
        "no_payment": True,
        "no_delivered_units": True,
    }


# --- Retención (contrato documental para 2A.2 PWA) ---
OFFLINE_OPS_RETENTION_POLICY = {
    "auto_delete_unconfirmed": False,
    "on_401": "retain_outbox",
    "on_logout_with_pending": "warn_and_retain_or_block",
    "on_other_user_login": "isolate_only_never_wipe_other_outbox",
    "on_sw_or_build_id": "retain_outbox",
    "authorized_delete": "explicit_user_or_admin_procedure_only",
}
