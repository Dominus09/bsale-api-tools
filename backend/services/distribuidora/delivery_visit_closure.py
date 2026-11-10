"""Validaciones de cierre P4 (servidor). Sin mutar Bsale/picking."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from fastapi import HTTPException

from backend.repositories.distribuidora import delivery_visit_repo as visit_repo
from backend.services.distribuidora.delivery_semaphore import (
    PHOTO_REQUIRED_REASONS,
    PROXIMITY_REQUIRED_REASONS,
    assert_fresh_gps,
    assert_within_incident_radius,
)

INCIDENT_KINDS = frozenset(
    {"closed_store", "not_found", "rejected_order", "absent_customer", "other"}
)


def _active_conformity(cur, visit_id: int) -> dict[str, Any] | None:
    conf = visit_repo.get_conformity(cur, int(visit_id))
    if not conf:
        return None
    if conf.get("invalidated_at"):
        return None
    return conf


def assert_finalize_ready(
    cur,
    *,
    visit: dict[str, Any],
    status: str,
    result_reason: str | None,
    result_note: str | None,
    has_end_gps: bool,
    expected_related_ids: list[int],
    end_lat: float | None = None,
    end_lng: float | None = None,
    end_accuracy_m: float | None = None,
    end_gps_captured_at: str | None = None,
    client_lat: float | None = None,
    client_lng: float | None = None,
    proximity_exception: bool = False,
) -> dict[str, Any]:
    """Devuelve {closure_review_status, notes}. Lanza HTTPException si bloquea."""
    visit_id = int(visit["id"])
    docs = visit_repo.list_document_results(cur, visit_id)
    lines = visit_repo.list_line_results(cur, visit_id)
    evidence = visit_repo.list_evidence(cur, visit_id)
    conf = _active_conformity(cur, visit_id)

    review = "verified"
    notes: list[str] = []
    gps_meta: dict[str, Any] = {}

    # GPS: real o excepción ya registrada en la visita / cierre
    if not has_end_gps:
        if not (visit.get("gps_exception_code") or visit.get("end_gps_source") == "exception"):
            # Permitir cierre con excepción explícita vía result_note + pending_review
            # solo para incidencias; completed/partial exigen GPS o excepción auditada.
            if status in ("completed", "partial"):
                raise HTTPException(
                    status_code=422,
                    detail=(
                        "Cierre de entrega requiere GPS final o excepción GPS "
                        "auditable registrada al iniciar/cerrar"
                    ),
                )
            review = "pending_review"
            notes.append("sin_gps_final")

    if status in ("completed", "partial"):
        if not expected_related_ids:
            raise HTTPException(
                status_code=422,
                detail="No hay documentos esperados del cliente para cerrar entrega",
            )
        saved_ids = {
            int(d["related_document_id"])
            for d in docs
            if d.get("related_document_id") is not None
        }
        missing = [i for i in expected_related_ids if i not in saved_ids]
        if missing:
            raise HTTPException(
                status_code=422,
                detail=(
                    "Faltan resultados explícitos de documentos: "
                    + ", ".join(str(x) for x in missing)
                ),
            )
        if not lines:
            raise HTTPException(
                status_code=422,
                detail="Debe registrar cantidades por línea antes de finalizar",
            )

        ambiguous = [
            ln
            for ln in lines
            if ln.get("source_detail_id") is None
        ]
        if ambiguous:
            raise HTTPException(
                status_code=422,
                detail=(
                    "Hay líneas sin source_detail_id confiable; "
                    "no se puede cerrar la entrega como verificada"
                ),
            )

        for ln in lines:
            planned = float(ln.get("planned_units") or 0)
            delivered = float(ln.get("delivered_units") or 0)
            undel = float(ln.get("undelivered_units") or 0)
            if delivered < 0 or undel < 0 or delivered > planned:
                raise HTTPException(status_code=422, detail="Cantidades incoherentes")
            if undel > 0 and not (ln.get("undelivered_reason") or "").strip():
                raise HTTPException(
                    status_code=422,
                    detail="Motivo obligatorio para cantidades no entregadas",
                )

        total_undel = sum(float(x.get("undelivered_units") or 0) for x in lines)
        if status == "completed" and total_undel > 0:
            raise HTTPException(
                status_code=422,
                detail="Entrega completa no puede tener faltantes; use parcial",
            )
        if status == "partial" and total_undel <= 0:
            raise HTTPException(
                status_code=422,
                detail="Entrega parcial requiere diferencias registradas",
            )

        # Rechazo parcial de productos → foto de rechazo obligatoria.
        rejection_lines = [
            ln
            for ln in lines
            if (ln.get("undelivered_reason") or "").strip() == "customer_rejection"
            and float(ln.get("undelivered_units") or 0) > 0
        ]
        if rejection_lines:
            confirmed_rej = [
                e
                for e in evidence
                if e.get("evidence_kind") == "rejected_order"
                and e.get("upload_confirmed")
                and e.get("validation_status") in ("pending", "approved")
            ]
            if not confirmed_rej:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        "Rechazo parcial de productos requiere fotografía "
                        "del establecimiento confirmada en el servidor"
                    ),
                )
            # GPS 300 m o excepción → pending_review (no verified automático).
            if proximity_exception:
                review = "pending_review"
                notes.append("gps_proximity_exception_partial_rejection")
            elif has_end_gps and end_lat is not None and end_lng is not None:
                try:
                    fresh = assert_fresh_gps(
                        latitude=end_lat,
                        longitude=end_lng,
                        gps_captured_at=end_gps_captured_at,
                        accuracy_m=end_accuracy_m,
                    )
                    prox = assert_within_incident_radius(
                        driver_lat=fresh["latitude"],
                        driver_lng=fresh["longitude"],
                        client_lat=client_lat,
                        client_lng=client_lng,
                    )
                    gps_meta = {**fresh, **prox}
                    if fresh.get("accuracy_warn"):
                        review = "pending_review"
                        notes.append("gps_accuracy_warn")
                except HTTPException as exc:
                    raise HTTPException(
                        status_code=exc.status_code, detail=exc.detail
                    ) from exc
            else:
                raise HTTPException(
                    status_code=422,
                    detail={
                        "code": "GPS_REQUIRED",
                        "message_es": (
                            "Debes estar a menos de 300 metros del cliente "
                            "para registrar rechazo con evidencia, "
                            "o declarar excepción GPS auditable"
                        ),
                    },
                )

        # Forma de pago declarativa (método por documento; montos opcionales/legado).
        collections = visit_repo.list_active_collections(cur, visit_id)
        docs_needing_pay = [
            d
            for d in docs
            if str(d.get("result_status") or "") not in ("none", "skipped")
        ]
        if docs_needing_pay:
            missing_pay = False
            saw_doc_id = False
            for d in docs_needing_pay:
                did = d.get("id")
                if did is None:
                    continue
                saw_doc_id = True
                doc_cols = [
                    c
                    for c in collections
                    if c.get("document_result_id") is not None
                    and int(c["document_result_id"]) == int(did)
                ]
                if not doc_cols:
                    missing_pay = True
                    break
            if (saw_doc_id and missing_pay) or (not saw_doc_id and not collections):
                raise HTTPException(
                    status_code=422,
                    detail=(
                        "Declare la forma de pago de cada documento entregado "
                        "(sin montos) antes de finalizar"
                    ),
                )

        # Conformidad
        if not conf:
            raise HTTPException(
                status_code=422,
                detail="Falta conformidad del receptor (firma o negativa registrada)",
            )
        if conf.get("refused"):
            if not (conf.get("refuse_reason") or "").strip():
                raise HTTPException(
                    status_code=422, detail="Negativa de firma requiere motivo"
                )
            review = "pending_review"
            notes.append("firma_rechazada")
        else:
            if not conf.get("signature_object_key"):
                raise HTTPException(
                    status_code=422,
                    detail="Entrega completa/parcial requiere firma real almacenada",
                )
            if not (conf.get("receptor_name") or "").strip():
                raise HTTPException(
                    status_code=422, detail="Nombre del receptor requerido"
                )
            # La firma debe corresponder al resumen actual (anti-reuso / anti-cambio silencioso)
            current_summary = build_conformity_summary(
                visit=visit, docs=docs, lines=lines, collections=collections
            )
            current_sha = hashlib.sha256(
                json.dumps(current_summary, sort_keys=True, default=str).encode("utf-8")
            ).hexdigest()
            signed_sha = (conf.get("accepted_summary_sha256") or "").strip()
            if not signed_sha or signed_sha != current_sha:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        "La conformidad firmada no coincide con cantidades/"
                        "cobranza actuales; vuelva a firmar tras el cambio"
                    ),
                )
            signed_ver = conf.get("visit_version_at_sign")
            if signed_ver is not None and int(signed_ver) != int(visit.get("version") or 1):
                # Permite si el sha coincide (versión puede haber subido por eventos no-línea)
                # pero exige sha; si además el sha falla ya se bloqueó arriba.
                notes.append("version_firma_distinta_sha_ok")

    elif status == "not_delivered":
        reason = (result_reason or "").strip()
        if reason not in INCIDENT_KINDS:
            raise HTTPException(status_code=422, detail="Motivo de incidencia inválido")
        # No marcar cantidades como recibidas
        delivered_any = any(float(x.get("delivered_units") or 0) > 0 for x in lines)
        if delivered_any:
            raise HTTPException(
                status_code=422,
                detail="Incidencia no entregada no puede tener cantidades recibidas",
            )

        want = reason
        confirmed = [
            e
            for e in evidence
            if e.get("evidence_kind") == want
            and e.get("upload_confirmed")
            and e.get("validation_status") in ("pending", "approved")
        ]
        exceptions = [
            e
            for e in evidence
            if e.get("evidence_kind") == want
            and e.get("validation_status") == "exception"
        ]

        # Local cerrado / rechazo / ausente: foto SIEMPRE obligatoria.
        if reason in PHOTO_REQUIRED_REASONS:
            if not confirmed:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        "Esta incidencia requiere fotografía del establecimiento "
                        "confirmada en el servidor antes de finalizar"
                    ),
                )
        elif confirmed:
            pass
        elif exceptions:
            review = "pending_review"
            notes.append("evidencia_excepcion")
        else:
            raise HTTPException(
                status_code=422,
                detail=(
                    "Incidencia requiere fotografía de evidencia confirmada "
                    "o excepción auditable pendiente de revisión"
                ),
            )

        # Radio 300 m para local cerrado / rechazo (servidor).
        if reason in PROXIMITY_REQUIRED_REASONS:
            if proximity_exception:
                review = "pending_review"
                notes.append("gps_proximity_exception")
            elif has_end_gps and end_lat is not None and end_lng is not None:
                try:
                    fresh = assert_fresh_gps(
                        latitude=end_lat,
                        longitude=end_lng,
                        gps_captured_at=end_gps_captured_at,
                        accuracy_m=end_accuracy_m,
                    )
                    prox = assert_within_incident_radius(
                        driver_lat=fresh["latitude"],
                        driver_lng=fresh["longitude"],
                        client_lat=client_lat,
                        client_lng=client_lng,
                    )
                    gps_meta = {**fresh, **prox}
                    if fresh.get("accuracy_warn"):
                        review = "pending_review"
                        notes.append("gps_accuracy_warn")
                except HTTPException as exc:
                    # Sin radio OK no se cierra como verificado positivo.
                    detail = exc.detail
                    raise HTTPException(status_code=exc.status_code, detail=detail) from exc
            else:
                raise HTTPException(
                    status_code=422,
                    detail={
                        "code": "GPS_REQUIRED",
                        "message": (
                            "Debes estar a menos de 300 metros del cliente "
                            "para registrar esta incidencia"
                        ),
                        "message_es": (
                            "Debes estar a menos de 300 metros del cliente "
                            "para registrar esta incidencia"
                        ),
                    },
                )

        if reason == "other" and not (result_note or "").strip():
            raise HTTPException(
                status_code=422, detail="Observación obligatoria para 'other'"
            )

    return {
        "closure_review_status": review,
        "notes": notes,
        "gps_meta": gps_meta,
    }


def _num(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def build_conformity_summary(
    *,
    visit: dict[str, Any],
    docs: list[dict[str, Any]],
    lines: list[dict[str, Any]],
    collections: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Resumen canónico firmado. Orden estable + números normalizados (anti-flap SHA).

    Incluye cobranza declarada: si cambian montos/métodos tras firmar, el SHA
    deja de coincidir y hay que renovar la conformidad.
    """
    docs_n = sorted(
        docs,
        key=lambda d: (
            int(d["related_document_id"])
            if d.get("related_document_id") is not None
            else 0,
            str(d.get("document_number") or ""),
        ),
    )
    lines_n = sorted(
        lines,
        key=lambda ln: (
            int(ln["source_detail_id"])
            if ln.get("source_detail_id") is not None
            else 0,
            int(ln["variant_id"]) if ln.get("variant_id") is not None else 0,
            str(ln.get("sku_code") or ""),
            str(ln.get("product_name") or ""),
        ),
    )
    cols_src = list(collections or [])
    cols_n = sorted(
        cols_src,
        key=lambda c: (
            int(c["document_result_id"])
            if c.get("document_result_id") is not None
            else 0,
            str(c.get("method") or ""),
            _num(c.get("declared_amount")) or 0.0,
            int(c["id"]) if c.get("id") is not None else 0,
        ),
    )
    return {
        "visit_id": int(visit["id"]),
        "version": int(visit.get("version") or 1),
        "customer_key": visit.get("customer_key"),
        "documents": [
            {
                "related_document_id": d.get("related_document_id"),
                "document_number": d.get("document_number"),
                "document_type": d.get("document_type"),
                "document_total": _num(d.get("document_total")),
                "result_status": d.get("result_status"),
            }
            for d in docs_n
        ],
        "lines": [
            {
                "source_detail_id": ln.get("source_detail_id"),
                "line_number": ln.get("line_number"),
                "variant_id": ln.get("variant_id"),
                "sku_code": ln.get("sku_code"),
                "product_name": ln.get("product_name"),
                "planned_units": _num(ln.get("planned_units")),
                "delivered_units": _num(ln.get("delivered_units")),
                "undelivered_units": _num(ln.get("undelivered_units")),
                "undelivered_reason": ln.get("undelivered_reason"),
            }
            for ln in lines_n
        ],
        "collections": [
            {
                "document_result_id": c.get("document_result_id"),
                "method": c.get("method"),
                "document_amount": _num(c.get("document_amount")),
                "declared_amount": _num(c.get("declared_amount")),
                "pending_balance": _num(c.get("pending_balance")),
                "note": c.get("note"),
            }
            for c in cols_n
        ],
    }
