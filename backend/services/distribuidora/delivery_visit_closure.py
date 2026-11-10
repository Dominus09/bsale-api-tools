"""Validaciones de cierre P4 (servidor). Sin mutar Bsale/picking."""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from backend.repositories.distribuidora import delivery_visit_repo as visit_repo

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
) -> dict[str, Any]:
    """Devuelve {closure_review_status, notes}. Lanza HTTPException si bloquea."""
    visit_id = int(visit["id"])
    docs = visit_repo.list_document_results(cur, visit_id)
    lines = visit_repo.list_line_results(cur, visit_id)
    evidence = visit_repo.list_evidence(cur, visit_id)
    conf = _active_conformity(cur, visit_id)

    review = "verified"
    notes: list[str] = []

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

        kind_map = {
            "closed_store": "closed_store",
            "not_found": "not_found",
            "rejected_order": "rejected_order",
            "absent_customer": "absent_customer",
            "other": "other",
        }
        want = kind_map[reason]
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
        if confirmed:
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
        if reason == "other" and not (result_note or "").strip():
            raise HTTPException(
                status_code=422, detail="Observación obligatoria para 'other'"
            )

    return {"closure_review_status": review, "notes": notes}


def build_conformity_summary(
    *,
    visit: dict[str, Any],
    docs: list[dict[str, Any]],
    lines: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "visit_id": int(visit["id"]),
        "version": int(visit.get("version") or 1),
        "customer_key": visit.get("customer_key"),
        "documents": [
            {
                "related_document_id": d.get("related_document_id"),
                "document_number": d.get("document_number"),
                "document_type": d.get("document_type"),
                "document_total": d.get("document_total"),
                "result_status": d.get("result_status"),
            }
            for d in docs
        ],
        "lines": [
            {
                "variant_id": ln.get("variant_id"),
                "sku_code": ln.get("sku_code"),
                "product_name": ln.get("product_name"),
                "planned_units": ln.get("planned_units"),
                "delivered_units": ln.get("delivered_units"),
                "undelivered_units": ln.get("undelivered_units"),
                "undelivered_reason": ln.get("undelivered_reason"),
            }
            for ln in lines
        ],
    }
