"""Cuadratura operacional de plan de despacho (v1 legacy + v2 documental + F3-D3)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from backend.db import get_connection
from backend.repositories.distribuidora import dispatch_plan_cuadratura_repo as cuad_repo
from backend.repositories.distribuidora import dispatch_plan_load_batch_repo as batch_repo
from backend.repositories.distribuidora import dispatch_plan_repo as plan_repo
from backend.services.distribuidora.dispatch_plan_service import (
    get_invoiced_documents,
    get_picking_by_client,
    get_picking_by_product,
)
from backend.utils.dispatch_plan_cuadratura import (
    compute_cuadratura_result as compute_cuadratura_v1,
    observacion_required as observacion_required_v1,
)
from backend.utils.dispatch_plan_cuadratura_picking_adopt import (
    build_proposed_documents,
    compare_document_scopes,
    merge_adopted_documents,
)
from backend.utils.dispatch_plan_cuadratura_snapshot import (
    build_documents_from_picking_clients,
    build_product_catalog_from_picking,
    normalize_credit_notes,
    normalize_not_loaded_rows,
)
from backend.utils.dispatch_plan_cuadratura_v2 import (
    MEDIO_PAGO_LABELS,
    MEDIOS_PAGO,
    build_analytics_meta,
    compute_cuadratura_v2_result,
    default_cash_count,
    derive_operational_status,
    normalize_cash_count,
    observacion_required,
    operational_status_label,
)

ACTION_ADOPT_PICKING = "adopt_cuadratura_picking"


class CuadraturaConflictError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "cuadratura_conflict",
        http_status: int = 409,
        details: dict[str, Any] | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.http_status = http_status
        self.details = details or {}


def _default_row() -> dict[str, Any]:
    return {
        "transferencia_clp": 0,
        "efectivo_clp": 0,
        "cheque_clp": 0,
        "debito_clp": 0,
        "observacion": None,
        "credit_notes": [],
        "not_loaded": [],
        "schema_version": 1,
        "status": "pending",
        "documents": [],
        "credit_notes_v2": [],
        "not_loaded_v2": [],
        "cash_count": default_cash_count(),
        "picking_id": None,
        "picking_version": None,
        "closed_at": None,
        "closed_by": None,
    }


def _staff_label(user: dict[str, Any] | None) -> str | None:
    if not user:
        return None
    return str(user.get("email") or user.get("name") or user.get("sub") or "").strip() or None


def _venta_facturada_clp(plan_id: int) -> int:
    """Σ total_amount de facturas/boletas confirmadas vigentes (dedup por documento)."""
    try:
        inv = get_invoiced_documents(plan_id)
    except Exception:
        return 0
    total = 0
    seen: set[int] = set()
    for item in inv.get("items") or []:
        if item.get("status") != "confirmed":
            continue
        try:
            doc_id = int(item.get("related_document_id") or 0)
        except (TypeError, ValueError):
            doc_id = 0
        if doc_id:
            if doc_id in seen:
                continue
            seen.add(doc_id)
        try:
            total += int(round(float(item.get("document_total") or 0)))
        except (TypeError, ValueError):
            continue
    return total


def _load_picking_bundle(
    plan_id: int,
    *,
    version: int | None = None,
    picking_id: int | None = None,
) -> dict[str, Any]:
    client_data = get_picking_by_client(
        plan_id, version=version, picking_id=picking_id
    )
    product_data = get_picking_by_product(
        plan_id, version=version, picking_id=picking_id
    )
    clients = client_data.get("clients") or []
    items = product_data.get("items") or []
    totals = client_data.get("totals") or {}
    venta_picking = int(round(float(totals.get("document_total_clp") or 0)))
    if not venta_picking:
        venta_picking = sum(
            int(round(float(c.get("document_total") or 0))) for c in clients
        )
    return {
        "ready": bool(clients) or bool(client_data.get("ready")),
        "clients": clients,
        "items": items,
        "venta_picking_clp": venta_picking,
        "picking_id": client_data.get("picking_id"),
        "picking_version": client_data.get("version"),
        "header": client_data.get("header") or {},
        "is_current": bool(client_data.get("is_current")),
    }


def _venta_picking_clp(plan_id: int) -> int:
    return _load_picking_bundle(plan_id)["venta_picking_clp"]


def _documents_venta(docs: list[dict[str, Any]]) -> int:
    total = 0
    for row in docs or []:
        try:
            total += int(round(float(row.get("monto_clp") or 0)))
        except (TypeError, ValueError):
            continue
    return total


def _build_v2_payload(
    *,
    plan_id: int,
    plan: dict[str, Any],
    saved: dict[str, Any],
    picking: dict[str, Any],
    documents: list[dict[str, Any]] | None = None,
    venta_picking_clp: int | None = None,
) -> dict[str, Any]:
    docs = documents
    if docs is None:
        docs = build_documents_from_picking_clients(
            picking["clients"],
            saved_documents=saved.get("documents") or [],
        )
    catalog = build_product_catalog_from_picking(picking.get("items") or [])
    not_loaded_v2 = normalize_not_loaded_rows(
        saved.get("not_loaded_v2") or [],
        catalog,
    )
    credit_notes_v2 = normalize_credit_notes(saved.get("credit_notes_v2") or [])
    cash_count = normalize_cash_count(saved.get("cash_count") or default_cash_count())
    venta_picking = (
        int(venta_picking_clp)
        if venta_picking_clp is not None
        else int(picking.get("venta_picking_clp") or 0)
    )
    if not venta_picking:
        venta_picking = _documents_venta(docs)
    resultado = compute_cuadratura_v2_result(
        venta_picking_clp=venta_picking,
        documents=docs,
        credit_notes=credit_notes_v2,
        cash_count=cash_count,
    )
    has_work = bool(saved.get("updated_at")) or bool(saved.get("documents"))
    op_status = saved.get("status") or derive_operational_status(
        resultado=resultado,
        closed_at=saved.get("closed_at"),
        has_work=has_work,
    )
    return {
        "schema_version": 2,
        "documents": docs,
        "credit_notes_v2": credit_notes_v2,
        "not_loaded_v2": not_loaded_v2,
        "cash_count": cash_count,
        "product_catalog": catalog,
        "resultado": resultado,
        "operational_status": op_status,
        "operational_status_label": operational_status_label(str(op_status)),
        "picking_id": picking.get("picking_id"),
        "picking_version": picking.get("picking_version"),
        "venta_picking_clp": venta_picking,
        "analytics_meta": build_analytics_meta(
            plan=plan,
            resultado=resultado,
            documents=docs,
            credit_notes=credit_notes_v2,
            not_loaded=not_loaded_v2,
        ),
    }


def _resolve_adopted_scope(
    plan_id: int,
    *,
    saved: dict[str, Any],
    current: dict[str, Any],
    closed: bool,
) -> dict[str, Any]:
    """Determina alcance oficial adoptado sin mutar persistencia en GET."""
    saved_docs = list(saved.get("documents") or [])
    adopted_version = saved.get("picking_version")
    adopted_picking_id = saved.get("picking_id")
    needs_review = False
    source = "none"

    if closed:
        # Cierre: alcance congelado en JSON persistido (histórico).
        source = "closed_snapshot"
        venta = _documents_venta(saved_docs)
        if not venta and saved.get("resultado_cache"):
            try:
                venta = int(
                    (saved["resultado_cache"] or {}).get("venta_picking_clp")
                    or (saved["resultado_cache"] or {}).get("venta_ajustada_clp")
                    or 0
                )
            except (TypeError, ValueError):
                venta = 0
        return {
            "documents": saved_docs,
            "venta_picking_clp": venta,
            "picking_id": adopted_picking_id,
            "picking_version": adopted_version,
            "picking_bundle": {
                "clients": [],
                "items": [],
                "venta_picking_clp": venta,
                "picking_id": adopted_picking_id,
                "picking_version": adopted_version,
                "header": current.get("header") or {},
                "ready": bool(saved_docs),
            },
            "adoption_needs_review": False,
            "source": source,
        }

    if adopted_version is not None:
        try:
            ver = int(adopted_version)
        except (TypeError, ValueError):
            ver = None
        if ver is not None:
            bundle = _load_picking_bundle(plan_id, version=ver)
            if bundle.get("clients"):
                docs = build_documents_from_picking_clients(
                    bundle["clients"],
                    saved_documents=saved_docs,
                )
                return {
                    "documents": docs,
                    "venta_picking_clp": bundle["venta_picking_clp"] or _documents_venta(docs),
                    "picking_id": bundle.get("picking_id") or adopted_picking_id,
                    "picking_version": bundle.get("picking_version") or ver,
                    "picking_bundle": bundle,
                    "adoption_needs_review": False,
                    "source": "adopted_picking_version",
                }
            # Versión referenciada no recuperable: no inventar con picking actual.
            needs_review = True
            source = "adopted_version_missing"

    if saved_docs:
        # Histórico sin versión confiable: conservar documentos persistidos.
        return {
            "documents": saved_docs,
            "venta_picking_clp": _documents_venta(saved_docs),
            "picking_id": adopted_picking_id,
            "picking_version": adopted_version,
            "picking_bundle": {
                "clients": [],
                "items": current.get("items") or [],
                "venta_picking_clp": _documents_venta(saved_docs),
                "picking_id": adopted_picking_id,
                "picking_version": adopted_version,
                "header": current.get("header") or {},
                "ready": True,
            },
            "adoption_needs_review": True,
            "source": source or "saved_documents_unversioned",
        }

    # Sin documentos ni versión: bootstrap de lectura = picking actual (aún no adoptado).
    docs = build_documents_from_picking_clients(
        current.get("clients") or [],
        saved_documents=[],
    )
    return {
        "documents": docs,
        "venta_picking_clp": current.get("venta_picking_clp") or _documents_venta(docs),
        "picking_id": current.get("picking_id"),
        "picking_version": current.get("picking_version"),
        "picking_bundle": current,
        "adoption_needs_review": False,
        "source": "current_preview_unstarted",
    }


def _build_picking_update(
    *,
    adopted_docs: list[dict[str, Any]],
    adopted_version: int | None,
    adopted_picking_id: int | None,
    current: dict[str, Any],
    saved_docs: list[dict[str, Any]],
    cuadratura_started: bool,
    closed: bool,
    adoption_needs_review: bool,
) -> dict[str, Any]:
    proposed = build_proposed_documents(
        current.get("clients") or [],
        saved_documents=saved_docs or adopted_docs,
    )
    diff = compare_document_scopes(adopted_docs, proposed)
    cur_ver = current.get("picking_version")
    version_drift = (
        cuadratura_started
        and cur_ver is not None
        and adopted_version is not None
        and int(cur_ver) != int(adopted_version)
    )
    has_pending = bool(
        cuadratura_started
        and not closed
        and (version_drift or diff["has_changes"] or adoption_needs_review)
    )
    return {
        "pending": has_pending,
        "adopted_picking_id": adopted_picking_id,
        "adopted_picking_version": adopted_version,
        "available_picking_id": current.get("picking_id"),
        "available_picking_version": cur_ver,
        "adoption_needs_review": adoption_needs_review,
        "version_drift": bool(version_drift),
        "documents_added_count": diff["added_count"],
        "documents_removed_count": diff["removed_count"],
        "documents_modified_count": diff["modified_count"],
        "venta_adopted_clp": diff["venta_adopted_clp"],
        "venta_available_clp": diff["venta_proposed_clp"],
        "venta_delta_clp": diff["venta_delta_clp"],
        "diff": diff,
        "proposed_documents": proposed if has_pending else None,
        "write_blocked": has_pending,
        "close_blocked": has_pending,
        "message_es": (
            (
                "Actualización documental pendiente: el picking vigente difiere del "
                f"adoptado (v{adopted_version} → v{cur_ver}). Revise y adopte "
                "explícitamente antes de guardar o cerrar."
            )
            if has_pending and adopted_version is not None and cur_ver is not None
            else (
                "Actualización documental pendiente: revise diferencias y adopte "
                "el picking vigente antes de guardar o cerrar."
                if has_pending
                else None
            )
        ),
    }


def get_dispatch_plan_cuadratura(plan_id: int) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        plan = plan_repo.get_plan_by_id(cur, plan_id)
        if not plan:
            raise ValueError("Plan no encontrado")
        cur.execute(
            """
            SELECT COALESCE(SUM(oc_total_amount), 0)
            FROM distribuidora.dispatch_plan_orders
            WHERE dispatch_plan_id = %s
            """,
            (plan_id,),
        )
        venta_oc = int(round(float((cur.fetchone() or [0])[0] or 0)))
        saved_row = cuad_repo.get_cuadratura_row(cur, plan_id)
        cuadratura_started = saved_row is not None
        saved = saved_row or _default_row()
        history = cuad_repo.list_cuadratura_history(cur, plan_id)
        cur.close()
    finally:
        conn.close()

    current = _load_picking_bundle(plan_id)
    venta_facturada = _venta_facturada_clp(plan_id)
    closed = bool(saved.get("closed_at")) or str(saved.get("status") or "").lower() == "squared"

    schema_version = int(saved.get("schema_version") or 1)
    if schema_version >= 2 or current["ready"] or (saved.get("documents")):
        adopted = _resolve_adopted_scope(
            plan_id,
            saved=saved,
            current=current,
            closed=closed,
        )
        # GET no adopta: alcance oficial = adoptado / snapshot.
        v2 = _build_v2_payload(
            plan_id=plan_id,
            plan=plan,
            saved=saved,
            picking=adopted["picking_bundle"],
            documents=adopted["documents"],
            venta_picking_clp=adopted["venta_picking_clp"],
        )
        # Exponer versión adoptada (no la vigente) en campos principales.
        v2["picking_id"] = adopted["picking_id"]
        v2["picking_version"] = adopted["picking_version"]

        picking_update = _build_picking_update(
            adopted_docs=adopted["documents"],
            adopted_version=adopted["picking_version"],
            adopted_picking_id=adopted["picking_id"],
            current=current,
            saved_docs=list(saved.get("documents") or []),
            cuadratura_started=cuadratura_started,
            closed=closed,
            adoption_needs_review=bool(adopted.get("adoption_needs_review")),
        )

        resultado = v2["resultado"]
        op_status = v2["operational_status"]
        op_label = v2["operational_status_label"]
        if not cuadratura_started:
            op_status = "not_started"
            op_label = "Cuadratura no iniciada"
        elif closed and saved.get("status"):
            op_status = saved.get("status")
            op_label = operational_status_label(str(op_status))

        return {
            "dispatch_plan_id": plan_id,
            "schema_version": 2,
            "cuadratura_started": cuadratura_started,
            "persisted": cuadratura_started,
            "picking_ready": bool(current["ready"] or adopted["documents"]),
            "picking_id": v2["picking_id"],
            "picking_version": v2["picking_version"],
            "adopted_picking_id": adopted["picking_id"],
            "adopted_picking_version": adopted["picking_version"],
            "available_picking_id": current.get("picking_id"),
            "available_picking_version": current.get("picking_version"),
            "adoption_source": adopted.get("source"),
            "adoption_needs_review": bool(adopted.get("adoption_needs_review")),
            "header": current.get("header") or adopted["picking_bundle"].get("header") or {},
            "ventas": {
                "venta_oc_clp": venta_oc,
                "venta_facturada_clp": venta_facturada,
                # Base de balance = alcance adoptado (no picking vigente no adoptado).
                "venta_picking_clp": v2["venta_picking_clp"],
                "amounts_basis_label_es": "Total documento (con IVA)",
            },
            "documents": v2["documents"],
            "resumen_pagos": resultado["resumen_pagos"],
            "resumen_pagos_labels": MEDIO_PAGO_LABELS,
            "medios_pago_options": list(MEDIOS_PAGO),
            "credit_notes_v2": v2["credit_notes_v2"],
            "not_loaded_v2": v2["not_loaded_v2"],
            "cash_count": v2["cash_count"],
            "product_catalog": v2["product_catalog"],
            "observacion": saved.get("observacion"),
            "operational_status": op_status,
            "operational_status_label": op_label,
            "closed_at": saved.get("closed_at") if cuadratura_started else None,
            "closed_by": saved.get("closed_by") if cuadratura_started else None,
            "resultado": resultado,
            "observacion_required": observacion_required(resultado),
            "history": history,
            "analytics_meta": v2["analytics_meta"],
            "picking_update": picking_update,
            "write_blocked_by_picking_update": bool(picking_update.get("write_blocked")),
            "legacy": _legacy_block(saved, v2["venta_picking_clp"]),
        }

    legacy = _legacy_response(
        plan_id, venta_oc, venta_facturada, current["venta_picking_clp"], saved
    )
    legacy["cuadratura_started"] = cuadratura_started
    legacy["persisted"] = cuadratura_started
    if not cuadratura_started:
        legacy["operational_status"] = "not_started"
        legacy["operational_status_label"] = "Cuadratura no iniciada"
    return legacy


def _legacy_block(saved: dict[str, Any], venta_picking: int) -> dict[str, Any]:
    credit_notes = saved.get("credit_notes") or []
    not_loaded = saved.get("not_loaded") or []
    result = compute_cuadratura_v1(
        venta_picking_clp=venta_picking,
        credit_notes=credit_notes if isinstance(credit_notes, list) else [],
        not_loaded=not_loaded if isinstance(not_loaded, list) else [],
        transferencia_clp=int(saved.get("transferencia_clp") or 0),
        efectivo_clp=int(saved.get("efectivo_clp") or 0),
        cheque_clp=int(saved.get("cheque_clp") or 0),
        debito_clp=int(saved.get("debito_clp") or 0),
    )
    return {
        "pagos": {
            "transferencia_clp": int(saved.get("transferencia_clp") or 0),
            "efectivo_clp": int(saved.get("efectivo_clp") or 0),
            "cheque_clp": int(saved.get("cheque_clp") or 0),
            "debito_clp": int(saved.get("debito_clp") or 0),
        },
        "credit_notes": credit_notes,
        "not_loaded": not_loaded,
        "resultado": result,
    }


def _legacy_response(
    plan_id: int,
    venta_oc: int,
    venta_facturada: int,
    venta_picking: int,
    saved: dict[str, Any],
) -> dict[str, Any]:
    credit_notes = saved.get("credit_notes") or []
    not_loaded = saved.get("not_loaded") or []
    if isinstance(credit_notes, str):
        credit_notes = []
    if isinstance(not_loaded, str):
        not_loaded = []
    result = compute_cuadratura_v1(
        venta_picking_clp=venta_picking,
        credit_notes=credit_notes if isinstance(credit_notes, list) else [],
        not_loaded=not_loaded if isinstance(not_loaded, list) else [],
        transferencia_clp=int(saved.get("transferencia_clp") or 0),
        efectivo_clp=int(saved.get("efectivo_clp") or 0),
        cheque_clp=int(saved.get("cheque_clp") or 0),
        debito_clp=int(saved.get("debito_clp") or 0),
    )
    return {
        "dispatch_plan_id": plan_id,
        "schema_version": 1,
        "ventas": {
            "venta_oc_clp": venta_oc,
            "venta_facturada_clp": venta_facturada,
            "venta_picking_clp": venta_picking,
        },
        "pagos": {
            "transferencia_clp": int(saved.get("transferencia_clp") or 0),
            "efectivo_clp": int(saved.get("efectivo_clp") or 0),
            "cheque_clp": int(saved.get("cheque_clp") or 0),
            "debito_clp": int(saved.get("debito_clp") or 0),
        },
        "credit_notes": credit_notes,
        "not_loaded": not_loaded,
        "observacion": saved.get("observacion"),
        "resultado": result,
        "observacion_required": observacion_required_v1(int(result["diferencia_clp"])),
    }


def save_dispatch_plan_cuadratura(plan_id: int, body: dict[str, Any]) -> dict[str, Any]:
    if body.get("documents") is not None or body.get("schema_version") == 2:
        return save_dispatch_plan_cuadratura_v2(plan_id, body)
    return _save_dispatch_plan_cuadratura_v1(plan_id, body)


def save_dispatch_plan_cuadratura_v2(plan_id: int, body: dict[str, Any]) -> dict[str, Any]:
    preview = get_dispatch_plan_cuadratura(plan_id)
    if not preview.get("picking_ready"):
        raise ValueError("Se requiere picking generado para cuadratura documental.")
    if preview.get("closed_at"):
        raise CuadraturaConflictError(
            "Cuadratura cerrada: no se permiten modificaciones ordinarias.",
            code="cuadratura_closed",
            details={"closed_at": preview.get("closed_at")},
        )
    if preview.get("write_blocked_by_picking_update") or (
        preview.get("picking_update") or {}
    ).get("write_blocked"):
        upd = preview.get("picking_update") or {}
        raise CuadraturaConflictError(
            upd.get("message_es")
            or "Existe una nueva versión de picking. Revise y adopte antes de guardar.",
            code="picking_update_pending",
            details=upd,
        )

    current = _load_picking_bundle(plan_id)
    adopted_version = preview.get("adopted_picking_version")
    expected = body.get("expected_picking_version")
    if expected is None:
        expected = body.get("adopted_picking_version")
    # Bootstrap (primera escritura): pin a picking vigente.
    is_bootstrap = not preview.get("cuadratura_started")
    if is_bootstrap:
        pin_version = current.get("picking_version")
        pin_id = current.get("picking_id")
        scope_clients = current.get("clients") or []
    else:
        if expected is not None and adopted_version is not None:
            if int(expected) != int(adopted_version):
                raise CuadraturaConflictError(
                    "La versión de picking de la sesión no coincide con la adoptada. "
                    "Recargue la cuadratura.",
                    code="picking_version_mismatch",
                    details={
                        "expected_picking_version": expected,
                        "adopted_picking_version": adopted_version,
                        "available_picking_version": current.get("picking_version"),
                    },
                )
        pin_version = adopted_version
        pin_id = preview.get("adopted_picking_id")
        if pin_version is not None:
            scope_clients = _load_picking_bundle(
                plan_id, version=int(pin_version)
            ).get("clients") or []
        else:
            # Histórico sin versión: no ampliar alcance en save ordinario.
            scope_clients = []

    catalog = build_product_catalog_from_picking(
        current.get("items")
        if is_bootstrap
        else (
            _load_picking_bundle(plan_id, version=int(pin_version)).get("items")
            if pin_version is not None
            else current.get("items") or []
        )
    )
    documents = body.get("documents")
    if documents is None:
        if is_bootstrap:
            documents = build_documents_from_picking_clients(
                scope_clients,
                saved_documents=preview.get("documents") or [],
            )
        else:
            documents = preview.get("documents") or []
    elif not is_bootstrap and pin_version is not None and scope_clients:
        # No permitir colar docs fuera del alcance adoptado.
        allowed = {
            f"id:{int(c['related_document_id'])}"
            for c in scope_clients
            if c.get("related_document_id") is not None
        }
        allowed |= {
            f"num:{c['document_number']}"
            for c in scope_clients
            if c.get("related_document_id") is None and c.get("document_number") is not None
        }
        from backend.utils.dispatch_plan_cuadratura_picking_adopt import document_key

        filtered = []
        for row in documents:
            key = document_key(row)
            if key in allowed or not allowed:
                filtered.append(row)
        documents = filtered

    credit_notes_v2 = normalize_credit_notes(
        body.get("credit_notes_v2") or body.get("credit_notes") or []
    )
    not_loaded_v2 = normalize_not_loaded_rows(
        body.get("not_loaded_v2") or body.get("not_loaded") or [],
        catalog,
    )
    cash_count = normalize_cash_count(
        body.get("cash_count")
        if body.get("cash_count") is not None
        else preview.get("cash_count") or default_cash_count()
    )
    venta_picking = (
        int(current.get("venta_picking_clp") or 0)
        if is_bootstrap
        else int(preview["ventas"]["venta_picking_clp"])
    )
    if not is_bootstrap and not venta_picking:
        venta_picking = _documents_venta(documents)
    resultado = compute_cuadratura_v2_result(
        venta_picking_clp=venta_picking,
        documents=documents,
        credit_notes=credit_notes_v2,
        cash_count=cash_count,
    )
    obs = (body.get("observacion") or "").strip()
    if observacion_required(resultado) and not obs:
        raise ValueError(
            "Debe ingresar una observación cuando hay diferencia general o de efectivo."
        )

    op_status = derive_operational_status(
        resultado=resultado,
        closed_at=None,
        has_work=True,
    )

    conn = get_connection()
    try:
        cur = conn.cursor()
        if not plan_repo.get_plan_by_id(cur, plan_id):
            raise ValueError("Plan no encontrado")
        locked = cuad_repo.lock_cuadratura_row(cur, plan_id)
        if locked and locked.get("closed_at"):
            raise CuadraturaConflictError(
                "Cuadratura cerrada: no se permiten modificaciones ordinarias.",
                code="cuadratura_closed",
            )
        if locked and not is_bootstrap:
            # Revalidar drift bajo lock.
            live = _load_picking_bundle(plan_id)
            if (
                locked.get("picking_version") is not None
                and live.get("picking_version") is not None
                and int(locked["picking_version"]) != int(live["picking_version"])
            ):
                raise CuadraturaConflictError(
                    "El picking cambió mientras editaba. Recargue y adopte la nueva versión.",
                    code="picking_update_pending",
                    details={
                        "adopted_picking_version": locked.get("picking_version"),
                        "available_picking_version": live.get("picking_version"),
                    },
                )
        cuad_repo.upsert_cuadratura_v2(
            cur,
            plan_id=plan_id,
            observacion=obs or None,
            documents=documents,
            credit_notes_v2=credit_notes_v2,
            not_loaded_v2=not_loaded_v2,
            cash_count=cash_count,
            picking_id=pin_id,
            picking_version=pin_version,
            status=str(op_status),
            resultado_cache=resultado,
            update_closed=False,
        )
        conn.commit()
        cur.close()
    except CuadraturaConflictError:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return get_dispatch_plan_cuadratura(plan_id)


def adopt_cuadratura_picking(
    plan_id: int,
    *,
    user: dict[str, Any] | None = None,
    expected_available_version: int | None = None,
    force_drop_unworked: bool = True,
) -> dict[str, Any]:
    """Adopta el picking vigente como alcance oficial. Atómico e idempotente."""
    actor = _staff_label(user)
    conn = get_connection()
    try:
        cur = conn.cursor()
        if not plan_repo.get_plan_by_id(cur, plan_id):
            raise ValueError("Plan no encontrado")
        locked = cuad_repo.lock_cuadratura_row(cur, plan_id)
        if not locked:
            raise CuadraturaConflictError(
                "Inicie y guarde la cuadratura antes de adoptar un picking.",
                code="cuadratura_not_started",
                http_status=409,
            )
        if locked.get("closed_at") or str(locked.get("status") or "").lower() == "squared":
            raise CuadraturaConflictError(
                "Cuadratura cerrada: no se puede adoptar otra versión de picking.",
                code="cuadratura_closed",
            )

        current = _load_picking_bundle(plan_id)
        if not current.get("ready") or current.get("picking_version") is None:
            raise ValueError("No hay picking vigente para adoptar.")
        avail_ver = int(current["picking_version"])
        if (
            expected_available_version is not None
            and int(expected_available_version) != avail_ver
        ):
            raise CuadraturaConflictError(
                "La versión de picking disponible cambió. Recargue e intente de nuevo.",
                code="picking_concurrency",
                details={
                    "expected_available_version": expected_available_version,
                    "available_picking_version": avail_ver,
                },
            )

        prev_ver = locked.get("picking_version")
        if prev_ver is not None and int(prev_ver) == avail_ver:
            # Idempotente: ya adoptado.
            batch_repo.insert_order_event(
                cur,
                plan_id=plan_id,
                action=ACTION_ADOPT_PICKING,
                user_name=actor,
                reason="idempotent_already_adopted",
                picking_id=current.get("picking_id"),
                picking_version=avail_ver,
                payload={
                    "status": "unchanged",
                    "adopted_picking_version": avail_ver,
                    "previous_picking_version": prev_ver,
                },
            )
            conn.commit()
            cur.close()
            out = get_dispatch_plan_cuadratura(plan_id)
            out["adopt_status"] = "unchanged"
            return out

        adopted_docs = list(locked.get("documents") or [])
        if prev_ver is not None:
            try:
                prev_bundle = _load_picking_bundle(plan_id, version=int(prev_ver))
                if prev_bundle.get("clients"):
                    adopted_docs = build_documents_from_picking_clients(
                        prev_bundle["clients"],
                        saved_documents=adopted_docs,
                    )
            except Exception:
                pass

        proposed = build_proposed_documents(
            current.get("clients") or [],
            saved_documents=list(locked.get("documents") or []),
        )
        try:
            merged, diff = merge_adopted_documents(
                adopted_docs=adopted_docs,
                proposed_docs=proposed,
                allow_drop_without_work=force_drop_unworked,
            )
        except ValueError as exc:
            raise CuadraturaConflictError(
                str(exc),
                code="removed_docs_with_financial_work",
                details=compare_document_scopes(adopted_docs, proposed),
            ) from exc

        credit_notes_v2 = normalize_credit_notes(locked.get("credit_notes_v2") or [])
        catalog = build_product_catalog_from_picking(current.get("items") or [])
        not_loaded_v2 = normalize_not_loaded_rows(
            locked.get("not_loaded_v2") or [],
            catalog,
        )
        cash_count = normalize_cash_count(
            locked.get("cash_count") or default_cash_count()
        )
        venta_picking = int(current.get("venta_picking_clp") or 0) or _documents_venta(
            merged
        )
        resultado = compute_cuadratura_v2_result(
            venta_picking_clp=venta_picking,
            documents=merged,
            credit_notes=credit_notes_v2,
            cash_count=cash_count,
        )
        op_status = derive_operational_status(
            resultado=resultado,
            closed_at=None,
            has_work=True,
        )
        cuad_repo.upsert_cuadratura_v2(
            cur,
            plan_id=plan_id,
            observacion=locked.get("observacion"),
            documents=merged,
            credit_notes_v2=credit_notes_v2,
            not_loaded_v2=not_loaded_v2,
            cash_count=cash_count,
            picking_id=current.get("picking_id"),
            picking_version=avail_ver,
            status=str(op_status),
            resultado_cache=resultado,
            update_closed=False,
        )
        batch_repo.insert_order_event(
            cur,
            plan_id=plan_id,
            action=ACTION_ADOPT_PICKING,
            user_name=actor,
            reason="adopt_picking_current",
            picking_id=current.get("picking_id"),
            picking_version=avail_ver,
            payload={
                "status": "adopted",
                "previous_picking_version": prev_ver,
                "adopted_picking_version": avail_ver,
                "previous_picking_id": locked.get("picking_id"),
                "adopted_picking_id": current.get("picking_id"),
                "diff": {
                    "added_count": diff["added_count"],
                    "removed_count": diff["removed_count"],
                    "modified_count": diff["modified_count"],
                    "venta_delta_clp": diff["venta_delta_clp"],
                    "documents_added": diff["documents_added"],
                    "documents_removed": diff["documents_removed"],
                    "documents_modified": diff["documents_modified"],
                },
                "preserved": {
                    "credit_notes": True,
                    "cash_count": True,
                    "observacion": True,
                    "document_medios": True,
                },
            },
        )
        conn.commit()
        cur.close()
    except CuadraturaConflictError:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    out = get_dispatch_plan_cuadratura(plan_id)
    out["adopt_status"] = "adopted"
    return out


def close_dispatch_plan_cuadratura(
    plan_id: int,
    *,
    closed_by: str | None = None,
    observacion: str | None = None,
) -> dict[str, Any]:
    data = get_dispatch_plan_cuadratura(plan_id)
    if int(data.get("schema_version") or 1) < 2:
        raise ValueError("Cuadratura v2 requerida para cierre.")
    if data.get("closed_at"):
        raise CuadraturaConflictError(
            "La cuadratura ya está cerrada.",
            code="cuadratura_closed",
        )
    if data.get("write_blocked_by_picking_update") or (
        data.get("picking_update") or {}
    ).get("close_blocked"):
        upd = data.get("picking_update") or {}
        raise CuadraturaConflictError(
            upd.get("message_es")
            or "No se puede cerrar con actualización documental pendiente.",
            code="picking_update_pending",
            details=upd,
        )
    resultado = data["resultado"]
    obs = (observacion or data.get("observacion") or "").strip()
    if observacion_required(resultado) and not obs:
        raise ValueError(
            "No se puede cerrar la cuadratura sin observación cuando hay diferencia."
        )
    op_status = derive_operational_status(
        resultado=resultado,
        closed_at=datetime.now(timezone.utc),
        has_work=True,
    )
    diff = int(
        resultado.get("diferencia_general_clp")
        or resultado.get("diferencia_clp")
        or 0
    )
    now = datetime.now(timezone.utc)
    snapshot = {
        "ventas": data.get("ventas"),
        "documents": data.get("documents"),
        "resumen_pagos": data.get("resumen_pagos"),
        "credit_notes_v2": data.get("credit_notes_v2"),
        "not_loaded_v2": data.get("not_loaded_v2"),
        "cash_count": data.get("cash_count"),
        "resultado": resultado,
        "observacion": obs,
        "picking_id": data.get("adopted_picking_id") or data.get("picking_id"),
        "picking_version": data.get("adopted_picking_version")
        or data.get("picking_version"),
    }

    conn = get_connection()
    try:
        cur = conn.cursor()
        locked = cuad_repo.lock_cuadratura_row(cur, plan_id)
        if locked and locked.get("closed_at"):
            raise CuadraturaConflictError(
                "La cuadratura ya está cerrada.",
                code="cuadratura_closed",
            )
        # Drift bajo lock.
        live = _load_picking_bundle(plan_id)
        if (
            locked
            and locked.get("picking_version") is not None
            and live.get("picking_version") is not None
            and int(locked["picking_version"]) != int(live["picking_version"])
        ):
            raise CuadraturaConflictError(
                "El picking cambió: adopte la nueva versión antes de cerrar.",
                code="picking_update_pending",
            )
        version = cuad_repo.next_history_version(cur, plan_id)
        cuad_repo.insert_cuadratura_history(
            cur,
            plan_id=plan_id,
            version=version,
            status=op_status,
            snapshot=snapshot,
            closed_by=closed_by,
            observacion=obs,
            diferencia_clp=diff,
            diferencia_status=str(data["resultado"]["diferencia_status"]),
        )
        cuad_repo.upsert_cuadratura_v2(
            cur,
            plan_id=plan_id,
            observacion=obs,
            documents=data.get("documents") or [],
            credit_notes_v2=data.get("credit_notes_v2") or [],
            not_loaded_v2=data.get("not_loaded_v2") or [],
            cash_count=data.get("cash_count") or default_cash_count(),
            picking_id=data.get("adopted_picking_id") or data.get("picking_id"),
            picking_version=data.get("adopted_picking_version")
            or data.get("picking_version"),
            status=op_status,
            resultado_cache=data.get("resultado") or {},
            closed_at=now,
            closed_by=closed_by,
            update_closed=True,
        )
        conn.commit()
        cur.close()
    except CuadraturaConflictError:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return get_dispatch_plan_cuadratura(plan_id)


def list_cuadraturas(
    *,
    status: str = "all",
    search: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        items = cuad_repo.list_cuadraturas(
            cur,
            status_filter=status,
            search=search,
            limit=limit,
            offset=offset,
        )
        cur.close()
    finally:
        conn.close()
    for item in items:
        st = item.get("cuadratura_status") or "pending"
        item["operational_status_label"] = operational_status_label(str(st))
    return {"items": items, "count": len(items)}


def _save_dispatch_plan_cuadratura_v1(plan_id: int, body: dict[str, Any]) -> dict[str, Any]:
    preview = get_dispatch_plan_cuadratura(plan_id)
    if preview.get("closed_at"):
        raise CuadraturaConflictError(
            "Cuadratura cerrada: no se permiten modificaciones ordinarias.",
            code="cuadratura_closed",
        )
    legacy = preview.get("legacy") or preview
    venta_picking = int(
        preview.get("ventas", {}).get("venta_picking_clp")
        or legacy.get("ventas", {}).get("venta_picking_clp")
        or _venta_picking_clp(plan_id)
    )
    body_result = compute_cuadratura_v1(
        venta_picking_clp=venta_picking,
        credit_notes=body.get("credit_notes") or [],
        not_loaded=body.get("not_loaded") or [],
        transferencia_clp=int(body.get("transferencia_clp") or 0),
        efectivo_clp=int(body.get("efectivo_clp") or 0),
        cheque_clp=int(body.get("cheque_clp") or 0),
        debito_clp=int(body.get("debito_clp") or 0),
    )
    diff = int(body_result["diferencia_clp"])
    obs = (body.get("observacion") or "").strip()
    if observacion_required_v1(diff) and not obs:
        raise ValueError(
            "Debe ingresar una observación cuando la diferencia es distinta de cero."
        )

    credit_notes = body.get("credit_notes") or []
    not_loaded = body.get("not_loaded") or []
    if not isinstance(credit_notes, list):
        raise ValueError("credit_notes debe ser una lista")
    if not isinstance(not_loaded, list):
        raise ValueError("not_loaded debe ser una lista")

    conn = get_connection()
    try:
        cur = conn.cursor()
        if not plan_repo.get_plan_by_id(cur, plan_id):
            raise ValueError("Plan no encontrado")
        cuad_repo.upsert_cuadratura(
            cur,
            plan_id=plan_id,
            transferencia_clp=int(body.get("transferencia_clp") or 0),
            efectivo_clp=int(body.get("efectivo_clp") or 0),
            cheque_clp=int(body.get("cheque_clp") or 0),
            debito_clp=int(body.get("debito_clp") or 0),
            observacion=obs or None,
            credit_notes=credit_notes,
            not_loaded=not_loaded,
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()
    return get_dispatch_plan_cuadratura(plan_id)
