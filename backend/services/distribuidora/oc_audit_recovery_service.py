"""Recuperación manual de OC desde Auditoría (simular → confirmar).

Reutiliza ``reconcile_one_oc`` / discovery existente. No crea otro motor de sync.
Una sola OC por confirmación. Sin cron.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
from base64 import urlsafe_b64decode, urlsafe_b64encode
from datetime import datetime, timezone
from typing import Any, Callable

from backend.auth.jwt import SECRET as JWT_SECRET
from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.distribuidora.oc_correlative_audit_service import (
    STATUS_MISSING_IN_PG,
    STATUS_VERIFICATION_ERROR,
    CorrelativeRangeError,
    _annotate_finding,
    investigate_folio_in_bsale,
    load_local_oc_folios,
)
from backend.services.distribuidora.oc_reconciliation_service import (
    ActiveSyncConflict,
    reconcile_one_oc,
)
from backend.services.distribuidora.oc_source_resolver import (
    COMPANY_ID,
    OC_DOCUMENT_TYPE_ID,
    OFFICE_ID,
    summarize_bsale_document,
)

logger = logging.getLogger(__name__)

SIMULATION_TTL_SECONDS = 15 * 60
SIMULATION_PURPOSE = "oc_audit_recover_v1"


class RecoveryValidationError(ValueError):
    """Simulación/confirmación rechazada de forma segura."""


class RecoveryConflictError(RuntimeError):
    """Documento cambió entre simulación y confirmación, o lock activo."""


class RecoveryTransientError(RuntimeError):
    """Error Bsale 429/502 u otro fallo transitorio; fallar seguro."""


def _int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _client_preview(document: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(document, dict):
        return {"client_id": None, "client_name": None}
    raw = document.get("client") or document.get("customer") or {}
    if not isinstance(raw, dict):
        raw = {}
    name = (
        raw.get("companyName")
        or raw.get("company_name")
        or raw.get("name")
        or document.get("clientName")
        or document.get("client_name")
    )
    cid = raw.get("id")
    if cid is None:
        cid = document.get("clientId") or document.get("client_id")
    return {"client_id": _int(cid), "client_name": name if isinstance(name, str) else None}


def _fingerprint_from_bsale(
    document: dict[str, Any] | None,
    *,
    source_hash: str | None = None,
    details_count: int | None = None,
) -> str:
    summary = summarize_bsale_document(document or {})
    payload = {
        "id": summary.get("id"),
        "number": summary.get("number"),
        "state": summary.get("state"),
        "generationDate": summary.get("generationDate"),
        "modificationDate": summary.get("modificationDate"),
        "totalAmount": summary.get("totalAmount"),
        "source_hash": source_hash,
        "details_count": details_count,
    }
    raw = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _sign_simulation(claims: dict[str, Any]) -> str:
    body = urlsafe_b64encode(
        json.dumps(claims, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).decode("ascii")
    sig = hmac.new(
        JWT_SECRET.encode("utf-8"),
        body.encode("ascii"),
        hashlib.sha256,
    ).hexdigest()
    return f"{body}.{sig}"


def _verify_simulation(token: str) -> dict[str, Any]:
    try:
        body, sig = token.split(".", 1)
    except ValueError as exc:
        raise RecoveryValidationError("simulation_token inválido") from exc
    expected = hmac.new(
        JWT_SECRET.encode("utf-8"),
        body.encode("ascii"),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(expected, sig):
        raise RecoveryValidationError("simulation_token firma inválida")
    try:
        claims = json.loads(urlsafe_b64decode(body.encode("ascii")).decode("utf-8"))
    except Exception as exc:
        raise RecoveryValidationError("simulation_token ilegible") from exc
    if not isinstance(claims, dict):
        raise RecoveryValidationError("simulation_token malformado")
    if claims.get("purpose") != SIMULATION_PURPOSE:
        raise RecoveryValidationError("simulation_token propósito incorrecto")
    exp = _int(claims.get("exp"))
    now = int(time.time())
    if exp is None or exp < now:
        raise RecoveryValidationError(
            "La simulación expiró; ejecute Simular recuperación nuevamente"
        )
    return claims


def _classify_bsale_exc(exc: BaseException) -> RecoveryTransientError | None:
    msg = str(exc)
    kind = type(exc).__name__
    if "429" in msg or "502" in msg or "503" in msg or "504" in msg:
        return RecoveryTransientError(f"Bsale temporalmente no disponible ({kind}): {msg[:200]}")
    if "rate limit" in msg.lower():
        return RecoveryTransientError(f"Bsale rate limit ({kind})")
    if "HTTP 5" in msg:
        return RecoveryTransientError(f"Bsale error HTTP ({kind}): {msg[:200]}")
    return None


def _assert_recoverable_investigation(finding: dict[str, Any], *, folio: int) -> None:
    status = str(finding.get("status") or "")
    if status == STATUS_VERIFICATION_ERROR:
        raise RecoveryTransientError(
            f"No se pudo verificar folio #{folio} en Bsale: {finding.get('error') or status}"
        )
    if status != STATUS_MISSING_IN_PG or not finding.get("recoverable"):
        raise RecoveryValidationError(
            f"Folio #{folio} no es recuperable (estado={status}). "
            "Solo missing_in_pg con versión vigente en Bsale."
        )


def simulate_oc_recovery(
    client: BsaleClient,
    *,
    folio: int,
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
    document_type_id: int = OC_DOCUMENT_TYPE_ID,
    user_id: int | None = None,
    user_email: str | None = None,
    search_fn: Callable[..., list[dict[str, Any]]] | None = None,
    local_loader: Callable[..., dict[str, Any]] | None = None,
    reconcile_fn: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Valida sin escrituras y emite token de simulación para confirmar."""
    if int(document_type_id) != int(OC_DOCUMENT_TYPE_ID):
        raise CorrelativeRangeError(
            f"Solo OC tipo {OC_DOCUMENT_TYPE_ID}; recibido={document_type_id}"
        )
    folio_n = int(folio)
    if folio_n <= 0:
        raise RecoveryValidationError("folio debe ser > 0")

    started_at = datetime.now(timezone.utc)
    loader = local_loader or load_local_oc_folios
    try:
        local = loader(
            company_id=int(company_id),
            office_id=int(office_id),
            from_folio=folio_n,
            to_folio=folio_n,
            document_type_id=int(document_type_id),
        )
        local_row = (local.get("by_folio") or {}).get(folio_n)
    except Exception:
        logger.exception("oc_recover_simulate local_loader_failed folio=%s", folio_n)
        local_row = None

    if local_row is not None and int(local_row.get("state") or 0) == 0:
        raise RecoveryValidationError(
            f"Folio #{folio_n} ya existe vigente en PostgreSQL "
            f"(document_id={local_row.get('document_id')})"
        )

    try:
        finding = investigate_folio_in_bsale(
            client,
            folio=folio_n,
            company_id=int(company_id),
            office_id=int(office_id),
            document_type_id=int(document_type_id),
            local_row=None,
            search_fn=search_fn,
        )
    except Exception as exc:
        transient = _classify_bsale_exc(exc)
        if transient:
            raise transient from exc
        raise RecoveryTransientError(f"Error consultando Bsale: {type(exc).__name__}") from exc

    finding = _annotate_finding(finding)
    _assert_recoverable_investigation(finding, folio=folio_n)

    reconcile = reconcile_fn or reconcile_one_oc
    try:
        dry = reconcile(
            client,
            folio=folio_n,
            dry_run=True,
            user_email=user_email,
            company_id=int(company_id),
            office_id=int(office_id),
        )
    except ActiveSyncConflict as exc:
        raise RecoveryConflictError(
            f"Conciliación diferida por sync activo: {exc}"
        ) from exc
    except Exception as exc:
        transient = _classify_bsale_exc(exc)
        if transient:
            raise transient from exc
        raise

    if dry.get("wrote"):
        raise RuntimeError("La simulación no debe escribir en PostgreSQL")

    bsale_doc = dry.get("bsale_document") if isinstance(dry.get("bsale_document"), dict) else {}
    # Preferir documento completo del discovery si está embebido.
    discovery = dry.get("source_discovery") if isinstance(dry.get("source_discovery"), dict) else {}
    active_summary = discovery.get("active_document") if isinstance(
        discovery.get("active_document"), dict
    ) else bsale_doc

    source_id = _int(dry.get("current_bsale_source_document_id")) or _int(
        finding.get("bsale_source_document_id")
    )
    source_hash = dry.get("source_hash")
    details_count = _int(dry.get("bsale_details_count"))
    fingerprint = _fingerprint_from_bsale(
        active_summary if isinstance(active_summary, dict) else None,
        source_hash=str(source_hash) if source_hash else None,
        details_count=details_count,
    )

    warnings: list[str] = []
    if dry.get("status") == "source_not_found":
        raise RecoveryValidationError(
            f"Folio #{folio_n}: reconcile no halló source vigente tras investigación"
        )
    if dry.get("needs_detail_sync") and (details_count or 0) == 0:
        warnings.append("Bsale reporta total pero 0 líneas en dry-run; revisar antes de confirmar")
    if dry.get("status") not in (
        "dry_run_needs_sync",
        "dry_run_in_sync",
        "dry_run_needs_weight_snapshot",
        "dry_run",
    ):
        warnings.append(f"Resultado dry-run inesperado: {dry.get('status')}")

    client_info = _client_preview(
        # dry report stores summary only; try raw fields if present
        active_summary if isinstance(active_summary, dict) else None
    )
    # Enrich client from full selected doc if reconcile left it under discovery documents.
    if client_info.get("client_name") is None:
        for cand in discovery.get("documents") or []:
            if isinstance(cand, dict) and _int(cand.get("id")) == source_id:
                # evaluated items are summaries; still try nested client
                pass

    now = int(time.time())
    claims = {
        "purpose": SIMULATION_PURPOSE,
        "folio": folio_n,
        "company_id": int(company_id),
        "office_id": int(office_id),
        "document_type_id": int(document_type_id),
        "bsale_source_document_id": source_id,
        "fingerprint": fingerprint,
        "source_hash": source_hash,
        "user_id": _int(user_id),
        "iat": now,
        "exp": now + SIMULATION_TTL_SECONDS,
    }
    token = _sign_simulation(claims)

    logger.info(
        "oc_audit_recover_simulate user=%s folio=%s company=%s office=%s "
        "bsale_source=%s dry_status=%s wrote=false",
        user_email or user_id,
        folio_n,
        company_id,
        office_id,
        source_id,
        dry.get("status"),
    )

    return {
        "status": "simulated",
        "wrote": False,
        "dry_run": True,
        "folio": folio_n,
        "company_id": int(company_id),
        "office_id": int(office_id),
        "document_type_id": int(document_type_id),
        "client": client_info,
        "bsale_source_document_id": source_id,
        "bsale_document": active_summary,
        "total_amount": (active_summary or {}).get("totalAmount")
        if isinstance(active_summary, dict)
        else finding.get("bsale_total_amount"),
        "details_count": details_count,
        "origin": {
            "bsale_source_document_id": source_id,
            "source_hash": source_hash,
            "fingerprint": fingerprint,
            "postgresql_present": local_row is not None,
            "local_document_id": (local_row or {}).get("document_id"),
        },
        "predicted_reconciliation": {
            "status": dry.get("status"),
            "needs_detail_sync": dry.get("needs_detail_sync"),
            "source_changed": dry.get("source_changed"),
            "diff_matches": (dry.get("diff") or {}).get("matches")
            if isinstance(dry.get("diff"), dict)
            else None,
        },
        "warnings": warnings,
        "investigation": finding,
        "simulation_token": token,
        "simulation_expires_at": datetime.fromtimestamp(
            claims["exp"], tz=timezone.utc
        ).isoformat(),
        "simulated_at": started_at.isoformat(),
        "simulated_by": {"user_id": _int(user_id), "email": user_email},
    }


def confirm_oc_recovery(
    client: BsaleClient,
    *,
    folio: int,
    simulation_token: str,
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
    document_type_id: int = OC_DOCUMENT_TYPE_ID,
    user_id: int | None = None,
    user_email: str | None = None,
    search_fn: Callable[..., list[dict[str, Any]]] | None = None,
    local_loader: Callable[..., dict[str, Any]] | None = None,
    reconcile_fn: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Revalida vigencia/identidad y ejecuta ``reconcile_one_oc`` (escritura)."""
    if int(document_type_id) != int(OC_DOCUMENT_TYPE_ID):
        raise CorrelativeRangeError(
            f"Solo OC tipo {OC_DOCUMENT_TYPE_ID}; recibido={document_type_id}"
        )
    folio_n = int(folio)
    claims = _verify_simulation(simulation_token)
    if int(claims.get("folio") or 0) != folio_n:
        raise RecoveryValidationError("El folio no coincide con la simulación")
    if int(claims.get("company_id") or 0) != int(company_id):
        raise RecoveryValidationError("company_id no coincide con la simulación")
    if int(claims.get("office_id") or 0) != int(office_id):
        raise RecoveryValidationError("office_id no coincide con la simulación")
    claim_user = _int(claims.get("user_id"))
    if claim_user is not None and _int(user_id) is not None and claim_user != _int(user_id):
        raise RecoveryValidationError(
            "La simulación pertenece a otro usuario; simule nuevamente"
        )

    expected_source = _int(claims.get("bsale_source_document_id"))
    expected_fp = str(claims.get("fingerprint") or "")

    loader = local_loader or load_local_oc_folios
    try:
        local = loader(
            company_id=int(company_id),
            office_id=int(office_id),
            from_folio=folio_n,
            to_folio=folio_n,
            document_type_id=int(document_type_id),
        )
        local_row = (local.get("by_folio") or {}).get(folio_n)
    except Exception:
        logger.exception("oc_recover_confirm local_loader_failed folio=%s", folio_n)
        local_row = None

    if local_row is not None and int(local_row.get("state") or 0) == 0:
        # Idempotencia: ya recuperada / presente.
        logger.info(
            "oc_audit_recover_confirm_idempotent user=%s folio=%s local_document_id=%s",
            user_email or user_id,
            folio_n,
            local_row.get("document_id"),
        )
        return {
            "status": "already_present",
            "wrote": False,
            "folio": folio_n,
            "company_id": int(company_id),
            "office_id": int(office_id),
            "local_document_id": local_row.get("document_id"),
            "postgresql_source_document_id": local_row.get("source_document_id"),
            "detail": "Ya existe una versión vigente en PostgreSQL; no se duplicó",
            "finding": _annotate_finding(
                {
                    "folio": folio_n,
                    "status": "ok",
                    "local_document_id": local_row.get("document_id"),
                    "postgresql_source_document_id": local_row.get("source_document_id"),
                    "recoverable": False,
                }
            ),
            "confirmed_by": {"user_id": _int(user_id), "email": user_email},
            "confirmed_at": datetime.now(timezone.utc).isoformat(),
        }

    try:
        finding = investigate_folio_in_bsale(
            client,
            folio=folio_n,
            company_id=int(company_id),
            office_id=int(office_id),
            document_type_id=int(document_type_id),
            local_row=None,
            search_fn=search_fn,
        )
    except Exception as exc:
        transient = _classify_bsale_exc(exc)
        if transient:
            raise transient from exc
        raise RecoveryTransientError(f"Error consultando Bsale: {type(exc).__name__}") from exc

    finding = _annotate_finding(finding)
    _assert_recoverable_investigation(finding, folio=folio_n)

    current_source = _int(finding.get("bsale_source_document_id"))
    if expected_source is not None and current_source != expected_source:
        raise RecoveryConflictError(
            "El documento Bsale cambió desde la simulación "
            f"(antes={expected_source}, ahora={current_source}). "
            "Ejecute una nueva simulación."
        )

    reconcile = reconcile_fn or reconcile_one_oc
    # Dry-run intermedio para fingerprint (sin escribir).
    try:
        dry = reconcile(
            client,
            folio=folio_n,
            dry_run=True,
            user_email=user_email,
            company_id=int(company_id),
            office_id=int(office_id),
        )
    except ActiveSyncConflict as exc:
        raise RecoveryConflictError(f"Sync activo: {exc}") from exc
    except Exception as exc:
        transient = _classify_bsale_exc(exc)
        if transient:
            raise transient from exc
        raise

    discovery = dry.get("source_discovery") if isinstance(dry.get("source_discovery"), dict) else {}
    active_summary = discovery.get("active_document") if isinstance(
        discovery.get("active_document"), dict
    ) else dry.get("bsale_document")
    current_fp = _fingerprint_from_bsale(
        active_summary if isinstance(active_summary, dict) else None,
        source_hash=str(dry.get("source_hash")) if dry.get("source_hash") else None,
        details_count=_int(dry.get("bsale_details_count")),
    )
    if current_fp != expected_fp:
        raise RecoveryConflictError(
            "La identidad/vigencia del documento cambió desde la simulación. "
            "Ejecute Simular recuperación nuevamente."
        )

    try:
        result = reconcile(
            client,
            folio=folio_n,
            dry_run=False,
            user_email=user_email,
            company_id=int(company_id),
            office_id=int(office_id),
        )
    except ActiveSyncConflict as exc:
        raise RecoveryConflictError(
            f"No se pudo adquirir lock de escritura: {exc}"
        ) from exc
    except Exception as exc:
        transient = _classify_bsale_exc(exc)
        if transient:
            raise transient from exc
        logger.exception(
            "oc_audit_recover_confirm_failed user=%s folio=%s",
            user_email or user_id,
            folio_n,
        )
        raise

    wrote = bool(result.get("wrote"))
    logger.info(
        "oc_audit_recover_confirm user=%s folio=%s company=%s office=%s "
        "bsale_source=%s status=%s wrote=%s local_document_id=%s",
        user_email or user_id,
        folio_n,
        company_id,
        office_id,
        result.get("current_bsale_source_document_id") or current_source,
        result.get("status"),
        wrote,
        result.get("local_document_id"),
    )

    post_status = "ok" if wrote or result.get("status") in (
        "synced",
        "already_in_sync",
        "synced_weight_snapshot",
        "header_ok_details_pending",
    ) else str(result.get("status") or "unknown")

    return {
        "status": "recovered" if wrote else str(result.get("status") or "completed"),
        "wrote": wrote,
        "folio": folio_n,
        "company_id": int(company_id),
        "office_id": int(office_id),
        "local_document_id": result.get("local_document_id"),
        "bsale_source_document_id": result.get("current_bsale_source_document_id")
        or current_source,
        "reconciliation": {
            "status": result.get("status"),
            "wrote": wrote,
            "dry_run": False,
            "source_hash": result.get("source_hash"),
            "bsale_details_count": result.get("bsale_details_count"),
            "postgresql_details_count": result.get("postgresql_details_count"),
        },
        "finding": _annotate_finding(
            {
                "folio": folio_n,
                "status": post_status if post_status == "ok" else STATUS_MISSING_IN_PG,
                "local_document_id": result.get("local_document_id"),
                "bsale_source_document_id": result.get("current_bsale_source_document_id")
                or current_source,
                "recoverable": False if wrote else True,
                "detail": f"Recuperación: {result.get('status')}",
            }
        ),
        "confirmed_by": {"user_id": _int(user_id), "email": user_email},
        "confirmed_at": datetime.now(timezone.utc).isoformat(),
    }
