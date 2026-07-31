"""Sincronización incremental / catchup de Costos V2 (reutiliza motor + persistencia E.4)."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from backend.repositories.cost_v2_backfill_repo import CostV2BackfillRepository
from backend.services.analytics.cost_v2_backfill import (
    DEFAULT_COMMIT_BATCH_SIZE,
    MAX_COMMIT_BATCH_SIZE,
    MAX_TIMEOUT,
    CostV2BackfillArgs,
    _calculate_single_row,
    _fingerprints_unchanged,
    _process_backfill_batch,
    build_variant_net_outlier_stats,
)
from backend.services.analytics.cost_v2_calculator import CALCULATION_VERSION
from backend.services.analytics.validate_distribuidora_source import (
    AnalyticsValidationError,
)

DEFAULT_LOOKBACK_DAYS = 45
DEFAULT_MAX_CANDIDATES = 2000
DISCOVERY_PAGE = 500


@dataclass(frozen=True, slots=True)
class CostV2SyncArgs:
    mode: str  # catchup | incremental
    company_id: int
    office_id: int
    dry_run: bool
    apply: bool
    calculation_version: str
    commit_batch_size: int
    statement_timeout_seconds: int
    date_from: date | None = None
    date_to: date | None = None
    confirm_candidate_count: int | None = None
    lookback_days: int = DEFAULT_LOOKBACK_DAYS
    max_candidates: int = DEFAULT_MAX_CANDIDATES
    max_batches: int | None = None
    start_after_history_id: int = 0


def clamp_sync_args(
    *,
    mode: str,
    company_id: int,
    office_id: int | None,
    dry_run: bool = True,
    apply: bool = False,
    calculation_version: str = CALCULATION_VERSION,
    commit_batch_size: int = DEFAULT_COMMIT_BATCH_SIZE,
    statement_timeout_seconds: int = 30,
    date_from: date | None = None,
    date_to: date | None = None,
    confirm_candidate_count: int | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
    max_batches: int | None = None,
    start_after_history_id: int = 0,
) -> CostV2SyncArgs:
    mode_n = (mode or "").strip().lower()
    if mode_n not in ("catchup", "incremental"):
        raise AnalyticsValidationError(
            f"modo desconocido: {mode}",
            error_type="invalid_mode",
        )
    if apply and dry_run:
        raise AnalyticsValidationError(
            "--dry-run y --apply no pueden usarse juntos",
            error_type="apply_dry_run_conflict",
        )
    if int(company_id) <= 0:
        raise AnalyticsValidationError(
            "company_id is required and must be > 0",
            error_type="invalid_args",
        )
    if office_id is None or int(office_id) <= 0:
        raise AnalyticsValidationError(
            "office_id es obligatorio",
            error_type="office_required",
        )
    cbs = int(commit_batch_size)
    if cbs <= 0 or cbs > MAX_COMMIT_BATCH_SIZE:
        raise AnalyticsValidationError(
            "commit-batch-size debe estar entre 1 y 500",
            error_type="invalid_batch_size",
            details={"commit_batch_size": cbs, "cap": MAX_COMMIT_BATCH_SIZE},
        )
    start_after = int(start_after_history_id or 0)
    if start_after < 0:
        raise AnalyticsValidationError(
            "start-after-history-id debe ser >= 0",
            error_type="invalid_start_after",
        )
    mb = None if max_batches is None else int(max_batches)
    if mb is not None and mb <= 0:
        raise AnalyticsValidationError(
            "max-batches debe ser > 0 cuando se informa",
            error_type="invalid_max_batches",
        )
    version = str(calculation_version or "").strip() or CALCULATION_VERSION

    if mode_n == "catchup":
        if date_from is None or date_to is None:
            raise AnalyticsValidationError(
                "catchup requiere --date-from y --date-to",
                error_type="catchup_dates_required",
            )
        if date_to < date_from:
            raise AnalyticsValidationError(
                "date_to must be >= date_from",
                error_type="invalid_args",
            )
        if apply and confirm_candidate_count is None:
            raise AnalyticsValidationError(
                "catchup apply requiere --confirm-candidate-count",
                error_type="catchup_confirm_required",
            )
        if confirm_candidate_count is not None and int(confirm_candidate_count) < 0:
            raise AnalyticsValidationError(
                "confirm-candidate-count debe ser >= 0",
                error_type="catchup_confirm_invalid",
            )
    else:
        if date_from is not None or date_to is not None:
            raise AnalyticsValidationError(
                "incremental no acepta date-from/date-to (use catchup/repair)",
                error_type="incremental_dates_forbidden",
            )
        if confirm_candidate_count is not None:
            raise AnalyticsValidationError(
                "incremental no usa --confirm-candidate-count",
                error_type="incremental_confirm_forbidden",
            )
        if int(lookback_days) <= 0:
            raise AnalyticsValidationError(
                "lookback-days debe ser > 0",
                error_type="invalid_lookback",
            )
        if int(max_candidates) <= 0:
            raise AnalyticsValidationError(
                "max-candidates debe ser > 0",
                error_type="invalid_max_candidates",
            )

    return CostV2SyncArgs(
        mode=mode_n,
        company_id=int(company_id),
        office_id=int(office_id),
        dry_run=not apply,
        apply=bool(apply),
        calculation_version=version,
        commit_batch_size=cbs,
        statement_timeout_seconds=max(1, min(int(statement_timeout_seconds), MAX_TIMEOUT)),
        date_from=date_from,
        date_to=date_to,
        confirm_candidate_count=(
            int(confirm_candidate_count) if confirm_candidate_count is not None else None
        ),
        lookback_days=int(lookback_days),
        max_candidates=int(max_candidates),
        max_batches=mb,
        start_after_history_id=start_after,
    )


def _backfill_stub(args: CostV2SyncArgs) -> CostV2BackfillArgs:
    today = date.today()
    return CostV2BackfillArgs(
        company_id=args.company_id,
        office_id=args.office_id,
        date_from=args.date_from or (today - timedelta(days=3650)),
        date_to=args.date_to or today,
        dry_run=False,
        batch_size=DISCOVERY_PAGE,
        sample_limit=20,
        statement_timeout_seconds=args.statement_timeout_seconds,
        calculation_version=args.calculation_version,
        apply=True,
    )


def _classify_row(
    *,
    repository: CostV2BackfillRepository,
    row: dict[str, Any],
    tax_catalog: dict,
    outlier_stats: dict,
    calculation_version: str,
) -> tuple[str, Any]:
    """Retorna (kind, calc) con kind in new|changed|unchanged."""
    calc = _calculate_single_row(
        row=row,
        tax_catalog=tax_catalog,
        outlier_stats=outlier_stats,
        calculation_version=calculation_version,
    )
    existing = repository.get_existing_calculation(
        history_id=calc.history_id,
        calculation_version=calc.calculation_version,
    )
    if existing is None:
        return "new", calc
    if _fingerprints_unchanged(existing, calc):
        return "unchanged", calc
    return "changed", calc


def _prepare_batch_context(
    *,
    repository: CostV2BackfillRepository,
    args: CostV2SyncArgs,
    rows: list[dict[str, Any]],
) -> tuple[dict, dict]:
    variant_ids = sorted({int(r["variant_id"]) for r in rows})
    outlier_stats = build_variant_net_outlier_stats(
        repository.fetch_outlier_baseline_cost_nets(
            company_id=args.company_id,
            office_id=args.office_id,
            variant_ids=variant_ids,
        )
    )
    all_tax_ids: list[int] = []
    for r in rows:
        all_tax_ids.extend(r.get("catalog_tax_ids") or [])
    tax_catalog = repository.fetch_taxes_for_ids(
        company_id=args.company_id, tax_ids=all_tax_ids
    )
    return tax_catalog, outlier_stats


def discover_candidates(
    *,
    args: CostV2SyncArgs,
    repository: CostV2BackfillRepository,
) -> dict[str, Any]:
    """Descubre candidatos (new/changed) sin escribir. No carga todo el scope a la vez."""
    new_ids: list[int] = []
    changed_ids: list[int] = []
    page = DISCOVERY_PAGE

    # A) Missing
    after = 0
    while True:
        if args.mode == "incremental" and len(new_ids) + len(changed_ids) >= args.max_candidates:
            break
        batch = repository.fetch_missing_calculated_batch(
            company_id=args.company_id,
            office_id=args.office_id,
            calculation_version=args.calculation_version,
            after_id=after,
            batch_size=page,
            date_from=args.date_from if args.mode == "catchup" else None,
            date_to=args.date_to if args.mode == "catchup" else None,
        )
        if not batch:
            break
        for r in batch:
            hid = int(r["history_id"])
            if hid <= args.start_after_history_id:
                continue
            new_ids.append(hid)
            if args.mode == "incremental" and len(new_ids) + len(changed_ids) >= args.max_candidates:
                break
        after = int(batch[-1]["history_id"])
        if len(batch) < page:
            break
        if args.mode == "incremental" and len(new_ids) + len(changed_ids) >= args.max_candidates:
            break

    # B) Rechequeo fingerprints en ventana
    if args.mode == "catchup":
        win_from = args.date_from
        win_to = args.date_to
    else:
        # lookback por admission_date (no hay updated_at confiable)
        win_to = date.today()
        win_from = win_to - timedelta(days=int(args.lookback_days))

    if win_from is not None and win_to is not None:
        after = 0
        while True:
            if args.mode == "incremental" and len(new_ids) + len(changed_ids) >= args.max_candidates:
                break
            batch = repository.fetch_calculated_window_batch(
                company_id=args.company_id,
                office_id=args.office_id,
                calculation_version=args.calculation_version,
                date_from=win_from,
                date_to=win_to,
                after_id=after,
                batch_size=page,
            )
            if not batch:
                break
            tax_catalog, outlier_stats = _prepare_batch_context(
                repository=repository, args=args, rows=batch
            )
            for r in batch:
                hid = int(r["history_id"])
                if hid <= args.start_after_history_id:
                    continue
                if hid in new_ids:
                    continue
                kind, _calc = _classify_row(
                    repository=repository,
                    row=r,
                    tax_catalog=tax_catalog,
                    outlier_stats=outlier_stats,
                    calculation_version=args.calculation_version,
                )
                if kind == "changed":
                    changed_ids.append(hid)
                    if (
                        args.mode == "incremental"
                        and len(new_ids) + len(changed_ids) >= args.max_candidates
                    ):
                        break
            after = int(batch[-1]["history_id"])
            if len(batch) < page:
                break
            if args.mode == "incremental" and len(new_ids) + len(changed_ids) >= args.max_candidates:
                break

    candidate_ids = sorted(set(new_ids) | set(changed_ids))
    if args.mode == "incremental":
        candidate_ids = candidate_ids[: int(args.max_candidates)]

    return {
        "candidate_ids": candidate_ids,
        "new": len(new_ids),
        "changed": len(changed_ids),
        "total": len(candidate_ids),
        "lookback_from": win_from.isoformat() if win_from else None,
        "lookback_to": win_to.isoformat() if win_to else None,
        "limitation": (
            "history no tiene updated_at/synced_at; sync es append-only (DO NOTHING). "
            "Cambios se detectan recalculando fingerprints en ventana admission_date "
            "(lookback o fechas catchup). Filas nuevas se detectan sin límite de lookback."
        ),
    }


def run_cost_v2_sync(
    *,
    args: CostV2SyncArgs,
    repository: CostV2BackfillRepository,
    commit_fn: Any | None = None,
    rollback_fn: Any | None = None,
    emit_fn: Any | None = None,
) -> dict[str, Any]:
    """Dry-run o apply catchup/incremental."""
    if not repository.calculated_table_exists():
        raise AnalyticsValidationError(
            "Tabla analytics.cost_reception_calculated no existe",
            error_type="destination_table_missing",
        )

    audit = repository.fetch_history_sync_audit(
        company_id=args.company_id,
        office_id=args.office_id,
        after_admission_date=date(2026, 6, 22),
        calculation_version=args.calculation_version,
    )
    discovery = discover_candidates(args=args, repository=repository)
    candidates = discovery["candidate_ids"]

    if args.apply and args.mode == "catchup":
        # confirm aplica al descubrimiento completo (start_after=0); reanudación no revalida el total
        if int(args.start_after_history_id) == 0:
            if int(discovery["total"]) != int(args.confirm_candidate_count or -1):
                raise AnalyticsValidationError(
                    "Cantidad de candidatos no coincide con --confirm-candidate-count",
                    error_type="catchup_confirm_mismatch",
                    details={
                        "candidates_found": discovery["total"],
                        "confirm_candidate_count": args.confirm_candidate_count,
                    },
                )

    run_batch_id = str(uuid.uuid4())
    base = {
        "ok": True,
        "mode": args.mode,
        "committed": False if args.dry_run else True,
        "dry_run": args.dry_run,
        "run_batch_id": run_batch_id,
        "calculation_version": args.calculation_version,
        "audit": audit,
        "candidates": {
            "total": discovery["total"],
            "new": discovery["new"],
            "changed": discovery["changed"],
            "lookback_from": discovery["lookback_from"],
            "lookback_to": discovery["lookback_to"],
            "limitation": discovery["limitation"],
        },
        "persistence": {"inserted": 0, "updated": 0, "unchanged": 0},
        "checkpoint": {
            "last_committed_history_id": args.start_after_history_id,
            "batches_committed": 0,
        },
        "batches": [],
    }

    if discovery["total"] == 0:
        base["committed"] = True
        base["message"] = "Sin candidatos"
        return base

    if args.dry_run or not args.apply:
        base["committed"] = True
        base["message"] = "Dry-run: no se escribió"
        return base

    if commit_fn is None or rollback_fn is None:
        raise AnalyticsValidationError(
            "apply requiere commit_fn y rollback_fn",
            error_type="invalid_args",
        )

    stub = _backfill_stub(args)
    last_committed = int(args.start_after_history_id)
    batches_committed = 0
    persistence_total = {"inserted": 0, "updated": 0, "unchanged": 0}
    batch_events: list[dict[str, Any]] = []
    # Solo candidatos > start_after
    pending = [i for i in candidates if i > args.start_after_history_id]
    commit_size = int(args.commit_batch_size)

    while pending:
        if args.max_batches is not None and batches_committed >= int(args.max_batches):
            break
        batch_ids = pending[:commit_size]
        pending = pending[commit_size:]
        batch_number = batches_committed + 1
        transaction_batch_id = str(uuid.uuid4())
        t0 = time.perf_counter()
        try:
            rows = repository.fetch_history_rows_by_ids(
                company_id=args.company_id,
                office_id=args.office_id,
                history_ids=batch_ids,
            )
            if not rows:
                break
            persistence, _calcs = _process_backfill_batch(
                repository=repository,
                args=stub,
                rows=rows,
                transaction_batch_id=transaction_batch_id,
            )
            # En sync solo deberían entrar new/changed; si aparece unchanged, cuenta igual
            commit_fn()
            first_hid = int(rows[0]["history_id"])
            last_hid = int(rows[-1]["history_id"])
            last_committed = last_hid
            batches_committed += 1
            for k in persistence_total:
                persistence_total[k] += persistence[k]
            event = {
                "event": "batch_committed",
                "batch_number": batch_number,
                "transaction_batch_id": transaction_batch_id,
                "run_batch_id": run_batch_id,
                "first_history_id": first_hid,
                "last_history_id": last_hid,
                "rows_processed": len(rows),
                "inserted": persistence["inserted"],
                "updated": persistence["updated"],
                "unchanged": persistence["unchanged"],
                "duration_ms": int((time.perf_counter() - t0) * 1000),
            }
            batch_events.append(event)
            if emit_fn is not None:
                emit_fn(event)
        except Exception as exc:
            try:
                rollback_fn()
            except Exception:
                pass
            return {
                "ok": False,
                "mode": args.mode,
                "committed": False,
                "partial": batches_committed > 0,
                "failed_batch": batch_number,
                "last_committed_history_id": last_committed,
                "resume_after_history_id": last_committed,
                "run_batch_id": run_batch_id,
                "error": str(exc),
                "error_type": getattr(exc, "error_type", type(exc).__name__),
                "candidates": base["candidates"],
                "persistence": dict(persistence_total),
                "checkpoint": {
                    "last_committed_history_id": last_committed,
                    "batches_committed": batches_committed,
                },
                "batches": batch_events,
                "audit": audit,
            }

    partial = bool(pending) or (
        args.max_batches is not None and batches_committed >= int(args.max_batches)
        and bool(pending)
    )
    # Fix partial: if we stopped due to max_batches and more remain
    remaining = len(pending) > 0

    return {
        "ok": True,
        "mode": args.mode,
        "committed": True,
        "partial": remaining,
        "dry_run": False,
        "run_batch_id": run_batch_id,
        "calculation_version": args.calculation_version,
        "audit": audit,
        "candidates": base["candidates"],
        "persistence": dict(persistence_total),
        "checkpoint": {
            "last_committed_history_id": last_committed,
            "batches_committed": batches_committed,
            "resume_after_history_id": last_committed if remaining else None,
        },
        "resume_after_history_id": last_committed if remaining else None,
        "batches": batch_events,
    }
