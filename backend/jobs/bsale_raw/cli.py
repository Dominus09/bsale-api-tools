"""Entrypoint único ``bsale_raw``.

    python -m backend.jobs.bsale_raw sync --company 3 --resource offices --mode full-reconcile [--dry-run]
    python -m backend.jobs.bsale_raw sync --company 3 --resource stocks --office 1 --mode scanner [--dry-run]

Exit: 0 SUCCESS, 1 FAILED, 2 PARTIAL, 3 lock ocupado (SKIPPED), 64 uso inválido.
La salida nunca incluye token ni payload.
"""

from __future__ import annotations

import argparse
import logging
import socket
import sys
from typing import Callable, TextIO

from backend.services.bsale_raw.core.models import RunStatus, SyncMode
from backend.services.bsale_raw.core.store import EntityOutcome

EXIT_SUCCESS = 0
EXIT_FAILED = 1
EXIT_PARTIAL = 2
EXIT_LOCKED = 3
EXIT_USAGE = 64

MODES = {"full-reconcile": SyncMode.FULL_RECONCILE, "scanner": SyncMode.SCANNER}

OUTPUT_FIELDS = (
    ("company", "company_id"),
    ("resource", "resource"),
    ("scope", "scope"),
    ("mode", "mode"),
    ("api_count", "api_count"),
    ("received", "rows_received"),
    ("inserted", "rows_inserted"),
    ("updated", "rows_updated"),
    ("unchanged", "rows_unchanged"),
    ("skipped_newer", "rows_skipped_newer"),
    ("missing", "rows_missing"),
    ("deleted", "rows_deleted"),
    ("requests", "requests"),
    ("duration_ms", "duration_ms"),
    ("status", "status"),
)


def exit_code(outcome: EntityOutcome) -> int:
    return {
        RunStatus.SUCCESS.value: EXIT_SUCCESS,
        RunStatus.PARTIAL.value: EXIT_PARTIAL,
        RunStatus.SKIPPED.value: EXIT_LOCKED,
    }.get(outcome.status, EXIT_FAILED)


def format_outcome(outcome: EntityOutcome) -> str:
    lines = []
    if outcome.dry_run:
        lines.append("dry_run=true")
    for label, attr in OUTPUT_FIELDS:
        value = getattr(outcome, attr)
        lines.append(f"{label}={'' if value is None else value}")
    if outcome.sync_run_id is not None:
        lines.append(f"sync_run_id={outcome.sync_run_id}")
    if outcome.fuse is not None and outcome.fuse.get("tripped"):
        lines.append(f"fuse={outcome.fuse.get('reason')}")
    if outcome.error:
        lines.append(f"error={outcome.error}")
    return "\n".join(lines)


def build_parser(resources: list[str]) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m backend.jobs.bsale_raw")
    sub = parser.add_subparsers(dest="command", required=True)
    sync = sub.add_parser("sync", help="sincroniza un recurso de una empresa")
    sync.add_argument("--company", type=int, required=True, help="company_id (bsale_raw.sources)")
    sync.add_argument("--resource", required=True, choices=resources)
    sync.add_argument("--office", type=int, help="office_id (obligatorio y exclusivo de recursos por sucursal)")
    sync.add_argument("--mode", required=True, choices=sorted(MODES))
    sync.add_argument("--dry-run", action="store_true", help="consulta API y valida; no escribe en la BD")
    return parser


def usage_error(args: argparse.Namespace, spec) -> str | None:
    """Combinaciones recurso / modo / sucursal inválidas (antes de tocar API o BD)."""
    mode = MODES[args.mode]
    if mode not in spec.pipeline_modes:
        allowed = ", ".join(k for k, v in MODES.items() if v in spec.pipeline_modes)
        return f"--mode {args.mode} no habilitado para {spec.name} (permitidos: {allowed})"
    if spec.partition_by_office and args.office is None:
        return f"{spec.name} exige --office"
    if not spec.partition_by_office and args.office is not None:
        return f"{spec.name} no acepta --office"
    if args.office is not None and args.office <= 0:
        return "--office debe ser un entero positivo"
    return None


def _default_runner(
    *, company_id: int, resource: str, mode: SyncMode, dry_run: bool, office_id: int | None = None
) -> EntityOutcome:
    from backend.services.bsale_raw.core.store import PgRawStore
    from backend.utils.bsale_token_env import load_dotenv_if_available

    load_dotenv_if_available()
    store = PgRawStore(read_only=dry_run)
    try:
        if office_id is not None:
            from backend.services.bsale_raw.core.stock_engine import run_stock_sync

            return run_stock_sync(
                store=store, company_id=company_id, office_id=office_id, resource=resource,
                mode=mode, dry_run=dry_run, host=socket.gethostname(),
            )
        from backend.services.bsale_raw.core.engine import run_entity_sync

        return run_entity_sync(
            store=store, company_id=company_id, resource=resource, mode=mode,
            dry_run=dry_run, host=socket.gethostname(),
        )
    finally:
        store.close()


def main(
    argv: list[str] | None = None,
    *,
    runner: Callable[..., EntityOutcome] = _default_runner,
    out: TextIO | None = None,
    err: TextIO | None = None,
) -> int:
    import backend.services.bsale_raw.resources  # noqa: F401
    from backend.services.bsale_raw.core.registry import REGISTRY

    out = out or sys.stdout
    err = err or sys.stderr
    parser = build_parser(REGISTRY.pipeline_names())
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return EXIT_SUCCESS if exc.code == 0 else EXIT_USAGE

    problem = usage_error(args, REGISTRY.get(args.resource))
    if problem:
        print(f"uso inválido: {problem}", file=err)
        return EXIT_USAGE

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s", stream=sys.stderr)
    outcome = runner(
        company_id=args.company,
        resource=args.resource,
        mode=MODES[args.mode],
        dry_run=args.dry_run,
        office_id=args.office,
    )
    print(format_outcome(outcome), file=out)
    return exit_code(outcome)
