"""Entrypoint único ``bsale_raw``.

    python -m backend.jobs.bsale_raw sync --company 3 --resource offices --mode full-reconcile [--dry-run]
    python -m backend.jobs.bsale_raw sync --company 3 --resource stocks --office 1 --mode scanner [--dry-run]
    python -m backend.jobs.bsale_raw sync --company 3 --resource stocks --variant 10888 [--office 1] --mode point [--dry-run]
    python -m backend.jobs.bsale_raw sync --company 3 --resource documents --document <ID> --mode point [--dry-run]
    python -m backend.jobs.bsale_raw scan-stocks --company 3 [--dry-run]
    python -m backend.jobs.bsale_raw scan-costs --company 3 [--batch 1000] [--dry-run]
    python -m backend.jobs.bsale_raw refresh-costs --company 3 --variant <ID> [--variant <ID> ...] [--dry-run]
    python -m backend.jobs.bsale_raw sync-prices --company 3 [--lists active|inactive|all] [--price-list <ID> ...] [--dry-run]
    python -m backend.jobs.bsale_raw refresh-prices --company 3 --variant <ID> [--variant <ID> ...] [--price-list <ID> ...] [--dry-run]
    python -m backend.jobs.bsale_raw sync-nightly [--company N ...] [--dry-run]

``--document`` es el id TÉCNICO del documento en Bsale (``/v1/documents/{id}.json``), no el folio
(``number``) ni el número visible de la OC. No hay búsqueda por folio.

Exit: 0 SUCCESS, 1 FAILED, 2 PARTIAL, 3 lock ocupado (SKIPPED), 64 uso inválido.
``scan-stocks``: 3 = otro ciclo de la empresa en curso (no escaneó nada).
``scan-costs``: 3 = otro scanner de costos de la empresa en curso; 2 = lote con variantes con error.
``sync-prices``: 3 = otra sincronización de precios de la empresa en curso; 2 = alguna lista (o la
metadata de listas) falló; 1 = ninguna lista sincronizada.
``sync-nightly`` usa 0/1/2/64 (un lock ocupado cuenta como recurso FAILED → PARTIAL).
La salida nunca incluye token, payload ni datos del cliente.
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

MODES = {"full-reconcile": SyncMode.FULL_RECONCILE, "scanner": SyncMode.SCANNER, "point": SyncMode.POINT}

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


def _flag(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _format_document(outcome: EntityOutcome) -> list[str]:
    doc = outcome.document or {}

    def count(key: str) -> str:
        values = doc.get(key)
        return "" if values is None else str(len(values))

    return [
        f"company={outcome.company_id}",
        f"resource={outcome.resource}",
        f"scope={outcome.scope}",
        f"mode={outcome.mode}",
        f"document_type_id={_flag(doc.get('document_type_id'))}",
        f"office_id={_flag(doc.get('office_id'))}",
        f"details={_flag(doc.get('details'))}",
        f"references={_flag(doc.get('references'))}",
        f"sellers={_flag(doc.get('sellers'))}",
        f"attributes={_flag(doc.get('attributes'))}",
        f"change_kind={_flag(doc.get('change_kind'))}",
        f"version_changed={_flag(doc.get('version_changed'))}",
        f"skipped_newer={outcome.rows_skipped_newer}",
        f"previous_variants={count('previous_variants')}",
        f"current_variants={count('current_variants')}",
        f"affected_variants={count('affected_variants')}",
        f"pending_stock_changes={count('pending_change_ids')}",
        f"stock_variants={count('stock_variants')}",
        f"stock_office_id={_flag(doc.get('stock_office_id'))}",
        f"stock_refresh={_flag(doc.get('stock_refresh'))}",
        f"requests={outcome.requests}",
        f"stock_requests={_flag(doc.get('stock_requests', 0))}",
        f"duration_ms={outcome.duration_ms}",
        f"status={outcome.status}",
    ]


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
    if outcome.document is not None:
        lines.extend(_format_document(outcome))
    else:
        for label, attr in OUTPUT_FIELDS:
            value = getattr(outcome, attr)
            lines.append(f"{label}={'' if value is None else value}")
    if outcome.sync_run_id is not None:
        lines.append(f"sync_run_id={outcome.sync_run_id}")
    if outcome.point is not None:
        statuses = [r.get("status") for r in outcome.point.get("results", {}).values()]
        lines.append(f"variants={len(outcome.point.get('variant_ids', []))}")
        lines.append(f"no_rows={statuses.count('NO_ROWS')}")
        lines.append(f"failed_variants={statuses.count('FAILED')}")
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
    sync.add_argument("--office", type=int, help="office_id (recursos por sucursal; opcional en --mode point)")
    sync.add_argument("--variant", type=int, help="variant_id (sólo stocks --mode point)")
    sync.add_argument(
        "--document", type=int,
        help="id TÉCNICO Bsale del documento (sólo documents --mode point); NO es folio/number",
    )
    sync.add_argument("--mode", required=True, choices=sorted(MODES))
    sync.add_argument("--dry-run", action="store_true", help="consulta API y valida; no escribe en la BD")
    scan_stocks = sub.add_parser(
        "scan-stocks",
        help="SCANNER serial de stock de todas las sucursales activas de una empresa (bsale_raw.offices)",
    )
    scan_stocks.add_argument("--company", type=int, required=True, help="company_id (bsale_raw.sources)")
    scan_stocks.add_argument("--dry-run", action="store_true", help="consulta API y valida; no escribe en la BD")
    scan_costs = sub.add_parser(
        "scan-costs",
        help="SCANNER de costos (variants/{id}/costs.json): un lote de bsale_raw.variants por corrida, con cursor",
    )
    scan_costs.add_argument("--company", type=int, required=True, help="company_id (bsale_raw.sources)")
    scan_costs.add_argument("--batch", type=int, default=1000, help="variantes por corrida (1..5000; default 1000)")
    scan_costs.add_argument("--dry-run", action="store_true", help="consulta API y valida; no escribe en la BD")
    refresh_costs = sub.add_parser("refresh-costs", help="costos de variantes explícitas (POINT, prioridad P0)")
    refresh_costs.add_argument("--company", type=int, required=True, help="company_id (bsale_raw.sources)")
    refresh_costs.add_argument("--variant", type=int, action="append", required=True, help="variant_id (repetible)")
    refresh_costs.add_argument("--dry-run", action="store_true", help="consulta API y valida; no escribe en la BD")
    sync_prices = sub.add_parser(
        "sync-prices",
        help="refresca price_lists y barre price_lists/{id}/details.json por lista (snapshot completo, sin DELETE)",
    )
    sync_prices.add_argument("--company", type=int, required=True, help="company_id (bsale_raw.sources)")
    sync_prices.add_argument(
        "--lists", choices=("active", "inactive", "all"), default="active",
        help="listas a barrer según bsale_raw.price_lists.state (default active)",
    )
    sync_prices.add_argument(
        "--price-list", type=int, action="append", dest="price_list",
        help="limita a esta lista (repetible); debe estar dentro de --lists",
    )
    sync_prices.add_argument("--dry-run", action="store_true", help="consulta API y valida; no escribe en la BD")
    refresh_prices = sub.add_parser(
        "refresh-prices", help="precios de variantes explícitas (POINT, prioridad P0); no marca ausencias",
    )
    refresh_prices.add_argument("--company", type=int, required=True, help="company_id (bsale_raw.sources)")
    refresh_prices.add_argument("--variant", type=int, action="append", required=True, help="variant_id (repetible)")
    refresh_prices.add_argument(
        "--price-list", type=int, action="append", dest="price_list",
        help="lista a consultar (repetible); por defecto todas las activas de bsale_raw.price_lists",
    )
    refresh_prices.add_argument("--dry-run", action="store_true", help="consulta API y valida; no escribe en la BD")
    nightly = sub.add_parser(
        "sync-nightly",
        help="metadata + catálogo (full-reconcile) de todas las empresas activas de bsale_raw.sources",
    )
    nightly.add_argument(
        "--company", type=int, action="append",
        help="limita a esta empresa (repetible); por defecto todas las activas",
    )
    nightly.add_argument("--dry-run", action="store_true", help="consulta API y valida; no escribe en la BD")
    return parser


def usage_error(args: argparse.Namespace, spec) -> str | None:
    """Combinaciones recurso / modo / sucursal inválidas (antes de tocar API o BD)."""
    mode = MODES[args.mode]
    if mode not in spec.pipeline_modes:
        allowed = ", ".join(k for k, v in MODES.items() if v in spec.pipeline_modes)
        return f"--mode {args.mode} no habilitado para {spec.name} (permitidos: {allowed})"
    targets = {"variant": args.variant, "document": args.document}
    if mode is SyncMode.POINT:
        key = spec.point_key
        if key not in targets:
            return f"{spec.name} no tiene refresh POINT"
        if targets[key] is None:
            return f"--mode point en {spec.name} exige --{key}"
        for other, value in targets.items():
            if other != key and value is not None:
                return f"--{other} no se acepta en {spec.name}"
    else:
        for name, value in targets.items():
            if value is not None:
                return f"--{name} sólo se acepta con --mode point"
        if spec.partition_by_office and args.office is None:
            return f"{spec.name} exige --office en --mode {args.mode}"
    if not spec.partition_by_office and args.office is not None:
        return f"{spec.name} no acepta --office"
    if args.office is not None and args.office <= 0:
        return "--office debe ser un entero positivo"
    for name, value in targets.items():
        if value is not None and value <= 0:
            return f"--{name} debe ser un entero positivo"
    return None


def _default_runner(
    *,
    company_id: int,
    resource: str,
    mode: SyncMode,
    dry_run: bool,
    office_id: int | None = None,
    variant_id: int | None = None,
    document_id: int | None = None,
) -> EntityOutcome:
    from backend.services.bsale_raw.core.store import PgRawStore
    from backend.utils.bsale_token_env import load_dotenv_if_available

    load_dotenv_if_available()
    store = PgRawStore(read_only=dry_run)
    try:
        if mode is SyncMode.POINT and document_id is not None:
            from backend.services.bsale_raw.core.document_engine import refresh_document_point

            return refresh_document_point(
                store=store, company_id=company_id, document_id=document_id, resource=resource,
                dry_run=dry_run, host=socket.gethostname(),
            )
        if mode is SyncMode.POINT:
            from backend.services.bsale_raw.core.stock_engine import refresh_stock_point

            return refresh_stock_point(
                store=store, company_id=company_id, variant_id=variant_id, office_id=office_id,
                resource=resource, dry_run=dry_run, host=socket.gethostname(),
            )
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


def _default_stock_cycle_runner(*, company_id: int, dry_run: bool):
    from backend.services.bsale_raw.stock_cycle import PgStockCycleStore, run_stock_cycle
    from backend.utils.bsale_token_env import load_dotenv_if_available

    load_dotenv_if_available()
    store = PgStockCycleStore()
    try:
        return run_stock_cycle(company_id=company_id, dry_run=dry_run, reader=store, lock=store)
    finally:
        store.close()


def stock_cycle_exit_code(report) -> int:
    return {"SUCCESS": EXIT_SUCCESS, "PARTIAL": EXIT_PARTIAL, "SKIPPED": EXIT_LOCKED}.get(
        report.status, EXIT_FAILED
    )


def _default_cost_runner(
    *, command: str, company_id: int, dry_run: bool, batch: int | None = None, variant_ids: list[int] | None = None
) -> EntityOutcome:
    from backend.services.bsale_raw.core.cost_engine import PgCostStore, refresh_costs, run_cost_scan
    from backend.utils.bsale_token_env import load_dotenv_if_available

    load_dotenv_if_available()
    store = PgCostStore(read_only=dry_run)
    try:
        if command == "refresh-costs":
            return refresh_costs(
                store=store, company_id=company_id, variant_ids=variant_ids or [], dry_run=dry_run,
                host=socket.gethostname(),
            )
        return run_cost_scan(
            store=store, company_id=company_id, batch_size=batch, dry_run=dry_run, host=socket.gethostname(),
        )
    finally:
        store.close()


def cost_usage_error(args: argparse.Namespace) -> str | None:
    from backend.services.bsale_raw.core.cost_engine import MAX_BATCH, MAX_POINT_VARIANTS

    if args.company <= 0:
        return "--company debe ser un entero positivo"
    if args.command == "scan-costs" and not 1 <= args.batch <= MAX_BATCH:
        return f"--batch debe estar entre 1 y {MAX_BATCH}"
    if args.command == "refresh-costs":
        if any(v <= 0 for v in args.variant):
            return "--variant debe ser un entero positivo"
        if len(set(args.variant)) > MAX_POINT_VARIANTS:
            return f"máximo {MAX_POINT_VARIANTS} variantes por refresh"
    return None


def _default_price_runner(
    *, command: str, company_id: int, dry_run: bool, lists: str = "active",
    price_list_ids: list[int] | None = None, variant_ids: list[int] | None = None,
):
    from backend.services.bsale_raw.core.price_engine import PgPriceStore, refresh_prices, sync_prices
    from backend.utils.bsale_token_env import load_dotenv_if_available

    load_dotenv_if_available()
    store = PgPriceStore(read_only=dry_run)
    try:
        if command == "refresh-prices":
            return refresh_prices(
                store=store, company_id=company_id, variant_ids=variant_ids or [], price_list_ids=price_list_ids,
                dry_run=dry_run, host=socket.gethostname(),
            )
        return sync_prices(
            store=store, company_id=company_id, selection=lists, price_list_ids=price_list_ids, dry_run=dry_run,
            host=socket.gethostname(),
        )
    finally:
        store.close()


def price_usage_error(args: argparse.Namespace) -> str | None:
    from backend.services.bsale_raw.core.price_engine import MAX_POINT_VARIANTS

    if args.company <= 0:
        return "--company debe ser un entero positivo"
    if any(v <= 0 for v in args.price_list or []):
        return "--price-list debe ser un entero positivo"
    if args.command == "refresh-prices":
        if any(v <= 0 for v in args.variant):
            return "--variant debe ser un entero positivo"
        if len(set(args.variant)) > MAX_POINT_VARIANTS:
            return f"máximo {MAX_POINT_VARIANTS} variantes por refresh"
    return None


def price_exit_code(report) -> int:
    return {"SUCCESS": EXIT_SUCCESS, "PARTIAL": EXIT_PARTIAL, "SKIPPED": EXIT_LOCKED}.get(report.status, EXIT_FAILED)


def _default_nightly_runner(*, companies: list[int] | None, dry_run: bool):
    from backend.services.bsale_raw.nightly import PgNightlyReader, run_nightly
    from backend.utils.bsale_token_env import load_dotenv_if_available

    load_dotenv_if_available()
    reader = PgNightlyReader()
    try:
        return run_nightly(reader=reader, companies=companies, dry_run=dry_run)
    finally:
        reader.close()


def nightly_exit_code(report) -> int:
    return {"SUCCESS": EXIT_SUCCESS, "PARTIAL": EXIT_PARTIAL}.get(report.status, EXIT_FAILED)


def main(
    argv: list[str] | None = None,
    *,
    runner: Callable[..., EntityOutcome] = _default_runner,
    stock_cycle_runner: Callable[..., object] = _default_stock_cycle_runner,
    cost_runner: Callable[..., EntityOutcome] = _default_cost_runner,
    price_runner: Callable[..., object] = _default_price_runner,
    nightly_runner: Callable[..., object] = _default_nightly_runner,
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

    if args.command == "scan-stocks":
        from backend.services.bsale_raw.stock_cycle import format_cycle

        if args.company <= 0:
            print("uso inválido: --company debe ser un entero positivo", file=err)
            return EXIT_USAGE
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s", stream=sys.stderr)
        report = stock_cycle_runner(company_id=args.company, dry_run=args.dry_run)
        print(format_cycle(report), file=out)
        return stock_cycle_exit_code(report)

    if args.command in ("scan-costs", "refresh-costs"):
        from backend.services.bsale_raw.core.cost_engine import format_cost_outcome

        problem = cost_usage_error(args)
        if problem:
            print(f"uso inválido: {problem}", file=err)
            return EXIT_USAGE
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s", stream=sys.stderr)
        outcome = cost_runner(
            command=args.command,
            company_id=args.company,
            dry_run=args.dry_run,
            batch=getattr(args, "batch", None),
            variant_ids=getattr(args, "variant", None),
        )
        print(format_cost_outcome(outcome), file=out)
        return exit_code(outcome)

    if args.command in ("sync-prices", "refresh-prices"):
        from backend.services.bsale_raw.core.price_engine import format_price_point, format_price_report

        problem = price_usage_error(args)
        if problem:
            print(f"uso inválido: {problem}", file=err)
            return EXIT_USAGE
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s", stream=sys.stderr)
        result = price_runner(
            command=args.command,
            company_id=args.company,
            dry_run=args.dry_run,
            lists=getattr(args, "lists", "active"),
            price_list_ids=args.price_list,
            variant_ids=getattr(args, "variant", None),
        )
        if args.command == "refresh-prices":
            print(format_price_point(result), file=out)
            return exit_code(result)
        print(format_price_report(result), file=out)
        return price_exit_code(result)

    if args.command == "sync-nightly":
        from backend.services.bsale_raw.nightly import format_report

        if any(c <= 0 for c in args.company or []):
            print("uso inválido: --company debe ser un entero positivo", file=err)
            return EXIT_USAGE
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s", stream=sys.stderr)
        report = nightly_runner(companies=args.company, dry_run=args.dry_run)
        print(format_report(report), file=out)
        return nightly_exit_code(report)

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
        variant_id=args.variant,
        document_id=args.document,
    )
    print(format_outcome(outcome), file=out)
    return exit_code(outcome)
