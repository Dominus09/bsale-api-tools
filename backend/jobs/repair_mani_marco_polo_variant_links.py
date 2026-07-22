"""
Reparación puntual de associations incorrectas en bsale.products_master.

Casos:
  - id 4152 (MANI MARCO POLO SALADO) dejó de apuntar a variant 8942 (jugo)
    y pasa a variant 28941 / product 5594.
  - id 4177 (MANI MARCO POLO CON PASAS) dejó de apuntar a variant 8943 (jugo)
    y pasa a variant 23054 / product 5594.

Uso::

    python -m backend.jobs.repair_mani_marco_polo_variant_links
    python -m backend.jobs.repair_mani_marco_polo_variant_links --execute
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from backend.db import get_connection
from backend.utils.bsale_token_env import load_dotenv_if_available

logger = logging.getLogger(__name__)

COMPANY_ID = 3

LOCKED_IDS = (3924, 3925, 4152, 4177)

JUICE_500 = {
    "id": 3925,
    "variant_id": 8942,
    "product_id": 4063,
    "barcode": "7802337801038",
}
JUICE_250 = {
    "id": 3924,
    "variant_id": 8943,
    "product_id": 4063,
    "barcode": "7802337801014",
}


@dataclass(frozen=True)
class RepairTarget:
    id: int
    expected_barcode: str
    expected_variant_id: int
    expected_product_id: int
    target_variant_id: int
    target_product_id: int


TARGETS = (
    RepairTarget(
        id=4152,
        expected_barcode="7802420009518",
        expected_variant_id=8942,
        expected_product_id=2928,
        target_variant_id=28941,
        target_product_id=5594,
    ),
    RepairTarget(
        id=4177,
        expected_barcode="7802420125430",
        expected_variant_id=8943,
        expected_product_id=2928,
        target_variant_id=23054,
        target_product_id=5594,
    ),
)


class RepairGuardError(RuntimeError):
    """Alguna validación de seguridad falló; la reparación no puede continuar."""


def _norm_barcode(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _row_dict(cur, row: tuple | None) -> dict[str, Any] | None:
    if row is None:
        return None
    cols = [d[0] for d in cur.description]
    return {k: v for k, v in zip(cols, row)}


def _serialize_row(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    out: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, datetime):
            out[key] = value.isoformat()
        else:
            out[key] = value
    return out


def _fetch_locked_products_master(cur) -> dict[int, dict[str, Any]]:
    cur.execute(
        """
        SELECT
            id,
            barcode,
            product_id,
            variant_id,
            product_name,
            variant_name,
            units_per_box,
            weight_box_kg,
            sale_type,
            is_active,
            created_at,
            updated_at
        FROM bsale.products_master
        WHERE id = ANY(%s::bigint[])
        ORDER BY id
        FOR UPDATE
        """,
        (list(LOCKED_IDS),),
    )
    rows = [_row_dict(cur, row) for row in cur.fetchall()]
    by_id = {int(row["id"]): row for row in rows if row is not None}
    missing = [row_id for row_id in LOCKED_IDS if row_id not in by_id]
    if missing:
        raise RepairGuardError(f"Filas bloqueadas ausentes: {missing}")
    return by_id


def _assert_expected_target(row: dict[str, Any], target: RepairTarget) -> None:
    barcode = _norm_barcode(row.get("barcode"))
    if (
        int(row["id"]) != target.id
        or barcode != target.expected_barcode
        or int(row["variant_id"]) != target.expected_variant_id
        or int(row["product_id"]) != target.expected_product_id
        or row.get("is_active") is not True
    ):
        raise RepairGuardError(
            "Estado inicial inesperado para products_master.id="
            f"{target.id}: got barcode={barcode!r} variant_id={row.get('variant_id')!r} "
            f"product_id={row.get('product_id')!r} is_active={row.get('is_active')!r}; "
            f"expected barcode={target.expected_barcode!r} "
            f"variant_id={target.expected_variant_id} "
            f"product_id={target.expected_product_id} is_active=True"
        )


def _assert_juice_row(row: dict[str, Any], expected: dict[str, Any]) -> None:
    barcode = _norm_barcode(row.get("barcode"))
    if (
        int(row["id"]) != expected["id"]
        or int(row["variant_id"]) != expected["variant_id"]
        or int(row["product_id"]) != expected["product_id"]
        or barcode != expected["barcode"]
        or row.get("is_active") is not True
    ):
        raise RepairGuardError(
            "Fila canónica de jugo inesperada para products_master.id="
            f"{expected['id']}: got barcode={barcode!r} variant_id={row.get('variant_id')!r} "
            f"product_id={row.get('product_id')!r} is_active={row.get('is_active')!r}; "
            f"expected barcode={expected['barcode']!r} "
            f"variant_id={expected['variant_id']} "
            f"product_id={expected['product_id']} is_active=True"
        )


def _assert_authoritative_variants(cur) -> list[dict[str, Any]]:
    expected = {
        28941: {"product_id": 5594, "barcode": "7802420009518"},
        23054: {"product_id": 5594, "barcode": "7802420125430"},
    }
    cur.execute(
        """
        SELECT
            bsale_id AS variant_id,
            product_id,
            BTRIM(bar_code) AS barcode,
            description,
            units_per_box
        FROM bsale.variants
        WHERE company_id = %s
          AND bsale_id = ANY(%s::bigint[])
        ORDER BY bsale_id
        """,
        (COMPANY_ID, list(expected.keys())),
    )
    rows = [_row_dict(cur, row) for row in cur.fetchall()]
    by_id = {int(row["variant_id"]): row for row in rows if row is not None}
    missing = [vid for vid in expected if vid not in by_id]
    if missing:
        raise RepairGuardError(
            f"Variantes autoritativas ausentes en bsale.variants company_id={COMPANY_ID}: {missing}"
        )
    for variant_id, exp in expected.items():
        row = by_id[variant_id]
        if int(row["product_id"]) != exp["product_id"] or _norm_barcode(row.get("barcode")) != exp["barcode"]:
            raise RepairGuardError(
                f"Variante autoritativa inconsistente variant_id={variant_id}: "
                f"got product_id={row.get('product_id')!r} barcode={_norm_barcode(row.get('barcode'))!r}; "
                f"expected product_id={exp['product_id']} barcode={exp['barcode']!r}"
            )
    return rows


def _assert_no_active_conflicts(cur) -> None:
    target_variant_ids = [t.target_variant_id for t in TARGETS]
    target_barcodes = [t.expected_barcode for t in TARGETS]
    repair_ids = [t.id for t in TARGETS]
    cur.execute(
        """
        SELECT id, variant_id, barcode, product_id, is_active
        FROM bsale.products_master
        WHERE is_active = TRUE
          AND id <> ALL(%s::bigint[])
          AND (
                variant_id = ANY(%s::bigint[])
             OR BTRIM(barcode) = ANY(%s::text[])
          )
        ORDER BY id
        """,
        (repair_ids, target_variant_ids, target_barcodes),
    )
    conflicts = [_row_dict(cur, row) for row in cur.fetchall()]
    if conflicts:
        raise RepairGuardError(
            "Existe otra fila activa conflictiva para los variant_id/barcodes destino: "
            f"{[_serialize_row(row) for row in conflicts]}"
        )


def _project_after(before: dict[str, Any], target: RepairTarget) -> dict[str, Any]:
    projected = dict(before)
    projected["variant_id"] = target.target_variant_id
    projected["product_id"] = target.target_product_id
    projected["updated_at"] = "<NOW()>"
    return projected


def _apply_updates(cur) -> list[dict[str, Any]]:
    updated: list[dict[str, Any]] = []
    for target in TARGETS:
        cur.execute(
            """
            UPDATE bsale.products_master
            SET
                variant_id = %s,
                product_id = %s,
                updated_at = NOW()
            WHERE id = %s
              AND BTRIM(barcode) = %s
              AND variant_id = %s
              AND product_id = %s
              AND is_active = TRUE
            RETURNING
                id,
                barcode,
                product_id,
                variant_id,
                product_name,
                variant_name,
                units_per_box,
                weight_box_kg,
                sale_type,
                is_active,
                created_at,
                updated_at
            """,
            (
                target.target_variant_id,
                target.target_product_id,
                target.id,
                target.expected_barcode,
                target.expected_variant_id,
                target.expected_product_id,
            ),
        )
        if cur.rowcount != 1:
            raise RepairGuardError(
                f"UPDATE de products_master.id={target.id} afectó "
                f"{cur.rowcount} filas; se esperaba exactamente 1"
            )
        row = _row_dict(cur, cur.fetchone())
        if row is None:
            raise RepairGuardError(
                f"UPDATE de products_master.id={target.id} no devolvió fila"
            )
        updated.append(row)
    return updated


def _assert_unique_active_variant(cur, *, variant_id: int, expected_id: int) -> dict[str, Any]:
    cur.execute(
        """
        SELECT id, barcode, product_id, variant_id, is_active
        FROM bsale.products_master
        WHERE is_active = TRUE
          AND variant_id = %s
        ORDER BY id
        """,
        (variant_id,),
    )
    rows = [_row_dict(cur, row) for row in cur.fetchall()]
    if len(rows) != 1 or int(rows[0]["id"]) != expected_id:
        raise RepairGuardError(
            f"variant_id={variant_id} debe tener una sola fila activa id={expected_id}; "
            f"encontrado={[ _serialize_row(r) for r in rows ]}"
        )
    return rows[0]


def _assert_unique_logistics(cur, variant_ids: list[int]) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT
            variant_id,
            COUNT(*)::integer AS active_rows,
            ARRAY_AGG(products_master_id ORDER BY products_master_id) AS products_master_ids
        FROM bsale.v_product_logistics
        WHERE is_active = TRUE
          AND variant_id = ANY(%s::bigint[])
        GROUP BY variant_id
        ORDER BY variant_id
        """,
        (variant_ids,),
    )
    rows = [_row_dict(cur, row) for row in cur.fetchall()]
    by_variant = {int(row["variant_id"]): row for row in rows if row is not None}
    missing = [vid for vid in variant_ids if vid not in by_variant]
    if missing:
        raise RepairGuardError(
            f"v_product_logistics sin filas activas para variant_id={missing}"
        )
    bad = [
        row
        for row in rows
        if int(row["active_rows"]) != 1
    ]
    if bad:
        raise RepairGuardError(
            "v_product_logistics con más de una fila activa: "
            f"{[_serialize_row(row) for row in bad]}"
        )
    return rows


def _run_preflight(cur) -> dict[str, Any]:
    locked = _fetch_locked_products_master(cur)
    _assert_juice_row(locked[3925], JUICE_500)
    _assert_juice_row(locked[3924], JUICE_250)

    before: dict[str, Any] = {}
    projected: dict[str, Any] = {}
    for target in TARGETS:
        row = locked[target.id]
        _assert_expected_target(row, target)
        before[str(target.id)] = _serialize_row(row)
        projected[str(target.id)] = _serialize_row(_project_after(row, target))

    authoritative = _assert_authoritative_variants(cur)
    _assert_no_active_conflicts(cur)

    return {
        "before": before,
        "projected": projected,
        "juices": {
            "3925": _serialize_row(locked[3925]),
            "3924": _serialize_row(locked[3924]),
        },
        "authoritative_variants": [_serialize_row(row) for row in authoritative],
        "validations": {
            "targets_match_expected_state": True,
            "juice_rows_intact": True,
            "authoritative_variants_ok": True,
            "no_active_conflicts": True,
        },
    }


def _run_final_validations(cur) -> dict[str, Any]:
    after_rows = {}
    for target in TARGETS:
        cur.execute(
            """
            SELECT
                id,
                barcode,
                product_id,
                variant_id,
                product_name,
                variant_name,
                units_per_box,
                weight_box_kg,
                sale_type,
                is_active,
                created_at,
                updated_at
            FROM bsale.products_master
            WHERE id = %s
            """,
            (target.id,),
        )
        row = _row_dict(cur, cur.fetchone())
        if row is None:
            raise RepairGuardError(f"Fila reparada ausente id={target.id}")
        if (
            int(row["variant_id"]) != target.target_variant_id
            or int(row["product_id"]) != target.target_product_id
            or _norm_barcode(row.get("barcode")) != target.expected_barcode
            or row.get("is_active") is not True
        ):
            raise RepairGuardError(
                f"Estado final inesperado para id={target.id}: {_serialize_row(row)}"
            )
        after_rows[str(target.id)] = _serialize_row(row)

    unique = {
        "8942": _serialize_row(_assert_unique_active_variant(cur, variant_id=8942, expected_id=3925)),
        "8943": _serialize_row(_assert_unique_active_variant(cur, variant_id=8943, expected_id=3924)),
        "28941": _serialize_row(_assert_unique_active_variant(cur, variant_id=28941, expected_id=4152)),
        "23054": _serialize_row(_assert_unique_active_variant(cur, variant_id=23054, expected_id=4177)),
    }
    logistics = _assert_unique_logistics(cur, [8942, 8943, 28941, 23054])
    return {
        "after": after_rows,
        "unique_active_variants": unique,
        "logistics": [_serialize_row(row) for row in logistics],
        "validations": {
            "unique_active_8942_is_3925": True,
            "unique_active_8943_is_3924": True,
            "unique_active_28941_is_4152": True,
            "unique_active_23054_is_4177": True,
            "v_product_logistics_unique_for_four_variants": True,
        },
    }


def run_repair(*, execute: bool = False, connection_factory=get_connection) -> dict[str, Any]:
    """Ejecuta la reparación en una sola transacción. Dry-run por defecto."""
    conn = connection_factory()
    wrote = False
    committed = False
    try:
        cur = conn.cursor()
        try:
            preflight = _run_preflight(cur)
            report: dict[str, Any] = {
                "mode": "execute" if execute else "dry-run",
                "before": preflight["before"],
                "projected": preflight["projected"],
                "juices": preflight["juices"],
                "authoritative_variants": preflight["authoritative_variants"],
                "validations": dict(preflight["validations"]),
                "after": None,
                "committed": False,
                "wrote": False,
            }

            if not execute:
                conn.rollback()
                report["message"] = (
                    "Dry-run OK: guardas pasaron; no se ejecutó UPDATE ni COMMIT"
                )
                return report

            updated = _apply_updates(cur)
            wrote = True
            finals = _run_final_validations(cur)
            report["after"] = finals["after"]
            report["unique_active_variants"] = finals["unique_active_variants"]
            report["logistics"] = finals["logistics"]
            report["validations"].update(finals["validations"])
            report["updated_rows"] = [_serialize_row(row) for row in updated]
            conn.commit()
            committed = True
            report["committed"] = True
            report["wrote"] = True
            report["message"] = "Reparación aplicada y validada"
            return report
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
    finally:
        if not committed and wrote:
            logger.error(
                "repair_mani_marco_polo_variant_links wrote=%s committed=%s",
                wrote,
                committed,
            )
        conn.close()


def main(argv: list[str] | None = None) -> int:
    load_dotenv_if_available()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(
        description=(
            "Corrige variant_id/product_id de MANI MARCO POLO "
            "(products_master 4152 y 4177)"
        )
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Aplica UPDATE + COMMIT. Sin este flag solo dry-run.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fuerza dry-run aunque también se pase --execute",
    )
    args = parser.parse_args(argv)
    execute = bool(args.execute and not args.dry_run)

    try:
        report = run_repair(execute=execute)
    except RepairGuardError as exc:
        logger.error("repair_guard_failed error=%s", exc)
        print(
            json.dumps(
                {
                    "ok": False,
                    "committed": False,
                    "wrote": False,
                    "error": str(exc),
                },
                ensure_ascii=False,
                indent=2,
            ),
            file=sys.stderr,
        )
        return 1
    except Exception as exc:
        logger.exception("repair_failed error=%s", exc)
        print(
            json.dumps(
                {
                    "ok": False,
                    "committed": False,
                    "wrote": False,
                    "error": str(exc),
                },
                ensure_ascii=False,
                indent=2,
            ),
            file=sys.stderr,
        )
        return 1

    report["ok"] = True
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
