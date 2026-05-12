#!/usr/bin/env python3
"""
Limpieza selectiva de ``distribuidora.document_related``: filas con
``related_document_type NOT IN (1, 6, 9)`` (no operacionales para OC→boleta/factura/NC).

Por defecto **DRY_RUN**: solo lista candidatas; no modifica la BD.

Ejecución real (una transacción: backup → DELETE por ``id`` exacto):

    python backend/scripts/cleanup_document_related_invalid_types.py --execute

Requisitos: variables PG_* o ``.env`` como el resto de scripts.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

# Raíz del repositorio
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv

from backend.db import get_connection

RELATED_TYPES_ALLOWED = (1, 6, 9)
OC_TYPE_ID = 33

CREATE_BACKUP_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS distribuidora.document_related_cleanup_backup (
    backup_id BIGSERIAL PRIMARY KEY,
    original_row_id BIGINT NOT NULL,
    detail_id BIGINT NOT NULL,
    related_document_id BIGINT NOT NULL,
    related_document_type INTEGER NOT NULL,
    original_created_at TIMESTAMPTZ,
    cleaned_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


def _rows(cur) -> list[dict[str, Any]]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _scalar(cur, sql: str, params: tuple | None = None) -> Any:
    cur.execute(sql, params or ())
    row = cur.fetchone()
    return row[0] if row else None


def fetch_invalid_rows(cur) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT id, detail_id, related_document_id, related_document_type, created_at
        FROM distribuidora.document_related
        WHERE related_document_type NOT IN (1, 6, 9)
        ORDER BY id
        """
    )
    return _rows(cur)


def fetch_type_breakdown(cur) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT related_document_type, COUNT(*)::bigint AS n
        FROM distribuidora.document_related
        WHERE related_document_type NOT IN (1, 6, 9)
        GROUP BY related_document_type
        ORDER BY related_document_type
        """
    )
    return _rows(cur)


def run_post_delete_audit(cur) -> dict[str, Any]:
    total = int(_scalar(cur, "SELECT COUNT(*) FROM distribuidora.document_related") or 0)
    invalid_n = int(
        _scalar(
            cur,
            """
            SELECT COUNT(*) FROM distribuidora.document_related
            WHERE related_document_type NOT IN (1, 6, 9)
            """,
        )
        or 0
    )
    orphan_n = int(
        _scalar(
            cur,
            """
            SELECT COUNT(*) FROM distribuidora.document_related dr
            LEFT JOIN distribuidora.documents rel ON rel.document_id = dr.related_document_id
            WHERE rel.document_id IS NULL
            """,
        )
        or 0
    )
    return {
        "total_rows": total,
        "count_invalid_related_types": invalid_n,
        "count_orphan_related_documents": orphan_n,
    }


def _print_report(
    rows: list[dict[str, Any]],
    breakdown: list[dict[str, Any]],
) -> None:
    print("=" * 60)
    print("cleanup_document_related_invalid_types — candidatas")
    print("=" * 60)
    print()
    print("Criterio: related_document_type NOT IN (1, 6, 9)")
    print("  (1=boleta, 6=factura, 9=nota de crédito; 33=OC no debe ser tipo relacionado)")
    print()
    n = len(rows)
    print(f"Cantidad encontrada: {n}")
    print()
    print("Desglose por related_document_type (solo inválidos):")
    if not breakdown:
        print("  (ninguno)")
    else:
        for b in breakdown:
            tid = b.get("related_document_type")
            c = b.get("n")
            note = ""
            if tid == OC_TYPE_ID:
                note = "  ← OC (tipo 33): no operacional en document_related"
            print(f"  type={tid!r}  count={c}{note}")
    print()
    ids = [r["id"] for r in rows]
    print(f"IDs (document_related.id): {ids}")
    print()
    print("Filas completas:")
    for i, r in enumerate(rows, start=1):
        print(f"  [{i}] {json.dumps(r, default=str, ensure_ascii=False)}")
    print()
    if n > 0 and all(r.get("related_document_type") == OC_TYPE_ID for r in rows):
        print(
            f"Confirmación: las {n} fila(s) tienen related_document_type = {OC_TYPE_ID} (OC)."
        )
    elif n > 0:
        print(
            "Confirmación: hay tipos distintos de 33 entre las filas inválidas; "
            "revisar desglose arriba."
        )
    print("=" * 60)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Elimina filas document_related con related_document_type fuera de 1,6,9."
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Ejecutar backup + DELETE en una transacción (sin este flag: dry-run).",
    )
    args = parser.parse_args()
    dry_run = not args.execute

    load_dotenv(REPO_ROOT / ".env")
    load_dotenv()

    conn = get_connection()
    conn.autocommit = False
    try:
        cur = conn.cursor()
        rows = fetch_invalid_rows(cur)
        breakdown = fetch_type_breakdown(cur)
        _print_report(rows, breakdown)

        if dry_run:
            print("DRY_RUN: no se ha modificado la base de datos.")
            conn.rollback()
            return 0

        if not rows:
            print("No hay filas que eliminar; COMMIT vacío.")
            conn.commit()
            return 0

        ids = [int(r["id"]) for r in rows]
        placeholders = ",".join(["%s"] * len(ids))

        try:
            cur.execute(CREATE_BACKUP_TABLE_SQL)

            cur.execute(
                f"""
                INSERT INTO distribuidora.document_related_cleanup_backup (
                    original_row_id, detail_id, related_document_id,
                    related_document_type, original_created_at
                )
                SELECT id, detail_id, related_document_id, related_document_type, created_at
                FROM distribuidora.document_related
                WHERE id IN ({placeholders})
                """,
                ids,
            )
            backed_up = cur.rowcount

            cur.execute(
                f"""
                DELETE FROM distribuidora.document_related
                WHERE id IN ({placeholders})
                """,
                ids,
            )
            deleted = cur.rowcount

            conn.commit()
            print(f"Transacción completada: backup insertadas={backed_up}, delete={deleted}")
        except Exception:
            conn.rollback()
            raise

        cur = conn.cursor()
        post = run_post_delete_audit(cur)
        print()
        print("Auditoría básica posterior (mismas consultas que audit):")
        print(f"  total_rows:                      {post['total_rows']}")
        print(f"  count_invalid_related_types:   {post['count_invalid_related_types']}")
        print(f"  count_orphan_related_documents: {post['count_orphan_related_documents']}")
        conn.commit()

    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
