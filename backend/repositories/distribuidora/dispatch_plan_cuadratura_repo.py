"""Persistencia cuadratura operacional por plan."""

from __future__ import annotations

import json
from typing import Any


def _table_exists(cur) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'distribuidora'
          AND table_name = 'dispatch_plan_cuadratura'
        """
    )
    return cur.fetchone() is not None


def get_cuadratura_row(cur, plan_id: int) -> dict[str, Any] | None:
    if not _table_exists(cur):
        return None
    cur.execute(
        """
        SELECT transferencia_clp, efectivo_clp, cheque_clp, debito_clp,
               observacion, credit_notes, not_loaded, updated_at
        FROM distribuidora.dispatch_plan_cuadratura
        WHERE dispatch_plan_id = %s
        """,
        (int(plan_id),),
    )
    row = cur.fetchone()
    if not row:
        return None
    cols = [d[0] for d in cur.description]
    out = dict(zip(cols, row))
    for key in ("credit_notes", "not_loaded"):
        raw = out.get(key)
        if isinstance(raw, str):
            try:
                out[key] = json.loads(raw)
            except json.JSONDecodeError:
                out[key] = []
        elif raw is None:
            out[key] = []
    return out


def upsert_cuadratura(
    cur,
    *,
    plan_id: int,
    transferencia_clp: int,
    efectivo_clp: int,
    cheque_clp: int,
    debito_clp: int,
    observacion: str | None,
    credit_notes: list[dict[str, Any]],
    not_loaded: list[dict[str, Any]],
) -> None:
    if not _table_exists(cur):
        return
    cur.execute(
        """
        INSERT INTO distribuidora.dispatch_plan_cuadratura (
            dispatch_plan_id,
            transferencia_clp, efectivo_clp, cheque_clp, debito_clp,
            observacion, credit_notes, not_loaded, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, NOW())
        ON CONFLICT (dispatch_plan_id) DO UPDATE
        SET transferencia_clp = EXCLUDED.transferencia_clp,
            efectivo_clp = EXCLUDED.efectivo_clp,
            cheque_clp = EXCLUDED.cheque_clp,
            debito_clp = EXCLUDED.debito_clp,
            observacion = EXCLUDED.observacion,
            credit_notes = EXCLUDED.credit_notes,
            not_loaded = EXCLUDED.not_loaded,
            updated_at = NOW()
        """,
        (
            int(plan_id),
            max(0, int(transferencia_clp)),
            max(0, int(efectivo_clp)),
            max(0, int(cheque_clp)),
            max(0, int(debito_clp)),
            (observacion or "").strip() or None,
            json.dumps(credit_notes or []),
            json.dumps(not_loaded or []),
        ),
    )
