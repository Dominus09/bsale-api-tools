"""Persistencia de ``distribuidora.document_probable_matches``."""

from __future__ import annotations

from typing import Any

from psycopg2.extras import execute_values


def upsert_probable_matches(cur, rows: list[dict[str, Any]]) -> int:
    """Inserta o actualiza filas; retorna cantidad de filas enviadas."""
    if not rows:
        return 0
    cols = (
        "oc_document_id",
        "candidate_document_id",
        "candidate_document_type",
        "score",
        "match_products_pct",
        "same_client",
        "same_seller",
        "same_day",
        "same_amount",
        "tracking_match",
    )
    values = [
        tuple(r[c] for c in cols)
        for r in rows
    ]
    execute_values(
        cur,
        """
        INSERT INTO distribuidora.document_probable_matches (
            oc_document_id,
            candidate_document_id,
            candidate_document_type,
            score,
            match_products_pct,
            same_client,
            same_seller,
            same_day,
            same_amount,
            tracking_match
        ) VALUES %s
        ON CONFLICT (oc_document_id, candidate_document_id) DO UPDATE SET
            candidate_document_type = EXCLUDED.candidate_document_type,
            score = EXCLUDED.score,
            match_products_pct = EXCLUDED.match_products_pct,
            same_client = EXCLUDED.same_client,
            same_seller = EXCLUDED.same_seller,
            same_day = EXCLUDED.same_day,
            same_amount = EXCLUDED.same_amount,
            tracking_match = EXCLUDED.tracking_match
        """,
        values,
    )
    return len(rows)


def delete_probable_matches_for_oc(cur, oc_document_id: int) -> None:
    cur.execute(
        """
        DELETE FROM distribuidora.document_probable_matches
        WHERE oc_document_id = %s
        """,
        (oc_document_id,),
    )


def delete_probable_matches_below_score(cur, min_score: float) -> int:
    cur.execute(
        """
        DELETE FROM distribuidora.document_probable_matches
        WHERE score < %s
        """,
        (min_score,),
    )
    return int(cur.rowcount)
