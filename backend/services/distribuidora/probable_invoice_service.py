"""
Heurística «probable facturada» para OCs sin linkage API (relateddetailid / references vacíos).

Solo capa analítica: escribe en ``document_probable_matches``. No toca ``document_related``.
"""

from __future__ import annotations

import logging
import os
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from backend.db import get_connection
from backend.repositories.distribuidora.probable_matches_repo import (
    delete_probable_matches_below_score,
    upsert_probable_matches,
)

logger = logging.getLogger(__name__)

COMPANY_ID = 3
OFFICE_ID = 1
DOC_TYPE_OC = 33
DOC_TYPES_INVOICE = (1, 6)
MIN_SCORE_PERSIST = 60.0

WEIGHT_PRODUCTS = 50.0
WEIGHT_CLIENT = 15.0
WEIGHT_DATE = 12.0
WEIGHT_AMOUNT = 13.0
WEIGHT_SELLER = 5.0
WEIGHT_TRACKING_BONUS = 20.0
WEIGHT_ADDRESS = 5.0
WEIGHT_SUPERSET_BONUS = 10.0

DEFAULT_WINDOW_DAYS = 3
DEFAULT_AMOUNT_TOLERANCE_PCT = 15.0


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def amount_tolerance_pct() -> float:
    return _env_float("PROBABLE_INVOICE_AMOUNT_TOLERANCE_PCT", DEFAULT_AMOUNT_TOLERANCE_PCT)


def match_window_days() -> int:
    return _env_int("PROBABLE_INVOICE_WINDOW_DAYS", DEFAULT_WINDOW_DAYS)


def score_tier(score: float) -> str | None:
    if score >= 90:
        return "PROBABLE_FACTURADA_HIGH"
    if score >= 75:
        return "PROBABLE_FACTURADA_MEDIUM"
    if score >= 60:
        return "PROBABLE_FACTURADA_LOW"
    return None


@dataclass(frozen=True)
class DocumentLine:
    variant_id: int
    quantity: float


@dataclass(frozen=True)
class DocumentSnapshot:
    document_id: int
    document_type_id: int
    number: int | None
    client_id: int | None
    user_id: int | None
    seller_id: int | None
    emission_date: datetime | None
    total_amount: float | None
    tracking_number: str | None
    municipality: str | None
    address: str | None
    lines: tuple[DocumentLine, ...]


@dataclass(frozen=True)
class MatchScoreResult:
    score: float
    tier: str | None
    match_products_pct: float
    same_client: bool
    same_seller: bool
    same_day: bool
    same_amount: bool
    tracking_match: bool


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _norm_tracking(v: str | None) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _norm_text(v: str | None) -> str | None:
    if v is None:
        return None
    s = " ".join(str(v).strip().lower().split())
    return s if s else None


def _line_signature(lines: tuple[DocumentLine, ...]) -> Counter[tuple[int, float]]:
    sig: Counter[tuple[int, float]] = Counter()
    for ln in lines:
        sig[(ln.variant_id, ln.quantity)] += 1
    return sig


def _pct_amount_diff(a: float, b: float) -> float:
    base = max(abs(a), 1.0)
    return abs(a - b) / base * 100.0


def compute_probable_match_score(
    oc: DocumentSnapshot,
    candidate: DocumentSnapshot,
    *,
    amount_tol_pct: float | None = None,
    window_days: int | None = None,
) -> MatchScoreResult:
    """
    Score 0–100. Factores: productos (variant_id+cantidad), cliente, fecha, monto,
    vendedor, tracking, dirección; bonus si la boleta contiene todas las líneas de la OC.
    """
    tol = amount_tol_pct if amount_tol_pct is not None else amount_tolerance_pct()
    win = window_days if window_days is not None else match_window_days()

    oc_sig = _line_signature(oc.lines)
    cand_sig = _line_signature(candidate.lines)
    oc_line_count = sum(oc_sig.values())
    if oc_line_count == 0:
        return MatchScoreResult(
            score=0.0,
            tier=None,
            match_products_pct=0.0,
            same_client=False,
            same_seller=False,
            same_day=False,
            same_amount=False,
            tracking_match=False,
        )

    matched_units = 0
    cand_remaining = cand_sig.copy()
    for key, need in oc_sig.items():
        have = cand_remaining.get(key, 0)
        take = min(need, have)
        matched_units += take
        if take:
            cand_remaining[key] = have - take
            if cand_remaining[key] <= 0:
                del cand_remaining[key]

    match_products_pct = (matched_units / oc_line_count) * 100.0
    score = (match_products_pct / 100.0) * WEIGHT_PRODUCTS

    same_client = (
        oc.client_id is not None
        and candidate.client_id is not None
        and oc.client_id == candidate.client_id
    )
    if same_client:
        score += WEIGHT_CLIENT

    same_day = False
    date_points = 0.0
    if oc.emission_date and candidate.emission_date:
        oc_d = oc.emission_date.date()
        cand_d = candidate.emission_date.date()
        same_day = oc_d == cand_d
        delta = abs((cand_d - oc_d).days)
        if same_day:
            date_points = WEIGHT_DATE
        elif delta <= win:
            date_points = WEIGHT_DATE * (1.0 - (delta / max(win, 1)) * 0.35)
    score += date_points

    same_amount = False
    oc_total = oc.total_amount
    cand_total = candidate.total_amount
    if oc_total is not None and cand_total is not None:
        if _pct_amount_diff(oc_total, cand_total) <= tol:
            same_amount = True
            score += WEIGHT_AMOUNT
        elif match_products_pct >= 99.99 and cand_total >= oc_total:
            # Boleta consolidada: mismas líneas OC + extras (ej. OC 66697 → boleta 2616098).
            score += WEIGHT_AMOUNT * 0.75
        elif match_products_pct >= 99.99:
            score += WEIGHT_AMOUNT * 0.5

    same_seller = False
    oc_seller = oc.seller_id or oc.user_id
    cand_seller = candidate.seller_id or candidate.user_id
    if oc_seller is not None and cand_seller is not None and oc_seller == cand_seller:
        same_seller = True
        score += WEIGHT_SELLER

    oc_track = _norm_tracking(oc.tracking_number)
    cand_track = _norm_tracking(candidate.tracking_number)
    tracking_match = bool(
        oc_track and cand_track and oc_track == cand_track
    )
    if tracking_match:
        score += WEIGHT_TRACKING_BONUS

    if match_products_pct >= 99.99 and len(cand_sig) >= len(oc_sig):
        score += WEIGHT_SUPERSET_BONUS

    if (
        _norm_text(oc.address)
        and _norm_text(oc.address) == _norm_text(candidate.address)
    ):
        score += WEIGHT_ADDRESS

    score = min(100.0, round(score, 2))
    tier = score_tier(score)
    return MatchScoreResult(
        score=score,
        tier=tier,
        match_products_pct=round(match_products_pct, 2),
        same_client=same_client,
        same_seller=same_seller,
        same_day=same_day,
        same_amount=same_amount,
        tracking_match=tracking_match,
    )


def _row_to_snapshot(
    header: dict[str, Any],
    lines: list[dict[str, Any]],
) -> DocumentSnapshot:
    doc_lines: list[DocumentLine] = []
    for ln in lines:
        vid = ln.get("variant_id")
        qty = _to_float(ln.get("quantity"))
        if vid is None or qty is None or qty <= 0:
            continue
        doc_lines.append(DocumentLine(variant_id=int(vid), quantity=qty))

    em = header.get("emission_date")
    if isinstance(em, date) and not isinstance(em, datetime):
        em = datetime.combine(em, datetime.min.time(), tzinfo=timezone.utc)

    return DocumentSnapshot(
        document_id=int(header["document_id"]),
        document_type_id=int(header["document_type_id"]),
        number=int(header["number"]) if header.get("number") is not None else None,
        client_id=int(header["client_id"]) if header.get("client_id") is not None else None,
        user_id=int(header["user_id"]) if header.get("user_id") is not None else None,
        seller_id=int(header["seller_id"]) if header.get("seller_id") is not None else None,
        emission_date=em,
        total_amount=_to_float(header.get("total_amount")),
        tracking_number=header.get("tracking_number"),
        municipality=header.get("municipality"),
        address=header.get("address"),
        lines=tuple(doc_lines),
    )


def _fetch_oc_without_confirmed_invoice(
    cur,
    *,
    emission_from: date,
    emission_to: date,
) -> list[int]:
    cur.execute(
        """
        SELECT d.document_id
        FROM distribuidora.v_documents_latest d
        WHERE d.company_id = %s
          AND d.office_id = %s
          AND d.document_type_id = %s
          AND d.emission_date >= %s::date
          AND d.emission_date < (%s::date + interval '1 day')
          AND NOT EXISTS (
              SELECT 1
              FROM distribuidora.document_related dr
              INNER JOIN distribuidora.document_details dd ON dd.detail_id = dr.detail_id
              INNER JOIN distribuidora.v_documents_latest inv
                  ON inv.document_id = dr.related_document_id
                 AND inv.document_type_id IN (1, 6)
                 AND inv.company_id = d.company_id
                 AND inv.office_id = d.office_id
              WHERE dd.document_id = d.document_id
          )
        ORDER BY d.document_id
        """,
        (COMPANY_ID, OFFICE_ID, DOC_TYPE_OC, emission_from, emission_to),
    )
    return [int(r[0]) for r in cur.fetchall()]


def _fetch_document_bundle(cur, document_id: int) -> DocumentSnapshot | None:
    cur.execute(
        """
        SELECT
            document_id,
            document_type_id,
            number,
            client_id,
            user_id,
            seller_id,
            emission_date,
            total_amount,
            tracking_number,
            municipality,
            address
        FROM distribuidora.v_documents_latest
        WHERE document_id = %s
        """,
        (document_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    cols = [d[0] for d in cur.description]
    header = dict(zip(cols, row))
    cur.execute(
        """
        SELECT variant_id, quantity
        FROM distribuidora.document_details
        WHERE document_id = %s
          AND variant_id IS NOT NULL
        ORDER BY line_number NULLS LAST, detail_id
        """,
        (document_id,),
    )
    line_cols = [d[0] for d in cur.description]
    lines = [dict(zip(line_cols, r)) for r in cur.fetchall()]
    return _row_to_snapshot(header, lines)


def _fetch_invoice_candidates_for_oc(
    cur,
    oc: DocumentSnapshot,
    *,
    window_days: int,
) -> list[int]:
    if oc.client_id is None or oc.emission_date is None:
        return []
    em = oc.emission_date
    if em.tzinfo is None:
        em = em.replace(tzinfo=timezone.utc)
    d0 = (em - timedelta(days=window_days)).date()
    d1 = (em + timedelta(days=window_days)).date()
    cur.execute(
        """
        SELECT d.document_id
        FROM distribuidora.v_documents_latest d
        WHERE d.company_id = %s
          AND d.office_id = %s
          AND d.document_type_id IN (1, 6)
          AND d.client_id = %s
          AND d.emission_date >= %s::date
          AND d.emission_date < (%s::date + interval '1 day')
          AND d.document_id <> %s
        ORDER BY d.emission_date DESC NULLS LAST, d.document_id DESC
        """,
        (COMPANY_ID, OFFICE_ID, oc.client_id, d0, d1, oc.document_id),
    )
    return [int(r[0]) for r in cur.fetchall()]


def build_probable_matches_for_oc(
    cur,
    oc_document_id: int,
    *,
    amount_tol_pct: float | None = None,
    window_days: int | None = None,
    min_score: float = MIN_SCORE_PERSIST,
) -> list[dict[str, Any]]:
    """Calcula y retorna filas a persistir (score >= min_score)."""
    oc = _fetch_document_bundle(cur, oc_document_id)
    if oc is None or oc.document_type_id != DOC_TYPE_OC:
        return []

    win = window_days if window_days is not None else match_window_days()
    tol = amount_tol_pct if amount_tol_pct is not None else amount_tolerance_pct()
    cand_ids = _fetch_invoice_candidates_for_oc(cur, oc, window_days=win)
    rows: list[dict[str, Any]] = []
    for cid in cand_ids:
        cand = _fetch_document_bundle(cur, cid)
        if cand is None or cand.document_type_id not in DOC_TYPES_INVOICE:
            continue
        result = compute_probable_match_score(
            oc, cand, amount_tol_pct=tol, window_days=win
        )
        if result.score < min_score or result.tier is None:
            continue
        rows.append(
            {
                "oc_document_id": oc.document_id,
                "candidate_document_id": cand.document_id,
                "candidate_document_type": cand.document_type_id,
                "score": result.score,
                "match_products_pct": result.match_products_pct,
                "same_client": result.same_client,
                "same_seller": result.same_seller,
                "same_day": result.same_day,
                "same_amount": result.same_amount,
                "tracking_match": result.tracking_match,
            }
        )
    return rows


def build_probable_invoice_matches_may_2026(
    *,
    emission_from: date | None = None,
    emission_to: date | None = None,
) -> dict[str, Any]:
    """
    Job mayo 2026: OCs sin factura confirmada → candidatos boleta/factura en ventana ±N días.
    Solo escribe ``document_probable_matches`` (lectura DB; sin mutar API Bsale).
    """
    d0 = emission_from or date(2026, 5, 1)
    d1 = emission_to or date(2026, 5, 31)
    stats: dict[str, Any] = {
        "emission_from": d0.isoformat(),
        "emission_to": d1.isoformat(),
        "ocs_processed": 0,
        "candidates_evaluated": 0,
        "rows_upserted": 0,
        "high_tier": 0,
        "medium_tier": 0,
        "low_tier": 0,
        "errors": [],
    }

    conn = get_connection()
    try:
        conn.autocommit = False
        cur = conn.cursor()
        oc_ids = _fetch_oc_without_confirmed_invoice(
            cur, emission_from=d0, emission_to=d1
        )
        stats["ocs_total"] = len(oc_ids)

        batch: list[dict[str, Any]] = []
        for oc_id in oc_ids:
            try:
                rows = build_probable_matches_for_oc(cur, oc_id)
                stats["ocs_processed"] += 1
                stats["candidates_evaluated"] += len(rows)
                for r in rows:
                    sc = float(r["score"])
                    tier = score_tier(sc)
                    if tier == "PROBABLE_FACTURADA_HIGH":
                        stats["high_tier"] += 1
                    elif tier == "PROBABLE_FACTURADA_MEDIUM":
                        stats["medium_tier"] += 1
                    elif tier == "PROBABLE_FACTURADA_LOW":
                        stats["low_tier"] += 1
                batch.extend(rows)
                if len(batch) >= 500:
                    upsert_probable_matches(cur, batch)
                    stats["rows_upserted"] += len(batch)
                    batch.clear()
            except Exception as e:
                logger.exception("probable match oc_id=%s", oc_id)
                stats["errors"].append(f"oc {oc_id}: {e}")

        if batch:
            upsert_probable_matches(cur, batch)
            stats["rows_upserted"] += len(batch)

        deleted = delete_probable_matches_below_score(cur, MIN_SCORE_PERSIST)
        stats["rows_deleted_below_min"] = deleted
        conn.commit()
        cur.close()
    except Exception as e:
        conn.rollback()
        stats["errors"].append(str(e))
        raise
    finally:
        conn.close()

    return stats


def validate_oc_probable_match(
    oc_number: int,
    expected_candidate_number: int,
    *,
    min_tier: str = "PROBABLE_FACTURADA_HIGH",
) -> dict[str, Any]:
    """Validación puntual (ej. OC 66697 → boleta 2616098)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT document_id FROM distribuidora.v_documents_latest
            WHERE company_id = %s AND office_id = %s AND document_type_id = %s
              AND number = %s
            ORDER BY document_id DESC LIMIT 1
            """,
            (COMPANY_ID, OFFICE_ID, DOC_TYPE_OC, oc_number),
        )
        oc_row = cur.fetchone()
        cur.execute(
            """
            SELECT document_id FROM distribuidora.v_documents_latest
            WHERE company_id = %s AND office_id = %s AND document_type_id IN (1, 6)
              AND number = %s
            ORDER BY document_id DESC LIMIT 1
            """,
            (COMPANY_ID, OFFICE_ID, expected_candidate_number),
        )
        cand_row = cur.fetchone()
        if not oc_row or not cand_row:
            return {"ok": False, "reason": "document_not_in_db"}

        oc_id, cand_id = int(oc_row[0]), int(cand_row[0])
        oc = _fetch_document_bundle(cur, oc_id)
        cand = _fetch_document_bundle(cur, cand_id)
        if not oc or not cand:
            return {"ok": False, "reason": "snapshot_failed"}

        result = compute_probable_match_score(oc, cand)
        tier_order = {
            "PROBABLE_FACTURADA_HIGH": 3,
            "PROBABLE_FACTURADA_MEDIUM": 2,
            "PROBABLE_FACTURADA_LOW": 1,
        }
        ok = (
            result.tier is not None
            and tier_order.get(result.tier, 0) >= tier_order.get(min_tier, 3)
        )
        cur.execute(
            """
            SELECT score, match_products_pct FROM distribuidora.document_probable_matches
            WHERE oc_document_id = %s AND candidate_document_id = %s
            """,
            (oc_id, cand_id),
        )
        persisted = cur.fetchone()
        cur.close()
        return {
            "ok": ok,
            "oc_document_id": oc_id,
            "candidate_document_id": cand_id,
            "computed": {
                "score": result.score,
                "tier": result.tier,
                "match_products_pct": result.match_products_pct,
                "same_client": result.same_client,
                "same_seller": result.same_seller,
                "same_day": result.same_day,
                "same_amount": result.same_amount,
                "tracking_match": result.tracking_match,
            },
            "persisted": (
                {"score": float(persisted[0]), "match_products_pct": float(persisted[1])}
                if persisted
                else None
            ),
        }
    finally:
        conn.close()
