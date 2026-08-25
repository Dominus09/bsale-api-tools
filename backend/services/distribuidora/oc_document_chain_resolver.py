"""
Resolver canónico de cadena documental OC → (intermedio) → factura → NC.

Solo PostgreSQL sincronizado. No llama Bsale.
No inventa confirmed por cliente+monto (probable queda probable).
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterable, Sequence

from backend.services.distribuidora.oc_operational_status import (
    CREDIT_NOTE_DOC_TYPE,
    INTERMEDIATE_DOC_TYPES,
    INVOICE_DOC_TYPES,
    LinkedDocument,
    OcDocumentChain,
    OcOperationalStatus,
    build_operational_status,
)

COMPANY_ID_DEFAULT = 3
OFFICE_ID_DEFAULT = 1


def _int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _dec(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def issued_at_from_document_row(row: dict[str, Any]) -> datetime | None:
    """
    Preferir generationDate (hora real). emissionDate en Bsale suele ser
    medianoche del día y NO sirve para preexisting same-day.
    """
    raw = row.get("raw_data") if isinstance(row.get("raw_data"), dict) else {}
    for key in ("generationDate", "generation_date"):
        ts = _int(raw.get(key) if raw else None)
        if ts is None:
            ts = _int(row.get(key))
        if ts is not None and ts > 0:
            return datetime.fromtimestamp(ts, tz=timezone.utc)
    # Fallback: date-only → medianoche UTC (solo útil cross-day).
    ed = row.get("emission_date")
    if isinstance(ed, datetime):
        if ed.tzinfo is None:
            return ed.replace(tzinfo=timezone.utc)
        return ed
    return None


def _link_from_row(row: dict[str, Any], path: tuple[str, ...]) -> LinkedDocument:
    return LinkedDocument(
        document_id=int(row["document_id"]),
        number=_int(row.get("number")),
        document_type_id=int(row["document_type_id"]),
        total_amount=_dec(row.get("total_amount")),
        issued_at=issued_at_from_document_row(row),
        path=path,
    )


def assemble_chains_from_edges(
    oc_rows: Sequence[dict[str, Any]],
    edges: Sequence[dict[str, Any]],
    *,
    probable_by_oc: dict[int, dict[str, Any]] | None = None,
    credit_notes_by_invoice: dict[int, list[dict[str, Any]]] | None = None,
) -> dict[int, OcDocumentChain]:
    """
    Pure assembly (testable sin DB).

    edges: {from_document_id, to_document_id, to_number, to_type, to_total, to_raw/emission}
    """
    probable_by_oc = probable_by_oc or {}
    credit_notes_by_invoice = credit_notes_by_invoice or {}

    chains: dict[int, OcDocumentChain] = {}
    for oc in oc_rows:
        oid = int(oc["document_id"])
        chains[oid] = OcDocumentChain(
            oc_document_id=oid,
            oc_number=_int(oc.get("number")),
            oc_state=int(oc.get("state") or 0),
        )

    # Adjacency from_id -> list of target rows
    adj: dict[int, list[dict[str, Any]]] = {}
    for e in edges:
        fid = _int(e.get("from_document_id"))
        tid = _int(e.get("to_document_id"))
        if fid is None or tid is None:
            continue
        adj.setdefault(fid, []).append(e)

    for oid, chain in chains.items():
        seen: set[int] = set()
        # Direct edges from OC
        for e in adj.get(oid, []):
            to_id = int(e["to_document_id"])
            if to_id in seen:
                continue
            seen.add(to_id)
            dtype = int(e["to_document_type_id"])
            row = {
                "document_id": to_id,
                "number": e.get("to_number"),
                "document_type_id": dtype,
                "total_amount": e.get("to_total_amount"),
                "raw_data": e.get("to_raw_data") or {},
                "emission_date": e.get("to_emission_date"),
            }
            if dtype in INVOICE_DOC_TYPES:
                path = ("oc", "invoice")
                chain.confirmed_invoices.append(_link_from_row(row, path))
                chain.relation_paths.append(path)
                chain.evidence_source = "direct_related"
            elif dtype == CREDIT_NOTE_DOC_TYPE:
                path = ("oc", "credit_note")
                chain.credit_notes.append(_link_from_row(row, path))
            elif dtype in INTERMEDIATE_DOC_TYPES or True:
                # Cualquier no-factura: intentar 2º hop a factura.
                label = "picking" if dtype in (5, 8, 12) else "intermediate"
                mid_path = ("oc", label)
                mid = _link_from_row(row, mid_path)
                chain.intermediates.append(mid)
                if label == "picking":
                    chain.pickings.append(mid)
                for e2 in adj.get(to_id, []):
                    dtype2 = int(e2["to_document_type_id"])
                    if dtype2 not in INVOICE_DOC_TYPES:
                        continue
                    to2 = int(e2["to_document_id"])
                    if to2 in seen:
                        continue
                    seen.add(to2)
                    row2 = {
                        "document_id": to2,
                        "number": e2.get("to_number"),
                        "document_type_id": dtype2,
                        "total_amount": e2.get("to_total_amount"),
                        "raw_data": e2.get("to_raw_data") or {},
                        "emission_date": e2.get("to_emission_date"),
                    }
                    path2 = ("oc", label, "invoice")
                    chain.confirmed_invoices.append(_link_from_row(row2, path2))
                    chain.relation_paths.append(path2)
                    if chain.evidence_source in ("none",):
                        chain.evidence_source = "indirect_related"

        # NC asociadas a facturas confirmadas (returns / related desde factura).
        for inv in list(chain.confirmed_invoices):
            for cn in credit_notes_by_invoice.get(inv.document_id, []):
                cn_id = int(cn["document_id"])
                if any(c.document_id == cn_id for c in chain.credit_notes):
                    continue
                chain.credit_notes.append(
                    LinkedDocument(
                        document_id=cn_id,
                        number=_int(cn.get("number")),
                        document_type_id=CREDIT_NOTE_DOC_TYPE,
                        total_amount=_dec(cn.get("total_amount")),
                        issued_at=issued_at_from_document_row(cn),
                        path=("invoice", "credit_note"),
                    )
                )

        # Probable: solo si NO hay confirmed.
        if not chain.confirmed_invoices:
            prob = probable_by_oc.get(oid)
            if prob and (prob.get("score") or 0) >= 60:
                chain.probable_score = float(prob["score"])
                chain.probable_invoices.append(
                    LinkedDocument(
                        document_id=int(prob["document_id"]),
                        number=_int(prob.get("number")),
                        document_type_id=int(prob.get("document_type_id") or 6),
                        total_amount=_dec(prob.get("total_amount")),
                        issued_at=issued_at_from_document_row(prob),
                        path=("oc", "probable"),
                    )
                )
                chain.evidence_source = "probable_match"

        if chain.confirmed_invoices and chain.evidence_source == "none":
            chain.evidence_source = "direct_related"

    return chains


def resolve_oc_document_chain_from_parts(
    oc_row: dict[str, Any],
    edges: Sequence[dict[str, Any]],
    *,
    probable: dict[str, Any] | None = None,
    credit_notes_by_invoice: dict[int, list[dict[str, Any]]] | None = None,
) -> OcDocumentChain:
    oid = int(oc_row["document_id"])
    chains = assemble_chains_from_edges(
        [oc_row],
        edges,
        probable_by_oc={oid: probable} if probable else {},
        credit_notes_by_invoice=credit_notes_by_invoice or {},
    )
    return chains[oid]


def resolve_oc_operational_status_from_parts(
    oc_row: dict[str, Any],
    edges: Sequence[dict[str, Any]],
    *,
    probable: dict[str, Any] | None = None,
    credit_notes_by_invoice: dict[int, list[dict[str, Any]]] | None = None,
    planned_at: datetime | None = None,
    has_picking: bool = False,
    in_plan: bool = False,
) -> OcOperationalStatus:
    chain = resolve_oc_document_chain_from_parts(
        oc_row,
        edges,
        probable=probable,
        credit_notes_by_invoice=credit_notes_by_invoice,
    )
    return build_operational_status(
        chain,
        planned_at=planned_at,
        has_picking=has_picking,
        in_plan=in_plan,
    )


_EDGES_SQL = """
SELECT
    dd.document_id AS from_document_id,
    inv.document_id AS to_document_id,
    inv.number AS to_number,
    inv.document_type_id AS to_document_type_id,
    inv.total_amount AS to_total_amount,
    inv.raw_data AS to_raw_data,
    inv.emission_date AS to_emission_date
FROM distribuidora.document_details dd
INNER JOIN distribuidora.document_related dr ON dr.detail_id = dd.detail_id
INNER JOIN distribuidora.documents inv ON inv.document_id = dr.related_document_id
WHERE dd.document_id = ANY(%s)
"""


_OC_SQL = """
SELECT document_id, number, state, total_amount, emission_date, raw_data
FROM distribuidora.documents
WHERE document_id = ANY(%s)
"""


_PROBABLE_SQL = """
SELECT DISTINCT ON (pm.oc_document_id)
    pm.oc_document_id,
    pm.score,
    d.document_id,
    d.number,
    d.document_type_id,
    d.total_amount,
    d.raw_data,
    d.emission_date
FROM distribuidora.document_probable_matches pm
INNER JOIN distribuidora.documents d
    ON d.document_id = pm.candidate_document_id
   AND d.document_type_id IN (1, 6)
WHERE pm.oc_document_id = ANY(%s)
  AND pm.score >= 60
ORDER BY pm.oc_document_id, pm.score DESC, d.document_id DESC
"""


_CN_FROM_RELATED_DETAIL_SQL = """
SELECT DISTINCT
    invd.document_id AS invoice_document_id,
    nc.document_id,
    nc.number,
    nc.total_amount,
    nc.raw_data,
    nc.emission_date,
    COALESCE(nc.state, 0) AS state
FROM distribuidora.document_details ncd
INNER JOIN distribuidora.documents nc
    ON nc.document_id = ncd.document_id
   AND nc.document_type_id = 9
INNER JOIN distribuidora.document_details invd
    ON invd.detail_id = ncd.related_detail_id
WHERE invd.document_id = ANY(%s)
  AND ncd.related_detail_id IS NOT NULL
  AND COALESCE(nc.state, 0) = 0
"""


_CN_FROM_RETURNS_SQL = """
SELECT
    r.reference_document_id AS invoice_document_id,
    nc.document_id,
    nc.number,
    nc.total_amount,
    nc.raw_data,
    nc.emission_date
FROM bsale.returns r
INNER JOIN distribuidora.documents nc
    ON nc.document_id = r.credit_note_id
   AND nc.document_type_id = 9
WHERE r.company_id = %s
  AND r.reference_document_id = ANY(%s)
  AND r.credit_note_id IS NOT NULL
"""


def fetch_document_edges_batch(cur, document_ids: Sequence[int]) -> list[dict[str, Any]]:
    ids = [int(x) for x in document_ids if x is not None]
    if not ids:
        return []
    cur.execute(_EDGES_SQL, (ids,))
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def resolve_oc_document_chains_batch(
    cur,
    oc_document_ids: Sequence[int],
    *,
    company_id: int = COMPANY_ID_DEFAULT,
) -> dict[int, OcDocumentChain]:
    """Batch: sin N+1. Expande un hop de intermedios si aparecen en edges."""
    ids = sorted({int(x) for x in oc_document_ids if x is not None})
    if not ids:
        return {}

    cur.execute(_OC_SQL, (ids,))
    cols = [d[0] for d in cur.description]
    oc_rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    edges = fetch_document_edges_batch(cur, ids)
    # Segundo hop: documentos intermedios alcanzados desde OC.
    mid_ids = sorted(
        {
            int(e["to_document_id"])
            for e in edges
            if int(e["to_document_type_id"]) not in INVOICE_DOC_TYPES
            and int(e["to_document_type_id"]) != CREDIT_NOTE_DOC_TYPE
        }
    )
    if mid_ids:
        edges.extend(fetch_document_edges_batch(cur, mid_ids))

    probable_by_oc: dict[int, dict[str, Any]] = {}
    try:
        cur.execute(_PROBABLE_SQL, (ids,))
        pcols = [d[0] for d in cur.description]
        for row in cur.fetchall():
            prow = dict(zip(pcols, row))
            probable_by_oc[int(prow["oc_document_id"])] = prow
    except Exception:
        # Tabla puede no existir en algunos entornos de test.
        pass

    # Facturas confirmadas (directas o vía mid) para buscar NC.
    # Pre-pass: assemble without NC then attach.
    chains = assemble_chains_from_edges(oc_rows, edges, probable_by_oc=probable_by_oc)

    inv_ids = sorted(
        {inv.document_id for ch in chains.values() for inv in ch.confirmed_invoices}
    )
    credit_notes_by_invoice: dict[int, list[dict[str, Any]]] = {}
    if inv_ids:
        # Preferido: NC.details.related_detail_id → detalle de factura (evidencia Bsale real).
        try:
            cur.execute(_CN_FROM_RELATED_DETAIL_SQL, (inv_ids,))
            rcols = [d[0] for d in cur.description]
            for row in cur.fetchall():
                r = dict(zip(rcols, row))
                inv_id = int(r["invoice_document_id"])
                credit_notes_by_invoice.setdefault(inv_id, []).append(
                    {
                        "document_id": int(r["document_id"]),
                        "number": r.get("number"),
                        "total_amount": r.get("total_amount"),
                        "raw_data": r.get("raw_data") or {},
                        "emission_date": r.get("emission_date"),
                    }
                )
        except Exception:
            pass
        # Related desde factura → NC (si ya materializado en document_related)
        cn_edges = fetch_document_edges_batch(cur, inv_ids)
        for e in cn_edges:
            if int(e["to_document_type_id"]) != CREDIT_NOTE_DOC_TYPE:
                continue
            inv_id = int(e["from_document_id"])
            credit_notes_by_invoice.setdefault(inv_id, []).append(
                {
                    "document_id": int(e["to_document_id"]),
                    "number": e.get("to_number"),
                    "total_amount": e.get("to_total_amount"),
                    "raw_data": e.get("to_raw_data") or {},
                    "emission_date": e.get("to_emission_date"),
                }
            )
        # returns analytics (si hay datos)
        try:
            cur.execute(_CN_FROM_RETURNS_SQL, (company_id, inv_ids))
            rcols = [d[0] for d in cur.description]
            for row in cur.fetchall():
                r = dict(zip(rcols, row))
                inv_id = int(r["invoice_document_id"])
                credit_notes_by_invoice.setdefault(inv_id, []).append(
                    {
                        "document_id": int(r["document_id"]),
                        "number": r.get("number"),
                        "total_amount": r.get("total_amount"),
                        "raw_data": r.get("raw_data") or {},
                        "emission_date": r.get("emission_date"),
                    }
                )
        except Exception:
            pass

    # Deduplicar NC por invoice
    for inv_id, lst in list(credit_notes_by_invoice.items()):
        seen_cn: set[int] = set()
        deduped: list[dict[str, Any]] = []
        for cn in lst:
            cid = int(cn["document_id"])
            if cid in seen_cn:
                continue
            seen_cn.add(cid)
            deduped.append(cn)
        credit_notes_by_invoice[inv_id] = deduped

    if credit_notes_by_invoice:
        chains = assemble_chains_from_edges(
            oc_rows,
            edges,
            probable_by_oc=probable_by_oc,
            credit_notes_by_invoice=credit_notes_by_invoice,
        )

    return chains


def resolve_oc_document_chain(cur, document_id: int) -> OcDocumentChain:
    chains = resolve_oc_document_chains_batch(cur, [document_id])
    if document_id not in chains:
        return OcDocumentChain(oc_document_id=int(document_id), evidence_source="none")
    return chains[document_id]


def resolve_operational_statuses_batch(
    cur,
    oc_document_ids: Sequence[int],
    *,
    planned_at_by_oc: dict[int, datetime] | None = None,
    picking_oc_ids: Iterable[int] | None = None,
    in_plan: bool = False,
    company_id: int = COMPANY_ID_DEFAULT,
) -> dict[int, OcOperationalStatus]:
    planned_at_by_oc = planned_at_by_oc or {}
    picking_set = {int(x) for x in (picking_oc_ids or [])}
    chains = resolve_oc_document_chains_batch(
        cur, oc_document_ids, company_id=company_id
    )
    out: dict[int, OcOperationalStatus] = {}
    for oid, chain in chains.items():
        out[oid] = build_operational_status(
            chain,
            planned_at=planned_at_by_oc.get(oid),
            has_picking=oid in picking_set,
            in_plan=in_plan or oid in planned_at_by_oc,
        )
    return out
