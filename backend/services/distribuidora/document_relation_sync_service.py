"""
Descubrimiento / catchup de relaciones documentales (OC↔factura, factura↔NC).

Fuente de verdad operacional:
- OC → factura/boleta:
  1. Bsale ``GET /documents.json?relateddetailid=`` (vía sync_related), y
  2. **local** vía ``document_details.related_detail_id`` de la factura/boleta
     apuntando a un detalle de OC (mismo campo Bsale ``relatedDetailId``).
     Esto cubre facturación diferida cuando el sync API no alcanza la OC
     (presupuesto de runtime / cola pending más recientes primero).
- Factura/boleta → NC: **local** vía ``document_details.related_detail_id`` de la NC
  (campo Bsale ``relatedDetailId`` en details de tipo 9), NO ``references.json``
  (vacío en muestras) ni ``bsale.returns`` (0 filas company 3).

Este módulo NO inventa vínculos por cliente/monto.
``--dry-run`` por defecto: solo reporta; apply requiere ``dry_run=False`` explícito.
"""

from __future__ import annotations

import logging
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable

logger = logging.getLogger(__name__)

COMPANY_ID = 3
OFFICE_ID = 1
DOC_TYPE_OC = 33
DOC_TYPE_BOLETA = 1
DOC_TYPE_FACTURA = 6
DOC_TYPE_NC = 9
INVOICE_TYPES = frozenset({DOC_TYPE_BOLETA, DOC_TYPE_FACTURA})
PROBABLE_QUEUE_MIN_SCORE = 75.0

# Ventana de sanación local OC←factura: se aplica a la **emisión de la factura/boleta**,
# no a la OC. Así una OC de hace meses facturada recientemente sigue conciliándose.
DEFAULT_LOCAL_OC_INVOICE_HEAL_LOOKBACK_DAYS = 120


def resolve_local_oc_invoice_heal_lookback_days(
    related_lookback_days: int | None = None,
) -> int:
    """
    Días hacia atrás sobre ``invoice.emission_date`` para materializar desde
    ``related_detail_id``. La edad de la OC **no** se filtra.

    Precedencia:
      1. ``RELATED_LOCAL_HEAL_LOOKBACK_DAYS``
      2. ``max(related_lookback_days, DEFAULT_LOCAL_OC_INVOICE_HEAL_LOOKBACK_DAYS)``
         si se pasa related_lookback
      3. ``DEFAULT_LOCAL_OC_INVOICE_HEAL_LOOKBACK_DAYS`` (120)
    """
    raw = os.getenv("RELATED_LOCAL_HEAL_LOOKBACK_DAYS", "").strip()
    if raw:
        try:
            return max(1, int(raw))
        except ValueError:
            pass
    base = DEFAULT_LOCAL_OC_INVOICE_HEAL_LOOKBACK_DAYS
    if related_lookback_days is not None:
        return max(1, max(int(related_lookback_days), base))
    return base


def oc_has_confirmed_invoice_relation(related_types: Iterable[int]) -> bool:
    """
    True si la OC tiene al menos una relación confirmada boleta/factura (1/6).

    Usa ``document_related.related_document_type`` (lo que persiste sync_related).
    Tipo 9 (NC) NO cuenta como factura confirmada.
    """
    return any(int(t) in INVOICE_TYPES for t in related_types)


def filter_probable_investigation_queue(
    rows: list[dict[str, Any]],
    *,
    confirmed_oc_document_ids: Iterable[int] | None = None,
    confirmed_related_by_oc: dict[int, Iterable[int]] | None = None,
    min_score: float = PROBABLE_QUEUE_MIN_SCORE,
) -> list[dict[str, Any]]:
    """
    Post-filtro defensivo: CONFIRMED > PROBABLE.

    Excluye OCs con relación 1/6 conocida aunque el SQL upstream falle.
    Deduplica por ``oc_document_id`` (score más alto gana).
    """
    confirmed_ids: set[int] = set(confirmed_oc_document_ids or ())
    if confirmed_related_by_oc:
        for oc_id, types in confirmed_related_by_oc.items():
            if oc_has_confirmed_invoice_relation(types):
                confirmed_ids.add(int(oc_id))

    eligible: list[dict[str, Any]] = []
    for row in rows:
        oc_id = int(row["oc_document_id"])
        if oc_id in confirmed_ids:
            continue
        score = row.get("probable_score")
        if score is None or float(score) < min_score:
            continue
        eligible.append(dict(row))

    by_oc: dict[int, dict[str, Any]] = {}
    for row in eligible:
        oc_id = int(row["oc_document_id"])
        prev = by_oc.get(oc_id)
        if prev is None or float(row.get("probable_score") or 0) > float(
            prev.get("probable_score") or 0
        ):
            by_oc[oc_id] = row

    out = sorted(
        by_oc.values(),
        key=lambda r: (-float(r.get("probable_score") or 0), -int(r.get("oc_number") or 0)),
    )
    return out


def build_probable_investigation_queue_from_fixtures(
    *,
    oc_probables: list[dict[str, Any]],
    related_types_by_oc: dict[int, list[int]],
    min_score: float = PROBABLE_QUEUE_MIN_SCORE,
) -> list[dict[str, Any]]:
    """Cola probable in-memory para tests (sin PostgreSQL)."""
    raw: list[dict[str, Any]] = []
    for p in oc_probables:
        oc_id = int(p["oc_document_id"])
        score = float(p.get("probable_score") or 0)
        if score < min_score:
            continue
        if oc_has_confirmed_invoice_relation(related_types_by_oc.get(oc_id, [])):
            continue
        raw.append(dict(p))
    return filter_probable_investigation_queue(raw, min_score=min_score)


def _confirmed_invoice_not_exists_sql(oc_alias: str = "o") -> str:
    """
    Fragmento NOT EXISTS: relación confirmada 1/6 en document_related de la OC.

    No depende de JOIN a ``documents`` (evita falsos negativos si el header
    de la factura aún no está materializado pero ``related_document_type`` sí).
    """
    return f"""
    NOT EXISTS (
        SELECT 1
        FROM distribuidora.v_document_related_resolved dr
        WHERE dr.origin_document_id = {oc_alias}.document_id
          AND dr.related_document_type IN (1, 6)
    )
    """.strip()


def discover_oc_missing_related_with_probable_sql() -> str:
    """
    Cola de investigación: score >= 75 AND sin relación confirmada 1/6.

    Precedencia: confirmed domina probable (NOT EXISTS antes de incluir fila).
    Una fila por OC (DISTINCT ON, mayor score).
    """
    confirmed_filter = _confirmed_invoice_not_exists_sql("o")
    return f"""
    WITH ocs AS (
        SELECT d.document_id, d.number, d.emission_date, d.total_amount, d.client_id
        FROM distribuidora.documents d
        WHERE d.company_id = %s
          AND d.office_id = %s
          AND d.document_type_id = 33
          AND COALESCE(d.state, 0) = 0
          AND d.emission_date >= (NOW() AT TIME ZONE 'UTC' - (%s * interval '1 day'))
    )
    SELECT DISTINCT ON (o.document_id)
        o.document_id AS oc_document_id,
        o.number AS oc_number,
        pm.score AS probable_score,
        pm.candidate_document_id,
        cand.number AS candidate_number,
        cand.document_type_id AS candidate_type,
        (o.client_id IS NOT DISTINCT FROM cand.client_id) AS same_client,
        (o.total_amount IS NOT DISTINCT FROM cand.total_amount) AS same_amount
    FROM ocs o
    INNER JOIN distribuidora.document_probable_matches pm
        ON pm.oc_document_id = o.document_id AND pm.score >= {int(PROBABLE_QUEUE_MIN_SCORE)}
    INNER JOIN distribuidora.documents cand
        ON cand.document_id = pm.candidate_document_id
    WHERE {confirmed_filter}
    ORDER BY o.document_id, pm.score DESC, o.number DESC
    """


@dataclass
class RelationSyncReport:
    company_id: int = COMPANY_ID
    office_id: int = OFFICE_ID
    recent_days: int = 45
    dry_run: bool = True
    documents_scanned: int = 0
    oc_scanned: int = 0
    nc_scanned: int = 0
    relations_existing: int = 0
    relations_discovered: int = 0
    invoice_links_new: int = 0
    credit_note_links_new: int = 0
    credit_note_links_existing: int = 0
    unchanged: int = 0
    unresolved: int = 0
    errors: list[str] = field(default_factory=list)
    samples: dict[str, Any] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def discover_credit_note_invoice_links_sql() -> str:
    """
    NC (9) → factura/boleta (1/6) vía related_detail_id ya persistido en details.
    No llama Bsale.
    """
    return """
    SELECT DISTINCT
        nc.document_id AS nc_document_id,
        nc.number AS nc_number,
        nc.total_amount AS nc_total,
        COALESCE(nc.state, 0) AS nc_state,
        invd.detail_id AS invoice_detail_id,
        inv.document_id AS invoice_document_id,
        inv.number AS invoice_number,
        inv.document_type_id AS invoice_document_type_id,
        inv.total_amount AS invoice_total,
        EXISTS (
            SELECT 1
            FROM distribuidora.document_related dr
            WHERE dr.detail_id = invd.detail_id
              AND dr.related_document_id = nc.document_id
        ) AS already_in_document_related
    FROM distribuidora.documents nc
    INNER JOIN distribuidora.document_details ncd
        ON ncd.document_id = nc.document_id
       AND ncd.related_detail_id IS NOT NULL
    INNER JOIN distribuidora.v_document_detail_lineage invd
        ON invd.detail_id = ncd.related_detail_id
    INNER JOIN distribuidora.documents inv
        ON inv.document_id = invd.document_id
       AND inv.document_type_id IN (1, 6)
    WHERE nc.company_id = %s
      AND nc.office_id = %s
      AND nc.document_type_id = 9
      AND nc.emission_date >= (NOW() AT TIME ZONE 'UTC' - (%s * interval '1 day'))
    ORDER BY nc.number DESC NULLS LAST, inv.number DESC NULLS LAST
    """


def dry_run_plan_fulfillment_sql() -> str:
    """Dry-run exclusión vs completed usando generationDate vs added_at."""
    return """
    SELECT
        dpo.dispatch_plan_id,
        dpo.oc_number,
        dpo.oc_document_id,
        dpo.created_at AS added_at,
        inv.number AS invoice_number,
        to_timestamp((inv.raw_data->>'generationDate')::bigint) AS invoice_generation,
        CASE
            WHEN to_timestamp((inv.raw_data->>'generationDate')::bigint) < dpo.created_at
                THEN 'excluded_preexisting_invoice'
            WHEN to_timestamp((inv.raw_data->>'generationDate')::bigint) >= dpo.created_at
                THEN 'fulfilled_by_invoice'
            ELSE 'unresolved_timestamp'
        END AS would_fulfillment
    FROM distribuidora.dispatch_plan_orders dpo
    INNER JOIN distribuidora.dispatch_plan dp ON dp.id = dpo.dispatch_plan_id
    INNER JOIN LATERAL (
        SELECT inv.*
        FROM distribuidora.v_document_related_resolved dr
        INNER JOIN distribuidora.documents inv
            ON inv.document_id = dr.related_document_id
           AND inv.document_type_id IN (1, 6)
        WHERE dr.origin_document_id = dpo.oc_document_id
        ORDER BY COALESCE(
            to_timestamp((inv.raw_data->>'generationDate')::bigint),
            inv.emission_date
        ) ASC
        LIMIT 1
    ) inv ON TRUE
    WHERE dp.created_at >= (NOW() AT TIME ZONE 'UTC' - (%s * interval '1 day'))
    ORDER BY dpo.dispatch_plan_id, dpo.oc_number
    """


def materialize_cn_related_rows(links: list[dict[str, Any]]) -> list[tuple[int, int, int]]:
    """
    Triples (invoice_detail_id, nc_document_id, 9) a insertar en document_related.
    Solo filas aún no presentes; ignora NC anuladas.
    """
    out: list[tuple[int, int, int]] = []
    seen: set[tuple[int, int]] = set()
    for row in links:
        if int(row.get("nc_state") or 0) != 0:
            continue
        if row.get("already_in_document_related"):
            continue
        detail_id = int(row["invoice_detail_id"])
        nc_id = int(row["nc_document_id"])
        key = (detail_id, nc_id)
        if key in seen:
            continue
        seen.add(key)
        out.append((detail_id, nc_id, DOC_TYPE_NC))
    return out


def discover_oc_invoice_links_from_related_detail_sql(
    *,
    oc_document_id: int | None = None,
) -> str:
    """
    OC (33) ← factura/boleta (1/6) vía ``related_detail_id`` ya persistido en details
    de la factura (apunta al detalle de la OC). No llama Bsale.

    La arista operacional en ``document_related`` es OC_detail → invoice_document.

    Integridad: misma empresa/oficina en factura y OC; tipos 1/6 → 33; vínculo real
    ``related_detail_id`` (no cliente/monto). La ventana temporal filtra solo
    ``inv.emission_date`` — la OC puede ser de meses atrás.
    """
    oc_filter = ""
    if oc_document_id is not None:
        oc_filter = "AND oc.document_id = %s"
    return f"""
    SELECT DISTINCT
        ocd.detail_id AS oc_detail_id,
        oc.document_id AS oc_document_id,
        oc.number AS oc_number,
        COALESCE(oc.state, 0) AS oc_state,
        inv.document_id AS invoice_document_id,
        inv.number AS invoice_number,
        inv.document_type_id AS invoice_document_type_id,
        inv.total_amount AS invoice_total,
        COALESCE(inv.state, 0) AS invoice_state,
        EXISTS (
            SELECT 1
            FROM distribuidora.document_related dr
            WHERE dr.detail_id = ocd.detail_id
              AND dr.related_document_id = inv.document_id
        ) AS already_in_document_related
    FROM distribuidora.documents inv
    INNER JOIN distribuidora.document_details invd
        ON invd.document_id = inv.document_id
       AND invd.related_detail_id IS NOT NULL
       AND invd.related_detail_id <> 0
    INNER JOIN distribuidora.v_document_detail_lineage ocd
        ON ocd.detail_id = invd.related_detail_id
    INNER JOIN distribuidora.documents oc
        ON oc.document_id = ocd.document_id
       AND oc.document_type_id = {DOC_TYPE_OC}
       AND oc.company_id = inv.company_id
       AND oc.office_id = inv.office_id
    WHERE inv.company_id = %s
      AND inv.office_id = %s
      AND inv.document_type_id IN ({DOC_TYPE_BOLETA}, {DOC_TYPE_FACTURA})
      AND inv.emission_date >= (NOW() AT TIME ZONE 'UTC' - (%s * interval '1 day'))
      {oc_filter}
    ORDER BY oc.number DESC NULLS LAST, inv.number DESC NULLS LAST
    """


def materialize_oc_invoice_related_rows(
    links: list[dict[str, Any]],
) -> list[tuple[int, int, int]]:
    """
    Triples (oc_detail_id, invoice_document_id, invoice_type 1|6) a insertar.

    Solo filas aún no presentes; ignora facturas/OC anuladas; exige tipo 1/6.
    No usa cliente/monto: el vínculo debe venir de ``related_detail_id``.
    """
    out: list[tuple[int, int, int]] = []
    seen: set[tuple[int, int]] = set()
    for row in links:
        if int(row.get("invoice_state") or 0) != 0:
            continue
        if int(row.get("oc_state") or 0) != 0:
            continue
        if row.get("already_in_document_related"):
            continue
        inv_type = int(row.get("invoice_document_type_id") or 0)
        if inv_type not in INVOICE_TYPES:
            continue
        oc_detail_id = int(row["oc_detail_id"])
        inv_id = int(row["invoice_document_id"])
        key = (oc_detail_id, inv_id)
        if key in seen:
            continue
        seen.add(key)
        out.append((oc_detail_id, inv_id, inv_type))
    return out


def fetch_oc_invoice_links_from_related_detail(
    cur,
    *,
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
    recent_days: int | None = None,
    oc_document_id: int | None = None,
) -> list[dict[str, Any]]:
    """Descubre pares OC↔factura locales (solo lectura)."""
    days = (
        max(1, int(recent_days))
        if recent_days is not None
        else resolve_local_oc_invoice_heal_lookback_days()
    )
    sql = discover_oc_invoice_links_from_related_detail_sql(
        oc_document_id=oc_document_id
    )
    params: list[Any] = [company_id, office_id, days]
    if oc_document_id is not None:
        params.append(int(oc_document_id))
    cur.execute(sql, tuple(params))
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def apply_oc_invoice_links_from_related_detail(
    cur,
    *,
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
    recent_days: int | None = None,
    oc_document_id: int | None = None,
    dry_run: bool = True,
) -> dict[str, Any]:
    """
    Materializa aristas OC→factura desde ``related_detail_id`` ya en PG.

    Idempotente (``ON CONFLICT DO NOTHING``). Con ``dry_run=True`` no escribe.
    Si ``oc_document_id`` se indica, limita el alcance a esa OC (sanación puntual).
    ``recent_days`` acota emisión de la **factura**; la OC no tiene límite de edad.
    """
    days = (
        max(1, int(recent_days))
        if recent_days is not None
        else resolve_local_oc_invoice_heal_lookback_days()
    )
    links = fetch_oc_invoice_links_from_related_detail(
        cur,
        company_id=company_id,
        office_id=office_id,
        recent_days=days,
        oc_document_id=oc_document_id,
    )
    existing = sum(1 for r in links if r.get("already_in_document_related"))
    triples = materialize_oc_invoice_related_rows(links)
    oc_ids_with_new: set[int] = set()
    inv_by_detail = {
        (int(r["oc_detail_id"]), int(r["invoice_document_id"])): int(r["oc_document_id"])
        for r in links
        if r.get("oc_detail_id") is not None and r.get("invoice_document_id") is not None
    }
    for detail_id, inv_id, _tid in triples:
        oc_id = inv_by_detail.get((detail_id, inv_id))
        if oc_id is not None:
            oc_ids_with_new.add(oc_id)
    result: dict[str, Any] = {
        "dry_run": dry_run,
        "lookback_days": days,
        "links_found": len(links),
        "links_existing": existing,
        "triples_new": len(triples),
        "rows_inserted": 0,
        "oc_document_ids": sorted({int(r["oc_document_id"]) for r in links}),
        "oc_document_ids_new": sorted(oc_ids_with_new),
        "sample": [
            {
                "oc_number": r.get("oc_number"),
                "invoice_number": r.get("invoice_number"),
                "invoice_type": r.get("invoice_document_type_id"),
                "already_related": bool(r.get("already_in_document_related")),
            }
            for r in links[:8]
        ],
    }
    if dry_run or not triples:
        return result

    inserted = 0
    for detail_id, inv_id, tid in triples:
        cur.execute(
            """
            INSERT INTO distribuidora.document_related (
                detail_id, related_document_id, related_document_type
            )
            VALUES (%s, %s, %s)
            ON CONFLICT (detail_id, related_document_id) DO NOTHING
            """,
            (detail_id, inv_id, tid),
        )
        inserted += int(cur.rowcount or 0)
    result["rows_inserted"] = inserted
    return result


def run_relation_sync_audit(
    cur,
    *,
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
    recent_days: int = 45,
    dry_run: bool = True,
) -> RelationSyncReport:
    """
    Auditoría + descubrimiento. Con ``dry_run=True`` (default) no escribe.
    Apply materializa:
      - NC: aristas invoice_detail → NC en document_related
      - OC→factura: aristas OC_detail → factura/boleta desde related_detail_id local
    El sync live también llama a la sanación OC local (sin depender solo de la API).
    """
    report = RelationSyncReport(
        company_id=company_id,
        office_id=office_id,
        recent_days=recent_days,
        dry_run=dry_run,
    )
    report.notes.extend(
        [
            "OC→factura: Bsale relateddetailid (sync_related) + sanación local "
            f"desde related_detail_id de factura/boleta (ventana={recent_days}d).",
            "Factura→NC: relatedDetailId en details de NC (ya en PG); "
            "references.json vacío en muestras; bsale.returns=0.",
            "Probable>=75 no se promociona a confirmed.",
        ]
    )

    try:
        oc_inv_links = fetch_oc_invoice_links_from_related_detail(
            cur,
            company_id=company_id,
            office_id=office_id,
            recent_days=recent_days,
        )
    except Exception as exc:
        report.errors.append(f"oc_invoice_discover: {exc}")
        oc_inv_links = []

    oc_inv_existing = sum(1 for r in oc_inv_links if r.get("already_in_document_related"))
    oc_inv_triples = materialize_oc_invoice_related_rows(oc_inv_links)
    report.invoice_links_new = len(oc_inv_triples)
    report.relations_discovered += len(oc_inv_triples)
    report.relations_existing += oc_inv_existing
    report.samples["oc_invoice_links"] = [
        {
            "oc_number": r.get("oc_number"),
            "invoice_number": r.get("invoice_number"),
            "invoice_type": r.get("invoice_document_type_id"),
            "already_related": bool(r.get("already_in_document_related")),
            "oc_state": r.get("oc_state"),
            "invoice_state": r.get("invoice_state"),
        }
        for r in oc_inv_links[:8]
    ]
    report.samples["oc_invoice_links_missing_edge"] = [
        {
            "oc_number": r.get("oc_number"),
            "oc_document_id": r.get("oc_document_id"),
            "invoice_number": r.get("invoice_number"),
            "invoice_document_id": r.get("invoice_document_id"),
        }
        for r in oc_inv_links
        if not r.get("already_in_document_related")
        and int(r.get("invoice_state") or 0) == 0
        and int(r.get("oc_state") or 0) == 0
    ][:10]

    try:
        cur.execute(
            discover_credit_note_invoice_links_sql(),
            (company_id, office_id, recent_days),
        )
        cols = [d[0] for d in cur.description]
        cn_links = [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception as exc:
        report.errors.append(f"cn_discover: {exc}")
        cn_links = []

    report.nc_scanned = len({int(r["nc_document_id"]) for r in cn_links})
    report.credit_note_links_existing = sum(
        1 for r in cn_links if r.get("already_in_document_related")
    )
    new_triples = materialize_cn_related_rows(cn_links)
    report.credit_note_links_new = len(new_triples)
    report.relations_discovered += len(new_triples)
    report.relations_existing += report.credit_note_links_existing
    report.samples["credit_note_links"] = [
        {
            "nc_number": r.get("nc_number"),
            "invoice_number": r.get("invoice_number"),
            "invoice_type": r.get("invoice_document_type_id"),
            "already_related": bool(r.get("already_in_document_related")),
            "nc_state": r.get("nc_state"),
        }
        for r in cn_links[:8]
    ]

    # NC sin related_detail resoluble
    try:
        cur.execute(
            """
            SELECT COUNT(*)::int
            FROM distribuidora.documents nc
            WHERE nc.company_id = %s AND nc.office_id = %s AND nc.document_type_id = 9
              AND nc.emission_date >= (NOW() AT TIME ZONE 'UTC' - (%s * interval '1 day'))
              AND NOT EXISTS (
                SELECT 1
                FROM distribuidora.document_details ncd
                INNER JOIN distribuidora.v_document_detail_lineage invd
                    ON invd.detail_id = ncd.related_detail_id
                INNER JOIN distribuidora.documents inv
                    ON inv.document_id = invd.document_id
                   AND inv.document_type_id IN (1, 6)
                WHERE ncd.document_id = nc.document_id
                  AND ncd.related_detail_id IS NOT NULL
              )
            """,
            (company_id, office_id, recent_days),
        )
        report.unresolved = int(cur.fetchone()[0] or 0)
    except Exception as exc:
        report.errors.append(f"cn_unresolved: {exc}")

    try:
        cur.execute(
            discover_oc_missing_related_with_probable_sql(),
            (company_id, office_id, recent_days),
        )
        pcols = [d[0] for d in cur.description]
        prob_rows = [dict(zip(pcols, r)) for r in cur.fetchall()]
    except Exception as exc:
        report.errors.append(f"probable_queue: {exc}")
        prob_rows = []

    prob_rows = filter_probable_investigation_queue(prob_rows)

    report.samples["probable_investigation_queue"] = [
        {
            "oc_document_id": r.get("oc_document_id"),
            "oc_number": r.get("oc_number"),
            "score": float(r["probable_score"]) if r.get("probable_score") is not None else None,
            "candidate_number": r.get("candidate_number"),
            "same_client": r.get("same_client"),
            "same_amount": r.get("same_amount"),
            "action": "investigate_relateddetailid_catchup",
        }
        for r in prob_rows[:10]
    ]
    report.samples["probable_queue_count"] = len(prob_rows)

    try:
        cur.execute(
            """
            SELECT COUNT(*)::int
            FROM distribuidora.documents d
            WHERE d.company_id = %s AND d.office_id = %s AND d.document_type_id = 33
              AND d.emission_date >= (NOW() AT TIME ZONE 'UTC' - (%s * interval '1 day'))
            """,
            (company_id, office_id, recent_days),
        )
        report.oc_scanned = int(cur.fetchone()[0] or 0)
    except Exception as exc:
        report.errors.append(f"oc_scanned: {exc}")

    report.documents_scanned = report.oc_scanned + report.nc_scanned

    try:
        cur.execute(dry_run_plan_fulfillment_sql(), (recent_days,))
        fcols = [d[0] for d in cur.description]
        plan_rows = [dict(zip(fcols, r)) for r in cur.fetchall()]
        from collections import Counter

        c = Counter(r.get("would_fulfillment") for r in plan_rows)
        report.samples["plan_fulfillment_dry_run"] = {
            "rows": len(plan_rows),
            "counts": dict(c),
            "examples": [
                {
                    "plan_id": r.get("dispatch_plan_id"),
                    "oc_number": r.get("oc_number"),
                    "invoice_number": r.get("invoice_number"),
                    "would_fulfillment": r.get("would_fulfillment"),
                }
                for r in plan_rows[:5]
            ],
        }
    except Exception as exc:
        report.errors.append(f"plan_dry_run: {exc}")

    if dry_run:
        report.unchanged = report.credit_note_links_existing + oc_inv_existing
        report.notes.append("dry_run=True: no se insertó ninguna fila en document_related.")
        return report

    # APPLY explícito: NC + OC→factura local (idempotente ON CONFLICT DO NOTHING).
    inserted = 0
    for detail_id, nc_id, tid in new_triples:
        try:
            cur.execute(
                """
                INSERT INTO distribuidora.document_related (
                    detail_id, related_document_id, related_document_type
                )
                VALUES (%s, %s, %s)
                ON CONFLICT (detail_id, related_document_id) DO NOTHING
                """,
                (detail_id, nc_id, tid),
            )
            inserted += int(cur.rowcount or 0)
        except Exception as exc:
            report.errors.append(f"insert nc {detail_id}->{nc_id}: {exc}")

    oc_inserted = 0
    for detail_id, inv_id, tid in oc_inv_triples:
        try:
            cur.execute(
                """
                INSERT INTO distribuidora.document_related (
                    detail_id, related_document_id, related_document_type
                )
                VALUES (%s, %s, %s)
                ON CONFLICT (detail_id, related_document_id) DO NOTHING
                """,
                (detail_id, inv_id, tid),
            )
            oc_inserted += int(cur.rowcount or 0)
        except Exception as exc:
            report.errors.append(f"insert oc_inv {detail_id}->{inv_id}: {exc}")

    report.unchanged = report.credit_note_links_existing + oc_inv_existing
    report.notes.append(
        f"apply: nc_insertados={inserted}/{len(new_triples)} "
        f"oc_factura_insertados={oc_inserted}/{len(oc_inv_triples)}."
    )
    return report


def classify_absence_bucket(
    *,
    has_related: bool,
    probable_score: float | None,
    same_client: bool | None,
    same_amount: bool | None,
    has_indirect: bool = False,
) -> str:
    """Clasificación heurística READ-ONLY (no confirma sync Bsale sin API)."""
    if has_related:
        return "has_related"
    if has_indirect:
        return "indirect_only"
    if probable_score is not None and probable_score >= 75:
        if same_client and same_amount:
            return "likely_missing_sync_exact_match"
        if same_client:
            return "probable_same_client_diff_amount"
        return "probable_weak"
    if probable_score is not None and probable_score >= 60:
        return "probable_lt75"
    return "no_related_no_probable"
