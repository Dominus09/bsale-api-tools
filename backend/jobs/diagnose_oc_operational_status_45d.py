"""
Auditoría READ-ONLY 45d: estados OC vs related / NC / planificación.

Uso:
  python -m backend.jobs.diagnose_oc_operational_status_45d

No escribe en producción. No repara OCs.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from backend.db import get_connection

COMPANY_ID = 3
OFFICE_ID = 1
DAYS = 45


def _jsonable(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_jsonable(x) for x in obj]
    return obj


def run_audit(cur) -> dict[str, Any]:
    cur.execute(
        """
        WITH ocs AS (
          SELECT d.document_id, d.number
          FROM distribuidora.documents d
          WHERE d.company_id = %s AND d.office_id = %s
            AND d.document_type_id = 33 AND d.state = 0
            AND d.emission_date >= CURRENT_DATE - (%s || ' days')::interval
        ),
        direct AS (
          SELECT DISTINCT dd.document_id
          FROM distribuidora.document_details dd
          JOIN distribuidora.document_related dr ON dr.detail_id = dd.detail_id
          JOIN distribuidora.documents inv
            ON inv.document_id = dr.related_document_id
           AND inv.document_type_id IN (1, 6)
          JOIN ocs ON ocs.document_id = dd.document_id
        ),
        probable_ge75 AS (
          SELECT pm.oc_document_id, MAX(pm.score) AS score
          FROM distribuidora.document_probable_matches pm
          JOIN ocs ON ocs.document_id = pm.oc_document_id
          WHERE pm.score >= 75
          GROUP BY 1
        ),
        a AS (
          SELECT o.number
          FROM ocs o
          LEFT JOIN direct d ON d.document_id = o.document_id
          JOIN probable_ge75 p ON p.oc_document_id = o.document_id
          WHERE d.document_id IS NULL
        ),
        nc_docs AS (
          SELECT COUNT(*)::int AS n
          FROM distribuidora.documents nc
          WHERE nc.company_id = %s AND nc.document_type_id = 9
            AND nc.emission_date >= CURRENT_DATE - (%s || ' days')::interval
        ),
        nc_related AS (
          SELECT COUNT(*)::int AS n
          FROM distribuidora.documents nc
          JOIN distribuidora.document_details dd ON dd.document_id = nc.document_id
          JOIN distribuidora.document_related dr ON dr.detail_id = dd.detail_id
          WHERE nc.company_id = %s AND nc.document_type_id = 9
            AND nc.emission_date >= CURRENT_DATE - (%s || ' days')::interval
        ),
        plan_ocs AS (
          SELECT dpo.dispatch_plan_id, dpo.oc_number, dpo.oc_document_id,
                 dpo.created_at AS added_at, dp.created_at::date AS planned_day
          FROM distribuidora.dispatch_plan_orders dpo
          JOIN distribuidora.dispatch_plan dp ON dp.id = dpo.dispatch_plan_id
          WHERE dp.created_at >= CURRENT_DATE - (%s || ' days')::interval
        ),
        plan_related AS (
          SELECT p.*,
            st.inv_number,
            to_timestamp((st.raw_data->>'generationDate')::bigint) AS inv_gen,
            st.inv_day
          FROM plan_ocs p
          LEFT JOIN LATERAL (
            SELECT inv.number AS inv_number, inv.raw_data,
                   inv.emission_date::date AS inv_day
            FROM distribuidora.document_details dd
            JOIN distribuidora.document_related dr ON dr.detail_id = dd.detail_id
            JOIN distribuidora.documents inv
              ON inv.document_id = dr.related_document_id
             AND inv.document_type_id IN (1, 6)
            WHERE dd.document_id = p.oc_document_id
            ORDER BY COALESCE(
              to_timestamp((inv.raw_data->>'generationDate')::bigint),
              inv.emission_date
            ) ASC
            LIMIT 1
          ) st ON TRUE
        ),
        plan_probable AS (
          SELECT p.oc_document_id, p.oc_number, p.dispatch_plan_id, p.added_at,
                 pm.score, d.number AS cand_number,
                 to_timestamp((d.raw_data->>'generationDate')::bigint) AS cand_gen
          FROM plan_ocs p
          LEFT JOIN LATERAL (
            SELECT 1
            FROM distribuidora.document_details dd
            JOIN distribuidora.document_related dr ON dr.detail_id = dd.detail_id
            JOIN distribuidora.documents inv
              ON inv.document_id = dr.related_document_id
             AND inv.document_type_id IN (1, 6)
            WHERE dd.document_id = p.oc_document_id
            LIMIT 1
          ) rel ON TRUE
          JOIN distribuidora.document_probable_matches pm
            ON pm.oc_document_id = p.oc_document_id AND pm.score >= 60
          JOIN distribuidora.documents d
            ON d.document_id = pm.candidate_document_id AND d.document_type_id IN (1, 6)
          WHERE rel IS NULL
        ),
        indirect AS (
          SELECT COUNT(DISTINCT oc.document_id)::int AS n
          FROM ocs oc
          JOIN distribuidora.document_details dd ON dd.document_id = oc.document_id
          JOIN distribuidora.document_related dr ON dr.detail_id = dd.detail_id
          JOIN distribuidora.documents mid
            ON mid.document_id = dr.related_document_id
           AND mid.document_type_id NOT IN (1, 6, 9)
          JOIN distribuidora.document_details dd2 ON dd2.document_id = mid.document_id
          JOIN distribuidora.document_related dr2 ON dr2.detail_id = dd2.detail_id
          JOIN distribuidora.documents inv
            ON inv.document_id = dr2.related_document_id
           AND inv.document_type_id IN (1, 6)
          LEFT JOIN direct d ON d.document_id = oc.document_id
          WHERE d.document_id IS NULL
        )
        SELECT
          (SELECT COUNT(*) FROM ocs)::int AS ocs_45d,
          (SELECT COUNT(*) FROM direct)::int AS with_direct_inv,
          (SELECT COUNT(*) FROM ocs) - (SELECT COUNT(*) FROM direct) AS without_direct_inv,
          (SELECT COUNT(*) FROM a)::int AS A_pending_but_probable_ge75,
          (SELECT string_agg(number::text, ', ' ORDER BY number DESC)
             FROM (SELECT number FROM a ORDER BY number DESC LIMIT 8) x) AS A_folios,
          (SELECT n FROM nc_docs) AS nc_documents_45d,
          (SELECT n FROM nc_related) AS nc_with_outbound_related,
          (SELECT COUNT(*) FROM plan_ocs)::int AS plan_ocs_45d,
          (SELECT COUNT(*) FROM plan_related WHERE inv_number IS NULL)::int
            AS D_plan_without_related,
          (SELECT COUNT(*) FROM plan_related
             WHERE inv_number IS NOT NULL AND inv_gen < added_at)::int
            AS C_gen_before_added_at,
          (SELECT COUNT(*) FROM plan_related
             WHERE inv_number IS NOT NULL AND inv_day < planned_day)::int
            AS C_inv_day_before_plan_day,
          (SELECT COUNT(*) FROM plan_probable WHERE score >= 75)::int
            AS D_plan_no_related_probable_ge75,
          (SELECT COUNT(*) FROM plan_probable WHERE score < 75 OR score IS NULL)::int
            AS E_plan_probable_only_lt75,
          (SELECT COUNT(*) FROM plan_ocs p
             WHERE NOT EXISTS (
               SELECT 1 FROM distribuidora.document_details dd
               JOIN distribuidora.document_related dr ON dr.detail_id = dd.detail_id
               JOIN distribuidora.documents inv
                 ON inv.document_id = dr.related_document_id AND inv.document_type_id IN (1,6)
               WHERE dd.document_id = p.oc_document_id
             )
             AND NOT EXISTS (
               SELECT 1 FROM distribuidora.document_probable_matches pm
               WHERE pm.oc_document_id = p.oc_document_id AND pm.score >= 60
             )
          )::int AS D_plan_missing_no_probable,
          (SELECT n FROM indirect) AS F_indirect_only,
          (SELECT string_agg(oc_number::text, ', ' ORDER BY oc_number DESC)
             FROM (
               SELECT oc_number FROM plan_ocs p
               WHERE NOT EXISTS (
                 SELECT 1 FROM distribuidora.document_details dd
                 JOIN distribuidora.document_related dr ON dr.detail_id = dd.detail_id
                 JOIN distribuidora.documents inv
                   ON inv.document_id = dr.related_document_id AND inv.document_type_id IN (1,6)
                 WHERE dd.document_id = p.oc_document_id
               )
               ORDER BY oc_number DESC LIMIT 12
             ) z
          ) AS D_folios
        """,
        (
            COMPANY_ID,
            OFFICE_ID,
            DAYS,
            COMPANY_ID,
            DAYS,
            COMPANY_ID,
            DAYS,
            DAYS,
        ),
    )
    cols = [d[0] for d in cur.description]
    row = dict(zip(cols, cur.fetchall()[0]))

    cur.execute(
        """
        SELECT COUNT(*)::int AS returns_rows
        FROM bsale.returns WHERE company_id = %s
        """,
        (COMPANY_ID,),
    )
    row["returns_rows_company"] = cur.fetchone()[0]

    return {
        "scope": {
            "company_id": COMPANY_ID,
            "office_id": OFFICE_ID,
            "days": DAYS,
            "read_only": True,
            "mass_repair": False,
        },
        "notes": [
            "A: UI/SQL 'Pendiente' usa solo related directo a tipo 1/6; "
            "probable≥75 no cuenta en pre-despacho admission.",
            "B: NC tipo 9 existen pero document_related y bsale.returns "
            "no vinculan NC↔factura en PG (gap de sync).",
            "C: usar generationDate, no emissionDate (medianoche).",
            "Código no reabre OC a Pendiente por NC; el síntoma operativo "
            "principal es related ausente / stale.",
        ],
        "counts": _jsonable(row),
    }


def main() -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        report = run_audit(cur)
        cur.close()
    finally:
        conn.close()
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
