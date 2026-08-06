"""Diagnóstico read-only: OCs con día/estado potencialmente desactualizados.

Uso::

    python -m backend.jobs.diagnose_oc_header_drift --recent-days 45

No escribe en base.
"""

from __future__ import annotations

import argparse
import json
import sys

from backend.db import get_connection
from backend.utils.bsale_token_env import load_dotenv_if_available

DIAGNOSTIC_SQL = """
WITH oc AS (
    SELECT
        d.document_id,
        d.number AS order_number,
        d.client_id,
        d.total_amount,
        d.updated_at AS header_updated_at,
        (
            SELECT MAX(dd.created_at)
            FROM distribuidora.document_details dd
            WHERE dd.document_id = d.document_id
        ) AS details_updated_at,
        (
            SELECT MAX(da.created_at)
            FROM distribuidora.document_attributes da
            WHERE da.document_id = d.document_id
        ) AS attrs_updated_at,
        (
            SELECT da.attribute_value
            FROM distribuidora.document_attributes da
            WHERE da.document_id = d.document_id
              AND UPPER(BTRIM(da.attribute_name)) = 'OBSERVACIONES'
            ORDER BY da.created_at DESC NULLS LAST
            LIMIT 1
        ) AS observaciones,
        EXISTS (
            SELECT 1
            FROM distribuidora.document_details dd
            INNER JOIN distribuidora.document_related dr ON dr.detail_id = dd.detail_id
            INNER JOIN distribuidora.documents inv
                ON inv.document_id = dr.related_document_id
               AND inv.document_type_id IN (1, 6)
               AND COALESCE(inv.state, 0) = 0
            WHERE dd.document_id = d.document_id
        ) AS has_invoice_related,
        EXISTS (
            SELECT 1
            FROM distribuidora.document_probable_matches pm
            WHERE pm.oc_document_id = d.document_id AND pm.score >= 60
        ) AS has_probable,
        EXISTS (
            SELECT 1
            FROM distribuidora.documents inv
            WHERE inv.client_id = d.client_id
              AND inv.document_type_id IN (1, 6)
              AND inv.total_amount = d.total_amount
              AND COALESCE(inv.state, 0) = 0
              AND inv.emission_date >= d.emission_date
              AND inv.emission_date < d.emission_date + INTERVAL '14 days'
        ) AS has_same_client_amount_invoice
    FROM distribuidora.documents d
    WHERE d.company_id = %s
      AND d.office_id = %s
      AND d.document_type_id = 33
      AND COALESCE(d.state, 0) = 0
      AND d.emission_date >= NOW() - (%s * INTERVAL '1 day')
)
SELECT
    COUNT(*) FILTER (
        WHERE details_updated_at IS NOT NULL
          AND attrs_updated_at IS NOT NULL
          AND details_updated_at > attrs_updated_at + INTERVAL '1 minute'
    )::int AS day_stale_vs_details,
    COUNT(*) FILTER (
        WHERE has_same_client_amount_invoice
          AND NOT has_invoice_related
          AND NOT has_probable
    )::int AS status_likely_stale_no_link,
    COUNT(*) FILTER (
        WHERE has_same_client_amount_invoice AND NOT has_invoice_related
    )::int AS invoice_not_linked,
    COUNT(*) FILTER (
        WHERE details_updated_at IS NOT NULL
          AND attrs_updated_at IS NOT NULL
          AND details_updated_at > attrs_updated_at + INTERVAL '1 minute'
          AND has_same_client_amount_invoice
          AND NOT has_invoice_related
    )::int AS both_inconsistencies
FROM oc
"""


def main(argv: list[str] | None = None) -> int:
    load_dotenv_if_available()
    p = argparse.ArgumentParser(description="Diagnóstico drift día/estado OC (read-only)")
    p.add_argument("--recent-days", type=int, default=45)
    p.add_argument("--company-id", type=int, default=3)
    p.add_argument("--office-id", type=int, default=1)
    args = p.parse_args(argv)

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            DIAGNOSTIC_SQL,
            (int(args.company_id), int(args.office_id), int(args.recent_days)),
        )
        row = cur.fetchone()
        cols = [d[0] for d in cur.description]
        cur.close()
        payload = dict(zip(cols, row)) if row else {}
    finally:
        conn.close()

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
