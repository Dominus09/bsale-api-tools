"""Margen comercial real desde documentos facturados + costo variante Bsale (variant_cost)."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

# Auditoría Bsale (muestras exports/debug_document_types + API documents/details):
# - documents.json: totalAmount, netAmount, taxAmount — NO trae totalCost/margin/netMargin.
# - details.json: netAmount, totalAmount, quantity, variant — NO trae cost en línea.
# - Margen operativo debe calcularse: SUM(venta línea) - SUM(costo variante × qty)
#   con bsale.variant_cost.average_cost_net × products.tax_factor (misma convención que
#   bsale.margin_analysis_view). Si falta costo en alguna línea → no inventar margen.


@dataclass
class CommercialMarginResult:
    commercial_margin_clp: int | None
    invoiced_revenue_clp: int
    invoiced_cost_clp: int | None
    lines_total: int
    lines_with_cost: int
    source: str
    documents_count: int
    partial: bool
    message: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "commercial_margin_clp": self.commercial_margin_clp,
            "invoiced_revenue_clp": self.invoiced_revenue_clp,
            "invoiced_cost_clp": self.invoiced_cost_clp,
            "lines_total": self.lines_total,
            "lines_with_cost": self.lines_with_cost,
            "source": self.source,
            "documents_count": self.documents_count,
            "partial": self.partial,
            "message": self.message,
        }


def compute_plan_commercial_margin(cur, plan_id: int) -> CommercialMarginResult:
    """
    Margen comercial = ventas confirmadas (boleta/factura relateddetailid)
    menos costo de líneas según variant_cost en catálogo Bsale.
    """
    cur.execute(
        """
        WITH confirmed_docs AS (
            SELECT DISTINCT inv.related_document_id AS document_id
            FROM distribuidora.v_dispatch_plan_invoiced_documents inv
            WHERE inv.dispatch_plan_id = %s
              AND inv.status = 'confirmed'
              AND inv.related_document_id IS NOT NULL
        ),
        line_costs AS (
            SELECT
                cd.document_id,
                d.total_amount AS doc_revenue,
                dd.detail_id,
                dd.quantity,
                dd.total_amount AS line_revenue,
                vc.average_cost_net,
                COALESCE(NULLIF(p.tax_factor, 0), 1)::numeric AS tax_factor
            FROM confirmed_docs cd
            INNER JOIN distribuidora.v_documents_latest d
                ON d.document_id = cd.document_id
            INNER JOIN distribuidora.document_details dd
                ON dd.document_id = cd.document_id
            LEFT JOIN bsale.variants v
                ON v.company_id = 3 AND v.bsale_id = dd.variant_id
            LEFT JOIN bsale.products p
                ON p.company_id = v.company_id AND p.bsale_id = v.product_id
            LEFT JOIN bsale.variant_cost vc
                ON vc.company_id = 3 AND vc.variant_id = dd.variant_id
        ),
        per_doc AS (
            SELECT
                document_id,
                MAX(doc_revenue) AS doc_revenue,
                COUNT(*)::int AS lines_total,
                COUNT(average_cost_net) FILTER (WHERE average_cost_net IS NOT NULL)::int AS lines_with_cost,
                COALESCE(SUM(line_revenue), 0) AS sum_line_revenue,
                COALESCE(SUM(
                    CASE
                        WHEN average_cost_net IS NOT NULL AND quantity IS NOT NULL
                        THEN ROUND(
                            (average_cost_net * tax_factor * quantity)::numeric,
                            0
                        )
                        ELSE NULL
                    END
                ), 0) AS sum_line_cost
            FROM line_costs
            GROUP BY document_id
        )
        SELECT
            COALESCE(COUNT(DISTINCT document_id), 0)::int AS documents_count,
            COALESCE(SUM(lines_total), 0)::int AS lines_total,
            COALESCE(SUM(lines_with_cost), 0)::int AS lines_with_cost,
            COALESCE(SUM(COALESCE(doc_revenue, sum_line_revenue)), 0) AS revenue,
            COALESCE(SUM(sum_line_cost), 0) AS cost_sum
        FROM per_doc
        """,
        (plan_id,),
    )
    row = cur.fetchone()
    if not row:
        return CommercialMarginResult(
            commercial_margin_clp=None,
            invoiced_revenue_clp=0,
            invoiced_cost_clp=None,
            lines_total=0,
            lines_with_cost=0,
            source="unavailable",
            documents_count=0,
            partial=True,
            message="Sin documentos confirmados para calcular margen.",
        )

    docs_count = int(row[0] or 0)
    lines_total = int(row[1] or 0)
    lines_with_cost = int(row[2] or 0)
    revenue = int(round(float(row[3] or 0)))
    cost_sum = int(round(float(row[4] or 0)))

    if docs_count == 0:
        return CommercialMarginResult(
            commercial_margin_clp=None,
            invoiced_revenue_clp=0,
            invoiced_cost_clp=None,
            lines_total=0,
            lines_with_cost=0,
            source="unavailable",
            documents_count=0,
            partial=True,
            message="Sin documentos confirmados (document_related).",
        )

    if lines_total == 0:
        return CommercialMarginResult(
            commercial_margin_clp=None,
            invoiced_revenue_clp=revenue,
            invoiced_cost_clp=None,
            lines_total=0,
            lines_with_cost=0,
            source="unavailable",
            documents_count=docs_count,
            partial=True,
            message="Documentos sin líneas en document_details.",
        )

    if lines_with_cost < lines_total:
        return CommercialMarginResult(
            commercial_margin_clp=None,
            invoiced_revenue_clp=revenue,
            invoiced_cost_clp=None,
            lines_total=lines_total,
            lines_with_cost=lines_with_cost,
            source="variant_cost_partial",
            documents_count=docs_count,
            partial=True,
            message=(
                f"Costo de variante incompleto ({lines_with_cost}/{lines_total} líneas). "
                "No se muestra margen hasta tener costos Bsale."
            ),
        )

    margin = revenue - cost_sum
    return CommercialMarginResult(
        commercial_margin_clp=margin,
        invoiced_revenue_clp=revenue,
        invoiced_cost_clp=cost_sum,
        lines_total=lines_total,
        lines_with_cost=lines_with_cost,
        source="variant_cost",
        documents_count=docs_count,
        partial=False,
        message="Margen desde venta documento y costo promedio variante (Bsale variant_cost).",
    )


def audit_bsale_margin_fields(cur) -> dict[str, Any]:
    """Muestra si raw_data de documentos trae claves de margen/costo (auditoría en BD)."""
    cur.execute(
        """
        SELECT
            COUNT(*)::int AS n,
            COUNT(*) FILTER (
                WHERE raw_data ? 'totalCost'
                   OR raw_data ? 'netCost'
                   OR raw_data ? 'margin'
                   OR raw_data ? 'commercialMargin'
                   OR raw_data ? 'netMargin'
            )::int AS with_margin_key
        FROM distribuidora.documents
        WHERE company_id = 3
          AND document_type_id IN (1, 6)
          AND emission_date >= NOW() - INTERVAL '90 days'
        """
    )
    n, with_key = cur.fetchone()
    cur.execute(
        """
        SELECT COUNT(*)::int AS lines,
               COUNT(*) FILTER (
                   WHERE dd.raw_data ? 'cost'
                      OR dd.raw_data ? 'totalCost'
                      OR dd.raw_data ? 'averageCost'
               )::int AS with_cost_key
        FROM distribuidora.document_details dd
        INNER JOIN distribuidora.documents d ON d.document_id = dd.document_id
        WHERE d.company_id = 3
          AND d.document_type_id IN (1, 6)
          AND d.emission_date >= NOW() - INTERVAL '90 days'
        """
    )
    ln, lc = cur.fetchone()
    return {
        "documents_sampled_note": "Últimos 90d tipos 1/6",
        "documents_count": int(n or 0),
        "documents_with_margin_like_keys": int(with_key or 0),
        "detail_lines_checked": int(ln or 0),
        "detail_lines_with_cost_like_keys": int(lc or 0),
        "recommended_source": "variant_cost_catalog",
        "conclusion": (
            "Bsale API (muestras repo) no entrega margen en documento; "
            "usar variant_cost × tax_factor por línea o dejar margen NULL."
        ),
    }
