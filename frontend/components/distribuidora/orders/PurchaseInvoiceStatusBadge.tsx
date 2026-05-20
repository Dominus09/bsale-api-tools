"use client"

import { Badge } from "@/components/ui/badge"
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip"
export type PurchaseInvoiceStatusFields = {
  is_invoiced?: boolean | null
  purchase_status?: string | null
  estado_real?: string | null
  probable_score?: number | null
  probable_tier?: string | null
}

const PROBABLE_TOOLTIP =
  "Detectada por coincidencia operacional Bsale (sin linkage API confirmado)"

function resolveStatus(row: PurchaseInvoiceStatusFields): {
  kind: "confirmed" | "probable" | "pending"
  label: string
} {
  const purchaseStatus =
    typeof row.purchase_status === "string" ? row.purchase_status.trim() : ""
  if (purchaseStatus === "FACTURADA_CONFIRMADA") {
    return { kind: "confirmed", label: "Facturada" }
  }
  if (purchaseStatus === "PROBABLE_FACTURADA") {
    return { kind: "probable", label: "Probable facturada" }
  }

  const estado =
    typeof row.estado_real === "string" ? row.estado_real.trim() : ""
  if (estado === "Facturada" || row.is_invoiced === true) {
    return { kind: "confirmed", label: "Facturada" }
  }
  if (estado === "Probable facturada") {
    return { kind: "probable", label: "Probable facturada" }
  }
  return { kind: "pending", label: "Pendiente" }
}

export function PurchaseInvoiceStatusBadge({
  row,
}: {
  row: PurchaseInvoiceStatusFields
}) {
  const { kind, label } = resolveStatus(row)

  if (kind === "confirmed") {
    return (
      <Badge className="bg-emerald-600 hover:bg-emerald-600">{label}</Badge>
    )
  }

  if (kind === "probable") {
    const tier =
      typeof row.probable_tier === "string" ? row.probable_tier.trim() : ""
    const score =
      row.probable_score != null && Number.isFinite(Number(row.probable_score))
        ? Math.round(Number(row.probable_score))
        : null
    const hint =
      score != null
        ? `${PROBABLE_TOOLTIP}${tier ? ` · ${tier}` : ""} · score ${score}`
        : PROBABLE_TOOLTIP

    return (
      <Tooltip>
        <TooltipTrigger asChild>
          <span className="inline-flex cursor-help">
            <Badge className="bg-amber-500 text-amber-950 hover:bg-amber-500">
              {label}
            </Badge>
          </span>
        </TooltipTrigger>
        <TooltipContent side="top" className="max-w-xs">
          {hint}
        </TooltipContent>
      </Tooltip>
    )
  }

  return <Badge variant="secondary">{label}</Badge>
}
