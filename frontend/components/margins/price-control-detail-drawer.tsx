"use client"

import Link from "next/link"
import { ExternalLink } from "lucide-react"

import { PriceControlStatusBadge } from "@/components/margins/price-control-status-badge"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Separator } from "@/components/ui/separator"
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet"
import { formatDateShort, formatMoneyCLP } from "@/lib/costos/format"
import { GROSS_COST_QUALITY_LABEL, type GrossCostQualityKind } from "@/lib/costos/quality-labels"
import type { PriceControlRow } from "@/lib/margins/adapt-price-control"
import { statusExplanation } from "@/lib/margins/price-policy"

function qualityLabel(q: string | null): string {
  if (!q) return "Sin información"
  if (q in GROSS_COST_QUALITY_LABEL) {
    return GROSS_COST_QUALITY_LABEL[q as GrossCostQualityKind]
  }
  if (q === "current_tax_profile_fallback") return "Costo actual (fallback)"
  return q
}

function fmtPctPlain(v: number | null): string {
  if (v == null || !Number.isFinite(v)) return "—"
  return `${v.toFixed(2)}%`
}

export function PriceControlDetailDrawer({
  open,
  onOpenChange,
  row,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  row: PriceControlRow | null
}) {
  if (!row) return null

  const costosHref =
    row.variantId > 0
      ? `/costos/productos/${row.variantId}?company_id=${row.companyId}`
      : `/costos?company_id=${row.companyId}${
          row.barcode ? `&search=${encodeURIComponent(row.barcode)}` : ""
        }`

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="w-full overflow-y-auto sm:max-w-md">
        <SheetHeader>
          <SheetTitle className="pr-6 text-left leading-snug">
            {row.productName || "Producto"}
            {row.variantName ? ` — ${row.variantName}` : ""}
          </SheetTitle>
          <SheetDescription className="text-left">
            {row.priceListName || `Lista ${row.priceListId}`}
            {row.barcode ? ` · ${row.barcode}` : ""}
          </SheetDescription>
        </SheetHeader>

        <div className="mt-4 space-y-4 text-sm">
          <div className="flex flex-wrap items-center gap-2">
            <PriceControlStatusBadge status={row.status} />
            {row.policyCompliance && row.policyCompliance !== row.status ? (
              <Badge variant="outline" className="text-xs font-normal">
                Si se evalúa: {row.policyCompliance}
              </Badge>
            ) : null}
          </div>
          <p className="text-muted-foreground">{statusExplanation(row.status)}</p>

          <Separator />

          <dl className="grid grid-cols-2 gap-x-3 gap-y-3">
            <div>
              <dt className="text-xs text-muted-foreground">Precio bruto actual</dt>
              <dd className="font-medium tabular-nums">{formatMoneyCLP(row.grossPrice)}</dd>
            </div>
            <div>
              <dt className="text-xs text-muted-foreground">Costo bruto de referencia</dt>
              <dd className="font-medium tabular-nums">
                {formatMoneyCLP(row.referenceGrossCost)}
              </dd>
            </div>
            <div>
              <dt className="text-xs text-muted-foreground">Fecha del costo</dt>
              <dd>{formatDateShort(row.costDate)}</dd>
            </div>
            <div>
              <dt className="text-xs text-muted-foreground">Fuente</dt>
              <dd className="truncate">{row.costSource || "—"}</dd>
            </div>
            <div>
              <dt className="text-xs text-muted-foreground">Recargo real</dt>
              <dd className="font-medium tabular-nums">{fmtPctPlain(row.actualMarkupPct)}</dd>
            </div>
            <div>
              <dt className="text-xs text-muted-foreground">Margen sobre precio</dt>
              <dd className="tabular-nums text-muted-foreground">
                {fmtPctPlain(row.grossMarginPct)}
              </dd>
            </div>
            <div>
              <dt className="text-xs text-muted-foreground">Recargo mínimo objetivo</dt>
              <dd className="tabular-nums">{fmtPctPlain(row.minMarkupPct)}</dd>
            </div>
            <div>
              <dt className="text-xs text-muted-foreground">Recargo máximo objetivo</dt>
              <dd className="tabular-nums">{fmtPctPlain(row.maxMarkupPct)}</dd>
            </div>
            <div>
              <dt className="text-xs text-muted-foreground">Precio mín. recomendado</dt>
              <dd className="tabular-nums">
                {formatMoneyCLP(row.minimumRecommendedGrossPrice)}
              </dd>
            </div>
            <div>
              <dt className="text-xs text-muted-foreground">Precio máx. recomendado</dt>
              <dd className="tabular-nums">
                {formatMoneyCLP(row.maximumRecommendedGrossPrice)}
              </dd>
            </div>
            <div>
              <dt className="text-xs text-muted-foreground">Ajuste al mínimo</dt>
              <dd className="tabular-nums">{formatMoneyCLP(row.priceAdjustmentToMinimum)}</dd>
            </div>
            <div>
              <dt className="text-xs text-muted-foreground">Stock (informativo)</dt>
              <dd className="tabular-nums">
                {row.stockQuantity == null ? "—" : row.stockQuantity}
              </dd>
            </div>
            <div className="col-span-2">
              <dt className="text-xs text-muted-foreground">Calidad del costo</dt>
              <dd>{qualityLabel(row.grossCostQuality)}</dd>
            </div>
            {row.resolutionReason ? (
              <div className="col-span-2">
                <dt className="text-xs text-muted-foreground">Resolución</dt>
                <dd className="text-xs text-muted-foreground">{row.resolutionReason}</dd>
              </div>
            ) : null}
          </dl>

          <Button asChild className="w-full" variant="outline">
            <Link href={costosHref}>
              Ver historial en Costos
              <ExternalLink className="ml-1.5 h-3.5 w-3.5" />
            </Link>
          </Button>
        </div>
      </SheetContent>
    </Sheet>
  )
}
