"use client"

import { useEffect, useState, type ReactNode } from "react"
import Link from "next/link"

import { CostHistoryChart } from "@/components/costos/cost-history-chart"
import {
  CostAgeBadge,
  CostQualityBadge,
  CostTaxBadge,
} from "@/components/costos/cost-quality-badge"
import { Button } from "@/components/ui/button"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Separator } from "@/components/ui/separator"
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet"
import { Skeleton } from "@/components/ui/skeleton"
import {
  getCostVariantHistory,
  type CostVariantHistory,
} from "@/lib/api"
import {
  buildEvolutionSeries,
  type CostTableRow,
} from "@/lib/costos/adapt-cost-analytics"
import { formatDateTime, formatMoneyCLP, formatPct } from "@/lib/costos/format"
import { COST_ORIGIN_LABEL } from "@/lib/costos/quality-labels"

function Field({
  label,
  value,
}: {
  label: string
  value: ReactNode
}) {
  return (
    <div className="grid grid-cols-[7.5rem_1fr] gap-2 text-sm sm:grid-cols-[9rem_1fr]">
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="min-w-0 break-words font-medium tabular-nums">{value}</dd>
    </div>
  )
}

export function CostDetailDrawer({
  open,
  onOpenChange,
  row,
  companyId,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  row: CostTableRow | null
  companyId: number
}) {
  const [history, setHistory] = useState<CostVariantHistory | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!open || !row || !companyId) return
    let cancelled = false
    setLoading(true)
    setError(null)
    setHistory(null)
    ;(async () => {
      try {
        const data = await getCostVariantHistory(row.variantId, companyId)
        if (!cancelled) setHistory(data)
      } catch (e) {
        if (!cancelled) {
          setError(e instanceof Error ? e.message : "No se pudo cargar el historial")
        }
      } finally {
        if (!cancelled) setLoading(false)
      }
    })()
    return () => {
      cancelled = true
    }
  }, [open, row, companyId])

  const series = buildEvolutionSeries(history)

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent
        side="right"
        className="flex w-full flex-col gap-0 p-0 sm:max-w-lg"
      >
        <SheetHeader className="border-b border-border/60 px-4 py-4 text-left">
          <SheetTitle className="pr-8 text-base leading-snug">
            {row?.productName || "Producto"}
          </SheetTitle>
          <SheetDescription className="text-xs">
            {row?.variantName || `Variante ${row?.variantId ?? ""}`}
            {row?.barcode ? ` · ${row.barcode}` : ""}
          </SheetDescription>
        </SheetHeader>

        <ScrollArea className="flex-1 px-4 py-4">
          {!row ? null : (
            <div className="space-y-5">
              <div className="flex flex-wrap gap-1.5">
                <CostQualityBadge kind={row.grossCostQuality} />
                <CostTaxBadge kind={row.taxBreakdownQuality} />
                <CostAgeBadge kind={row.ageBucket} />
              </div>

              <dl className="space-y-2.5">
                <Field label="Costo neto" value={formatMoneyCLP(row.costNet)} />
                <Field label="IVA" value={formatMoneyCLP(row.ivaAmount)} />
                <Field
                  label="ILA / otros imp."
                  value={formatMoneyCLP(row.otherTaxes)}
                />
                <Field label="Costo bruto" value={formatMoneyCLP(row.costGross)} />
                <Field
                  label="Bruto anterior"
                  value={formatMoneyCLP(row.previousCostGross)}
                />
                <Field
                  label="Bruto máx. válido"
                  value={formatMoneyCLP(row.maxValidGrossCost)}
                />
                <Field
                  label="Bruto mínimo"
                  value={formatMoneyCLP(row.minValidGrossCost)}
                />
                <Field label="Fecha" value={formatDateTime(row.lastReceptionDate)} />
                <Field
                  label="Documento"
                  value={row.documentLabel || "Sin información"}
                />
                <Field
                  label="Proveedor"
                  value={row.supplierName || "Sin información"}
                />
                <Field
                  label="Cantidad"
                  value={
                    row.quantity != null ? String(row.quantity) : "Sin información"
                  }
                />
                <Field label="Costo promedio" value={formatMoneyCLP(row.averageCost)} />
                <Field
                  label="Origen"
                  value={COST_ORIGIN_LABEL[row.origin]}
                />
                <Field label="Variación" value={formatPct(row.variationPct)} />
                <Field
                  label="Oficina"
                  value={row.officeName || "Sin información"}
                />
              </dl>

              <Separator />

              <div>
                <h3 className="mb-2 text-sm font-medium">Evolución del costo bruto</h3>
                {loading ? (
                  <Skeleton className="h-[220px] w-full" />
                ) : error ? (
                  <p className="text-sm text-destructive">{error}</p>
                ) : (
                  <CostHistoryChart data={series} />
                )}
              </div>

              <div>
                <h3 className="mb-2 text-sm font-medium">Historial reciente</h3>
                {loading ? (
                  <div className="space-y-2">
                    <Skeleton className="h-8 w-full" />
                    <Skeleton className="h-8 w-full" />
                  </div>
                ) : history?.items?.length ? (
                  <ul className="space-y-2 text-xs">
                    {history.items.slice(0, 12).map((h) => (
                      <li
                        key={`${h.reception_detail_id}-${h.admission_date}`}
                        className="flex items-center justify-between gap-2 rounded-md border border-border/50 px-2 py-1.5"
                      >
                        <span className="text-muted-foreground">
                          {formatDateTime(h.admission_date)}
                        </span>
                        <span className="tabular-nums font-medium">
                          {formatMoneyCLP(h.cost_bruto_erp ?? h.cost_net)}
                        </span>
                      </li>
                    ))}
                  </ul>
                ) : (
                  <p className="text-sm text-muted-foreground">Sin historial.</p>
                )}
              </div>

              <Button asChild variant="outline" size="sm" className="w-full">
                <Link href={`/costos/productos/${row.variantId}?company_id=${companyId}`}>
                  Ver ficha completa
                </Link>
              </Button>
            </div>
          )}
        </ScrollArea>
      </SheetContent>
    </Sheet>
  )
}
