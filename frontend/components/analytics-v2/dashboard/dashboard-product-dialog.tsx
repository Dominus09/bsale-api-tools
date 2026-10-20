"use client"

import { useEffect, useState } from "react"
import Link from "next/link"
import { Loader2 } from "lucide-react"

import { ReliabilityBadge } from "@/components/analytics-v2/costs/badges"
import { Button } from "@/components/ui/button"
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { cn } from "@/lib/utils"
import { getMarginProduct } from "@/lib/analytics-v2/api"
import { GROSS_STATUS_LABELS, TAX_LABELS, formatDateTime, formatMoney, formatPct } from "@/lib/analytics-v2/labels"
import {
  COST_BASIS_LABELS,
  MARGIN_RISK_LABELS,
  MARGIN_STATUS_LABELS,
  MARGIN_STATUS_TONES,
  PROMOTION_CHECK_LABELS,
} from "@/lib/analytics-v2/margins-labels"
import type { MarginRow } from "@/lib/analytics-v2/margins-types"
import { withQuery } from "@/lib/analytics-v2/url"

import { COSTS_PATH, DataClassBadge, MARGINS_PATH } from "./dashboard-sections"
import type { DataClass } from "@/lib/analytics-v2/dashboard-types"

const chip = "inline-flex items-center whitespace-nowrap rounded-md border px-1.5 py-0.5 text-[11px] font-medium"
const UNAVAILABLE = new Set(["sin_precio", "sin_costo", "sin_regla", "costo_no_verificado", "requiere_revision_tributaria"])

/** Misma regla que el backend (``margins_service.data_class``), aplicada a la fila completa. */
function rowDataClass(r: MarginRow): DataClass {
  if (UNAVAILABLE.has(r.status)) return "no_disponible"
  if (r.confirmation === "confirmado" && r.validity.status === "validado") return "confirmado"
  return "provisional"
}

export function DashboardProductDialog({
  companyId,
  variantId,
  costBasis,
  canSeeCosts,
  onOpenChange,
  onOpenCostHistory,
}: {
  companyId: number
  variantId: number | null
  costBasis: string
  canSeeCosts: boolean
  onOpenChange: (open: boolean) => void
  onOpenCostHistory: (variantId: number) => void
}) {
  const [rows, setRows] = useState<MarginRow[] | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (variantId == null) return
    const ac = new AbortController()
    setRows(null)
    setError(null)
    setLoading(true)
    getMarginProduct(companyId, variantId, costBasis, ac.signal)
      .then((r) => setRows(r.rows))
      .catch((e: unknown) => {
        if (!ac.signal.aborted) setError(e instanceof Error ? e.message : "Error al cargar el producto")
      })
      .finally(() => {
        if (!ac.signal.aborted) setLoading(false)
      })
    return () => ac.abort()
  }, [companyId, variantId, costBasis])

  const first = rows?.[0]
  return (
    <Dialog open={variantId != null} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[90vh] overflow-y-auto sm:max-w-4xl">
        <DialogHeader>
          <DialogTitle>{first?.product_name ?? (variantId != null ? `SKU ${variantId}` : "")}</DialogTitle>
          <DialogDescription>
            {first ? (
              <>
                {first.code ?? "—"}
                {first.variant_name ? ` · ${first.variant_name}` : ""}
                {first.product_type.name ? ` · ${first.product_type.name}` : ""}
              </>
            ) : (
              "Márgenes del SKU en todas las listas evaluadas"
            )}
          </DialogDescription>
        </DialogHeader>

        {loading ? (
          <p className="flex items-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" /> Cargando…
          </p>
        ) : null}
        {error ? <p className="text-sm text-red-700 dark:text-red-300">{error}</p> : null}

        {first ? (
          <div className="space-y-1 text-sm">
            <div className="flex flex-wrap items-center gap-2">
              <span className="text-muted-foreground">Costo bruto comercial:</span>
              <span className="font-medium tabular-nums">{formatMoney(first.costs.reference.gross)}</span>
              <ReliabilityBadge status={first.costs.reliability} />
              <span className="text-[11px] text-muted-foreground">
                {COST_BASIS_LABELS[first.costs.reference.basis]} neto {formatMoney(first.costs.reference.net)} · IVA{" "}
                {first.costs.reference.iva_rate_pct ?? "—"}% · ILA {first.costs.reference.ila_rate_pct ?? "—"}% · factor{" "}
                {first.costs.reference.tax_factor ?? "—"}
              </span>
            </div>
            <p className="text-[11px] text-muted-foreground">
              {first.costs.reference.tax_status === "por_componente"
                ? "Impuestos por componente del pack"
                : TAX_LABELS[first.costs.reference.tax_status]}{" "}
              · {GROSS_STATUS_LABELS[first.costs.reference.gross_status]} ·
              neto última compra {formatMoney(first.costs.last_reception_net)} · promedio Bsale{" "}
              {formatMoney(first.costs.bsale_average_net)}
            </p>
          </div>
        ) : null}

        {rows?.length ? (
          <div className="overflow-x-auto rounded-md border">
            <table className="w-full min-w-[720px] text-sm">
              <thead className="bg-muted/40 text-left text-[11px] uppercase tracking-wide text-muted-foreground">
                <tr>
                  <th className="px-3 py-1.5 font-medium">Lista</th>
                  <th className="px-3 py-1.5 text-right font-medium">Precio bruto</th>
                  <th className="px-3 py-1.5 text-right font-medium">Recargo</th>
                  <th className="px-3 py-1.5 text-right font-medium">Mín. / máx.</th>
                  <th className="px-3 py-1.5 font-medium">Estado</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border/60">
                {rows.map((r) => (
                  <tr key={r.price_list.id} className="align-top">
                    <td className="px-3 py-1.5">
                      <span className="font-medium">{r.price_list.label ?? r.price_list.name}</span>
                      <span className="block text-[11px] text-muted-foreground">
                        Precio sincronizado {formatDateTime(r.price.fetched_at)}
                      </span>
                    </td>
                    <td className="px-3 py-1.5 text-right tabular-nums">{formatMoney(r.price.gross)}</td>
                    <td className="px-3 py-1.5 text-right tabular-nums">{formatPct(r.markup_pct)}</td>
                    <td className="px-3 py-1.5 text-right tabular-nums text-xs">
                      {r.rule ? `${formatPct(r.rule.min_markup_pct)} / ${formatPct(r.rule.max_markup_pct)}` : "Sin regla"}
                    </td>
                    <td className="px-3 py-1.5">
                      <div className="flex flex-wrap gap-1">
                        <span className={cn(chip, MARGIN_STATUS_TONES[r.status])}>{MARGIN_STATUS_LABELS[r.status]}</span>
                        <DataClassBadge value={rowDataClass(r)} />
                        {r.risks.map((k) => (
                          <span key={k} className={cn(chip, "border-red-300 bg-red-50 text-red-800 dark:text-red-200 dark:border-red-400/25 dark:bg-red-400/10 dark:text-red-200")}>
                            {MARGIN_RISK_LABELS[k]}
                          </span>
                        ))}
                      </div>
                      {r.promotion.check !== "sin_excepcion" ? (
                        <p className="mt-0.5 text-[11px] text-muted-foreground">{PROMOTION_CHECK_LABELS[r.promotion.check]}</p>
                      ) : null}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : null}

        {variantId != null ? (
          <div className="flex flex-wrap gap-2">
            {canSeeCosts ? (
              <Button size="sm" variant="outline" onClick={() => onOpenCostHistory(variantId)}>
                Historial de costos
              </Button>
            ) : null}
            <Button size="sm" variant="outline" asChild>
              <Link href={withQuery(MARGINS_PATH, { q: first?.code ?? undefined, cost_basis: costBasis })}>Ver en Márgenes V2</Link>
            </Button>
            {canSeeCosts ? (
              <Button size="sm" variant="outline" asChild>
                <Link href={withQuery(COSTS_PATH, { variant_id: variantId })}>Abrir en Costos V2</Link>
              </Button>
            ) : null}
          </div>
        ) : null}
      </DialogContent>
    </Dialog>
  )
}
