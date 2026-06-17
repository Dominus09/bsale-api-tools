"use client"

import { Loader2 } from "lucide-react"
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip"
import type { PromotionGridRow } from "@/lib/api"
import {
  calcDiscountPercent,
  formatCurrency,
  formatDateShort,
  groupPromotionsByStartDate,
  productDisplayName,
  calendarRowClass,
} from "@/lib/promotions-utils"
import { PromotionStatusBadge, PromotionTipoBadge } from "@/components/promotions/promotion-badges"

type PromotionCalendarViewProps = {
  rows: PromotionGridRow[]
  loading: boolean
  companyNameById: Map<number, string>
  onOpen: (row: PromotionGridRow) => void
}

export function PromotionCalendarView({
  rows,
  loading,
  companyNameById,
  onOpen,
}: PromotionCalendarViewProps) {
  if (loading) {
    return (
      <div className="flex items-center justify-center gap-2 py-24 text-muted-foreground">
        <Loader2 className="h-6 w-6 animate-spin" />
        <span>Cargando calendario…</span>
      </div>
    )
  }

  const months = groupPromotionsByStartDate(rows)

  if (months.length === 0) {
    return (
      <div className="rounded-xl border border-dashed py-20 text-center">
        <p className="text-muted-foreground text-sm">Sin promociones para mostrar en el calendario.</p>
      </div>
    )
  }

  return (
    <TooltipProvider delayDuration={200}>
      <div className="space-y-8">
        {months.map((month) => (
          <section key={month.monthKey}>
            <h2 className="mb-4 text-lg font-semibold capitalize">{month.monthLabel}</h2>
            <div className="space-y-6">
              {month.days.map((day) => (
                <div key={day.dateKey} className="rounded-xl border bg-card p-4 shadow-sm">
                  <h3 className="text-muted-foreground mb-3 text-sm font-semibold capitalize">
                    {day.dateLabel}
                  </h3>
                  <ul className="space-y-2">
                    {day.items.map((row) => {
                      const empresa =
                        companyNameById.get(row.company_id) ?? `Empresa ${row.company_id}`
                      const pct = calcDiscountPercent(row.regular_price, row.sale_price)
                      const tooltip = [
                        empresa,
                        `Antes ${formatCurrency(row.regular_price)} → Ahora ${formatCurrency(row.sale_price)}`,
                        pct != null ? `Descuento -${pct}%` : "",
                        `Vigencia hasta ${formatDateShort(row.fecha_fin)}`,
                      ]
                        .filter(Boolean)
                        .join(" · ")

                      return (
                        <li key={`${row.snapshot_id}-${row.promotion_id}`}>
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <button
                                type="button"
                                className={`hover:opacity-90 flex w-full items-center justify-between gap-3 rounded-lg border border-l-4 px-3 py-2.5 text-left transition-opacity ${calendarRowClass(row.tipo, row.estado)}`}
                                onClick={() => onOpen(row)}
                              >
                                <div className="min-w-0 flex-1">
                                  <p className="truncate font-medium">{productDisplayName(row)}</p>
                                  <p className="text-muted-foreground text-xs">
                                    {empresa}
                                    {pct != null ? ` · -${pct}%` : ""}
                                  </p>
                                </div>
                                <div className="flex shrink-0 items-center gap-2">
                                  <PromotionTipoBadge tipo={row.tipo} />
                                  <PromotionStatusBadge estado={row.estado} />
                                </div>
                              </button>
                            </TooltipTrigger>
                            <TooltipContent side="top" className="max-w-xs text-xs">
                              {tooltip}
                            </TooltipContent>
                          </Tooltip>
                        </li>
                      )
                    })}
                  </ul>
                </div>
              ))}
            </div>
          </section>
        ))}
      </div>
    </TooltipProvider>
  )
}
