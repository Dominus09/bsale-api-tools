"use client"

import { Loader2 } from "lucide-react"
import type { PromotionGridRow } from "@/lib/api"
import {
  formatCurrency,
  formatDiscountBadge,
  groupPromotionsByStartDate,
  productDisplayName,
} from "@/lib/promotions-utils"
import {
  PromotionStatusBadge,
  PromotionTipoBadge,
} from "@/components/promotions/promotion-badges"

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
    <div className="space-y-8">
      {months.map((month) => (
        <section key={month.monthKey}>
          <h2 className="mb-4 text-lg font-semibold capitalize">{month.monthLabel}</h2>
          <div className="space-y-6">
            {month.days.map((day) => (
              <div key={day.dateKey} className="rounded-xl border bg-card p-4">
                <h3 className="text-muted-foreground mb-3 text-sm font-semibold uppercase tracking-wide">
                  {day.dateLabel}
                </h3>
                <ul className="space-y-2">
                  {day.items.map((row) => (
                    <li key={`${row.snapshot_id}-${row.promotion_id}`}>
                      <button
                        type="button"
                        className="hover:bg-muted/60 flex w-full items-center justify-between gap-3 rounded-lg border px-3 py-2.5 text-left transition-colors"
                        onClick={() => onOpen(row)}
                      >
                        <div className="min-w-0 flex-1">
                          <p className="truncate font-medium">{productDisplayName(row)}</p>
                          <p className="text-muted-foreground text-xs">
                            {companyNameById.get(row.company_id) ?? `Empresa ${row.company_id}`}
                            {" · "}
                            {formatCurrency(row.sale_price)}
                            {" · "}
                            {formatDiscountBadge(row.regular_price, row.sale_price)}
                          </p>
                        </div>
                        <div className="flex shrink-0 items-center gap-2">
                          <PromotionTipoBadge tipo={row.tipo} />
                          <PromotionStatusBadge estado={row.estado} />
                        </div>
                      </button>
                    </li>
                  ))}
                </ul>
              </div>
            ))}
          </div>
        </section>
      ))}
    </div>
  )
}
