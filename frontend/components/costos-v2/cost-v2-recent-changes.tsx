"use client"

import { ArrowDownRight, ArrowUpRight, Minus } from "lucide-react"

import { Button } from "@/components/ui/button"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  changeDirection,
  formatDateCL,
  formatMoneyCLPTable,
  formatPercentCL,
} from "@/lib/costos-v2/format"
import type { CostV2ProductItem } from "@/lib/costos-v2/types"
import { cn } from "@/lib/utils"

function ChangeCell({
  amount,
  percent,
}: {
  amount: string | null
  percent: string | null
}) {
  const dir = changeDirection(amount)
  const Icon =
    dir === "up" ? ArrowUpRight : dir === "down" ? ArrowDownRight : Minus
  return (
    <div
      className={cn(
        "inline-flex items-center gap-1 tabular-nums",
        dir === "up" && "text-emerald-700 dark:text-emerald-400",
        dir === "down" && "text-red-700 dark:text-red-400",
        dir === "none" && "text-muted-foreground",
      )}
    >
      <Icon className="h-3.5 w-3.5 shrink-0" />
      <span>
        {formatMoneyCLPTable(amount)}
        <span className="ml-1 text-xs text-muted-foreground">
          {formatPercentCL(percent)}
        </span>
      </span>
    </div>
  )
}

export function CostV2RecentChanges({
  items,
  loading,
  onOpen,
  onSeeAll,
}: {
  items: CostV2ProductItem[]
  loading?: boolean
  onOpen: (variantId: number) => void
  onSeeAll: () => void
}) {
  const rows = items.slice(0, 10)
  return (
    <section className="rounded-md border border-border/70">
      <div className="flex items-center justify-between border-b border-border/60 px-3 py-2">
        <h2 className="text-sm font-semibold">Cambios de costo recientes</h2>
        <Button type="button" variant="ghost" size="sm" onClick={onSeeAll}>
          Ver todos
        </Button>
      </div>
      {loading && !rows.length ? (
        <p className="px-3 py-8 text-center text-sm text-muted-foreground">Cargando…</p>
      ) : !rows.length ? (
        <p className="px-3 py-8 text-center text-sm text-muted-foreground">
          Sin cambios comparables en el rango.
        </p>
      ) : (
        <div className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Producto</TableHead>
                <TableHead className="text-right">Anterior</TableHead>
                <TableHead className="text-right">Actual</TableHead>
                <TableHead>Variación</TableHead>
                <TableHead>Fecha</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.map((r) => (
                <TableRow
                  key={r.variant_id}
                  className="cursor-pointer"
                  onClick={() => onOpen(r.variant_id)}
                >
                  <TableCell className="max-w-[220px]">
                    <div className="truncate font-medium leading-tight">
                      {r.product_name || "Producto"}
                    </div>
                    <div className="truncate text-[11px] text-muted-foreground">
                      {[r.variant_name, r.barcode].filter(Boolean).join(" · ") || "—"}
                    </div>
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    {formatMoneyCLPTable(r.previous_corrected_gross_cost)}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    {formatMoneyCLPTable(r.current_corrected_gross_cost)}
                  </TableCell>
                  <TableCell>
                    <ChangeCell
                      amount={r.unit_change_amount}
                      percent={r.unit_change_percent}
                    />
                  </TableCell>
                  <TableCell className="whitespace-nowrap tabular-nums text-xs">
                    {formatDateCL(r.latest_admission_date)}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
    </section>
  )
}

export { ChangeCell }
