"use client"

import { CostV2StatusBadge } from "@/components/costos-v2/cost-v2-status-badge"
import { ChangeCell } from "@/components/costos-v2/cost-v2-recent-changes"
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
  displayCorrectedGross,
  formatDateCL,
  formatMoneyCLPTable,
} from "@/lib/costos-v2/format"
import type { CostV2ProductItem } from "@/lib/costos-v2/types"

/** Máximo 9 columnas visibles. */
export const COST_V2_PRODUCT_TABLE_COLUMNS = [
  "Producto",
  "Último costo",
  "Costo anterior",
  "Variación",
  "Última recepción",
  "Estado",
  "Ver",
] as const

export function CostV2ProductsTable({
  items,
  loading,
  onOpen,
}: {
  items: CostV2ProductItem[]
  loading?: boolean
  onOpen: (variantId: number) => void
}) {
  if (!loading && !items.length) {
    return (
      <p className="rounded-md border border-dashed border-border/70 px-4 py-10 text-center text-sm text-muted-foreground">
        No hay productos para los filtros seleccionados.
      </p>
    )
  }

  return (
    <div className="overflow-x-auto rounded-md border border-border/70">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Producto</TableHead>
            <TableHead className="text-right">Último costo</TableHead>
            <TableHead className="text-right">Costo anterior</TableHead>
            <TableHead>Variación</TableHead>
            <TableHead>Última recepción</TableHead>
            <TableHead>Estado</TableHead>
            <TableHead className="text-right">Ver</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {items.map((r) => (
            <TableRow key={r.variant_id} className={loading ? "opacity-60" : undefined}>
              <TableCell className="max-w-[260px]">
                <div className="truncate font-medium leading-tight">
                  {r.product_name || "Producto"}
                </div>
                <div className="truncate text-[11px] text-muted-foreground">
                  {[r.variant_name, r.barcode].filter(Boolean).join(" · ") || "—"}
                </div>
              </TableCell>
              <TableCell className="text-right tabular-nums">
                {displayCorrectedGross(r.current_corrected_gross_cost)}
              </TableCell>
              <TableCell className="text-right tabular-nums">
                {formatMoneyCLPTable(r.previous_corrected_gross_cost)}
              </TableCell>
              <TableCell>
                <ChangeCell
                  amount={r.unit_change_amount}
                  percent={r.unit_change_percent}
                />
              </TableCell>
              <TableCell className="whitespace-nowrap text-xs tabular-nums">
                {formatDateCL(r.latest_admission_date)}
              </TableCell>
              <TableCell>
                <CostV2StatusBadge status={r.current_quality_status} />
              </TableCell>
              <TableCell className="text-right">
                <Button
                  type="button"
                  size="sm"
                  variant="ghost"
                  onClick={() => onOpen(r.variant_id)}
                >
                  Ver
                </Button>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  )
}
