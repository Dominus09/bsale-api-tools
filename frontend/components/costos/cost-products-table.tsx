"use client"

import { Badge } from "@/components/ui/badge"
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
  formatDateCL,
  formatChangeCell,
  formatMoneyCLPTable,
} from "@/lib/costos/control/format"
import { BUSINESS_SITUATION_LABEL } from "@/lib/costos/control/labels"
import type { CompanyProductItem } from "@/lib/costos/control/types"

/** Máximo 7 columnas visibles en el control consolidado. */
export const COST_V2_PRODUCT_TABLE_COLUMNS = [
  "Producto",
  "Costo vigente",
  "Cambio",
  "Último cambio",
  "Origen",
  "Situación",
  "Ver",
] as const

export function CostProductsTable({
  items,
  loading,
  onOpen,
}: {
  items: CompanyProductItem[]
  loading?: boolean
  onOpen: (variantId: number) => void
}) {
  if (!loading && !items.length) {
    return (
      <p className="rounded-md border border-dashed border-border/70 px-4 py-10 text-center text-sm text-muted-foreground">
        No existen recepciones calculadas para el periodo seleccionado.
      </p>
    )
  }

  return (
    <div className="overflow-x-auto rounded-md border border-border/70">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Producto</TableHead>
            <TableHead className="text-right">Costo vigente</TableHead>
            <TableHead>Cambio</TableHead>
            <TableHead>Último cambio</TableHead>
            <TableHead>Origen</TableHead>
            <TableHead>Situación</TableHead>
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
                  {[r.variant_name, r.barcode].filter(Boolean).join(" · ") || "Variante sin código"}
                </div>
              </TableCell>
              <TableCell className="text-right tabular-nums">
                {formatMoneyCLPTable(r.current_cost)}
              </TableCell>
              <TableCell>
                <span className="text-xs font-medium tabular-nums">{formatChangeCell({ amount: r.change_amount, percent: r.change_percent, hasComparable: r.has_comparable_cost, visualNoChange: r.visual_no_change })}</span>
              </TableCell>
              <TableCell className="whitespace-nowrap text-xs tabular-nums">
                {formatDateCL(r.last_change_date)}
              </TableCell>
              <TableCell>
                <span className="text-xs">{r.current_office_name || "Sin origen disponible"}</span>
              </TableCell>
              <TableCell className="min-w-[180px]">
                <div className="flex flex-wrap gap-1">
                  {r.business_statuses.map((status) => <Badge key={status} variant="outline" className="text-[10px] font-normal">{BUSINESS_SITUATION_LABEL[status] ?? "Situación pendiente"}</Badge>)}
                </div>
                <p className="mt-1 text-[11px] text-muted-foreground">{r.coverage_label}</p>
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
