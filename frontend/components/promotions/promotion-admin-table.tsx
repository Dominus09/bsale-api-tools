"use client"

import { Loader2 } from "lucide-react"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import type { PromotionGridRow } from "@/lib/api"
import {
  formatCurrency,
  formatDateShort,
  formatDiscountBadge,
  productDisplayName,
} from "@/lib/promotions-utils"
import { PromotionStatusBadge } from "@/components/promotions/promotion-badges"

type PromotionAdminTableProps = {
  rows: PromotionGridRow[]
  loading: boolean
  companyNameById: Map<number, string>
  onOpen: (row: PromotionGridRow) => void
}

export function PromotionAdminTable({
  rows,
  loading,
  companyNameById,
  onOpen,
}: PromotionAdminTableProps) {
  if (loading) {
    return (
      <div className="flex items-center justify-center gap-2 py-16 text-muted-foreground">
        <Loader2 className="h-6 w-6 animate-spin" />
        <span>Cargando…</span>
      </div>
    )
  }

  return (
    <div className="overflow-x-auto rounded-xl border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Estado</TableHead>
            <TableHead>Producto</TableHead>
            <TableHead>Empresa</TableHead>
            <TableHead className="text-right">Antes</TableHead>
            <TableHead className="text-right">Ahora</TableHead>
            <TableHead className="text-right">Descuento</TableHead>
            <TableHead>Inicio</TableHead>
            <TableHead>Fin</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.length === 0 ? (
            <TableRow>
              <TableCell colSpan={8} className="text-muted-foreground text-center">
                Sin registros.
              </TableCell>
            </TableRow>
          ) : (
            rows.map((row) => (
              <TableRow
                key={`${row.snapshot_id}-${row.promotion_id}`}
                className="cursor-pointer"
                onClick={() => onOpen(row)}
              >
                <TableCell>
                  <PromotionStatusBadge estado={row.estado} />
                </TableCell>
                <TableCell className="max-w-[220px]">
                  <p className="truncate font-medium">{productDisplayName(row)}</p>
                  <p className="text-muted-foreground capitalize text-xs">{row.tipo}</p>
                </TableCell>
                <TableCell className="max-w-[160px] truncate text-sm">
                  {companyNameById.get(row.company_id) ?? `ID ${row.company_id}`}
                </TableCell>
                <TableCell className="text-right text-muted-foreground line-through">
                  {formatCurrency(row.regular_price)}
                </TableCell>
                <TableCell className="text-right font-semibold text-emerald-700">
                  {formatCurrency(row.sale_price)}
                </TableCell>
                <TableCell className="text-right font-medium">
                  {formatDiscountBadge(row.regular_price, row.sale_price)}
                </TableCell>
                <TableCell>{formatDateShort(row.fecha_inicio)}</TableCell>
                <TableCell>{formatDateShort(row.fecha_fin)}</TableCell>
              </TableRow>
            ))
          )}
        </TableBody>
      </Table>
    </div>
  )
}
