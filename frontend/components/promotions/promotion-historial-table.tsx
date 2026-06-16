"use client"

import { useMemo, useState } from "react"
import { ArrowDown, ArrowUp, Download, Loader2 } from "lucide-react"
import { Button } from "@/components/ui/button"
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
  calcDiscountPercent,
  formatCurrency,
  formatDateShort,
  formatDiscountBadge,
  productDisplayName,
} from "@/lib/promotions-utils"
import { PromotionStatusBadge } from "@/components/promotions/promotion-badges"
import { PromotionLabelStatusBadge } from "@/components/promotions/promotion-label-status-badge"

type SortKey =
  | "estado"
  | "empresa"
  | "producto"
  | "codigo"
  | "antes"
  | "ahora"
  | "descuento"
  | "inicio"
  | "fin"

type PromotionHistorialTableProps = {
  rows: PromotionGridRow[]
  loading: boolean
  companyNameById: Map<number, string>
  onOpen: (row: PromotionGridRow) => void
}

function compareRows(
  a: PromotionGridRow,
  b: PromotionGridRow,
  key: SortKey,
  companyNameById: Map<number, string>,
): number {
  switch (key) {
    case "estado":
      return a.estado.localeCompare(b.estado)
    case "empresa":
      return (companyNameById.get(a.company_id) ?? "").localeCompare(
        companyNameById.get(b.company_id) ?? "",
      )
    case "producto":
      return productDisplayName(a).localeCompare(productDisplayName(b))
    case "codigo":
      return a.codigo_barras.localeCompare(b.codigo_barras)
    case "antes":
      return Number(a.regular_price) - Number(b.regular_price)
    case "ahora":
      return Number(a.sale_price) - Number(b.sale_price)
    case "descuento":
      return (
        (calcDiscountPercent(a.regular_price, a.sale_price) ?? 0) -
        (calcDiscountPercent(b.regular_price, b.sale_price) ?? 0)
      )
    case "inicio":
      return a.fecha_inicio.localeCompare(b.fecha_inicio)
    case "fin":
      return a.fecha_fin.localeCompare(b.fecha_fin)
    default:
      return 0
  }
}

function exportHistorialCsv(
  rows: PromotionGridRow[],
  companyNameById: Map<number, string>,
) {
  const header = [
    "Estado",
    "Empresa",
    "Producto",
    "Codigo",
    "Antes",
    "Ahora",
    "Descuento",
    "Inicio",
    "Fin",
    "Usuario",
    "Ultima modificacion",
    "Etiqueta",
  ]
  const lines = rows.map((r) => [
    r.estado,
    companyNameById.get(r.company_id) ?? "",
    productDisplayName(r),
    r.codigo_barras,
    String(r.regular_price),
    String(r.sale_price),
    formatDiscountBadge(r.regular_price, r.sale_price),
    r.fecha_inicio,
    r.fecha_fin,
    "",
    "",
    r.has_label_generated ? "Si" : "No",
  ])
  const csv = [header, ...lines]
    .map((row) => row.map((c) => `"${String(c).replace(/"/g, '""')}"`).join(","))
    .join("\n")
  const blob = new Blob(["\uFEFF" + csv], { type: "text/csv;charset=utf-8" })
  const url = URL.createObjectURL(blob)
  const a = document.createElement("a")
  a.href = url
  a.download = `promociones-historial-${new Date().toISOString().slice(0, 10)}.csv`
  a.click()
  URL.revokeObjectURL(url)
}

export function PromotionHistorialTable({
  rows,
  loading,
  companyNameById,
  onOpen,
}: PromotionHistorialTableProps) {
  const [sortKey, setSortKey] = useState<SortKey>("inicio")
  const [sortAsc, setSortAsc] = useState(false)

  const sorted = useMemo(() => {
    const copy = [...rows]
    copy.sort((a, b) => {
      const c = compareRows(a, b, sortKey, companyNameById)
      return sortAsc ? c : -c
    })
    return copy
  }, [rows, sortKey, sortAsc, companyNameById])

  const toggleSort = (key: SortKey) => {
    if (sortKey === key) setSortAsc((v) => !v)
    else {
      setSortKey(key)
      setSortAsc(true)
    }
  }

  const SortHead = ({ label, col }: { label: string; col: SortKey }) => (
    <TableHead>
      <button
        type="button"
        className="hover:text-foreground flex items-center gap-1 font-medium"
        onClick={() => toggleSort(col)}
      >
        {label}
        {sortKey === col ? (
          sortAsc ? (
            <ArrowUp className="h-3.5 w-3.5" />
          ) : (
            <ArrowDown className="h-3.5 w-3.5" />
          )
        ) : null}
      </button>
    </TableHead>
  )

  if (loading) {
    return (
      <div className="flex items-center justify-center gap-2 py-16 text-muted-foreground">
        <Loader2 className="h-6 w-6 animate-spin" />
        <span>Cargando historial…</span>
      </div>
    )
  }

  return (
    <div className="space-y-3">
      <div className="flex justify-end">
        <Button
          type="button"
          variant="outline"
          size="sm"
          disabled={rows.length === 0}
          onClick={() => exportHistorialCsv(sorted, companyNameById)}
        >
          <Download className="mr-2 h-4 w-4" />
          Exportar Excel
        </Button>
      </div>
      <div className="overflow-x-auto rounded-xl border">
        <Table>
          <TableHeader>
            <TableRow>
              <SortHead label="Estado" col="estado" />
              <SortHead label="Empresa" col="empresa" />
              <SortHead label="Producto" col="producto" />
              <SortHead label="Código" col="codigo" />
              <SortHead label="Antes" col="antes" />
              <SortHead label="Ahora" col="ahora" />
              <SortHead label="Descuento" col="descuento" />
              <SortHead label="Inicio" col="inicio" />
              <SortHead label="Fin" col="fin" />
              <TableHead>Usuario</TableHead>
              <TableHead>Últ. modificación</TableHead>
              <TableHead>Etiqueta</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {sorted.length === 0 ? (
              <TableRow>
                <TableCell colSpan={12} className="text-muted-foreground text-center">
                  Sin registros en el historial.
                </TableCell>
              </TableRow>
            ) : (
              sorted.map((row) => (
                <TableRow
                  key={`${row.snapshot_id}-${row.promotion_id}`}
                  className="cursor-pointer"
                  onClick={() => onOpen(row)}
                >
                  <TableCell>
                    <PromotionStatusBadge estado={row.estado} />
                  </TableCell>
                  <TableCell className="max-w-[140px] truncate text-sm">
                    {companyNameById.get(row.company_id) ?? `ID ${row.company_id}`}
                  </TableCell>
                  <TableCell className="max-w-[200px] truncate font-medium">
                    {productDisplayName(row)}
                  </TableCell>
                  <TableCell className="font-mono text-xs">{row.codigo_barras}</TableCell>
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
                  <TableCell className="text-muted-foreground text-xs">—</TableCell>
                  <TableCell className="text-muted-foreground text-xs">—</TableCell>
                  <TableCell>
                    <PromotionLabelStatusBadge generated={row.has_label_generated} />
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  )
}
