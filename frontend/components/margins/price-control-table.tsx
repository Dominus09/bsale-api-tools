"use client"

import { useMemo, useState } from "react"
import { ArrowDown, ArrowUp, ArrowUpDown, Columns3, Eye } from "lucide-react"

import {
  PriceControlStatusBadge,
  rowStatusClass,
} from "@/components/margins/price-control-status-badge"
import { Button } from "@/components/ui/button"
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyTitle,
} from "@/components/ui/empty"
import { Skeleton } from "@/components/ui/skeleton"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { formatDateShort, formatMoneyCLP } from "@/lib/costos/format"
import { GROSS_COST_QUALITY_LABEL, type GrossCostQualityKind } from "@/lib/costos/quality-labels"
import type { PriceControlRow } from "@/lib/margins/adapt-price-control"
import { cn } from "@/lib/utils"

type ColId =
  | "product"
  | "variant"
  | "barcode"
  | "type"
  | "list"
  | "cost"
  | "costDate"
  | "age"
  | "price"
  | "diff"
  | "markup"
  | "minMarkup"
  | "maxMarkup"
  | "marginOnPrice"
  | "minRec"
  | "maxRec"
  | "adj"
  | "status"
  | "quality"
  | "actions"

const ALL_COLS: { id: ColId; label: string; defaultOn: boolean }[] = [
  { id: "product", label: "Producto", defaultOn: true },
  { id: "variant", label: "Variante", defaultOn: true },
  { id: "barcode", label: "Código de barras", defaultOn: true },
  { id: "type", label: "Tipo", defaultOn: false },
  { id: "list", label: "Lista de precio", defaultOn: true },
  { id: "cost", label: "Costo bruto máx.", defaultOn: true },
  { id: "costDate", label: "Fecha costo", defaultOn: true },
  { id: "age", label: "Antigüedad", defaultOn: true },
  { id: "price", label: "Precio bruto", defaultOn: true },
  { id: "diff", label: "Diferencia $", defaultOn: true },
  { id: "markup", label: "Recargo real", defaultOn: true },
  { id: "minMarkup", label: "Recargo mín.", defaultOn: true },
  { id: "maxMarkup", label: "Recargo máx.", defaultOn: true },
  { id: "marginOnPrice", label: "Margen / precio", defaultOn: false },
  { id: "minRec", label: "Precio mín. rec.", defaultOn: true },
  { id: "maxRec", label: "Precio máx. rec.", defaultOn: false },
  { id: "adj", label: "Ajuste requerido", defaultOn: true },
  { id: "status", label: "Estado", defaultOn: true },
  { id: "quality", label: "Calidad costo", defaultOn: false },
  { id: "actions", label: "Acciones", defaultOn: true },
]

type SortKey =
  | "product"
  | "list"
  | "markup"
  | "adj"
  | "age"
  | "price"
  | "cost"

function fmtPct(v: number | null): string {
  if (v == null || !Number.isFinite(v)) return "—"
  return `${v.toFixed(2)}%`
}

function qualityLabel(q: string | null): string {
  if (!q) return "—"
  if (q in GROSS_COST_QUALITY_LABEL) {
    return GROSS_COST_QUALITY_LABEL[q as GrossCostQualityKind]
  }
  if (q === "current_tax_profile_fallback") return "Fallback"
  return q
}

function sortRows(rows: PriceControlRow[], key: SortKey, dir: "asc" | "desc") {
  const mul = dir === "asc" ? 1 : -1
  return [...rows].sort((a, b) => {
    const av =
      key === "product"
        ? (a.productName || "").toLowerCase()
        : key === "list"
          ? (a.priceListName || "").toLowerCase()
          : key === "markup"
            ? a.actualMarkupPct ?? -999
            : key === "adj"
              ? a.priceAdjustmentToMinimum ?? -999999
              : key === "age"
                ? a.costAgeDays ?? 99999
                : key === "price"
                  ? a.grossPrice ?? -1
                  : a.referenceGrossCost ?? -1
    const bv =
      key === "product"
        ? (b.productName || "").toLowerCase()
        : key === "list"
          ? (b.priceListName || "").toLowerCase()
          : key === "markup"
            ? b.actualMarkupPct ?? -999
            : key === "adj"
              ? b.priceAdjustmentToMinimum ?? -999999
              : key === "age"
                ? b.costAgeDays ?? 99999
                : key === "price"
                  ? b.grossPrice ?? -1
                  : b.referenceGrossCost ?? -1
    if (av < bv) return -1 * mul
    if (av > bv) return 1 * mul
    return 0
  })
}

export function PriceControlTable({
  rows,
  loading,
  error,
  page,
  pageSize,
  onPageChange,
  onSelect,
}: {
  rows: PriceControlRow[]
  loading?: boolean
  error?: string | null
  page: number
  pageSize: number
  onPageChange: (page: number) => void
  onSelect: (row: PriceControlRow) => void
}) {
  const [visible, setVisible] = useState<Record<ColId, boolean>>(() =>
    Object.fromEntries(ALL_COLS.map((c) => [c.id, c.defaultOn])) as Record<ColId, boolean>,
  )
  const [sortKey, setSortKey] = useState<SortKey>("adj")
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc")

  const sorted = useMemo(
    () => sortRows(rows, sortKey, sortDir),
    [rows, sortKey, sortDir],
  )
  const totalPages = Math.max(1, Math.ceil(sorted.length / pageSize))
  const pageSafe = Math.min(page, totalPages)
  const pageRows = sorted.slice((pageSafe - 1) * pageSize, pageSafe * pageSize)
  const cols = ALL_COLS.filter((c) => visible[c.id])

  function toggleSort(key: SortKey) {
    if (sortKey === key) setSortDir((d) => (d === "asc" ? "desc" : "asc"))
    else {
      setSortKey(key)
      setSortDir(key === "product" || key === "list" ? "asc" : "desc")
    }
  }

  function SortIcon({ k }: { k: SortKey }) {
    if (sortKey !== k) return <ArrowUpDown className="ml-1 inline h-3 w-3 opacity-40" />
    return sortDir === "asc" ? (
      <ArrowUp className="ml-1 inline h-3 w-3" />
    ) : (
      <ArrowDown className="ml-1 inline h-3 w-3" />
    )
  }

  if (error) {
    return (
      <div className="rounded-lg border border-destructive/40 bg-destructive/5 p-6 text-sm text-destructive">
        {error}
      </div>
    )
  }

  if (loading) {
    return (
      <div className="space-y-2">
        {Array.from({ length: 8 }).map((_, i) => (
          <Skeleton key={i} className="h-10 w-full" />
        ))}
      </div>
    )
  }

  if (!rows.length) {
    return (
      <Empty className="border">
        <EmptyHeader>
          <EmptyTitle>Sin combinaciones</EmptyTitle>
          <EmptyDescription>
            No hay filas variante × lista con los filtros actuales.
          </EmptyDescription>
        </EmptyHeader>
      </Empty>
    )
  }

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between gap-2">
        <p className="text-xs text-muted-foreground">
          {rows.length} filas · página {pageSafe}/{totalPages}
        </p>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button type="button" variant="outline" size="sm" className="gap-1">
              <Columns3 className="h-3.5 w-3.5" />
              Columnas
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end" className="max-h-80 overflow-y-auto">
            <DropdownMenuLabel>Visible</DropdownMenuLabel>
            <DropdownMenuSeparator />
            {ALL_COLS.map((c) => (
              <DropdownMenuCheckboxItem
                key={c.id}
                checked={visible[c.id]}
                onCheckedChange={(v) =>
                  setVisible((prev) => ({ ...prev, [c.id]: Boolean(v) }))
                }
              >
                {c.label}
              </DropdownMenuCheckboxItem>
            ))}
          </DropdownMenuContent>
        </DropdownMenu>
      </div>

      <div className="overflow-auto rounded-lg border border-border/70">
        <Table>
          <TableHeader className="sticky top-0 z-10 bg-background/95 backdrop-blur">
            <TableRow>
              {cols.map((c) => {
                const sortable =
                  c.id === "product" ||
                  c.id === "list" ||
                  c.id === "markup" ||
                  c.id === "adj" ||
                  c.id === "age" ||
                  c.id === "price" ||
                  c.id === "cost"
                const sk =
                  c.id === "product"
                    ? "product"
                    : c.id === "list"
                      ? "list"
                      : c.id === "markup"
                        ? "markup"
                        : c.id === "adj"
                          ? "adj"
                          : c.id === "age"
                            ? "age"
                            : c.id === "price"
                              ? "price"
                              : c.id === "cost"
                                ? "cost"
                                : null
                return (
                  <TableHead key={c.id} className="whitespace-nowrap text-xs">
                    {sortable && sk ? (
                      <button
                        type="button"
                        className="inline-flex items-center"
                        onClick={() => toggleSort(sk)}
                      >
                        {c.label}
                        <SortIcon k={sk} />
                      </button>
                    ) : (
                      c.label
                    )}
                  </TableHead>
                )
              })}
            </TableRow>
          </TableHeader>
          <TableBody>
            {pageRows.map((r) => (
              <TableRow
                key={`${r.variantId}-${r.priceListId}`}
                className={cn("cursor-pointer", rowStatusClass(r.status))}
                onClick={() => onSelect(r)}
              >
                {cols.map((c) => (
                  <TableCell key={c.id} className="whitespace-nowrap text-xs tabular-nums">
                    {c.id === "product" ? (
                      <span className="font-medium normal-nums">{r.productName || "—"}</span>
                    ) : c.id === "variant" ? (
                      <span className="normal-nums">{r.variantName || "—"}</span>
                    ) : c.id === "barcode" ? (
                      <span className="normal-nums">{r.barcode || r.sku || "—"}</span>
                    ) : c.id === "type" ? (
                      <span className="normal-nums">{r.productTypeName || "—"}</span>
                    ) : c.id === "list" ? (
                      <span className="normal-nums">{r.priceListName || r.priceListId}</span>
                    ) : c.id === "cost" ? (
                      formatMoneyCLP(r.referenceGrossCost)
                    ) : c.id === "costDate" ? (
                      formatDateShort(r.costDate)
                    ) : c.id === "age" ? (
                      r.costAgeDays == null ? "—" : `${r.costAgeDays}d`
                    ) : c.id === "price" ? (
                      formatMoneyCLP(r.grossPrice)
                    ) : c.id === "diff" ? (
                      formatMoneyCLP(r.priceDiffVsCost)
                    ) : c.id === "markup" ? (
                      fmtPct(r.actualMarkupPct)
                    ) : c.id === "minMarkup" ? (
                      fmtPct(r.minMarkupPct)
                    ) : c.id === "maxMarkup" ? (
                      fmtPct(r.maxMarkupPct)
                    ) : c.id === "marginOnPrice" ? (
                      <span className="text-muted-foreground">{fmtPct(r.grossMarginPct)}</span>
                    ) : c.id === "minRec" ? (
                      formatMoneyCLP(r.minimumRecommendedGrossPrice)
                    ) : c.id === "maxRec" ? (
                      formatMoneyCLP(r.maximumRecommendedGrossPrice)
                    ) : c.id === "adj" ? (
                      formatMoneyCLP(r.priceAdjustmentToMinimum)
                    ) : c.id === "status" ? (
                      <PriceControlStatusBadge status={r.status} />
                    ) : c.id === "quality" ? (
                      qualityLabel(r.grossCostQuality)
                    ) : (
                      <Button
                        type="button"
                        size="sm"
                        variant="ghost"
                        className="h-7 px-2"
                        onClick={(e) => {
                          e.stopPropagation()
                          onSelect(r)
                        }}
                      >
                        <Eye className="h-3.5 w-3.5" />
                      </Button>
                    )}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>

      <div className="flex items-center justify-end gap-2">
        <Button
          type="button"
          variant="outline"
          size="sm"
          disabled={pageSafe <= 1}
          onClick={() => onPageChange(pageSafe - 1)}
        >
          Anterior
        </Button>
        <Button
          type="button"
          variant="outline"
          size="sm"
          disabled={pageSafe >= totalPages}
          onClick={() => onPageChange(pageSafe + 1)}
        >
          Siguiente
        </Button>
      </div>
    </div>
  )
}
