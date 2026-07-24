"use client"

import { useMemo, useState } from "react"
import { ArrowDown, ArrowUp, ArrowUpDown, Columns3 } from "lucide-react"

import {
  CostAgeBadge,
  CostQualityBadge,
} from "@/components/costos/cost-quality-badge"
import { Badge } from "@/components/ui/badge"
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
import type { CostTableRow } from "@/lib/costos/adapt-cost-analytics"
import { formatDateShort, formatMoneyCLP, formatPct } from "@/lib/costos/format"
import { COST_ORIGIN_LABEL } from "@/lib/costos/quality-labels"
import { cn } from "@/lib/utils"

type ColId =
  | "product"
  | "variant"
  | "code"
  | "date"
  | "net"
  | "iva"
  | "other"
  | "gross"
  | "prevGross"
  | "maxGross"
  | "minGross"
  | "varAmt"
  | "varPct"
  | "avg"
  | "origin"
  | "age"
  | "quality"
  | "actions"

const ALL_COLS: { id: ColId; label: string; defaultOn: boolean }[] = [
  { id: "product", label: "Producto", defaultOn: true },
  { id: "variant", label: "Variante", defaultOn: true },
  { id: "code", label: "Código", defaultOn: true },
  { id: "date", label: "Última recepción", defaultOn: true },
  { id: "net", label: "Costo neto", defaultOn: true },
  { id: "iva", label: "IVA", defaultOn: true },
  { id: "other", label: "ILA / otros imp.", defaultOn: true },
  { id: "gross", label: "Costo bruto", defaultOn: true },
  { id: "prevGross", label: "Bruto anterior", defaultOn: true },
  { id: "maxGross", label: "Bruto máx. válido", defaultOn: true },
  { id: "minGross", label: "Bruto mínimo", defaultOn: false },
  { id: "varAmt", label: "Var. $", defaultOn: true },
  { id: "varPct", label: "Var. %", defaultOn: true },
  { id: "avg", label: "Promedio", defaultOn: false },
  { id: "origin", label: "Origen", defaultOn: true },
  { id: "age", label: "Antigüedad", defaultOn: true },
  { id: "quality", label: "Calidad", defaultOn: true },
  { id: "actions", label: "Acciones", defaultOn: true },
]

type SortKey = "product" | "date" | "gross" | "varPct" | "age"

function sortRows(rows: CostTableRow[], key: SortKey, dir: "asc" | "desc") {
  const mul = dir === "asc" ? 1 : -1
  return [...rows].sort((a, b) => {
    const av =
      key === "product"
        ? (a.productName || "").toLowerCase()
        : key === "date"
          ? a.lastReceptionDate || ""
          : key === "gross"
            ? a.costGross ?? -1
            : key === "varPct"
              ? a.variationPct ?? -999
              : a.ageDays ?? 9999
    const bv =
      key === "product"
        ? (b.productName || "").toLowerCase()
        : key === "date"
          ? b.lastReceptionDate || ""
          : key === "gross"
            ? b.costGross ?? -1
            : key === "varPct"
              ? b.variationPct ?? -999
              : b.ageDays ?? 9999
    if (av < bv) return -1 * mul
    if (av > bv) return 1 * mul
    return 0
  })
}

export function CostMainTable({
  rows,
  loading,
  error,
  page,
  pageSize,
  total,
  onPageChange,
  onSelect,
}: {
  rows: CostTableRow[]
  loading?: boolean
  error?: string | null
  page: number
  pageSize: number
  total: number
  onPageChange: (page: number) => void
  onSelect: (row: CostTableRow) => void
}) {
  const [visible, setVisible] = useState<Record<ColId, boolean>>(() =>
    Object.fromEntries(ALL_COLS.map((c) => [c.id, c.defaultOn])) as Record<
      ColId,
      boolean
    >,
  )
  const [sortKey, setSortKey] = useState<SortKey>("date")
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc")

  const sorted = useMemo(
    () => sortRows(rows, sortKey, sortDir),
    [rows, sortKey, sortDir],
  )

  const pageCount = Math.max(1, Math.ceil(total / pageSize))
  const show = (id: ColId) => visible[id]

  function toggleSort(key: SortKey) {
    if (sortKey === key) setSortDir((d) => (d === "asc" ? "desc" : "asc"))
    else {
      setSortKey(key)
      setSortDir(key === "product" ? "asc" : "desc")
    }
  }

  function SortBtn({
    col,
    label,
  }: {
    col: SortKey
    label: string
  }) {
    const active = sortKey === col
    const Icon = !active ? ArrowUpDown : sortDir === "asc" ? ArrowUp : ArrowDown
    return (
      <button
        type="button"
        className="inline-flex items-center gap-1 outline-none focus-visible:ring-2 focus-visible:ring-ring"
        onClick={() => toggleSort(col)}
      >
        {label}
        <Icon className="h-3 w-3 opacity-60" />
      </button>
    )
  }

  const summaryGross = rows.reduce(
    (acc, r) => acc + (r.costGross != null && r.costGross > 0 ? r.costGross : 0),
    0,
  )

  if (error) {
    return (
      <div className="rounded-lg border border-destructive/40 bg-destructive/5 p-6 text-sm text-destructive">
        {error}
      </div>
    )
  }

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <p className="text-xs text-muted-foreground">
          {total} variantes · suma bruto visible (página):{" "}
          <span className="font-medium text-foreground tabular-nums">
            {formatMoneyCLP(summaryGross || null)}
          </span>
        </p>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button type="button" variant="outline" size="sm" className="gap-1.5">
              <Columns3 className="h-3.5 w-3.5" />
              Columnas
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end" className="w-52">
            <DropdownMenuLabel>Mostrar columnas</DropdownMenuLabel>
            <DropdownMenuSeparator />
            {ALL_COLS.filter((c) => c.id !== "actions").map((c) => (
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

      {/* Desktop table */}
      <div className="hidden overflow-hidden rounded-lg border border-border/70 md:block">
        <div className="max-h-[min(70vh,720px)] overflow-auto">
          <Table>
            <TableHeader className="sticky top-0 z-10 bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/80">
              <TableRow>
                {show("product") ? (
                  <TableHead>
                    <SortBtn col="product" label="Producto" />
                  </TableHead>
                ) : null}
                {show("variant") ? <TableHead>Variante</TableHead> : null}
                {show("code") ? <TableHead>Código</TableHead> : null}
                {show("date") ? (
                  <TableHead>
                    <SortBtn col="date" label="Última recepción" />
                  </TableHead>
                ) : null}
                {show("net") ? <TableHead className="text-right">Neto</TableHead> : null}
                {show("iva") ? <TableHead className="text-right">IVA</TableHead> : null}
                {show("other") ? (
                  <TableHead className="text-right">Otros imp.</TableHead>
                ) : null}
                {show("gross") ? (
                  <TableHead className="text-right">
                    <SortBtn col="gross" label="Bruto" />
                  </TableHead>
                ) : null}
                {show("prevGross") ? (
                  <TableHead className="text-right">Bruto ant.</TableHead>
                ) : null}
                {show("maxGross") ? (
                  <TableHead className="text-right">Máx. válido</TableHead>
                ) : null}
                {show("minGross") ? (
                  <TableHead className="text-right">Mínimo</TableHead>
                ) : null}
                {show("varAmt") ? (
                  <TableHead className="text-right">Var. $</TableHead>
                ) : null}
                {show("varPct") ? (
                  <TableHead className="text-right">
                    <SortBtn col="varPct" label="Var. %" />
                  </TableHead>
                ) : null}
                {show("avg") ? (
                  <TableHead className="text-right">Promedio</TableHead>
                ) : null}
                {show("origin") ? <TableHead>Origen</TableHead> : null}
                {show("age") ? (
                  <TableHead>
                    <SortBtn col="age" label="Antigüedad" />
                  </TableHead>
                ) : null}
                {show("quality") ? <TableHead>Calidad</TableHead> : null}
                {show("actions") ? <TableHead /> : null}
              </TableRow>
            </TableHeader>
            <TableBody>
              {loading
                ? Array.from({ length: 6 }).map((_, i) => (
                    <TableRow key={i}>
                      <TableCell colSpan={8}>
                        <Skeleton className="h-8 w-full" />
                      </TableCell>
                    </TableRow>
                  ))
                : sorted.length === 0
                  ? (
                      <TableRow>
                        <TableCell colSpan={16} className="p-0">
                          <Empty className="border-0">
                            <EmptyHeader>
                              <EmptyTitle>Sin costos en este filtro</EmptyTitle>
                              <EmptyDescription>
                                Ajuste el rango de fechas o limpie los filtros.
                              </EmptyDescription>
                            </EmptyHeader>
                          </Empty>
                        </TableCell>
                      </TableRow>
                    )
                  : sorted.map((r) => (
                      <TableRow
                        key={r.id}
                        className="cursor-pointer"
                        tabIndex={0}
                        onClick={() => onSelect(r)}
                        onKeyDown={(e) => {
                          if (e.key === "Enter" || e.key === " ") {
                            e.preventDefault()
                            onSelect(r)
                          }
                        }}
                      >
                        {show("product") ? (
                          <TableCell className="max-w-[180px] truncate font-medium">
                            {r.productName || "Sin información"}
                          </TableCell>
                        ) : null}
                        {show("variant") ? (
                          <TableCell className="max-w-[140px] truncate text-muted-foreground">
                            {r.variantName || "—"}
                          </TableCell>
                        ) : null}
                        {show("code") ? (
                          <TableCell className="font-mono text-xs">
                            {r.barcode || "—"}
                          </TableCell>
                        ) : null}
                        {show("date") ? (
                          <TableCell>{formatDateShort(r.lastReceptionDate)}</TableCell>
                        ) : null}
                        {show("net") ? (
                          <TableCell className="text-right tabular-nums">
                            {formatMoneyCLP(r.costNet)}
                          </TableCell>
                        ) : null}
                        {show("iva") ? (
                          <TableCell className="text-right tabular-nums">
                            {formatMoneyCLP(r.ivaAmount)}
                          </TableCell>
                        ) : null}
                        {show("other") ? (
                          <TableCell className="text-right tabular-nums">
                            {formatMoneyCLP(r.otherTaxes)}
                          </TableCell>
                        ) : null}
                        {show("gross") ? (
                          <TableCell className="text-right tabular-nums font-medium">
                            {formatMoneyCLP(r.costGross)}
                          </TableCell>
                        ) : null}
                        {show("prevGross") ? (
                          <TableCell className="text-right tabular-nums">
                            {formatMoneyCLP(r.previousCostGross)}
                          </TableCell>
                        ) : null}
                        {show("maxGross") ? (
                          <TableCell className="text-right tabular-nums">
                            {formatMoneyCLP(r.maxValidGrossCost)}
                          </TableCell>
                        ) : null}
                        {show("minGross") ? (
                          <TableCell className="text-right tabular-nums">
                            {formatMoneyCLP(r.minValidGrossCost)}
                          </TableCell>
                        ) : null}
                        {show("varAmt") ? (
                          <TableCell
                            className={cn(
                              "text-right tabular-nums",
                              (r.variationAmount ?? 0) > 0 && "text-red-600",
                              (r.variationAmount ?? 0) < 0 && "text-emerald-600",
                            )}
                          >
                            {formatMoneyCLP(r.variationAmount)}
                          </TableCell>
                        ) : null}
                        {show("varPct") ? (
                          <TableCell
                            className={cn(
                              "text-right tabular-nums",
                              (r.variationPct ?? 0) > 0 && "text-red-600",
                              (r.variationPct ?? 0) < 0 && "text-emerald-600",
                            )}
                          >
                            {formatPct(r.variationPct)}
                          </TableCell>
                        ) : null}
                        {show("avg") ? (
                          <TableCell className="text-right tabular-nums">
                            {formatMoneyCLP(r.averageCost)}
                          </TableCell>
                        ) : null}
                        {show("origin") ? (
                          <TableCell className="text-xs">
                            {COST_ORIGIN_LABEL[r.origin]}
                          </TableCell>
                        ) : null}
                        {show("age") ? (
                          <TableCell>
                            <CostAgeBadge kind={r.ageBucket} />
                          </TableCell>
                        ) : null}
                        {show("quality") ? (
                          <TableCell>
                            <div className="flex flex-col gap-1">
                              <CostQualityBadge kind={r.grossCostQuality} />
                              {r.isOutlier ? (
                                <Badge
                                  variant="outline"
                                  className="w-fit border-red-500/40 text-[10px] text-red-700"
                                >
                                  Extremo
                                </Badge>
                              ) : null}
                            </div>
                          </TableCell>
                        ) : null}
                        {show("actions") ? (
                          <TableCell>
                            <Button
                              type="button"
                              size="sm"
                              variant="ghost"
                              onClick={(e) => {
                                e.stopPropagation()
                                onSelect(r)
                              }}
                            >
                              Detalle
                            </Button>
                          </TableCell>
                        ) : null}
                      </TableRow>
                    ))}
            </TableBody>
          </Table>
        </div>
      </div>

      {/* Mobile cards */}
      <div className="space-y-2 md:hidden">
        {loading ? (
          Array.from({ length: 4 }).map((_, i) => (
            <Skeleton key={i} className="h-24 w-full rounded-lg" />
          ))
        ) : sorted.length === 0 ? (
          <Empty>
            <EmptyHeader>
              <EmptyTitle>Sin costos</EmptyTitle>
              <EmptyDescription>Ajuste los filtros.</EmptyDescription>
            </EmptyHeader>
          </Empty>
        ) : (
          sorted.map((r) => (
            <button
              key={r.id}
              type="button"
              onClick={() => onSelect(r)}
              className="w-full rounded-lg border border-border/70 bg-card p-3 text-left outline-none focus-visible:ring-2 focus-visible:ring-ring"
            >
              <div className="flex items-start justify-between gap-2">
                <div className="min-w-0">
                  <p className="truncate font-medium">
                    {r.productName || "Sin información"}
                  </p>
                  <p className="truncate text-xs text-muted-foreground">
                    {r.barcode || r.variantName || `ID ${r.variantId}`}
                  </p>
                </div>
                <p className="shrink-0 tabular-nums font-semibold">
                  {formatMoneyCLP(r.costGross)}
                </p>
              </div>
              <div className="mt-2 flex flex-wrap gap-1">
                <CostQualityBadge kind={r.grossCostQuality} />
                <CostAgeBadge kind={r.ageBucket} />
                <span className="text-xs text-muted-foreground">
                  {formatPct(r.variationPct)}
                </span>
              </div>
            </button>
          ))
        )}
      </div>

      <div className="flex items-center justify-between gap-2 text-xs text-muted-foreground">
        <span>
          Página {page} de {pageCount}
        </span>
        <div className="flex gap-2">
          <Button
            type="button"
            size="sm"
            variant="outline"
            disabled={page <= 1 || loading}
            onClick={() => onPageChange(page - 1)}
          >
            Anterior
          </Button>
          <Button
            type="button"
            size="sm"
            variant="outline"
            disabled={page >= pageCount || loading}
            onClick={() => onPageChange(page + 1)}
          >
            Siguiente
          </Button>
        </div>
      </div>
    </div>
  )
}
