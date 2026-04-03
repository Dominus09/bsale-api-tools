"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import {
  AlertTriangle,
  ChevronDown,
  FileSpreadsheet,
  Loader2,
  PencilLine,
  Percent,
  Search,
  X,
} from "lucide-react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Label } from "@/components/ui/label"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import { Switch } from "@/components/ui/switch"
import { Input } from "@/components/ui/input"
import {
  getCompanies,
  getMarginAnalysisView,
  getPriceLists,
  type Company,
  type MarginAnalysisViewRow,
  type PriceListRef,
} from "@/lib/api"
import { cn } from "@/lib/utils"

const PRODUCT_TYPE_NULL = "__null__"
const PROBLEM_STATUSES = new Set(["LOW", "PLACEHOLDER_PRICE"])

function num(v: number | string | null | undefined): number | null {
  if (v === null || v === undefined || v === "") return null
  const n = typeof v === "number" ? v : Number(v)
  return Number.isFinite(n) ? n : null
}

/** Formato tipo $4.154 (CLP, sin decimales) */
function formatMoney(value: number | null) {
  if (value === null) return "—"
  return new Intl.NumberFormat("es-CL", {
    style: "currency",
    currency: "CLP",
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value)
}

function formatPct(value: number | null) {
  if (value === null) return "—"
  return `${value.toFixed(2)}%`
}

function formatMarginDiff(value: number | null) {
  if (value === null) return "—"
  const sign = value > 0 ? "+" : ""
  return `${sign}${value.toFixed(2)}%`
}

function rowKey(r: Pick<MarginAnalysisViewRow, "variant_id" | "price_list_id">) {
  return `${r.variant_id}-${r.price_list_id}`
}

/** Redondeo entero: cost * (1 + min_margin_percent / 100) */
function suggestedPriceFromRule(cost: number | null, minMarginPercent: number | null): number | null {
  if (cost == null || cost <= 0) return null
  if (minMarginPercent == null || !Number.isFinite(minMarginPercent)) return null
  return Math.round(cost * (1 + minMarginPercent / 100))
}

function newMarginPercentPreview(cost: number | null, newPrice: number | null): number | null {
  if (cost == null || cost <= 0 || newPrice == null || !Number.isFinite(newPrice)) return null
  return ((newPrice - cost) / cost) * 100
}

/** Color del margen simulado vs mínimo (solo vista previa de edición). */
function previewMarginClass(
  newPct: number | null,
  minMargin: number | null,
): string {
  if (newPct == null) return "text-muted-foreground"
  if (minMargin == null || !Number.isFinite(minMargin)) return "text-foreground"
  if (newPct < minMargin) return "text-red-600 dark:text-red-400"
  return "text-green-600 dark:text-green-500"
}

const statusBadgeClass: Record<string, string> = {
  LOW: "bg-red-500/15 text-red-800 dark:text-red-200 border-red-500/40",
  PLACEHOLDER_PRICE: "bg-yellow-500/15 text-yellow-800 dark:text-yellow-200 border-yellow-500/40",
  NO_STOCK: "bg-muted text-muted-foreground border-border",
}

function StatusBadge({ status }: { status: string | null }) {
  const key = (status ?? "").trim()
  const cls =
    statusBadgeClass[key] ?? "bg-muted/80 text-muted-foreground border-border"
  return (
    <Badge variant="outline" className={cn("text-xs", cls)}>
      {key || "—"}
    </Badge>
  )
}

function rowStatusClass(status: string | null): string {
  const s = (status ?? "").trim()
  if (s === "LOW") return "bg-red-500/[0.07] dark:bg-red-950/25"
  if (s === "PLACEHOLDER_PRICE") return "bg-amber-400/[0.12] dark:bg-amber-950/20"
  if (s === "NO_STOCK") return "bg-muted/60 dark:bg-muted/30"
  return ""
}

/** Color del margen % según status (no según signo del valor). */
function marginPercentClassByStatus(status: string | null): string {
  const s = (status ?? "").trim()
  if (s === "LOW") return "text-red-600 dark:text-red-400"
  if (s === "PLACEHOLDER_PRICE") return "text-amber-600 dark:text-amber-500"
  if (s === "OK") return "text-green-600 dark:text-green-500"
  return "text-foreground"
}

const STATUS_FILTER_OPTIONS = [
  { value: "TODOS", label: "Todos" },
  { value: "LOW", label: "LOW" },
  { value: "PLACEHOLDER_PRICE", label: "PLACEHOLDER" },
  { value: "OK", label: "OK" },
  { value: "NO_STOCK", label: "NO_STOCK" },
  { value: "NO_COST", label: "NO_COST" },
  { value: "NO_RULE", label: "NO_RULE" },
] as const

type TypeOption = { key: string; label: string }

function ProductTypeMultiSelect({
  options,
  selectedKeys,
  onChange,
  disabled,
}: {
  options: TypeOption[]
  selectedKeys: string[]
  onChange: (keys: string[]) => void
  disabled?: boolean
}) {
  const [open, setOpen] = useState(false)
  const selectedSet = useMemo(() => new Set(selectedKeys), [selectedKeys])
  const allKeys = useMemo(() => options.map((o) => o.key), [options])

  const toggle = useCallback(
    (key: string) => {
      const next = new Set(selectedKeys)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      onChange([...next])
    },
    [selectedKeys, onChange],
  )

  const selectAll = useCallback(() => {
    onChange([...allKeys])
  }, [allKeys, onChange])

  const clear = useCallback(() => {
    onChange([])
  }, [onChange])

  const summary =
    selectedKeys.length === 0
      ? "Todos los tipos"
      : selectedKeys.length === 1
        ? options.find((o) => o.key === selectedKeys[0])?.label ?? "1 tipo"
        : `${selectedKeys.length} tipos`

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          type="button"
          variant="outline"
          disabled={disabled}
          className="h-8 min-w-[10rem] justify-between px-2 text-xs font-normal"
        >
          <span className="truncate">{summary}</span>
          <ChevronDown className="size-3.5 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-64 p-0" align="start">
        <div className="flex gap-1 border-b border-border p-1.5">
          <Button type="button" variant="ghost" size="sm" className="h-7 flex-1 text-xs" onClick={selectAll}>
            Seleccionar todos
          </Button>
          <Button type="button" variant="ghost" size="sm" className="h-7 flex-1 text-xs" onClick={clear}>
            Limpiar
          </Button>
        </div>
        <div className="max-h-56 overflow-y-auto p-1.5">
          {options.length === 0 ? (
            <p className="px-2 py-2 text-xs text-muted-foreground">Sin tipos en los datos</p>
          ) : (
            <ul className="space-y-1">
              {options.map((o) => (
                <li key={o.key}>
                  <label className="flex cursor-pointer items-center gap-2 rounded px-1.5 py-1 text-xs hover:bg-muted/80">
                    <Checkbox
                      checked={selectedSet.has(o.key)}
                      onCheckedChange={() => toggle(o.key)}
                    />
                    <span className="leading-tight">{o.label}</span>
                  </label>
                </li>
              ))}
            </ul>
          )}
        </div>
      </PopoverContent>
    </Popover>
  )
}

export default function MarginsPage() {
  const [companies, setCompanies] = useState<Company[]>([])
  const [companyId, setCompanyId] = useState<string>("")
  const [priceListOptions, setPriceListOptions] = useState<PriceListRef[]>([])
  const [priceListId, setPriceListId] = useState<string>("")
  const [rows, setRows] = useState<MarginAnalysisViewRow[]>([])
  const [selectedTypeKeys, setSelectedTypeKeys] = useState<string[]>([])
  const [statusFilter, setStatusFilter] = useState<string>("TODOS")
  const [problemsOnly, setProblemsOnly] = useState(true)
  const [listsLoading, setListsLoading] = useState(false)
  const [marginsLoading, setMarginsLoading] = useState(false)
  const [companiesLoading, setCompaniesLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [searchInput, setSearchInput] = useState("")
  const [debouncedSearch, setDebouncedSearch] = useState("")
  /** Solo claves explícitamente editadas o sugeridas (no muta `rows`). */
  const [editedPrices, setEditedPrices] = useState<Record<string, number>>({})

  useEffect(() => {
    const t = window.setTimeout(() => setDebouncedSearch(searchInput), 300)
    return () => window.clearTimeout(t)
  }, [searchInput])

  useEffect(() => {
    setEditedPrices({})
  }, [companyId, priceListId])

  useEffect(() => {
    let cancelled = false
    ;(async () => {
      setCompaniesLoading(true)
      try {
        const list = await getCompanies()
        if (cancelled) return
        setCompanies(list)
        const stored =
          typeof window !== "undefined" ? localStorage.getItem("company_id") : null
        const parsed = stored ? parseInt(stored, 10) : NaN
        const defaultId =
          list.find((c) => c.company_id === parsed)?.company_id ?? list[0]?.company_id
        if (defaultId != null) {
          setCompanyId(String(defaultId))
        }
      } catch {
        if (!cancelled) setError("Error al cargar empresas")
      } finally {
        if (!cancelled) setCompaniesLoading(false)
      }
    })()
    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    if (!companyId) {
      setPriceListOptions([])
      setPriceListId("")
      setRows([])
      setListsLoading(false)
      return
    }
    const cid = parseInt(companyId, 10)
    if (Number.isNaN(cid)) return

    let cancelled = false
    setListsLoading(true)
    setError(null)
    ;(async () => {
      try {
        const lists = await getPriceLists(cid)
        if (cancelled) return
        setPriceListOptions(lists)
        if (lists.length > 0) {
          setPriceListId(String(lists[0].id))
        } else {
          setPriceListId("")
          setRows([])
        }
      } catch {
        if (!cancelled) {
          setError("Error al cargar listas de precios")
          setPriceListOptions([])
          setPriceListId("")
          setRows([])
        }
      } finally {
        if (!cancelled) setListsLoading(false)
      }
    })()
    return () => {
      cancelled = true
    }
  }, [companyId])

  useEffect(() => {
    setSelectedTypeKeys([])
  }, [priceListId])

  useEffect(() => {
    const cid = parseInt(companyId, 10)
    if (!companyId || Number.isNaN(cid)) {
      setRows([])
      setMarginsLoading(false)
      return
    }
    const plid = parseInt(priceListId, 10)
    if (!priceListId || Number.isNaN(plid)) {
      setRows([])
      setMarginsLoading(false)
      return
    }

    let cancelled = false
    setMarginsLoading(true)
    setError(null)
    ;(async () => {
      try {
        const data = await getMarginAnalysisView(cid, plid)
        if (cancelled) return
        setRows(Array.isArray(data) ? data : [])
      } catch {
        if (!cancelled) {
          setError("Error al cargar análisis de márgenes")
          setRows([])
        }
      } finally {
        if (!cancelled) setMarginsLoading(false)
      }
    })()
    return () => {
      cancelled = true
    }
  }, [companyId, priceListId])

  const productTypeOptions = useMemo(() => {
    const seen = new Set<string>()
    const opts: TypeOption[] = []
    for (const r of rows) {
      const id = r.product_type_id
      const key = id == null || id === undefined ? PRODUCT_TYPE_NULL : String(id)
      if (seen.has(key)) continue
      seen.add(key)
      const label =
        (r.product_type_name && String(r.product_type_name).trim()) ||
        (id == null ? "Sin tipo" : `Tipo #${id}`)
      opts.push({ key, label })
    }
    opts.sort((a, b) => a.label.localeCompare(b.label, "es"))
    return opts
  }, [rows])

  const optionKeysSet = useMemo(() => new Set(productTypeOptions.map((o) => o.key)), [productTypeOptions])

  useEffect(() => {
    setSelectedTypeKeys((prev) => prev.filter((k) => optionKeysSet.has(k)))
  }, [optionKeysSet])

  const filteredRows = useMemo(() => {
    let out = rows
    if (problemsOnly) {
      out = out.filter((r) => PROBLEM_STATUSES.has((r.status ?? "").trim()))
    }
    if (selectedTypeKeys.length > 0) {
      const want = new Set(selectedTypeKeys)
      out = out.filter((r) => {
        const key = r.product_type_id == null ? PRODUCT_TYPE_NULL : String(r.product_type_id)
        return want.has(key)
      })
    }
    if (statusFilter !== "TODOS") {
      out = out.filter((r) => (r.status ?? "").trim() === statusFilter)
    }
    const q = debouncedSearch.trim().toLowerCase()
    if (q) {
      out = out.filter((r) => {
        const pn = (r.product_name ?? "").toLowerCase()
        const vn = (r.variant_name ?? "").toLowerCase()
        return pn.includes(q) || vn.includes(q)
      })
    }
    return out
  }, [rows, problemsOnly, selectedTypeKeys, statusFilter, debouncedSearch])

  const stats = useMemo(() => {
    const low = filteredRows.filter((r) => (r.status ?? "").trim() === "LOW").length
    const ph = filteredRows.filter((r) => (r.status ?? "").trim() === "PLACEHOLDER_PRICE").length
    return { low, placeholder: ph, total: filteredRows.length }
  }, [filteredRows])

  const rowByKey = useMemo(() => {
    const m = new Map<string, MarginAnalysisViewRow>()
    for (const r of rows) {
      m.set(rowKey(r), r)
    }
    return m
  }, [rows])

  const editedCount = useMemo(() => Object.keys(editedPrices).length, [editedPrices])

  const lowVisibleCount = useMemo(
    () => filteredRows.filter((r) => (r.status ?? "").trim() === "LOW").length,
    [filteredRows],
  )

  const setPriceForRow = useCallback((key: string, raw: string) => {
    setEditedPrices((prev) => {
      const next = { ...prev }
      if (raw === "" || raw === "-") {
        delete next[key]
        return next
      }
      const n = Number(raw)
      if (!Number.isFinite(n)) return prev
      next[key] = n
      return next
    })
  }, [])

  const suggestRow = useCallback((r: MarginAnalysisViewRow) => {
    const k = rowKey(r)
    const s = suggestedPriceFromRule(num(r.cost), num(r.min_margin_percent))
    if (s == null) return
    setEditedPrices((prev) => ({ ...prev, [k]: s }))
  }, [])

  const suggestAllLowVisible = useCallback(() => {
    setEditedPrices((prev) => {
      const next = { ...prev }
      for (const r of filteredRows) {
        if ((r.status ?? "").trim() !== "LOW") continue
        const k = rowKey(r)
        const s = suggestedPriceFromRule(num(r.cost), num(r.min_margin_percent))
        if (s != null) next[k] = s
      }
      return next
    })
  }, [filteredRows])

  const resetEdits = useCallback(() => setEditedPrices({}), [])

  const exportExcel = useCallback(async () => {
    const keys = Object.keys(editedPrices)
    if (keys.length === 0) return
    const XLSX = await import("xlsx")
    const sheetRows: Record<string, string | number>[] = []
    for (const k of keys) {
      const r = rowByKey.get(k)
      if (!r) continue
      sheetRows.push({
        product_name: r.product_name ?? "",
        variant_name: r.variant_name ?? "",
        sku: r.sku ?? "",
        barcode: r.barcode ?? "",
        price_actual: num(r.price) ?? "",
        nuevo_precio: editedPrices[k],
      })
    }
    if (sheetRows.length === 0) return
    const ws = XLSX.utils.json_to_sheet(sheetRows)
    const wb = XLSX.utils.book_new()
    XLSX.utils.book_append_sheet(wb, ws, "Margenes")
    XLSX.writeFile(wb, `margenes_${companyId}_${priceListId}.xlsx`)
  }, [editedPrices, rowByKey, companyId, priceListId])

  const filterSelectClass =
    "h-8 min-w-[9.5rem] rounded-md border border-input bg-background px-2 text-xs ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"

  const tableBusy = marginsLoading || listsLoading

  if (companiesLoading) {
    return (
      <div className="flex h-full items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    )
  }

  if (error && companies.length === 0) {
    return (
      <div className="flex h-full items-center justify-center">
        <Card className="w-full max-w-md">
          <CardContent className="flex flex-col items-center py-8">
            <AlertTriangle className="mb-4 h-12 w-12 text-destructive" />
            <p className="text-center text-muted-foreground">{error}</p>
          </CardContent>
        </Card>
      </div>
    )
  }

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-xl font-bold text-foreground">Análisis de márgenes</h1>
        <p className="text-xs text-muted-foreground">Revisión rápida de precios y márgenes por lista</p>
      </div>

      <Card className="py-0">
        <CardHeader className="flex flex-row items-center justify-between space-y-0 border-b border-border py-2.5">
          <CardTitle className="flex items-center gap-2 text-sm font-semibold">
            <Percent className="size-4 text-primary" />
            Filtros
          </CardTitle>
          <div className="flex items-center gap-2">
            <Switch
              id="problems-only"
              checked={problemsOnly}
              onCheckedChange={setProblemsOnly}
            />
            <Label htmlFor="problems-only" className="cursor-pointer text-xs font-medium">
              Solo problemas
            </Label>
          </div>
        </CardHeader>
        <CardContent className="flex flex-wrap items-end gap-2 py-2.5">
          <div className="flex flex-col gap-0.5">
            <label htmlFor="margins-company" className="text-[11px] font-medium text-muted-foreground">
              Empresa
            </label>
            <select
              id="margins-company"
              className={filterSelectClass}
              value={companyId}
              onChange={(e) => {
                setCompanyId(e.target.value)
                setPriceListId("")
                setPriceListOptions([])
                setSelectedTypeKeys([])
                setSearchInput("")
                setDebouncedSearch("")
                setEditedPrices({})
                setRows([])
              }}
            >
              <option value="">—</option>
              {companies.map((c) => (
                <option key={c.company_id} value={String(c.company_id)}>
                  {c.name} ({c.company_id})
                </option>
              ))}
            </select>
          </div>

          <div className="flex flex-col gap-0.5">
            <label htmlFor="margins-price-list" className="text-[11px] font-medium text-muted-foreground">
              Lista de precios
            </label>
            <select
              id="margins-price-list"
              className={filterSelectClass}
              value={priceListId}
              onChange={(e) => setPriceListId(e.target.value)}
              disabled={!companyId || listsLoading || priceListOptions.length === 0}
            >
              {priceListOptions.length === 0 && companyId ? (
                <option value="">Sin listas activas</option>
              ) : null}
              {priceListOptions.map((pl) => (
                <option key={pl.id} value={String(pl.id)}>
                  {pl.name} ({pl.id})
                </option>
              ))}
            </select>
          </div>

          <div className="flex flex-col gap-0.5">
            <span className="text-[11px] font-medium text-muted-foreground">Tipo de producto</span>
            <ProductTypeMultiSelect
              options={productTypeOptions}
              selectedKeys={selectedTypeKeys}
              onChange={setSelectedTypeKeys}
              disabled={marginsLoading || rows.length === 0}
            />
          </div>

          <div className="flex flex-col gap-0.5">
            <label htmlFor="margins-status" className="text-[11px] font-medium text-muted-foreground">
              Estado
            </label>
            <select
              id="margins-status"
              className={filterSelectClass}
              value={statusFilter}
              onChange={(e) => setStatusFilter(e.target.value)}
            >
              {STATUS_FILTER_OPTIONS.map((o) => (
                <option key={o.value} value={o.value}>
                  {o.label}
                </option>
              ))}
            </select>
          </div>
        </CardContent>
      </Card>

      <Card className="py-0">
        <CardHeader className="border-b border-border py-2">
          <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
            <CardTitle className="text-sm font-semibold">Detalle</CardTitle>
            {!tableBusy && companyId && priceListOptions.length > 0 ? (
              <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs">
                <span className="font-medium text-red-600 dark:text-red-400">
                  🔴 {stats.low} con margen bajo
                </span>
                <span className="font-medium text-amber-700 dark:text-amber-400">
                  🟡 {stats.placeholder} precios placeholder
                </span>
                <span className="font-medium text-foreground">
                  📦 {stats.total} productos visibles
                </span>
              </div>
            ) : null}
          </div>
        </CardHeader>
        <CardContent className="px-2 pb-2 pt-2 sm:px-3">
          {tableBusy ? (
            <div className="flex justify-center py-10">
              <Loader2 className="h-7 w-7 animate-spin text-primary" />
            </div>
          ) : error ? (
            <p className="py-6 text-center text-sm text-destructive">{error}</p>
          ) : !companyId ? (
            <p className="py-6 text-center text-xs text-muted-foreground">Selecciona una empresa</p>
          ) : priceListOptions.length === 0 ? (
            <p className="py-6 text-center text-xs text-muted-foreground">
              No hay listas de precio activas para esta empresa
            </p>
          ) : rows.length === 0 ? (
            <p className="py-6 text-center text-xs text-muted-foreground">Sin datos para esta lista</p>
          ) : (
            <>
              <div className="relative mb-2">
                <Search
                  className="pointer-events-none absolute left-2 top-1/2 size-3.5 -translate-y-1/2 text-muted-foreground"
                  aria-hidden
                />
                <Input
                  type="search"
                  className="h-8 pl-7 pr-8 text-xs"
                  placeholder="Buscar producto o variante..."
                  value={searchInput}
                  onChange={(e) => setSearchInput(e.target.value)}
                  autoComplete="off"
                />
                {searchInput ? (
                  <button
                    type="button"
                    className="absolute right-1 top-1/2 flex size-7 -translate-y-1/2 items-center justify-center rounded-md text-muted-foreground hover:bg-muted hover:text-foreground"
                    onClick={() => {
                      setSearchInput("")
                      setDebouncedSearch("")
                    }}
                    aria-label="Limpiar búsqueda"
                  >
                    <X className="size-3.5" />
                  </button>
                ) : null}
              </div>
              <div className="mb-2 flex flex-wrap items-center gap-2 border-b border-border pb-2">
                <span className="text-xs text-muted-foreground">
                  <span className="font-semibold text-foreground">{editedCount}</span> productos modificados
                </span>
                <Button
                  type="button"
                  variant="secondary"
                  size="sm"
                  className="h-7 text-xs"
                  onClick={suggestAllLowVisible}
                  disabled={lowVisibleCount === 0 || tableBusy}
                >
                  Sugerir todos
                </Button>
                <Button
                  type="button"
                  size="sm"
                  className="h-7 gap-1 text-xs"
                  onClick={() => void exportExcel()}
                  disabled={editedCount === 0 || tableBusy}
                >
                  <FileSpreadsheet className="size-3.5" />
                  Exportar Excel
                </Button>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  className="h-7 text-xs"
                  onClick={resetEdits}
                  disabled={editedCount === 0}
                >
                  Reset
                </Button>
              </div>
              {filteredRows.length === 0 ? (
                <p className="py-6 text-center text-xs text-muted-foreground">
                  Sin filas para los filtros o la búsqueda actuales
                </p>
              ) : (
                <div className="overflow-x-auto rounded-md border border-border">
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="border-b border-border bg-muted/40">
                        <th className="px-2 py-1.5 text-left font-medium text-muted-foreground">Producto</th>
                        <th className="px-2 py-1.5 text-left font-medium text-muted-foreground">Variante</th>
                        <th className="px-2 py-1.5 text-left font-medium text-muted-foreground">SKU</th>
                        <th className="px-2 py-1.5 text-right font-medium text-muted-foreground">Stock</th>
                        <th className="px-2 py-1.5 text-right font-medium text-muted-foreground">Costo</th>
                        <th className="px-2 py-1.5 text-right font-medium text-muted-foreground">Precio</th>
                        <th className="px-2 py-1.5 text-right font-medium text-muted-foreground">Margen %</th>
                        <th className="px-2 py-1.5 text-right font-medium text-muted-foreground">Mín. %</th>
                        <th className="px-2 py-1.5 text-right font-medium text-muted-foreground">Δ vs regla</th>
                        <th className="px-2 py-1.5 text-center font-medium text-muted-foreground">
                          Nuevo precio
                        </th>
                        <th className="px-2 py-1.5 text-right font-medium text-muted-foreground">
                          Nuevo margen %
                        </th>
                        <th className="px-2 py-1.5 text-center font-medium text-muted-foreground">Estado</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredRows.map((r) => {
                        const st = num(r.stock_quantity)
                        const lowStock = st !== null && st < 3 && st >= 0
                        const mp = num(r.margin_percent)
                        const k = rowKey(r)
                        const editedVal = editedPrices[k]
                        const isEdited = editedVal !== undefined
                        const suggestVal = suggestedPriceFromRule(num(r.cost), num(r.min_margin_percent))
                        const previewPct = isEdited
                          ? newMarginPercentPreview(num(r.cost), editedVal)
                          : null
                        const minM = num(r.min_margin_percent)
                        const currentPrice = num(r.price)
                        return (
                          <tr
                            key={k}
                            className={cn(
                              "border-b border-border last:border-0",
                              rowStatusClass(r.status),
                              "hover:bg-muted/40",
                            )}
                          >
                            <td className="max-w-[12rem] truncate px-2 py-1">{r.product_name ?? "—"}</td>
                            <td className="max-w-[10rem] truncate px-2 py-1 text-muted-foreground">
                              {r.variant_name ?? "—"}
                            </td>
                            <td className="whitespace-nowrap px-2 py-1 font-mono">{r.sku ?? "—"}</td>
                            <td
                              className={cn(
                                "whitespace-nowrap px-2 py-1 text-right tabular-nums",
                                lowStock && "font-medium text-orange-600 dark:text-orange-400",
                              )}
                            >
                              {st ?? "—"}
                            </td>
                            <td className="whitespace-nowrap px-2 py-1 text-right tabular-nums">
                              {formatMoney(num(r.cost))}
                            </td>
                            <td className="whitespace-nowrap px-2 py-1 text-right tabular-nums">
                              {formatMoney(currentPrice)}
                            </td>
                            <td
                              className={cn(
                                "whitespace-nowrap px-2 py-1 text-right tabular-nums font-medium",
                                marginPercentClassByStatus(r.status),
                              )}
                            >
                              {formatPct(mp)}
                            </td>
                            <td className="whitespace-nowrap px-2 py-1 text-right tabular-nums text-muted-foreground">
                              {formatPct(minM)}
                            </td>
                            <td className="whitespace-nowrap px-2 py-1 text-right tabular-nums text-muted-foreground">
                              {formatMarginDiff(num(r.margin_diff))}
                            </td>
                            <td
                              className={cn(
                                "px-1 py-0.5 align-middle",
                                isEdited && "bg-blue-500/15 dark:bg-blue-950/30",
                              )}
                            >
                              <div className="flex flex-col items-stretch gap-0.5">
                                <div className="flex items-center gap-0.5">
                                  {isEdited ? (
                                    <PencilLine
                                      className="size-3 shrink-0 text-blue-600 dark:text-blue-400"
                                      aria-hidden
                                    />
                                  ) : null}
                                  <Input
                                    type="number"
                                    inputMode="numeric"
                                    min={0}
                                    step={1}
                                    className="h-7 min-w-[4.5rem] flex-1 px-1.5 text-right text-xs tabular-nums"
                                    placeholder={
                                      currentPrice != null ? String(Math.round(currentPrice)) : ""
                                    }
                                    value={isEdited ? String(editedVal) : ""}
                                    onChange={(e) => setPriceForRow(k, e.target.value)}
                                  />
                                </div>
                                <Button
                                  type="button"
                                  variant="ghost"
                                  size="sm"
                                  className="h-6 px-1 text-[10px] leading-none"
                                  disabled={suggestVal == null}
                                  onClick={() => suggestRow(r)}
                                >
                                  Sugerir
                                </Button>
                              </div>
                            </td>
                            <td
                              className={cn(
                                "whitespace-nowrap px-2 py-1 text-right tabular-nums font-medium",
                                isEdited ? previewMarginClass(previewPct, minM) : "text-muted-foreground",
                              )}
                            >
                              {isEdited ? formatPct(previewPct) : "—"}
                            </td>
                            <td className="px-2 py-1 text-center">
                              <StatusBadge status={r.status} />
                            </td>
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
