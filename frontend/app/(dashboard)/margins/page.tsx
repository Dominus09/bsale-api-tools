"use client"

import { useEffect, useMemo, useState } from "react"
import { AlertTriangle, Loader2, Percent } from "lucide-react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import {
  getCompanies,
  getMarginAnalysisView,
  getPriceLists,
  type Company,
  type MarginAnalysisViewRow,
  type PriceListRef,
} from "@/lib/api"

function num(v: number | string | null | undefined): number | null {
  if (v === null || v === undefined || v === "") return null
  const n = typeof v === "number" ? v : Number(v)
  return Number.isFinite(n) ? n : null
}

function formatMoney(value: number | null) {
  if (value === null) return "—"
  return new Intl.NumberFormat("es-CL", {
    style: "currency",
    currency: "CLP",
    minimumFractionDigits: 0,
  }).format(value)
}

function formatPct(value: number | null) {
  if (value === null) return "—"
  return `${value.toFixed(2)}%`
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
    <Badge variant="outline" className={cls}>
      {key || "—"}
    </Badge>
  )
}

/** Valores de status en bsale.margin_analysis_view */
const STATUS_FILTER_OPTIONS = [
  { value: "TODOS", label: "TODOS" },
  { value: "LOW", label: "LOW" },
  { value: "PLACEHOLDER_PRICE", label: "PLACEHOLDER_PRICE" },
  { value: "OK", label: "OK" },
  { value: "NO_STOCK", label: "NO_STOCK" },
  { value: "NO_COST", label: "NO_COST" },
  { value: "NO_RULE", label: "NO_RULE" },
] as const

const PRODUCT_TYPE_ALL = "all"
const PRODUCT_TYPE_NULL = "__null__"

export default function MarginsPage() {
  const [companies, setCompanies] = useState<Company[]>([])
  const [companyId, setCompanyId] = useState<string>("")
  const [priceListOptions, setPriceListOptions] = useState<PriceListRef[]>([])
  const [priceListId, setPriceListId] = useState<string>("")
  const [rows, setRows] = useState<MarginAnalysisViewRow[]>([])
  const [productTypeFilter, setProductTypeFilter] = useState<string>(PRODUCT_TYPE_ALL)
  const [statusFilter, setStatusFilter] = useState<string>("TODOS")
  const [listsLoading, setListsLoading] = useState(false)
  const [marginsLoading, setMarginsLoading] = useState(false)
  const [companiesLoading, setCompaniesLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

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
    setProductTypeFilter(PRODUCT_TYPE_ALL)
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
    const opts: { key: string; label: string }[] = []
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

  const filteredRows = useMemo(() => {
    let out = rows
    if (productTypeFilter !== PRODUCT_TYPE_ALL) {
      if (productTypeFilter === PRODUCT_TYPE_NULL) {
        out = out.filter((r) => r.product_type_id == null)
      } else {
        const want = parseInt(productTypeFilter, 10)
        out = out.filter((r) => num(r.product_type_id) === want)
      }
    }
    if (statusFilter !== "TODOS") {
      out = out.filter((r) => r.status === statusFilter)
    }
    return out
  }, [rows, productTypeFilter, statusFilter])

  const selectClass =
    "h-10 w-full max-w-xs rounded-md border border-input bg-background px-3 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring sm:w-auto"

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
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-foreground">Análisis de márgenes</h1>
        <p className="text-muted-foreground">
          Por empresa, lista activa y tipo de producto (vista SQL)
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Percent className="h-5 w-5 text-primary" />
            Filtros
          </CardTitle>
          <CardDescription>
            Empresa → lista de precios (activas) → tipo de producto → estado
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-4 sm:flex-row sm:flex-wrap sm:items-end">
          <div className="flex flex-col gap-1.5">
            <label htmlFor="margins-company" className="text-xs font-medium text-muted-foreground">
              Empresa
            </label>
            <select
              id="margins-company"
              className={selectClass}
              value={companyId}
              onChange={(e) => {
                setCompanyId(e.target.value)
                setPriceListId("")
                setPriceListOptions([])
                setProductTypeFilter(PRODUCT_TYPE_ALL)
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

          <div className="flex flex-col gap-1.5">
            <label htmlFor="margins-price-list" className="text-xs font-medium text-muted-foreground">
              Lista de precios
            </label>
            <select
              id="margins-price-list"
              className={selectClass}
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

          <div className="flex flex-col gap-1.5">
            <label htmlFor="margins-product-type" className="text-xs font-medium text-muted-foreground">
              Tipo de producto
            </label>
            <select
              id="margins-product-type"
              className={selectClass}
              value={productTypeFilter}
              onChange={(e) => setProductTypeFilter(e.target.value)}
              disabled={marginsLoading}
            >
              <option value={PRODUCT_TYPE_ALL}>Todos</option>
              {productTypeOptions.map((o) => (
                <option key={o.key} value={o.key}>
                  {o.label}
                </option>
              ))}
            </select>
          </div>

          <div className="flex flex-col gap-1.5">
            <label htmlFor="margins-status" className="text-xs font-medium text-muted-foreground">
              Estado
            </label>
            <select
              id="margins-status"
              className={selectClass}
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

      <Card>
        <CardHeader>
          <CardTitle>Detalle ({filteredRows.length})</CardTitle>
          <CardDescription>GET /margin-analysis-view</CardDescription>
        </CardHeader>
        <CardContent>
          {tableBusy ? (
            <div className="flex justify-center py-12">
              <Loader2 className="h-8 w-8 animate-spin text-primary" />
            </div>
          ) : error ? (
            <p className="py-8 text-center text-destructive">{error}</p>
          ) : !companyId ? (
            <p className="py-8 text-center text-muted-foreground">Selecciona una empresa</p>
          ) : priceListOptions.length === 0 ? (
            <p className="py-8 text-center text-muted-foreground">
              No hay listas de precio activas (state = 0) para esta empresa
            </p>
          ) : filteredRows.length === 0 ? (
            <p className="py-8 text-center text-muted-foreground">Sin filas para los filtros actuales</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-border">
                    <th className="pb-3 text-left text-sm font-medium text-muted-foreground">
                      Producto
                    </th>
                    <th className="pb-3 text-left text-sm font-medium text-muted-foreground">
                      Tipo de producto
                    </th>
                    <th className="pb-3 text-left text-sm font-medium text-muted-foreground">
                      Variante
                    </th>
                    <th className="pb-3 text-left text-sm font-medium text-muted-foreground">SKU</th>
                    <th className="pb-3 text-right text-sm font-medium text-muted-foreground">Stock</th>
                    <th className="pb-3 text-right text-sm font-medium text-muted-foreground">Costo</th>
                    <th className="pb-3 text-right text-sm font-medium text-muted-foreground">Precio</th>
                    <th className="pb-3 text-right text-sm font-medium text-muted-foreground">
                      Margen %
                    </th>
                    <th className="pb-3 text-right text-sm font-medium text-muted-foreground">
                      Mín. %
                    </th>
                    <th className="pb-3 text-center text-sm font-medium text-muted-foreground">Estado</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredRows.map((r) => (
                    <tr
                      key={`${r.variant_id}-${r.price_list_id}`}
                      className="border-b border-border last:border-0 hover:bg-muted/50"
                    >
                      <td className="py-3 text-sm">{r.product_name ?? "—"}</td>
                      <td className="py-3 text-sm text-muted-foreground">
                        {r.product_type_name?.trim() ||
                          (r.product_type_id != null ? `Tipo #${r.product_type_id}` : "—")}
                      </td>
                      <td className="py-3 text-sm text-muted-foreground">{r.variant_name ?? "—"}</td>
                      <td className="py-3 font-mono text-sm">{r.sku ?? "—"}</td>
                      <td className="py-3 text-right text-sm">{num(r.stock_quantity) ?? "—"}</td>
                      <td className="py-3 text-right text-sm">{formatMoney(num(r.cost))}</td>
                      <td className="py-3 text-right text-sm">{formatMoney(num(r.price))}</td>
                      <td className="py-3 text-right text-sm">{formatPct(num(r.margin_percent))}</td>
                      <td className="py-3 text-right text-sm">
                        {formatPct(num(r.min_margin_percent))}
                      </td>
                      <td className="py-3 text-center">
                        <StatusBadge status={r.status} />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
