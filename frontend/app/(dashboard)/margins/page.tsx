"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { AlertTriangle, Loader2, Percent } from "lucide-react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import {
  getCompanies,
  getMarginAnalysisView,
  type Company,
  type MarginAnalysisViewRow,
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

type PriceListOption = { id: number; name: string }

export default function MarginsPage() {
  const [companies, setCompanies] = useState<Company[]>([])
  const [companyId, setCompanyId] = useState<string>("")
  const [priceListId, setPriceListId] = useState<string>("all")
  const [priceListOptions, setPriceListOptions] = useState<PriceListOption[]>([])
  const [rows, setRows] = useState<MarginAnalysisViewRow[]>([])
  const [statusFilter, setStatusFilter] = useState<string>("TODOS")
  const [loading, setLoading] = useState(false)
  const [companiesLoading, setCompaniesLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const buildPriceListOptions = useCallback((data: MarginAnalysisViewRow[]) => {
    const map = new Map<number, string>()
    for (const r of data) {
      const id = r.price_list_id
      if (id == null) continue
      const label = (r.price_list_name ?? "").trim() || `Lista #${id}`
      if (!map.has(id)) map.set(id, label)
    }
    return [...map.entries()]
      .map(([id, name]) => ({ id, name }))
      .sort((a, b) => a.name.localeCompare(b.name, "es"))
  }, [])

  const loadRows = useCallback(
    async (cid: number, listId: "all" | number) => {
      setLoading(true)
      setError(null)
      try {
        const data = await getMarginAnalysisView(
          cid,
          listId === "all" ? null : listId,
        )
        setRows(Array.isArray(data) ? data : [])
        if (listId === "all") {
          setPriceListOptions(buildPriceListOptions(data))
        }
      } catch {
        setError("Error al cargar análisis de márgenes")
        setRows([])
      } finally {
        setLoading(false)
      }
    },
    [buildPriceListOptions],
  )

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
    const cid = parseInt(companyId, 10)
    if (!companyId || Number.isNaN(cid)) {
      setRows([])
      setPriceListOptions([])
      setLoading(false)
      return
    }
    const listId = priceListId === "all" ? "all" : parseInt(priceListId, 10)
    if (priceListId !== "all" && Number.isNaN(listId)) return
    loadRows(cid, listId === "all" || priceListId === "all" ? "all" : listId)
  }, [companyId, priceListId, loadRows])

  const filteredRows = useMemo(() => {
    if (statusFilter === "TODOS") return rows
    if (statusFilter === "LOW") return rows.filter((r) => r.status === "LOW")
    if (statusFilter === "PLACEHOLDER_PRICE") {
      return rows.filter((r) => r.status === "PLACEHOLDER_PRICE")
    }
    return rows
  }, [rows, statusFilter])

  const selectClass =
    "h-10 w-full max-w-xs rounded-md border border-input bg-background px-3 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring sm:w-auto"

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
          Vista por empresa y lista de precios (sin edición)
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Percent className="h-5 w-5 text-primary" />
            Filtros
          </CardTitle>
          <CardDescription>Empresa, lista de precios y estado de análisis</CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-4 sm:flex-row sm:flex-wrap sm:items-end">
          <div className="flex flex-col gap-1.5">
            <label htmlFor="margins-company" className="text-xs font-medium text-muted-foreground">
              Empresa (company_id)
            </label>
            <select
              id="margins-company"
              className={selectClass}
              value={companyId}
              onChange={(e) => {
                setCompanyId(e.target.value)
                setPriceListId("all")
                setPriceListOptions([])
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
              Lista de precios (price_list_id)
            </label>
            <select
              id="margins-price-list"
              className={selectClass}
              value={priceListId}
              onChange={(e) => setPriceListId(e.target.value)}
              disabled={!companyId}
            >
              <option value="all">Todas</option>
              {priceListOptions.map((pl) => (
                <option key={pl.id} value={String(pl.id)}>
                  {pl.name} ({pl.id})
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
              <option value="TODOS">TODOS</option>
              <option value="LOW">LOW</option>
              <option value="PLACEHOLDER_PRICE">PLACEHOLDER_PRICE</option>
            </select>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Detalle ({filteredRows.length})</CardTitle>
          <CardDescription>
            GET /margin-analysis-view — columnas de la vista SQL
          </CardDescription>
        </CardHeader>
        <CardContent>
          {loading ? (
            <div className="flex justify-center py-12">
              <Loader2 className="h-8 w-8 animate-spin text-primary" />
            </div>
          ) : error ? (
            <p className="py-8 text-center text-destructive">{error}</p>
          ) : !companyId ? (
            <p className="py-8 text-center text-muted-foreground">Selecciona una empresa</p>
          ) : filteredRows.length === 0 ? (
            <p className="py-8 text-center text-muted-foreground">Sin filas para los filtros actuales</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-border">
                    <th className="pb-3 text-left text-sm font-medium text-muted-foreground">
                      product_name
                    </th>
                    <th className="pb-3 text-left text-sm font-medium text-muted-foreground">
                      variant_name
                    </th>
                    <th className="pb-3 text-left text-sm font-medium text-muted-foreground">sku</th>
                    <th className="pb-3 text-right text-sm font-medium text-muted-foreground">
                      stock_quantity
                    </th>
                    <th className="pb-3 text-right text-sm font-medium text-muted-foreground">cost</th>
                    <th className="pb-3 text-right text-sm font-medium text-muted-foreground">price</th>
                    <th className="pb-3 text-right text-sm font-medium text-muted-foreground">
                      margin_percent
                    </th>
                    <th className="pb-3 text-right text-sm font-medium text-muted-foreground">
                      min_margin_percent
                    </th>
                    <th className="pb-3 text-center text-sm font-medium text-muted-foreground">status</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredRows.map((r) => (
                    <tr
                      key={`${r.variant_id}-${r.price_list_id}`}
                      className="border-b border-border last:border-0 hover:bg-muted/50"
                    >
                      <td className="py-3 text-sm">{r.product_name ?? "—"}</td>
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
