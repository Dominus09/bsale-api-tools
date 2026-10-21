"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { AlertTriangle, Download, Info, Loader2, Search } from "lucide-react"

import {
  AnalyticPageHeader,
  AnalyticPageShell,
  AnalyticsLoading,
  EmptyState,
  ProfessionalDataTable,
  SmartFilterBar,
  StatusBadge,
} from "@/components/analytics-v2/shared/analytics-ui"
import { Pager } from "@/components/analytics-v2/costs/cost-v2-products"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { getStoredCompanyId } from "@/lib/api"
import { AnalyticsV2Error, downloadXlsx, getCatalogRows } from "@/lib/analytics-v2/api"
import type { CatalogMeta, CatalogRow } from "@/lib/analytics-v2/catalog-types"
import { formatDateTime, formatMoney, formatQty } from "@/lib/analytics-v2/labels"
import { cn } from "@/lib/utils"

const selectClass =
  "h-10 rounded-lg border border-lq-line bg-lq-surface px-3 text-sm text-lq-ink shadow-lq-xs outline-none focus:border-primary/50 focus:ring-2 focus:ring-primary/15"
const labelClass = "flex flex-col gap-1.5 text-xs font-medium text-lq-body"

const TAX_FILTERS = [
  { value: "all", label: "Todos los impuestos" },
  { value: "iva", label: "Con IVA" },
  { value: "ila", label: "Con ILA" },
  { value: "sin_identificar", label: "Impuestos sin identificar" },
  { value: "sin_configuracion", label: "Sin configuración en Bsale" },
  { value: "revision", label: "Pendientes de revisión" },
]

function taxLabel(row: CatalogRow): string {
  if (!row.taxes.length) {
    return row.tax_profile.status === "sin_impuestos" ? "Sin configuración en Bsale" : "—"
  }
  return row.taxes
    .map((t) => `${t.name}${t.percentage != null ? ` ${t.percentage}%` : ""}`)
    .join(" · ")
}

export default function CatalogoMaestroPage() {
  const [companyId, setCompanyId] = useState<number | null>(null)
  const [meta, setMeta] = useState<CatalogMeta | null>(null)
  const [rows, setRows] = useState<CatalogRow[]>([])
  const [total, setTotal] = useState(0)
  const [totalPages, setTotalPages] = useState(1)
  const [loading, setLoading] = useState(false)
  const [downloading, setDownloading] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [blocking, setBlocking] = useState<string | null>(null)

  const [q, setQ] = useState("")
  const [productTypeId, setProductTypeId] = useState<string>("")
  const [state, setState] = useState("all")
  const [cost, setCost] = useState("all")
  const [stock, setStock] = useState("all")
  const [tax, setTax] = useState("all")
  const [priceListId, setPriceListId] = useState<string>("")
  const [pricePresence, setPricePresence] = useState<string>("")
  const [sort, setSort] = useState("product")
  const [page, setPage] = useState(1)
  const [visibleLists, setVisibleLists] = useState<number[]>([])
  const abortRef = useRef<AbortController | null>(null)

  useEffect(() => {
    setCompanyId(getStoredCompanyId())
  }, [])

  const filters = useMemo(
    () => ({
      q: q.trim() || null,
      product_type_id: productTypeId ? Number(productTypeId) : null,
      state,
      cost,
      stock,
      tax,
      price_list_id: priceListId ? Number(priceListId) : null,
      price_presence: pricePresence || null,
      sort,
    }),
    [q, productTypeId, state, cost, stock, tax, priceListId, pricePresence, sort],
  )

  const load = useCallback(() => {
    if (companyId == null) return
    abortRef.current?.abort()
    const ac = new AbortController()
    abortRef.current = ac
    setLoading(true)
    setError(null)
    getCatalogRows(
      {
        company_id: companyId,
        ...filters,
        price_list_ids: visibleLists.length ? visibleLists.join(",") : null,
        page,
        page_size: 50,
      },
      ac.signal,
    )
      .then((d) => {
        setRows(d.items)
        setTotal(d.total)
        setTotalPages(d.total_pages)
        setMeta(d.meta)
        setBlocking(null)
        if (!visibleLists.length && d.price_list_ids.length) {
          const preferred = [12, 16, 13].filter((id) => d.price_list_ids.includes(id))
          setVisibleLists(preferred.length ? preferred : d.price_list_ids.slice(0, 3))
        }
      })
      .catch((e: unknown) => {
        if (ac.signal.aborted) return
        if (e instanceof AnalyticsV2Error && [401, 403, 409].includes(e.status)) setBlocking(e.message)
        else setError(e instanceof Error ? e.message : "Error al cargar el catálogo")
      })
      .finally(() => {
        if (!ac.signal.aborted) setLoading(false)
      })
  }, [companyId, filters, page, visibleLists])

  useEffect(() => {
    load()
    return () => abortRef.current?.abort()
  }, [load])

  useEffect(() => {
    setPage(1)
  }, [filters])

  const toggleList = (id: number) => {
    setVisibleLists((prev) => (prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id].sort((a, b) => a - b)))
    setPage(1)
  }

  const listName = (id: number) => meta?.price_lists.find((l) => l.id === id)?.name ?? `Lista ${id}`

  const exportParams = {
    company_id: companyId ?? 0,
    ...filters,
    price_list_ids: visibleLists.length ? visibleLists.join(",") : null,
  }

  const handleDownload = async (scope: "full" | "filtered") => {
    if (companyId == null) return
    setDownloading(scope)
    setError(null)
    try {
      await downloadXlsx(
        "/analytics-v2/catalog/export.xlsx",
        { ...exportParams, scope },
        `catalogo-maestro-empresa-${companyId}${scope === "filtered" ? "-filtrado" : ""}.xlsx`,
      )
    } catch (e) {
      setError(e instanceof Error ? e.message : "No se pudo generar el Excel")
    } finally {
      setDownloading(null)
    }
  }

  if (companyId == null) {
    return <p className="p-6 text-sm text-muted-foreground">Seleccione una empresa para consultar el Catálogo Maestro.</p>
  }

  return (
    <AnalyticPageShell>
      <AnalyticPageHeader
        eyebrow="Inteligencia comercial"
        title="Catálogo Maestro"
        description={`${meta?.company_name ?? `Empresa ${companyId}`} · Una fila por variante con costo promedio Bsale, precios brutos por lista e impuestos reales sincronizados. Solo consulta y exportación: no modifica datos de Bsale.`}
        updatedAt={
          meta ? (
            <>
              {total.toLocaleString("es-CL")} variantes · Catálogo sincronizado: {formatDateTime(meta.freshness.products ?? null)} ·
              Precios: {formatDateTime(meta.freshness.variant_prices ?? null)} · Costos: {formatDateTime(meta.freshness.variant_costs ?? null)} ·
              Stock: {formatDateTime(meta.freshness.stocks ?? null)}
            </>
          ) : undefined
        }
        actions={
          <>
            <Button variant="outline" size="sm" className="h-10 sm:h-9" onClick={() => handleDownload("filtered")} disabled={downloading != null || loading}>
              {downloading === "filtered" ? <Loader2 className="h-4 w-4 animate-spin" /> : <Download className="h-4 w-4" />}
              Descargar resultados filtrados
            </Button>
            <Button size="sm" className="h-10 sm:h-9" onClick={() => handleDownload("full")} disabled={downloading != null}>
              {downloading === "full" ? <Loader2 className="h-4 w-4 animate-spin" /> : <Download className="h-4 w-4" />}
              Descargar catálogo completo
            </Button>
          </>
        }
      />

      {blocking ? (
        <Alert>
          <Info className="h-4 w-4" />
          <AlertTitle>Catálogo Maestro no disponible para esta empresa</AlertTitle>
          <AlertDescription>
            {blocking} Las empresas sin BSALE RAW completo quedan en estado «Pendiente de habilitación»; no se muestran datos incompletos.
          </AlertDescription>
        </Alert>
      ) : null}

      {error ? (
        <Alert variant="destructive">
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      {!blocking ? (
        <>
          <SmartFilterBar>
            <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
              <label className={labelClass}>
                Buscar
                <span className="relative">
                  <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-lq-faint" />
                  <Input
                    className="h-10 rounded-lg border-lq-line pl-9 shadow-lq-xs"
                    value={q}
                    maxLength={100}
                    onChange={(e) => setQ(e.target.value)}
                    placeholder="Producto, variante, SKU o código de barras"
                  />
                </span>
              </label>
              <label className={labelClass}>
                Tipo de producto
                <select className={selectClass} value={productTypeId} onChange={(e) => setProductTypeId(e.target.value)}>
                  <option value="">Todos los tipos</option>
                  {(meta?.product_types ?? []).map((t) => (
                    <option key={t.id} value={t.id}>
                      {t.name ?? `Tipo ${t.id}`}
                    </option>
                  ))}
                </select>
              </label>
              <label className={labelClass}>
                Estado
                <select className={selectClass} value={state} onChange={(e) => setState(e.target.value)}>
                  <option value="all">Activas e inactivas</option>
                  <option value="active">Solo activas</option>
                  <option value="inactive">Solo inactivas</option>
                </select>
              </label>
              <label className={labelClass}>
                Costo promedio
                <select className={selectClass} value={cost} onChange={(e) => setCost(e.target.value)}>
                  <option value="all">Con y sin costo</option>
                  <option value="with">Con costo</option>
                  <option value="without">Sin costo o costo cero</option>
                </select>
              </label>
              <label className={labelClass}>
                Existencias
                <select className={selectClass} value={stock} onChange={(e) => setStock(e.target.value)}>
                  <option value="all">Todas las variantes</option>
                  <option value="with">Con stock positivo</option>
                  <option value="without">Cero o negativo</option>
                </select>
                <span className="font-normal text-lq-muted">
                  «Todas» incluye las variantes sin stock sincronizado (s/d); esas quedan fuera de los otros dos filtros.
                </span>
              </label>
              <label className={labelClass}>
                Impuestos
                <select className={selectClass} value={tax} onChange={(e) => setTax(e.target.value)}>
                  {TAX_FILTERS.map((t) => (
                    <option key={t.value} value={t.value}>
                      {t.label}
                    </option>
                  ))}
                </select>
              </label>
              <label className={labelClass}>
                Precio en lista
                <span className="flex gap-2">
                  <select className={selectClass} value={priceListId} onChange={(e) => setPriceListId(e.target.value)}>
                    <option value="">Cualquier lista</option>
                    {(meta?.price_lists ?? []).map((l) => (
                      <option key={l.id} value={l.id}>
                        {l.name ?? `Lista ${l.id}`} (#{l.id})
                      </option>
                    ))}
                  </select>
                  <select className={selectClass} value={pricePresence} onChange={(e) => setPricePresence(e.target.value)}>
                    <option value="">—</option>
                    <option value="with">Con precio</option>
                    <option value="without">Sin precio</option>
                  </select>
                </span>
              </label>
              <label className={labelClass}>
                Ordenar por
                <select className={selectClass} value={sort} onChange={(e) => setSort(e.target.value)}>
                  <option value="product">Producto</option>
                  <option value="type_product">Tipo y producto</option>
                  <option value="code">SKU</option>
                  <option value="cost_desc">Costo mayor a menor</option>
                  <option value="cost_asc">Costo menor a mayor</option>
                </select>
              </label>
              <div className={labelClass}>
                <span>Listas visibles y exportadas</span>
                <div className="flex flex-wrap gap-1.5">
                  {(meta?.price_lists ?? []).map((l) => (
                    <button
                      key={l.id}
                      type="button"
                      onClick={() => toggleList(l.id)}
                      aria-pressed={visibleLists.includes(l.id)}
                      className={cn(
                        "rounded-full border px-2.5 py-1.5 text-xs font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/40",
                        visibleLists.includes(l.id)
                          ? "border-primary/40 bg-primary/10 text-primary"
                          : "border-lq-line bg-lq-surface text-lq-muted hover:text-lq-ink",
                      )}
                    >
                      {l.name ?? `Lista ${l.id}`} #{l.id}
                    </button>
                  ))}
                </div>
              </div>
            </div>
          </SmartFilterBar>

          {loading && !rows.length ? <AnalyticsLoading label="Cargando catálogo…" /> : null}

          {(!loading || rows.length > 0) && (
            <section>
              <ProfessionalDataTable loading={loading}>
                <table className="w-full min-w-[1180px] text-sm">
                  <thead className="border-b border-lq-line bg-lq-canvas text-left text-xs text-lq-muted">
                    <tr>
                      <th className="px-4 py-3 font-medium">Tipo</th>
                      <th className="px-4 py-3 font-medium">Producto / variante</th>
                      <th className="px-4 py-3 font-medium">SKU · Barras</th>
                      <th className="px-4 py-3 font-medium">Impuestos</th>
                      <th className="px-4 py-3 text-right font-medium">Costo neto prom.</th>
                      <th className="px-4 py-3 text-right font-medium">Stock Total</th>
                      {visibleLists.map((id) => (
                        <th key={id} className="px-4 py-3 text-right font-medium">
                          {listName(id)} #{id}
                        </th>
                      ))}
                      <th className="px-4 py-3 font-medium">Estado</th>
                      <th className="px-4 py-3 font-medium">Integridad</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-lq-line-soft">
                    {rows.map((r) => (
                      <tr key={r.variant_id} className="align-top transition-colors hover:bg-lq-canvas">
                        <td className="px-4 py-3 text-lq-body">{r.product_type ?? "—"}</td>
                        <td className="px-4 py-3">
                          <span className="block font-medium text-lq-ink">{r.product_name ?? `Producto ${r.product_id ?? "—"}`}</span>
                          <span className="mt-0.5 block text-xs text-lq-muted">{r.variant_name ?? "—"}</span>
                        </td>
                        <td className="px-4 py-3 text-xs text-lq-body">
                          <span className="block tabular-nums">{r.code ?? "Sin SKU"}</span>
                          <span className="mt-0.5 block tabular-nums text-lq-muted">{r.barcode ?? "Sin código"}</span>
                        </td>
                        <td className="px-4 py-3 text-xs text-lq-body">
                          <span className="block">{taxLabel(r)}</span>
                          {r.iva_pct != null || r.ila_pct != null ? (
                            <span className="mt-0.5 block text-lq-muted">
                              IVA {r.iva_pct ?? "—"}%{r.ila_pct != null ? ` · ILA ${r.ila_pct}%` : ""}
                            </span>
                          ) : null}
                        </td>
                        <td className="px-4 py-3 text-right tabular-nums text-lq-ink">
                          {r.average_cost_net != null ? formatMoney(r.average_cost_net) : <span className="text-lq-muted">Sin costo</span>}
                          {r.cost_state === "cero" ? <span className="block text-xs text-lq-warning">Costo cero</span> : null}
                        </td>
                        <td className="px-4 py-3 text-right tabular-nums text-lq-ink">
                          {/* Solo el Stock Total acumulado: sin desglose ni
                              conteos por sucursal en esta vista. */}
                          {r.stock_total != null ? (
                            formatQty(r.stock_total)
                          ) : (
                            <span className="text-lq-muted" title="Variante sin stock sincronizado: no equivale a cero">
                              s/d
                            </span>
                          )}
                        </td>
                        {visibleLists.map((id) => {
                          const p = r.prices[String(id)]
                          return (
                            <td key={id} className="px-4 py-3 text-right tabular-nums">
                              {p?.gross != null ? (
                                <span className="text-lq-ink">{formatMoney(p.gross)}</span>
                              ) : (
                                <span className="text-lq-muted">Sin precio</span>
                              )}
                            </td>
                          )
                        })}
                        <td className="px-4 py-3">
                          <StatusBadge tone={r.active ? "success" : "neutral"}>{r.active ? "Activa" : "Inactiva"}</StatusBadge>
                        </td>
                        <td className="px-4 py-3">
                          {r.integrity_issues.length ? (
                            <span className="text-xs text-lq-warning">{r.integrity_issues.join(", ")}</span>
                          ) : (
                            <StatusBadge tone="success">Completo</StatusBadge>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {!rows.length && !loading ? (
                  <EmptyState title="Sin resultados" description="Ninguna variante cumple los filtros seleccionados. Ajuste la búsqueda o limpie los filtros." />
                ) : null}
              </ProfessionalDataTable>
              <div className="mt-3">
                <Pager page={page} totalPages={totalPages} total={total} loading={loading} onPage={setPage} />
              </div>
            </section>
          )}
        </>
      ) : null}
    </AnalyticPageShell>
  )
}
