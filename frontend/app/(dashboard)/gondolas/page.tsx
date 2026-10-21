"use client"

import { useCallback, useEffect, useRef, useState } from "react"
import Link from "next/link"
import { useRouter } from "next/navigation"
import {
  AlertTriangle,
  ArrowRight,
  CirclePlay,
  History,
  PackageSearch,
  RefreshCw,
  ScanBarcode,
  Search,
} from "lucide-react"

import {
  AnalyticPageHeader,
  AnalyticsLoading,
  DataStatusBar,
  EmptyState,
  ProfessionalDataTable,
  SectionCard,
  SmartFilterBar,
  StatusBadge,
} from "@/components/analytics-v2/shared/analytics-ui"
import {
  ExhibitionBadge,
  formatDate,
  formatDateTime,
  formatQty,
  movementLabel,
} from "@/components/gondolas/gondola-labels"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import {
  GondolasError,
  createGondolaSession,
  getGondolaCatalog,
  getGondolaCompanies,
  getGondolaOptions,
  getGondolaSessions,
} from "@/lib/gondolas/api"
import type {
  CatalogResponse,
  GondolaCompany,
  GondolaOptions,
  GondolaSession,
} from "@/lib/gondolas/types"
import { getStoredCompanyId } from "@/lib/api"
import { cn } from "@/lib/utils"

const PAGE_SIZE = 100

const selectClass =
  "min-h-11 w-full rounded-lg border border-border bg-card px-3 text-sm text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/35"

function sessionTone(status: GondolaSession["status"]) {
  if (status === "en_curso") return "info" as const
  if (status === "cerrado") return "success" as const
  return "danger" as const
}

function sessionLabel(status: GondolaSession["status"]) {
  if (status === "en_curso") return "En curso"
  if (status === "cerrado") return "Cerrado"
  return "Cancelado"
}

export default function GondolasPage() {
  const router = useRouter()
  const [companies, setCompanies] = useState<GondolaCompany[]>([])
  const [companyId, setCompanyId] = useState<number | null>(null)
  const [options, setOptions] = useState<GondolaOptions | null>(null)
  const [officeId, setOfficeId] = useState<number | null>(null)
  const [productTypeId, setProductTypeId] = useState<number | null>(null)
  const [q, setQ] = useState("")
  const [stockFilter, setStockFilter] = useState("con_stock")
  const [sort, setSort] = useState("producto")
  const [catalog, setCatalog] = useState<CatalogResponse | null>(null)
  const [sessions, setSessions] = useState<GondolaSession[]>([])
  const [loading, setLoading] = useState(true)
  const [loadingCatalog, setLoadingCatalog] = useState(false)
  const [creating, setCreating] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const abortRef = useRef<AbortController | null>(null)

  useEffect(() => {
    const ac = new AbortController()
    getGondolaCompanies(ac.signal)
      .then(({ companies: list }) => {
        setCompanies(list)
        const stored = getStoredCompanyId()
        const initial = list.find((c) => c.company_id === stored) ?? list[0]
        setCompanyId(initial?.company_id ?? stored ?? null)
        setLoading(false)
      })
      .catch((err) => {
        if (err instanceof DOMException && err.name === "AbortError") return
        setError(err instanceof GondolasError ? err.message : "No se pudieron cargar las empresas disponibles.")
        setLoading(false)
      })
    return () => ac.abort()
  }, [])

  useEffect(() => {
    if (companyId == null) return
    const ac = new AbortController()
    setOptions(null)
    setCatalog(null)
    setError(null)
    getGondolaOptions(companyId, ac.signal)
      .then((opts) => {
        setOptions(opts)
        setOfficeId((prev) => (prev && opts.offices.some((o) => o.office_id === prev) ? prev : (opts.offices[0]?.office_id ?? null)))
        setProductTypeId((prev) =>
          prev && opts.product_types.some((t) => t.product_type_id === prev)
            ? prev
            : (opts.product_types.find((t) => t.name.toUpperCase().includes("VINO"))?.product_type_id ??
              opts.product_types[0]?.product_type_id ??
              null),
        )
      })
      .catch((err) => {
        if (err instanceof DOMException && err.name === "AbortError") return
        setError(err instanceof GondolasError ? err.message : "No se pudieron cargar las sucursales y tipos.")
      })
    getGondolaSessions(companyId, ac.signal)
      .then(({ items }) => setSessions(items))
      .catch(() => setSessions([]))
    return () => ac.abort()
  }, [companyId])

  const loadCatalog = useCallback(() => {
    if (companyId == null || officeId == null || productTypeId == null) return
    abortRef.current?.abort()
    const ac = new AbortController()
    abortRef.current = ac
    setLoadingCatalog(true)
    getGondolaCatalog(
      {
        company_id: companyId,
        office_id: officeId,
        product_type_id: productTypeId,
        q,
        stock_filter: stockFilter,
        sort,
        page: 1,
        page_size: PAGE_SIZE,
      },
      ac.signal,
    )
      .then(setCatalog)
      .catch((err) => {
        if (err instanceof DOMException && err.name === "AbortError") return
        setError(err instanceof GondolasError ? err.message : "No se pudo cargar el catálogo de góndolas.")
      })
      .finally(() => setLoadingCatalog(false))
  }, [companyId, officeId, productTypeId, q, stockFilter, sort])

  useEffect(() => {
    const t = setTimeout(loadCatalog, 250)
    return () => clearTimeout(t)
  }, [loadCatalog])

  async function startSession() {
    if (companyId == null || officeId == null || productTypeId == null) return
    setCreating(true)
    setError(null)
    try {
      const session = await createGondolaSession(companyId, officeId, productTypeId)
      router.push(`/gondolas/recorridos/${session.id}?company_id=${companyId}`)
    } catch (err) {
      setError(err instanceof GondolasError ? err.message : "No se pudo iniciar el recorrido.")
    } finally {
      setCreating(false)
    }
  }

  const activeSession = sessions.find(
    (s) => s.status === "en_curso" && s.office_id === officeId && s.product_type_id === productTypeId,
  )
  const officeName = options?.offices.find((o) => o.office_id === officeId)?.name
  const typeName = options?.product_types.find((t) => t.product_type_id === productTypeId)?.name

  if (loading) return <AnalyticsLoading label="Preparando Control de Góndolas…" />

  return (
    <div className="mx-auto w-full max-w-[1600px] space-y-6 px-4 py-6 md:px-6 lg:px-8">
      <AnalyticPageHeader
        eyebrow="Sucursales · Exhibición"
        title="Control de Góndolas"
        description="Recorra la sala, escanee los productos exhibidos y genere la lista de reposición sin tocar el stock de Bsale."
        actions={
          <>
            <Button type="button" variant="outline" onClick={loadCatalog} disabled={loadingCatalog} className="min-h-11">
              <RefreshCw className={cn("mr-2 h-4 w-4", loadingCatalog && "animate-spin")} /> Actualizar
            </Button>
            {activeSession ? (
              <Button asChild className="min-h-11">
                <Link href={`/gondolas/recorridos/${activeSession.id}?company_id=${companyId}`}>
                  <CirclePlay className="mr-2 h-4 w-4" /> Continuar recorrido
                </Link>
              </Button>
            ) : (
              <Button type="button" onClick={() => void startSession()} disabled={creating || !options?.tables_ready} className="min-h-11">
                <ScanBarcode className="mr-2 h-4 w-4" />
                {creating ? "Iniciando…" : "Iniciar recorrido"}
              </Button>
            )}
          </>
        }
      />

      {error ? (
        <Alert variant="destructive">
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>No se pudo completar la operación</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      {options && !options.tables_ready ? (
        <DataStatusBar
          status="warning"
          title="Recorridos en preparación"
          summary="El catálogo ya se puede consultar; falta aplicar la migración revisada para guardar recorridos."
        >
          <p>
            Las tablas propias de góndolas todavía no existen en esta base. El catálogo, el stock por sucursal y la
            última entrada se leen desde BSALE RAW. Para crear recorridos se debe aplicar, con autorización, la
            migración <code>backend/sql/gondolas/001_gondola_control.sql</code>.
          </p>
        </DataStatusBar>
      ) : (
        <DataStatusBar status="info" title="Datos desde BSALE RAW" summary="Stock y recepciones corresponden a la sucursal elegida.">
          <ul className="list-disc space-y-1 pl-5">
            <li>El universo del recorrido son SKU distintos con stock positivo al momento de iniciar.</li>
            <li>La última entrada proviene de recepciones registradas para esa oficina, no de una fecha global.</li>
            <li>Este módulo no modifica existencias, documentos ni precios de Bsale.</li>
          </ul>
        </DataStatusBar>
      )}

      <SmartFilterBar>
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          <label className="space-y-1.5">
            <span className="text-xs font-medium text-muted-foreground">Empresa</span>
            <select
              className={selectClass}
              value={companyId ?? ""}
              onChange={(e) => setCompanyId(Number(e.target.value))}
              aria-label="Empresa"
            >
              {companies.map((c) => (
                <option key={c.company_id} value={c.company_id}>{c.name}</option>
              ))}
              {companies.length === 0 && companyId != null ? <option value={companyId}>Empresa {companyId}</option> : null}
            </select>
          </label>
          <label className="space-y-1.5">
            <span className="text-xs font-medium text-muted-foreground">Sucursal (oficina Bsale)</span>
            <select
              className={selectClass}
              value={officeId ?? ""}
              onChange={(e) => setOfficeId(Number(e.target.value))}
              aria-label="Sucursal"
            >
              {(options?.offices ?? []).map((o) => (
                <option key={o.office_id} value={o.office_id}>{o.name} · Oficina {o.office_id}</option>
              ))}
            </select>
          </label>
          <label className="space-y-1.5">
            <span className="text-xs font-medium text-muted-foreground">Tipo de producto</span>
            <select
              className={selectClass}
              value={productTypeId ?? ""}
              onChange={(e) => setProductTypeId(Number(e.target.value))}
              aria-label="Tipo de producto"
            >
              {(options?.product_types ?? []).map((t) => (
                <option key={t.product_type_id} value={t.product_type_id}>{t.name}</option>
              ))}
            </select>
          </label>
          <label className="space-y-1.5">
            <span className="text-xs font-medium text-muted-foreground">Buscar producto, SKU o código</span>
            <span className="relative block">
              <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
              <Input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Ej.: vino, 780…" className="min-h-11 pl-9" />
            </span>
          </label>
        </div>
        <div className="flex flex-wrap items-end gap-3">
          <label className="space-y-1.5">
            <span className="text-xs font-medium text-muted-foreground">Stock</span>
            <select className={selectClass} value={stockFilter} onChange={(e) => setStockFilter(e.target.value)} aria-label="Filtro de stock">
              <option value="con_stock">Con stock positivo</option>
              <option value="todos">Todos los SKU</option>
              <option value="sin_stock">Sin stock</option>
            </select>
          </label>
          <label className="space-y-1.5">
            <span className="text-xs font-medium text-muted-foreground">Ordenar por</span>
            <select className={selectClass} value={sort} onChange={(e) => setSort(e.target.value)} aria-label="Ordenamiento">
              <option value="producto">Nombre</option>
              <option value="stock_desc">Mayor stock</option>
              <option value="stock_asc">Menor stock</option>
              <option value="ultima_entrada_desc">Última entrada reciente</option>
            </select>
          </label>
          {catalog ? (
            <p className="pb-2 text-sm text-muted-foreground">
              <strong className="text-foreground">{catalog.total}</strong> SKU distintos
              {officeName ? <> en {officeName}</> : null}
              {typeName ? <> · {typeName}</> : null}
            </p>
          ) : null}
        </div>
      </SmartFilterBar>

      <div className="grid gap-6 xl:grid-cols-[minmax(0,1fr)_360px]">
        <SectionCard
          title="Catálogo de la sucursal"
          description="Stock registrado en Bsale para la oficina elegida y última entrada a esa misma sucursal."
          contentClassName="p-0"
        >
          {catalog && catalog.items.length === 0 && !loadingCatalog ? (
            <EmptyState
              title="Sin productos para este filtro"
              description="No hay SKU que coincidan con la sucursal, el tipo y los filtros seleccionados."
            />
          ) : (
            <>
              <div className="grid gap-3 p-4 md:hidden">
                {(catalog?.items ?? []).map((item) => (
                  <article key={item.variant_id} className="rounded-xl border border-border bg-card p-4">
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <h3 className="text-sm font-semibold leading-5 text-foreground">{item.product_name}</h3>
                        <p className="mt-0.5 text-xs text-muted-foreground">{item.variant_name || "Sin variante"}</p>
                      </div>
                      <ExhibitionBadge status={item.exhibition_status} />
                    </div>
                    <dl className="mt-3 grid grid-cols-2 gap-2 text-xs text-muted-foreground">
                      <div><dt className="text-muted-foreground">Código</dt><dd className="font-mono">{item.bar_code || item.sku_code || "—"}</dd></div>
                      <div><dt className="text-muted-foreground">Stock</dt><dd className="font-semibold text-foreground">{formatQty(item.stock)}</dd></div>
                      <div><dt className="text-muted-foreground">Disponible</dt><dd>{formatQty(item.stock_available)}</dd></div>
                      <div><dt className="text-muted-foreground">Última entrada</dt><dd>{formatDate(item.last_entry_at)}</dd></div>
                    </dl>
                  </article>
                ))}
              </div>
              <ProfessionalDataTable loading={loadingCatalog} className="hidden rounded-none border-0 shadow-none md:block">
                <table className="w-full min-w-[880px] text-left text-sm">
                  <thead className="bg-muted/40 text-xs uppercase tracking-wide text-muted-foreground">
                    <tr>
                      <th className="px-4 py-3 font-medium">Producto</th>
                      <th className="px-4 py-3 font-medium">Código de barras</th>
                      <th className="px-4 py-3 text-right font-medium">Stock</th>
                      <th className="px-4 py-3 text-right font-medium">Disponible</th>
                      <th className="px-4 py-3 font-medium">Última entrada a la sucursal</th>
                      <th className="px-4 py-3 font-medium">Exhibición</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border">
                    {(catalog?.items ?? []).map((item) => (
                      <tr key={item.variant_id} className="transition hover:bg-muted/40/70">
                        <td className="px-4 py-3">
                          <span className="block font-medium leading-5 text-foreground">{item.product_name}</span>
                          <span className="mt-0.5 block text-xs text-muted-foreground">
                            {item.variant_name || "Sin variante"}{item.sku_code ? <> · SKU {item.sku_code}</> : null}
                          </span>
                        </td>
                        <td className="px-4 py-3 font-mono text-xs text-muted-foreground">{item.bar_code || "—"}</td>
                        <td className="px-4 py-3 text-right font-semibold tabular-nums text-foreground">{formatQty(item.stock)}</td>
                        <td className="px-4 py-3 text-right tabular-nums text-muted-foreground">{formatQty(item.stock_available)}</td>
                        <td className="px-4 py-3 text-muted-foreground">
                          {item.last_entry_at ? (
                            <>
                              <span className="block">{formatDate(item.last_entry_at)}</span>
                              <span className="mt-0.5 block text-xs text-muted-foreground">{movementLabel(item.last_entry_movement)}</span>
                            </>
                          ) : (
                            <span className="text-muted-foreground">Sin ingreso registrado en RAW</span>
                          )}
                        </td>
                        <td className="px-4 py-3"><ExhibitionBadge status={item.exhibition_status} /></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </ProfessionalDataTable>
            </>
          )}
        </SectionCard>

        <SectionCard title="Recorridos" description="Continúe un recorrido guardado o revise los anteriores." contentClassName="p-3">
          {sessions.length === 0 ? (
            <div className="flex gap-3 px-2 py-4 text-sm text-muted-foreground">
              <History className="mt-0.5 h-4 w-4 shrink-0" />
              <p>Todavía no hay recorridos registrados para esta empresa.</p>
            </div>
          ) : (
            <ul className="divide-y divide-border">
              {sessions.map((session) => (
                <li key={session.id}>
                  <Link
                    href={`/gondolas/recorridos/${session.id}?company_id=${companyId}`}
                    className="group flex items-center gap-3 rounded-lg px-2 py-3 transition hover:bg-muted/40 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/30"
                  >
                    <span className="min-w-0 flex-1">
                      <span className="flex flex-wrap items-center gap-2">
                        <strong className="text-sm font-semibold text-foreground">
                          {session.office_name || `Oficina ${session.office_id}`}
                        </strong>
                        <StatusBadge tone={sessionTone(session.status)}>{sessionLabel(session.status)}</StatusBadge>
                      </span>
                      <span className="mt-1 block text-xs leading-5 text-muted-foreground">
                        {session.product_type_name || "Tipo"} · {session.found_count} de {session.total_sku} SKU ·{" "}
                        {session.coverage_pct.toFixed(1).replace(".", ",")} % · {formatDateTime(session.started_at)}
                      </span>
                    </span>
                    <ArrowRight className="h-4 w-4 shrink-0 text-muted-foreground transition group-hover:translate-x-0.5" />
                  </Link>
                </li>
              ))}
            </ul>
          )}
          <div className="mt-3 flex gap-2 rounded-lg bg-muted/40 px-3 py-3 text-xs leading-5 text-muted-foreground">
            <PackageSearch className="mt-0.5 h-4 w-4 shrink-0" />
            <p>Al cerrar un recorrido, los SKU no detectados pasan a la lista de reposición para buscarlos en bodega.</p>
          </div>
        </SectionCard>
      </div>
    </div>
  )
}
