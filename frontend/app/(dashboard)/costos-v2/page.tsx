"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { AlertTriangle, Loader2 } from "lucide-react"

import { CostV2AlertsPanel } from "@/components/costos-v2/cost-v2-alerts-panel"
import { CostV2ControlKpis } from "@/components/costos-v2/cost-v2-control-kpis"
import {
  CostV2Filters,
  type CostV2FilterDraft,
} from "@/components/costos-v2/cost-v2-filters"
import { CostV2ProductDetailDrawer } from "@/components/costos-v2/cost-v2-product-detail-drawer"
import { CostV2ProductsTable } from "@/components/costos-v2/cost-v2-products-table"
import { CostV2RecentChanges } from "@/components/costos-v2/cost-v2-recent-changes"
import { CostV2ReceptionsTable } from "@/components/costos-v2/cost-v2-receptions-table"
import { CostV2DetailDrawer } from "@/components/costos-v2/cost-v2-detail-drawer"
import { CostV2SymbologyPanel } from "@/components/costos-v2/cost-v2-symbology-panel"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import {
  getCompanies,
  getCostOffices,
  getStoredCompanyId,
  type Company,
  type CostOfficeRef,
} from "@/lib/api"
import {
  CostV2ApiError,
  defaultCostV2DateRange,
  getCostV2Products,
  getCostV2ProductsSummary,
  getCostV2Receptions,
  mergeProductsByVariantId,
  mergeReceptionItemsByHistoryId,
} from "@/lib/costos-v2/api"
import { formatDateCL, formatDateTimeCL } from "@/lib/costos-v2/format"
import type {
  CostV2ProductItem,
  CostV2ProductsSummaryBody,
  CostV2ReceptionListItem,
} from "@/lib/costos-v2/types"
import {
  COST_V2_DEFAULT_LIMIT,
  COST_V2_DEFAULT_OFFICE_ID,
} from "@/lib/costos-v2/types"

const THRESHOLD = 10

function emptyDraft(range: { from: string; to: string }): CostV2FilterDraft {
  return {
    officeId: String(COST_V2_DEFAULT_OFFICE_ID),
    dateFrom: range.from,
    dateTo: range.to,
    search: "",
    status: "",
    warning: "",
    barcode: "",
    minChangePercent: "",
    onlyWithChanges: false,
    onlyNeedsReview: false,
  }
}

export default function CostosV2Page() {
  const range0 = useMemo(() => defaultCostV2DateRange(30), [])
  const [companies, setCompanies] = useState<Company[]>([])
  const [offices, setOffices] = useState<CostOfficeRef[]>([])
  const [companyId, setCompanyId] = useState<number | null>(null)
  const [draft, setDraft] = useState<CostV2FilterDraft>(() => emptyDraft(range0))
  const [applied, setApplied] = useState<CostV2FilterDraft | null>(null)
  const [moreOpen, setMoreOpen] = useState(false)
  const [tab, setTab] = useState("resumen")

  const [summary, setSummary] = useState<CostV2ProductsSummaryBody | null>(null)
  const [products, setProducts] = useState<CostV2ProductItem[]>([])
  const [recentChanges, setRecentChanges] = useState<CostV2ProductItem[]>([])
  const [nextCursor, setNextCursor] = useState<string | null>(null)
  const [hasMore, setHasMore] = useState(false)

  const [receptions, setReceptions] = useState<CostV2ReceptionListItem[]>([])
  const [recCursor, setRecCursor] = useState<string | null>(null)
  const [recHasMore, setRecHasMore] = useState(false)

  const [loading, setLoading] = useState(false)
  const [loadingMore, setLoadingMore] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const [detailVariantId, setDetailVariantId] = useState<number | null>(null)
  const [detailOpen, setDetailOpen] = useState(false)
  const [recDetailId, setRecDetailId] = useState<number | null>(null)
  const [recDetailOpen, setRecDetailOpen] = useState(false)

  const abortRef = useRef<AbortController | null>(null)
  const bootstrappedRef = useRef(false)

  useEffect(() => {
    let cancelled = false
    ;(async () => {
      try {
        const list = await getCompanies()
        if (cancelled) return
        setCompanies(list)
        const stored = getStoredCompanyId()
        const def =
          list.find((c) => c.company_id === stored)?.company_id ??
          list[0]?.company_id ??
          null
        if (def != null) setCompanyId(def)
      } catch {
        if (!cancelled) setError("No se pudieron cargar las empresas.")
      }
    })()
    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    if (companyId == null) return
    let cancelled = false
    ;(async () => {
      try {
        const res = await getCostOffices(companyId)
        if (cancelled) return
        const list = res.items || []
        setOffices(list)
        const prefer =
          list.find((o) => o.office_id === COST_V2_DEFAULT_OFFICE_ID) ?? list[0]
        if (prefer) {
          setDraft((d) => ({ ...d, officeId: String(prefer.office_id) }))
        }
      } catch {
        if (!cancelled) setOffices([])
      }
    })()
    return () => {
      cancelled = true
    }
  }, [companyId])

  useEffect(() => {
    if (bootstrappedRef.current) return
    if (companyId == null || !draft.officeId || !draft.dateFrom || !draft.dateTo) return
    if (draft.dateFrom > draft.dateTo) return
    bootstrappedRef.current = true
    setApplied({ ...draft })
  }, [companyId, draft])

  const canQuery = Boolean(
    companyId &&
      applied?.officeId &&
      applied.dateFrom &&
      applied.dateTo &&
      applied.dateFrom <= applied.dateTo,
  )

  const loadData = useCallback(
    async (mode: "replace" | "append", cursor: string | null = null) => {
      if (!companyId || !applied?.officeId || !applied.dateFrom || !applied.dateTo) return
      abortRef.current?.abort()
      const ac = new AbortController()
      abortRef.current = ac

      if (mode === "replace") {
        setLoading(true)
        setProducts([])
        setNextCursor(null)
        setHasMore(false)
      } else {
        if (!cursor) return
        setLoadingMore(true)
      }
      setError(null)

      const base = {
        company_id: companyId,
        office_id: Number(applied.officeId),
        date_from: applied.dateFrom,
        date_to: applied.dateTo,
        status: applied.status || null,
        warning: applied.warning || null,
        barcode: applied.barcode || null,
        search: applied.search || null,
        only_with_changes: applied.onlyWithChanges,
        only_needs_review: applied.onlyNeedsReview,
        min_abs_change_percent: applied.minChangePercent || null,
        signal: ac.signal,
      }

      try {
        if (mode === "replace") {
          const [sum, changed, list, rec] = await Promise.all([
            getCostV2ProductsSummary({
              ...base,
              change_threshold_percent: THRESHOLD,
            }),
            getCostV2Products({
              ...base,
              sort: "pct_increase",
              only_with_changes: true,
              limit: 12,
            }),
            getCostV2Products({
              ...base,
              sort: "latest_reception",
              limit: COST_V2_DEFAULT_LIMIT,
            }),
            getCostV2Receptions({
              ...base,
              limit: COST_V2_DEFAULT_LIMIT,
            }),
          ])
          if (ac.signal.aborted) return
          setSummary(sum.summary)
          setRecentChanges(changed.items)
          setProducts(list.items)
          setHasMore(Boolean(list.page.has_more))
          setNextCursor(list.page.next_cursor)
          setReceptions(rec.items)
          setRecHasMore(Boolean(rec.page.has_more))
          setRecCursor(rec.page.next_cursor)
        } else {
          const list = await getCostV2Products({
            ...base,
            sort: "latest_reception",
            limit: COST_V2_DEFAULT_LIMIT,
            cursor,
          })
          if (ac.signal.aborted) return
          setProducts((prev) => mergeProductsByVariantId(prev, list.items))
          setHasMore(Boolean(list.page.has_more))
          setNextCursor(list.page.next_cursor)
        }
      } catch (e) {
        if (ac.signal.aborted) return
        if (e instanceof DOMException && e.name === "AbortError") return
        if (e instanceof CostV2ApiError) setError(e.message)
        else setError("Error de red. Intente nuevamente.")
      } finally {
        if (!ac.signal.aborted) {
          setLoading(false)
          setLoadingMore(false)
        }
      }
    },
    [companyId, applied],
  )

  useEffect(() => {
    if (!canQuery) return
    void loadData("replace")
  }, [canQuery, applied, companyId, loadData])

  const applyFilters = () => {
    if (!draft.officeId || !draft.dateFrom || !draft.dateTo) {
      setError("Oficina y fechas son obligatorias.")
      return
    }
    if (draft.dateFrom > draft.dateTo) {
      setError("La fecha desde debe ser ≤ fecha hasta.")
      return
    }
    setApplied({ ...draft })
  }

  const clearFilters = () => {
    const next = emptyDraft(range0)
    if (offices.some((o) => o.office_id === COST_V2_DEFAULT_OFFICE_ID)) {
      next.officeId = String(COST_V2_DEFAULT_OFFICE_ID)
    } else if (offices[0]) next.officeId = String(offices[0].office_id)
    setDraft(next)
    setApplied({ ...next })
  }

  const openProduct = (variantId: number) => {
    setDetailVariantId(variantId)
    setDetailOpen(true)
  }

  const applyKpiFilter = (key: "all" | "changes" | "review" | "outlier") => {
    const patch: Partial<CostV2FilterDraft> = {
      status: "",
      warning: "",
      onlyWithChanges: false,
      onlyNeedsReview: false,
      minChangePercent: "",
    }
    if (key === "changes") {
      patch.onlyWithChanges = true
      patch.minChangePercent = String(THRESHOLD)
    } else if (key === "review") {
      patch.onlyNeedsReview = true
    } else if (key === "outlier") {
      patch.warning = "suspicious_outlier"
    }
    const next = { ...draft, ...patch }
    setDraft(next)
    setApplied(next)
    setTab(key === "all" ? "productos" : key === "outlier" || key === "review" ? "alertas" : "productos")
    if (key !== "all") setMoreOpen(true)
  }

  const officeName =
    offices.find((o) => String(o.office_id) === applied?.officeId)?.office_name ||
    (applied?.officeId ? `Oficina ${applied.officeId}` : "—")

  const companyName =
    companies.find((c) => c.company_id === companyId)?.name ??
    (companyId != null ? `Empresa ${companyId}` : "—")

  const alertProducts = products.filter(
    (p) =>
      p.needs_review ||
      (p.current_warnings ?? []).includes("suspicious_outlier") ||
      (p.current_warnings ?? []).includes("stored_components_rounding") ||
      p.current_quality_status === "incomplete_tax_context" ||
      p.current_quality_status === "missing_cost",
  )

  return (
    <div className="mx-auto flex w-full max-w-[1440px] flex-col gap-3 p-3 md:p-4">
      <header className="flex flex-col gap-2 border-b border-border/60 pb-3 sm:flex-row sm:items-start sm:justify-between">
        <div className="min-w-0 space-y-1">
          <div className="flex flex-wrap items-center gap-2">
            <h1 className="text-xl font-semibold tracking-tight md:text-2xl">
              Control de costos
            </h1>
            <Badge variant="outline" className="font-normal text-[10px]">
              V2 validación
            </Badge>
          </div>
          <p className="text-sm text-muted-foreground">
            Últimos costos de recepción corregidos por impuestos
          </p>
          <p className="text-xs text-muted-foreground">
            {companyName} · {officeName}
            {applied
              ? ` · ${formatDateCL(applied.dateFrom)} → ${formatDateCL(applied.dateTo)}`
              : ""}
            {summary?.latest_calculation_at
              ? ` · Actualizado ${formatDateTimeCL(summary.latest_calculation_at)}`
              : ""}
            {summary?.latest_reception_date ? (
              <span className="ml-2 inline-flex items-center rounded bg-emerald-50 px-1.5 py-0.5 text-[10px] font-medium text-emerald-800 dark:bg-emerald-950/50 dark:text-emerald-200">
                Datos al día · últ. recepción {formatDateCL(summary.latest_reception_date)}
              </span>
            ) : null}
          </p>
        </div>
        <Button
          type="button"
          size="sm"
          className="bg-red-700 hover:bg-red-800"
          disabled={loading}
          onClick={() => (applied ? void loadData("replace") : applyFilters())}
        >
          {loading ? <Loader2 className="mr-1.5 h-3.5 w-3.5 animate-spin" /> : null}
          Actualizar
        </Button>
      </header>

      <CostV2Filters
        offices={offices}
        draft={draft}
        onChange={(p) => setDraft((d) => ({ ...d, ...p }))}
        onApply={applyFilters}
        onClear={clearFilters}
        loading={loading}
        disabled={companyId == null}
        moreOpen={moreOpen}
        onMoreOpenChange={setMoreOpen}
      />

      {error ? (
        <Alert variant="destructive">
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Error</AlertTitle>
          <AlertDescription className="flex flex-wrap items-center gap-2">
            <span>{error}</span>
            <Button type="button" size="sm" variant="outline" onClick={() => void loadData("replace")}>
              Reintentar
            </Button>
          </AlertDescription>
        </Alert>
      ) : null}

      {!applied ? (
        <div className="space-y-3">
          <p className="rounded-md border border-dashed px-4 py-8 text-center text-sm text-muted-foreground">
            Configure oficina y fechas, luego Actualizar.
          </p>
          <Tabs defaultValue="simbologia" className="gap-3">
            <TabsList>
              <TabsTrigger value="simbologia">Simbología</TabsTrigger>
            </TabsList>
            <TabsContent value="simbologia">
              <CostV2SymbologyPanel />
            </TabsContent>
          </Tabs>
        </div>
      ) : (
        <Tabs value={tab} onValueChange={setTab} className="gap-3">
          <TabsList>
            <TabsTrigger value="resumen">Resumen</TabsTrigger>
            <TabsTrigger value="productos">Productos</TabsTrigger>
            <TabsTrigger value="alertas">Alertas</TabsTrigger>
            <TabsTrigger value="recepciones">Recepciones</TabsTrigger>
            <TabsTrigger value="simbologia">Simbología</TabsTrigger>
          </TabsList>

          <TabsContent value="resumen" className="space-y-3">
            <CostV2ControlKpis
              totalProducts={summary?.total_products ?? null}
              relevantChanges={summary?.products_with_change_over_threshold ?? null}
              needsReview={summary?.products_needing_review ?? null}
              outliers={summary?.products_with_outlier ?? null}
              thresholdLabel={`${THRESHOLD} %`}
              loading={loading && !summary}
              onSelect={applyKpiFilter}
            />
            <div className="grid gap-3 lg:grid-cols-[minmax(0,1.6fr)_minmax(0,1fr)]">
              <CostV2RecentChanges
                items={recentChanges}
                loading={loading}
                onOpen={openProduct}
                onSeeAll={() => {
                  setDraft((d) => ({ ...d, onlyWithChanges: true }))
                  setApplied((a) =>
                    a ? { ...a, onlyWithChanges: true } : a,
                  )
                  setTab("productos")
                }}
              />
              <CostV2AlertsPanel
                summary={summary}
                loading={loading && !summary}
                onSelect={(key) => {
                  if (key === "incomplete_tax_context") {
                    const next = {
                      ...draft,
                      status: "incomplete_tax_context",
                      warning: "",
                      onlyNeedsReview: false,
                    }
                    setDraft(next)
                    setApplied(next)
                  } else if (key === "missing_cost") {
                    const next = {
                      ...draft,
                      status: "missing_cost",
                      warning: "",
                      onlyNeedsReview: false,
                    }
                    setDraft(next)
                    setApplied(next)
                  } else {
                    const next = {
                      ...draft,
                      status: "",
                      warning: key,
                      onlyNeedsReview: false,
                    }
                    setDraft(next)
                    setApplied(next)
                  }
                  setTab("alertas")
                  setMoreOpen(true)
                }}
              />
            </div>
          </TabsContent>

          <TabsContent value="productos" className="space-y-3">
            <CostV2ProductsTable
              items={products}
              loading={loading}
              onOpen={openProduct}
            />
            {hasMore ? (
              <div className="flex justify-center">
                <Button
                  type="button"
                  variant="outline"
                  disabled={loadingMore}
                  onClick={() => void loadData("append", nextCursor)}
                >
                  {loadingMore ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                  Cargar más
                </Button>
              </div>
            ) : null}
          </TabsContent>

          <TabsContent value="alertas" className="space-y-3">
            <CostV2AlertsPanel
              summary={summary}
              loading={loading && !summary}
              onSelect={() => undefined}
            />
            <CostV2ProductsTable
              items={alertProducts.length ? alertProducts : products}
              loading={loading}
              onOpen={openProduct}
            />
          </TabsContent>

          <TabsContent value="recepciones" className="space-y-3">
            <p className="text-xs text-muted-foreground">
              Consulta secundaria por recepción individual (no es la vista principal).
            </p>
            <CostV2ReceptionsTable
              items={receptions}
              loading={loading}
              onOpenDetail={(id) => {
                setRecDetailId(id)
                setRecDetailOpen(true)
              }}
            />
            {recHasMore ? (
              <div className="flex justify-center">
                <Button
                  type="button"
                  variant="outline"
                  disabled={loadingMore || !recCursor}
                  onClick={async () => {
                    if (!companyId || !applied || !recCursor) return
                    setLoadingMore(true)
                    try {
                      const res = await getCostV2Receptions({
                        company_id: companyId,
                        office_id: Number(applied.officeId),
                        date_from: applied.dateFrom,
                        date_to: applied.dateTo,
                        status: applied.status || null,
                        warning: applied.warning || null,
                        barcode: applied.barcode || null,
                        search: applied.search || null,
                        cursor: recCursor,
                        limit: COST_V2_DEFAULT_LIMIT,
                      })
                      setReceptions((prev) =>
                        mergeReceptionItemsByHistoryId(prev, res.items),
                      )
                      setRecHasMore(Boolean(res.page.has_more))
                      setRecCursor(res.page.next_cursor)
                    } catch (e) {
                      if (e instanceof CostV2ApiError) setError(e.message)
                    } finally {
                      setLoadingMore(false)
                    }
                  }}
                >
                  Cargar más recepciones
                </Button>
              </div>
            ) : null}
          </TabsContent>

          <TabsContent value="simbologia" className="space-y-3">
            <CostV2SymbologyPanel />
          </TabsContent>
        </Tabs>
      )}

      {companyId != null && applied?.officeId ? (
        <>
          <CostV2ProductDetailDrawer
            open={detailOpen}
            onOpenChange={setDetailOpen}
            variantId={detailVariantId}
            companyId={companyId}
            officeId={Number(applied.officeId)}
            dateFrom={applied.dateFrom}
            dateTo={applied.dateTo}
            onOpenSymbology={() => {
              setDetailOpen(false)
              setTab("simbologia")
            }}
          />
          <CostV2DetailDrawer
            open={recDetailOpen}
            onOpenChange={setRecDetailOpen}
            historyId={recDetailId}
            companyId={companyId}
            officeId={Number(applied.officeId)}
            onOpenSymbology={() => {
              setRecDetailOpen(false)
              setTab("simbologia")
            }}
          />
        </>
      ) : null}
    </div>
  )
}
