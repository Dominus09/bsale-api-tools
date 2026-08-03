"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { AlertTriangle, Loader2 } from "lucide-react"

import {
  CostCompanyFilters,
  type CostCompanyFilterDraft,
} from "@/components/costos/cost-company-filters"
import { CostControlKpis } from "@/components/costos/cost-control-kpis"
import { CostProductDetailDrawer } from "@/components/costos/cost-product-detail-drawer"
import { CostProductsTable } from "@/components/costos/cost-products-table"
import { CostSymbologyPanel } from "@/components/costos/cost-symbology-panel"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { getCompanies, getStoredCompanyId, type Company } from "@/lib/api"
import {
  CostV2ApiError,
  defaultCostV2DateRange,
  getCostV2CompanyProducts,
  getCostV2CompanySummary,
  mergeProductsByVariantId,
} from "@/lib/costos/control/api"
import { formatDateCL, formatDateTimeCL } from "@/lib/costos/control/format"
import type { CompanyProductItem, CompanySummary } from "@/lib/costos/control/types"
import { COST_V2_DEFAULT_LIMIT } from "@/lib/costos/control/types"

const THRESHOLD = 10

function emptyDraft(range: { from: string; to: string }): CostCompanyFilterDraft {
  return {
    dateFrom: range.from,
    dateTo: range.to,
    search: "",
    minChangePercent: String(THRESHOLD),
    movement: "",
    situation: "",
    warning: "",
    onlyRelevantChanges: false,
  }
}

export default function CostosPage() {
  const range0 = useMemo(() => defaultCostV2DateRange(90), [])
  const [companies, setCompanies] = useState<Company[]>([])
  const [companyId, setCompanyId] = useState<number | null>(null)
  const [draft, setDraft] = useState<CostCompanyFilterDraft>(() => emptyDraft(range0))
  const [applied, setApplied] = useState<CostCompanyFilterDraft | null>(null)
  const [moreOpen, setMoreOpen] = useState(false)
  const [tab, setTab] = useState("resumen")

  const [summary, setSummary] = useState<CompanySummary | null>(null)
  const [products, setProducts] = useState<CompanyProductItem[]>([])
  const [recentChanges, setRecentChanges] = useState<CompanyProductItem[]>([])
  const [nextCursor, setNextCursor] = useState<string | null>(null)
  const [hasMore, setHasMore] = useState(false)

  const [loading, setLoading] = useState(false)
  const [loadingMore, setLoadingMore] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const [detailVariantId, setDetailVariantId] = useState<number | null>(null)
  const [detailOpen, setDetailOpen] = useState(false)

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
    if (bootstrappedRef.current) return
    if (companyId == null || !draft.dateFrom || !draft.dateTo) return
    if (draft.dateFrom > draft.dateTo) return
    bootstrappedRef.current = true
    setApplied({ ...draft })
  }, [companyId, draft])

  const canQuery = Boolean(
    companyId &&
      applied?.dateFrom &&
      applied?.dateTo &&
      applied.dateFrom <= applied.dateTo,
  )

  const loadData = useCallback(
    async (mode: "replace" | "append", cursor: string | null = null) => {
      if (!companyId || !applied?.dateFrom || !applied.dateTo) return
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
        date_from: applied.dateFrom,
        date_to: applied.dateTo,
        warning: applied.warning || null,
        search: applied.search || null,
        min_abs_change_percent: applied.minChangePercent || null,
        movement: (applied.movement || null) as "up" | "down" | "flat" | null,
        situation: (applied.situation || null) as
          | "requires_review"
          | "office_difference"
          | "partial_coverage"
          | null,
        only_relevant_changes: applied.onlyRelevantChanges,
        signal: ac.signal,
      }

      try {
        if (mode === "replace") {
          const [sum, changed, list] = await Promise.all([
            getCostV2CompanySummary({
              ...base,
              change_threshold_percent: THRESHOLD,
            }),
            getCostV2CompanyProducts({
              ...base,
              sort: "pct_increase",
              only_relevant_changes: true,
              limit: 12,
            }),
            getCostV2CompanyProducts({
              ...base,
              sort: "latest_reception",
              limit: COST_V2_DEFAULT_LIMIT,
            }),
          ])
          if (ac.signal.aborted) return
          setSummary(sum.summary)
          setRecentChanges(changed.items)
          setProducts(list.items)
          setHasMore(Boolean(list.page.has_more))
          setNextCursor(list.page.next_cursor)
        } else {
          const list = await getCostV2CompanyProducts({
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
    if (!draft.dateFrom || !draft.dateTo) {
      setError("Las fechas son obligatorias.")
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
    setDraft(next)
    setApplied({ ...next })
  }

  const openProduct = (variantId: number) => {
    setDetailVariantId(variantId)
    setDetailOpen(true)
  }

  const applyKpiFilter = (key: "all" | "changes" | "review" | "office") => {
    const patch: Partial<CostCompanyFilterDraft> = {
      warning: "",
      movement: "",
      situation: "",
      onlyRelevantChanges: false,
      minChangePercent: String(THRESHOLD),
    }
    if (key === "changes") {
      patch.onlyRelevantChanges = true
      patch.minChangePercent = String(THRESHOLD)
    } else if (key === "review") {
      patch.situation = "requires_review"
    } else if (key === "office") {
      patch.situation = "office_difference"
    }
    const next = { ...draft, ...patch }
    setDraft(next)
    setApplied(next)
    setTab(key === "review" ? "alertas" : "productos")
    if (key !== "all") setMoreOpen(true)
  }

  const companyName =
    companies.find((c) => c.company_id === companyId)?.name ??
    (companyId != null ? `Empresa ${companyId}` : "—")

  const alertProducts = products.filter(
    (p) =>
      p.requires_review ||
      p.has_office_difference ||
      (p.current_warnings ?? []).length > 0,
  )

  return (
    <div className="mx-auto flex w-full max-w-[1440px] flex-col gap-3 p-3 md:p-4">
      <header className="flex flex-col gap-2 border-b border-border/60 pb-3 sm:flex-row sm:items-start sm:justify-between">
        <div className="min-w-0 space-y-1">
          <h1 className="text-xl font-semibold tracking-tight md:text-2xl">
            Control de costos
          </h1>
          <p className="text-sm text-muted-foreground">
            Costos vigentes y cambios detectados en {companyName}
          </p>
          <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
            <span>
              Actualizado:{" "}
              {formatDateTimeCL(summary?.latest_sync_or_calculation_at)}
            </span>
            <span>
              Última recepción: {formatDateCL(summary?.latest_reception_date)}
            </span>
            <span>Cobertura actual: {summary?.coverage_label ?? "—"}</span>
          </div>
        </div>
      </header>

      <CostCompanyFilters
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
            <Button
              type="button"
              size="sm"
              variant="outline"
              onClick={() => void loadData("replace")}
            >
              Reintentar
            </Button>
          </AlertDescription>
        </Alert>
      ) : null}

      {!applied ? (
        <div className="space-y-3">
          <p className="rounded-md border border-dashed px-4 py-8 text-center text-sm text-muted-foreground">
            Configure las fechas, luego Actualizar.
          </p>
          <Tabs defaultValue="simbologia" className="gap-3">
            <TabsList>
              <TabsTrigger value="simbologia">Simbología</TabsTrigger>
            </TabsList>
            <TabsContent value="simbologia">
              <CostSymbologyPanel />
            </TabsContent>
          </Tabs>
        </div>
      ) : (
        <Tabs value={tab} onValueChange={setTab} className="gap-3">
          <TabsList>
            <TabsTrigger value="resumen">Resumen</TabsTrigger>
            <TabsTrigger value="productos">Productos</TabsTrigger>
            <TabsTrigger value="alertas">Alertas</TabsTrigger>
            <TabsTrigger value="simbologia">Simbología</TabsTrigger>
          </TabsList>

          <TabsContent value="resumen" className="space-y-3">
            <CostControlKpis
              totalProducts={summary?.products_with_current_cost ?? null}
              relevantChanges={summary?.relevant_changes ?? null}
              needsReview={summary?.products_requiring_review ?? null}
              officeDifferences={summary?.products_with_office_difference ?? null}
              officeDifferenceComparable={Boolean(
                summary?.office_difference_comparable,
              )}
              coverageLabel={summary?.coverage_label ?? "—"}
              thresholdLabel={`${THRESHOLD} %`}
              loading={loading && !summary}
              onSelect={applyKpiFilter}
            />
            <section className="grid gap-3 lg:grid-cols-2">
              <div className="rounded-md border p-3">
                <h2 className="text-sm font-semibold">Cambios recientes</h2>
                <CostProductsTable
                  items={recentChanges}
                  loading={loading}
                  onOpen={openProduct}
                />
              </div>
              <div className="rounded-md border p-3 text-sm">
                <h2 className="font-semibold">Cobertura y alertas prioritarias</h2>
                <p className="mt-2 text-muted-foreground">
                  Cobertura: {summary?.coverage_label ?? "—"}. Revise productos
                  marcados para validación o con diferencias entre oficinas.
                </p>
              </div>
            </section>
          </TabsContent>

          <TabsContent value="productos" className="space-y-3">
            <CostProductsTable
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
                  {loadingMore ? (
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  ) : null}
                  Cargar más
                </Button>
              </div>
            ) : null}
          </TabsContent>

          <TabsContent value="alertas" className="space-y-3">
            <CostProductsTable
              items={alertProducts.length ? alertProducts : products}
              loading={loading}
              onOpen={openProduct}
            />
          </TabsContent>

          <TabsContent value="simbologia" className="space-y-3">
            <CostSymbologyPanel />
          </TabsContent>
        </Tabs>
      )}

      {companyId != null && applied ? (
        <CostProductDetailDrawer
          open={detailOpen}
          onOpenChange={setDetailOpen}
          variantId={detailVariantId}
          companyId={companyId}
          dateFrom={applied.dateFrom}
          dateTo={applied.dateTo}
          onOpenSymbology={() => {
            setDetailOpen(false)
            setTab("simbologia")
          }}
        />
      ) : null}
    </div>
  )
}
