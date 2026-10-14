"use client"

import { useCallback, useEffect, useRef, useState } from "react"
import { AlertTriangle, Info } from "lucide-react"

import { CostV2ProductSheet } from "@/components/analytics-v2/costs/cost-v2-product-sheet"
import { Pager } from "@/components/analytics-v2/costs/cost-v2-products"
import { CostV2ReceptionDialog } from "@/components/analytics-v2/costs/cost-v2-reception-dialog"
import { MarginV2PolicyReview } from "@/components/analytics-v2/margins/margin-v2-policy-review"
import { MarginV2Kpis, MarginV2ListBreakdown } from "@/components/analytics-v2/margins/margin-v2-summary"
import {
  EMPTY_MARGIN_FILTERS,
  MarginV2Filters,
  MarginV2Table,
  type MarginFilterDraft,
} from "@/components/analytics-v2/margins/margin-v2-table"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { getStoredCompanyId } from "@/lib/api"
import {
  AnalyticsV2Error,
  getCostFilters,
  getMarginRows,
  getMarginsFilters,
  getMarginsSummary,
} from "@/lib/analytics-v2/api"
import { formatDateTime } from "@/lib/analytics-v2/labels"
import {
  COST_BASIS_LABELS,
  LIST_SCOPE_LABELS,
  MARGIN_SORT_LABELS,
  MARGIN_STATUS_LABELS,
} from "@/lib/analytics-v2/margins-labels"
import { queryEnum, queryInt, queryText, readQuery } from "@/lib/analytics-v2/url"
import type {
  CostBasis,
  MarginRowsResponse,
  MarginStatus,
  MarginsFilters,
  MarginsSummary,
} from "@/lib/analytics-v2/margins-types"
import type { CostFilters } from "@/lib/analytics-v2/types"
import { hasManagementAccess, staffUserFromLocalStorage } from "@/lib/permissions"

const PAGE_SIZE = 50

export default function MarginsV2Page() {
  const [companyId, setCompanyId] = useState<number | null>(null)
  const [canSeeCosts, setCanSeeCosts] = useState(false)
  const [summary, setSummary] = useState<MarginsSummary | null>(null)
  const [filters, setFilters] = useState<MarginsFilters | null>(null)
  const [costFilters, setCostFilters] = useState<CostFilters | null>(null)
  const [draft, setDraft] = useState<MarginFilterDraft>(EMPTY_MARGIN_FILTERS)
  const [applied, setApplied] = useState<MarginFilterDraft>(EMPTY_MARGIN_FILTERS)
  const [page, setPage] = useState(1)
  const [rows, setRows] = useState<MarginRowsResponse | null>(null)
  const [tab, setTab] = useState("resumen")
  const [loadingSummary, setLoadingSummary] = useState(false)
  const [loadingRows, setLoadingRows] = useState(false)
  const [blocking, setBlocking] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [variantId, setVariantId] = useState<number | null>(null)
  const [reception, setReception] = useState<{ id: number; detailId: number } | null>(null)
  const rowsAbort = useRef<AbortController | null>(null)

  useEffect(() => {
    setCompanyId(getStoredCompanyId())
    const management = hasManagementAccess(staffUserFromLocalStorage())
    setCanSeeCosts(management)
    const qs = readQuery()
    const statuses = Object.keys(MARGIN_STATUS_LABELS) as MarginStatus[]
    const patch: Partial<MarginFilterDraft> = {}
    const q = queryText(qs, "q")
    if (q) patch.q = q
    const status = queryEnum(qs, "status", statuses)
    if (status) patch.status = status
    const qualifier = queryEnum(qs, "qualifier", statuses)
    if (qualifier) patch.qualifier = qualifier
    const listId = queryInt(qs, "price_list_id")
    if (listId) patch.priceListId = String(listId)
    const sort = queryEnum(qs, "sort", Object.keys(MARGIN_SORT_LABELS))
    if (sort) patch.sort = sort
    const costBasis = queryEnum(qs, "cost_basis", Object.keys(COST_BASIS_LABELS) as CostBasis[])
    if (Object.keys(patch).length) {
      const next = { ...EMPTY_MARGIN_FILTERS, ...patch, ...(costBasis ? { costBasis } : {}) }
      setDraft(next)
      setApplied(next)
      setTab("detalle")
    } else {
      if (costBasis) {
        const next = { ...EMPTY_MARGIN_FILTERS, costBasis }
        setDraft(next)
        setApplied(next)
      }
      if (qs.get("tab") === "politica") setTab("politica")
    }
    const variant = queryInt(qs, "variant_id")
    if (variant && management) setVariantId(variant)
  }, [])

  useEffect(() => {
    if (companyId == null) return
    const ac = new AbortController()
    setLoadingSummary(true)
    setBlocking(null)
    Promise.all([getMarginsSummary(companyId, applied.costBasis, ac.signal), getMarginsFilters(companyId, ac.signal)])
      .then(([s, f]) => {
        setSummary(s)
        setFilters(f)
      })
      .catch((e: unknown) => {
        if (ac.signal.aborted) return
        if (e instanceof AnalyticsV2Error && [401, 403, 409].includes(e.status)) setBlocking(e.message)
        else setError(e instanceof Error ? e.message : "Error al cargar el resumen")
      })
      .finally(() => {
        if (!ac.signal.aborted) setLoadingSummary(false)
      })
    return () => ac.abort()
  }, [companyId, applied.costBasis])

  useEffect(() => {
    if (companyId == null || !canSeeCosts) return
    const ac = new AbortController()
    getCostFilters(companyId, ac.signal)
      .then(setCostFilters)
      .catch(() => setCostFilters(null))
    return () => ac.abort()
  }, [companyId, canSeeCosts])

  const loadRows = useCallback(() => {
    if (companyId == null || blocking) return
    rowsAbort.current?.abort()
    const ac = new AbortController()
    rowsAbort.current = ac
    setLoadingRows(true)
    setError(null)
    getMarginRows(
      {
        company_id: companyId,
        q: applied.q.trim() || undefined,
        price_list_id: applied.priceListId ? Number(applied.priceListId) : null,
        product_type_id: applied.productTypeId ? Number(applied.productTypeId) : null,
        status: applied.status || null,
        qualifier: applied.qualifier || null,
        reliability: applied.reliability || null,
        cost_basis: applied.costBasis,
        sort: applied.sort,
        page,
        page_size: PAGE_SIZE,
      },
      ac.signal,
    )
      .then(setRows)
      .catch((e: unknown) => {
        if (!ac.signal.aborted) setError(e instanceof Error ? e.message : "Error al cargar márgenes")
      })
      .finally(() => {
        if (!ac.signal.aborted) setLoadingRows(false)
      })
  }, [companyId, applied, page, blocking])

  useEffect(() => {
    if (tab === "detalle") loadRows()
  }, [tab, loadRows])

  const applyFilters = (patch: Partial<MarginFilterDraft> = {}) => {
    const next = { ...draft, ...patch }
    setDraft(next)
    setApplied(next)
    setPage(1)
    setTab("detalle")
  }

  if (companyId == null) {
    return <p className="p-6 text-sm text-muted-foreground">Seleccione una empresa para usar Márgenes V2.</p>
  }

  const discrepancies = summary?.scope.discrepancies ?? []

  return (
    <div className="mx-auto flex w-full max-w-[1440px] flex-col gap-3 p-3 md:p-4">
      <header className="flex flex-col gap-1 border-b border-border/60 pb-3">
        <div className="flex flex-wrap items-center gap-2">
          <h1 className="text-xl font-semibold tracking-tight md:text-2xl">Márgenes V2</h1>
          <span className="rounded-md border border-amber-300 bg-amber-50 px-1.5 py-0.5 text-[11px] font-medium text-amber-800">
            En validación
          </span>
        </div>
        <p className="text-sm text-muted-foreground">
          {summary?.meta.company_name ?? `Empresa ${companyId}`} · Precios y costos: BSALE RAW (solo lectura) · Reglas:
          política de márgenes vigente
        </p>
        <p className="text-xs text-muted-foreground">
          Calculado {formatDateTime(summary?.meta.generated_at)} · Recargo % = (Precio bruto / Costo bruto − 1) × 100
        </p>
      </header>

      {blocking ? (
        <Alert>
          <Info className="h-4 w-4" />
          <AlertTitle>Márgenes V2 no disponible</AlertTitle>
          <AlertDescription>{blocking}</AlertDescription>
        </Alert>
      ) : (
        <>
          {error ? (
            <Alert variant="destructive">
              <AlertTriangle className="h-4 w-4" />
              <AlertTitle>Error</AlertTitle>
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          ) : null}

          {summary ? (
            <Alert>
              <Info className="h-4 w-4" />
              <AlertTitle>Resultados provisionales</AlertTitle>
              <AlertDescription>
                {!summary.meta.policies_reviewed ? "La política de márgenes está pendiente de revisión con el Excel de gerencia. " : ""}
                {!summary.meta.net_cost_verified
                  ? "El costo bruto es preliminar (costo neto de recepción + IVA/ILA, sin contrastar con facturas). "
                  : ""}
                {!summary.meta.promotions_integrated
                  ? "Promociones V2 aún no está integrado: un SKU bajo política puede estar promocionado y no se descarta."
                  : "Una promoción o remate solo excluye la alerta si está aprobado, vigente y su precio coincide con Bsale; los riesgos de costo siguen visibles."}
              </AlertDescription>
            </Alert>
          ) : null}

          {discrepancies.length ? (
            <Alert variant="destructive">
              <AlertTriangle className="h-4 w-4" />
              <AlertTitle>Discrepancia de listas para revisión administrativa</AlertTitle>
              <AlertDescription>
                {discrepancies
                  .map((d) => `#${d.price_list_id} ${d.name ?? ""}: ${LIST_SCOPE_LABELS[d.status]}`)
                  .join(" · ")}
                . No se incorporan ni se excluyen automáticamente.
              </AlertDescription>
            </Alert>
          ) : null}

          <MarginV2Kpis
            summary={summary}
            loading={loadingSummary}
            onSelectStatus={(status: MarginStatus | null) =>
              applyFilters({ ...EMPTY_MARGIN_FILTERS, costBasis: draft.costBasis, status: status ?? "" })
            }
          />

          <Tabs value={tab} onValueChange={setTab} className="gap-3">
            <TabsList>
              <TabsTrigger value="resumen">Resumen por lista</TabsTrigger>
              <TabsTrigger value="detalle">SKU × lista</TabsTrigger>
              <TabsTrigger value="politica">Política pendiente de revisión</TabsTrigger>
            </TabsList>

            <TabsContent value="resumen">
              <MarginV2ListBreakdown
                summary={summary}
                onSelect={(priceListId, status) =>
                  applyFilters({ ...EMPTY_MARGIN_FILTERS, costBasis: draft.costBasis, priceListId: String(priceListId), status })
                }
              />
            </TabsContent>

            <TabsContent value="detalle" className="space-y-3">
              <MarginV2Filters
                draft={draft}
                filters={filters}
                loading={loadingRows}
                onChange={(patch) => setDraft((d) => ({ ...d, ...patch }))}
                onApply={() => applyFilters()}
                onClear={() => applyFilters(EMPTY_MARGIN_FILTERS)}
              />
              <MarginV2Table
                items={rows?.items ?? []}
                loading={loadingRows}
                canOpenCostHistory={canSeeCosts}
                onOpenCostHistory={setVariantId}
              />
              {rows ? (
                <Pager page={rows.page} totalPages={rows.total_pages} total={rows.total} loading={loadingRows} onPage={setPage} />
              ) : null}
            </TabsContent>

            <TabsContent value="politica">
              <MarginV2PolicyReview companyId={companyId} />
            </TabsContent>
          </Tabs>
        </>
      )}

      {canSeeCosts ? (
        <>
          <CostV2ProductSheet
            companyId={companyId}
            variantId={variantId}
            offices={costFilters?.offices ?? []}
            onOpenChange={(open) => {
              if (!open) setVariantId(null)
            }}
            onOpenReception={(id, detailId) => setReception({ id, detailId })}
          />
          <CostV2ReceptionDialog
            companyId={companyId}
            receptionId={reception?.id ?? null}
            highlightDetailId={reception?.detailId ?? null}
            onOpenChange={(open) => {
              if (!open) setReception(null)
            }}
          />
        </>
      ) : null}
    </div>
  )
}
