"use client"

import { useCallback, useEffect, useRef, useState } from "react"
import { AlertTriangle, Download, Info, Loader2 } from "lucide-react"

import { CostV2ProductSheet } from "@/components/analytics-v2/costs/cost-v2-product-sheet"
import { Pager } from "@/components/analytics-v2/costs/cost-v2-products"
import { CostV2ReceptionDialog } from "@/components/analytics-v2/costs/cost-v2-reception-dialog"
import { DashboardProductDialog } from "@/components/analytics-v2/dashboard/dashboard-product-dialog"
import { MarginV2PolicyReview } from "@/components/analytics-v2/margins/margin-v2-policy-review"
import { MarginV2Kpis, MarginV2ListBreakdown } from "@/components/analytics-v2/margins/margin-v2-summary"
import {
  EMPTY_MARGIN_FILTERS,
  MarginV2Filters,
  MarginV2Table,
  type MarginFilterDraft,
} from "@/components/analytics-v2/margins/margin-v2-table"
import { AnalyticPageHeader, DataStatusBar, StatusBadge } from "@/components/analytics-v2/shared/analytics-ui"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { getStoredCompanyId } from "@/lib/api"
import {
  AnalyticsV2Error,
  downloadXlsx,
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
  const [costVariantId, setCostVariantId] = useState<number | null>(null)
  const [reception, setReception] = useState<{ id: number; detailId: number } | null>(null)
  // Todos los Hooks deben ejecutarse antes de cualquier retorno anticipado:
  // este useState estaba tras `if (companyId == null) return …` y provocaba
  // el error React #310 al cargar la empresa (más hooks que el render previo).
  const [exportingMode, setExportingMode] = useState<string | null>(null)
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
        stock_filter: applied.stockFilter,
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

  const handleExport = async (mode: "bajo_costo" | "bajo_politica" | "completo") => {
    if (companyId == null) return
    setExportingMode(mode)
    setError(null)
    try {
      await downloadXlsx(
        "/analytics-v2/margins/export.xlsx",
        {
          company_id: companyId,
          mode,
          q: applied.q.trim() || null,
          price_list_id: applied.priceListId ? Number(applied.priceListId) : null,
          product_type_id: applied.productTypeId ? Number(applied.productTypeId) : null,
          status: applied.status || null,
          qualifier: applied.qualifier || null,
          reliability: applied.reliability || null,
          stock_filter: applied.stockFilter,
          cost_basis: applied.costBasis,
        },
        `margenes-empresa-${companyId}-${mode}.xlsx`,
      )
    } catch (e) {
      setError(e instanceof Error ? e.message : "No se pudo generar el Excel")
    } finally {
      setExportingMode(null)
    }
  }

  return (
    <div className="flex w-full min-w-0 flex-col gap-6 pb-10">
      <AnalyticPageHeader
        eyebrow="Analítica V2"
        title="Márgenes comerciales"
        description={`Compara precios, costos y políticas para detectar rápidamente qué productos requieren atención · ${summary?.meta.company_name ?? `Empresa ${companyId}`}.`}
        updatedAt={<>Última actualización: {formatDateTime(summary?.meta.generated_at)} · Recargo = (precio bruto / costo bruto − 1) × 100</>}
        actions={
          <>
            <span className="flex flex-wrap items-center gap-1.5">
              <span className="text-[11px] font-medium uppercase text-lq-muted">Exportar análisis Excel</span>
              {(
                [
                  ["bajo_costo", "Bajo costo"],
                  ["bajo_politica", "Bajo política"],
                  ["completo", "Completo"],
                ] as const
              ).map(([mode, label]) => (
                <Button
                  key={mode}
                  variant="outline"
                  size="sm"
                  className="h-10 sm:h-9"
                  onClick={() => handleExport(mode)}
                  disabled={exportingMode != null}
                >
                  {exportingMode === mode ? <Loader2 className="h-4 w-4 animate-spin" /> : <Download className="h-4 w-4" />}
                  {label}
                </Button>
              ))}
            </span>
            <StatusBadge tone="warning">Resultados provisionales</StatusBadge>
          </>
        }
      />

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
            <DataStatusBar status="warning" title="Márgenes en validación" summary="Precios reales de Bsale; costos, impuestos y políticas conservan su estado provisional.">
              <div className="grid gap-3 text-lq-body md:grid-cols-2">
                <p><strong className="text-lq-ink">Costo bruto comercial:</strong> incorpora IVA/ILA, pero aún no está contrastado contablemente.</p>
                <p><strong className="text-lq-ink">Política:</strong> las reglas actuales se mantienen sin cambios hasta la revisión del Excel de gerencia.</p>
                <p><strong className="text-lq-ink">Promociones:</strong> solo justifican una excepción cuando están aprobadas, vigentes y verificadas.</p>
                <p><strong className="text-lq-ink">Riesgos:</strong> una excepción nunca oculta ventas bajo costo o costos sospechosos.</p>
              </div>
            </DataStatusBar>
          ) : null}

          {discrepancies.length ? (
            <Alert variant="destructive">
              <AlertTriangle className="h-4 w-4" />
              <AlertTitle>Discrepancia de listas para revisión administrativa</AlertTitle>
              <AlertDescription>
                {discrepancies
                  .map((d) => `${d.commercial_label ?? d.name ?? "Lista sin nombre"}: ${LIST_SCOPE_LABELS[d.status]}`)
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

          <Tabs value={tab} onValueChange={setTab} className="gap-4">
            <TabsList className="h-11 rounded-xl border border-lq-line bg-lq-surface p-1 shadow-sm">
              <TabsTrigger value="resumen">Resumen por lista</TabsTrigger>
              <TabsTrigger value="detalle">Explorar productos</TabsTrigger>
              <TabsTrigger value="politica">Política comercial</TabsTrigger>
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
                onOpenProduct={setVariantId}
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

      <DashboardProductDialog
        companyId={companyId}
        variantId={variantId}
        costBasis={applied.costBasis}
        canSeeCosts={canSeeCosts}
        onOpenChange={(open) => { if (!open) setVariantId(null) }}
        onOpenCostHistory={(id) => {
          setVariantId(null)
          setCostVariantId(id)
        }}
      />

      {canSeeCosts ? (
        <>
          <CostV2ProductSheet
            companyId={companyId}
            variantId={costVariantId}
            offices={costFilters?.offices ?? []}
            onOpenChange={(open) => {
              if (!open) setCostVariantId(null)
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
