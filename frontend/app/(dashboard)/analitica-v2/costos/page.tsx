"use client"

import { useCallback, useEffect, useRef, useState } from "react"
import { AlertTriangle, Download, Info, Loader2 } from "lucide-react"

import { CostV2Alerts } from "@/components/analytics-v2/costs/cost-v2-alerts"
import { CostV2ProductSheet } from "@/components/analytics-v2/costs/cost-v2-product-sheet"
import {
  CostV2ProductFilters,
  CostV2ProductsTable,
  EMPTY_PRODUCT_FILTERS,
  Pager,
  type ProductFilterDraft,
} from "@/components/analytics-v2/costs/cost-v2-products"
import { CostV2ReceptionDialog } from "@/components/analytics-v2/costs/cost-v2-reception-dialog"
import { CostV2Kpis, CostV2SummaryPanels } from "@/components/analytics-v2/costs/cost-v2-summary"
import { AnalyticPageHeader, DataStatusBar, StatusBadge } from "@/components/analytics-v2/shared/analytics-ui"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { getStoredCompanyId } from "@/lib/api"
import { AnalyticsV2Error, downloadXlsx, getCostFilters, getCostProducts, getCostSummary, getRawFreshness } from "@/lib/analytics-v2/api"
import { RELIABILITY_LABELS, SORT_LABELS, TAX_LABELS, formatDateTime } from "@/lib/analytics-v2/labels"
import { queryEnum, queryInt, queryText, readQuery } from "@/lib/analytics-v2/url"
import type {
  CostReliability,
  CostFilters,
  CostProductsResponse,
  CostSummary,
  FreshnessResponse,
  TaxProfileStatus,
} from "@/lib/analytics-v2/types"

const PAGE_SIZE = 50

export default function CostControlV2Page() {
  const [companyId, setCompanyId] = useState<number | null>(null)
  const [summary, setSummary] = useState<CostSummary | null>(null)
  const [freshness, setFreshness] = useState<FreshnessResponse | null>(null)
  const [filters, setFilters] = useState<CostFilters | null>(null)
  const [draft, setDraft] = useState<ProductFilterDraft>(EMPTY_PRODUCT_FILTERS)
  const [applied, setApplied] = useState<ProductFilterDraft>(EMPTY_PRODUCT_FILTERS)
  const [page, setPage] = useState(1)
  const [products, setProducts] = useState<CostProductsResponse | null>(null)
  const [tab, setTab] = useState("resumen")
  const [loadingSummary, setLoadingSummary] = useState(false)
  const [loadingProducts, setLoadingProducts] = useState(false)
  const [blocking, setBlocking] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [variantId, setVariantId] = useState<number | null>(null)
  const [reception, setReception] = useState<{ id: number; detailId: number } | null>(null)
  const productsAbort = useRef<AbortController | null>(null)

  useEffect(() => {
    setCompanyId(getStoredCompanyId())
    const qs = readQuery()
    const patch: Partial<ProductFilterDraft> = {}
    const q = queryText(qs, "q")
    if (q) patch.q = q
    const reliability = queryEnum(qs, "reliability", Object.keys(RELIABILITY_LABELS) as CostReliability[])
    if (reliability) patch.reliability = reliability
    const sort = queryEnum(qs, "sort", Object.keys(SORT_LABELS))
    if (sort) patch.sort = sort
    const taxStatus = queryEnum(qs, "tax_status", Object.keys(TAX_LABELS) as TaxProfileStatus[])
    if (taxStatus) patch.taxStatus = taxStatus
    const minChange = queryInt(qs, "min_change")
    if (minChange) patch.minChange = String(minChange)
    if (Object.keys(patch).length) {
      const next = { ...EMPTY_PRODUCT_FILTERS, ...patch }
      setDraft(next)
      setApplied(next)
      setTab("productos")
    } else if (qs.get("tab") === "alertas") {
      setTab("alertas")
    }
    const variant = queryInt(qs, "variant_id")
    if (variant) setVariantId(variant)
  }, [])

  useEffect(() => {
    if (companyId == null) return
    const ac = new AbortController()
    setLoadingSummary(true)
    setBlocking(null)
    Promise.all([
      getCostSummary(companyId, ac.signal),
      getCostFilters(companyId, ac.signal),
      getRawFreshness(companyId, ac.signal),
    ])
      .then(([s, f, fr]) => {
        setSummary(s)
        setFilters(f)
        setFreshness(fr)
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
  }, [companyId])

  const loadProducts = useCallback(() => {
    if (companyId == null || blocking) return
    productsAbort.current?.abort()
    const ac = new AbortController()
    productsAbort.current = ac
    setLoadingProducts(true)
    setError(null)
    const minChange = applied.minChange ? Number(applied.minChange) : null
    getCostProducts(
      {
        company_id: companyId,
        q: applied.q.trim() || undefined,
        product_type_id: applied.productTypeId ? Number(applied.productTypeId) : null,
        variant_state: applied.variantState,
        reliability: applied.reliability || null,
        tax_status: applied.taxStatus || null,
        last_movement_class: applied.movement || null,
        anomaly_code: applied.anomaly || null,
        office_id: applied.officeId ? Number(applied.officeId) : null,
        min_abs_change_pct: minChange !== null && Number.isFinite(minChange) ? minChange : null,
        sort: applied.sort,
        page,
        page_size: PAGE_SIZE,
      },
      ac.signal,
    )
      .then(setProducts)
      .catch((e: unknown) => {
        if (!ac.signal.aborted) setError(e instanceof Error ? e.message : "Error al cargar productos")
      })
      .finally(() => {
        if (!ac.signal.aborted) setLoadingProducts(false)
      })
  }, [companyId, applied, page, blocking])

  useEffect(() => {
    if (tab === "productos") loadProducts()
  }, [tab, loadProducts])

  const applyFilters = (patch: Partial<ProductFilterDraft> = {}) => {
    const next = { ...draft, ...patch }
    setDraft(next)
    setApplied(next)
    setPage(1)
    setTab("productos")
  }

  const openReception = (id: number, detailId: number) => setReception({ id, detailId })

  const [exporting, setExporting] = useState(false)
  const [exportingReceptions, setExportingReceptions] = useState(false)
  const [exportingPremium, setExportingPremium] = useState(false)
  const [receptionSegment, setReceptionSegment] = useState(1)

  const exportParams = () => {
    const minChange = applied.minChange ? Number(applied.minChange) : null
    return {
      company_id: companyId,
      q: applied.q.trim() || null,
      product_type_id: applied.productTypeId ? Number(applied.productTypeId) : null,
      variant_state: applied.variantState,
      reliability: applied.reliability || null,
      tax_status: applied.taxStatus || null,
      last_movement_class: applied.movement || null,
      anomaly_code: applied.anomaly || null,
      office_id: applied.officeId ? Number(applied.officeId) : null,
      min_abs_change_pct: minChange !== null && Number.isFinite(minChange) ? minChange : null,
      sort: applied.sort,
    }
  }

  const handleExportReceptions = async () => {
    if (companyId == null) return
    setExportingReceptions(true)
    setError(null)
    try {
      await downloadXlsx(
        "/analytics-v2/costs/receptions/export.xlsx",
        { ...exportParams(), segment: receptionSegment },
        `recepciones-empresa-${companyId}-segmento-${receptionSegment}.xlsx`,
      )
    } catch (e) {
      setError(e instanceof Error ? e.message : "No se pudo generar el historial de recepciones")
    } finally {
      setExportingReceptions(false)
    }
  }

  const handleExport = async () => {
    if (companyId == null) return
    setExporting(true)
    setError(null)
    const minChange = applied.minChange ? Number(applied.minChange) : null
    try {
      await downloadXlsx(
        "/analytics-v2/costs/export.xlsx",
        {
          company_id: companyId,
          q: applied.q.trim() || null,
          product_type_id: applied.productTypeId ? Number(applied.productTypeId) : null,
          variant_state: applied.variantState,
          reliability: applied.reliability || null,
          tax_status: applied.taxStatus || null,
          last_movement_class: applied.movement || null,
          anomaly_code: applied.anomaly || null,
          office_id: applied.officeId ? Number(applied.officeId) : null,
          min_abs_change_pct: minChange !== null && Number.isFinite(minChange) ? minChange : null,
          sort: applied.sort,
        },
        `control-costos-empresa-${companyId}.xlsx`,
      )
    } catch (e) {
      setError(e instanceof Error ? e.message : "No se pudo generar el Excel")
    } finally {
      setExporting(false)
    }
  }

  const handleExportPremium = async () => {
    if (companyId == null) return
    setExportingPremium(true)
    setError(null)
    const stamp = new Date().toISOString().slice(0, 10)
    try {
      await downloadXlsx(
        "/analytics-v2/audit/commercial-premium/export.xlsx",
        { company_id: companyId },
        `Auditoria_Comercial_Premium_Empresa_${companyId}_${stamp}.xlsx`,
      )
    } catch (e) {
      setError(
        e instanceof Error
          ? e.message
          : "No se pudo generar el Excel Premium de auditoría. Verifique permisos y reintente.",
      )
    } finally {
      setExportingPremium(false)
    }
  }

  if (companyId == null) {
    return (
      <p className="p-6 text-sm text-muted-foreground">Seleccione una empresa para usar Control de Costos V2.</p>
    )
  }

  return (
    <div className="flex w-full min-w-0 flex-col gap-6 pb-10">
      <AnalyticPageHeader
        eyebrow="Analítica V2"
        title="Centro de control de costos"
        description={`Revisa la calidad de los costos, detecta anomalías de compra y consulta la trazabilidad de cada SKU · ${summary?.meta.company_name ?? `Empresa ${companyId}`}.`}
        updatedAt={<>Última actualización: {formatDateTime(summary?.meta.generated_at)} · Fuente Bsale RAW de solo lectura</>}
        actions={
          <>
            <Button variant="outline" size="sm" className="h-10 sm:h-9" onClick={handleExport} disabled={exporting}>
              {exporting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Download className="h-4 w-4" />}
              Exportar análisis Excel
            </Button>
            <Button
              variant="outline"
              size="sm"
              className="h-10 sm:h-9"
              onClick={handleExportPremium}
              disabled={exportingPremium}
              title="Auditoría Comercial 8.0: seis hojas con universo completo de divergencias (sin tope top 30)."
            >
              {exportingPremium ? <Loader2 className="h-4 w-4 animate-spin" /> : <Download className="h-4 w-4" />}
              Excel Premium auditoría
            </Button>
            <div className="flex items-center gap-1.5" title="Historial de recepciones por segmentos de 300 variantes, con los filtros aplicados. Cada archivo declara su segmento y cobertura.">
              <label htmlFor="reception-segment" className="text-xs text-muted-foreground">Segmento</label>
              <input
                id="reception-segment"
                type="number"
                min={1}
                value={receptionSegment}
                onChange={(e) => setReceptionSegment(Math.max(1, Number(e.target.value) || 1))}
                className="h-10 w-16 rounded-md border bg-background px-2 text-sm sm:h-9"
              />
              <Button variant="outline" size="sm" className="h-10 sm:h-9" onClick={handleExportReceptions} disabled={exportingReceptions}>
                {exportingReceptions ? <Loader2 className="h-4 w-4 animate-spin" /> : <Download className="h-4 w-4" />}
                Historial de recepciones
              </Button>
            </div>
            <StatusBadge tone="warning">Validación contable pendiente</StatusBadge>
          </>
        }
      />

      {blocking ? (
        <Alert>
          <Info className="h-4 w-4" />
          <AlertTitle>Control de Costos V2 no disponible</AlertTitle>
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

          {summary && !summary.meta.net_cost_verified ? (
            <DataStatusBar status="warning" title="Costos comerciales en validación" summary="La consistencia operativa no equivale a validación contable.">
              <div className="grid gap-3 text-lq-body md:grid-cols-2">
                <p><strong className="text-lq-ink">Costo promedio Bsale:</strong> se muestra sin recalcular ni modificar.</p>
                <p><strong className="text-lq-ink">Última compra:</strong> corresponde a la recepción comparable más reciente.</p>
                <p><strong className="text-lq-ink">Costo bruto comercial:</strong> aplica IVA/ILA cuando el perfil tributario es calculable.</p>
                <p><strong className="text-lq-ink">Pendiente:</strong> contrastar costos netos e impuestos contra documentación contable.</p>
              </div>
            </DataStatusBar>
          ) : null}

          <CostV2Kpis
            summary={summary}
            loading={loadingSummary}
            onSelectReliability={(status) => applyFilters({ ...EMPTY_PRODUCT_FILTERS, reliability: status ?? "" })}
          />

          <Tabs value={tab} onValueChange={setTab} className="gap-4">
            <TabsList className="h-11 rounded-xl border border-lq-line bg-lq-surface p-1 shadow-sm">
              <TabsTrigger value="resumen">Resumen</TabsTrigger>
              <TabsTrigger value="productos">Explorar productos</TabsTrigger>
              <TabsTrigger value="alertas">Alertas de recepción</TabsTrigger>
            </TabsList>

            <TabsContent value="resumen">
              <CostV2SummaryPanels
                summary={summary}
                freshness={freshness}
                onSelectReliability={(status) => applyFilters({ ...EMPTY_PRODUCT_FILTERS, reliability: status })}
                onSelectAnomaly={(code) => applyFilters({ ...EMPTY_PRODUCT_FILTERS, anomaly: code })}
                onSelectTax={(status) => applyFilters({ ...EMPTY_PRODUCT_FILTERS, taxStatus: status })}
              />
            </TabsContent>

            <TabsContent value="productos" className="space-y-3">
              <CostV2ProductFilters
                draft={draft}
                filters={filters}
                loading={loadingProducts}
                onChange={(patch) => setDraft((d) => ({ ...d, ...patch }))}
                onApply={() => applyFilters()}
                onClear={() => applyFilters(EMPTY_PRODUCT_FILTERS)}
              />
              <CostV2ProductsTable
                items={products?.items ?? []}
                loading={loadingProducts}
                onOpen={setVariantId}
              />
              {products ? (
                <Pager
                  page={products.page}
                  totalPages={products.total_pages}
                  total={products.total}
                  loading={loadingProducts}
                  onPage={setPage}
                />
              ) : null}
            </TabsContent>

            <TabsContent value="alertas">
              <CostV2Alerts
                companyId={companyId}
                filters={filters}
                onOpenProduct={setVariantId}
                onOpenReception={openReception}
              />
            </TabsContent>
          </Tabs>
        </>
      )}

      <CostV2ProductSheet
        companyId={companyId}
        variantId={variantId}
        offices={filters?.offices ?? []}
        onOpenChange={(open) => {
          if (!open) setVariantId(null)
        }}
        onOpenReception={openReception}
      />
      <CostV2ReceptionDialog
        companyId={companyId}
        receptionId={reception?.id ?? null}
        highlightDetailId={reception?.detailId ?? null}
        onOpenChange={(open) => {
          if (!open) setReception(null)
        }}
      />
    </div>
  )
}
