"use client"

import { useCallback, useEffect, useRef, useState } from "react"
import Link from "next/link"
import { AlertTriangle, ArrowRight, ChevronDown, Info, Loader2, RefreshCw } from "lucide-react"

import { CostV2ProductSheet } from "@/components/analytics-v2/costs/cost-v2-product-sheet"
import { CostV2ReceptionDialog } from "@/components/analytics-v2/costs/cost-v2-reception-dialog"
import { DashboardProductDialog } from "@/components/analytics-v2/dashboard/dashboard-product-dialog"
import {
  BlockState,
  AttentionPanel,
  COSTS_PATH,
  CostsOverview,
  DashboardExecutiveSummary,
  FreshnessPanel,
  MARGINS_PATH,
  MarginRowsTable,
  MarginsOverview,
  PACKS_PATH,
  PROMOTIONS_PATH,
  PacksOverview,
  PolicyCard,
  PromotionsOverview,
  TaxesOverview,
} from "@/components/analytics-v2/dashboard/dashboard-sections"
import {
  AnalyticPageHeader,
  AnalyticPageShell,
  AnalyticsLoading,
  DataStatusBar,
  SectionCard,
  StatusBadge,
} from "@/components/analytics-v2/shared/analytics-ui"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { getStoredCompanyId } from "@/lib/api"
import { AnalyticsV2Error, getCostFilters, getDashboard } from "@/lib/analytics-v2/api"
import { warningLabel } from "@/lib/analytics-v2/dashboard-labels"
import type { DashboardResponse } from "@/lib/analytics-v2/dashboard-types"
import { formatDateTime } from "@/lib/analytics-v2/labels"
import { COST_BASIS_LABELS } from "@/lib/analytics-v2/margins-labels"
import type { CostBasis } from "@/lib/analytics-v2/margins-types"
import type { CostFilters } from "@/lib/analytics-v2/types"

const selectClass = "h-9 rounded-md border border-lq-line bg-lq-surface px-2 text-sm text-lq-ink shadow-lq-xs"

export default function DashboardV2Page() {
  const [companyId, setCompanyId] = useState<number | null>(null)
  const [costBasis, setCostBasis] = useState<CostBasis>("promedio_bsale")
  const [data, setData] = useState<DashboardResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [blocking, setBlocking] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [productId, setProductId] = useState<number | null>(null)
  const [costVariantId, setCostVariantId] = useState<number | null>(null)
  const [reception, setReception] = useState<{ id: number; detailId: number } | null>(null)
  const [costFilters, setCostFilters] = useState<CostFilters | null>(null)
  const abortRef = useRef<AbortController | null>(null)

  useEffect(() => {
    setCompanyId(getStoredCompanyId())
  }, [])

  const load = useCallback(
    (refresh: boolean) => {
      if (companyId == null) return
      abortRef.current?.abort()
      const ac = new AbortController()
      abortRef.current = ac
      setLoading(true)
      setError(null)
      getDashboard(companyId, costBasis, refresh, ac.signal)
        .then((d) => {
          setData(d)
          setBlocking(null)
        })
        .catch((e: unknown) => {
          if (ac.signal.aborted) return
          if (e instanceof AnalyticsV2Error && [401, 403].includes(e.status)) setBlocking(e.message)
          else setError(e instanceof Error ? e.message : "Error al cargar el dashboard")
        })
        .finally(() => {
          if (!ac.signal.aborted) setLoading(false)
        })
    },
    [companyId, costBasis],
  )

  useEffect(() => {
    load(false)
    return () => abortRef.current?.abort()
  }, [load])

  const canSeeCosts = data?.permissions.costs ?? false

  useEffect(() => {
    if (companyId == null || !canSeeCosts) return
    const ac = new AbortController()
    getCostFilters(companyId, ac.signal)
      .then(setCostFilters)
      .catch(() => setCostFilters(null))
    return () => ac.abort()
  }, [companyId, canSeeCosts])

  if (companyId == null) {
    return <p className="p-6 text-sm text-muted-foreground">Seleccione una empresa para usar el Dashboard V2.</p>
  }

  const openCostHistory = (variantId: number) => {
    setProductId(null)
    setCostVariantId(variantId)
  }

  return (
    <AnalyticPageShell>
      <AnalyticPageHeader
        eyebrow="Inteligencia comercial"
        title="Centro de inteligencia comercial"
        description={`${data?.meta.company_name ?? `Empresa ${companyId}`} · Costos, márgenes, listas y packs en una vista orientada a decisiones.`}
        updatedAt={
          <>
            Última actualización: {formatDateTime(data?.meta.generated_at)}
            {data?.meta.cached ? " · respuesta en caché" : ""}
            {data?.meta.elapsed_ms != null ? ` · ${data.meta.elapsed_ms} ms` : ""}
          </>
        }
        actions={
          <>
          <label className="flex flex-col gap-1 text-[11px] font-medium uppercase text-muted-foreground">
            Base de costo
            <select
              className={selectClass}
              value={costBasis}
              onChange={(e) => setCostBasis(e.target.value as CostBasis)}
              disabled={loading}
            >
              {(Object.keys(COST_BASIS_LABELS) as CostBasis[]).map((k) => (
                <option key={k} value={k}>
                  {COST_BASIS_LABELS[k]}
                </option>
              ))}
            </select>
          </label>
          <Button variant="outline" size="sm" className="h-9" onClick={() => load(true)} disabled={loading}>
            {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
            Actualizar
          </Button>
          </>
        }
      />

      {blocking ? (
        <Alert>
          <Info className="h-4 w-4" />
          <AlertTitle>Dashboard V2 no disponible</AlertTitle>
          <AlertDescription>{blocking}</AlertDescription>
        </Alert>
      ) : null}

      {error ? (
        <Alert variant="destructive">
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      {!data && loading ? <AnalyticsLoading label="Preparando el panorama comercial…" /> : null}

      {data && !blocking ? (
        <>
          <DashboardExecutiveSummary data={data} />

          <div className="grid items-start gap-6 xl:grid-cols-[minmax(0,2fr)_minmax(320px,1fr)]">
            <AttentionPanel data={data} />
            <div className="flex min-w-0 flex-col gap-6">
              <DataStatusBar
                status={data.reliability.level === "confirmado" ? "success" : data.reliability.level === "no_disponible" ? "danger" : "warning"}
                title={data.reliability.level === "confirmado" ? "Datos confirmados" : "Resultados en validación"}
                summary="Condición provisional hasta la validación contable."
              >
                <div className="space-y-3">
                  <div>
                    <p className="font-medium text-lq-ink">Criterios de validación</p>
                    <ul className="mt-1 list-disc pl-5 text-lq-body">
                      <li>Precios: leídos desde Bsale RAW.</li>
                      <li>Impuestos IVA/ILA: configuración aún no contrastada contablemente.</li>
                      <li>Costo bruto comercial: cálculo provisional.</li>
                      <li>Política de márgenes: pendiente de revisión con el Excel de gerencia.</li>
                    </ul>
                  </div>
                  {data.reliability.warnings.length ? (
                    <div>
                      <p className="font-medium text-lq-ink">Observaciones actuales</p>
                      <ul className="mt-1 list-disc pl-5 text-lq-body">
                        {data.reliability.warnings.map((w) => <li key={w}>{warningLabel(w)}</li>)}
                      </ul>
                    </div>
                  ) : null}
                </div>
              </DataStatusBar>

              <BlockState title="Sincronización BSALE RAW" block={data.freshness}>
                {(f) => <FreshnessPanel freshness={f} />}
              </BlockState>

              <SectionCard title="Ir al detalle" contentClassName="divide-y divide-lq-line-soft p-0">
                {[
                  { label: "Control de costos", hint: "Anomalías, recepciones y confiabilidad", href: COSTS_PATH },
                  { label: "Márgenes por producto", hint: "Recargo, política y estado comercial", href: MARGINS_PATH },
                  { label: "Rentabilidad de packs", hint: "Costo por componentes y problemas", href: PACKS_PATH },
                  { label: "Promociones y remates", hint: "Campañas vigentes y pendientes", href: PROMOTIONS_PATH },
                ].map((item) => (
                  <Link
                    key={item.href}
                    href={item.href}
                    className="group flex items-center justify-between gap-3 px-5 py-3.5 transition-colors hover:bg-lq-canvas focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-primary/30"
                  >
                    <span className="min-w-0">
                      <span className="block text-sm font-medium text-lq-ink">{item.label}</span>
                      <span className="mt-0.5 block text-xs text-lq-faint">{item.hint}</span>
                    </span>
                    <ArrowRight className="h-4 w-4 shrink-0 text-lq-faint transition group-hover:translate-x-0.5 group-hover:text-lq-muted" />
                  </Link>
                ))}
              </SectionCard>
            </div>
          </div>

          <BlockState title="Márgenes" block={data.margins}>
            {(m) => (
              <div className="space-y-6">
                <MarginsOverview data={m} costBasis={data.cost_basis} />
                <section>
                  <div className="mb-3 flex items-center justify-between gap-3">
                    <div>
                      <h2 className="text-[19px] font-semibold tracking-[-0.015em] text-lq-ink">Problemas destacados</h2>
                      <p className="mt-1 text-[13px] text-lq-muted">Mayores brechas comerciales para iniciar la revisión.</p>
                    </div>
                    <StatusBadge tone={m.top_alerts.length ? "danger" : "success"}>
                      {m.top_alerts.length ? `${m.top_alerts.length} destacados` : "Sin alertas"}
                    </StatusBadge>
                  </div>
                  <div className="grid gap-4 xl:grid-cols-2">
                  <MarginRowsTable
                    title="Mayores brechas bajo política"
                    rows={m.top_alerts}
                    empty="Sin combinaciones bajo política."
                    onOpenProduct={setProductId}
                  />
                  <MarginRowsTable
                    title="Promociones y remates con riesgo de costo"
                    rows={m.top_exception_risks}
                    empty="Ninguna promoción o remate vigente con riesgo de costo."
                    onOpenProduct={setProductId}
                  />
                  </div>
                </section>
              </div>
            )}
          </BlockState>

          <SectionCard
            title="Análisis complementario"
            description="Costos, impuestos, packs y campañas permanecen disponibles sin competir con el resumen ejecutivo."
            contentClassName="p-0"
          >
            <div className="divide-y divide-lq-line-soft">
              <details className="group">
                <summary className="flex cursor-pointer list-none items-center justify-between px-5 py-4 text-sm font-semibold text-lq-body hover:bg-lq-canvas [&::-webkit-details-marker]:hidden">
                  Control de costos
                  <ChevronDown className="h-4 w-4 transition-transform group-open:rotate-180" />
                </summary>
                <div className="border-t border-lq-line-soft p-4 md:p-5">
                  <BlockState title="Costos" block={data.costs}>{(c) => <CostsOverview data={c} onOpenProduct={setProductId} />}</BlockState>
                </div>
              </details>
              <details className="group">
                <summary className="flex cursor-pointer list-none items-center justify-between px-5 py-4 text-sm font-semibold text-lq-body hover:bg-lq-canvas [&::-webkit-details-marker]:hidden">
                  Impuestos y costo bruto comercial
                  <ChevronDown className="h-4 w-4 transition-transform group-open:rotate-180" />
                </summary>
                <div className="border-t border-lq-line-soft p-4 md:p-5">
                  <BlockState title="Impuestos" block={data.taxes}>{(t) => <TaxesOverview data={t} canSeeCosts={canSeeCosts} />}</BlockState>
                </div>
              </details>
              <details className="group">
                <summary className="flex cursor-pointer list-none items-center justify-between px-5 py-4 text-sm font-semibold text-lq-body hover:bg-lq-canvas [&::-webkit-details-marker]:hidden">
                  Packs Bsale
                  <ChevronDown className="h-4 w-4 transition-transform group-open:rotate-180" />
                </summary>
                <div className="border-t border-lq-line-soft p-4 md:p-5">
                  <BlockState title="Packs" block={data.packs}>{(p) => <PacksOverview data={p} costBasis={data.cost_basis} />}</BlockState>
                </div>
              </details>
              <details className="group">
                <summary className="flex cursor-pointer list-none items-center justify-between px-5 py-4 text-sm font-semibold text-lq-body hover:bg-lq-canvas [&::-webkit-details-marker]:hidden">
                  Política y campañas
                  <ChevronDown className="h-4 w-4 transition-transform group-open:rotate-180" />
                </summary>
                <div className="grid gap-4 border-t border-lq-line-soft p-4 lg:grid-cols-[320px_1fr] md:p-5">
                  {data.margins.status === "ok" && data.margins.data ? <PolicyCard data={data.margins.data} /> : null}
                  <BlockState title="Ofertas y remates" block={data.promotions}>{(p) => <PromotionsOverview data={p} />}</BlockState>
                </div>
              </details>
            </div>
          </SectionCard>
        </>
      ) : null}

      <DashboardProductDialog
        companyId={companyId}
        variantId={productId}
        costBasis={data?.cost_basis ?? costBasis}
        canSeeCosts={canSeeCosts}
        onOpenChange={(open) => {
          if (!open) setProductId(null)
        }}
        onOpenCostHistory={openCostHistory}
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
    </AnalyticPageShell>
  )
}
