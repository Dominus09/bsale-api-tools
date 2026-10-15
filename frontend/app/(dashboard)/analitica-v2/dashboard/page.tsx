"use client"

import { useCallback, useEffect, useRef, useState } from "react"
import { AlertTriangle, Info, Loader2, RefreshCw } from "lucide-react"

import { CostV2ProductSheet } from "@/components/analytics-v2/costs/cost-v2-product-sheet"
import { CostV2ReceptionDialog } from "@/components/analytics-v2/costs/cost-v2-reception-dialog"
import { DashboardProductDialog } from "@/components/analytics-v2/dashboard/dashboard-product-dialog"
import {
  BlockState,
  CostsOverview,
  DataClassBadge,
  FreshnessPanel,
  MarginRowsTable,
  MarginsOverview,
  PacksOverview,
  PolicyCard,
  PromotionsOverview,
  TaxesOverview,
} from "@/components/analytics-v2/dashboard/dashboard-sections"
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

const selectClass = "h-9 rounded-md border border-input bg-background px-2 text-sm"

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
    <div className="mx-auto flex w-full max-w-[1440px] flex-col gap-4 p-3 md:p-4">
      <header className="flex flex-col gap-2 border-b border-border/60 pb-3 md:flex-row md:items-end md:justify-between">
        <div className="flex flex-col gap-1">
          <div className="flex flex-wrap items-center gap-2">
            <h1 className="text-xl font-semibold tracking-tight md:text-2xl">Dashboard V2</h1>
            <span className="rounded-md border border-amber-300 bg-amber-50 px-1.5 py-0.5 text-[11px] font-medium text-amber-800">
              En validación
            </span>
            {data ? <DataClassBadge value={data.reliability.level} /> : null}
          </div>
          <p className="text-sm text-muted-foreground">
            {data?.meta.company_name ?? `Empresa ${companyId}`} · Costos, márgenes, listas y promociones · BSALE RAW (solo lectura)
          </p>
          <p className="text-xs text-muted-foreground">
            Calculado {formatDateTime(data?.meta.generated_at)}
            {data?.meta.cached ? " (caché de hasta 60 s)" : ""}
            {data?.meta.elapsed_ms != null ? ` · ${data.meta.elapsed_ms} ms` : ""}
          </p>
        </div>
        <div className="flex items-end gap-2">
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
        </div>
      </header>

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

      {!data && loading ? (
        <p className="flex items-center gap-2 text-sm text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin" /> Calculando indicadores…
        </p>
      ) : null}

      {data && !blocking ? (
        <>
          {data.reliability.warnings.length ? (
            <Alert>
              <Info className="h-4 w-4" />
              <AlertTitle>Confiabilidad de los datos</AlertTitle>
              <AlertDescription>
                <ul className="list-disc pl-4">
                  {data.reliability.warnings.map((w) => (
                    <li key={w}>{warningLabel(w)}</li>
                  ))}
                </ul>
              </AlertDescription>
            </Alert>
          ) : null}

          <BlockState title="Sincronización BSALE RAW" block={data.freshness}>
            {(f) => <FreshnessPanel freshness={f} />}
          </BlockState>

          <BlockState title="Márgenes" block={data.margins}>
            {(m) => (
              <>
                <MarginsOverview data={m} costBasis={data.cost_basis} />
                <div className="grid gap-3 xl:grid-cols-[1fr_1fr_280px]">
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
                  <PolicyCard data={m} />
                </div>
              </>
            )}
          </BlockState>

          <BlockState title="Costos" block={data.costs}>
            {(c) => <CostsOverview data={c} onOpenProduct={setProductId} />}
          </BlockState>

          <BlockState title="Impuestos y costo bruto comercial" block={data.taxes}>
            {(t) => <TaxesOverview data={t} canSeeCosts={canSeeCosts} />}
          </BlockState>

          <BlockState title="Ofertas y remates" block={data.promotions}>
            {(p) => <PromotionsOverview data={p} />}
          </BlockState>

          {data.packs ? (
            <BlockState title="Packs Bsale" block={data.packs}>
              {(p) => <PacksOverview data={p} costBasis={data.cost_basis} />}
            </BlockState>
          ) : null}
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
    </div>
  )
}
