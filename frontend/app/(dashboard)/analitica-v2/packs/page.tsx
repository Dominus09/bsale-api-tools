"use client"

import { useCallback, useEffect, useRef, useState } from "react"
import { AlertTriangle, Boxes, CircleDollarSign, Info, PackageCheck, PackageX, TrendingDown } from "lucide-react"

import { Pager } from "@/components/analytics-v2/costs/cost-v2-products"
import { PackV2DetailSheet } from "@/components/analytics-v2/packs/pack-v2-detail-sheet"
import {
  EMPTY_PACK_FILTERS,
  PackV2Filters,
  PackV2Table,
  type PackFilterDraft,
} from "@/components/analytics-v2/packs/pack-v2-table"
import {
  AnalyticPageHeader,
  DataStatusBar,
  MetricCard,
  SectionCard,
  StatusBadge,
} from "@/components/analytics-v2/shared/analytics-ui"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { getStoredCompanyId } from "@/lib/api"
import { AnalyticsV2Error, getPackRows, getPacksSummary } from "@/lib/analytics-v2/api"
import { formatDateTime, formatPct } from "@/lib/analytics-v2/labels"
import { COST_BASIS_LABELS, MARGIN_STATUS_LABELS } from "@/lib/analytics-v2/margins-labels"
import { PACK_PROBLEM_LABELS, PACK_SORT_LABELS, PACK_STATUS_LABELS } from "@/lib/analytics-v2/packs-labels"
import type { CostBasis, MarginStatus } from "@/lib/analytics-v2/margins-types"
import type { PackCostStatus, PackProblem, PackRowsResponse, PacksSummary } from "@/lib/analytics-v2/packs-types"
import { queryEnum, queryInt, queryText, readQuery } from "@/lib/analytics-v2/url"

const PAGE_SIZE = 50

export default function PacksV2Page() {
  const [companyId, setCompanyId] = useState<number | null>(null)
  const [summary, setSummary] = useState<PacksSummary | null>(null)
  const [draft, setDraft] = useState<PackFilterDraft>(EMPTY_PACK_FILTERS)
  const [applied, setApplied] = useState<PackFilterDraft>(EMPTY_PACK_FILTERS)
  const [page, setPage] = useState(1)
  const [rows, setRows] = useState<PackRowsResponse | null>(null)
  const [loadingSummary, setLoadingSummary] = useState(false)
  const [loadingRows, setLoadingRows] = useState(false)
  const [blocking, setBlocking] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [variantId, setVariantId] = useState<number | null>(null)
  const rowsAbort = useRef<AbortController | null>(null)

  useEffect(() => {
    setCompanyId(getStoredCompanyId())
    const qs = readQuery()
    const patch: Partial<PackFilterDraft> = {}
    const q = queryText(qs, "q")
    if (q) patch.q = q
    const listId = queryInt(qs, "price_list_id")
    if (listId) patch.priceListId = String(listId)
    const packStatus = queryEnum(qs, "pack_status", Object.keys(PACK_STATUS_LABELS) as PackCostStatus[])
    if (packStatus) patch.packStatus = packStatus
    const marginStatus = queryEnum(qs, "margin_status", Object.keys(MARGIN_STATUS_LABELS) as MarginStatus[])
    if (marginStatus) patch.marginStatus = marginStatus
    const problem = queryEnum(qs, "problem", Object.keys(PACK_PROBLEM_LABELS) as PackProblem[])
    if (problem) patch.problem = problem
    const packState = queryEnum(qs, "pack_state", ["active", "inactive", "all"] as const)
    if (packState) patch.packState = packState
    const costBasis = queryEnum(qs, "cost_basis", Object.keys(COST_BASIS_LABELS) as CostBasis[])
    if (costBasis) patch.costBasis = costBasis
    const sort = queryEnum(qs, "sort", Object.keys(PACK_SORT_LABELS))
    if (sort) patch.sort = sort
    if (Object.keys(patch).length) {
      const next = { ...EMPTY_PACK_FILTERS, ...patch }
      setDraft(next)
      setApplied(next)
    }
    const variant = queryInt(qs, "variant_id")
    if (variant) setVariantId(variant)
  }, [])

  useEffect(() => {
    if (companyId == null) return
    const ac = new AbortController()
    setLoadingSummary(true)
    setBlocking(null)
    getPacksSummary(companyId, applied.costBasis, ac.signal)
      .then(setSummary)
      .catch((e: unknown) => {
        if (ac.signal.aborted) return
        if (e instanceof AnalyticsV2Error && [401, 403, 409].includes(e.status)) setBlocking(e.message)
        else setError(e instanceof Error ? e.message : "Error al cargar el resumen de packs")
      })
      .finally(() => {
        if (!ac.signal.aborted) setLoadingSummary(false)
      })
    return () => ac.abort()
  }, [companyId, applied.costBasis])

  const loadRows = useCallback(() => {
    if (companyId == null || blocking) return
    rowsAbort.current?.abort()
    const ac = new AbortController()
    rowsAbort.current = ac
    setLoadingRows(true)
    setError(null)
    getPackRows(
      {
        company_id: companyId,
        q: applied.q.trim() || undefined,
        price_list_id: applied.priceListId ? Number(applied.priceListId) : null,
        pack_status: applied.packStatus || null,
        margin_status: applied.marginStatus || null,
        problem: applied.problem || null,
        pack_state: applied.packState,
        cost_basis: applied.costBasis,
        sort: applied.sort,
        page,
        page_size: PAGE_SIZE,
      },
      ac.signal,
    )
      .then(setRows)
      .catch((e: unknown) => {
        if (ac.signal.aborted) return
        if (e instanceof AnalyticsV2Error && [401, 403, 409].includes(e.status)) setBlocking(e.message)
        else setError(e instanceof Error ? e.message : "Error al cargar packs")
      })
      .finally(() => {
        if (!ac.signal.aborted) setLoadingRows(false)
      })
  }, [companyId, applied, page, blocking])

  useEffect(() => {
    loadRows()
  }, [loadRows])

  const applyFilters = (patch: Partial<PackFilterDraft> = {}) => {
    const next = { ...draft, ...patch }
    setDraft(next)
    setApplied(next)
    setPage(1)
  }

  if (companyId == null) {
    return <p className="p-6 text-sm text-muted-foreground">Seleccione una empresa para usar Rentabilidad de packs.</p>
  }

  const pendingStatuses = summary
    ? (Object.entries(summary.by_pack_status) as [PackCostStatus, number][]).filter(([s, n]) => s !== "calculado" && n > 0)
    : []
  const priceLists = rows?.price_lists ?? summary?.price_lists ?? []

  return (
    <div className="mx-auto flex w-full max-w-[1600px] flex-col gap-6 pb-10">
      <AnalyticPageHeader
        eyebrow="Analítica V2"
        title="Rentabilidad de packs"
        description={`Analiza el costo de cada componente y descubre cuáles promociones necesitan atención · ${summary?.meta.company_name ?? `Empresa ${companyId}`}.`}
        updatedAt={
          <>
            Última actualización: {formatDateTime(summary?.meta.generated_at)} · Composición Bsale RAW actualizada entre{" "}
            {formatDateTime(summary?.composition_fetched.min)} y {formatDateTime(summary?.composition_fetched.max)}
          </>
        }
        actions={<StatusBadge tone="warning">Resultados provisionales</StatusBadge>}
      />

      {blocking ? (
        <Alert>
          <Info className="h-4 w-4" />
          <AlertTitle>Rentabilidad de packs no disponible</AlertTitle>
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

          <DataStatusBar
            status="warning"
            title="Costo comercial en validación"
            summary="Los cálculos son correctos matemáticamente, pero aún no constituyen cifras contables definitivas."
          >
            <ul className="grid gap-2 text-slate-600 md:grid-cols-2">
              <li>• El costo del pack suma costo neto × cantidad por componente.</li>
              <li>• IVA e ILA se aplican por componente mediante IDs Bsale.</li>
              <li>• El costo propio del SKU del pack no se usa para la rentabilidad.</li>
              <li>• Packs incompletos o con componentes pendientes no generan margen.</li>
            </ul>
          </DataStatusBar>

          <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
            <MetricCard
              label="Packs activos"
              value={loadingSummary && !summary ? "…" : (summary?.packs_active ?? "—")}
              context={`${summary?.packs_total ?? "—"} packs registrados`}
              icon={Boxes}
              tone="info"
              onClick={() => applyFilters({ ...EMPTY_PACK_FILTERS, costBasis: draft.costBasis })}
            />
            <MetricCard
              label="Costo calculable"
              value={summary?.packs_active_calculable ?? "—"}
              context={summary?.coverage_pct != null ? `Cobertura ${formatPct(summary.coverage_pct)}` : undefined}
              icon={PackageCheck}
              tone="success"
              onClick={() => applyFilters({ ...EMPTY_PACK_FILTERS, costBasis: draft.costBasis, packStatus: "calculado" })}
            />
            <MetricCard
              label="Costo pendiente"
              value={summary?.packs_active_pending ?? "—"}
              tone={summary?.packs_active_pending ? "warning" : "success"}
              context="Packs únicos sin margen calculable"
              icon={PackageX}
            />
            <MetricCard
              label="Precio bajo costo"
              value={summary?.packs_by_problem.precio_bajo_costo ?? "—"}
              tone={summary?.packs_by_problem.precio_bajo_costo ? "danger" : "success"}
              context="Packs únicos con alguna lista afectada"
              icon={CircleDollarSign}
              onClick={() => applyFilters({ ...EMPTY_PACK_FILTERS, costBasis: draft.costBasis, problem: "precio_bajo_costo" })}
            />
            <MetricCard
              label="Bajo política (pack × lista)"
              value={summary?.combinations_by_margin_status.bajo_politica ?? "—"}
              tone={summary?.combinations_by_margin_status.bajo_politica ? "danger" : "success"}
              context="Combinaciones de pack y lista"
              icon={TrendingDown}
              onClick={() => applyFilters({ ...EMPTY_PACK_FILTERS, costBasis: draft.costBasis, marginStatus: "bajo_politica" })}
            />
          </section>

          {pendingStatuses.length ? (
            <SectionCard
              title="¿Qué impide calcular algunos packs?"
              description="Cada cantidad corresponde a packs únicos. Selecciona una causa para revisar los productos afectados."
              contentClassName="grid gap-3 sm:grid-cols-2 xl:grid-cols-4"
            >
                  {pendingStatuses.map(([s, n]) => (
                    <button
                      key={s}
                      type="button"
                      className="flex min-h-20 items-center justify-between gap-4 rounded-lg border border-slate-200 bg-slate-50/60 px-4 py-3 text-left transition hover:border-amber-300 hover:bg-amber-50/50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/30"
                      onClick={() => applyFilters({ ...EMPTY_PACK_FILTERS, costBasis: draft.costBasis, packStatus: s })}
                    >
                      <span>
                        <span className="block text-sm font-medium text-slate-900">{PACK_STATUS_LABELS[s]}</span>
                        <span className="mt-1 block text-xs text-slate-500">Ver packs afectados</span>
                      </span>
                      <span className="text-2xl font-semibold tabular-nums text-amber-800">{n}</span>
                    </button>
                  ))}
            </SectionCard>
          ) : null}

          <section className="space-y-3">
            <div>
              <h2 className="text-[19px] font-semibold tracking-[-0.015em] text-slate-950">Explorar packs</h2>
              <p className="mt-1 text-[13px] text-slate-500">
                Selecciona una lista para ver una fila por pack. Usa “Todas” cuando necesites comparar combinaciones pack × lista.
              </p>
            </div>
          <PackV2Filters
            draft={draft}
            priceLists={priceLists}
            loading={loadingRows}
            onChange={(patch) => setDraft((d) => ({ ...d, ...patch }))}
            onApply={() => applyFilters()}
            onClear={() => applyFilters(EMPTY_PACK_FILTERS)}
          />
          <PackV2Table items={rows?.items ?? []} loading={loadingRows} onOpen={setVariantId} />
          {rows ? <Pager page={rows.page} totalPages={rows.total_pages} total={rows.total} loading={loadingRows} onPage={setPage} /> : null}
          </section>
        </>
      )}

      <PackV2DetailSheet
        companyId={companyId}
        variantId={variantId}
        costBasis={applied.costBasis}
        onOpenChange={(open) => {
          if (!open) setVariantId(null)
        }}
      />
    </div>
  )
}
