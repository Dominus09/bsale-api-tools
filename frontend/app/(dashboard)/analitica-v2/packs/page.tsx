"use client"

import { useCallback, useEffect, useRef, useState } from "react"
import { AlertTriangle, Info } from "lucide-react"

import { Pager } from "@/components/analytics-v2/costs/cost-v2-products"
import { PackV2DetailSheet } from "@/components/analytics-v2/packs/pack-v2-detail-sheet"
import {
  EMPTY_PACK_FILTERS,
  PackV2Filters,
  PackV2Table,
  type PackFilterDraft,
} from "@/components/analytics-v2/packs/pack-v2-table"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { cn } from "@/lib/utils"
import { getStoredCompanyId } from "@/lib/api"
import { AnalyticsV2Error, getPackRows, getPacksSummary } from "@/lib/analytics-v2/api"
import { formatDateTime, formatPct } from "@/lib/analytics-v2/labels"
import { COST_BASIS_LABELS, MARGIN_STATUS_LABELS } from "@/lib/analytics-v2/margins-labels"
import { PACK_PROBLEM_LABELS, PACK_SORT_LABELS, PACK_STATUS_LABELS, PACK_STATUS_TONES } from "@/lib/analytics-v2/packs-labels"
import type { CostBasis, MarginStatus } from "@/lib/analytics-v2/margins-types"
import type { PackCostStatus, PackProblem, PackRowsResponse, PacksSummary } from "@/lib/analytics-v2/packs-types"
import { queryEnum, queryInt, queryText, readQuery } from "@/lib/analytics-v2/url"

const PAGE_SIZE = 50

function Kpi({
  label,
  value,
  hint,
  tone,
  onClick,
}: {
  label: string
  value: string | number
  hint?: string
  tone?: string
  onClick?: () => void
}) {
  return (
    <button
      type="button"
      disabled={!onClick}
      onClick={onClick}
      className={cn("rounded-md border p-3 text-left disabled:cursor-default", onClick && "hover:bg-muted/40", tone)}
    >
      <span className="block text-[11px] font-medium uppercase text-muted-foreground">{label}</span>
      <span className="block text-xl font-semibold tabular-nums">{value}</span>
      {hint ? <span className="block text-[11px] text-muted-foreground">{hint}</span> : null}
    </button>
  )
}

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
  const problems = summary
    ? (Object.entries(summary.packs_by_problem) as [PackProblem, number][]).filter(([, n]) => n > 0)
    : []
  const priceLists = rows?.price_lists ?? summary?.price_lists ?? []

  return (
    <div className="mx-auto flex w-full max-w-[1440px] flex-col gap-3 p-3 md:p-4">
      <header className="flex flex-col gap-1 border-b border-border/60 pb-3">
        <div className="flex flex-wrap items-center gap-2">
          <h1 className="text-xl font-semibold tracking-tight md:text-2xl">Rentabilidad de packs Bsale</h1>
          <span className="rounded-md border border-amber-300 bg-amber-50 px-1.5 py-0.5 text-[11px] font-medium text-amber-800">
            En validación
          </span>
        </div>
        <p className="text-sm text-muted-foreground">
          {summary?.meta.company_name ?? `Empresa ${companyId}`} · Composición: BSALE RAW (pack_details del producto) · Costo:
          suma de componentes con sus propios impuestos
        </p>
        <p className="text-xs text-muted-foreground">
          Calculado {formatDateTime(summary?.meta.generated_at)} · Composición leída entre {formatDateTime(summary?.composition_fetched.min)} y{" "}
          {formatDateTime(summary?.composition_fetched.max)}
        </p>
      </header>

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

          <Alert>
            <Info className="h-4 w-4" />
            <AlertTitle>Resultados provisionales</AlertTitle>
            <AlertDescription>
              El costo del pack es la suma de costo neto × cantidad de cada componente; el costo bruto aplica los impuestos de cada
              componente por ID Bsale. No se usa el costo promedio propio del SKU del pack ni sus impuestos propios. Un pack con
              componentes sin costo, inexistentes, abiertos, anidados no resolubles o con impuestos pendientes no tiene margen
              definitivo. Ser pack no elimina alertas: solo una promoción o remate aprobado y verificado justifica una excepción. La
              rentabilidad histórica de ventas de packs no está implementada.
            </AlertDescription>
          </Alert>

          <section className="grid gap-2 sm:grid-cols-2 lg:grid-cols-5">
            <Kpi label="Packs activos" value={loadingSummary && !summary ? "…" : (summary?.packs_active ?? "—")} hint={`${summary?.packs_total ?? "—"} packs en total`} />
            <Kpi
              label="Calculables"
              value={summary?.packs_active_calculable ?? "—"}
              hint={summary?.coverage_pct != null ? `Cobertura ${formatPct(summary.coverage_pct)}` : undefined}
              onClick={() => applyFilters({ ...EMPTY_PACK_FILTERS, costBasis: draft.costBasis, packStatus: "calculado" })}
            />
            <Kpi
              label="Costo pendiente"
              value={summary?.packs_active_pending ?? "—"}
              tone={summary?.packs_active_pending ? "border-amber-300 bg-amber-50/50" : undefined}
              hint="Sin margen definitivo"
            />
            <Kpi
              label="Packs con precio bajo costo"
              value={summary?.packs_by_problem.precio_bajo_costo ?? "—"}
              tone={summary?.packs_by_problem.precio_bajo_costo ? "border-red-300 bg-red-50/50" : undefined}
              onClick={() => applyFilters({ ...EMPTY_PACK_FILTERS, costBasis: draft.costBasis, problem: "precio_bajo_costo" })}
            />
            <Kpi
              label="Bajo política (pack × lista)"
              value={summary?.combinations_by_margin_status.bajo_politica ?? "—"}
              tone={summary?.combinations_by_margin_status.bajo_politica ? "border-red-300 bg-red-50/50" : undefined}
              onClick={() => applyFilters({ ...EMPTY_PACK_FILTERS, costBasis: draft.costBasis, marginStatus: "bajo_politica" })}
            />
          </section>

          {pendingStatuses.length || problems.length ? (
            <section className="grid gap-2 md:grid-cols-2">
              <div className="rounded-md border p-3">
                <h2 className="mb-2 text-sm font-semibold">Packs activos con costo pendiente, por causa</h2>
                <div className="flex flex-wrap gap-1.5">
                  {pendingStatuses.map(([s, n]) => (
                    <button
                      key={s}
                      type="button"
                      className={cn("rounded-md border px-2 py-1 text-xs hover:opacity-80", PACK_STATUS_TONES[s])}
                      onClick={() => applyFilters({ ...EMPTY_PACK_FILTERS, costBasis: draft.costBasis, packStatus: s })}
                    >
                      {PACK_STATUS_LABELS[s]}: <span className="font-semibold tabular-nums">{n}</span>
                    </button>
                  ))}
                  {!pendingStatuses.length ? <span className="text-xs text-muted-foreground">Sin pendientes.</span> : null}
                </div>
              </div>
              <div className="rounded-md border p-3">
                <h2 className="mb-2 text-sm font-semibold">Problemas detectados (packs activos)</h2>
                <div className="flex flex-wrap gap-1.5">
                  {problems.map(([p, n]) => (
                    <button
                      key={p}
                      type="button"
                      className="rounded-md border border-red-200 bg-red-50 px-2 py-1 text-xs text-red-800 hover:opacity-80"
                      onClick={() => applyFilters({ ...EMPTY_PACK_FILTERS, costBasis: draft.costBasis, problem: p })}
                    >
                      {PACK_PROBLEM_LABELS[p]}: <span className="font-semibold tabular-nums">{n}</span>
                    </button>
                  ))}
                  {!problems.length ? <span className="text-xs text-muted-foreground">Sin problemas detectados.</span> : null}
                </div>
                <p className="mt-2 text-[11px] text-muted-foreground">
                  Una diferencia extrema entre listas debe confirmarse en BSALE RAW antes de clasificarse como anomalía. No se cambian
                  precios automáticamente.
                </p>
              </div>
            </section>
          ) : null}

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
