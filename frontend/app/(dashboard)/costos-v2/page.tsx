"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { AlertTriangle, Loader2 } from "lucide-react"

import { AnalyticsPageHeader } from "@/components/analytics/analytics-page-header"
import { CostV2DetailDrawer } from "@/components/costos-v2/cost-v2-detail-drawer"
import {
  CostV2Filters,
  type CostV2FilterDraft,
} from "@/components/costos-v2/cost-v2-filters"
import { CostV2ReceptionsTable } from "@/components/costos-v2/cost-v2-receptions-table"
import { CostV2SummaryPanel } from "@/components/costos-v2/cost-v2-summary-panel"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
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
  getCostV2Receptions,
  getCostV2Summary,
  mergeReceptionItemsByHistoryId,
} from "@/lib/costos-v2/api"
import type {
  CostV2ReceptionListItem,
  CostV2SummaryBody,
} from "@/lib/costos-v2/types"
import {
  COST_V2_DEFAULT_LIMIT,
  COST_V2_DEFAULT_OFFICE_ID,
} from "@/lib/costos-v2/types"

function emptyDraft(range: { from: string; to: string }): CostV2FilterDraft {
  return {
    officeId: String(COST_V2_DEFAULT_OFFICE_ID),
    dateFrom: range.from,
    dateTo: range.to,
    search: "",
    status: "",
    warning: "",
    barcode: "",
  }
}

export default function CostosV2Page() {
  const range0 = useMemo(() => defaultCostV2DateRange(30), [])
  const [companies, setCompanies] = useState<Company[]>([])
  const [offices, setOffices] = useState<CostOfficeRef[]>([])
  const [companyId, setCompanyId] = useState<number | null>(null)

  const [draft, setDraft] = useState<CostV2FilterDraft>(() => emptyDraft(range0))
  const [applied, setApplied] = useState<CostV2FilterDraft | null>(null)

  const [summary, setSummary] = useState<CostV2SummaryBody | null>(null)
  const [items, setItems] = useState<CostV2ReceptionListItem[]>([])
  const [nextCursor, setNextCursor] = useState<string | null>(null)
  const [hasMore, setHasMore] = useState(false)

  const [loading, setLoading] = useState(false)
  const [loadingMore, setLoadingMore] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const [detailId, setDetailId] = useState<number | null>(null)
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
          setDraft((d) => ({
            ...d,
            officeId: String(prefer.office_id),
          }))
        }
      } catch {
        if (!cancelled) setOffices([])
      }
    })()
    return () => {
      cancelled = true
    }
  }, [companyId])

  // Primera carga automática cuando hay empresa + oficina + fechas.
  useEffect(() => {
    if (bootstrappedRef.current) return
    if (companyId == null || !draft.officeId || !draft.dateFrom || !draft.dateTo) {
      return
    }
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

  const loadPage = useCallback(
    async (mode: "replace" | "append", cursorForAppend: string | null = null) => {
      if (!companyId || !applied?.officeId || !applied.dateFrom || !applied.dateTo) {
        return
      }
      if (applied.dateFrom > applied.dateTo) {
        setError("La fecha desde debe ser ≤ fecha hasta.")
        return
      }

      abortRef.current?.abort()
      const ac = new AbortController()
      abortRef.current = ac

      if (mode === "replace") {
        setLoading(true)
        setItems([])
        setNextCursor(null)
        setHasMore(false)
      } else {
        if (!cursorForAppend) return
        setLoadingMore(true)
      }
      setError(null)

      const officeId = Number(applied.officeId)
      const base = {
        company_id: companyId,
        office_id: officeId,
        date_from: applied.dateFrom,
        date_to: applied.dateTo,
        status: applied.status || null,
        warning: applied.warning || null,
        barcode: applied.barcode || null,
        search: applied.search || null,
        limit: COST_V2_DEFAULT_LIMIT,
        signal: ac.signal,
      }

      try {
        const [sumRes, listRes] = await Promise.all([
          mode === "replace"
            ? getCostV2Summary(base)
            : Promise.resolve(null),
          getCostV2Receptions({
            ...base,
            cursor: mode === "append" ? cursorForAppend : null,
          }),
        ])

        if (ac.signal.aborted) return

        if (sumRes) setSummary(sumRes.summary)
        setHasMore(Boolean(listRes.page.has_more))
        setNextCursor(listRes.page.next_cursor)
        setItems((prev) =>
          mode === "replace"
            ? listRes.items
            : mergeReceptionItemsByHistoryId(prev, listRes.items),
        )
      } catch (e) {
        if (ac.signal.aborted) return
        if (e instanceof DOMException && e.name === "AbortError") return
        if (e instanceof CostV2ApiError) setError(e.message)
        else setError("Error de red. Verifique su conexión e intente nuevamente.")
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
    void loadPage("replace")
  }, [canQuery, applied, companyId, loadPage])

  const applyFilters = () => {
    if (!draft.officeId) {
      setError("Seleccione una oficina.")
      return
    }
    if (!draft.dateFrom || !draft.dateTo) {
      setError("Indique fecha desde y hasta.")
      return
    }
    if (draft.dateFrom > draft.dateTo) {
      setError("La fecha desde debe ser ≤ fecha hasta.")
      return
    }
    setItems([])
    setNextCursor(null)
    setHasMore(false)
    setApplied({ ...draft })
  }

  const clearFilters = () => {
    const next = emptyDraft(range0)
    if (offices.some((o) => o.office_id === COST_V2_DEFAULT_OFFICE_ID)) {
      next.officeId = String(COST_V2_DEFAULT_OFFICE_ID)
    } else if (offices[0]) {
      next.officeId = String(offices[0].office_id)
    }
    setDraft(next)
    setItems([])
    setNextCursor(null)
    setHasMore(false)
    setSummary(null)
    setApplied({ ...next })
  }

  const companyName =
    companies.find((c) => c.company_id === companyId)?.name ??
    (companyId != null ? `Empresa ${companyId}` : "—")

  return (
    <div className="mx-auto flex w-full max-w-[1400px] flex-col gap-6 p-4 md:p-6">
      <AnalyticsPageHeader
        title="Costos V2"
        subtitle="Vista de validación sobre el cálculo unitario corregido. No reemplaza /costos."
        meta={
          <span className="inline-flex items-center rounded-md border border-amber-300/70 bg-amber-50 px-2 py-0.5 text-amber-900 dark:border-amber-700 dark:bg-amber-950/40 dark:text-amber-100">
            Costos V2 — Vista de validación
          </span>
        }
        actions={[
          {
            label: "Actualizar",
            onClick: () => (applied ? void loadPage("replace") : applyFilters()),
            loading,
          },
        ]}
      />

      <p className="text-xs text-muted-foreground">
        Empresa: <span className="font-medium text-foreground">{companyName}</span>
        {" · "}
        Costos unitarios (sin agregados de impacto).
      </p>

      <CostV2Filters
        offices={offices}
        draft={draft}
        onChange={(patch) => setDraft((d) => ({ ...d, ...patch }))}
        onApply={applyFilters}
        onClear={clearFilters}
        loading={loading}
        disabled={companyId == null}
      />

      {error ? (
        <Alert variant="destructive">
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Error</AlertTitle>
          <AlertDescription className="flex flex-wrap items-center gap-3">
            <span>{error}</span>
            <Button
              type="button"
              size="sm"
              variant="outline"
              onClick={() => (applied ? void loadPage("replace") : applyFilters())}
            >
              Reintentar
            </Button>
          </AlertDescription>
        </Alert>
      ) : null}

      {!applied ? (
        <p className="rounded-md border border-dashed border-border/70 px-4 py-10 text-center text-sm text-muted-foreground">
          Configure oficina y fechas, luego pulse Actualizar.
        </p>
      ) : (
        <>
          <CostV2SummaryPanel summary={summary} loading={loading && !summary} />

          <div className="space-y-3">
            <div className="flex items-center justify-between gap-2">
              <h2 className="text-sm font-semibold">Recepciones</h2>
              {loading ? (
                <span className="inline-flex items-center gap-1.5 text-xs text-muted-foreground">
                  <Loader2 className="h-3.5 w-3.5 animate-spin" />
                  Cargando…
                </span>
              ) : (
                <span className="text-xs text-muted-foreground">
                  {items.length} filas en vista
                  {hasMore ? " · hay más" : ""}
                </span>
              )}
            </div>

            <CostV2ReceptionsTable
              items={items}
              loading={loading}
              onOpenDetail={(id) => {
                setDetailId(id)
                setDetailOpen(true)
              }}
            />

            {hasMore ? (
              <div className="flex justify-center">
                <Button
                  type="button"
                  variant="outline"
                  disabled={loadingMore || loading || !nextCursor}
                  onClick={() => void loadPage("append", nextCursor)}
                >
                  {loadingMore ? (
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  ) : null}
                  Cargar más
                </Button>
              </div>
            ) : null}
          </div>
        </>
      )}

      {companyId != null && applied?.officeId ? (
        <CostV2DetailDrawer
          open={detailOpen}
          onOpenChange={setDetailOpen}
          historyId={detailId}
          companyId={companyId}
          officeId={Number(applied.officeId)}
        />
      ) : null}
    </div>
  )
}
