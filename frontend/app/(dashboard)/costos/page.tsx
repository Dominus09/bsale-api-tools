"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import {
  AlertTriangle,
  Download,
  RefreshCw,
} from "lucide-react"

import { AnalyticsFilterBar, type FilterChip } from "@/components/analytics/analytics-filter-bar"
import { AnalyticsKpiCard } from "@/components/analytics/analytics-kpi-card"
import { AnalyticsPageHeader } from "@/components/analytics/analytics-page-header"
import { CostDetailDrawer } from "@/components/costos/cost-detail-drawer"
import {
  CostAgeDistributionChart,
  CostHistoryChart,
  CostTopIncreasesList,
} from "@/components/costos/cost-history-chart"
import { CostMainTable } from "@/components/costos/cost-main-table"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Sheet, SheetContent, SheetHeader, SheetTitle } from "@/components/ui/sheet"
import {
  getCompanies,
  getCostAlerts,
  getCostAnalyticsDashboard,
  getCostOffices,
  getStoredCompanyId,
  listCostHistory,
  listCostOpportunities,
  syncCostAnalytics,
  type Company,
  type CostAnalyticsDashboard,
  type CostHistoryRow,
  type CostOfficeRef,
  type CostOpportunityRow,
} from "@/lib/api"
import {
  adaptDashboardKpis,
  aggregateGrossEvolution,
  buildAgeDistribution,
  buildTopIncreases,
  buildVariantAuditRows,
  type CostTableRow,
} from "@/lib/costos/adapt-cost-analytics"
import { formatDateTime, formatPct } from "@/lib/costos/format"
import {
  AGE_BUCKET_LABEL,
  GROSS_COST_QUALITY_LABEL,
  type AgeBucketKind,
  type GrossCostQualityKind,
} from "@/lib/costos/quality-labels"
import { useIsMobile } from "@/components/ui/use-mobile"

const ALL = "__all__"
const PAGE_SIZE = 40

function defaultDateRange() {
  const to = new Date()
  const from = new Date()
  from.setDate(from.getDate() - 90)
  const iso = (d: Date) => d.toISOString().slice(0, 10)
  return { from: iso(from), to: iso(to) }
}

function downloadCsv(rows: CostTableRow[], filename: string) {
  const headers = [
    "producto",
    "variante",
    "codigo",
    "fecha",
    "costo_neto",
    "iva",
    "otros_impuestos",
    "costo_bruto",
    "variacion_pct",
    "calidad",
    "antiguedad",
  ]
  const lines = [headers.join(",")]
  for (const r of rows) {
    lines.push(
      [
        JSON.stringify(r.productName ?? ""),
        JSON.stringify(r.variantName ?? ""),
        JSON.stringify(r.barcode ?? ""),
        r.lastReceptionDate ?? "",
        r.costNet ?? "",
        r.ivaAmount ?? "",
        r.otherTaxes ?? "",
        r.costGross ?? "",
        r.variationPct ?? "",
        GROSS_COST_QUALITY_LABEL[r.grossCostQuality],
        AGE_BUCKET_LABEL[r.ageBucket],
      ].join(","),
    )
  }
  const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8" })
  const url = URL.createObjectURL(blob)
  const a = document.createElement("a")
  a.href = url
  a.download = filename
  a.click()
  URL.revokeObjectURL(url)
}

export default function CostosPage() {
  const isMobile = useIsMobile()
  const range0 = useMemo(() => defaultDateRange(), [])

  const [companies, setCompanies] = useState<Company[]>([])
  const [offices, setOffices] = useState<CostOfficeRef[]>([])
  const [companyId, setCompanyId] = useState("")
  const [officeId, setOfficeId] = useState(ALL)
  const [dateFrom, setDateFrom] = useState(range0.from)
  const [dateTo, setDateTo] = useState(range0.to)
  const [search, setSearch] = useState("")
  const [costState, setCostState] = useState<string>(ALL)
  const [originFilter, setOriginFilter] = useState<string>(ALL)
  const [ageFilter, setAgeFilter] = useState<string>(ALL)

  const [applied, setApplied] = useState({
    companyId: "",
    officeId: ALL as string,
    dateFrom: range0.from,
    dateTo: range0.to,
    search: "",
    costState: ALL,
    origin: ALL,
    age: ALL,
  })

  const [dash, setDash] = useState<CostAnalyticsDashboard | null>(null)
  const [rawHistory, setRawHistory] = useState<CostHistoryRow[]>([])
  const [historyTotal, setHistoryTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [loading, setLoading] = useState(true)
  const [syncing, setSyncing] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [alertCount, setAlertCount] = useState(0)
  const [opportunities, setOpportunities] = useState<CostOpportunityRow[]>([])
  const [showAlerts, setShowAlerts] = useState(false)
  const [filtersOpen, setFiltersOpen] = useState(false)
  const [selected, setSelected] = useState<CostTableRow | null>(null)
  const [drawerOpen, setDrawerOpen] = useState(false)

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
          list[0]?.company_id
        if (def != null) {
          setCompanyId(String(def))
          setApplied((a) => ({ ...a, companyId: String(def) }))
        }
      } catch {
        if (!cancelled) setError("No se pudieron cargar las empresas.")
      }
    })()
    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    if (!companyId) return
    let cancelled = false
    ;(async () => {
      try {
        const res = await getCostOffices(Number(companyId))
        if (!cancelled) setOffices(res.items || [])
      } catch {
        if (!cancelled) setOffices([])
      }
    })()
    return () => {
      cancelled = true
    }
  }, [companyId])

  const loadData = useCallback(async () => {
    if (!applied.companyId) return
    setLoading(true)
    setError(null)
    const cid = Number(applied.companyId)
    const office =
      applied.officeId !== ALL ? Number(applied.officeId) : undefined
    try {
      const [dashboard, history, alerts, opps] = await Promise.all([
        getCostAnalyticsDashboard({
          company_id: cid,
          office_id: office,
          date_from: applied.dateFrom,
          date_to: applied.dateTo,
        }),
        listCostHistory({
          company_id: cid,
          office_id: office,
          date_from: applied.dateFrom,
          date_to: applied.dateTo,
          q: applied.search || undefined,
          limit: 500,
          offset: 0,
        }),
        getCostAlerts({ company_id: cid, limit: 50 }).catch(() => ({
          items: [] as never[],
        })),
        listCostOpportunities({ company_id: cid, limit: 20 }).catch(() => ({
          items: [] as CostOpportunityRow[],
        })),
      ])
      setDash(dashboard)
      setRawHistory(history.items || [])
      setHistoryTotal(history.total ?? history.items?.length ?? 0)
      setAlertCount(Array.isArray(alerts.items) ? alerts.items.length : 0)
      setOpportunities(opps.items || [])
    } catch (e) {
      setError(
        e instanceof Error
          ? e.message
          : "Error al cargar costos. Intente de nuevo.",
      )
      setRawHistory([])
    } finally {
      setLoading(false)
    }
  }, [applied])

  useEffect(() => {
    void loadData()
  }, [loadData])

  const tableAll = useMemo(() => buildVariantAuditRows(rawHistory), [rawHistory])

  const tableFiltered = useMemo(() => {
    return tableAll.filter((r) => {
      if (applied.costState !== ALL) {
        if (r.grossCostQuality !== (applied.costState as GrossCostQualityKind)) {
          return false
        }
      }
      if (applied.origin !== ALL && r.origin !== applied.origin) return false
      if (applied.age !== ALL && r.ageBucket !== (applied.age as AgeBucketKind)) {
        return false
      }
      return true
    })
  }, [tableAll, applied])

  const pageRows = useMemo(() => {
    const start = (page - 1) * PAGE_SIZE
    return tableFiltered.slice(start, start + PAGE_SIZE)
  }, [tableFiltered, page])

  const kpis = useMemo(
    () => adaptDashboardKpis(dash, tableFiltered),
    [dash, tableFiltered],
  )
  const ageDist = useMemo(
    () => buildAgeDistribution(tableFiltered),
    [tableFiltered],
  )
  const topUp = useMemo(() => buildTopIncreases(tableFiltered), [tableFiltered])
  const evolution = useMemo(
    () => aggregateGrossEvolution(rawHistory),
    [rawHistory],
  )

  const chips: FilterChip[] = useMemo(() => {
    const list: FilterChip[] = []
    if (applied.dateFrom || applied.dateTo) {
      list.push({
        id: "dates",
        label: `${applied.dateFrom} → ${applied.dateTo}`,
        onRemove: () => {
          const r = defaultDateRange()
          setDateFrom(r.from)
          setDateTo(r.to)
          setApplied((a) => ({ ...a, dateFrom: r.from, dateTo: r.to }))
          setPage(1)
        },
      })
    }
    if (applied.officeId !== ALL) {
      const name =
        offices.find((o) => String(o.office_id) === applied.officeId)
          ?.office_name || applied.officeId
      list.push({
        id: "office",
        label: `Oficina: ${name}`,
        onRemove: () => {
          setOfficeId(ALL)
          setApplied((a) => ({ ...a, officeId: ALL }))
          setPage(1)
        },
      })
    }
    if (applied.search) {
      list.push({
        id: "q",
        label: `Buscar: ${applied.search}`,
        onRemove: () => {
          setSearch("")
          setApplied((a) => ({ ...a, search: "" }))
          setPage(1)
        },
      })
    }
    if (applied.costState !== ALL) {
      list.push({
        id: "state",
        label: GROSS_COST_QUALITY_LABEL[applied.costState as GrossCostQualityKind],
        onRemove: () => {
          setCostState(ALL)
          setApplied((a) => ({ ...a, costState: ALL }))
          setPage(1)
        },
      })
    }
    if (applied.age !== ALL) {
      list.push({
        id: "age",
        label: AGE_BUCKET_LABEL[applied.age as AgeBucketKind],
        onRemove: () => {
          setAgeFilter(ALL)
          setApplied((a) => ({ ...a, age: ALL }))
          setPage(1)
        },
      })
    }
    return list
  }, [applied, offices])

  function applyFilters() {
    setApplied({
      companyId,
      officeId,
      dateFrom,
      dateTo,
      search: search.trim(),
      costState,
      origin: originFilter,
      age: ageFilter,
    })
    setPage(1)
    setFiltersOpen(false)
  }

  function clearFilters() {
    const r = defaultDateRange()
    setOfficeId(ALL)
    setDateFrom(r.from)
    setDateTo(r.to)
    setSearch("")
    setCostState(ALL)
    setOriginFilter(ALL)
    setAgeFilter(ALL)
    setApplied((a) => ({
      ...a,
      officeId: ALL,
      dateFrom: r.from,
      dateTo: r.to,
      search: "",
      costState: ALL,
      origin: ALL,
      age: ALL,
    }))
    setPage(1)
  }

  async function handleSync() {
    if (!companyId) return
    setSyncing(true)
    setError(null)
    try {
      await syncCostAnalytics({ company_id: Number(companyId) })
      await loadData()
    } catch (e) {
      setError(
        e instanceof Error ? e.message : "No se pudo sincronizar costos.",
      )
    } finally {
      setSyncing(false)
    }
  }

  const syncLabel = kpis.lastSyncAt
    ? `${formatDateTime(kpis.lastSyncAt)}${
        kpis.lastSyncStatus ? ` · ${kpis.lastSyncStatus}` : ""
      }`
    : "Sin sincronización registrada"

  const filterFields = (
    <>
      <div className="space-y-1.5">
        <Label htmlFor="costos-company">Empresa</Label>
        <Select
          value={companyId}
          onValueChange={(v) => {
            setCompanyId(v)
            setOfficeId(ALL)
          }}
        >
          <SelectTrigger id="costos-company" className="w-[200px]">
            <SelectValue placeholder="Empresa" />
          </SelectTrigger>
          <SelectContent>
            {companies.map((c) => (
              <SelectItem key={c.company_id} value={String(c.company_id)}>
                {c.name || `Empresa ${c.company_id}`}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
      <div className="space-y-1.5">
        <Label htmlFor="costos-from">Desde</Label>
        <Input
          id="costos-from"
          type="date"
          value={dateFrom}
          onChange={(e) => setDateFrom(e.target.value)}
          className="w-[150px]"
        />
      </div>
      <div className="space-y-1.5">
        <Label htmlFor="costos-to">Hasta</Label>
        <Input
          id="costos-to"
          type="date"
          value={dateTo}
          onChange={(e) => setDateTo(e.target.value)}
          className="w-[150px]"
        />
      </div>
      <div className="space-y-1.5">
        <Label htmlFor="costos-office">Oficina</Label>
        <Select value={officeId} onValueChange={setOfficeId}>
          <SelectTrigger id="costos-office" className="w-[180px]">
            <SelectValue placeholder="Todas" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value={ALL}>Todas</SelectItem>
            {offices.map((o) => (
              <SelectItem key={o.office_id} value={String(o.office_id)}>
                {o.office_name || `Oficina ${o.office_id}`}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
      <div className="min-w-[180px] flex-1 space-y-1.5">
        <Label htmlFor="costos-q">Buscar</Label>
        <Input
          id="costos-q"
          placeholder="Nombre, código o barras"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") applyFilters()
          }}
        />
      </div>
    </>
  )

  const advancedFields = (
    <>
      <div className="space-y-1.5">
        <Label>Estado del costo</Label>
        <Select value={costState} onValueChange={setCostState}>
          <SelectTrigger className="w-[220px]">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value={ALL}>Todos</SelectItem>
            {(
              Object.keys(GROSS_COST_QUALITY_LABEL) as GrossCostQualityKind[]
            ).map((k) => (
              <SelectItem key={k} value={k}>
                {GROSS_COST_QUALITY_LABEL[k]}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
      <div className="space-y-1.5">
        <Label>Origen</Label>
        <Select value={originFilter} onValueChange={setOriginFilter}>
          <SelectTrigger className="w-[200px]">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value={ALL}>Todos</SelectItem>
            <SelectItem value="reception_history">Recepción / compra</SelectItem>
            <SelectItem value="variant_cost">Costo actual Bsale</SelectItem>
          </SelectContent>
        </Select>
      </div>
      <div className="space-y-1.5">
        <Label>Antigüedad</Label>
        <Select value={ageFilter} onValueChange={setAgeFilter}>
          <SelectTrigger className="w-[180px]">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value={ALL}>Todas</SelectItem>
            {(Object.keys(AGE_BUCKET_LABEL) as AgeBucketKind[])
              .filter((k) => k !== "unknown")
              .map((k) => (
                <SelectItem key={k} value={k}>
                  {AGE_BUCKET_LABEL[k]}
                </SelectItem>
              ))}
          </SelectContent>
        </Select>
      </div>
      <div className="space-y-1.5 opacity-70">
        <Label>Categoría / tipo</Label>
        <Input disabled placeholder="Sin información en API" className="w-[200px]" />
      </div>
      <div className="space-y-1.5 opacity-70">
        <Label>Proveedor</Label>
        <Input disabled placeholder="Sin información en API" className="w-[200px]" />
      </div>
    </>
  )

  return (
    <div className="mx-auto flex w-full max-w-[1400px] flex-col gap-5 p-4 sm:p-6">
      <AnalyticsPageHeader
        title="Costos"
        subtitle="Auditoría e historial de costos de compra (neto, IVA, ILA/otros, bruto ERP). No es un módulo de ventas ni utilidad."
        meta={
          <span>
            Fuente: historial de recepciones · Fallback: costo actual Bsale · Última sync:{" "}
            <span className="font-medium text-foreground">{syncLabel}</span>
          </span>
        }
        actions={[
          {
            label: "Actualizar",
            onClick: () => void handleSync(),
            loading: syncing,
            variant: "default",
            icon: <RefreshCw className="mr-1.5 h-3.5 w-3.5" />,
          },
          {
            label: "Exportar",
            onClick: () =>
              downloadCsv(
                tableFiltered,
                `costos_${applied.dateFrom}_${applied.dateTo}.csv`,
              ),
            icon: <Download className="mr-1.5 h-3.5 w-3.5" />,
            disabled: tableFiltered.length === 0,
          },
          {
            label: alertCount ? `Alertas (${alertCount})` : "Ver alertas",
            onClick: () => setShowAlerts(true),
            icon: <AlertTriangle className="mr-1.5 h-3.5 w-3.5" />,
          },
        ]}
      />

      {error ? (
        <Alert variant="destructive">
          <AlertTitle>No se pudo cargar el módulo</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      {isMobile ? (
        <>
          <Button
            type="button"
            variant="outline"
            className="w-full"
            onClick={() => setFiltersOpen(true)}
          >
            Filtros
            {chips.length ? ` (${chips.length})` : ""}
          </Button>
          <Sheet open={filtersOpen} onOpenChange={setFiltersOpen}>
            <SheetContent side="bottom" className="max-h-[85vh] overflow-y-auto">
              <SheetHeader>
                <SheetTitle>Filtros de costos</SheetTitle>
              </SheetHeader>
              <div className="space-y-4 py-4">
                <AnalyticsFilterBar
                  chips={chips}
                  onApply={applyFilters}
                  onClear={clearFilters}
                  advanced={advancedFields}
                >
                  {filterFields}
                </AnalyticsFilterBar>
              </div>
            </SheetContent>
          </Sheet>
        </>
      ) : (
        <AnalyticsFilterBar
          chips={chips}
          onApply={applyFilters}
          onClear={clearFilters}
          advanced={advancedFields}
        >
          {filterFields}
        </AnalyticsFilterBar>
      )}

      <section
        className="grid grid-cols-2 gap-3 sm:grid-cols-2 lg:grid-cols-4"
        aria-label="Indicadores"
      >
        <AnalyticsKpiCard
          title="Variantes analizadas"
          value={kpis.variantsAnalyzed ?? "—"}
          tooltip="Variantes con actividad de costo en el período o catálogo monitoreado."
          loading={loading}
          delta={
            kpis.deltas.variantsAnalyzed != null
              ? `Δ ${kpis.deltas.variantsAnalyzed}`
              : "Δ período previo: sin información"
          }
        />
        <AnalyticsKpiCard
          title="Cobertura costo bruto"
          value={
            kpis.grossCoveragePct != null
              ? `${kpis.grossCoveragePct.toFixed(1)}%`
              : "—"
          }
          tooltip="Porcentaje de variantes con costo bruto conocido (preferente cost_bruto_erp)."
          loading={loading}
        />
        <AnalyticsKpiCard
          title="Sin costo"
          value={kpis.withoutCost ?? "—"}
          tooltip="Variantes del catálogo sin average_cost en variant_cost."
          loading={loading}
        />
        <AnalyticsKpiCard
          title="Actualizados 30 días"
          value={kpis.updatedLast30d ?? "—"}
          subtitle="Recepciones distintas"
          tooltip="Recepciones de compra en los últimos 30 días (dashboard)."
          loading={loading}
        />
        <AnalyticsKpiCard
          title="Antiguos +90 días"
          value={kpis.olderThan90d ?? "—"}
          tooltip="Última recepción con más de 90 días en el conjunto filtrado."
          loading={loading}
        />
        <AnalyticsKpiCard
          title="Variación promedio"
          value={formatPct(kpis.avgVariationPct)}
          tooltip="Promedio de variation_pct de la última recepción por variante."
          loading={loading}
        />
        <AnalyticsKpiCard
          title="Alzas importantes"
          value={kpis.productsWithMajorIncrease ?? "—"}
          tooltip="Productos con alza de costo >10% (KPI backend)."
          loading={loading}
        />
        <AnalyticsKpiCard
          title="Última sincronización"
          value={kpis.lastSyncStatus || "—"}
          subtitle={formatDateTime(kpis.lastSyncAt)}
          tooltip="Estado del job sync_cost_receptions para la empresa."
          loading={loading}
        />
      </section>

      <section className="grid gap-4 lg:grid-cols-2">
        <Card className="shadow-none">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">
              Evolución del costo bruto
            </CardTitle>
          </CardHeader>
          <CardContent>
            <CostHistoryChart data={evolution} />
          </CardContent>
        </Card>
        <Card className="shadow-none">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">
              Distribución por antigüedad
            </CardTitle>
          </CardHeader>
          <CardContent>
            <CostAgeDistributionChart data={ageDist} />
          </CardContent>
        </Card>
        <Card className="shadow-none">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">
              Top productos con mayor alza
            </CardTitle>
          </CardHeader>
          <CardContent>
            <CostTopIncreasesList items={topUp} />
          </CardContent>
        </Card>
        <Card className="shadow-none">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">
              Oportunidades y riesgos de compra
            </CardTitle>
          </CardHeader>
          <CardContent>
            <p className="mb-2 text-xs text-muted-foreground">
              Derivado de /cost-analytics (costos recientes vs promedios). No usa
              ventas.
            </p>
            {opportunities.length === 0 ? (
              <p className="py-8 text-center text-sm text-muted-foreground">
                Sin oportunidades detectadas en el período.
              </p>
            ) : (
              <ul className="divide-y divide-border/60">
                {opportunities.slice(0, 8).map((o) => (
                  <li
                    key={o.variant_id}
                    className="flex items-center justify-between gap-2 py-2 text-sm"
                  >
                    <button
                      type="button"
                      className="min-w-0 truncate text-left font-medium outline-none hover:underline focus-visible:ring-2 focus-visible:ring-ring"
                      onClick={() => {
                        const row = tableFiltered.find(
                          (r) => r.variantId === o.variant_id,
                        )
                        if (row) {
                          setSelected(row)
                          setDrawerOpen(true)
                        }
                      }}
                    >
                      {o.product_name || `Variante ${o.variant_id}`}
                    </button>
                    <span className="shrink-0 text-xs text-muted-foreground">
                      {o.status_label || o.status || "—"}
                    </span>
                  </li>
                ))}
              </ul>
            )}
          </CardContent>
        </Card>
      </section>

      <section aria-label="Tabla de costos" className="space-y-2">
        <h2 className="text-sm font-medium text-foreground">
          Último costo bruto por variante
        </h2>
        <CostMainTable
          rows={pageRows}
          loading={loading}
          error={null}
          page={page}
          pageSize={PAGE_SIZE}
          total={tableFiltered.length}
          onPageChange={setPage}
          onSelect={(row) => {
            setSelected(row)
            setDrawerOpen(true)
          }}
        />
        {historyTotal > rawHistory.length ? (
          <p className="text-xs text-muted-foreground">
            Mostrando hasta {rawHistory.length} líneas de historial de{" "}
            {historyTotal} en el servidor. Refine fechas si necesita más
            cobertura.
          </p>
        ) : null}
      </section>

      <CostDetailDrawer
        open={drawerOpen}
        onOpenChange={setDrawerOpen}
        row={selected}
        companyId={Number(applied.companyId) || 0}
      />

      <Sheet open={showAlerts} onOpenChange={setShowAlerts}>
        <SheetContent className="sm:max-w-md">
          <SheetHeader>
            <SheetTitle>Alertas de costo</SheetTitle>
          </SheetHeader>
          <p className="mt-4 text-sm text-muted-foreground">
            Hay {alertCount} alertas recientes para la empresa. Use la ficha de
            producto o el historial para revisar variaciones y costos faltantes.
          </p>
          <Button
            type="button"
            className="mt-4"
            variant="outline"
            onClick={() => setShowAlerts(false)}
          >
            Cerrar
          </Button>
        </SheetContent>
      </Sheet>
    </div>
  )
}
