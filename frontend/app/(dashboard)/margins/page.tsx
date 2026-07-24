"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import Link from "next/link"
import { Download, RefreshCw, Settings2 } from "lucide-react"

import { AnalyticsFilterBar, type FilterChip } from "@/components/analytics/analytics-filter-bar"
import { AnalyticsKpiCard } from "@/components/analytics/analytics-kpi-card"
import { AnalyticsPageHeader } from "@/components/analytics/analytics-page-header"
import {
  ComplianceByDimensionChart,
  StatusDistributionChart,
  TopBelowMinimumList,
} from "@/components/margins/price-control-charts"
import { PriceControlDetailDrawer } from "@/components/margins/price-control-detail-drawer"
import { PriceControlTable } from "@/components/margins/price-control-table"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
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
import {
  getCompanies,
  getPriceListControl,
  getPriceLists,
  getStoredCompanyId,
  type Company,
  type PriceListRef,
} from "@/lib/api"
import {
  adaptPriceControlRow,
  adaptPriceControlSummary,
  ageBucket,
  buildComplianceByList,
  buildComplianceByType,
  buildStatusDistribution,
  buildTopBelowMinimum,
  type PriceControlRow,
  type PriceControlSummary,
} from "@/lib/margins/adapt-price-control"
import {
  PRICE_POLICY_STATUS_LABEL,
  type PricePolicyStatus,
} from "@/lib/margins/price-policy"

const ALL = "__all__"
const PAGE_SIZE = 40

const STATUS_OPTIONS: { value: string; label: string }[] = [
  { value: ALL, label: "Todos los estados" },
  ...Object.entries(PRICE_POLICY_STATUS_LABEL).map(([value, label]) => ({
    value,
    label,
  })),
]

function downloadCsv(rows: PriceControlRow[], filename: string) {
  const headers = [
    "producto",
    "variante",
    "barcode",
    "tipo",
    "lista",
    "costo_bruto_max",
    "fecha_costo",
    "antiguedad_dias",
    "precio_bruto",
    "diferencia",
    "recargo_real_pct",
    "recargo_min_pct",
    "recargo_max_pct",
    "margen_sobre_precio_pct",
    "precio_min_recomendado",
    "precio_max_recomendado",
    "ajuste_al_minimo",
    "estado",
    "calidad_costo",
  ]
  const lines = [headers.join(",")]
  for (const r of rows) {
    lines.push(
      [
        JSON.stringify(r.productName ?? ""),
        JSON.stringify(r.variantName ?? ""),
        JSON.stringify(r.barcode ?? ""),
        JSON.stringify(r.productTypeName ?? ""),
        JSON.stringify(r.priceListName ?? ""),
        r.referenceGrossCost ?? "",
        r.costDate ?? "",
        r.costAgeDays ?? "",
        r.grossPrice ?? "",
        r.priceDiffVsCost ?? "",
        r.actualMarkupPct ?? "",
        r.minMarkupPct ?? "",
        r.maxMarkupPct ?? "",
        r.grossMarginPct ?? "",
        r.minimumRecommendedGrossPrice ?? "",
        r.maximumRecommendedGrossPrice ?? "",
        r.priceAdjustmentToMinimum ?? "",
        r.status,
        r.grossCostQuality ?? "",
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

export default function MarginsPage() {
  const [companies, setCompanies] = useState<Company[]>([])
  const [priceLists, setPriceLists] = useState<PriceListRef[]>([])
  const [companyId, setCompanyId] = useState("")
  const [priceListId, setPriceListId] = useState(ALL)
  const [statusFilter, setStatusFilter] = useState(ALL)
  const [ruleFilter, setRuleFilter] = useState(ALL)
  const [costFilter, setCostFilter] = useState(ALL)
  const [ageFilter, setAgeFilter] = useState(ALL)
  const [search, setSearch] = useState("")
  const [productTypeFilter, setProductTypeFilter] = useState(ALL)

  const [applied, setApplied] = useState({
    companyId: "",
    priceListId: ALL as string,
  })

  const [rows, setRows] = useState<PriceControlRow[]>([])
  const [summary, setSummary] = useState<PriceControlSummary | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [page, setPage] = useState(1)
  const [selected, setSelected] = useState<PriceControlRow | null>(null)
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
          list.find((c) => c.company_id === stored)?.company_id ?? list[0]?.company_id
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
        const lists = await getPriceLists(Number(companyId))
        if (!cancelled) setPriceLists(lists)
      } catch {
        if (!cancelled) setPriceLists([])
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
    try {
      const pl =
        applied.priceListId !== ALL && applied.priceListId
          ? Number(applied.priceListId)
          : null
      const res = await getPriceListControl(Number(applied.companyId), pl)
      setRows(res.items.map((r) => adaptPriceControlRow(r)))
      setSummary(adaptPriceControlSummary(res.summary))
      setPage(1)
    } catch (e) {
      setRows([])
      setSummary(null)
      setError(e instanceof Error ? e.message : "Error al cargar control de precios")
    } finally {
      setLoading(false)
    }
  }, [applied.companyId, applied.priceListId])

  useEffect(() => {
    void loadData()
  }, [loadData])

  const productTypes = useMemo(() => {
    const set = new Map<string, string>()
    for (const r of rows) {
      const key = r.productTypeId != null ? String(r.productTypeId) : ALL
      const label = r.productTypeName || "Sin tipo"
      if (r.productTypeId != null) set.set(key, label)
    }
    return [...set.entries()].map(([value, label]) => ({ value, label }))
  }, [rows])

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase()
    return rows.filter((r) => {
      if (statusFilter !== ALL && r.status !== statusFilter) return false
      if (ruleFilter === "with" && !r.hasRule) return false
      if (ruleFilter === "without" && r.hasRule) return false
      if (costFilter === "with" && (r.referenceGrossCost == null || r.referenceGrossCost <= 0)) {
        return false
      }
      if (costFilter === "without" && r.referenceGrossCost != null && r.referenceGrossCost > 0) {
        return false
      }
      if (ageFilter !== ALL && ageBucket(r.costAgeDays) !== ageFilter) return false
      if (productTypeFilter !== ALL && String(r.productTypeId) !== productTypeFilter) {
        return false
      }
      if (q) {
        const hay = [
          r.productName,
          r.variantName,
          r.barcode,
          r.sku,
          r.priceListName,
        ]
          .filter(Boolean)
          .join(" ")
          .toLowerCase()
        if (!hay.includes(q)) return false
      }
      return true
    })
  }, [
    rows,
    search,
    statusFilter,
    ruleFilter,
    costFilter,
    ageFilter,
    productTypeFilter,
  ])

  const chips: FilterChip[] = useMemo(() => {
    const out: FilterChip[] = []
    if (applied.priceListId !== ALL) {
      const pl = priceLists.find((p) => String(p.id) === applied.priceListId)
      out.push({
        id: "list",
        label: `Lista: ${pl?.name ?? applied.priceListId}`,
        onRemove: () => {
          setPriceListId(ALL)
          setApplied((a) => ({ ...a, priceListId: ALL }))
        },
      })
    }
    if (statusFilter !== ALL) {
      out.push({
        id: "status",
        label: `Estado: ${PRICE_POLICY_STATUS_LABEL[statusFilter as PricePolicyStatus] ?? statusFilter}`,
        onRemove: () => setStatusFilter(ALL),
      })
    }
    if (ruleFilter !== ALL) {
      out.push({
        id: "rule",
        label: ruleFilter === "with" ? "Con regla" : "Sin regla",
        onRemove: () => setRuleFilter(ALL),
      })
    }
    if (costFilter !== ALL) {
      out.push({
        id: "cost",
        label: costFilter === "with" ? "Con costo" : "Sin costo",
        onRemove: () => setCostFilter(ALL),
      })
    }
    if (ageFilter !== ALL) {
      out.push({
        id: "age",
        label: `Antigüedad: ${ageFilter}`,
        onRemove: () => setAgeFilter(ALL),
      })
    }
    if (productTypeFilter !== ALL) {
      const t = productTypes.find((x) => x.value === productTypeFilter)
      out.push({
        id: "type",
        label: `Tipo: ${t?.label ?? productTypeFilter}`,
        onRemove: () => setProductTypeFilter(ALL),
      })
    }
    if (search.trim()) {
      out.push({
        id: "search",
        label: `Búsqueda: ${search.trim()}`,
        onRemove: () => setSearch(""),
      })
    }
    return out
  }, [
    applied.priceListId,
    priceLists,
    statusFilter,
    ruleFilter,
    costFilter,
    ageFilter,
    productTypeFilter,
    productTypes,
    search,
  ])

  const byList = useMemo(() => buildComplianceByList(filtered), [filtered])
  const byType = useMemo(() => buildComplianceByType(filtered), [filtered])
  const statusDist = useMemo(() => buildStatusDistribution(filtered), [filtered])
  const topBelow = useMemo(() => buildTopBelowMinimum(filtered), [filtered])

  const filteredSummary = useMemo(() => {
    if (!summary) return null
    const within = filtered.filter((r) => r.status === "within_policy").length
    const total = filtered.length
    return {
      ...summary,
      evaluatedPairs: total,
      withinPolicy: within,
      withinPolicyPct: total ? Math.round((within / total) * 1000) / 10 : null,
      belowMinimum: filtered.filter((r) => r.status === "below_minimum").length,
      aboveMaximum: filtered.filter((r) => r.status === "above_maximum").length,
      missingRule: filtered.filter((r) => r.status === "missing_rule").length,
      missingCost: filtered.filter((r) => r.status === "missing_cost").length,
      staleCost: filtered.filter((r) => r.status === "stale_cost").length,
      needsReview: filtered.filter((r) => r.status !== "within_policy").length,
    }
  }, [summary, filtered])

  function applyFilters() {
    setApplied({
      companyId,
      priceListId,
    })
  }

  function clearFilters() {
    setPriceListId(ALL)
    setStatusFilter(ALL)
    setRuleFilter(ALL)
    setCostFilter(ALL)
    setAgeFilter(ALL)
    setProductTypeFilter(ALL)
    setSearch("")
    setApplied((a) => ({ ...a, priceListId: ALL }))
  }

  return (
    <div className="space-y-6 p-4 sm:p-6">
      <AnalyticsPageHeader
        title="Control de márgenes"
        subtitle="Cumplimiento de precios por lista según el costo bruto máximo registrado"
        actions={[
          {
            label: "Actualizar",
            onClick: () => void loadData(),
            loading,
            icon: <RefreshCw className="mr-1.5 h-3.5 w-3.5" />,
          },
          {
            label: "Exportar",
            onClick: () =>
              downloadCsv(
                filtered,
                `control-margenes-${applied.companyId || "empresa"}.csv`,
              ),
            disabled: !filtered.length,
            icon: <Download className="mr-1.5 h-3.5 w-3.5" />,
          },
          {
            label: "Abrir Política de Márgenes",
            onClick: () => {
              window.location.href = "/politica-margenes"
            },
            icon: <Settings2 className="mr-1.5 h-3.5 w-3.5" />,
            variant: "default",
          },
        ]}
      />

      <AnalyticsFilterBar
        chips={chips}
        onApply={applyFilters}
        onClear={clearFilters}
        applying={loading}
        advanced={
          <>
            <div className="space-y-1.5">
              <Label className="text-xs">Estado</Label>
              <Select value={statusFilter} onValueChange={setStatusFilter}>
                <SelectTrigger className="w-[200px]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {STATUS_OPTIONS.map((o) => (
                    <SelectItem key={o.value} value={o.value}>
                      {o.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-1.5">
              <Label className="text-xs">Regla</Label>
              <Select value={ruleFilter} onValueChange={setRuleFilter}>
                <SelectTrigger className="w-[160px]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value={ALL}>Todas</SelectItem>
                  <SelectItem value="with">Con regla</SelectItem>
                  <SelectItem value="without">Sin regla</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-1.5">
              <Label className="text-xs">Costo</Label>
              <Select value={costFilter} onValueChange={setCostFilter}>
                <SelectTrigger className="w-[160px]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value={ALL}>Todos</SelectItem>
                  <SelectItem value="with">Con costo</SelectItem>
                  <SelectItem value="without">Sin costo</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-1.5">
              <Label className="text-xs">Antigüedad del costo</Label>
              <Select value={ageFilter} onValueChange={setAgeFilter}>
                <SelectTrigger className="w-[180px]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value={ALL}>Todas</SelectItem>
                  <SelectItem value="0_30">0–30 días</SelectItem>
                  <SelectItem value="31_60">31–60 días</SelectItem>
                  <SelectItem value="61_90">61–90 días</SelectItem>
                  <SelectItem value="90_plus">+90 días</SelectItem>
                  <SelectItem value="unknown">Sin fecha</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-1.5">
              <Label className="text-xs">Tipo de producto</Label>
              <Select value={productTypeFilter} onValueChange={setProductTypeFilter}>
                <SelectTrigger className="w-[200px]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value={ALL}>Todos</SelectItem>
                  {productTypes.map((t) => (
                    <SelectItem key={t.value} value={t.value}>
                      {t.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </>
        }
      >
        <div className="space-y-1.5">
          <Label className="text-xs">Empresa</Label>
          <Select
            value={companyId}
            onValueChange={(v) => {
              setCompanyId(v)
              setPriceListId(ALL)
            }}
          >
            <SelectTrigger className="w-[220px]">
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
          <Label className="text-xs">Lista de precios</Label>
          <Select value={priceListId} onValueChange={setPriceListId}>
            <SelectTrigger className="w-[220px]">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value={ALL}>Todas las listas</SelectItem>
              {priceLists.map((pl) => (
                <SelectItem key={pl.id} value={String(pl.id)}>
                  {pl.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="space-y-1.5">
          <Label className="text-xs">Buscar</Label>
          <Input
            className="w-[240px]"
            placeholder="Nombre, código o barcode"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
        </div>
      </AnalyticsFilterBar>

      {error ? (
        <Alert variant="destructive">
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4 xl:grid-cols-4">
        <AnalyticsKpiCard
          title="Combinaciones evaluadas"
          value={filteredSummary?.evaluatedPairs ?? "—"}
          loading={loading}
          tooltip="Filas variante × lista de precios."
        />
        <AnalyticsKpiCard
          title="% dentro de política"
          value={
            filteredSummary?.withinPolicyPct != null
              ? `${filteredSummary.withinPolicyPct}%`
              : "—"
          }
          loading={loading}
        />
        <AnalyticsKpiCard
          title="Bajo el mínimo"
          value={filteredSummary?.belowMinimum ?? "—"}
          loading={loading}
        />
        <AnalyticsKpiCard
          title="Sobre el máximo"
          value={filteredSummary?.aboveMaximum ?? "—"}
          loading={loading}
        />
        <AnalyticsKpiCard
          title="Sin regla"
          value={filteredSummary?.missingRule ?? "—"}
          loading={loading}
        />
        <AnalyticsKpiCard
          title="Sin costo"
          value={filteredSummary?.missingCost ?? "—"}
          loading={loading}
        />
        <AnalyticsKpiCard
          title="Costos desactualizados"
          value={filteredSummary?.staleCost ?? "—"}
          loading={loading}
        />
        <AnalyticsKpiCard
          title="Requieren revisión"
          value={filteredSummary?.needsReview ?? "—"}
          loading={loading}
          tooltip="Todo lo que no está dentro de política."
        />
      </div>

      <div className="grid gap-4 lg:grid-cols-2 xl:grid-cols-4">
        <Card className="shadow-none xl:col-span-1">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">Cumplimiento por lista</CardTitle>
          </CardHeader>
          <CardContent>
            <ComplianceByDimensionChart data={byList} />
          </CardContent>
        </Card>
        <Card className="shadow-none xl:col-span-1">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">Cumplimiento por tipo</CardTitle>
          </CardHeader>
          <CardContent>
            <ComplianceByDimensionChart data={byType} />
          </CardContent>
        </Card>
        <Card className="shadow-none xl:col-span-1">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">Distribución de estados</CardTitle>
          </CardHeader>
          <CardContent>
            <StatusDistributionChart data={statusDist} />
          </CardContent>
        </Card>
        <Card className="shadow-none xl:col-span-1">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">
              Más alejados del mínimo
            </CardTitle>
          </CardHeader>
          <CardContent>
            <TopBelowMinimumList
              items={topBelow}
              onSelect={(key) => {
                const row = filtered.find((r) => `${r.variantId}-${r.priceListId}` === key)
                if (row) {
                  setSelected(row)
                  setDrawerOpen(true)
                }
              }}
            />
          </CardContent>
        </Card>
      </div>

      <Card className="shadow-none">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium">
            Precios por lista (variante × lista)
          </CardTitle>
        </CardHeader>
        <CardContent>
          <PriceControlTable
            rows={filtered}
            loading={loading}
            error={null}
            page={page}
            pageSize={PAGE_SIZE}
            onPageChange={setPage}
            onSelect={(row) => {
              setSelected(row)
              setDrawerOpen(true)
            }}
          />
        </CardContent>
      </Card>

      <p className="text-xs text-muted-foreground">
        Las reglas de{" "}
        <Link href="/politica-margenes" className="underline underline-offset-2">
          Política de Márgenes
        </Link>{" "}
        son porcentajes sobre costo (recargo). Etiquetas futuras sugeridas: «Recargo
        mínimo objetivo» / «Recargo máximo objetivo».
      </p>

      <PriceControlDetailDrawer
        open={drawerOpen}
        onOpenChange={setDrawerOpen}
        row={selected}
      />
    </div>
  )
}
