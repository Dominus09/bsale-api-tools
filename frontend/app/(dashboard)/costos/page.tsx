"use client"

import Link from "next/link"
import { useCallback, useEffect, useMemo, useState } from "react"
import {
  AlertTriangle,
  CircleDollarSign,
  Loader2,
  RefreshCw,
  Search,
  TrendingDown,
  TrendingUp,
} from "lucide-react"

import {
  getCompanies,
  getCostAlerts,
  getCostAnalyticsDashboard,
  getCostReceptionDetail,
  getCostReceptions,
  getStoredCompanyId,
  listCostBranchComparison,
  listCostHistory,
  listCostOpportunities,
  listCostProducts,
  searchCostHistory,
  syncCostAnalytics,
  type Company,
  type CostAlertRow,
  type CostBranchComparisonRow,
  type CostHistoryRow,
  type CostHistorySearchHit,
  type CostOfficeRef,
  type CostOpportunityRow,
  type CostProductRow,
  type CostReceptionDetail,
  type CostReceptionRow,
} from "@/lib/api"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
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
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { cn } from "@/lib/utils"

const ALL = "__all__"

function formatMoney(value: number | null | undefined) {
  if (value == null || !Number.isFinite(Number(value))) return "—"
  return new Intl.NumberFormat("es-CL", {
    style: "currency",
    currency: "CLP",
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(Number(value))
}

function formatPct(value: number | null | undefined) {
  if (value == null || !Number.isFinite(Number(value))) return "—"
  const n = Number(value)
  return `${n > 0 ? "+" : ""}${n.toFixed(2)}%`
}

function formatDate(value: string | null | undefined) {
  if (!value) return "—"
  const d = new Date(value)
  if (Number.isNaN(d.getTime())) return value
  return d.toLocaleString("es-CL", { dateStyle: "short", timeStyle: "short" })
}

function variationClass(pct: number | null | undefined) {
  if (pct == null) return ""
  const av = Math.abs(Number(pct))
  if (av >= 20) return "text-red-600 dark:text-red-400 font-medium"
  if (av >= 10) return "text-amber-600 dark:text-amber-500"
  return ""
}

const ALERT_LABELS: Record<string, string> = {
  no_history: "Sin historial",
  missing_cost: "Costo faltante",
  zero_cost: "Costo cero",
  variation_10: "Variación >10%",
  variation_20: "Variación >20%",
  anomalous_cost: "Costo anómalo",
  suspicious_reception: "Recepción sospechosa",
  cross_branch_diff: "Diferencia entre sucursales",
  cost_decrease_10: "Baja de costo >10%",
}

const RECEPTION_TYPE_LABELS: Record<string, string> = {
  recepcion_normal: "Normal",
  recepcion_ajuste: "Ajuste",
  recepcion_devolucion: "Devolución",
  recepcion_nc: "NC",
}

const OPPORTUNITY_STATUS: Record<string, { label: string; className: string }> = {
  oportunidad_compra: { label: "Comprar", className: "bg-green-100 text-green-800 dark:bg-green-950 dark:text-green-300" },
  riesgo_comercial: { label: "Costo elevado", className: "bg-red-100 text-red-800 dark:bg-red-950 dark:text-red-300" },
}

function SemaphoreDot({ level }: { level: "green" | "yellow" | "red" }) {
  const cls =
    level === "red"
      ? "bg-red-500"
      : level === "yellow"
        ? "bg-amber-400"
        : "bg-green-500"
  return <span className={cn("inline-block h-3 w-3 rounded-full", cls)} title={level} />
}

function KpiCard({ title, value, hint }: { title: string; value: number | string; hint?: string }) {
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium text-muted-foreground">{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <p className="text-2xl font-semibold tabular-nums">{value}</p>
        {hint ? <p className="mt-1 text-xs text-muted-foreground">{hint}</p> : null}
      </CardContent>
    </Card>
  )
}

function OfficeFilter({
  offices,
  value,
  onChange,
}: {
  offices: CostOfficeRef[]
  value: string
  onChange: (v: string) => void
}) {
  return (
    <div>
      <Label>Sucursal</Label>
      <Select value={value} onValueChange={onChange}>
        <SelectTrigger>
          <SelectValue placeholder="Todas" />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value={ALL}>Todas las sucursales</SelectItem>
          {offices.map((o) => (
            <SelectItem key={o.office_id} value={String(o.office_id)}>
              {o.office_name ?? `Sucursal ${o.office_id}`}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  )
}

function DateFilters({
  dateFrom,
  dateTo,
  onFrom,
  onTo,
}: {
  dateFrom: string
  dateTo: string
  onFrom: (v: string) => void
  onTo: (v: string) => void
}) {
  return (
    <>
      <div>
        <Label>Desde</Label>
        <Input type="date" value={dateFrom} onChange={(e) => onFrom(e.target.value)} />
      </div>
      <div>
        <Label>Hasta</Label>
        <Input type="date" value={dateTo} onChange={(e) => onTo(e.target.value)} />
      </div>
    </>
  )
}

function HistoryTable({ rows }: { rows: CostHistoryRow[] }) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Fecha</TableHead>
          <TableHead>Empresa</TableHead>
          <TableHead>Sucursal</TableHead>
          <TableHead>Producto / Variante</TableHead>
          <TableHead>Documento</TableHead>
          <TableHead className="text-right">Cant.</TableHead>
          <TableHead className="text-right">Neto</TableHead>
          <TableHead className="text-right">IVA</TableHead>
          <TableHead className="text-right">Bruto ERP</TableHead>
          <TableHead className="text-right">Promedio</TableHead>
          <TableHead className="text-right">Var. %</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {rows.map((r) => (
          <TableRow key={r.reception_detail_id}>
            <TableCell>{formatDate(r.admission_date)}</TableCell>
            <TableCell>{r.company_name ?? "—"}</TableCell>
            <TableCell>{r.office_name ?? "—"}</TableCell>
            <TableCell>
              <div>{r.product_name ?? "—"}</div>
              <div className="text-xs text-muted-foreground">{r.variant_name ?? ""}</div>
            </TableCell>
            <TableCell>
              {r.document ?? ""} {r.document_number ?? ""}
              <div className="text-xs text-muted-foreground">Rec. #{r.reception_id}</div>
            </TableCell>
            <TableCell className="text-right tabular-nums">{r.quantity}</TableCell>
            <TableCell className="text-right">{formatMoney(r.cost_net)}</TableCell>
            <TableCell className="text-right">{formatMoney(r.iva_amount)}</TableCell>
            <TableCell className="text-right">{formatMoney(r.cost_bruto_erp)}</TableCell>
            <TableCell className="text-right">{formatMoney(r.average_cost)}</TableCell>
            <TableCell className={cn("text-right", variationClass(r.variation_pct))}>
              {formatPct(r.variation_pct)}
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  )
}

export default function CostosPage() {
  const [companies, setCompanies] = useState<Company[]>([])
  const [companyId, setCompanyId] = useState<number | null>(null)
  const [offices, setOffices] = useState<CostOfficeRef[]>([])
  const [officeFilter, setOfficeFilter] = useState(ALL)
  const [dateFrom, setDateFrom] = useState("")
  const [dateTo, setDateTo] = useState("")
  const [tab, setTab] = useState("dashboard")

  const filterParams = useMemo(
    () => ({
      company_id: companyId ?? undefined,
      office_id: officeFilter !== ALL ? Number(officeFilter) : undefined,
      date_from: dateFrom || undefined,
      date_to: dateTo || undefined,
    }),
    [companyId, officeFilter, dateFrom, dateTo],
  )

  useEffect(() => {
    void getCompanies()
      .then((list) => {
        setCompanies(list)
        const stored = getStoredCompanyId()
        if (stored && list.some((c) => c.company_id === stored)) setCompanyId(stored)
        else if (list.length > 0) setCompanyId(list[0].company_id)
      })
      .catch(() => setCompanies([]))
  }, [])

  const onCompanyChange = (id: number) => {
    setCompanyId(id)
    if (typeof window !== "undefined") {
      localStorage.setItem("company_id", String(id))
      const name = companies.find((c) => c.company_id === id)?.name
      if (name) localStorage.setItem("company_name", name)
    }
    setOfficeFilter(ALL)
  }

  useEffect(() => {
    if (companyId == null) return
    void getCostAnalyticsDashboard({ company_id: companyId })
      .then((d) => setOffices(d.offices ?? []))
      .catch(() => setOffices([]))
  }, [companyId])

  const companyLabel = useMemo(() => {
    if (companyId == null) return ""
    return companies.find((c) => c.company_id === companyId)?.name ?? `Empresa ${companyId}`
  }, [companies, companyId])

  if (companyId == null) {
    return (
      <div className="p-6">
        <Alert>
          <AlertDescription>Seleccione una empresa para ver costos.</AlertDescription>
        </Alert>
      </div>
    )
  }

  return (
    <div className="space-y-6 p-6">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <h1 className="flex items-center gap-2 text-2xl font-semibold tracking-tight">
            <CircleDollarSign className="h-7 w-7" />
            Costos
          </h1>
          <p className="text-sm text-muted-foreground">
            Centro de inteligencia de compras y costos — {companyLabel}
          </p>
        </div>
        <Select value={String(companyId)} onValueChange={(v) => onCompanyChange(Number(v))}>
          <SelectTrigger className="w-[260px]">
            <SelectValue placeholder="Empresa" />
          </SelectTrigger>
          <SelectContent>
            {companies.map((c) => (
              <SelectItem key={c.company_id} value={String(c.company_id)}>
                {c.name}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <Tabs value={tab} onValueChange={setTab}>
        <TabsList className="flex h-auto flex-wrap">
          <TabsTrigger value="dashboard">Dashboard</TabsTrigger>
          <TabsTrigger value="productos">Productos</TabsTrigger>
          <TabsTrigger value="recepciones">Recepciones</TabsTrigger>
          <TabsTrigger value="alertas">Alertas</TabsTrigger>
          <TabsTrigger value="oportunidades">Oportunidades</TabsTrigger>
          <TabsTrigger value="comparacion">Comparación sucursales</TabsTrigger>
          <TabsTrigger value="historial">Historial</TabsTrigger>
        </TabsList>

        <TabsContent value="dashboard" className="mt-4">
          <DashboardTab companyId={companyId} offices={offices} filterParams={filterParams} />
        </TabsContent>
        <TabsContent value="productos" className="mt-4">
          <ProductosTab companyId={companyId} />
        </TabsContent>
        <TabsContent value="recepciones" className="mt-4">
          <RecepcionesTab companyId={companyId} offices={offices} filterParams={filterParams} />
        </TabsContent>
        <TabsContent value="alertas" className="mt-4">
          <AlertasTab companyId={companyId} offices={offices} officeFilter={officeFilter} />
        </TabsContent>
        <TabsContent value="oportunidades" className="mt-4">
          <OportunidadesTab companyId={companyId} />
        </TabsContent>
        <TabsContent value="comparacion" className="mt-4">
          <ComparacionTab companyId={companyId} />
        </TabsContent>
        <TabsContent value="historial" className="mt-4">
          <HistorialTab companyId={companyId} offices={offices} filterParams={filterParams} />
        </TabsContent>
      </Tabs>
    </div>
  )
}

function DashboardTab({
  companyId,
  offices,
  filterParams,
}: {
  companyId: number
  offices: CostOfficeRef[]
  filterParams: {
    company_id?: number
    office_id?: number
    date_from?: string
    date_to?: string
  }
}) {
  const [loading, setLoading] = useState(true)
  const [syncing, setSyncing] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [kpis, setKpis] = useState<Record<string, number | null | undefined> | null>(null)
  const [lastSync, setLastSync] = useState<{
    last_run_at?: string | null
    last_status?: string | null
    last_message?: string | null
    total_lines_processed?: number
  } | null>(null)
  const [officeFilter, setOfficeFilter] = useState(ALL)
  const [dateFrom, setDateFrom] = useState("")
  const [dateTo, setDateTo] = useState("")

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const data = await getCostAnalyticsDashboard({
        company_id: companyId,
        office_id: officeFilter !== ALL ? Number(officeFilter) : undefined,
        date_from: dateFrom || undefined,
        date_to: dateTo || undefined,
      })
      setKpis(data.kpis)
      setLastSync(data.last_sync)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error al cargar dashboard")
    } finally {
      setLoading(false)
    }
  }, [companyId, officeFilter, dateFrom, dateTo])

  useEffect(() => {
    void load()
  }, [load])

  return (
    <div className="space-y-4">
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <OfficeFilter offices={offices} value={officeFilter} onChange={setOfficeFilter} />
        <DateFilters dateFrom={dateFrom} dateTo={dateTo} onFrom={setDateFrom} onTo={setDateTo} />
        <div className="flex items-end gap-2">
          <Button variant="outline" onClick={() => void load()}>
            Aplicar filtros
          </Button>
          <Button
            variant="outline"
            onClick={() => {
              setSyncing(true)
              void syncCostAnalytics({ company_id: companyId })
                .then(() => load())
                .catch((e: unknown) =>
                  setError(e instanceof Error ? e.message : "Error al sincronizar"),
                )
                .finally(() => setSyncing(false))
            }}
            disabled={syncing}
          >
            {syncing ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
          </Button>
        </div>
      </div>

      <div className="text-sm text-muted-foreground">
        Último sync: <strong>{formatDate(lastSync?.last_run_at)}</strong>
        {lastSync?.last_status ? (
          <Badge variant="outline" className="ml-2">
            {lastSync.last_status}
          </Badge>
        ) : null}
        {lastSync?.last_message ? <span className="ml-2">— {lastSync.last_message}</span> : null}
        {lastSync?.total_lines_processed != null ? (
          <span className="ml-2">· Líneas acumuladas: {lastSync.total_lines_processed}</span>
        ) : null}
      </div>

      {error ? (
        <Alert variant="destructive">
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      {loading ? (
        <div className="flex justify-center py-12">
          <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
        </div>
      ) : (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          <KpiCard
            title="Costo promedio empresa"
            value={formatMoney(kpis?.avg_cost_company ?? undefined)}
          />
          <KpiCard title="Productos monitoreados" value={kpis?.products_monitored ?? kpis?.with_cost ?? 0} />
          <KpiCard title="Recepciones 30 días" value={kpis?.receptions_30d ?? 0} />
          <KpiCard title="Alertas activas" value={(kpis?.variation_gt_10 ?? 0) + (kpis?.variation_gt_20 ?? 0)} />
          <KpiCard title="Productos con alza" value={kpis?.products_cost_up ?? kpis?.variation_gt_10 ?? 0} hint=">10% vs anterior" />
          <KpiCard title="Productos con baja" value={kpis?.products_cost_down ?? 0} hint=">10% vs anterior" />
          <KpiCard title="Oportunidades detectadas" value={kpis?.opportunities_detected ?? 0} />
          <KpiCard title="Líneas en historial" value={kpis?.lines_processed ?? 0} />
        </div>
      )}
    </div>
  )
}

function HistorialTab({
  companyId,
  offices,
}: {
  companyId: number
  offices: CostOfficeRef[]
  filterParams: object
}) {
  const [q, setQ] = useState("")
  const [rows, setRows] = useState<CostHistoryRow[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [officeFilter, setOfficeFilter] = useState(ALL)
  const [hits, setHits] = useState<CostHistorySearchHit[]>([])

  const search = async () => {
    setLoading(true)
    setError(null)
    try {
      if (q.trim().length >= 2) {
        const found = await searchCostHistory(q, companyId)
        setHits(found.items)
        const hist = await listCostHistory({
          company_id: companyId,
          q: q.trim(),
          office_id: officeFilter !== ALL ? Number(officeFilter) : undefined,
          limit: 200,
        })
        setRows(hist.items)
      } else {
        const hist = await listCostHistory({
          company_id: companyId,
          office_id: officeFilter !== ALL ? Number(officeFilter) : undefined,
          limit: 100,
        })
        setRows(hist.items)
        setHits([])
      }
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error en historial")
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void search()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [companyId])

  return (
    <div className="space-y-4">
      <div className="grid gap-3 sm:grid-cols-4">
        <div className="sm:col-span-2">
          <Label>Búsqueda (código, producto, variante)</Label>
          <Input
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="Mínimo 2 caracteres…"
            onKeyDown={(e) => e.key === "Enter" && void search()}
          />
        </div>
        <OfficeFilter offices={offices} value={officeFilter} onChange={setOfficeFilter} />
        <div className="flex items-end">
          <Button onClick={() => void search()} disabled={loading}>
            {loading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Search className="mr-2 h-4 w-4" />}
            Buscar
          </Button>
        </div>
      </div>

      {error ? <Alert variant="destructive"><AlertDescription>{error}</AlertDescription></Alert> : null}

      {hits.length > 0 ? (
        <div className="flex flex-wrap gap-2">
          {hits.map((h) => (
            <Button key={h.variant_id} size="sm" variant="outline" asChild>
              <Link href={`/costos/productos/${h.variant_id}?company_id=${companyId}`}>
                {h.product_name} — {h.variant_name}
              </Link>
            </Button>
          ))}
        </div>
      ) : null}

      {rows.length > 0 ? <HistoryTable rows={rows} /> : (
        !loading && <p className="text-sm text-muted-foreground">Sin registros. Ejecute sincronización en Dashboard.</p>
      )}
    </div>
  )
}

function RecepcionesTab({
  companyId,
  offices,
}: {
  companyId: number
  offices: CostOfficeRef[]
  filterParams: object
}) {
  const [rows, setRows] = useState<CostReceptionRow[]>([])
  const [loading, setLoading] = useState(true)
  const [detail, setDetail] = useState<CostReceptionDetail | null>(null)
  const [officeFilter, setOfficeFilter] = useState(ALL)
  const [dateFrom, setDateFrom] = useState("")
  const [dateTo, setDateTo] = useState("")
  const [docFilter, setDocFilter] = useState("")

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const data = await getCostReceptions({
        company_id: companyId,
        office_id: officeFilter !== ALL ? Number(officeFilter) : undefined,
        date_from: dateFrom || undefined,
        date_to: dateTo || undefined,
        document_type: docFilter || undefined,
      })
      setRows(data.items)
    } finally {
      setLoading(false)
    }
  }, [companyId, officeFilter, dateFrom, dateTo, docFilter])

  useEffect(() => {
    void load()
  }, [load])

  return (
    <div className="space-y-4">
      <div className="grid gap-3 sm:grid-cols-5">
        <OfficeFilter offices={offices} value={officeFilter} onChange={setOfficeFilter} />
        <DateFilters dateFrom={dateFrom} dateTo={dateTo} onFrom={setDateFrom} onTo={setDateTo} />
        <div>
          <Label>Documento</Label>
          <Input value={docFilter} onChange={(e) => setDocFilter(e.target.value)} placeholder="FACTURA…" />
        </div>
        <div className="flex items-end">
          <Button variant="outline" onClick={() => void load()}>Filtrar</Button>
        </div>
      </div>

      {loading ? (
        <Loader2 className="mx-auto h-6 w-6 animate-spin" />
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Fecha</TableHead>
              <TableHead>Empresa</TableHead>
              <TableHead>Sucursal</TableHead>
              <TableHead>Documento</TableHead>
              <TableHead>Tipo</TableHead>
              <TableHead className="text-right">Productos</TableHead>
              <TableHead className="text-right">Unidades</TableHead>
              <TableHead className="text-right">Costo total</TableHead>
              <TableHead />
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((r) => (
              <TableRow key={r.reception_id}>
                <TableCell>{formatDate(r.admission_date)}</TableCell>
                <TableCell>{r.company_name ?? "—"}</TableCell>
                <TableCell>{r.office_name ?? "—"}</TableCell>
                <TableCell>{r.document ?? ""} {r.document_number ?? ""}</TableCell>
                <TableCell>
                  {r.reception_type ? (
                    <Badge variant="outline">
                      {RECEPTION_TYPE_LABELS[r.reception_type] ?? r.reception_type}
                    </Badge>
                  ) : (
                    "—"
                  )}
                </TableCell>
                <TableCell className="text-right">{r.products_count}</TableCell>
                <TableCell className="text-right">{r.total_quantity}</TableCell>
                <TableCell className="text-right">{formatMoney(r.total_cost_bruto ?? r.total_cost_net)}</TableCell>
                <TableCell>
                  <Button size="sm" variant="ghost" onClick={() => void getCostReceptionDetail(r.reception_id, companyId).then(setDetail)}>
                    Detalle
                  </Button>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}

      {detail ? (
        <Card>
          <CardHeader className="flex flex-row justify-between">
            <CardTitle className="text-base">Recepción #{detail.reception_id}</CardTitle>
            <Button size="sm" variant="ghost" onClick={() => setDetail(null)}>Cerrar</Button>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Producto</TableHead>
                  <TableHead className="text-right">Cant.</TableHead>
                  <TableHead className="text-right">Neto</TableHead>
                  <TableHead className="text-right">IVA</TableHead>
                  <TableHead className="text-right">Bruto</TableHead>
                  <TableHead className="text-right">Var. %</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {detail.items.map((line) => (
                  <TableRow key={line.reception_detail_id}>
                    <TableCell>{line.product_name} — {line.variant_name}</TableCell>
                    <TableCell className="text-right">{line.quantity}</TableCell>
                    <TableCell className="text-right">{formatMoney(line.cost_net)}</TableCell>
                    <TableCell className="text-right">{formatMoney(line.iva_amount)}</TableCell>
                    <TableCell className="text-right">{formatMoney(line.cost_bruto_erp)}</TableCell>
                    <TableCell className={cn("text-right", variationClass(line.variation_pct))}>
                      {formatPct(line.variation_pct)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      ) : null}
    </div>
  )
}

function AlertasTab({
  companyId,
  offices,
  officeFilter: _,
}: {
  companyId: number
  offices: CostOfficeRef[]
  officeFilter: string
}) {
  const [items, setItems] = useState<CostAlertRow[]>([])
  const [loading, setLoading] = useState(true)
  const [officeFilter, setOfficeFilter] = useState(ALL)

  useEffect(() => {
    void (async () => {
      setLoading(true)
      try {
        const data = await getCostAlerts({
          company_id: companyId,
          office_id: officeFilter !== ALL ? Number(officeFilter) : undefined,
        })
        setItems(data.items)
      } finally {
        setLoading(false)
      }
    })()
  }, [companyId, officeFilter])

  return (
    <div className="space-y-4">
      <div className="max-w-xs">
        <OfficeFilter offices={offices} value={officeFilter} onChange={setOfficeFilter} />
      </div>
      {loading ? (
        <Loader2 className="mx-auto h-6 w-6 animate-spin" />
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead />
              <TableHead>Producto</TableHead>
              <TableHead>Sucursal</TableHead>
              <TableHead className="text-right">Costo</TableHead>
              <TableHead className="text-right">Var. %</TableHead>
              <TableHead>Alertas</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {items.length === 0 ? (
              <TableRow>
                <TableCell colSpan={6} className="text-center text-muted-foreground">
                  Sin alertas activas.
                </TableCell>
              </TableRow>
            ) : (
              items.map((a) => (
                <TableRow key={`${a.variant_id}-${a.office_id ?? 0}`}>
                  <TableCell><SemaphoreDot level={a.semaphore} /></TableCell>
                  <TableCell>{a.product_name} — {a.variant_name}</TableCell>
                  <TableCell>{a.office_name ?? "—"}</TableCell>
                  <TableCell className="text-right">{formatMoney(a.cost_net)}</TableCell>
                  <TableCell className={cn("text-right", variationClass(a.variation_pct))}>
                    {formatPct(a.variation_pct)}
                  </TableCell>
                  <TableCell>
                    <div className="flex flex-wrap gap-1">
                      {a.alert_types.map((t) => (
                        <Badge key={t} variant="outline" className="text-xs">
                          {ALERT_LABELS[t] ?? t}
                        </Badge>
                      ))}
                    </div>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      )}
    </div>
  )
}

function ComparacionTab({ companyId }: { companyId: number }) {
  const [q, setQ] = useState("")
  const [items, setItems] = useState<CostBranchComparisonRow[]>([])
  const [selected, setSelected] = useState<CostBranchComparisonRow | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const data = await listCostBranchComparison({
        company_id: companyId,
        q: q.trim().length >= 2 ? q.trim() : undefined,
        limit: 20,
      })
      setItems(data.items)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error al comparar")
    } finally {
      setLoading(false)
    }
  }, [companyId, q])

  useEffect(() => {
    void load()
  }, [load])

  return (
    <div className="space-y-4">
      <p className="text-sm text-muted-foreground">
        Variación interna entre sucursales por producto. Semáforo: 0–3% verde, 3–10% amarillo, &gt;10% rojo.
      </p>
      <div className="flex gap-2">
        <Input
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="Filtrar producto o código…"
          className="max-w-md"
          onKeyDown={(e) => e.key === "Enter" && void load()}
        />
        <Button onClick={() => void load()} disabled={loading}>
          {loading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Search className="mr-2 h-4 w-4" />}
          Buscar
        </Button>
      </div>
      {error ? <Alert variant="destructive"><AlertDescription>{error}</AlertDescription></Alert> : null}

      {loading ? (
        <Loader2 className="mx-auto h-6 w-6 animate-spin" />
      ) : items.length === 0 ? (
        <p className="text-sm text-muted-foreground">Sin productos con costos en múltiples sucursales.</p>
      ) : (
        <div className="space-y-2">
          {items.map((cmp) => (
            <Card
              key={cmp.variant_id}
              className="cursor-pointer hover:bg-muted/40"
              onClick={() => setSelected(cmp)}
            >
              <CardContent className="flex flex-wrap items-center justify-between gap-2 py-4">
                <div className="flex items-center gap-3">
                  <SemaphoreDot level={cmp.semaphore ?? "green"} />
                  <div>
                    <p className="font-medium">{cmp.product_name} — {cmp.variant_name}</p>
                    <p className="text-xs text-muted-foreground">{cmp.barcode}</p>
                  </div>
                </div>
                <div className="text-right text-sm">
                  <p>Mín {formatMoney(cmp.min_cost)} · Máx {formatMoney(cmp.max_cost)}</p>
                  <Badge variant={cmp.semaphore === "red" ? "destructive" : "secondary"}>
                    Var. interna {formatPct(cmp.internal_variation_pct)}
                  </Badge>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {selected ? (
        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              <SemaphoreDot level={selected.semaphore ?? "green"} />
              {selected.product_name} — {selected.variant_name}
            </CardTitle>
            <p className="text-sm text-muted-foreground">
              Variación interna: {formatPct(selected.internal_variation_pct)}
            </p>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Sucursal</TableHead>
                  <TableHead className="text-right">Costo actual</TableHead>
                  <TableHead>Última recepción</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {(selected.offices ?? []).map((o) => (
                  <TableRow key={o.office_id}>
                    <TableCell>{o.office_name ?? `Sucursal ${o.office_id}`}</TableCell>
                    <TableCell className="text-right font-medium">{formatMoney(o.cost_net)}</TableCell>
                    <TableCell>{formatDate(o.admission_date)}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            <Button className="mt-4" variant="outline" size="sm" asChild>
              <Link href={`/costos/productos/${selected.variant_id}?company_id=${companyId}`}>
                Ver ficha producto
              </Link>
            </Button>
          </CardContent>
        </Card>
      ) : null}
    </div>
  )
}

function ProductosTab({ companyId }: { companyId: number }) {
  const [q, setQ] = useState("")
  const [rows, setRows] = useState<CostProductRow[]>([])
  const [loading, setLoading] = useState(true)

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const data = await listCostProducts({
        company_id: companyId,
        q: q.trim() || undefined,
        limit: 100,
      })
      setRows(data.items)
    } finally {
      setLoading(false)
    }
  }, [companyId, q])

  useEffect(() => {
    void load()
  }, [load])

  return (
    <div className="space-y-4">
      <div className="flex gap-2">
        <Input
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="Buscar producto…"
          className="max-w-md"
          onKeyDown={(e) => e.key === "Enter" && void load()}
        />
        <Button onClick={() => void load()} disabled={loading}>Buscar</Button>
      </div>
      {loading ? (
        <Loader2 className="mx-auto h-6 w-6 animate-spin" />
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Producto</TableHead>
              <TableHead className="text-right">Costo actual</TableHead>
              <TableHead className="text-right">Promedio</TableHead>
              <TableHead className="text-right">Stock</TableHead>
              <TableHead>Última recepción</TableHead>
              <TableHead />
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((r) => (
              <TableRow key={r.variant_id}>
                <TableCell>
                  <div className="font-medium">{r.product_name}</div>
                  <div className="text-xs text-muted-foreground">{r.variant_name} · {r.barcode}</div>
                </TableCell>
                <TableCell className="text-right">{formatMoney(r.current_cost)}</TableCell>
                <TableCell className="text-right">{formatMoney(r.average_cost)}</TableCell>
                <TableCell className="text-right tabular-nums">{r.stock_quantity ?? "—"}</TableCell>
                <TableCell>{formatDate(r.last_reception_date)}</TableCell>
                <TableCell>
                  <Button size="sm" variant="ghost" asChild>
                    <Link href={`/costos/productos/${r.variant_id}?company_id=${companyId}`}>
                      Ficha
                    </Link>
                  </Button>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
    </div>
  )
}

function OportunidadesTab({ companyId }: { companyId: number }) {
  const [status, setStatus] = useState<string>(ALL)
  const [rows, setRows] = useState<CostOpportunityRow[]>([])
  const [counts, setCounts] = useState({ oportunidad_compra: 0, riesgo_comercial: 0 })
  const [loading, setLoading] = useState(true)

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const data = await listCostOpportunities({
        company_id: companyId,
        status:
          status === ALL
            ? undefined
            : (status as "oportunidad_compra" | "riesgo_comercial"),
        limit: 50,
      })
      setRows(data.items)
      setCounts(data.counts)
    } finally {
      setLoading(false)
    }
  }, [companyId, status])

  useEffect(() => {
    void load()
  }, [load])

  return (
    <div className="space-y-4">
      <div className="grid gap-4 sm:grid-cols-2">
        <Card className="border-green-200 dark:border-green-900">
          <CardContent className="flex items-center gap-3 py-4">
            <TrendingDown className="h-8 w-8 text-green-600" />
            <div>
              <p className="text-2xl font-semibold">{counts.oportunidad_compra}</p>
              <p className="text-sm text-muted-foreground">Oportunidades de compra</p>
            </div>
          </CardContent>
        </Card>
        <Card className="border-red-200 dark:border-red-900">
          <CardContent className="flex items-center gap-3 py-4">
            <TrendingUp className="h-8 w-8 text-red-600" />
            <div>
              <p className="text-2xl font-semibold">{counts.riesgo_comercial}</p>
              <p className="text-sm text-muted-foreground">Costos elevados</p>
            </div>
          </CardContent>
        </Card>
      </div>
      <Select value={status} onValueChange={setStatus}>
        <SelectTrigger className="w-[220px]">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value={ALL}>Todas</SelectItem>
          <SelectItem value="oportunidad_compra">Oportunidad compra</SelectItem>
          <SelectItem value="riesgo_comercial">Riesgo comercial</SelectItem>
        </SelectContent>
      </Select>
      {loading ? (
        <Loader2 className="mx-auto h-6 w-6 animate-spin" />
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Producto</TableHead>
              <TableHead className="text-right">Costo actual</TableHead>
              <TableHead className="text-right">Prom. 90d</TableHead>
              <TableHead className="text-right">Variación</TableHead>
              <TableHead className="text-right">Stock</TableHead>
              <TableHead>Estado</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.length === 0 ? (
              <TableRow>
                <TableCell colSpan={6} className="text-center text-muted-foreground">
                  Sin oportunidades con el filtro seleccionado.
                </TableCell>
              </TableRow>
            ) : (
              rows.map((r) => {
                const st = r.status ? OPPORTUNITY_STATUS[r.status] : null
                return (
                  <TableRow key={r.variant_id}>
                    <TableCell>
                      <div className="font-medium">{r.product_name}</div>
                      <div className="text-xs text-muted-foreground">{r.variant_name}</div>
                    </TableCell>
                    <TableCell className="text-right">{formatMoney(r.current_cost)}</TableCell>
                    <TableCell className="text-right">{formatMoney(r.avg_90d)}</TableCell>
                    <TableCell className={cn("text-right", variationClass(r.variation_pct_90d))}>
                      {formatPct(r.variation_pct_90d)}
                    </TableCell>
                    <TableCell className="text-right">{r.stock_quantity ?? "—"}</TableCell>
                    <TableCell>
                      {st ? (
                        <Badge className={st.className}>{st.label}</Badge>
                      ) : (
                        "—"
                      )}
                    </TableCell>
                  </TableRow>
                )
              })
            )}
          </TableBody>
        </Table>
      )}
    </div>
  )
}
