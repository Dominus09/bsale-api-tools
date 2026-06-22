"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import {
  AlertTriangle,
  CircleDollarSign,
  Loader2,
  RefreshCw,
  Search,
} from "lucide-react"

import {
  compareCostOffices,
  getCompanies,
  getCostAlerts,
  getCostAnalyticsDashboard,
  getCostReceptionDetail,
  getCostReceptions,
  getCostVariantHistory,
  getStoredCompanyId,
  listCostHistory,
  searchCostHistory,
  syncCostAnalytics,
  type Company,
  type CostAlertRow,
  type CostHistoryRow,
  type CostHistorySearchHit,
  type CostOfficeComparison,
  type CostOfficeRef,
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
            Trazabilidad por empresa → sucursal → recepción → variante — {companyLabel}
          </p>
        </div>
        {companies.length > 1 ? (
          <Select value={String(companyId)} onValueChange={(v) => setCompanyId(Number(v))}>
            <SelectTrigger className="w-[220px]">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {companies.map((c) => (
                <SelectItem key={c.company_id} value={String(c.company_id)}>
                  {c.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        ) : null}
      </div>

      <Tabs value={tab} onValueChange={setTab}>
        <TabsList className="flex h-auto flex-wrap">
          <TabsTrigger value="dashboard">Dashboard</TabsTrigger>
          <TabsTrigger value="historial">Historial</TabsTrigger>
          <TabsTrigger value="recepciones">Recepciones</TabsTrigger>
          <TabsTrigger value="alertas">Alertas</TabsTrigger>
          <TabsTrigger value="comparacion">Comparación sucursales</TabsTrigger>
        </TabsList>

        <TabsContent value="dashboard" className="mt-4">
          <DashboardTab companyId={companyId} offices={offices} filterParams={filterParams} />
        </TabsContent>
        <TabsContent value="historial" className="mt-4">
          <HistorialTab companyId={companyId} offices={offices} filterParams={filterParams} />
        </TabsContent>
        <TabsContent value="recepciones" className="mt-4">
          <RecepcionesTab companyId={companyId} offices={offices} filterParams={filterParams} />
        </TabsContent>
        <TabsContent value="alertas" className="mt-4">
          <AlertasTab companyId={companyId} offices={offices} officeFilter={officeFilter} />
        </TabsContent>
        <TabsContent value="comparacion" className="mt-4">
          <ComparacionTab companyId={companyId} />
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
  const [kpis, setKpis] = useState<Record<string, number> | null>(null)
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
          <KpiCard title="Productos con costo" value={kpis?.with_cost ?? 0} />
          <KpiCard title="Productos sin costo" value={kpis?.without_cost ?? 0} />
          <KpiCard title="Productos costo cero" value={kpis?.zero_cost ?? 0} />
          <KpiCard title="Recepciones últimas 24h" value={kpis?.receptions_24h ?? 0} />
          <KpiCard title="Variaciones >10%" value={kpis?.variation_gt_10 ?? 0} />
          <KpiCard title="Variaciones >20%" value={kpis?.variation_gt_20 ?? 0} />
          <KpiCard title="Recepciones procesadas" value={kpis?.receptions_processed ?? 0} />
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

  const openVariant = async (variantId: number) => {
    setLoading(true)
    try {
      const data = await getCostVariantHistory(variantId, companyId)
      setRows(data.items)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error al cargar variante")
    } finally {
      setLoading(false)
    }
  }

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
            <Button key={h.variant_id} size="sm" variant="outline" onClick={() => void openVariant(h.variant_id)}>
              {h.product_name} — {h.variant_name}
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
  const [items, setItems] = useState<CostOfficeComparison[]>([])
  const [selected, setSelected] = useState<CostOfficeComparison | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const run = async () => {
    if (q.trim().length < 2) {
      setError("Ingrese al menos 2 caracteres.")
      return
    }
    setLoading(true)
    setError(null)
    setSelected(null)
    try {
      const data = await compareCostOffices({ company_id: companyId, q: q.trim() })
      if ("items" in data) setItems(data.items)
      else if ("comparison" in data) setSelected(data.comparison)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error al comparar")
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="space-y-4">
      <div className="flex gap-2">
        <Input
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="Producto o código de barras…"
          className="max-w-md"
          onKeyDown={(e) => e.key === "Enter" && void run()}
        />
        <Button onClick={() => void run()} disabled={loading}>
          {loading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Search className="mr-2 h-4 w-4" />}
          Comparar
        </Button>
      </div>
      {error ? <Alert variant="destructive"><AlertDescription>{error}</AlertDescription></Alert> : null}

      {items.length > 0 ? (
        <div className="space-y-2">
          {items.map((cmp) => (
            <Card key={cmp.variant_id} className="cursor-pointer hover:bg-muted/40" onClick={() => setSelected(cmp)}>
              <CardContent className="flex flex-wrap items-center justify-between gap-2 py-4">
                <div>
                  <p className="font-medium">{cmp.product_name} — {cmp.variant_name}</p>
                  <p className="text-xs text-muted-foreground">{cmp.barcode}</p>
                </div>
                <Badge variant={cmp.max_spread_pct && cmp.max_spread_pct >= 10 ? "destructive" : "secondary"}>
                  Dif. máx. {formatPct(cmp.max_spread_pct)}
                </Badge>
              </CardContent>
            </Card>
          ))}
        </div>
      ) : null}

      {selected ? (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">
              {selected.product_name} — {selected.variant_name}
            </CardTitle>
            <p className="text-sm text-muted-foreground">
              Diferencia máxima entre sucursales: {formatPct(selected.max_spread_pct)}
            </p>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Sucursal</TableHead>
                  <TableHead className="text-right">Costo neto</TableHead>
                  <TableHead className="text-right">Bruto ERP</TableHead>
                  <TableHead>Última recepción</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {selected.offices.map((o) => (
                  <TableRow key={o.office_id}>
                    <TableCell>{o.office_name ?? `Sucursal ${o.office_id}`}</TableCell>
                    <TableCell className="text-right font-medium">{formatMoney(o.cost_net)}</TableCell>
                    <TableCell className="text-right">{formatMoney(o.cost_bruto_erp)}</TableCell>
                    <TableCell>{formatDate(o.admission_date)}</TableCell>
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
