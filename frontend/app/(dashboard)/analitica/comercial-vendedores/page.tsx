"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  Pie,
  PieChart,
  Cell,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
  Legend,
} from "recharts"
import {
  AlertCircle,
  ArrowDownRight,
  ArrowUpRight,
  Loader2,
  Minus,
  TrendingUp,
  Users,
  Wallet,
  Package,
  Target,
} from "lucide-react"

import {
  getCommercialCrossSelling,
  getCommercialDashboard,
  getCommercialFilterOptions,
  getCommercialLostClients,
  getCommercialProductPerformance,
  getCommercialSellerPerformance,
  getCommercialSummary,
  getCommercialUniqueClients,
  getCommercialClientProfile,
  type CommercialAnalyticsParams,
  type CommercialDashboardResponse,
  type CommercialFilterOptions,
  type CommercialKpiCompare,
  type CommercialSellerRow,
} from "@/lib/api"
import { Alert, AlertDescription } from "@/components/ui/alert"
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
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet"
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

function formatCLP(n: number): string {
  return n.toLocaleString("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  })
}

function formatPct(n: number): string {
  const sign = n > 0 ? "+" : ""
  return `${sign}${n.toFixed(1)}%`
}

function isoDate(d: Date): string {
  return d.toISOString().slice(0, 10)
}

function currentMonthRange(): { from: string; to: string } {
  const today = new Date()
  const first = new Date(today.getFullYear(), today.getMonth(), 1)
  return { from: isoDate(first), to: isoDate(today) }
}

function previousMonthRange(): { from: string; to: string } {
  const today = new Date()
  const firstThis = new Date(today.getFullYear(), today.getMonth(), 1)
  const lastPrev = new Date(firstThis.getTime() - 86400000)
  const firstPrev = new Date(lastPrev.getFullYear(), lastPrev.getMonth(), 1)
  return { from: isoDate(firstPrev), to: isoDate(lastPrev) }
}

const STATUS_COLORS: Record<string, string> = {
  activo: "#22c55e",
  nuevo: "#3b82f6",
  recuperado: "#8b5cf6",
  perdido: "#ef4444",
  en_riesgo: "#f59e0b",
}

const PIE_COLORS = ["#22c55e", "#3b82f6", "#8b5cf6", "#ef4444", "#f59e0b"]

function TrendIcon({ trend }: { trend: CommercialKpiCompare["trend"] }) {
  if (trend === "up") return <ArrowUpRight className="h-4 w-4 text-emerald-600" />
  if (trend === "down") return <ArrowDownRight className="h-4 w-4 text-red-600" />
  return <Minus className="h-4 w-4 text-muted-foreground" />
}

function KpiCard({
  title,
  icon,
  kpi,
  format = "number",
  invertTrend = false,
}: {
  title: string
  icon: React.ReactNode
  kpi: CommercialKpiCompare | null | undefined
  format?: "currency" | "number"
  invertTrend?: boolean
}) {
  if (!kpi) {
    return (
      <Card>
        <CardHeader className="flex flex-row items-center justify-between pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">{title}</CardTitle>
          {icon}
        </CardHeader>
        <CardContent>
          <p className="text-2xl font-bold text-muted-foreground">N/D</p>
        </CardContent>
      </Card>
    )
  }
  const display =
    format === "currency" ? formatCLP(kpi.current) : kpi.current.toLocaleString("es-CL")
  const trend = invertTrend
    ? kpi.trend === "up"
      ? "down"
      : kpi.trend === "down"
        ? "up"
        : "flat"
    : kpi.trend
  const trendColor =
    trend === "up" ? "text-emerald-600" : trend === "down" ? "text-red-600" : "text-muted-foreground"

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between pb-2">
        <CardTitle className="text-sm font-medium text-muted-foreground">{title}</CardTitle>
        {icon}
      </CardHeader>
      <CardContent>
        <p className="text-2xl font-bold">{display}</p>
        <div className={cn("mt-1 flex items-center gap-1 text-sm", trendColor)}>
          <TrendIcon trend={trend} />
          <span>{formatPct(kpi.delta_pct)}</span>
          <span className="text-muted-foreground">
            ({format === "currency" ? formatCLP(kpi.delta_abs) : kpi.delta_abs})
          </span>
        </div>
        <p className="mt-1 text-xs text-muted-foreground">
          Anterior: {format === "currency" ? formatCLP(kpi.previous) : kpi.previous}
        </p>
      </CardContent>
    </Card>
  )
}

function PriorityBadge({ prioridad }: { prioridad: string }) {
  const map: Record<string, string> = {
    alta: "destructive",
    media: "default",
    baja: "secondary",
  }
  return (
    <Badge variant={(map[prioridad] as "destructive" | "default" | "secondary") ?? "outline"}>
      {prioridad}
    </Badge>
  )
}

function StatusBadge({ status }: { status: string }) {
  const labels: Record<string, string> = {
    activo: "Activo",
    nuevo: "Nuevo",
    recuperado: "Recuperado",
    perdido: "Perdido",
    en_riesgo: "En riesgo",
  }
  return (
    <Badge
      variant="outline"
      className="border-0"
      style={{ backgroundColor: `${STATUS_COLORS[status] ?? "#94a3b8"}22`, color: STATUS_COLORS[status] }}
    >
      {labels[status] ?? status}
    </Badge>
  )
}

export default function ComercialVendedoresPage() {
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [periodPreset, setPeriodPreset] = useState<"current" | "previous" | "custom">("current")
  const [dateFrom, setDateFrom] = useState(currentMonthRange().from)
  const [dateTo, setDateTo] = useState(currentMonthRange().to)
  const [seller, setSeller] = useState<string>("")
  const [city, setCity] = useState<string>("")
  const [documentType, setDocumentType] = useState<string>("all")
  const [filterOptions, setFilterOptions] = useState<CommercialFilterOptions | null>(null)
  const [tab, setTab] = useState("dashboard")

  const [dashboard, setDashboard] = useState<CommercialDashboardResponse | null>(null)
  const [summary, setSummary] = useState<{ title: string; bullets: string[] } | null>(null)
  const [sellers, setSellers] = useState<CommercialSellerRow[]>([])
  const [rankings, setRankings] = useState<Record<string, string[]>>({})
  const [uniqueClients, setUniqueClients] = useState<
    { client_id: number; client_name: string; seller_name: string; status: string; venta_actual: number }[]
  >([])
  const [lostClients, setLostClients] = useState<
    Awaited<ReturnType<typeof getCommercialLostClients>>["items"]
  >([])
  const [crossSelling, setCrossSelling] = useState<
    Awaited<ReturnType<typeof getCommercialCrossSelling>>["items"]
  >([])
  const [products, setProducts] = useState<
    Awaited<ReturnType<typeof getCommercialProductPerformance>> | null
  >(null)

  const [profileClientId, setProfileClientId] = useState<number | null>(null)
  const [profile, setProfile] = useState<Awaited<ReturnType<typeof getCommercialClientProfile>> | null>(null)
  const [profileLoading, setProfileLoading] = useState(false)

  const params: CommercialAnalyticsParams = useMemo(
    () => ({
      date_from: dateFrom,
      date_to: dateTo,
      seller: seller || undefined,
      city: city || undefined,
      document_type: documentType === "all" ? undefined : documentType,
    }),
    [dateFrom, dateTo, seller, city, documentType],
  )

  useEffect(() => {
    void getCommercialFilterOptions()
      .then(setFilterOptions)
      .catch(() => setFilterOptions(null))
  }, [])

  const applyPreset = useCallback((preset: "current" | "previous" | "custom") => {
    setPeriodPreset(preset)
    if (preset === "current") {
      const r = currentMonthRange()
      setDateFrom(r.from)
      setDateTo(r.to)
    } else if (preset === "previous") {
      const r = previousMonthRange()
      setDateFrom(r.from)
      setDateTo(r.to)
    }
  }, [])

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const [dash, sum, sellerPerf, unique, lost, cross, prod] = await Promise.all([
        getCommercialDashboard(params),
        getCommercialSummary(params),
        getCommercialSellerPerformance({ ...params, limit: 50 }),
        getCommercialUniqueClients({ ...params, limit: 300 }),
        getCommercialLostClients({ ...params, limit: 100 }),
        getCommercialCrossSelling({ ...params, limit: 100 }),
        getCommercialProductPerformance({ ...params, limit: 50 }),
      ])
      setDashboard(dash)
      setSummary(sum)
      setSellers(sellerPerf.items)
      setRankings(sellerPerf.rankings)
      setUniqueClients(unique.items)
      setLostClients(lost.items)
      setCrossSelling(cross.items)
      setProducts(prod)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error al cargar analítica comercial")
    } finally {
      setLoading(false)
    }
  }, [params])

  useEffect(() => {
    void load()
  }, [load])

  const openProfile = useCallback(
    async (clientId: number) => {
      setProfileClientId(clientId)
      setProfileLoading(true)
      setProfile(null)
      try {
        const p = await getCommercialClientProfile(clientId, {
          date_from: dateFrom,
          date_to: dateTo,
          document_type: documentType === "all" ? undefined : documentType,
        })
        setProfile(p)
      } catch {
        setProfile(null)
      } finally {
        setProfileLoading(false)
      }
    },
    [dateFrom, dateTo, documentType],
  )

  const classificationPie = useMemo(() => {
    if (!dashboard) return []
    const c = dashboard.client_classification
    return [
      { name: "Activos", value: c.activos },
      { name: "Nuevos", value: c.nuevos },
      { name: "Recuperados", value: c.recuperados },
      { name: "Perdidos", value: c.perdidos },
      { name: "En riesgo", value: c.en_riesgo },
    ].filter((x) => x.value > 0)
  }, [dashboard])

  return (
    <div className="flex flex-col gap-6 p-6">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Comercial Vendedores</h1>
          <p className="text-sm text-muted-foreground">
            Inteligencia comercial — Facturas y boletas (Company 3 / Office 1)
          </p>
        </div>
        <Button variant="outline" size="sm" onClick={() => void load()} disabled={loading}>
          {loading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
          Actualizar
        </Button>
      </div>

      {summary && (
        <Card className="border-primary/20 bg-primary/5">
          <CardHeader className="pb-2">
            <CardTitle className="text-base">{summary.title}</CardTitle>
          </CardHeader>
          <CardContent>
            <ul className="space-y-1 text-sm">
              {summary.bullets.map((b, i) => (
                <li key={i} className="flex items-start gap-2">
                  <span className="mt-1.5 h-1.5 w-1.5 shrink-0 rounded-full bg-primary" />
                  {b}
                </li>
              ))}
            </ul>
          </CardContent>
        </Card>
      )}

      <Card>
        <CardContent className="flex flex-wrap items-end gap-4 pt-6">
          <div className="flex gap-2">
            <Button
              variant={periodPreset === "current" ? "default" : "outline"}
              size="sm"
              onClick={() => applyPreset("current")}
            >
              Mes actual
            </Button>
            <Button
              variant={periodPreset === "previous" ? "default" : "outline"}
              size="sm"
              onClick={() => applyPreset("previous")}
            >
              Mes anterior
            </Button>
            <Button
              variant={periodPreset === "custom" ? "default" : "outline"}
              size="sm"
              onClick={() => setPeriodPreset("custom")}
            >
              Rango personalizado
            </Button>
          </div>
          {periodPreset === "custom" && (
            <>
              <div>
                <Label className="text-xs">Desde</Label>
                <Input type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)} />
              </div>
              <div>
                <Label className="text-xs">Hasta</Label>
                <Input type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)} />
              </div>
            </>
          )}
          <div className="min-w-[160px]">
            <Label className="text-xs">Vendedor</Label>
            <Select value={seller || "__all__"} onValueChange={(v) => setSeller(v === "__all__" ? "" : v)}>
              <SelectTrigger>
                <SelectValue placeholder="Todos" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="__all__">Todos</SelectItem>
                {(filterOptions?.sellers ?? []).map((s) => (
                  <SelectItem key={s} value={s}>
                    {s}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="min-w-[160px]">
            <Label className="text-xs">Comuna</Label>
            <Select value={city || "__all__"} onValueChange={(v) => setCity(v === "__all__" ? "" : v)}>
              <SelectTrigger>
                <SelectValue placeholder="Todas" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="__all__">Todas</SelectItem>
                {(filterOptions?.cities ?? []).map((c) => (
                  <SelectItem key={c} value={c}>
                    {c}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="min-w-[140px]">
            <Label className="text-xs">Tipo documento</Label>
            <Select value={documentType} onValueChange={setDocumentType}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">Todos</SelectItem>
                <SelectItem value="factura">Factura</SelectItem>
                <SelectItem value="boleta">Boleta</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </CardContent>
      </Card>

      {error && (
        <Alert variant="destructive">
          <AlertCircle className="h-4 w-4" />
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {loading && !dashboard ? (
        <div className="flex items-center justify-center py-24">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      ) : (
        <Tabs value={tab} onValueChange={setTab}>
          <TabsList className="flex h-auto flex-wrap gap-1">
            <TabsTrigger value="dashboard">Dashboard</TabsTrigger>
            <TabsTrigger value="vendedores">Vendedores</TabsTrigger>
            <TabsTrigger value="clientes">Clientes únicos</TabsTrigger>
            <TabsTrigger value="perdidos">Perdidos</TabsTrigger>
            <TabsTrigger value="productos">Productos</TabsTrigger>
            <TabsTrigger value="cross">Cross-selling</TabsTrigger>
          </TabsList>

          <TabsContent value="dashboard" className="space-y-6">
            {dashboard && (
              <>
                <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5">
                  <KpiCard title="Venta neta" icon={<Wallet className="h-4 w-4 text-muted-foreground" />} kpi={dashboard.kpis.venta_neta} format="currency" />
                  <KpiCard title="Clientes únicos" icon={<Users className="h-4 w-4 text-muted-foreground" />} kpi={dashboard.kpis.clientes_unicos} />
                  <KpiCard title="Clientes nuevos" icon={<Users className="h-4 w-4 text-muted-foreground" />} kpi={dashboard.kpis.clientes_nuevos as CommercialKpiCompare} />
                  <KpiCard title="Clientes recuperados" icon={<TrendingUp className="h-4 w-4 text-muted-foreground" />} kpi={dashboard.kpis.clientes_recuperados as CommercialKpiCompare} />
                  <KpiCard title="Clientes perdidos" icon={<Users className="h-4 w-4 text-muted-foreground" />} kpi={dashboard.kpis.clientes_perdidos as CommercialKpiCompare} invertTrend />
                  <KpiCard title="Ticket promedio" icon={<Wallet className="h-4 w-4 text-muted-foreground" />} kpi={dashboard.kpis.ticket_promedio} format="currency" />
                  <KpiCard title="Documentos emitidos" icon={<Package className="h-4 w-4 text-muted-foreground" />} kpi={dashboard.kpis.documentos_emitidos} />
                  <KpiCard title="Unidades vendidas" icon={<Package className="h-4 w-4 text-muted-foreground" />} kpi={dashboard.kpis.unidades_vendidas} />
                  <KpiCard title="Productos distintos" icon={<Package className="h-4 w-4 text-muted-foreground" />} kpi={dashboard.kpis.productos_distintos} />
                  {dashboard.kpis.margen_estimado && (
                    <KpiCard title="Margen estimado" icon={<Target className="h-4 w-4 text-muted-foreground" />} kpi={dashboard.kpis.margen_estimado} format="currency" />
                  )}
                </div>

                <div className="grid gap-6 lg:grid-cols-2">
                  <Card>
                    <CardHeader>
                      <CardTitle className="text-base">Evolución venta diaria</CardTitle>
                    </CardHeader>
                    <CardContent className="h-72">
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={dashboard.daily_sales}>
                          <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                          <XAxis dataKey="day" tick={{ fontSize: 11 }} tickFormatter={(v) => v.slice(5)} />
                          <YAxis tick={{ fontSize: 11 }} tickFormatter={(v) => `${(v / 1e6).toFixed(1)}M`} />
                          <Tooltip formatter={(v: number) => formatCLP(v)} />
                          <Line type="monotone" dataKey="venta_neta" stroke="#3b82f6" strokeWidth={2} dot={false} />
                        </LineChart>
                      </ResponsiveContainer>
                    </CardContent>
                  </Card>

                  <Card>
                    <CardHeader>
                      <CardTitle className="text-base">Clasificación de clientes</CardTitle>
                    </CardHeader>
                    <CardContent className="h-72">
                      {classificationPie.length > 0 ? (
                        <ResponsiveContainer width="100%" height="100%">
                          <PieChart>
                            <Pie data={classificationPie} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={90} label>
                              {classificationPie.map((_, i) => (
                                <Cell key={i} fill={PIE_COLORS[i % PIE_COLORS.length]} />
                              ))}
                            </Pie>
                            <Tooltip />
                            <Legend />
                          </PieChart>
                        </ResponsiveContainer>
                      ) : (
                        <p className="py-12 text-center text-sm text-muted-foreground">Sin datos en el período</p>
                      )}
                    </CardContent>
                  </Card>
                </div>
              </>
            )}
          </TabsContent>

          <TabsContent value="vendedores" className="space-y-6">
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-5">
              {[
                { key: "mayor_venta", label: "Mayor venta" },
                { key: "mayor_crecimiento", label: "Mayor crecimiento" },
                { key: "mayor_recuperacion", label: "Mayor recuperación" },
                { key: "mayor_perdida", label: "Mayor pérdida" },
                { key: "mejor_cobertura", label: "Mejor cobertura" },
              ].map(({ key, label }) => (
                <Card key={key}>
                  <CardHeader className="pb-2">
                    <CardTitle className="text-xs font-medium text-muted-foreground">{label}</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <ol className="space-y-1 text-sm">
                      {(rankings[key] ?? []).slice(0, 3).map((name, i) => (
                        <li key={name} className="flex items-center gap-2">
                          <span className="flex h-5 w-5 items-center justify-center rounded-full bg-muted text-xs font-bold">
                            {i + 1}
                          </span>
                          <span className="truncate">{name}</span>
                        </li>
                      ))}
                    </ol>
                  </CardContent>
                </Card>
              ))}
            </div>

            <Card>
              <CardHeader>
                <CardTitle className="text-base">Rendimiento por vendedor</CardTitle>
              </CardHeader>
              <CardContent className="overflow-x-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Vendedor</TableHead>
                      <TableHead className="text-right">Venta actual</TableHead>
                      <TableHead className="text-right">Var %</TableHead>
                      <TableHead className="text-right">Clientes</TableHead>
                      <TableHead className="text-right">Nuevos</TableHead>
                      <TableHead className="text-right">Perdidos</TableHead>
                      <TableHead className="text-right">Recuperados</TableHead>
                      <TableHead className="text-right">Ticket</TableHead>
                      <TableHead className="text-right">Productos</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {sellers.map((s) => (
                      <TableRow key={s.seller_name}>
                        <TableCell className="font-medium">{s.seller_name}</TableCell>
                        <TableCell className="text-right">{formatCLP(s.venta_actual)}</TableCell>
                        <TableCell className={cn("text-right font-medium", s.variacion_pct >= 0 ? "text-emerald-600" : "text-red-600")}>
                          {formatPct(s.variacion_pct)}
                        </TableCell>
                        <TableCell className="text-right">{s.clientes_unicos_actual}</TableCell>
                        <TableCell className="text-right">{s.clientes_nuevos}</TableCell>
                        <TableCell className="text-right text-red-600">{s.clientes_perdidos}</TableCell>
                        <TableCell className="text-right text-emerald-600">{s.clientes_recuperados}</TableCell>
                        <TableCell className="text-right">{formatCLP(s.ticket_promedio)}</TableCell>
                        <TableCell className="text-right">{s.productos_distintos}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="clientes">
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Clientes únicos — clasificación</CardTitle>
              </CardHeader>
              <CardContent className="overflow-x-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Cliente</TableHead>
                      <TableHead>Vendedor</TableHead>
                      <TableHead>Estado</TableHead>
                      <TableHead className="text-right">Venta período</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {uniqueClients.map((c) => (
                      <TableRow
                        key={c.client_id}
                        className="cursor-pointer hover:bg-muted/50"
                        onClick={() => void openProfile(c.client_id)}
                      >
                        <TableCell className="font-medium">{c.client_name}</TableCell>
                        <TableCell>{c.seller_name}</TableCell>
                        <TableCell>
                          <StatusBadge status={c.status} />
                        </TableCell>
                        <TableCell className="text-right">{formatCLP(c.venta_actual)}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="perdidos">
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Clientes perdidos</CardTitle>
              </CardHeader>
              <CardContent className="overflow-x-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Cliente</TableHead>
                      <TableHead>Vendedor</TableHead>
                      <TableHead>Comuna</TableHead>
                      <TableHead>Última compra</TableHead>
                      <TableHead className="text-right">Días sin comprar</TableHead>
                      <TableHead className="text-right">Ticket prom.</TableHead>
                      <TableHead>Productos habituales</TableHead>
                      <TableHead>Prioridad</TableHead>
                      <TableHead>Acción</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {lostClients.map((c) => (
                      <TableRow
                        key={c.client_id}
                        className="cursor-pointer hover:bg-muted/50"
                        onClick={() => void openProfile(c.client_id)}
                      >
                        <TableCell className="font-medium">{c.client_name}</TableCell>
                        <TableCell>{c.seller_name}</TableCell>
                        <TableCell>{c.municipality}</TableCell>
                        <TableCell>{c.ultima_compra ?? "—"}</TableCell>
                        <TableCell className="text-right">{c.dias_sin_comprar}</TableCell>
                        <TableCell className="text-right">{formatCLP(c.ticket_promedio)}</TableCell>
                        <TableCell className="max-w-[200px] truncate text-xs">
                          {(c.productos_habituales ?? []).join(", ") || "—"}
                        </TableCell>
                        <TableCell>
                          <PriorityBadge prioridad={c.prioridad} />
                        </TableCell>
                        <TableCell className="text-xs">{c.accion_sugerida}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="productos" className="space-y-6">
            {products && (
              <>
                <Card>
                  <CardHeader>
                    <CardTitle className="text-base">Productos más vendidos</CardTitle>
                  </CardHeader>
                  <CardContent className="h-80">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={products.top_products.slice(0, 10)} layout="vertical">
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis type="number" tickFormatter={(v) => `${(v / 1e6).toFixed(1)}M`} />
                        <YAxis type="category" dataKey="producto" width={140} tick={{ fontSize: 10 }} />
                        <Tooltip formatter={(v: number) => formatCLP(v)} />
                        <Bar dataKey="venta" fill="#3b82f6" radius={[0, 4, 4, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader>
                    <CardTitle className="text-base">Oportunidades — productos con baja cobertura</CardTitle>
                  </CardHeader>
                  <CardContent className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Producto</TableHead>
                          <TableHead className="text-right">Clientes empresa</TableHead>
                          <TableHead className="text-right">Clientes vendedor</TableHead>
                          <TableHead className="text-right">Brecha</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {products.oportunidades.map((p, i) => (
                          <TableRow key={i}>
                            <TableCell>{p.producto}</TableCell>
                            <TableCell className="text-right">{p.clientes_empresa}</TableCell>
                            <TableCell className="text-right">{p.clientes_vendedor}</TableCell>
                            <TableCell className="text-right font-medium text-amber-600">{p.brecha}</TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </CardContent>
                </Card>
              </>
            )}
          </TabsContent>

          <TabsContent value="cross">
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Oportunidades cross-selling</CardTitle>
              </CardHeader>
              <CardContent className="overflow-x-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Cliente</TableHead>
                      <TableHead>Vendedor</TableHead>
                      <TableHead>Compra</TableHead>
                      <TableHead>Recomendado</TableHead>
                      <TableHead>Motivo</TableHead>
                      <TableHead>Prioridad</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {crossSelling.map((r, i) => (
                      <TableRow
                        key={i}
                        className="cursor-pointer hover:bg-muted/50"
                        onClick={() => void openProfile(r.client_id)}
                      >
                        <TableCell className="font-medium">{r.client_name}</TableCell>
                        <TableCell>{r.seller_name}</TableCell>
                        <TableCell>{r.producto_comprado}</TableCell>
                        <TableCell>{r.producto_recomendado}</TableCell>
                        <TableCell className="max-w-[220px] text-xs">{r.motivo}</TableCell>
                        <TableCell>
                          <PriorityBadge prioridad={r.prioridad} />
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      )}

      <Sheet open={profileClientId !== null} onOpenChange={(o) => !o && setProfileClientId(null)}>
        <SheetContent className="w-full overflow-y-auto sm:max-w-xl">
          <SheetHeader>
            <SheetTitle>Ficha cliente</SheetTitle>
          </SheetHeader>
          {profileLoading ? (
            <div className="flex justify-center py-12">
              <Loader2 className="h-6 w-6 animate-spin" />
            </div>
          ) : profile ? (
            <div className="mt-6 space-y-6">
              <div>
                <h3 className="text-lg font-semibold">{profile.client.client_name}</h3>
                <p className="text-sm text-muted-foreground">{profile.client.municipality}</p>
                <p className="text-sm">Vendedor: {profile.client.seller_name}</p>
                <div className="mt-2 grid grid-cols-2 gap-2 text-sm">
                  <div>Última compra: {profile.client.ultima_compra ?? "—"}</div>
                  <div>Ticket prom.: {formatCLP(profile.client.ticket_promedio)}</div>
                  <div>Total compras: {profile.client.total_compras}</div>
                  <div>Venta total: {formatCLP(profile.client.venta_total)}</div>
                </div>
              </div>

              <div className="h-48">
                <p className="mb-2 text-sm font-medium">Evolución mensual (6 meses)</p>
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={profile.venta_mensual}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="mes" tick={{ fontSize: 10 }} />
                    <YAxis tick={{ fontSize: 10 }} tickFormatter={(v) => `${(v / 1e6).toFixed(1)}M`} />
                    <Tooltip formatter={(v: number) => formatCLP(v)} />
                    <Bar dataKey="venta" fill="#8b5cf6" radius={[4, 4, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>

              <div>
                <p className="mb-2 text-sm font-medium">Productos habituales</p>
                <ul className="space-y-1 text-sm">
                  {profile.productos_habituales.slice(0, 8).map((p, i) => (
                    <li key={i} className="flex justify-between">
                      <span className="truncate pr-2">{p.producto}</span>
                      <span className="shrink-0 text-muted-foreground">{formatCLP(p.venta)}</span>
                    </li>
                  ))}
                </ul>
              </div>

              {profile.oportunidades.length > 0 && (
                <div>
                  <p className="mb-2 text-sm font-medium">Oportunidades sugeridas</p>
                  <ul className="space-y-2 text-sm">
                    {profile.oportunidades.map((o, i) => (
                      <li key={i} className="rounded-md border p-2">
                        <span className="font-medium">{o.producto_recomendado}</span>
                        <p className="text-xs text-muted-foreground">{o.motivo}</p>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          ) : (
            <p className="py-8 text-center text-sm text-muted-foreground">No se pudo cargar la ficha</p>
          )}
        </SheetContent>
      </Sheet>
    </div>
  )
}
