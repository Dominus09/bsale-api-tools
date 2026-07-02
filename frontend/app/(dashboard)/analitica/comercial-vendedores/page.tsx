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
  getCommercialBundle,
  getCommercialFilterOptions,
  getCommercialClientProfile,
  getCommercialSellerProfile,
  type CommercialAnalyticsParams,
  type CommercialAnalysisScope,
  type CommercialAttackItem,
  type CommercialBundleMeta,
  type CommercialCrmLayer,
  type CommercialDashboardResponse,
  type CommercialFilterOptions,
  type CommercialInsight,
  type CommercialKpiCompare,
  type CommercialOpportunity,
  type CommercialSellerRow,
} from "@/lib/api"
import { CommercialCrmHome, WATCHLIST_KEY, type Watchlist } from "@/components/comercial/commercial-crm-home"
import { CommercialMapClient } from "@/components/comercial/commercial-map-client"
import { CommercialSimulator } from "@/components/comercial/commercial-simulator"
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

function formatMinutesAgo(iso: string): string {
  const then = new Date(iso).getTime()
  const mins = Math.max(0, Math.round((Date.now() - then) / 60000))
  if (mins < 1) return "hace un momento"
  if (mins === 1) return "hace 1 minuto"
  return `hace ${mins} minutos`
}

function PrioridadBadge({ prioridad }: { prioridad: string }) {
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

function InsightTypeBadge({ tipo }: { tipo: string }) {
  const labels: Record<string, string> = {
    vendedor: "Vendedor",
    riesgo: "Riesgo",
    oportunidad: "Oportunidad",
    recuperacion: "Recuperación",
    producto: "Producto",
  }
  return <Badge variant="outline">{labels[tipo] ?? tipo}</Badge>
}

function AttackList({
  title,
  items,
  onClientClick,
}: {
  title: string
  items: CommercialAttackItem[]
  onClientClick?: (id: number) => void
}) {
  if (!items.length) return null
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium">{title}</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {items.slice(0, 5).map((item, i) => (
          <div
            key={i}
            className={cn(
              "rounded-lg border p-3 text-sm",
              item.client_id && onClientClick && "cursor-pointer hover:bg-muted/50",
            )}
            onClick={() => item.client_id && onClientClick?.(item.client_id)}
          >
            <div className="mb-1 flex items-center justify-between gap-2">
              <span className="font-medium">
                {item.client_name ?? item.seller_name ?? item.producto ?? "Acción"}
              </span>
              <PrioridadBadge prioridad={item.prioridad} />
            </div>
            <p className="text-muted-foreground">{item.motivo}</p>
            <p className="mt-1 font-medium text-primary">{item.accion}</p>
            {item.monto_estimado != null && item.monto_estimado > 0 && (
              <p className="mt-1 text-xs text-muted-foreground">
                Monto estimado: {formatCLP(item.monto_estimado)}
              </p>
            )}
          </div>
        ))}
      </CardContent>
    </Card>
  )
}

function SellerScoreBadge({ score, label }: { score?: number; label?: string }) {
  if (score == null) return null
  const color =
    score >= 80 ? "text-emerald-600 bg-emerald-50" : score >= 60 ? "text-amber-600 bg-amber-50" : "text-red-600 bg-red-50"
  return (
    <span className={cn("rounded-full px-2 py-0.5 text-xs font-semibold", color)}>
      Score {score} — {label}
    </span>
  )
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
  const [tab, setTab] = useState("home")

  const [crm, setCrm] = useState<CommercialCrmLayer | null>(null)
  const [analysisScope, setAnalysisScope] = useState<CommercialAnalysisScope | null>(null)
  const [dashboard, setDashboard] = useState<CommercialDashboardResponse | null>(null)
  const [meta, setMeta] = useState<CommercialBundleMeta | null>(null)
  const [summary, setSummary] = useState<{
    title: string
    bullets: string[]
    insights: CommercialInsight[]
  } | null>(null)
  const [attackPlan, setAttackPlan] = useState<
    Awaited<ReturnType<typeof getCommercialBundle>>["attack_plan"] | null
  >(null)
  const [opportunities, setOpportunities] = useState<CommercialOpportunity[]>([])
  const [sellers, setSellers] = useState<CommercialSellerRow[]>([])
  const [rankings, setRankings] = useState<Record<string, string[]>>({})
  const [uniqueClients, setUniqueClients] = useState<
    Awaited<ReturnType<typeof getCommercialBundle>>["unique_clients"]["items"]
  >([])
  const [lostClients, setLostClients] = useState<
    Awaited<ReturnType<typeof getCommercialBundle>>["lost_clients"]["items"]
  >([])
  const [crossSelling, setCrossSelling] = useState<
    Awaited<ReturnType<typeof getCommercialBundle>>["cross_selling"]["items"]
  >([])
  const [products, setProducts] = useState<
    Awaited<ReturnType<typeof getCommercialBundle>>["product_performance"] | null
  >(null)

  const [profileClientId, setProfileClientId] = useState<number | null>(null)
  const [profile, setProfile] = useState<Awaited<ReturnType<typeof getCommercialClientProfile>> | null>(null)
  const [profileLoading, setProfileLoading] = useState(false)

  const [profileSellerName, setProfileSellerName] = useState<string | null>(null)
  const [sellerProfile, setSellerProfile] = useState<Awaited<ReturnType<typeof getCommercialSellerProfile>> | null>(null)
  const [sellerProfileLoading, setSellerProfileLoading] = useState(false)
  const [watchlist, setWatchlist] = useState<Watchlist>({ clients: {}, sellers: {} })

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

  useEffect(() => {
    try {
      const raw = localStorage.getItem(WATCHLIST_KEY)
      if (raw) setWatchlist(JSON.parse(raw) as Watchlist)
    } catch {
      /* ignore */
    }
  }, [])

  const toggleWatchlistClient = useCallback(
    (clientId: number, tag: Watchlist["clients"][number]) => {
      setWatchlist((prev) => {
        const next = { ...prev, clients: { ...prev.clients } }
        if (next.clients[clientId] === tag) delete next.clients[clientId]
        else next.clients[clientId] = tag
        try {
          localStorage.setItem(WATCHLIST_KEY, JSON.stringify(next))
        } catch {
          /* ignore */
        }
        return next
      })
    },
    [],
  )

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
      const bundle = await getCommercialBundle({
        ...params,
        seller_limit: 50,
        unique_limit: 300,
        lost_limit: 100,
        cross_limit: 100,
        product_limit: 50,
      })
      setDashboard(bundle.dashboard)
      setCrm(bundle.crm ?? null)
      setAnalysisScope(bundle.analysis_scope ?? null)
      setMeta(bundle.meta)
      setSummary(bundle.summary)
      setAttackPlan(bundle.attack_plan)
      setOpportunities(bundle.opportunities)
      setSellers(bundle.seller_performance.items)
      setRankings(bundle.seller_performance.rankings)
      setUniqueClients(bundle.unique_clients.items)
      setLostClients(bundle.lost_clients.items)
      setCrossSelling(bundle.cross_selling.items)
      setProducts(bundle.product_performance)
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

  const openSellerProfile = useCallback(
    async (sellerName: string) => {
      setProfileSellerName(sellerName)
      setSellerProfileLoading(true)
      setSellerProfile(null)
      try {
        const p = await getCommercialSellerProfile(sellerName, {
          date_from: dateFrom,
          date_to: dateTo,
          document_type: documentType === "all" ? undefined : documentType,
        })
        setSellerProfile(p)
      } catch {
        setSellerProfile(null)
      } finally {
        setSellerProfileLoading(false)
      }
    },
    [dateFrom, dateTo, documentType],
  )

  const dailySpark = useMemo(
    () => dashboard?.daily_sales.slice(-14).map((d) => ({ v: d.venta_neta })) ?? [],
    [dashboard],
  )

  const handleRadarClick = useCallback(
    (blockId: string) => {
      const map: Record<string, string> = {
        clientes_perdidos: "perdidos",
        clientes_riesgo: "clientes",
        cross_selling: "oportunidades",
        productos: "productos",
        productos_nuevos: "productos",
        nuevos: "clientes",
        vip: "clientes",
        oportunidades: "oportunidades",
        perdidos: "perdidos",
        recuperados: "clientes",
      }
      setTab(map[blockId] ?? "oportunidades")
    },
    [],
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
          <h1 className="text-2xl font-bold tracking-tight">CRM Comercial</h1>
          <p className="text-sm text-muted-foreground">
            Director Comercial Digital — Facturas y boletas (Company 3 / Office 1)
          </p>
          {meta && (
            <p className="mt-1 text-xs text-muted-foreground">
              Análisis generado {formatMinutesAgo(meta.generated_at)}
              {" · "}
              {meta.documents_analyzed.toLocaleString("es-CL")} documentos
              {" · "}
              {meta.execution_ms.toFixed(0)} ms
            </p>
          )}
        </div>
        <Button variant="outline" size="sm" onClick={() => void load()} disabled={loading}>
          {loading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
          Actualizar
        </Button>
      </div>

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
            <TabsTrigger value="home">Home CRM</TabsTrigger>
            <TabsTrigger value="mapa">Mapa</TabsTrigger>
            <TabsTrigger value="dashboard">Detalle KPIs</TabsTrigger>
            <TabsTrigger value="vendedores">Vendedores</TabsTrigger>
            <TabsTrigger value="oportunidades">Oportunidades</TabsTrigger>
            <TabsTrigger value="clientes">Clientes</TabsTrigger>
            <TabsTrigger value="perdidos">Perdidos</TabsTrigger>
            <TabsTrigger value="productos">Productos</TabsTrigger>
          </TabsList>

          <TabsContent value="home" className="space-y-6">
            {crm ? (
              <>
                <CommercialCrmHome
                  crm={crm}
                  analysisScope={analysisScope ?? undefined}
                  dailySpark={dailySpark}
                  selectedSeller={seller || undefined}
                  onClientClick={(id) => void openProfile(id)}
                  onSellerClick={(name) => void openSellerProfile(name)}
                  onRadarClick={handleRadarClick}
                />
                <CommercialSimulator
                  dateFrom={dateFrom}
                  dateTo={dateTo}
                  documentType={documentType === "all" ? undefined : documentType}
                  seller={seller || undefined}
                />
              </>
            ) : (
              <p className="py-12 text-center text-muted-foreground">Cargando inteligencia comercial…</p>
            )}
          </TabsContent>

          <TabsContent value="mapa" className="space-y-6">
            <CommercialMapClient params={params} onClientClick={(id) => void openProfile(id)} />
          </TabsContent>

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
                <div className="grid gap-6 lg:grid-cols-2">
                  <Card>
                    <CardHeader><CardTitle className="text-base">Top riesgos</CardTitle></CardHeader>
                    <CardContent className="space-y-2">
                      {opportunities.filter((o) => o.tipo.includes("riesgo") || o.tipo === "cliente_perdido").slice(0, 5).map((o, i) => (
                        <div key={i} className="cursor-pointer rounded-md border p-2 text-sm hover:bg-muted/50" onClick={() => o.client_id && void openProfile(o.client_id)}>
                          <div className="flex justify-between"><span className="font-medium">{o.titulo}</span><PrioridadBadge prioridad={o.prioridad} /></div>
                          <p className="text-xs text-muted-foreground">{o.explicacion}</p>
                        </div>
                      ))}
                    </CardContent>
                  </Card>
                  <Card>
                    <CardHeader><CardTitle className="text-base">Top oportunidades</CardTitle></CardHeader>
                    <CardContent className="space-y-2">
                      {opportunities.filter((o) => o.tipo === "cross_selling" || o.tipo.includes("producto")).slice(0, 5).map((o, i) => (
                        <div key={i} className="rounded-md border p-2 text-sm">
                          <div className="flex justify-between"><span className="font-medium">{o.titulo}</span><PrioridadBadge prioridad={o.prioridad} /></div>
                          <p className="text-xs text-muted-foreground">{o.accion_sugerida}</p>
                        </div>
                      ))}
                    </CardContent>
                  </Card>
                </div>
              </>
            )}
          </TabsContent>

          <TabsContent value="vendedores" className="space-y-6">
            <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
              {sellers.map((s) => (
                <Card
                  key={s.seller_name}
                  className="cursor-pointer transition-shadow hover:shadow-md"
                  onClick={() => void openSellerProfile(s.seller_name)}
                >
                  <CardHeader className="pb-2">
                    <div className="flex items-start justify-between gap-2">
                      <CardTitle className="text-base">{s.seller_name}</CardTitle>
                      <SellerScoreBadge score={s.commercial_score} label={s.score_status_label} />
                    </div>
                  </CardHeader>
                  <CardContent className="space-y-2 text-sm">
                    <div className="flex justify-between"><span className="text-muted-foreground">Venta</span><span className="font-semibold">{formatCLP(s.venta_actual)}</span></div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Clientes únicos</span>
                      <span>{s.clientes_unicos_actual}{s.clientes_unicos_variacion_pct != null && <span className={cn("ml-1", s.clientes_unicos_variacion_pct < 0 ? "text-red-600" : "text-emerald-600")}> ({formatPct(s.clientes_unicos_variacion_pct)})</span>}</span>
                    </div>
                    <div className="flex justify-between"><span className="text-muted-foreground">Perdidos / Rec.</span><span><span className="text-red-600">{s.clientes_perdidos}</span> / <span className="text-emerald-600">{s.clientes_recuperados}</span></span></div>
                    {s.accion_sugerida && <div className="mt-2 rounded-md bg-muted/60 p-2 text-xs"><span className="font-medium">Acción: </span>{s.accion_sugerida}</div>}
                  </CardContent>
                </Card>
              ))}
            </div>
          </TabsContent>

          <TabsContent value="oportunidades">
            <Card>
              <CardHeader><CardTitle className="text-base">Oportunidades ({opportunities.length})</CardTitle></CardHeader>
              <CardContent className="grid gap-3 md:grid-cols-2">
                {opportunities.map((o, i) => (
                  <div key={i} className={cn("rounded-lg border p-4 text-sm", o.client_id && "cursor-pointer hover:bg-muted/50")} onClick={() => o.client_id && void openProfile(o.client_id)}>
                    <div className="mb-2 flex gap-2"><Badge variant="outline">{o.tipo}</Badge><PrioridadBadge prioridad={o.prioridad} /></div>
                    <p className="font-semibold">{o.titulo}</p>
                    <p className="text-sm text-muted-foreground">{o.explicacion}</p>
                    <p className="mt-2 text-primary">{o.accion_sugerida}</p>
                  </div>
                ))}
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
                      <TableHead className="text-right">Score</TableHead>
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
                          {"client_health_label" in c && c.client_health_label && (
                            <span className="ml-1 text-xs text-muted-foreground">{String(c.client_health_label)}</span>
                          )}
                        </TableCell>
                        <TableCell className="text-right font-medium">{c.client_score ?? "—"}</TableCell>
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

          <TabsContent value="cross" className="hidden">
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
                <div className="mb-2 flex flex-wrap items-center gap-2">
                  <h3 className="text-lg font-semibold">{profile.client.client_name}</h3>
                  {"client_health_label" in profile.client && profile.client.client_health_label && (
                    <Badge variant="outline">{String(profile.client.client_health_label)}</Badge>
                  )}
                  {"client_score" in profile.client && profile.client.client_score != null && (
                    <Badge>Score {String(profile.client.client_score)}</Badge>
                  )}
                </div>
                <p className="text-sm text-muted-foreground">{profile.client.municipality}</p>
                <p className="text-sm">
                  Vendedor:{" "}
                  <button
                    type="button"
                    className="text-primary underline-offset-2 hover:underline"
                    onClick={() => void openSellerProfile(profile.client.seller_name)}
                  >
                    {profile.client.seller_name}
                  </button>
                </p>
                <div className="mt-3 flex flex-wrap gap-2">
                  {(["favorito", "critico", "vip", "observacion"] as const).map((tag) => (
                    <Button
                      key={tag}
                      size="sm"
                      variant={watchlist.clients[profile.client.client_id] === tag ? "default" : "outline"}
                      onClick={() => toggleWatchlistClient(profile.client.client_id, tag)}
                    >
                      {tag}
                    </Button>
                  ))}
                </div>
                <div className="mt-2 grid grid-cols-2 gap-2 text-sm">
                  <div>Última compra: {profile.client.ultima_compra ?? "—"}</div>
                  <div>Ticket prom.: {formatCLP(profile.client.ticket_promedio)}</div>
                  <div>Total compras: {profile.client.total_compras}</div>
                  <div>Venta total: {formatCLP(profile.client.venta_total)}</div>
                  {"dias_sin_comprar" in profile.client && (
                    <div>Días sin comprar: {profile.client.dias_sin_comprar ?? "—"}</div>
                  )}
                  {"frecuencia_dias" in profile.client && profile.client.frecuencia_dias && (
                    <div>Frecuencia: ~{profile.client.frecuencia_dias} días</div>
                  )}
                  {profile.client.potencial_mensual != null && (
                    <div>Potencial mensual: {formatCLP(profile.client.potencial_mensual)}</div>
                  )}
                  {profile.client.probabilidad_abandono != null && (
                    <div>Prob. abandono: {profile.client.probabilidad_abandono}%</div>
                  )}
                  {profile.client.probabilidad_recuperacion != null && (
                    <div>Prob. recuperación: {profile.client.probabilidad_recuperacion}%</div>
                  )}
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

              {"productos_abandonados" in profile && (profile.productos_abandonados as { producto: string }[]).length > 0 && (
                <div>
                  <p className="mb-2 text-sm font-medium">Productos abandonados</p>
                  <ul className="space-y-1 text-sm text-muted-foreground">
                    {(profile.productos_abandonados as { producto: string; ultima_compra?: string }[]).map((p, i) => (
                      <li key={i}>{p.producto}{p.ultima_compra ? ` — ${p.ultima_compra}` : ""}</li>
                    ))}
                  </ul>
                </div>
              )}

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

      <Sheet open={profileSellerName !== null} onOpenChange={(o) => !o && setProfileSellerName(null)}>
        <SheetContent className="w-full overflow-y-auto sm:max-w-xl">
          <SheetHeader>
            <SheetTitle>Ficha vendedor</SheetTitle>
          </SheetHeader>
          {sellerProfileLoading ? (
            <div className="flex justify-center py-12">
              <Loader2 className="h-6 w-6 animate-spin" />
            </div>
          ) : sellerProfile ? (
            <div className="mt-6 space-y-6">
              <div className="flex gap-4">
                <div className="flex h-16 w-16 shrink-0 items-center justify-center rounded-full bg-primary/10 text-2xl font-bold text-primary">
                  {sellerProfile.seller.seller_name.slice(0, 1)}
                </div>
                <div>
                  <h3 className="text-lg font-semibold">{sellerProfile.seller.seller_name}</h3>
                  <div className="mt-1 flex flex-wrap gap-2">
                    <Badge>Score {sellerProfile.seller.commercial_score ?? "—"}</Badge>
                    <Badge variant="outline">{sellerProfile.seller.score_status_label}</Badge>
                    {sellerProfile.seller.ranking_posicion != null && (
                      <Badge variant="secondary">
                        #{sellerProfile.seller.ranking_posicion} de {sellerProfile.seller.ranking_total}
                      </Badge>
                    )}
                  </div>
                </div>
              </div>

              <div className="grid grid-cols-2 gap-3 text-sm">
                <div className="rounded-lg border p-3">
                  <p className="text-muted-foreground">Venta período</p>
                  <p className="text-lg font-bold">{formatCLP(sellerProfile.seller.venta_actual)}</p>
                  <p className={cn("text-xs", sellerProfile.seller.variacion_pct >= 0 ? "text-emerald-600" : "text-red-600")}>
                    {formatPct(sellerProfile.seller.variacion_pct)}
                  </p>
                </div>
                <div className="rounded-lg border p-3">
                  <p className="text-muted-foreground">Clientes únicos</p>
                  <p className="text-lg font-bold">{sellerProfile.seller.clientes_unicos}</p>
                  <p className="text-xs text-muted-foreground">
                    +{sellerProfile.seller.clientes_nuevos} nuevos · {sellerProfile.seller.clientes_perdidos} perdidos
                  </p>
                </div>
              </div>

              {sellerProfile.seller.score_explanation && (
                <div className="text-sm">
                  <p className="mb-2 font-medium">Por qué este score</p>
                  <div className="flex flex-wrap gap-2">
                    {sellerProfile.seller.score_explanation.positives.map((p) => (
                      <span key={p} className="text-emerald-600">
                        ✔ {p}
                      </span>
                    ))}
                    {sellerProfile.seller.score_explanation.negatives.map((n) => (
                      <span key={n} className="text-red-600">
                        ✖ {n}
                      </span>
                    ))}
                  </div>
                </div>
              )}

              {sellerProfile.forecast_personal && (
                <div className="rounded-lg border border-amber-500/30 bg-amber-500/5 p-3 text-sm">
                  <p className="font-medium">Forecast personal</p>
                  <p>
                    Aporte necesario:{" "}
                    <strong>{formatCLP(sellerProfile.forecast_personal.aporte_necesario)}</strong>
                  </p>
                </div>
              )}

              {sellerProfile.acciones_sugeridas.length > 0 && (
                <div>
                  <p className="mb-2 text-sm font-medium">Acciones sugeridas</p>
                  <ul className="list-inside list-disc space-y-1 text-sm text-muted-foreground">
                    {sellerProfile.acciones_sugeridas.map((a, i) => (
                      <li key={i}>{a}</li>
                    ))}
                  </ul>
                </div>
              )}

              {sellerProfile.comunas.length > 0 && (
                <div>
                  <p className="mb-2 text-sm font-medium">Comunas</p>
                  <ul className="space-y-1 text-sm">
                    {sellerProfile.comunas.slice(0, 8).map((c) => (
                      <li key={c.comuna} className="flex justify-between">
                        <span>{c.comuna}</span>
                        <span className="text-muted-foreground">
                          {c.clientes} cli. · {formatCLP(c.venta)}
                        </span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}

              {sellerProfile.evolucion_mensual.length > 0 && (
                <div className="h-48">
                  <p className="mb-2 text-sm font-medium">Evolución mensual</p>
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={sellerProfile.evolucion_mensual}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="mes" tick={{ fontSize: 10 }} />
                      <YAxis tick={{ fontSize: 10 }} tickFormatter={(v) => `${(v / 1e6).toFixed(0)}M`} />
                      <Tooltip formatter={(v: number) => formatCLP(v)} />
                      <Bar dataKey="venta" fill="#3b82f6" radius={[4, 4, 0, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              )}

              {sellerProfile.ia_narrativas.length > 0 && (
                <div>
                  <p className="mb-2 text-sm font-medium">IA Comercial</p>
                  {sellerProfile.ia_narrativas.map((n, i) => (
                    <div key={i} className="mb-2 rounded-md border p-3 text-sm">
                      {n.parrafos.map((p, j) => (
                        <p key={j} className="text-muted-foreground">
                          {p}
                        </p>
                      ))}
                    </div>
                  ))}
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
