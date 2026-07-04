"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import {
  AlertTriangle,
  Brain,
  FileWarning,
  Loader2,
  MapPin,
  RefreshCw,
  Search,
  TrendingDown,
  TrendingUp,
} from "lucide-react"

import { ReturnsDetailSheet } from "@/components/notas-credito/returns-detail-sheet"
import { ReturnsTimelineChart } from "@/components/notas-credito/returns-timeline-chart"
import { formatCLP, formatDate, formatPct } from "@/components/notas-credito/format"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
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
import { hasAdminAccess, staffUserFromLocalStorage } from "@/lib/permissions"
import {
  getReturnsDashboard,
  getReturnsInsights,
  getReturnsList,
  getReturnsMap,
  getReturnsRankings,
  getReturnsTimeline,
  syncReturnsHistory,
  syncReturnsIncremental,
  type ReturnsDashboardResponse,
  type ReturnsInsight,
  type ReturnsListItem,
  type ReturnsRankingsResponse,
} from "@/lib/returns-analytics-api"

function KpiCard({
  title,
  value,
  hint,
  accent,
}: {
  title: string
  value: string | number
  hint?: string
  accent?: boolean
}) {
  return (
    <Card className={cn(accent && "border-red-900/25 bg-red-950/[0.03]")}>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium text-muted-foreground">{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <p className={cn("text-2xl font-semibold tabular-nums", accent && "text-red-800 dark:text-red-300")}>
          {value}
        </p>
        {hint ? <p className="mt-1 text-xs text-muted-foreground">{hint}</p> : null}
      </CardContent>
    </Card>
  )
}

function TrendBadge({ delta, pct }: { delta: number; pct: number }) {
  const up = delta > 0
  const Icon = up ? TrendingUp : TrendingDown
  return (
    <Badge
      variant="outline"
      className={cn(
        "gap-1 font-normal tabular-nums",
        up ? "border-red-300 text-red-700 dark:text-red-400" : "border-emerald-300 text-emerald-700",
      )}
    >
      <Icon className="h-3 w-3" />
      {formatPct(pct, 1)} ({formatCLP(delta)})
    </Badge>
  )
}

function SeverityIcon({ severity }: { severity: string }) {
  const cls =
    severity === "high"
      ? "text-red-600"
      : severity === "medium"
        ? "text-amber-600"
        : "text-slate-500"
  return <AlertTriangle className={cn("h-4 w-4 shrink-0", cls)} />
}

export default function NotasCreditoPage() {
  const [dateFrom, setDateFrom] = useState("2026-01-01")
  const [dateTo, setDateTo] = useState("2026-06-30")
  const [grain, setGrain] = useState<"day" | "week" | "month" | "year">("day")
  const [tab, setTab] = useState("dashboard")

  const [dashboard, setDashboard] = useState<ReturnsDashboardResponse | null>(null)
  const [rankings, setRankings] = useState<ReturnsRankingsResponse | null>(null)
  const [list, setList] = useState<ReturnsListItem[]>([])
  const [mapRows, setMapRows] = useState<{ municipality: string; quantity: number; amount: number; top_motive: string }[]>([])
  const [timeline, setTimeline] = useState<{ bucket: string; quantity: number; amount: number }[]>([])
  const [insights, setInsights] = useState<{ insights: ReturnsInsight[]; recommendations: string[] } | null>(null)

  const [loading, setLoading] = useState(true)
  const [syncing, setSyncing] = useState<"history" | "incremental" | null>(null)
  const [error, setError] = useState<string | null>(null)
  const isAdmin = hasAdminAccess(staffUserFromLocalStorage())

  const [detailId, setDetailId] = useState<number | null>(null)
  const [detailOpen, setDetailOpen] = useState(false)
  const [searchId, setSearchId] = useState("")

  const params = useMemo(() => ({ date_from: dateFrom, date_to: dateTo }), [dateFrom, dateTo])

  const openDetail = useCallback((id: number) => {
    setDetailId(id)
    setDetailOpen(true)
  }, [])

  const loadAll = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const [d, r, l, m, t, i] = await Promise.all([
        getReturnsDashboard(params),
        getReturnsRankings(params),
        getReturnsList({ ...params, limit: 40 }),
        getReturnsMap(params),
        getReturnsTimeline({ ...params, grain }),
        getReturnsInsights(params),
      ])
      setDashboard(d)
      setRankings(r)
      setList(l)
      setMapRows(m)
      setTimeline(t)
      setInsights(i)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al cargar análisis NC")
    } finally {
      setLoading(false)
    }
  }, [params, grain])

  useEffect(() => {
    void loadAll()
  }, [loadAll])

  const handleHistorySync = async (resume = false) => {
    setSyncing("history")
    setError(null)
    try {
      await syncReturnsHistory(resume)
      await loadAll()
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error en bootstrap histórico")
    } finally {
      setSyncing(null)
    }
  }

  const handleIncrementalSync = async () => {
    setSyncing("incremental")
    setError(null)
    try {
      await syncReturnsIncremental()
      await loadAll()
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error en sync incremental")
    } finally {
      setSyncing(null)
    }
  }

  const handleSearchDetail = () => {
    const id = Number(searchId.trim())
    if (!Number.isFinite(id) || id <= 0) return
    openDetail(id)
  }

  const kpis = dashboard?.kpis
  const scope = dashboard?.scope
  const syncInfo = dashboard?.sync
  const bootstrap = syncInfo?.bootstrap

  return (
    <div className="flex flex-col gap-6 p-4 md:p-6">
      <header className="relative overflow-hidden rounded-xl border border-red-900/20 bg-gradient-to-br from-slate-950 via-red-950/90 to-slate-900 px-6 py-8 text-white shadow-lg">
        <div className="relative z-10 flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <div className="mb-2 flex items-center gap-2 text-red-200/90">
              <FileWarning className="h-5 w-5" />
              <span className="text-xs font-semibold uppercase tracking-widest">Auditoría de pérdidas</span>
            </div>
            <h1 className="text-2xl font-bold tracking-tight md:text-3xl">Análisis de Notas de Crédito</h1>
            <p className="mt-2 max-w-2xl text-sm text-red-100/80">
              Devoluciones y NC — {scope?.company_name ?? "Company 3"} / {scope?.office_name ?? "Office 1"}.
              Módulo independiente del CRM Comercial.
            </p>
          </div>
          <div className="flex flex-wrap items-end gap-3">
            <div className="space-y-1">
              <Label htmlFor="nc-from" className="text-red-100/70">
                Desde
              </Label>
              <Input
                id="nc-from"
                type="date"
                value={dateFrom}
                onChange={(e) => setDateFrom(e.target.value)}
                className="border-white/20 bg-white/10 text-white"
              />
            </div>
            <div className="space-y-1">
              <Label htmlFor="nc-to" className="text-red-100/70">
                Hasta
              </Label>
              <Input
                id="nc-to"
                type="date"
                value={dateTo}
                onChange={(e) => setDateTo(e.target.value)}
                className="border-white/20 bg-white/10 text-white"
              />
            </div>
            <Button
              variant="secondary"
              onClick={() => void loadAll()}
              disabled={loading}
              className="bg-white/15 text-white hover:bg-white/25"
            >
              {loading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <RefreshCw className="mr-2 h-4 w-4" />}
              Actualizar
            </Button>
            {isAdmin && bootstrap && !bootstrap.completed ? (
              <Button
                onClick={() => void handleHistorySync(bootstrap.resumable)}
                disabled={syncing !== null}
                variant="outline"
                className="border-amber-300/50 bg-transparent text-amber-100"
              >
                {syncing === "history" ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                {bootstrap.resumable ? "Reanudar histórico" : "Cargar histórico H1 2026"}
              </Button>
            ) : null}
            {bootstrap?.completed ? (
              <Button
                onClick={() => void handleIncrementalSync()}
                disabled={syncing !== null}
                variant="outline"
                className="border-white/30 bg-transparent text-white"
              >
                {syncing === "incremental" ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                Sync nuevas NC
              </Button>
            ) : null}
          </div>
        </div>
        {syncInfo ? (
          <div className="relative z-10 mt-4 space-y-1 text-xs text-red-200/60">
            <p>
              Bootstrap {bootstrap?.date_from} → {bootstrap?.date_to}:{" "}
              {bootstrap?.completed ? (
                <span className="text-emerald-300">completado</span>
              ) : bootstrap?.resumable ? (
                <span className="text-amber-300">
                  incompleto ({bootstrap.pages_processed} páginas)
                </span>
              ) : (
                <span className="text-amber-300">pendiente</span>
              )}
              {bootstrap?.completed ? ` · ${bootstrap.records_processed} registros` : null}
            </p>
            {syncInfo.cursor.last_sync_at ? (
              <p>
                Última sync: {formatDate(syncInfo.cursor.last_sync_at)} · {syncInfo.cursor.records_total} registros totales
              </p>
            ) : null}
          </div>
        ) : null}
      </header>

      {error ? (
        <Alert variant="destructive">
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      <div className="flex flex-wrap items-center gap-2">
        <Input
          placeholder="ID devolución Bsale…"
          value={searchId}
          onChange={(e) => setSearchId(e.target.value)}
          className="max-w-[200px]"
          onKeyDown={(e) => e.key === "Enter" && handleSearchDetail()}
        />
        <Button variant="outline" size="sm" onClick={handleSearchDetail}>
          <Search className="mr-2 h-4 w-4" />
          Abrir ficha
        </Button>
      </div>

      <Tabs value={tab} onValueChange={setTab} className="space-y-4">
        <TabsList className="flex h-auto flex-wrap justify-start gap-1 bg-muted/50 p-1">
          <TabsTrigger value="dashboard">Dashboard</TabsTrigger>
          <TabsTrigger value="motivos">Motivos</TabsTrigger>
          <TabsTrigger value="vendedores">Vendedores</TabsTrigger>
          <TabsTrigger value="clientes">Clientes</TabsTrigger>
          <TabsTrigger value="productos">Productos</TabsTrigger>
          <TabsTrigger value="mapa">Mapa</TabsTrigger>
          <TabsTrigger value="timeline">Timeline</TabsTrigger>
          <TabsTrigger value="insights">IA</TabsTrigger>
        </TabsList>

        <TabsContent value="dashboard" className="space-y-4">
          {loading && !dashboard ? (
            <div className="flex justify-center py-16">
              <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
            </div>
          ) : (
            <>
              <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6">
                <KpiCard title="Total NC" value={kpis?.total_nc ?? "—"} />
                <KpiCard title="Total $" value={formatCLP(kpis?.total_amount)} accent />
                <KpiCard title="% sobre ventas" value={formatPct(kpis?.pct_over_sales)} hint={formatCLP(kpis?.sales_net_period)} />
                <KpiCard title="Ticket promedio NC" value={formatCLP(kpis?.ticket_promedio_nc)} />
                <KpiCard title="Clientes afectados" value={kpis?.clients_affected ?? "—"} />
                <KpiCard title="Productos afectados" value={kpis?.products_affected ?? "—"} />
              </div>

              <Card>
                <CardHeader>
                  <CardTitle className="text-base">Últimas devoluciones</CardTitle>
                  <CardDescription>Clic en una fila para abrir la ficha completa.</CardDescription>
                </CardHeader>
                <CardContent>
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Fecha</TableHead>
                        <TableHead>NC</TableHead>
                        <TableHead>Cliente</TableHead>
                        <TableHead>Vendedor</TableHead>
                        <TableHead>Motivo</TableHead>
                        <TableHead className="text-right">Monto</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {list.map((row) => (
                        <TableRow
                          key={row.return_id}
                          className="cursor-pointer hover:bg-red-950/5"
                          onClick={() => openDetail(row.return_id)}
                        >
                          <TableCell>{formatDate(row.return_date)}</TableCell>
                          <TableCell className="font-mono text-xs">{row.credit_note_number ?? row.return_id}</TableCell>
                          <TableCell className="max-w-[140px] truncate">{row.client || "—"}</TableCell>
                          <TableCell className="max-w-[120px] truncate">{row.seller || "—"}</TableCell>
                          <TableCell className="max-w-[160px] truncate">{row.motive || "—"}</TableCell>
                          <TableCell className="text-right font-medium text-red-700 dark:text-red-400">
                            {formatCLP(row.amount)}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </CardContent>
              </Card>
            </>
          )}
        </TabsContent>

        <TabsContent value="motivos">
          <RankingTable
            loading={loading}
            empty="Sin motivos en el período."
            columns={["Motivo", "Cantidad", "Monto", "%", "Tendencia vs período anterior"]}
            rows={(rankings?.motives ?? []).map((m) => [
              m.motive,
              String(m.quantity),
              formatCLP(m.amount),
              formatPct(m.pct),
              <TrendBadge key={m.motive} delta={m.trend_delta} pct={m.trend_pct} />,
            ])}
          />
        </TabsContent>

        <TabsContent value="vendedores">
          <RankingTable
            loading={loading}
            empty="Sin vendedores en el período."
            columns={["Vendedor", "Cantidad", "Monto NC", "% ventas", "Motivos"]}
            rows={(rankings?.sellers ?? []).map((s) => [
              s.seller,
              String(s.quantity),
              formatCLP(s.amount),
              formatPct(s.pct_over_sales),
              (s.motives || []).slice(0, 3).join(" · ") || "—",
            ])}
          />
        </TabsContent>

        <TabsContent value="clientes">
          <RankingTable
            loading={loading}
            empty="Sin clientes en el período."
            columns={["Cliente", "Cantidad", "Monto", "Última NC"]}
            rows={(rankings?.clients ?? []).map((c) => [
              c.client,
              String(c.quantity),
              formatCLP(c.amount),
              formatDate(c.last_return),
            ])}
          />
        </TabsContent>

        <TabsContent value="productos">
          <RankingTable
            loading={loading}
            empty="Sin productos en el período."
            columns={["Producto", "Cant. devuelta", "Monto", "Nº devoluciones"]}
            rows={(rankings?.products ?? []).map((p) => [
              p.product,
              String(p.quantity),
              formatCLP(p.amount),
              String(p.return_count),
            ])}
          />
        </TabsContent>

        <TabsContent value="mapa">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-base">
                <MapPin className="h-4 w-4 text-red-700" />
                NC por comuna
              </CardTitle>
            </CardHeader>
            <CardContent>
              <RankingTable
                loading={loading}
                empty="Sin datos geográficos."
                columns={["Comuna", "Cantidad", "Monto", "Motivo principal"]}
                rows={mapRows.map((r) => [
                  r.municipality,
                  String(r.quantity),
                  formatCLP(r.amount),
                  r.top_motive || "—",
                ])}
              />
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="timeline" className="space-y-4">
          <div className="flex items-center gap-3">
            <Label>Granularidad</Label>
            <Select value={grain} onValueChange={(v) => setGrain(v as typeof grain)}>
              <SelectTrigger className="w-[140px]">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="day">Día</SelectItem>
                <SelectItem value="week">Semana</SelectItem>
                <SelectItem value="month">Mes</SelectItem>
                <SelectItem value="year">Año</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Evolución temporal</CardTitle>
            </CardHeader>
            <CardContent>
              {loading ? (
                <div className="flex justify-center py-12">
                  <Loader2 className="h-6 w-6 animate-spin" />
                </div>
              ) : (
                <ReturnsTimelineChart data={timeline} />
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="insights" className="space-y-4">
          <Card className="border-red-900/20">
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-base">
                <Brain className="h-5 w-5 text-red-700" />
                Detección automática
              </CardTitle>
              <CardDescription>Reglas v1 — sin LLM. Prioriza pérdidas y reincidencia.</CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              {loading ? (
                <Loader2 className="mx-auto h-6 w-6 animate-spin" />
              ) : (
                <>
                  <ul className="space-y-3">
                    {(insights?.insights ?? []).map((item, i) => (
                      <li
                        key={`${item.type}-${i}`}
                        className="flex gap-3 rounded-lg border border-red-900/10 bg-red-950/[0.02] p-4"
                      >
                        <SeverityIcon severity={item.severity} />
                        <div>
                          <p className="font-medium">{item.title}</p>
                          <p className="mt-1 text-sm text-muted-foreground">{item.description}</p>
                          <p className="mt-1 text-xs text-red-700 dark:text-red-400">
                            Impacto: {formatCLP(item.impact)}
                          </p>
                        </div>
                      </li>
                    ))}
                  </ul>
                  {(insights?.recommendations ?? []).length > 0 ? (
                    <div>
                      <h3 className="mb-2 text-sm font-semibold">Recomendaciones</h3>
                      <ul className="list-disc space-y-1 pl-5 text-sm text-muted-foreground">
                        {insights?.recommendations.map((r, i) => (
                          <li key={i}>{r}</li>
                        ))}
                      </ul>
                    </div>
                  ) : null}
                </>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>

      <ReturnsDetailSheet returnId={detailId} open={detailOpen} onOpenChange={setDetailOpen} />
    </div>
  )
}

function RankingTable({
  loading,
  empty,
  columns,
  rows,
}: {
  loading: boolean
  empty: string
  columns: string[]
  rows: (string | React.ReactNode)[][]
}) {
  if (loading) {
    return (
      <div className="flex justify-center py-16">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    )
  }
  if (!rows.length) {
    return <p className="py-12 text-center text-sm text-muted-foreground">{empty}</p>
  }
  return (
    <Card>
      <CardContent className="pt-6">
        <Table>
          <TableHeader>
            <TableRow>
              {columns.map((c) => (
                <TableHead key={c}>{c}</TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((row, i) => (
              <TableRow key={i}>
                {row.map((cell, j) => (
                  <TableCell key={j} className={j > 0 ? "tabular-nums" : ""}>
                    {cell}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  )
}
