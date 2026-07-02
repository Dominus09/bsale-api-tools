"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts"
import {
  ArrowDownRight,
  ArrowUpRight,
  Award,
  Crosshair,
  Minus,
  Radar,
  Star,
  Target,
  TrendingUp,
  Users,
  Wallet,
  Zap,
} from "lucide-react"

import type {
  CommercialCrmLayer,
  CommercialDailyMission,
  CommercialEstadoCard,
  CommercialExecutiveCard,
  CommercialRadarStructured,
} from "@/lib/api"
import { CommercialAgenda, CommercialRouteTargets } from "@/components/comercial/commercial-agenda"
import { CommercialAlertsStrip } from "@/components/comercial/commercial-alerts-strip"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Progress } from "@/components/ui/progress"
import { cn } from "@/lib/utils"

const WATCHLIST_KEY = "commercial-crm-watchlist-v1"

type Watchlist = {
  clients: Record<number, "favorito" | "critico" | "vip" | "observacion">
  sellers: Record<string, "observacion">
}

function formatCLP(n: number): string {
  return n.toLocaleString("es-CL", { style: "currency", currency: "CLP", maximumFractionDigits: 0 })
}

function formatCardValue(card: CommercialEstadoCard): string {
  if (card.format === "currency") return formatCLP(card.value)
  if (card.format === "percent") return `${card.value.toFixed(1)}%`
  return card.value.toLocaleString("es-CL")
}

const MISSION_COLORS: Record<string, string> = {
  RECUPERAR: "border-red-500/40 bg-red-500/5",
  CROSS_SELLING: "border-blue-500/40 bg-blue-500/5",
  CLIENTE_EN_RIESGO: "border-amber-500/40 bg-amber-500/5",
  PRODUCTO: "border-violet-500/40 bg-violet-500/5",
  VIAJAR: "border-emerald-500/40 bg-emerald-500/5",
  CLIENTE_VIP: "border-yellow-500/40 bg-yellow-500/5",
  CLIENTE_NUEVO: "border-sky-500/40 bg-sky-500/5",
}

const RADAR_COLORS: Record<string, string> = {
  red: "from-red-500/20 to-red-500/5 border-red-500/30",
  purple: "from-purple-500/20 to-purple-500/5 border-purple-500/30",
  blue: "from-blue-500/20 to-blue-500/5 border-blue-500/30",
  amber: "from-amber-500/20 to-amber-500/5 border-amber-500/30",
  emerald: "from-emerald-500/20 to-emerald-500/5 border-emerald-500/30",
  sky: "from-sky-500/20 to-sky-500/5 border-sky-500/30",
  yellow: "from-yellow-500/20 to-yellow-500/5 border-yellow-500/30",
}

const RADAR_KEYS: { key: keyof CommercialRadarStructured; label: string; color: string }[] = [
  { key: "clientes_perdidos", label: "Clientes Perdidos", color: "red" },
  { key: "clientes_riesgo", label: "Clientes en Riesgo", color: "amber" },
  { key: "cross_selling", label: "Cross Selling", color: "blue" },
  { key: "productos", label: "Productos", color: "purple" },
  { key: "nuevos", label: "Clientes Nuevos", color: "sky" },
  { key: "vip", label: "VIP", color: "yellow" },
  { key: "oportunidades", label: "Oportunidades", color: "emerald" },
]

function TrendBadge({ trend, delta }: { trend: string; delta: number }) {
  const Icon = trend === "up" ? ArrowUpRight : trend === "down" ? ArrowDownRight : Minus
  const color = trend === "up" ? "text-emerald-600" : trend === "down" ? "text-red-600" : "text-muted-foreground"
  return (
    <span className={cn("flex items-center gap-0.5 text-xs font-medium", color)}>
      <Icon className="h-3.5 w-3.5" />
      {delta > 0 ? "+" : ""}
      {delta.toFixed(1)}%
    </span>
  )
}

function EstadoCard({
  card,
  spark,
}: {
  card: CommercialEstadoCard
  spark?: { v: number }[]
}) {
  const icons: Record<string, React.ReactNode> = {
    ventas_hoy: <Wallet className="h-5 w-5 text-blue-500" />,
    clientes_hoy: <Users className="h-5 w-5 text-emerald-500" />,
    clientes_periodo: <Users className="h-5 w-5 text-violet-500" />,
    venta_proyectada: <TrendingUp className="h-5 w-5 text-indigo-500" />,
    meta_mes: <Target className="h-5 w-5 text-amber-500" />,
    forecast: <Crosshair className="h-5 w-5 text-cyan-500" />,
    recuperacion: <Zap className="h-5 w-5 text-orange-500" />,
  }
  return (
    <Card className="overflow-hidden border-0 bg-gradient-to-br from-card to-muted/30 shadow-sm">
      <CardContent className="p-5">
        <div className="mb-3 flex items-start justify-between">
          <div>
            <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">{card.label}</p>
            <p className="mt-1 text-2xl font-bold tracking-tight">{formatCardValue(card)}</p>
          </div>
          <div className="rounded-lg bg-background/80 p-2 shadow-sm">{icons[card.key] ?? <Wallet className="h-5 w-5" />}</div>
        </div>
        {card.previous != null && card.format !== "percent" && (
          <TrendBadge trend={card.trend} delta={card.delta_pct} />
        )}
        {spark && spark.length > 1 && (
          <div className="mt-3 h-10">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={spark}>
                <Area type="monotone" dataKey="v" stroke="#3b82f6" fill="#3b82f633" strokeWidth={1.5} dot={false} />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        )}
      </CardContent>
    </Card>
  )
}

function MissionCard({
  mission,
  onClientClick,
  onSellerClick,
  watchlisted,
}: {
  mission: CommercialDailyMission
  onClientClick?: (id: number) => void
  onSellerClick?: (name: string) => void
  watchlisted?: boolean
}) {
  const clickable = Boolean(mission.client_id && onClientClick)
  return (
    <div
      className={cn(
        "rounded-xl border-2 p-4 transition-shadow hover:shadow-md",
        MISSION_COLORS[mission.mission_type] ?? "border-border bg-card",
        clickable && "cursor-pointer",
        watchlisted && "ring-2 ring-amber-400/60",
      )}
      onClick={() => mission.client_id && onClientClick?.(mission.client_id)}
    >
      <div className="mb-2 flex items-center justify-between gap-2">
        <Badge variant="outline" className="text-[10px] font-bold uppercase tracking-wider">
          {mission.mission_type.replace(/_/g, " ")}
        </Badge>
        {mission.probabilidad_label && (
          <span className="text-sm text-amber-500" title="Probabilidad">
            {mission.probabilidad_label}
          </span>
        )}
      </div>
      <h4 className="text-base font-semibold leading-tight">{mission.titulo}</h4>
      {mission.subtitulo && <p className="mt-1 text-sm text-muted-foreground line-clamp-2">{mission.subtitulo}</p>}
      {mission.detalle && <p className="mt-1 text-xs text-muted-foreground">{mission.detalle}</p>}
      <div className="mt-3 flex flex-wrap items-end justify-between gap-2">
        <div>
          {mission.monto_estimado != null && mission.monto_estimado > 0 && (
            <p className="text-lg font-bold text-primary">{formatCLP(mission.monto_estimado)}</p>
          )}
          {mission.accion && <p className="text-xs font-medium text-foreground/80">{mission.accion}</p>}
        </div>
        {mission.seller_name && (
          <button
            type="button"
            className="text-xs text-muted-foreground underline-offset-2 hover:underline"
            onClick={(e) => {
              e.stopPropagation()
              onSellerClick?.(mission.seller_name!)
            }}
          >
            {mission.seller_name}
          </button>
        )}
      </div>
    </div>
  )
}

function ExecutiveCardItem({
  card,
  onAction,
}: {
  card: CommercialExecutiveCard
  onAction?: (clientId: number | null) => void
}) {
  return (
    <Card className="border-l-4 border-l-primary/50 bg-gradient-to-r from-muted/40 to-background">
      <CardContent className="p-4">
        <div className="flex gap-3">
          <span className="text-2xl">{card.emoji}</span>
          <div className="min-w-0 flex-1">
            <p className="font-semibold leading-snug">{card.titulo}</p>
            <p className="mt-1 text-sm text-muted-foreground">{card.descripcion}</p>
            {card.monto_estimado != null && card.monto_estimado > 0 && (
              <p className="mt-2 text-sm font-bold">{formatCLP(card.monto_estimado)}</p>
            )}
            {card.action_label && (
              <Button
                variant="link"
                className="mt-1 h-auto p-0 text-xs"
                onClick={() => onAction?.(card.client_id)}
              >
                {card.action_label}
              </Button>
            )}
          </div>
        </div>
      </CardContent>
    </Card>
  )
}

export function CommercialCrmHome({
  crm,
  dailySpark,
  selectedSeller,
  onClientClick,
  onSellerClick,
  onRadarClick,
}: {
  crm: CommercialCrmLayer
  dailySpark?: { v: number }[]
  selectedSeller?: string
  onClientClick?: (clientId: number) => void
  onSellerClick?: (sellerName: string) => void
  onRadarClick?: (blockId: string) => void
}) {
  const [watchlist, setWatchlist] = useState<Watchlist>({ clients: {}, sellers: {} })

  useEffect(() => {
    try {
      const raw = localStorage.getItem(WATCHLIST_KEY)
      if (raw) setWatchlist(JSON.parse(raw) as Watchlist)
    } catch {
      /* ignore */
    }
  }, [])

  const persistWatchlist = useCallback((next: Watchlist) => {
    setWatchlist(next)
    try {
      localStorage.setItem(WATCHLIST_KEY, JSON.stringify(next))
    } catch {
      /* ignore */
    }
  }, [])

  const watchlistedMissions = useMemo(() => {
    const ids = new Set(Object.keys(watchlist.clients).map(Number))
    return crm.daily_missions.filter((m) => m.client_id && ids.has(m.client_id))
  }, [crm.daily_missions, watchlist.clients])

  const priorityMissions = useMemo(() => {
    const seen = new Set<number>()
    const merged: CommercialDailyMission[] = []
    for (const m of [...watchlistedMissions, ...crm.daily_missions]) {
      if (typeof m.client_id === "number") {
        if (seen.has(m.client_id)) continue
        seen.add(m.client_id)
      }
      merged.push(m)
      if (merged.length >= 24) break
    }
    return merged
  }, [watchlistedMissions, crm.daily_missions])

  return (
    <div className="space-y-8">
      {/* Objetivos del día */}
      <Card className="border-primary/30 bg-gradient-to-r from-primary/10 via-primary/5 to-background">
        <CardContent className="flex flex-wrap items-center justify-between gap-4 p-6">
          <div>
            <p className="text-xs font-semibold uppercase tracking-widest text-primary">Director Comercial Digital</p>
            <h2 className="text-xl font-bold">{crm.objetivos_diarios.titulo}</h2>
          </div>
          <div className="flex flex-wrap gap-6 text-sm">
            <div>
              <p className="text-muted-foreground">Visitar</p>
              <p className="text-2xl font-bold">{crm.objetivos_diarios.visitar_clientes}</p>
            </div>
            <div>
              <p className="text-muted-foreground">Recuperar</p>
              <p className="text-2xl font-bold text-red-600">{crm.objetivos_diarios.recuperar_clientes}</p>
            </div>
            <div>
              <p className="text-muted-foreground">Cross-selling</p>
              <p className="text-2xl font-bold text-blue-600">{crm.objetivos_diarios.cross_selling}</p>
            </div>
            <div>
              <p className="text-muted-foreground">Potencial</p>
              <p className="text-2xl font-bold text-emerald-600">{formatCLP(crm.objetivos_diarios.monto_potencial)}</p>
            </div>
          </div>
        </CardContent>
      </Card>

      {(crm.alerts?.length ?? 0) > 0 && (
        <CommercialAlertsStrip
          alerts={crm.alerts!}
          onClientClick={onClientClick}
          onSellerClick={onSellerClick}
        />
      )}

      {/* Estado hoy */}
      <section>
        <h2 className="mb-4 text-lg font-semibold tracking-tight">Estado Comercial Hoy</h2>
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
          {crm.estado_hoy.cards.map((card) => (
            <EstadoCard
              key={card.key}
              card={card}
              spark={card.key === "ventas_hoy" ? dailySpark : undefined}
            />
          ))}
        </div>
      </section>

      {/* Resumen ejecutivo */}
      {crm.executive_cards.length > 0 && (
        <section>
          <h2 className="mb-4 text-lg font-semibold tracking-tight">Resumen Ejecutivo</h2>
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
            {crm.executive_cards.map((card, i) => (
              <ExecutiveCardItem key={i} card={card} onAction={(id) => id && onClientClick?.(id)} />
            ))}
          </div>
        </section>
      )}

      {/* Agenda inteligente */}
      {crm.agenda && (
        <CommercialAgenda
          agenda={crm.agenda}
          selectedSeller={selectedSeller}
          onClientClick={onClientClick}
          onSellerClick={onSellerClick}
        />
      )}

      {/* Plan de ataque */}
      <section>
        <div className="mb-4 flex items-center justify-between">
          <h2 className="text-lg font-semibold tracking-tight">Plan de Ataque del Día</h2>
          <Badge variant="secondary">{priorityMissions.length} misiones</Badge>
        </div>
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {priorityMissions.map((m, i) => (
            <MissionCard
              key={`${m.mission_type}-${m.client_id ?? i}`}
              mission={m}
              onClientClick={onClientClick}
              onSellerClick={onSellerClick}
              watchlisted={Boolean(m.client_id && watchlist.clients[m.client_id])}
            />
          ))}
        </div>
      </section>

      {/* Forecast */}
      <section className="grid gap-6 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-base">
              <Target className="h-4 w-4" />
              Forecast Comercial
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-2 gap-4 text-sm">
              <div>
                <p className="text-muted-foreground">Meta</p>
                <p className="text-xl font-bold">{formatCLP(crm.forecast.meta)}</p>
              </div>
              <div>
                <p className="text-muted-foreground">Proyección</p>
                <p className="text-xl font-bold">{formatCLP(crm.forecast.proyeccion)}</p>
              </div>
            </div>
            <div>
              <div className="mb-1 flex justify-between text-sm">
                <span>Cumplimiento</span>
                <span className="font-semibold">{crm.forecast.cumplimiento_pct}%</span>
              </div>
              <Progress value={Math.min(100, crm.forecast.cumplimiento_pct)} className="h-2" />
            </div>
            {crm.forecast.faltan > 0 && (
              <p className="text-sm text-amber-600">
                Faltan <strong>{formatCLP(crm.forecast.faltan)}</strong> para la meta
              </p>
            )}
            <div className="space-y-2 border-t pt-3">
              <p className="text-xs font-semibold uppercase text-muted-foreground">Aporte por vendedor</p>
              {crm.forecast.seller_aportes.slice(0, 6).map((s) => (
                <button
                  key={s.seller_name}
                  type="button"
                  className="flex w-full items-center justify-between rounded-md px-2 py-1.5 text-sm hover:bg-muted/60"
                  onClick={() => onSellerClick?.(s.seller_name)}
                >
                  <span>{s.seller_name}</span>
                  <span className="font-semibold text-primary">+{formatCLP(s.aporte_necesario)}</span>
                </button>
              ))}
            </div>
          </CardContent>
        </Card>

        {/* Radar */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-base">
              <Radar className="h-4 w-4" />
              Radar Comercial
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
              {RADAR_KEYS.map(({ key, label, color }) => {
                const block = crm.radar?.[key]
                if (!block) return null
                return (
                  <button
                    key={key}
                    type="button"
                    className={cn(
                      "rounded-xl border bg-gradient-to-br p-4 text-left transition-transform hover:scale-[1.02]",
                      RADAR_COLORS[color] ?? "from-muted to-background",
                    )}
                    onClick={() => onRadarClick?.(key)}
                  >
                    <p className="text-xs font-semibold uppercase tracking-wide opacity-80">{label}</p>
                    <p className="mt-1 text-3xl font-bold">{block.cantidad}</p>
                    <p className="text-sm font-medium">{formatCLP(block.monto)}</p>
                    <Badge variant="outline" className="mt-2 text-[10px]">
                      {block.prioridad}
                    </Badge>
                  </button>
                )
              })}
            </div>
          </CardContent>
        </Card>

        {crm.route_targets && crm.route_targets.length > 0 && (
          <CommercialRouteTargets targets={crm.route_targets} onSellerClick={onSellerClick} />
        )}
      </section>

      {/* Ranking + Gamificación + IA */}
      <section className="grid gap-6 xl:grid-cols-3">
        <Card className="xl:col-span-2">
          <CardHeader>
            <CardTitle className="text-base">Ranking Comercial</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {crm.ranking.slice(0, 8).map((s, i) => (
              <button
                key={s.seller_name}
                type="button"
                className="flex w-full gap-4 rounded-lg border p-3 text-left transition-colors hover:bg-muted/50"
                onClick={() => onSellerClick?.(s.seller_name)}
              >
                <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-full bg-primary/10 text-lg font-bold text-primary">
                  {i + 1}
                </div>
                <div className="min-w-0 flex-1">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="font-semibold">{s.seller_name}</span>
                    <span className="flex items-center gap-0.5 text-amber-500">
                      <Star className="h-3.5 w-3.5 fill-current" />
                      {s.score_explanation?.stars ?? (s.commercial_score ?? 0) / 20}
                    </span>
                    <Badge>{s.commercial_score ?? "—"}</Badge>
                    <span className="text-xs text-muted-foreground">{s.score_explanation?.status_label}</span>
                  </div>
                  <div className="mt-1 flex flex-wrap gap-3 text-xs text-muted-foreground">
                    {s.score_explanation?.positives?.map((p) => (
                      <span key={p} className="text-emerald-600">
                        ✔ {p}
                      </span>
                    ))}
                    {s.score_explanation?.negatives?.map((n) => (
                      <span key={n} className="text-red-600">
                        ✖ {n}
                      </span>
                    ))}
                  </div>
                </div>
                <div className="shrink-0 text-right">
                  <p className="font-bold">{formatCLP(s.venta_actual)}</p>
                  <p className={cn("text-xs", s.variacion_pct >= 0 ? "text-emerald-600" : "text-red-600")}>
                    {s.variacion_pct >= 0 ? "+" : ""}
                    {s.variacion_pct}%
                  </p>
                </div>
              </button>
            ))}
          </CardContent>
        </Card>

        <div className="space-y-6">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="flex items-center gap-2 text-base">
                <Award className="h-4 w-4" />
                Gamificación semanal
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-2">
              {crm.gamificacion.badges.map((b) => (
                <div key={b.label} className="rounded-lg bg-muted/50 px-3 py-2 text-sm">
                  <p className="text-xs text-muted-foreground">{b.label}</p>
                  <p className="font-semibold">{b.seller_name}</p>
                  <p className="text-xs text-primary">{b.metric}</p>
                </div>
              ))}
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-base">IA Comercial</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              {crm.ia_comercial.slice(0, 4).map((n, i) => (
                <div key={i} className="rounded-lg border bg-muted/30 p-3 text-sm">
                  {n.seller_name && (
                    <p className="mb-1 text-xs font-semibold text-primary">{n.seller_name}</p>
                  )}
                  {n.parrafos.map((p, j) => (
                    <p key={j} className={cn("text-muted-foreground", j > 0 && "mt-1")}>
                      {p}
                    </p>
                  ))}
                </div>
              ))}
            </CardContent>
          </Card>
        </div>
      </section>

      {/* Timeline + Actividad */}
      <section className="grid gap-6 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Timeline Comercial (12 meses)</CardTitle>
          </CardHeader>
          <CardContent className="h-64">
            {crm.timeline.meses.length > 0 ? (
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={crm.timeline.meses}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                  <XAxis dataKey="mes" tick={{ fontSize: 10 }} tickFormatter={(v) => v.slice(5)} />
                  <YAxis tick={{ fontSize: 10 }} tickFormatter={(v) => `${(v / 1e6).toFixed(0)}M`} />
                  <Tooltip formatter={(v: number) => formatCLP(v)} />
                  <Bar dataKey="venta" fill="#6366f1" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <p className="py-12 text-center text-sm text-muted-foreground">Sin datos de timeline</p>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-base">Actividad reciente</CardTitle>
          </CardHeader>
          <CardContent>
            <ul className="space-y-3">
              {crm.actividad_reciente.map((ev, i) => (
                <li key={i} className="flex gap-3 text-sm">
                  <span className="shrink-0 font-mono text-xs text-muted-foreground">{ev.hora}</span>
                  <button
                    type="button"
                    className={cn("text-left", ev.client_id && "hover:text-primary hover:underline")}
                    onClick={() => ev.client_id && onClientClick?.(ev.client_id)}
                  >
                    {ev.texto}
                  </button>
                </li>
              ))}
            </ul>
          </CardContent>
        </Card>
      </section>

      {/* Watchlist hint */}
      <p className="text-center text-xs text-muted-foreground">
        Watchlist: marca clientes desde la ficha (favorito, crítico, VIP). Aparecen primero en el plan del día.
      </p>
    </div>
  )
}

export { WATCHLIST_KEY, type Watchlist }
