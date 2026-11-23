"use client"

/**
 * Centro de control de entregas — consulta gerencial (solo lectura).
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import Link from "next/link"
import {
  AlertTriangle,
  CheckCircle2,
  CircleDashed,
  Clock,
  Crosshair,
  Loader2,
  PackageCheck,
  RefreshCw,
  Route,
  Search,
  Split,
  Truck,
  Users,
} from "lucide-react"

import {
  getSeguimientoEntregasDashboard,
  getSeguimientoVisitDetail,
  type DeliveryVisitErpDetail,
  type SeguimientoActivityItem,
  type SeguimientoDashboard,
  type SeguimientoDriverRow,
} from "@/lib/api"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Skeleton } from "@/components/ui/skeleton"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { cn } from "@/lib/utils"
import {
  RESULT_LABEL,
  TONE_BADGE,
  TONE_DOT,
  managementLabel,
  resultTone,
} from "@/components/distribuidora/seguimiento-entregas/status"

const POLL_MS = 45_000

function todaySantiagoInput(): string {
  try {
    return new Intl.DateTimeFormat("en-CA", {
      timeZone: "America/Santiago",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).format(new Date())
  } catch {
    return new Date().toISOString().slice(0, 10)
  }
}

const STATUS_OPTS = [
  { value: "", label: "Todos los estados" },
  { value: "completed", label: "Completas" },
  { value: "partial", label: "Parciales" },
  { value: "not_delivered", label: "No entregadas" },
  { value: "pending", label: "Pendientes" },
]

function KpiCard({
  label,
  value,
  icon: Icon,
  tone = "neutral",
  hint,
}: {
  label: string
  value: string | number | null | undefined
  icon: React.ElementType
  tone?: "neutral" | "ok" | "warn" | "danger" | "brand" | "info"
  hint?: string
}) {
  const toneCls = {
    neutral: "text-lq-on-graphite-muted",
    ok: "text-emerald-400",
    warn: "text-amber-300",
    danger: "text-red-300",
    brand: "text-[var(--lq-brand)]",
    info: "text-sky-300",
  }[tone]
  return (
    <div className="rounded-xl border border-white/10 bg-lq-graphite px-4 py-4 shadow-sm ring-1 ring-black/20 transition-colors hover:bg-lq-graphite-elev">
      <div className="flex items-start justify-between gap-2">
        <p className="text-xs font-semibold uppercase tracking-[0.14em] text-lq-on-graphite-muted">
          {label}
        </p>
        <Icon className={cn("h-4 w-4 shrink-0", toneCls)} />
      </div>
      <div className="mt-2 text-[28px] font-semibold leading-none tracking-[-0.03em] text-lq-on-graphite tabular-nums">
        {value ?? "—"}
      </div>
      {hint ? (
        <p className="mt-2 text-sm text-lq-on-graphite-muted">{hint}</p>
      ) : null}
    </div>
  )
}

function DispatchCard({ row, date }: { row: SeguimientoDriverRow; date: string }) {
  const pct = row.progress_percent ?? 0
  const managed =
    (row.delivered ?? 0) + (row.partial ?? 0) + (row.not_delivered ?? 0)
  const status = managementLabel(row)
  return (
    <article className="rounded-2xl border border-white/10 bg-lq-graphite p-5 shadow-sm ring-1 ring-black/25 transition-colors hover:border-white/20 hover:bg-lq-graphite-elev">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0 space-y-1">
          <div className="flex flex-wrap items-center gap-2">
            <h3 className="truncate text-lg font-semibold text-lq-on-graphite">
              {row.driver_name || "Chofer sin asignar"}
            </h3>
            <span className="rounded-full border border-white/15 bg-white/5 px-2.5 py-0.5 text-xs font-medium text-lq-on-graphite-muted">
              {status}
            </span>
          </div>
          <p className="text-sm text-lq-on-graphite-muted">
            {[row.vehicle_label || row.truck_plate, row.planning_code, row.route_name]
              .filter(Boolean)
              .join(" · ")}
          </p>
          <p className="text-sm text-lq-on-graphite-muted">Fecha operativa {date}</p>
        </div>
        <Link
          href={`/distribuidora/seguimiento-entregas/planes/${row.dispatch_plan_id}`}
          className="inline-flex h-10 items-center rounded-lg bg-[var(--lq-brand)] px-4 text-sm font-semibold text-white transition-colors hover:bg-[var(--lq-brand-strong)]"
        >
          Ver detalle
        </Link>
      </div>

      <div className="mt-4 space-y-2">
        <div className="flex items-end justify-between gap-3">
          <p className="text-sm text-lq-on-graphite-muted">
            <span className="font-semibold text-lq-on-graphite tabular-nums">
              {managed}/{row.clients_total ?? 0}
            </span>{" "}
            clientes gestionados
          </p>
          <p className="text-2xl font-semibold tabular-nums text-lq-on-graphite">
            {pct}%
          </p>
        </div>
        <div className="h-2.5 overflow-hidden rounded-full bg-black/40">
          <div
            className="h-full rounded-full bg-[var(--lq-brand)] transition-all duration-500"
            style={{ width: `${Math.min(100, Math.max(0, pct))}%` }}
          />
        </div>
      </div>

      <div className="mt-4 grid grid-cols-2 gap-2 sm:grid-cols-4">
        {[
          { label: "Completas", value: row.delivered ?? 0, tone: "ok" as const },
          { label: "Parciales", value: row.partial ?? 0, tone: "warn" as const },
          {
            label: "No entregados",
            value: row.not_delivered ?? 0,
            tone: "danger" as const,
          },
          { label: "Pendientes", value: row.pending ?? 0, tone: "neutral" as const },
        ].map((m) => (
          <div
            key={m.label}
            className="rounded-lg border border-white/8 bg-black/20 px-3 py-2"
          >
            <p className="text-[11px] font-semibold uppercase tracking-wider text-lq-on-graphite-muted">
              {m.label}
            </p>
            <p
              className={cn(
                "mt-1 text-xl font-semibold tabular-nums",
                m.tone === "ok" && "text-emerald-300",
                m.tone === "warn" && "text-amber-300",
                m.tone === "danger" && "text-red-300",
                m.tone === "neutral" && "text-lq-on-graphite",
              )}
            >
              {m.value}
            </p>
          </div>
        ))}
      </div>

      <div className="mt-4 flex flex-wrap gap-x-5 gap-y-1 border-t border-white/10 pt-3 text-sm text-lq-on-graphite-muted">
        <span>
          1ª entrega{" "}
          <strong className="tabular-nums text-lq-on-graphite">
            {row.first_delivery_at_local || "—"}
          </strong>
        </span>
        <span>
          Últ. entrega{" "}
          <strong className="tabular-nums text-lq-on-graphite">
            {row.last_delivery_at_local || "—"}
          </strong>
        </span>
        <span>
          Últ. gestión{" "}
          <strong className="tabular-nums text-lq-on-graphite">
            {row.last_management_at_local || "—"}
          </strong>
        </span>
        {(row.pending_gps_review ?? 0) > 0 ? (
          <span className="text-sky-300">
            GPS por revisar: {row.pending_gps_review}
          </span>
        ) : null}
      </div>

      {(row.clients_total ?? 0) > 0 && managed === 0 ? (
        <p className="mt-3 rounded-lg border border-dashed border-white/15 bg-black/15 px-3 py-2 text-sm text-lq-on-graphite-muted">
          Este despacho aún no registra entregas. Las gestiones aparecerán cuando el
          chofer las confirme.
        </p>
      ) : null}
    </article>
  )
}

function ActivityRow({ item }: { item: SeguimientoActivityItem }) {
  const tone = resultTone(item.result_status, item.requires_review)
  return (
    <li className="flex gap-3 border-b border-white/8 py-3 last:border-0">
      <div className="mt-1.5 flex flex-col items-center">
        <span className={cn("h-2.5 w-2.5 rounded-full", TONE_DOT[tone])} />
        <span className="mt-1 w-px flex-1 bg-white/10" />
      </div>
      <div className="min-w-0 flex-1">
        <div className="flex flex-wrap items-baseline justify-between gap-2">
          <p className="truncate font-medium text-lq-on-graphite">
            {item.commercial_name}
          </p>
          <time className="shrink-0 tabular-nums text-xs text-lq-on-graphite-faint">
            {item.managed_at_local || "—"}
          </time>
        </div>
        <p className="mt-0.5 text-xs text-lq-on-graphite-muted">
          {item.driver_name} · {item.planning_code}
        </p>
        <div className="mt-1.5 flex flex-wrap items-center gap-2">
          <span
            className={cn(
              "inline-flex rounded-full border px-2 py-0.5 text-[11px] font-medium",
              TONE_BADGE[tone],
            )}
          >
            {RESULT_LABEL[item.result_status || ""] || item.result_status}
          </span>
          {item.result_reason_es ? (
            <span className="text-[11px] text-lq-on-graphite-faint">
              {item.result_reason_es}
            </span>
          ) : null}
        </div>
      </div>
    </li>
  )
}

export default function SeguimientoEntregasPage() {
  const [date, setDate] = useState(todaySantiagoInput)
  const [status, setStatus] = useState("")
  const [clientQ, setClientQ] = useState("")
  const [clientInput, setClientInput] = useState("")
  const [driverFilter, setDriverFilter] = useState("")
  const [planFilter, setPlanFilter] = useState("")
  const [data, setData] = useState<SeguimientoDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const scrollY = useRef(0)

  const [photoOpen, setPhotoOpen] = useState(false)
  const [photoLoading, setPhotoLoading] = useState(false)
  const [photoError, setPhotoError] = useState<string | null>(null)
  const [photoDetail, setPhotoDetail] = useState<DeliveryVisitErpDetail | null>(null)
  const [photoMeta, setPhotoMeta] = useState<SeguimientoActivityItem | null>(null)

  const load = useCallback(
    async (opts?: { silent?: boolean }) => {
      const silent = !!opts?.silent
      if (silent) {
        scrollY.current = typeof window !== "undefined" ? window.scrollY : 0
        setRefreshing(true)
      } else {
        setLoading(true)
      }
      setError(null)
      try {
        const res = await getSeguimientoEntregasDashboard({
          date,
          status: status || undefined,
          client_q: clientQ || undefined,
          driver_user_id: driverFilter ? Number(driverFilter) : undefined,
          plan_id: planFilter ? Number(planFilter) : undefined,
        })
        setData(res)
      } catch (e) {
        setError(e instanceof Error ? e.message : "Error al cargar")
      } finally {
        setLoading(false)
        setRefreshing(false)
        if (silent && typeof window !== "undefined") {
          requestAnimationFrame(() => window.scrollTo(0, scrollY.current))
        }
      }
    },
    [date, status, clientQ, driverFilter, planFilter],
  )

  useEffect(() => {
    void load()
  }, [load])

  useEffect(() => {
    const id = window.setInterval(() => {
      void load({ silent: true })
    }, POLL_MS)
    return () => window.clearInterval(id)
  }, [load])

  const drivers = useMemo(() => {
    const map = new Map<string, string>()
    for (const row of data?.by_driver || []) {
      if (row.driver_user_id != null) {
        map.set(String(row.driver_user_id), row.driver_name || `Chofer ${row.driver_user_id}`)
      }
    }
    return Array.from(map.entries())
  }, [data])

  const plans = useMemo(
    () =>
      (data?.by_driver || []).map((r) => ({
        id: String(r.dispatch_plan_id),
        label: r.planning_code || `PLAN ${r.dispatch_plan_id}`,
      })),
    [data],
  )

  const k = data?.kpis
  const notDelivered =
    k?.not_delivered_total ?? (k?.closed_store ?? 0) + (k?.other_not_delivered ?? 0)

  async function openIncidentPhoto(item: SeguimientoActivityItem) {
    if (!item.visit_id) return
    setPhotoMeta(item)
    setPhotoOpen(true)
    setPhotoDetail(null)
    setPhotoError(null)
    setPhotoLoading(true)
    try {
      const detail = await getSeguimientoVisitDetail(item.visit_id)
      setPhotoDetail(detail)
    } catch (e) {
      setPhotoError(e instanceof Error ? e.message : "No se pudo cargar la evidencia")
    } finally {
      setPhotoLoading(false)
    }
  }

  return (
    <div className="-mx-4 min-h-[calc(100vh-4rem)] bg-lq-graphite-deep px-4 py-5 text-lq-on-graphite sm:-mx-6 sm:px-6 lg:px-8">
      <div className="mx-auto max-w-[1600px] space-y-6">
        {/* Header */}
        <header className="flex flex-wrap items-start justify-between gap-4 border-b border-white/10 pb-5">
          <div className="min-w-0">
            <p className="text-xs font-semibold uppercase tracking-[0.16em] text-[var(--lq-brand)]">
              Distribuidora · Logística
            </p>
            <h1 className="mt-1 text-3xl font-semibold tracking-[-0.03em] text-lq-on-graphite sm:text-4xl">
              Centro de control de entregas
            </h1>
            <p className="mt-2 max-w-2xl text-base text-lq-on-graphite-muted">
              Monitorea despachos, entregas e incidencias de tus choferes.
            </p>
          </div>
          <div className="flex flex-col items-end gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => void load()}
              disabled={loading || refreshing}
              className="border-white/20 bg-white/5 text-lq-on-graphite hover:bg-white/10 hover:text-white"
            >
              {loading || refreshing ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <RefreshCw className="h-4 w-4" />
              )}
              <span className="ml-2">Actualizar</span>
            </Button>
            <p className="text-right text-sm text-lq-on-graphite-muted">
              Última actualización{" "}
              <span className="tabular-nums text-lq-on-graphite">
                {data?.panel_updated_at_local || "—"}
              </span>
              <span className="mx-1.5 text-white/20">·</span>
              Auto cada 45 s
            </p>
          </div>
        </header>

        {/* Filters */}
        <div className="grid gap-3 rounded-2xl border border-white/10 bg-lq-graphite p-4 sm:grid-cols-2 lg:grid-cols-6">
          <label className="space-y-1.5 text-sm">
            <span className="text-lq-on-graphite-faint">Fecha</span>
            <Input
              type="date"
              value={date}
              onChange={(e) => setDate(e.target.value)}
              className="border-white/15 bg-black/30 text-lq-on-graphite"
            />
          </label>
          <label className="space-y-1.5 text-sm">
            <span className="text-lq-on-graphite-faint">Chofer</span>
            <select
              className="flex h-10 w-full rounded-md border border-white/15 bg-black/30 px-3 text-sm text-lq-on-graphite"
              value={driverFilter}
              onChange={(e) => setDriverFilter(e.target.value)}
            >
              <option value="">Todos</option>
              {drivers.map(([id, name]) => (
                <option key={id} value={id}>
                  {name}
                </option>
              ))}
            </select>
          </label>
          <label className="space-y-1.5 text-sm">
            <span className="text-lq-on-graphite-faint">Despacho</span>
            <select
              className="flex h-10 w-full rounded-md border border-white/15 bg-black/30 px-3 text-sm text-lq-on-graphite"
              value={planFilter}
              onChange={(e) => setPlanFilter(e.target.value)}
            >
              <option value="">Todos</option>
              {plans.map((p) => (
                <option key={p.id} value={p.id}>
                  {p.label}
                </option>
              ))}
            </select>
          </label>
          <label className="space-y-1.5 text-sm">
            <span className="text-lq-on-graphite-faint">Estado</span>
            <select
              className="flex h-10 w-full rounded-md border border-white/15 bg-black/30 px-3 text-sm text-lq-on-graphite"
              value={status}
              onChange={(e) => setStatus(e.target.value)}
            >
              {STATUS_OPTS.map((o) => (
                <option key={o.value || "all"} value={o.value}>
                  {o.label}
                </option>
              ))}
            </select>
          </label>
          <label className="space-y-1.5 text-sm sm:col-span-2">
            <span className="text-lq-on-graphite-faint">Cliente</span>
            <div className="flex gap-2">
              <div className="relative flex-1">
                <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-lq-on-graphite-faint" />
                <Input
                  value={clientInput}
                  onChange={(e) => setClientInput(e.target.value)}
                  placeholder="Nombre comercial"
                  className="border-white/15 bg-black/30 pl-9 text-lq-on-graphite"
                  onKeyDown={(e) => {
                    if (e.key === "Enter") setClientQ(clientInput.trim())
                  }}
                />
              </div>
              <Button
                type="button"
                className="bg-[var(--lq-brand)] text-white hover:bg-[var(--lq-brand-strong)]"
                onClick={() => setClientQ(clientInput.trim())}
              >
                Filtrar
              </Button>
            </div>
          </label>
        </div>

        {error ? (
          <div className="rounded-xl border border-red-500/40 bg-red-500/10 px-4 py-3 text-sm text-red-200">
            <p className="font-medium">No se pudo cargar el panel</p>
            <p className="mt-1 whitespace-pre-wrap text-xs opacity-90">{error}</p>
            <Button
              size="sm"
              variant="outline"
              className="mt-3 border-red-400/40 text-red-100"
              onClick={() => void load()}
            >
              Reintentar
            </Button>
          </div>
        ) : null}

        {/* KPIs */}
        {loading && !data ? (
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4 xl:grid-cols-8">
            {Array.from({ length: 8 }).map((_, i) => (
              <Skeleton key={i} className="h-[104px] rounded-xl bg-white/5" />
            ))}
          </div>
        ) : (
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4 xl:grid-cols-8">
            <KpiCard label="Programados" value={k?.planned_clients ?? 0} icon={Users} />
            <KpiCard
              label="Gestionados"
              value={k?.managed_clients ?? 0}
              icon={PackageCheck}
              tone="info"
            />
            <KpiCard
              label="Completas"
              value={k?.completed ?? 0}
              icon={CheckCircle2}
              tone="ok"
            />
            <KpiCard
              label="Parciales"
              value={k?.partial ?? 0}
              icon={Split}
              tone="warn"
            />
            <KpiCard
              label="No entregados"
              value={notDelivered}
              icon={AlertTriangle}
              tone="danger"
              hint={
                (k?.closed_store ?? 0) > 0
                  ? `${k?.closed_store} local cerrado`
                  : undefined
              }
            />
            <KpiCard
              label="Pendientes"
              value={k?.pending_clients ?? 0}
              icon={CircleDashed}
            />
            <KpiCard
              label="Avance"
              value={`${k?.progress_percent ?? 0}%`}
              icon={Route}
              tone="brand"
            />
            <KpiCard
              label="GPS revisar"
              value={k?.pending_gps_review ?? 0}
              icon={Crosshair}
              tone={(k?.pending_gps_review ?? 0) > 0 ? "info" : "neutral"}
            />
          </div>
        )}

        {/* Main + side */}
        <div className="grid gap-6 xl:grid-cols-[minmax(0,1.6fr)_minmax(320px,0.9fr)]">
          <section className="space-y-4">
            <div className="flex items-end justify-between gap-3">
              <div>
                <h2 className="text-lg font-semibold text-lq-on-graphite">
                  Despachos activos
                </h2>
                <p className="text-sm text-lq-on-graphite-muted">
                  {(data?.by_driver || []).length} despacho
                  {(data?.by_driver || []).length === 1 ? "" : "s"} · {date}
                </p>
              </div>
            </div>

            {loading && !data ? (
              <div className="space-y-3">
                <Skeleton className="h-48 rounded-2xl bg-white/5" />
                <Skeleton className="h-48 rounded-2xl bg-white/5" />
              </div>
            ) : (data?.by_driver || []).length === 0 ? (
              <div className="rounded-2xl border border-dashed border-white/15 bg-lq-graphite px-6 py-12 text-center">
                <Truck className="mx-auto h-10 w-10 text-lq-on-graphite-faint" />
                <p className="mt-3 text-base font-medium text-lq-on-graphite">
                  No hay despachos para esta fecha
                </p>
                <p className="mt-1 text-sm text-lq-on-graphite-muted">
                  Las gestiones aparecerán aquí cuando el chofer las confirme.
                </p>
              </div>
            ) : (
              <div className="space-y-4">
                {(data?.by_driver || []).map((row) => (
                  <DispatchCard key={row.dispatch_plan_id} row={row} date={date} />
                ))}
              </div>
            )}
          </section>

          <aside className="space-y-4">
            <div className="rounded-2xl border border-white/10 bg-lq-graphite p-4 ring-1 ring-black/20">
              <div className="mb-2 flex items-center gap-2">
                <Clock className="h-4 w-4 text-[var(--lq-brand)]" />
                <h2 className="text-base font-semibold text-lq-on-graphite">
                  Actividad reciente
                </h2>
              </div>
              {(data?.recent_activity || []).length === 0 ? (
                <p className="py-8 text-center text-sm text-lq-on-graphite-muted">
                  Aún no hay gestiones confirmadas hoy.
                </p>
              ) : (
                <ul className="max-h-[420px] overflow-y-auto pr-1">
                  {(data?.recent_activity || []).map((item, idx) => (
                    <ActivityRow
                      key={`${item.visit_id ?? "x"}-${item.customer_key}-${idx}`}
                      item={item}
                    />
                  ))}
                </ul>
              )}
            </div>

            <div className="rounded-2xl border border-[var(--lq-brand)]/30 bg-lq-graphite p-4 ring-1 ring-black/20">
              <div className="mb-3 flex items-center gap-2">
                <AlertTriangle className="h-4 w-4 text-[var(--lq-brand)]" />
                <h2 className="text-base font-semibold text-lq-on-graphite">
                  Incidencias recientes
                </h2>
              </div>
              {(data?.recent_incidents || []).length === 0 ? (
                <p className="py-8 text-center text-sm text-lq-on-graphite-muted">
                  Sin incidencias confirmadas para los filtros actuales.
                </p>
              ) : (
                <ul className="space-y-3">
                  {(data?.recent_incidents || []).map((item, idx) => (
                    <li
                      key={`inc-${item.visit_id ?? idx}`}
                      className="overflow-hidden rounded-xl border border-white/10 bg-black/25"
                    >
                      <div className="flex gap-3 p-3">
                        {item.evidence_thumbnail?.url ? (
                          <button
                            type="button"
                            className="relative h-16 w-16 shrink-0 overflow-hidden rounded-lg bg-black/40"
                            onClick={() => void openIncidentPhoto(item)}
                            aria-label="Ver evidencia"
                          >
                            {/* eslint-disable-next-line @next/next/no-img-element */}
                            <img
                              src={item.evidence_thumbnail.url}
                              alt=""
                              className="h-full w-full object-cover"
                            />
                          </button>
                        ) : (
                          <div className="flex h-16 w-16 shrink-0 items-center justify-center rounded-lg border border-dashed border-white/15 bg-black/30 text-[10px] text-lq-on-graphite-faint">
                            Sin foto
                          </div>
                        )}
                        <div className="min-w-0 flex-1">
                          <p className="truncate font-medium text-lq-on-graphite">
                            {item.commercial_name}
                          </p>
                          <p className="text-sm text-lq-on-graphite-muted">
                            {item.driver_name} · {item.planning_code} ·{" "}
                            {item.managed_at_local || "—"}
                          </p>
                          <p className="mt-1 text-sm text-red-200">
                            {item.result_reason_es || "Incidencia"}
                          </p>
                          {item.observation ? (
                            <p className="mt-0.5 line-clamp-2 text-sm text-lq-on-graphite-muted">
                              {item.observation}
                            </p>
                          ) : null}
                          <div className="mt-1.5 flex flex-wrap gap-2 text-xs">
                            {item.requires_review ? (
                              <span className="text-sky-300">GPS pendiente de revisión</span>
                            ) : (
                              <span className="text-lq-on-graphite-muted">
                                GPS {item.closure_review_status || "—"}
                              </span>
                            )}
                            {item.visit_id ? (
                              <button
                                type="button"
                                className="text-[var(--lq-brand)] underline-offset-2 hover:underline"
                                onClick={() => void openIncidentPhoto(item)}
                              >
                                Ver evidencia
                              </button>
                            ) : null}
                          </div>
                        </div>
                      </div>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </aside>
        </div>
      </div>

      <Dialog open={photoOpen} onOpenChange={setPhotoOpen}>
        <DialogContent className="max-h-[90vh] max-w-3xl overflow-y-auto border-white/10 bg-lq-graphite text-lq-on-graphite">
          <DialogHeader>
            <DialogTitle>Evidencia de incidencia</DialogTitle>
            <DialogDescription className="text-lq-on-graphite-muted">
              {photoMeta?.commercial_name} · {photoMeta?.driver_name} · URL firmada
              temporal
            </DialogDescription>
          </DialogHeader>
          {photoLoading ? (
            <div className="flex items-center gap-2 text-sm text-lq-on-graphite-muted">
              <Loader2 className="h-4 w-4 animate-spin" /> Cargando…
            </div>
          ) : null}
          {photoError ? (
            <p className="text-sm text-red-300 whitespace-pre-wrap">{photoError}</p>
          ) : null}
          {photoDetail ? (
            <div className="space-y-3">
              {(photoDetail.incident_evidence || []).length === 0 ? (
                <p className="text-sm text-lq-on-graphite-muted">
                  Sin evidencia fotográfica
                </p>
              ) : (
                (photoDetail.incident_evidence || []).map((item, idx) =>
                  item.url ? (
                    // eslint-disable-next-line @next/next/no-img-element
                    <img
                      key={item.id ?? idx}
                      src={item.url}
                      alt={item.label_es || `Evidencia ${idx + 1}`}
                      className="max-h-[70vh] w-full rounded-lg object-contain bg-black/40"
                    />
                  ) : (
                    <p key={item.id ?? idx} className="text-sm text-lq-on-graphite-muted">
                      Sin evidencia fotográfica usable
                    </p>
                  ),
                )
              )}
            </div>
          ) : null}
        </DialogContent>
      </Dialog>
    </div>
  )
}
