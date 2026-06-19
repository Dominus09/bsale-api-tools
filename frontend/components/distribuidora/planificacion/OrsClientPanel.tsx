"use client"

import { useMemo, useState } from "react"
import { Clock, MapPin, Route, ScrollText, Users } from "lucide-react"

import type { OrsVisitRow } from "@/lib/ors-map-ui"
import { formatClp } from "@/lib/ors-map-ui"
import { cn } from "@/lib/utils"
import { Skeleton } from "@/components/ui/skeleton"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"

type RouteLegStats = {
  camion: string
  truck_name?: string | null
  distance_km: number
  duration_min: number
  km_per_liter_used?: number
  liters_estimated?: number
  fuel_cost_clp?: number
  crew_cost_clp?: number
  driver_count?: number
  assistant_count?: number
  driver_cost_clp?: number
  assistant_cost_clp?: number
  total_cost_clp?: number
}

const CREW_COUNT_OPTIONS = [0, 1, 2, 3, 4, 5] as const

type OrsClientPanelProps = {
  visits: OrsVisitRow[]
  routeStats: RouteLegStats[]
  kmTotal: number
  durationMin: number
  litersTotal: number
  fuelCostTotal: number
  crewCostTotal?: number
  routeCostTotal?: number
  dieselPricePerLiter?: number
  driverRatePerTrip?: number
  assistantRatePerTrip?: number
  truckOptions: string[]
  loading?: boolean
  selectedVisitId?: number | null
  onSelectVisit?: (documentId: number) => void
  onCrewChange?: (camion: string, driverCount: number, assistantCount: number) => void
  /** Si se define, el panel opera solo para este camión (sin filtro “todos”). */
  activeCamion?: string | null
}

function ClientCard({
  visit,
  active,
  onClick,
}: {
  visit: OrsVisitRow
  active?: boolean
  onClick?: () => void
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={cn(
        "w-full rounded-lg border px-3 py-2.5 text-left transition-all duration-150",
        "hover:border-primary/30 hover:bg-muted/50 hover:shadow-sm",
        "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
        active
          ? "border-primary/50 bg-primary/5 shadow-sm ring-1 ring-primary/20"
          : "border-border/70 bg-card/80",
      )}
    >
      <div className="flex items-start gap-2">
        <span
          className="flex size-7 shrink-0 items-center justify-center rounded-full text-[11px] font-bold text-white shadow-sm"
          style={{ backgroundColor: visit.routeColor ?? "#64748b" }}
        >
          {visit.stop_index}
        </span>
        <div className="min-w-0 flex-1">
          <p className="truncate text-sm font-medium text-foreground">{visit.nombre}</p>
          <p className="mt-0.5 flex items-center gap-1 truncate text-xs text-muted-foreground">
            <MapPin className="size-3 shrink-0" aria-hidden />
            {visit.cityLabel}
          </p>
          <div className="mt-1.5 flex flex-wrap items-center gap-x-2 gap-y-0.5 text-[11px] text-muted-foreground">
            <span className="font-medium text-foreground/80">{visit.ocLabel}</span>
            <span className="inline-flex items-center gap-0.5">
              <Clock className="size-3" aria-hidden />
              {visit.etaLabel}
            </span>
          </div>
        </div>
      </div>
    </button>
  )
}

export function OrsClientPanel({
  visits,
  routeStats,
  kmTotal,
  durationMin,
  litersTotal,
  fuelCostTotal,
  crewCostTotal = 0,
  routeCostTotal,
  dieselPricePerLiter,
  driverRatePerTrip,
  assistantRatePerTrip,
  truckOptions,
  loading,
  selectedVisitId,
  onSelectVisit,
  onCrewChange,
  activeCamion,
}: OrsClientPanelProps) {
  const [truckFilter, setTruckFilter] = useState<string>(activeCamion ?? "__all__")

  const effectiveFilter = activeCamion ?? truckFilter

  const filtered = useMemo(() => {
    if (effectiveFilter === "__all__") return visits
    return visits.filter((v) => v.camion === effectiveFilter)
  }, [visits, effectiveFilter])

  const panelKm = useMemo(() => {
    if (effectiveFilter === "__all__") return kmTotal
    const r = routeStats.find((x) => x.camion === effectiveFilter)
    return r?.distance_km ?? 0
  }, [effectiveFilter, kmTotal, routeStats])

  const panelMin = useMemo(() => {
    if (effectiveFilter === "__all__") return durationMin
    const r = routeStats.find((x) => x.camion === effectiveFilter)
    return r?.duration_min ?? 0
  }, [effectiveFilter, durationMin, routeStats])

  const activeLeg = useMemo(
    () =>
      effectiveFilter === "__all__"
        ? null
        : routeStats.find((x) => x.camion === effectiveFilter) ?? null,
    [effectiveFilter, routeStats],
  )

  const panelKpl = activeLeg?.km_per_liter_used
  const panelLiters =
    effectiveFilter === "__all__" ? litersTotal : (activeLeg?.liters_estimated ?? 0)
  const panelFuel =
    effectiveFilter === "__all__" ? fuelCostTotal : (activeLeg?.fuel_cost_clp ?? 0)
  const panelCrew =
    effectiveFilter === "__all__" ? crewCostTotal : (activeLeg?.crew_cost_clp ?? 0)
  const panelTotal =
    effectiveFilter === "__all__"
      ? (routeCostTotal ?? fuelCostTotal + crewCostTotal)
      : (activeLeg?.total_cost_clp ?? panelFuel + panelCrew)

  const panelDriverCount = activeLeg?.driver_count ?? 1
  const panelAssistantCount = activeLeg?.assistant_count ?? 0
  const panelDriverRate = activeLeg?.driver_cost_clp ?? driverRatePerTrip
  const panelAssistantRate = activeLeg?.assistant_cost_clp ?? assistantRatePerTrip

  return (
    <div className="flex flex-col bg-muted/15">
      <div className="shrink-0 space-y-3 border-b border-border/70 bg-card/80 p-3">
        <div className="flex items-center gap-2">
          <Route className="size-4 text-primary" aria-hidden />
          <h2 className="text-sm font-semibold tracking-tight">Ruta operacional</h2>
        </div>

        {loading ? (
          <div className="space-y-2">
            <Skeleton className="h-9 w-full" />
            <Skeleton className="h-16 w-full" />
          </div>
        ) : (
          <>
            {!activeCamion ? (
              <Select value={truckFilter} onValueChange={setTruckFilter}>
                <SelectTrigger className="h-9 text-xs">
                  <SelectValue placeholder="Camión" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="__all__">Todos los camiones</SelectItem>
                  {truckOptions.map((t) => (
                    <SelectItem key={t} value={t}>
                      {t}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            ) : (
              <p className="rounded-md border border-border/60 bg-background/80 px-2.5 py-2 text-xs font-medium">
                {activeCamion}
              </p>
            )}

            {activeLeg?.truck_name ? (
              <p className="text-[11px] text-muted-foreground">
                Unidad: <span className="font-medium text-foreground">{activeLeg.truck_name}</span>
              </p>
            ) : null}
            <dl className="grid grid-cols-2 gap-2 text-xs">
              <div className="rounded-md border border-border/60 bg-background/80 px-2.5 py-2">
                <dt className="text-muted-foreground">Kilómetros</dt>
                <dd className="font-semibold tabular-nums text-foreground">
                  {panelKm.toFixed(1)} km
                </dd>
              </div>
              <div className="rounded-md border border-border/60 bg-background/80 px-2.5 py-2">
                <dt className="text-muted-foreground">Tiempo est.</dt>
                <dd className="font-semibold tabular-nums text-foreground">
                  {Math.round(panelMin)} min
                </dd>
              </div>
              <div className="rounded-md border border-border/60 bg-background/80 px-2.5 py-2">
                <dt className="text-muted-foreground">Rendimiento</dt>
                <dd className="font-semibold tabular-nums text-foreground">
                  {panelKpl != null ? `${panelKpl.toFixed(1)} km/L` : "—"}
                </dd>
              </div>
              <div className="rounded-md border border-border/60 bg-background/80 px-2.5 py-2">
                <dt className="text-muted-foreground">Litros est.</dt>
                <dd className="font-semibold tabular-nums text-foreground">
                  {panelLiters.toFixed(1)} L
                </dd>
              </div>
              <div className="col-span-2 rounded-md border border-primary/20 bg-primary/5 px-2.5 py-2">
                <dt className="text-muted-foreground">Costo combustible</dt>
                <dd className="text-sm font-semibold tabular-nums text-foreground">
                  {formatClp(panelFuel)}
                </dd>
                {dieselPricePerLiter != null && panelKpl != null ? (
                  <dd className="mt-0.5 text-[10px] text-muted-foreground">
                    {panelKm.toFixed(1)} km ÷ {panelKpl.toFixed(1)} km/L ×{" "}
                    {Math.round(dieselPricePerLiter).toLocaleString("es-CL")} CLP/L
                  </dd>
                ) : null}
              </div>

              {effectiveFilter !== "__all__" && onCrewChange ? (
                <div className="col-span-2 space-y-2 rounded-md border border-border/70 bg-background/90 px-2.5 py-2.5">
                  <div className="flex items-center gap-1.5 text-xs font-medium text-foreground">
                    <Users className="size-3.5 text-primary" aria-hidden />
                    Personal por vuelta
                  </div>
                  <div className="grid grid-cols-2 gap-2">
                    <label className="space-y-1">
                      <span className="text-[10px] text-muted-foreground">Choferes</span>
                      <Select
                        value={String(panelDriverCount)}
                        onValueChange={(v) =>
                          onCrewChange(effectiveFilter, Number(v), panelAssistantCount)
                        }
                      >
                        <SelectTrigger className="h-8 text-xs">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          {CREW_COUNT_OPTIONS.map((n) => (
                            <SelectItem key={`d-${n}`} value={String(n)}>
                              {n}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </label>
                    <label className="space-y-1">
                      <span className="text-[10px] text-muted-foreground">Peonetas</span>
                      <Select
                        value={String(panelAssistantCount)}
                        onValueChange={(v) =>
                          onCrewChange(effectiveFilter, panelDriverCount, Number(v))
                        }
                      >
                        <SelectTrigger className="h-8 text-xs">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          {CREW_COUNT_OPTIONS.map((n) => (
                            <SelectItem key={`a-${n}`} value={String(n)}>
                              {n}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </label>
                  </div>
                  {panelDriverRate != null && panelAssistantRate != null ? (
                    <p className="text-[10px] leading-snug text-muted-foreground">
                      {panelDriverCount}×{formatClp(panelDriverRate)} + {panelAssistantCount}×
                      {formatClp(panelAssistantRate)}
                    </p>
                  ) : null}
                </div>
              ) : null}

              <div className="rounded-md border border-border/60 bg-background/80 px-2.5 py-2">
                <dt className="text-muted-foreground">Subtotal personal</dt>
                <dd className="font-semibold tabular-nums text-foreground">
                  {formatClp(panelCrew)}
                </dd>
              </div>
              <div className="rounded-md border border-emerald-500/25 bg-emerald-500/5 px-2.5 py-2">
                <dt className="text-muted-foreground">Total ruta</dt>
                <dd className="text-sm font-semibold tabular-nums text-foreground">
                  {formatClp(panelTotal)}
                </dd>
                <dd className="mt-0.5 text-[10px] text-muted-foreground">
                  Combustible + personal
                </dd>
              </div>
            </dl>
          </>
        )}
      </div>

      <div className="max-h-56 overflow-y-auto p-3">
        <p className="mb-2 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
          Orden de visitas ({filtered.length})
        </p>
        {loading ? (
          <div className="space-y-2">
            {Array.from({ length: 6 }).map((_, i) => (
              <Skeleton key={i} className="h-[4.5rem] w-full" />
            ))}
          </div>
        ) : filtered.length === 0 ? (
          <p className="py-8 text-center text-xs text-muted-foreground">
            Sin paradas para el filtro seleccionado.
          </p>
        ) : (
          <ul className="space-y-2">
            {filtered.map((v) => (
              <li key={`${v.camion}-${v.document_id}-${v.stop_index}`}>
                <ClientCard
                  visit={v}
                  active={selectedVisitId === v.document_id}
                  onClick={() => onSelectVisit?.(v.document_id)}
                />
              </li>
            ))}
          </ul>
        )}
      </div>

      <div className="shrink-0 border-t border-border/70 bg-card/80 p-3">
        <div className="flex items-center gap-2 text-xs font-medium text-muted-foreground">
          <ScrollText className="size-3.5" aria-hidden />
          Observaciones
        </div>
        <p className="mt-1.5 text-[11px] leading-relaxed text-muted-foreground">
          Las observaciones de entrega se capturan en pre-despacho. Esta vista se centra en
          geometría y orden de ruta ORS.
        </p>
      </div>
    </div>
  )
}
