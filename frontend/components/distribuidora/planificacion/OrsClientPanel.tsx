"use client"

import { useMemo, useState } from "react"
import { Clock, MapPin, Route, ScrollText } from "lucide-react"

import type { OrsVisitRow } from "@/lib/ors-map-ui"
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
  distance_km: number
  duration_min: number
}

type OrsClientPanelProps = {
  visits: OrsVisitRow[]
  routeStats: RouteLegStats[]
  kmTotal: number
  durationMin: number
  truckOptions: string[]
  loading?: boolean
  selectedVisitId?: number | null
  onSelectVisit?: (documentId: number) => void
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
  truckOptions,
  loading,
  selectedVisitId,
  onSelectVisit,
}: OrsClientPanelProps) {
  const [truckFilter, setTruckFilter] = useState<string>("__all__")

  const filtered = useMemo(() => {
    if (truckFilter === "__all__") return visits
    return visits.filter((v) => v.camion === truckFilter)
  }, [visits, truckFilter])

  const panelKm = useMemo(() => {
    if (truckFilter === "__all__") return kmTotal
    const r = routeStats.find((x) => x.camion === truckFilter)
    return r?.distance_km ?? 0
  }, [truckFilter, kmTotal, routeStats])

  const panelMin = useMemo(() => {
    if (truckFilter === "__all__") return durationMin
    const r = routeStats.find((x) => x.camion === truckFilter)
    return r?.duration_min ?? 0
  }, [truckFilter, durationMin, routeStats])

  return (
    <div className="flex h-full min-h-0 flex-col bg-muted/15">
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
            </dl>
          </>
        )}
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto p-3">
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
