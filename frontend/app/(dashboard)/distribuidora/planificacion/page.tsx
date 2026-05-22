"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import dynamic from "next/dynamic"
import Link from "next/link"
import { Loader2, RefreshCw, Trash2 } from "lucide-react"

import {
  postDistribuidoraPlanificacionOrsRoutes,
  type DistribuidoraPlanificacionOrsRoute,
} from "@/lib/api"
import {
  buildOrsVisitRows,
  estimateFuelCostClp,
} from "@/lib/ors-map-ui"
import {
  readPlanificacionPayload,
  clearPlanificacionPayload,
  type PlanificacionStoredOrder,
} from "@/lib/planificacion-despacho-storage"
import type { PlanificacionMapRoute } from "@/components/distribuidora/planificacion-despacho-map-client"
import { OrsClientPanel } from "@/components/distribuidora/planificacion/OrsClientPanel"
import { OrsDispatchEmptyState } from "@/components/distribuidora/planificacion/OrsDispatchEmptyState"
import { OrsMapSkeleton } from "@/components/distribuidora/planificacion/OrsMapSkeleton"
import { OrsTopBar } from "@/components/distribuidora/planificacion/OrsTopBar"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"

const PlanificacionMap = dynamic(
  () =>
    import("@/components/distribuidora/planificacion-despacho-map-client").then((m) => ({
      default: m.PlanificacionDespachoMapClient,
    })),
  { ssr: false, loading: () => <OrsMapSkeleton /> },
)

const TRUCK_COLORS = ["#2563eb", "#16a34a", "#ca8a04", "#9333ea", "#db2777", "#0891b2"]

function lineStringToLatLngs(
  g: DistribuidoraPlanificacionOrsRoute["geometry"],
): [number, number][] {
  if (!g?.coordinates?.length) return []
  return g.coordinates.map(([lon, lat]) => [lat, lon] as [number, number])
}

function groupOrdersByTruck(orders: PlanificacionStoredOrder[]) {
  const sorted = [...orders].sort((a, b) => {
    const c = a.camion.localeCompare(b.camion, "es")
    if (c !== 0) return c
    return a.stop_index - b.stop_index
  })
  const map = new Map<string, PlanificacionStoredOrder[]>()
  for (const o of sorted) {
    const arr = map.get(o.camion)
    if (arr) arr.push(o)
    else map.set(o.camion, [o])
  }
  return map
}

export default function PlanificacionDespachoPage() {
  const [orders, setOrders] = useState<PlanificacionStoredOrder[]>([])
  const [orsRoutes, setOrsRoutes] = useState<DistribuidoraPlanificacionOrsRoute[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [selectedVisitId, setSelectedVisitId] = useState<number | null>(null)

  const reloadFromStorage = useCallback(() => {
    const p = readPlanificacionPayload()
    setOrders(p?.orders ?? [])
  }, [])

  useEffect(() => {
    reloadFromStorage()
  }, [reloadFromStorage])

  const fetchRoutes = useCallback(async (list: PlanificacionStoredOrder[]) => {
    if (list.length === 0) {
      setOrsRoutes([])
      setLoading(false)
      return
    }
    setLoading(true)
    setError(null)
    try {
      const byT = groupOrdersByTruck(list)
      const truckEntries = Array.from(byT.entries()) as [
        string,
        PlanificacionStoredOrder[],
      ][]
      const routesPayload = truckEntries.map(([camion, stops]) => ({
        camion,
        coordinates: stops.map((o: PlanificacionStoredOrder) => [o.lng, o.lat] as number[]),
      }))
      const res = await postDistribuidoraPlanificacionOrsRoutes({ routes: routesPayload })
      setOrsRoutes(res.routes)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error ORS")
      setOrsRoutes([])
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void fetchRoutes(orders)
  }, [orders, fetchRoutes])

  const truckColorMap = useMemo(() => {
    const keys = Array.from(groupOrdersByTruck(orders).keys())
    return new Map(keys.map((k, i) => [k, TRUCK_COLORS[i % TRUCK_COLORS.length]!]))
  }, [orders])

  const mapRoutes: PlanificacionMapRoute[] = useMemo(() => {
    const byT = groupOrdersByTruck(orders)
    return orsRoutes.map((r, i) => {
      const color = TRUCK_COLORS[i % TRUCK_COLORS.length]!
      const stops = byT.get(r.camion) ?? []
      return {
        camion: r.camion,
        color,
        positions: lineStringToLatLngs(r.geometry),
        stops: stops.map((s: PlanificacionStoredOrder) => ({
          lat: s.lat,
          lng: s.lng,
          num: s.stop_index,
          label: `${s.nombre_fantasia?.trim() || "Cliente"} · OC ${s.oc ?? s.document_id}`,
        })),
      }
    })
  }, [orders, orsRoutes])

  const totals = useMemo(() => {
    let km = 0
    let min = 0
    for (const r of orsRoutes) {
      km += Number(r.distance_km) || 0
      min += Number(r.duration_min) || 0
    }
    const clients = new Set<number>()
    for (const o of orders) {
      if (o.client_id != null && Number.isFinite(Number(o.client_id))) {
        clients.add(Number(o.client_id))
      }
    }
    return {
      km,
      min,
      clientCount: clients.size,
      fuelClp: estimateFuelCostClp(km),
    }
  }, [orders, orsRoutes])

  const visits = useMemo(
    () => buildOrsVisitRows(orders, orsRoutes, truckColorMap),
    [orders, orsRoutes, truckColorMap],
  )

  const truckOptions = useMemo(
    () => Array.from(groupOrdersByTruck(orders).keys()),
    [orders],
  )

  const highlightedStopKey = useMemo(() => {
    if (selectedVisitId == null) return null
    const v = visits.find((x) => x.document_id === selectedVisitId)
    if (!v) return null
    return `${v.camion}-${v.stop_index}`
  }, [selectedVisitId, visits])

  const routeStats = useMemo(
    () =>
      orsRoutes.map((r) => ({
        camion: r.camion,
        distance_km: r.distance_km,
        duration_min: r.duration_min,
      })),
    [orsRoutes],
  )

  if (!loading && orders.length === 0) {
    return <OrsDispatchEmptyState />
  }

  return (
    <div className="-m-6 flex h-[calc(100dvh-4rem)] min-h-[640px] flex-col overflow-hidden bg-background">
      <header className="shrink-0 border-b border-border/80 bg-card/95 px-4 py-3 backdrop-blur-sm md:px-5">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              Dispatch center · ORS
            </p>
            <h1 className="text-lg font-semibold tracking-tight md:text-xl">
              Planif. mapa ORS
            </h1>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <Button
              type="button"
              variant="outline"
              size="sm"
              className="h-8 gap-1.5 text-xs"
              disabled={loading}
              onClick={() => void fetchRoutes(orders)}
            >
              {loading ? (
                <Loader2 className="size-3.5 animate-spin" aria-hidden />
              ) : (
                <RefreshCw className="size-3.5" aria-hidden />
              )}
              Recalcular rutas
            </Button>
            <Button asChild variant="outline" size="sm" className="h-8 text-xs">
              <Link href="/distribuidora/orders">Pre-despacho</Link>
            </Button>
            <Button
              type="button"
              variant="ghost"
              size="sm"
              className="h-8 gap-1 text-xs text-muted-foreground"
              onClick={() => {
                clearPlanificacionPayload()
                setOrders([])
                setOrsRoutes([])
                setSelectedVisitId(null)
              }}
            >
              <Trash2 className="size-3.5" aria-hidden />
              Limpiar cola
            </Button>
          </div>
        </div>
        <div className="mt-3">
          <OrsTopBar
            kmTotal={totals.km}
            clientCount={totals.clientCount}
            durationMin={totals.min}
            fuelCostClp={totals.fuelClp}
            loading={loading}
          />
        </div>
      </header>

      {error ? (
        <Alert variant="destructive" className="mx-4 mt-2 shrink-0 md:mx-5">
          <AlertTitle>Error ORS</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      <div className="flex min-h-0 flex-1">
        <aside className="flex w-[min(100%,20rem)] shrink-0 flex-col border-r border-border/80 md:w-80 lg:w-[22rem]">
          <OrsClientPanel
            visits={visits}
            routeStats={routeStats}
            kmTotal={totals.km}
            durationMin={totals.min}
            truckOptions={truckOptions}
            loading={loading}
            selectedVisitId={selectedVisitId}
            onSelectVisit={setSelectedVisitId}
          />
        </aside>

        <main className="relative min-w-0 flex-1 p-2 md:p-3">
          {loading ? (
            <OrsMapSkeleton />
          ) : (
            <PlanificacionMap
              routes={mapRoutes}
              highlightedStopKey={highlightedStopKey}
              className="h-full min-h-0 w-full overflow-hidden rounded-lg border border-border/80 bg-slate-950/5 shadow-md dark:bg-slate-950/50"
            />
          )}
        </main>
      </div>
    </div>
  )
}
