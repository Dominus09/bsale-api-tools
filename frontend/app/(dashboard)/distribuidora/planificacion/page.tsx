"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import dynamic from "next/dynamic"
import Link from "next/link"
import { Loader2, RefreshCw, Trash2 } from "lucide-react"

import {
  confirmDispatchPlan,
  getDispatchPlansBySession,
  getDistribuidoraPlanificacionRouteCrew,
  postDistribuidoraPlanificacionOrsRoutes,
  type DispatchPlanSummary,
  type DistribuidoraPlanificacionCrewDefaults,
  type DistribuidoraPlanificacionOrsResponse,
  type DistribuidoraPlanificacionOrsRoute,
} from "@/lib/api"
import { buildOrsVisitRows } from "@/lib/ors-map-ui"
import {
  readPlanificacionPayload,
  clearPlanificacionPayload,
  type PlanificacionStoredOrder,
} from "@/lib/planificacion-despacho-storage"
import type { PlanificacionMapRoute } from "@/components/distribuidora/planificacion-despacho-map-client"
import { OrsClientPanel } from "@/components/distribuidora/planificacion/OrsClientPanel"
import { OrsDispatchEmptyState } from "@/components/distribuidora/planificacion/OrsDispatchEmptyState"
import { OrsFuelConfigBar } from "@/components/distribuidora/planificacion/OrsFuelConfigBar"
import { OrsMapSkeleton } from "@/components/distribuidora/planificacion/OrsMapSkeleton"
import { OrsTopBar } from "@/components/distribuidora/planificacion/OrsTopBar"
import { OrsTruckSidebar } from "@/components/distribuidora/planificacion/OrsTruckSidebar"
import { OrsDispatchWorkflow } from "@/components/distribuidora/planificacion/OrsDispatchWorkflow"
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

type CrewCounts = { driverCount: number; assistantCount: number }

function crewMapFromOrders(
  orders: PlanificacionStoredOrder[],
  saved?: Map<string, CrewCounts>,
): Map<string, CrewCounts> {
  const map = new Map<string, CrewCounts>()
  for (const o of orders) {
    if (!map.has(o.camion)) {
      map.set(o.camion, saved?.get(o.camion) ?? { driverCount: 1, assistantCount: 0 })
    }
  }
  return map
}

function lineStringToLatLngs(
  g: DistribuidoraPlanificacionOrsRoute["geometry"],
): [number, number][] {
  if (!g?.coordinates?.length) return []
  return g.coordinates.map(([lon, lat]) => [lat, lon] as [number, number])
}

function groupOrdersByTruck(orders: PlanificacionStoredOrder[]) {
  const map = new Map<string, PlanificacionStoredOrder[]>()
  for (const o of orders) {
    const arr = map.get(o.camion)
    if (arr) arr.push(o)
    else map.set(o.camion, [o])
  }
  return map
}

function applyOptimizedStopOrder(
  orders: PlanificacionStoredOrder[],
  routes: DistribuidoraPlanificacionOrsRoute[],
): PlanificacionStoredOrder[] {
  const indexByCamionDoc = new Map<string, Map<number, number>>()
  for (const r of routes) {
    if (!r.stops_ordered?.length) continue
    const m = new Map<number, number>()
    for (const s of r.stops_ordered) {
      m.set(s.document_id, s.stop_index)
    }
    indexByCamionDoc.set(r.camion, m)
  }
  return orders.map((o) => {
    const m = indexByCamionDoc.get(o.camion)
    const idx = m?.get(o.document_id)
    if (idx == null) return o
    return { ...o, stop_index: idx }
  })
}

export default function PlanificacionDespachoPage() {
  const [orders, setOrders] = useState<PlanificacionStoredOrder[]>([])
  const [orsPayload, setOrsPayload] = useState<DistribuidoraPlanificacionOrsResponse | null>(
    null,
  )
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [selectedVisitId, setSelectedVisitId] = useState<number | null>(null)
  const [planSessionId, setPlanSessionId] = useState<string | null>(null)
  const [crewByCamion, setCrewByCamion] = useState<Map<string, CrewCounts>>(new Map())
  const [crewDefaults, setCrewDefaults] =
    useState<DistribuidoraPlanificacionCrewDefaults | null>(null)
  const [selectedCamion, setSelectedCamion] = useState<string | null>(null)
  const [sessionPlans, setSessionPlans] = useState<DispatchPlanSummary[]>([])

  const reloadFromStorage = useCallback(() => {
    const p = readPlanificacionPayload()
    setOrders(p?.orders ?? [])
    setPlanSessionId(p?.planSessionId ?? null)
  }, [])

  useEffect(() => {
    reloadFromStorage()
  }, [reloadFromStorage])

  const loadSessionPlans = useCallback(async (sessionId: string | null) => {
    if (!sessionId) {
      setSessionPlans([])
      return
    }
    try {
      const { items } = await getDispatchPlansBySession(sessionId)
      setSessionPlans(items)
    } catch {
      setSessionPlans([])
    }
  }, [])

  const fetchRoutes = useCallback(
    async (
      list: PlanificacionStoredOrder[],
      camion: string | null,
      sessionId: string | null,
      crewMap: Map<string, CrewCounts>,
    ) => {
      if (!camion || list.length === 0) {
        setOrsPayload(null)
        setLoading(false)
        return
      }
      const truckOrders = list.filter((o) => o.camion === camion)
      if (truckOrders.length === 0) {
        setOrsPayload(null)
        setLoading(false)
        return
      }
      setLoading(true)
      setError(null)
      try {
        const crew = crewMap.get(camion) ?? { driverCount: 1, assistantCount: 0 }
        const routesPayload = [
          {
            camion,
            truck_id: truckOrders[0]?.truck_id ?? null,
            driver_count: crew.driverCount,
            assistant_count: crew.assistantCount,
            stops: truckOrders.map((o) => ({
              document_id: o.document_id,
              lat: o.lat,
              lng: o.lng,
            })),
          },
        ]
        const res = await postDistribuidoraPlanificacionOrsRoutes({
          planSessionId: sessionId,
          routes: routesPayload,
        })
        setOrsPayload(res)
        if (res.crew_defaults) setCrewDefaults(res.crew_defaults)
        const truckOnly = list.filter((o) => o.camion === camion)
        const optimized = applyOptimizedStopOrder(truckOnly, res.routes)
        const optMap = new Map(optimized.map((o) => [o.document_id, o]))
        setOrders(list.map((o) => optMap.get(o.document_id) ?? o))
        setCrewByCamion(crewMapFromOrders(list, crewMap))
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Error ORS")
        setOrsPayload(null)
      } finally {
        setLoading(false)
      }
    },
    [],
  )

  useEffect(() => {
    const p = readPlanificacionPayload()
    const initial = p?.orders ?? []
    const sessionId = p?.planSessionId ?? null
    setPlanSessionId(sessionId)
    if (initial.length === 0) {
      setLoading(false)
      return
    }
    let cancelled = false
    ;(async () => {
      let crewMap = crewMapFromOrders(initial)
      if (sessionId) {
        try {
          const saved = await getDistribuidoraPlanificacionRouteCrew({
            planSessionId: sessionId,
          })
          if (cancelled) return
          if (saved.defaults) setCrewDefaults(saved.defaults)
          const fromDb = new Map<string, CrewCounts>()
          for (const row of saved.routes) {
            fromDb.set(row.camion, {
              driverCount: row.driver_count,
              assistantCount: row.assistant_count,
            })
          }
          crewMap = crewMapFromOrders(initial, fromDb)
          setCrewByCamion(crewMap)
        } catch {
          setCrewByCamion(crewMap)
        }
      } else {
        setCrewByCamion(crewMap)
      }
      const camiones = Array.from(groupOrdersByTruck(initial).keys())
      const first = camiones[0] ?? null
      if (!cancelled) setSelectedCamion(first)
      if (!cancelled && first) void fetchRoutes(initial, first, sessionId, crewMap)
      if (!cancelled && sessionId) void loadSessionPlans(sessionId)
    })()
    return () => {
      cancelled = true
    }
  }, [fetchRoutes, loadSessionPlans])

  const orsRoutes = orsPayload?.routes ?? []
  const activeRoute = orsRoutes[0] ?? null
  const depot = orsPayload?.depot ?? null

  const truckColorMap = useMemo(() => {
    const keys = Array.from(groupOrdersByTruck(orders).keys())
    return new Map(keys.map((k, i) => [k, TRUCK_COLORS[i % TRUCK_COLORS.length]!]))
  }, [orders])

  const truckSidebarRows = useMemo(() => {
    const byT = groupOrdersByTruck(orders)
    const planByTruck = new Map<number, DispatchPlanSummary>()
    for (const p of sessionPlans) {
      if (p.truck_id != null) planByTruck.set(Number(p.truck_id), p)
    }
    return Array.from(byT.entries()).map(([camion, stops]) => ({
      camion,
      truckId: stops[0]?.truck_id ?? 0,
      stopCount: stops.length,
      plan: planByTruck.get(stops[0]?.truck_id ?? -1) ?? null,
    }))
  }, [orders, sessionPlans])

  const activePlan = useMemo(() => {
    const row = truckSidebarRows.find((t) => t.camion === selectedCamion)
    return row?.plan ?? null
  }, [truckSidebarRows, selectedCamion])

  const ordersForTruck = useMemo(
    () => (selectedCamion ? orders.filter((o) => o.camion === selectedCamion) : []),
    [orders, selectedCamion],
  )

  const mapRoutes: PlanificacionMapRoute[] = useMemo(() => {
    if (!selectedCamion || !activeRoute) return []
    const byT = groupOrdersByTruck(orders)
    const r = activeRoute
    const color = truckColorMap.get(selectedCamion) ?? TRUCK_COLORS[0]!
    const stopsOrdered = r.stops_ordered ?? []
    const stopsFromOrder =
      stopsOrdered.length > 0
        ? stopsOrdered.map((s) => {
            const o = (byT.get(r.camion) ?? []).find((x) => x.document_id === s.document_id)
            return {
              lat: s.lat,
              lng: s.lng,
              num: s.stop_index,
              label: `${o?.nombre_fantasia?.trim() || "Cliente"} · OC ${o?.oc ?? s.document_id}`,
            }
          })
        : (byT.get(r.camion) ?? [])
            .sort((a, b) => a.stop_index - b.stop_index)
            .map((s) => ({
              lat: s.lat,
              lng: s.lng,
              num: s.stop_index,
              label: `${s.nombre_fantasia?.trim() || "Cliente"} · OC ${s.oc ?? s.document_id}`,
            }))
    return [
      {
        camion: r.camion,
        color,
        positions: lineStringToLatLngs(r.geometry),
        stops: stopsFromOrder,
      },
    ]
  }, [orders, activeRoute, selectedCamion, truckColorMap])

  const totals = useMemo(() => {
    if (orsPayload?.totals) {
      return {
        km: orsPayload.totals.distance_km,
        min: orsPayload.totals.duration_min,
        liters: orsPayload.totals.liters_estimated,
        fuelClp: orsPayload.totals.fuel_cost_clp,
        crewClp: orsPayload.totals.crew_cost_clp ?? 0,
        totalClp:
          orsPayload.totals.total_cost_clp ??
          orsPayload.totals.fuel_cost_clp + (orsPayload.totals.crew_cost_clp ?? 0),
        diesel: orsPayload.diesel_price_per_liter,
      }
    }
    let km = 0
    let min = 0
    let liters = 0
    let fuel = 0
    let crew = 0
    let total = 0
    for (const r of orsRoutes) {
      km += Number(r.distance_km) || 0
      min += Number(r.duration_min) || 0
      liters += Number(r.liters_estimated) || 0
      fuel += Number(r.fuel_cost_clp) || 0
      crew += Number(r.crew_cost_clp) || 0
      total += Number(r.cost_breakdown?.total_clp ?? r.fuel_cost_clp) || 0
    }
    return {
      km,
      min,
      liters,
      fuelClp: fuel,
      crewClp: crew,
      totalClp: total || fuel + crew,
      diesel: orsPayload?.diesel_price_per_liter,
    }
  }, [orsPayload, orsRoutes])

  const clientCount = useMemo(() => {
    const clients = new Set<number>()
    for (const o of ordersForTruck) {
      if (o.client_id != null && Number.isFinite(Number(o.client_id))) {
        clients.add(Number(o.client_id))
      }
    }
    return clients.size
  }, [ordersForTruck])

  const visits = useMemo(
    () => buildOrsVisitRows(ordersForTruck, orsRoutes, truckColorMap),
    [ordersForTruck, orsRoutes, truckColorMap],
  )

  const truckOptions = useMemo(
    () => (selectedCamion ? [selectedCamion] : []),
    [selectedCamion],
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
        truck_name: r.truck_name,
        distance_km: r.distance_km,
        duration_min: r.duration_min,
        km_per_liter_used: r.km_per_liter_used,
        liters_estimated: r.liters_estimated,
        fuel_cost_clp: r.fuel_cost_clp,
        crew_cost_clp: r.crew_cost_clp,
        driver_count: r.driver_count,
        assistant_count: r.assistant_count,
        driver_cost_clp: r.driver_cost_clp,
        assistant_cost_clp: r.assistant_cost_clp,
        total_cost_clp: r.cost_breakdown?.total_clp ?? r.fuel_cost_clp,
      })),
    [orsRoutes],
  )

  const handleCrewChange = useCallback(
    (camion: string, driverCount: number, assistantCount: number) => {
      const next = new Map(crewByCamion)
      next.set(camion, { driverCount, assistantCount })
      setCrewByCamion(next)
      void fetchRoutes(orders, camion, planSessionId, next)
    },
    [crewByCamion, fetchRoutes, orders, planSessionId],
  )

  const handleConfirmPlan = useCallback(async (planningName: string) => {
    if (!selectedCamion || !activeRoute || !planSessionId) return
    const truckOrders = orders.filter((o) => o.camion === selectedCamion)
    const truckId = truckOrders[0]?.truck_id
    if (!truckId) throw new Error("Camión sin truck_id")
    const crew = crewByCamion.get(selectedCamion) ?? { driverCount: 1, assistantCount: 0 }
    const bd = activeRoute.cost_breakdown
    await confirmDispatchPlan({
      plan_session_id: planSessionId,
      truck_id: truckId,
      route_name: selectedCamion,
      planning_name: planningName.trim() || selectedCamion,
      driver_count: crew.driverCount,
      assistant_count: crew.assistantCount,
      driver_cost_clp: activeRoute.driver_cost_clp ?? 0,
      assistant_cost_clp: activeRoute.assistant_cost_clp ?? 0,
      diesel_price_per_liter: orsPayload?.diesel_price_per_liter ?? 1200,
      km_total: activeRoute.distance_km,
      duration_min: activeRoute.duration_min,
      liters_estimated: activeRoute.liters_estimated ?? 0,
      fuel_cost_clp: activeRoute.fuel_cost_clp ?? 0,
      ferry_cost_clp: bd?.ferry_clp ?? 0,
      toll_cost_clp: bd?.toll_clp ?? 0,
      extras_cost_clp: 0,
      crew_cost_clp: activeRoute.crew_cost_clp ?? 0,
      total_route_cost_clp: bd?.total_clp ?? activeRoute.fuel_cost_clp ?? 0,
      route_geometry: activeRoute.geometry ?? null,
      orders: truckOrders.map((o) => ({
        oc_document_id: o.document_id,
        oc_number: o.oc ?? null,
        route_order: o.stop_index,
        client_id: o.client_id ?? null,
        client_name: o.nombre_fantasia ?? null,
        oc_total_amount: o.total_amount ?? null,
        lat: o.lat,
        lng: o.lng,
      })),
    })
    const { items } = await getDispatchPlansBySession(planSessionId)
    setSessionPlans(items)
  }, [
    selectedCamion,
    activeRoute,
    planSessionId,
    orders,
    crewByCamion,
    orsPayload,
    loadSessionPlans,
  ])

  if (!loading && orders.length === 0) {
    return <OrsDispatchEmptyState />
  }

  return (
    <div className="-m-6 flex h-[calc(100dvh-4rem)] min-h-[640px] flex-col overflow-hidden bg-background">
      <header className="shrink-0 border-b border-border/80 bg-card/95 px-4 py-3 backdrop-blur-sm md:px-5">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              Dispatch center · ORS 2.0
            </p>
            <h1 className="text-lg font-semibold tracking-tight md:text-xl">
              Planif. mapa ORS
            </h1>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <OrsFuelConfigBar
              onSaved={() => {
                if (selectedCamion && orders.length > 0) {
                  void fetchRoutes(orders, selectedCamion, planSessionId, crewByCamion)
                }
              }}
            />
            <Button
              type="button"
              variant="outline"
              size="sm"
              className="h-8 gap-1.5 text-xs"
              disabled={loading}
              onClick={() => {
                if (selectedCamion) {
                  void fetchRoutes(orders, selectedCamion, planSessionId, crewByCamion)
                }
              }}
            >
              {loading ? (
                <Loader2 className="size-3.5 animate-spin" aria-hidden />
              ) : (
                <RefreshCw className="size-3.5" aria-hidden />
              )}
              Recalcular rutas
            </Button>
            <Button asChild variant="outline" size="sm" className="h-8 text-xs">
              <Link href="/distribuidora/planificaciones">Historial</Link>
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
                setOrsPayload(null)
                setSelectedVisitId(null)
                setPlanSessionId(null)
                setCrewByCamion(new Map())
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
            clientCount={clientCount}
            durationMin={totals.min}
            litersEstimated={totals.liters}
            fuelCostClp={totals.fuelClp}
            crewCostClp={totals.crewClp}
            totalRouteCostClp={totals.totalClp}
            dieselPricePerLiter={totals.diesel}
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
        <OrsTruckSidebar
          trucks={truckSidebarRows}
          selectedCamion={selectedCamion}
          onSelect={(c) => {
            setSelectedCamion(c)
            void fetchRoutes(orders, c, planSessionId, crewByCamion)
          }}
          loading={loading}
        />
        <aside className="flex w-[min(100%,20rem)] shrink-0 flex-col border-r border-border/80 md:w-80 lg:w-[22rem]">
          <OrsClientPanel
            visits={visits}
            routeStats={routeStats}
            kmTotal={totals.km}
            durationMin={totals.min}
            litersTotal={totals.liters}
            fuelCostTotal={totals.fuelClp}
            crewCostTotal={totals.crewClp}
            routeCostTotal={totals.totalClp}
            dieselPricePerLiter={totals.diesel}
            driverRatePerTrip={crewDefaults?.driver_cost_clp_per_trip}
            assistantRatePerTrip={crewDefaults?.assistant_cost_clp_per_trip}
            truckOptions={truckOptions}
            loading={loading}
            selectedVisitId={selectedVisitId}
            onSelectVisit={setSelectedVisitId}
            onCrewChange={handleCrewChange}
            activeCamion={selectedCamion}
          />
          <OrsDispatchWorkflow
            plan={activePlan}
            canConfirm={!loading && !!activeRoute && !activePlan}
            defaultPlanningName={selectedCamion ?? ""}
            onConfirm={handleConfirmPlan}
            onPlanUpdated={() => {
              if (planSessionId) void loadSessionPlans(planSessionId)
            }}
          />
        </aside>

        <main className="relative min-w-0 flex-1 p-2 md:p-3">
          {loading ? (
            <OrsMapSkeleton />
          ) : (
            <PlanificacionMap
              routes={mapRoutes}
              depot={depot}
              highlightedStopKey={highlightedStopKey}
              className="h-full min-h-0 w-full overflow-hidden rounded-lg border border-border/80 bg-slate-950/5 shadow-md dark:bg-slate-950/50"
            />
          )}
        </main>
      </div>
    </div>
  )
}
