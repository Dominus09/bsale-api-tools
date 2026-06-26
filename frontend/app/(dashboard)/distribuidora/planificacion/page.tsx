"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import dynamic from "next/dynamic"
import Link from "next/link"
import { Loader2, RefreshCw, Trash2 } from "lucide-react"

import {
  confirmDispatchPlan,
  getDistribuidoraPlanificacionRouteCrew,
  getDistribuidoraPlanningLiveMetrics,
  getDistribuidoraTrucks,
  postDistribuidoraPlanificacionOrsRoutes,
  recalculateOrderWeightsBatch,
  type DispatchPlanSummary,
  type DistribuidoraPlanificacionCrewDefaults,
  type DistribuidoraPlanificacionOrsResponse,
  type DistribuidoraPlanificacionOrsRoute,
  type DistribuidoraTruck,
} from "@/lib/api"
import {
  fetchDispatchPlansBySessionDeduped,
  invalidateSessionPlansCache,
  logFrontendPlanDebug,
} from "@/lib/planificacion-fetch"
import { buildOrsVisitRows, buildRouteClientRows, buildStopPopupData } from "@/lib/ors-map-ui"
import {
  readPlanificacionPayload,
  clearPlanificacionPayload,
  writePlanificacionPayload,
  type PlanificacionStoredOrder,
} from "@/lib/planificacion-despacho-storage"
import {
  countBsaleUpdatedPending,
  mergeLiveMetricsIntoPlanOrders,
} from "@/lib/planificacion-live-refresh"
import { orderHasGeo, splitOrdersByGeo } from "@/lib/planificacion-geo"
import {
  computeOperationalCostClp,
  computeRouteSales,
  EMPTY_OPERATIONAL_COSTS,
  type RouteOperationalCosts,
} from "@/lib/planificacion-operational-costs"
import {
  estimateAssignedKgFromOrders,
  estimateAssignedKgFromStops,
  isTruckOverloaded,
  truckUtilizationPct,
} from "@/lib/ors-truck-capacity"
import type { PlanificacionMapRoute, PlanificacionMapStop } from "@/components/distribuidora/planificacion-despacho-map-client"
import { OrsClientPanel } from "@/components/distribuidora/planificacion/OrsClientPanel"
import { OrsClientRouteList } from "@/components/distribuidora/planificacion/OrsClientRouteList"
import {
  countCommercialSemaphores,
  OrsCommercialKpiStrip,
  type CommercialKpiCounts,
} from "@/components/distribuidora/planificacion/OrsCommercialKpiStrip"
import { OrsDispatchEmptyState } from "@/components/distribuidora/planificacion/OrsDispatchEmptyState"
import { OrsMapSkeleton } from "@/components/distribuidora/planificacion/OrsMapSkeleton"
import { OrsOperationalCostsPanel } from "@/components/distribuidora/planificacion/OrsOperationalCostsPanel"
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
  const [selectedClientId, setSelectedClientId] = useState<number | null>(null)
  const [planSessionId, setPlanSessionId] = useState<string | null>(null)
  const [crewByCamion, setCrewByCamion] = useState<Map<string, CrewCounts>>(new Map())
  const [crewDefaults, setCrewDefaults] =
    useState<DistribuidoraPlanificacionCrewDefaults | null>(null)
  const [selectedCamion, setSelectedCamion] = useState<string | null>(null)
  const [sessionPlans, setSessionPlans] = useState<DispatchPlanSummary[]>([])
  const [operationalCosts, setOperationalCosts] =
    useState<RouteOperationalCosts>(EMPTY_OPERATIONAL_COSTS)
  const [mapFlyTo, setMapFlyTo] = useState<{
    lat: number
    lng: number
    zoom?: number
    seq: number
  } | null>(null)
  const [openPopupStopKey, setOpenPopupStopKey] = useState<string | null>(null)
  const [trucksCatalog, setTrucksCatalog] = useState<DistribuidoraTruck[]>([])
  const [liveRefreshNote, setLiveRefreshNote] = useState<string | null>(null)

  const orsAbortRef = useRef<AbortController | null>(null)
  const orsInFlightKeyRef = useRef<string | null>(null)
  const sessionLoadedForRef = useRef<string | null>(null)
  const bootstrapDoneRef = useRef(false)
  const crewDebounceRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const ordersRef = useRef(orders)
  const planSessionIdRef = useRef(planSessionId)
  const crewByCamionRef = useRef(crewByCamion)
  const operationalCostsRef = useRef(operationalCosts)
  ordersRef.current = orders
  planSessionIdRef.current = planSessionId
  crewByCamionRef.current = crewByCamion
  operationalCostsRef.current = operationalCosts

  const loadSessionPlans = useCallback(async (sessionId: string | null, force = false) => {
    if (!sessionId) {
      setSessionPlans([])
      return
    }
    logFrontendPlanDebug("loadSessionPlans", {
      plan_session_id: sessionId,
      force,
      trigger: "callback",
    })
    try {
      const { items } = await fetchDispatchPlansBySessionDeduped(sessionId, { force })
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
      dieselPricePerLiter?: number | null,
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
      const { routable: routableOrders } = splitOrdersByGeo(truckOrders)
      if (routableOrders.length === 0) {
        setOrsPayload(null)
        setLoading(false)
        setError(null)
        return
      }

      const diesel =
        dieselPricePerLiter ??
        operationalCostsRef.current.diesel_clp_per_liter ??
        null
      const routeKey = `${camion}:${diesel ?? "d"}:${routableOrders.map((o) => o.document_id).join(",")}`
      if (orsInFlightKeyRef.current === routeKey) {
        logFrontendPlanDebug("ors-routes", { camion, skipped: "inflight-same-key" })
        return
      }

      orsAbortRef.current?.abort()
      const ac = new AbortController()
      orsAbortRef.current = ac
      orsInFlightKeyRef.current = routeKey

      setLoading(true)
      setError(null)
      logFrontendPlanDebug("ors-routes", {
        camion,
        plan_session_id: sessionId,
        stops: routableOrders.length,
        pending_georef: truckOrders.length - routableOrders.length,
        trigger: "fetchRoutes",
      })

      try {
        const crew = crewMap.get(camion) ?? { driverCount: 1, assistantCount: 0 }
        const res = await postDistribuidoraPlanificacionOrsRoutes({
          planSessionId: sessionId,
          dieselPricePerLiter: diesel,
          routes: [
            {
              camion,
              truck_id: routableOrders[0]?.truck_id ?? null,
              driver_count: crew.driverCount,
              assistant_count: crew.assistantCount,
              stops: routableOrders.map((o) => ({
                document_id: o.document_id,
                lat: Number(o.lat),
                lng: Number(o.lng),
              })),
            },
          ],
          signal: ac.signal,
        })
        if (ac.signal.aborted) return
        setOrsPayload(res)
        if (res.crew_defaults) setCrewDefaults(res.crew_defaults)
        const optimized = applyOptimizedStopOrder(truckOrders, res.routes)
        const optMap = new Map(optimized.map((o) => [o.document_id, o]))
        setOrders(list.map((o) => optMap.get(o.document_id) ?? o))
        setCrewByCamion(crewMapFromOrders(list, crewMap))
      } catch (e: unknown) {
        if (ac.signal.aborted) return
        setError(e instanceof Error ? e.message : "Error ORS")
        setOrsPayload(null)
      } finally {
        if (orsInFlightKeyRef.current === routeKey) {
          orsInFlightKeyRef.current = null
        }
        if (!ac.signal.aborted) setLoading(false)
      }
    },
    [],
  )

  /** Bootstrap único al montar (evita loop por deps de fetchRoutes/loadSessionPlans). */
  useEffect(() => {
    if (bootstrapDoneRef.current) return
    bootstrapDoneRef.current = true

    const p = readPlanificacionPayload()
    const initial = p?.orders ?? []
    const sessionId = p?.planSessionId ?? null
    setOrders(initial)
    setPlanSessionId(sessionId)

    if (initial.length === 0) {
      setLoading(false)
      return
    }

    let cancelled = false
    ;(async () => {
      let workingOrders = initial
      try {
        const live = await getDistribuidoraPlanningLiveMetrics({
          documentIds: initial.map((o) => o.document_id),
        })
        if (!cancelled && live.items.length > 0) {
          workingOrders = mergeLiveMetricsIntoPlanOrders(initial, live.items)
          const pending = countBsaleUpdatedPending(live.items)
          if (pending > 0) {
            setLiveRefreshNote(
              `${pending} orden(es) con cambios en Bsale no reflejados en el snapshot. ` +
                "Ejecute sync de órdenes y vuelva a cargar pre-despacho.",
            )
          }
          const refreshedPayload = {
            submittedAt: new Date().toISOString(),
            planSessionId: sessionId ?? p?.planSessionId ?? "",
            orders: workingOrders,
          }
          writePlanificacionPayload(refreshedPayload)
          setOrders(workingOrders)
        }
      } catch {
        /* refresco live opcional */
      }

      try {
        const trucksRes = await getDistribuidoraTrucks()
        if (!cancelled) setTrucksCatalog(trucksRes.items ?? [])
      } catch {
        /* catálogo opcional para capacidad */
      }

      let crewMap = crewMapFromOrders(workingOrders)
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
          crewMap = crewMapFromOrders(workingOrders, fromDb)
        } catch {
          /* defaults locales */
        }
      }
      if (cancelled) return
      setCrewByCamion(crewMap)

      const camiones = Array.from(groupOrdersByTruck(workingOrders).keys())
      const first = camiones[0] ?? null
      setSelectedCamion(first)

      if (sessionId && sessionLoadedForRef.current !== sessionId) {
        sessionLoadedForRef.current = sessionId
        void loadSessionPlans(sessionId)
      }

      if (first) void fetchRoutes(workingOrders, first, sessionId, crewMap)
    })()

    return () => {
      cancelled = true
      orsAbortRef.current?.abort()
      if (crewDebounceRef.current) clearTimeout(crewDebounceRef.current)
    }
    // Solo al montar — no re-ejecutar cuando cambian callbacks
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const orsRoutes = orsPayload?.routes ?? []
  const activeRoute = orsRoutes[0] ?? null
  const depot = orsPayload?.depot ?? null

  const truckColorMap = useMemo(() => {
    const keys = Array.from(groupOrdersByTruck(orders).keys())
    return new Map(keys.map((k, i) => [k, TRUCK_COLORS[i % TRUCK_COLORS.length]!]))
  }, [orders])

  const trucksById = useMemo(() => {
    const m = new Map<number, DistribuidoraTruck>()
    for (const t of trucksCatalog) m.set(t.id, t)
    return m
  }, [trucksCatalog])

  const truckSidebarRows = useMemo(() => {
    const byT = groupOrdersByTruck(orders)
    const planByTruck = new Map<number, DispatchPlanSummary>()
    for (const p of sessionPlans) {
      if (p.truck_id != null) planByTruck.set(Number(p.truck_id), p)
    }
    return Array.from(byT.entries()).map(([camion, stops]) => {
      const truckId = stops[0]?.truck_id ?? 0
      const meta = trucksById.get(truckId)
      const maxWeightKg = meta?.max_weight_kg ?? null
      const estimatedAssignedKg = estimateAssignedKgFromOrders(stops)
      const utilizationPct = truckUtilizationPct(estimatedAssignedKg, maxWeightKg)
      return {
        camion,
        truckId,
        stopCount: stops.length,
        maxWeightKg,
        estimatedAssignedKg,
        utilizationPct,
        overloaded: isTruckOverloaded(estimatedAssignedKg, maxWeightKg),
        plan: planByTruck.get(truckId) ?? null,
      }
    })
  }, [orders, sessionPlans, trucksById])

  const activePlan = useMemo(() => {
    const row = truckSidebarRows.find((t) => t.camion === selectedCamion)
    return row?.plan ?? null
  }, [truckSidebarRows, selectedCamion])

  const ordersForTruck = useMemo(
    () => (selectedCamion ? orders.filter((o) => o.camion === selectedCamion) : []),
    [orders, selectedCamion],
  )

  const { routable: routableForTruck, pendingGeoref: pendingGeorefForTruck } = useMemo(
    () => splitOrdersByGeo(ordersForTruck),
    [ordersForTruck],
  )

  const routeClientRows = useMemo(
    () => buildRouteClientRows(ordersForTruck, orsRoutes),
    [ordersForTruck, orsRoutes],
  )

  const commercialCounts = useMemo((): CommercialKpiCounts => {
    const base = countCommercialSemaphores(routeClientRows)
    return {
      ...base,
      isolated: routeClientRows.filter((c) => c.isolated).length,
    }
  }, [routeClientRows])

  const clientByDocumentId = useMemo(() => {
    const m = new Map<number, (typeof routeClientRows)[number]>()
    for (const c of routeClientRows) {
      for (const o of ordersForTruck) {
        if (o.client_id === c.client_id) {
          m.set(o.document_id, c)
        }
      }
    }
    return m
  }, [routeClientRows, ordersForTruck])

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
            const clientRow = clientByDocumentId.get(s.document_id)
            const stopKey = `${r.camion}-${s.stop_index}`
            return {
              lat: s.lat,
              lng: s.lng,
              num: s.stop_index,
              stopKey,
              documentId: s.document_id,
              clientId: o?.client_id ?? clientRow?.client_id ?? null,
              label: `${o?.nombre_fantasia?.trim() || "Cliente"} · OC ${o?.oc ?? s.document_id}`,
              comuna: o?.municipality?.trim() || clientRow?.comuna || null,
              semaphore: clientRow?.semaphore,
              popup: buildStopPopupData(o, clientRow),
            }
          })
        : (byT.get(r.camion) ?? [])
            .filter(orderHasGeo)
            .sort((a, b) => a.stop_index - b.stop_index)
            .map((s) => {
              const clientRow = clientByDocumentId.get(s.document_id)
              const stopKey = `${r.camion}-${s.stop_index}`
              return {
                lat: Number(s.lat),
                lng: Number(s.lng),
                num: s.stop_index,
                stopKey,
                documentId: s.document_id,
                clientId: s.client_id ?? clientRow?.client_id ?? null,
                label: `${s.nombre_fantasia?.trim() || "Cliente"} · OC ${s.oc ?? s.document_id}`,
                comuna: s.municipality?.trim() || clientRow?.comuna || null,
                semaphore: clientRow?.semaphore,
                popup: buildStopPopupData(s, clientRow),
              }
            })
    return [
      {
        camion: r.camion,
        color,
        positions: lineStringToLatLngs(r.geometry),
        stops: stopsFromOrder,
      },
    ]
  }, [orders, activeRoute, selectedCamion, truckColorMap, clientByDocumentId])

  const handleDieselChange = useCallback(
    (diesel: number) => {
      if (!selectedCamion) return
      void fetchRoutes(
        ordersRef.current,
        selectedCamion,
        planSessionIdRef.current,
        crewByCamionRef.current,
        diesel,
      )
    },
    [fetchRoutes, selectedCamion],
  )

  const handleSelectClient = useCallback(
    (client: (typeof routeClientRows)[number]) => {
      setSelectedClientId(client.client_id)
      setSelectedVisitId(client.primary_document_id)
      if (selectedCamion && client.lat != null && client.lng != null) {
        setMapFlyTo({
          lat: client.lat,
          lng: client.lng,
          zoom: 15,
          seq: Date.now(),
        })
        setOpenPopupStopKey(`${selectedCamion}-${client.stop_index_min}`)
      }
    },
    [selectedCamion],
  )

  const handleStopClick = useCallback((stop: PlanificacionMapStop) => {
    setOpenPopupStopKey(stop.stopKey)
    if (stop.documentId != null) setSelectedVisitId(stop.documentId)
    if (stop.clientId != null) setSelectedClientId(Number(stop.clientId))
    setMapFlyTo({ lat: stop.lat, lng: stop.lng, zoom: 15, seq: Date.now() })
  }, [])

  const handlePopupClose = useCallback(() => {
    setOpenPopupStopKey(null)
  }, [])

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

  const routeSalesClp = useMemo(
    () => computeRouteSales(ordersForTruck),
    [ordersForTruck],
  )

  const operationalCostClp = useMemo(
    () => computeOperationalCostClp(totals.fuelClp, operationalCosts),
    [totals.fuelClp, operationalCosts],
  )

  const activeTruckId = ordersForTruck[0]?.truck_id ?? null

  const visits = useMemo(
    () => buildOrsVisitRows(routableForTruck, orsRoutes, truckColorMap),
    [routableForTruck, orsRoutes, truckColorMap],
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
      const next = new Map(crewByCamionRef.current)
      next.set(camion, { driverCount, assistantCount })
      setCrewByCamion(next)
      if (crewDebounceRef.current) clearTimeout(crewDebounceRef.current)
      crewDebounceRef.current = setTimeout(() => {
        void fetchRoutes(
          ordersRef.current,
          camion,
          planSessionIdRef.current,
          next,
          operationalCostsRef.current.diesel_clp_per_liter,
        )
      }, 450)
    },
    [fetchRoutes],
  )

  const handleConfirmPlan = useCallback(async (planningName: string) => {
    if (!selectedCamion || !planSessionId) return
    const truckOrders = orders.filter((o) => o.camion === selectedCamion)
    const truckId = truckOrders[0]?.truck_id
    if (!truckId) throw new Error("Camión sin truck_id")
    const { routable, pendingGeoref } = splitOrdersByGeo(truckOrders)
    if (routable.length === 0 && pendingGeoref.length === 0) return
    const crew = crewByCamion.get(selectedCamion) ?? { driverCount: 1, assistantCount: 0 }
    const bd = activeRoute?.cost_breakdown
    const maxRoutedStop = routable.reduce(
      (m, o) => Math.max(m, o.stop_index ?? 0),
      0,
    )
    const planOrders = [
      ...routable.map((o) => ({
        oc_document_id: o.document_id,
        oc_number: o.oc ?? null,
        route_order: o.stop_index,
        client_id: o.client_id ?? null,
        client_name: o.nombre_fantasia ?? null,
        oc_total_amount: o.total_amount ?? null,
        lat: o.lat ?? null,
        lng: o.lng ?? null,
      })),
      ...pendingGeoref.map((o, i) => ({
        oc_document_id: o.document_id,
        oc_number: o.oc ?? null,
        route_order: maxRoutedStop + i + 1,
        client_id: o.client_id ?? null,
        client_name: o.nombre_fantasia ?? null,
        oc_total_amount: o.total_amount ?? null,
        lat: null,
        lng: null,
      })),
    ]
    const extrasClp = operationalCosts.per_diem_clp + operationalCosts.other_clp
    const fuelClp = activeRoute?.fuel_cost_clp ?? 0
    const crewClp = activeRoute?.crew_cost_clp ?? 0
    await confirmDispatchPlan({
      plan_session_id: planSessionId,
      truck_id: truckId,
      route_name: selectedCamion,
      planning_name: planningName.trim() || selectedCamion,
      driver_count: crew.driverCount,
      assistant_count: crew.assistantCount,
      driver_cost_clp: activeRoute?.driver_cost_clp ?? 0,
      assistant_cost_clp: activeRoute?.assistant_cost_clp ?? 0,
      diesel_price_per_liter:
        operationalCosts.diesel_clp_per_liter ??
        orsPayload?.diesel_price_per_liter ??
        1500,
      km_total: activeRoute?.distance_km ?? 0,
      duration_min: activeRoute?.duration_min ?? 0,
      liters_estimated: activeRoute?.liters_estimated ?? 0,
      fuel_cost_clp: fuelClp,
      ferry_cost_clp: operationalCosts.ferry_clp,
      toll_cost_clp: bd?.toll_clp ?? 0,
      extras_cost_clp: extrasClp,
      crew_cost_clp: crewClp,
      total_route_cost_clp:
        fuelClp +
        crewClp +
        operationalCosts.ferry_clp +
        extrasClp,
      route_geometry: activeRoute?.geometry ?? null,
      orders: planOrders,
    })
    invalidateSessionPlansCache(planSessionId)
    await loadSessionPlans(planSessionId, true)
  }, [
    selectedCamion,
    activeRoute,
    planSessionId,
    orders,
    crewByCamion,
    orsPayload,
    loadSessionPlans,
    operationalCosts,
  ])

  const handleRecalculateWeights = useCallback(async () => {
    if (!selectedCamion) return
    const truckOrders = orders.filter((o) => o.camion === selectedCamion)
    const ids = truckOrders.map((o) => o.document_id)
    if (!ids.length) return
    await recalculateOrderWeightsBatch({
      document_ids: ids,
      plan_session_id: planSessionId ?? undefined,
      motivo: "recalcular_planificacion_ors",
    })
    const live = await getDistribuidoraPlanningLiveMetrics({ documentIds: ids })
    const merged = mergeLiveMetricsIntoPlanOrders(truckOrders, live.items)
    const byId = new Map(merged.map((o) => [o.document_id, o]))
    const nextOrders = orders.map((o) => byId.get(o.document_id) ?? o)
    setOrders(nextOrders)
    writePlanificacionPayload({
      submittedAt: new Date().toISOString(),
      planSessionId: planSessionId ?? "",
      orders: nextOrders,
    })
  }, [selectedCamion, orders, planSessionId])

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
            <Button
              type="button"
              variant="outline"
              size="sm"
              className="h-8 gap-1.5 text-xs"
              disabled={loading}
              onClick={() => {
                if (selectedCamion) {
                  void fetchRoutes(
                    orders,
                    selectedCamion,
                    planSessionId,
                    crewByCamion,
                    operationalCosts.diesel_clp_per_liter,
                  )
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
        <div className="mt-3 space-y-2">
          <OrsTopBar
            kmTotal={totals.km}
            clientCount={clientCount}
            durationMin={totals.min}
            litersEstimated={totals.liters}
            fuelCostClp={totals.fuelClp}
            routeSalesClp={routeSalesClp}
            operationalCostClp={operationalCostClp}
            crewCostClp={totals.crewClp}
            totalRouteCostClp={operationalCostClp + totals.crewClp}
            dieselPricePerLiter={totals.diesel}
            loading={loading}
            routeSalesLoading={false}
          />
          <OrsCommercialKpiStrip counts={commercialCounts} loading={loading} />
        </div>
      </header>

      {error ? (
        <Alert variant="destructive" className="mx-4 mt-2 shrink-0 md:mx-5">
          <AlertTitle>Error ORS</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      {liveRefreshNote ? (
        <Alert className="mx-4 mt-2 shrink-0 border-red-500/40 bg-red-50/90 dark:bg-red-950/30 md:mx-5">
          <AlertTitle>🔴 Actualizada en Bsale</AlertTitle>
          <AlertDescription>{liveRefreshNote}</AlertDescription>
        </Alert>
      ) : null}

      <div className="flex min-h-0 flex-1">
        <OrsTruckSidebar
          trucks={truckSidebarRows}
          selectedCamion={selectedCamion}
          onSelect={(c) => {
            setSelectedCamion(c)
            setSelectedClientId(null)
            void fetchRoutes(
              orders,
              c,
              planSessionId,
              crewByCamion,
              operationalCosts.diesel_clp_per_liter,
            )
          }}
          loading={loading}
        />
        <aside className="flex min-h-0 w-[min(100%,20rem)] shrink-0 flex-col overflow-hidden border-r border-border/80 md:w-80 lg:w-[22rem]">
          {pendingGeorefForTruck.length > 0 ? (
            <div className="border-b border-amber-500/30 bg-amber-50/60 px-3 py-2 dark:bg-amber-950/30">
              <p className="text-[10px] font-semibold uppercase tracking-wide text-amber-900 dark:text-amber-200">
                Pendientes de georreferenciar ({pendingGeorefForTruck.length})
              </p>
              <ul className="mt-1 max-h-28 space-y-1 overflow-y-auto text-xs">
                {pendingGeorefForTruck.map((o) => (
                  <li key={o.document_id} className="truncate text-amber-950 dark:text-amber-100">
                    {o.nombre_fantasia?.trim() || "Cliente"} · OC {o.oc ?? o.document_id}
                  </li>
                ))}
              </ul>
            </div>
          ) : null}
          <div className="min-h-0 flex-1 overflow-y-auto">
          <OrsOperationalCostsPanel
            planSessionId={planSessionId}
            truckId={activeTruckId}
            fuelCostClp={totals.fuelClp}
            loading={loading}
            onCostsChange={setOperationalCosts}
            onDieselChange={handleDieselChange}
          />
          <OrsClientRouteList
            clients={routeClientRows}
            loading={loading}
            selectedClientId={selectedClientId}
            onSelectClient={handleSelectClient}
          />
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
            onSelectVisit={(documentId) => {
              setSelectedVisitId(documentId)
              const order = ordersForTruck.find((o) => o.document_id === documentId)
              setSelectedClientId(
                order?.client_id != null ? Number(order.client_id) : null,
              )
              const visit = visits.find((v) => v.document_id === documentId)
              if (
                selectedCamion &&
                visit &&
                order?.lat != null &&
                order?.lng != null
              ) {
                setMapFlyTo({
                  lat: Number(order.lat),
                  lng: Number(order.lng),
                  zoom: 15,
                  seq: Date.now(),
                })
                setOpenPopupStopKey(`${selectedCamion}-${visit.stop_index}`)
              }
            }}
            onCrewChange={handleCrewChange}
            activeCamion={selectedCamion}
          />
          </div>
          <OrsDispatchWorkflow
            plan={activePlan}
            canConfirm={
              !loading &&
              !activePlan &&
              ordersForTruck.length > 0 &&
              (routableForTruck.length === 0 || !!activeRoute)
            }
            defaultPlanningName={selectedCamion ?? ""}
            onConfirm={handleConfirmPlan}
            onPlanUpdated={() => {
              if (planSessionId) void loadSessionPlans(planSessionId, true)
            }}
            onRecalculateWeights={
              !activePlan ? () => handleRecalculateWeights() : undefined
            }
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
              flyToTarget={mapFlyTo}
              openPopupStopKey={openPopupStopKey}
              onPopupClose={handlePopupClose}
              onStopClick={handleStopClick}
              className="h-full min-h-0 w-full overflow-hidden rounded-lg border border-border/80 bg-slate-950/5 shadow-md dark:bg-slate-950/50"
            />
          )}
        </main>
      </div>
    </div>
  )
}
