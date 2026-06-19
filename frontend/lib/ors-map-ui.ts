import type {
  DistribuidoraPlanificacionOrsRoute,
  OrsStopOrdered,
} from "@/lib/api"
import {
  commercialSemaphore,
  type CommercialSemaphore,
} from "@/lib/ors-commercial-semaphore"
import type { PlanificacionStoredOrder } from "@/lib/planificacion-despacho-storage"

export type RouteClientRow = {
  client_id: number
  nombre: string
  comuna: string | null
  direccion: string | null
  venta_total: number
  oc_count: number
  stop_index_min: number
  list_index: number
  semaphore: CommercialSemaphore
  observaciones: string[]
  dia_entrega_label: string | null
  lat: number | null
  lng: number | null
  primary_document_id: number
}

export type OrsStopPopupData = {
  nombre: string
  direccion?: string | null
  comuna?: string | null
  ventaTotal: number
  ocCount: number
  observaciones?: string[]
  diaEntregaLabel?: string | null
  semaphore?: CommercialSemaphore
}

export function formatOrsEta(minutesFromStart: number): string {
  const m = Math.max(0, Math.round(minutesFromStart))
  if (m < 60) return `~${m} min`
  const h = Math.floor(m / 60)
  const r = m % 60
  return r > 0 ? `~${h}h ${r}m` : `~${h}h`
}

export function formatClp(n: number): string {
  return new Intl.NumberFormat("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  }).format(Number.isFinite(n) ? n : 0)
}

function orderIndexByDoc(stopsOrdered: OrsStopOrdered[] | undefined): Map<number, number> {
  const m = new Map<number, number>()
  if (!stopsOrdered?.length) return m
  for (const s of stopsOrdered) {
    m.set(s.document_id, s.stop_index)
  }
  return m
}

/** Visitas en orden ORS optimizado (stops_ordered) con ETA por reparto de duration_min. */
export function buildOrsVisitRows(
  orders: PlanificacionStoredOrder[],
  orsRoutes: DistribuidoraPlanificacionOrsRoute[],
  truckColors: Map<string, string>,
): OrsVisitRow[] {
  const routeByCamion = new Map(orsRoutes.map((r) => [r.camion, r]))
  const orderByCamionDoc = new Map<string, Map<number, number>>()
  for (const r of orsRoutes) {
    orderByCamionDoc.set(r.camion, orderIndexByDoc(r.stops_ordered))
  }

  const byTruck = new Map<string, PlanificacionStoredOrder[]>()
  for (const o of orders) {
    const arr = byTruck.get(o.camion)
    if (arr) arr.push(o)
    else byTruck.set(o.camion, [o])
  }

  const rows: OrsVisitRow[] = []

  for (const [camion, stops] of byTruck) {
    const route = routeByCamion.get(camion)
    const idxMap = orderByCamionDoc.get(camion)
    const sorted = [...stops].sort((a, b) => {
      const ia = idxMap?.get(a.document_id) ?? a.stop_index
      const ib = idxMap?.get(b.document_id) ?? b.stop_index
      return ia - ib
    })
    const durationMin = Number(route?.duration_min) || 0
    const n = sorted.length
    const legMin = n > 1 ? durationMin / (n - 1) : durationMin > 0 ? durationMin : 0
    let cumulative = 0

    for (const stop of sorted) {
      const visitIndex = idxMap?.get(stop.document_id) ?? stop.stop_index
      const venta = Number(stop.total_amount) || 0
      rows.push({
        document_id: stop.document_id,
        stop_index: visitIndex,
        camion,
        nombre: stop.nombre_fantasia?.trim() || "Cliente",
        ocLabel: stop.oc != null ? `OC ${stop.oc}` : `Doc ${stop.document_id}`,
        cityLabel: stop.municipality?.trim() || camion,
        etaMinutes: cumulative,
        etaLabel: formatOrsEta(cumulative),
        routeColor: truckColors.get(camion),
        client_id: stop.client_id ?? null,
        municipality: stop.municipality ?? null,
        ventaClp: venta,
        semaphore: commercialSemaphore(venta),
      })
      cumulative += legMin
    }
  }

  return rows.sort((a, b) => {
    const c = a.camion.localeCompare(b.camion, "es")
    if (c !== 0) return c
    return a.stop_index - b.stop_index
  })
}

function stopIndexForOrder(
  order: PlanificacionStoredOrder,
  idxMap: Map<number, number> | undefined,
): number {
  return idxMap?.get(order.document_id) ?? order.stop_index
}

/** Clientes únicos agrupados en orden ORS (primera parada del cliente). */
export function buildRouteClientRows(
  orders: PlanificacionStoredOrder[],
  orsRoutes: DistribuidoraPlanificacionOrsRoute[],
): RouteClientRow[] {
  const routeByCamion = new Map(orsRoutes.map((r) => [r.camion, r]))
  const orderByCamionDoc = new Map<string, Map<number, number>>()
  for (const r of orsRoutes) {
    orderByCamionDoc.set(r.camion, orderIndexByDoc(r.stops_ordered))
  }

  const grouped = new Map<number, PlanificacionStoredOrder[]>()
  for (const o of orders) {
    const cid = o.client_id != null ? Number(o.client_id) : null
    if (cid == null || !Number.isFinite(cid)) continue
    const arr = grouped.get(cid)
    if (arr) arr.push(o)
    else grouped.set(cid, [o])
  }

  const rows: RouteClientRow[] = []
  for (const [clientId, clientOrders] of grouped) {
    const camion = clientOrders[0]?.camion ?? ""
    const idxMap = orderByCamionDoc.get(camion)
    const stopMin = Math.min(
      ...clientOrders.map((o) => stopIndexForOrder(o, idxMap)),
    )
    const primary = [...clientOrders].sort(
      (a, b) => stopIndexForOrder(a, idxMap) - stopIndexForOrder(b, idxMap),
    )[0]!
    const ventaTotal = clientOrders.reduce(
      (s, o) => s + (Number(o.total_amount) || 0),
      0,
    )
    const obs = [
      ...new Set(
        clientOrders
          .map((o) => o.observaciones?.trim())
          .filter((x): x is string => Boolean(x)),
      ),
    ]
    const diaLabels = [
      ...new Set(
        clientOrders
          .map((o) => o.dia_entrega_label?.trim())
          .filter((x): x is string => Boolean(x)),
      ),
    ]
    rows.push({
      client_id: clientId,
      nombre: primary.nombre_fantasia?.trim() || `Cliente ${clientId}`,
      comuna: primary.municipality?.trim() || null,
      direccion: primary.direccion?.trim() || null,
      venta_total: ventaTotal,
      oc_count: clientOrders.length,
      stop_index_min: stopMin,
      list_index: 0,
      semaphore: commercialSemaphore(ventaTotal),
      observaciones: obs,
      dia_entrega_label: diaLabels[0] ?? null,
      lat: primary.lat != null ? Number(primary.lat) : null,
      lng: primary.lng != null ? Number(primary.lng) : null,
      primary_document_id: primary.document_id,
    })
  }

  rows.sort((a, b) => a.stop_index_min - b.stop_index_min)
  return rows.map((r, i) => ({ ...r, list_index: i + 1 }))
}

export function buildStopPopupData(
  order: PlanificacionStoredOrder | undefined,
  clientRow?: RouteClientRow,
): OrsStopPopupData | undefined {
  if (clientRow) {
    return {
      nombre: clientRow.nombre,
      direccion: clientRow.direccion,
      comuna: clientRow.comuna,
      ventaTotal: clientRow.venta_total,
      ocCount: clientRow.oc_count,
      observaciones: clientRow.observaciones,
      diaEntregaLabel: clientRow.dia_entrega_label,
      semaphore: clientRow.semaphore,
    }
  }
  if (!order) return undefined
  const venta = Number(order.total_amount) || 0
  return {
    nombre: order.nombre_fantasia?.trim() || "Cliente",
    direccion: order.direccion?.trim() || null,
    comuna: order.municipality?.trim() || null,
    ventaTotal: venta,
    ocCount: 1,
    observaciones: order.observaciones?.trim() ? [order.observaciones.trim()] : [],
    diaEntregaLabel: order.dia_entrega_label ?? null,
    semaphore: commercialSemaphore(venta),
  }
}
