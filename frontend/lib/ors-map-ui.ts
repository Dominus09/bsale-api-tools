import type {
  DistribuidoraPlanificacionOrsRoute,
  OrsStopOrdered,
} from "@/lib/api"
import type { PlanificacionStoredOrder } from "@/lib/planificacion-despacho-storage"

export type OrsVisitRow = {
  document_id: number
  stop_index: number
  camion: string
  nombre: string
  ocLabel: string
  cityLabel: string
  etaMinutes: number
  etaLabel: string
  routeColor?: string
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
      rows.push({
        document_id: stop.document_id,
        stop_index: visitIndex,
        camion,
        nombre: stop.nombre_fantasia?.trim() || "Cliente",
        ocLabel: stop.oc != null ? `OC ${stop.oc}` : `Doc ${stop.document_id}`,
        cityLabel: camion,
        etaMinutes: cumulative,
        etaLabel: formatOrsEta(cumulative),
        routeColor: truckColors.get(camion),
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
