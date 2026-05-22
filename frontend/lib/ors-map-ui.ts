import type { DistribuidoraPlanificacionOrsRoute } from "@/lib/api"
import type { PlanificacionStoredOrder } from "@/lib/planificacion-despacho-storage"

/** Estimación solo UI (no backend). CLP por km — ajustable en futuro vía config. */
export const ORS_FUEL_CLP_PER_KM = 180

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

export function estimateFuelCostClp(distanceKm: number): number {
  const km = Number.isFinite(distanceKm) ? distanceKm : 0
  return Math.round(km * ORS_FUEL_CLP_PER_KM)
}

export function formatClp(n: number): string {
  return new Intl.NumberFormat("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  }).format(Number.isFinite(n) ? n : 0)
}

/** Reparte duration_min de ORS entre paradas (heurística UI, sin alterar ORS). */
export function buildOrsVisitRows(
  orders: PlanificacionStoredOrder[],
  orsRoutes: DistribuidoraPlanificacionOrsRoute[],
  truckColors: Map<string, string>,
): OrsVisitRow[] {
  const routeByCamion = new Map(orsRoutes.map((r) => [r.camion, r]))
  const byTruck = new Map<string, PlanificacionStoredOrder[]>()

  const sorted = [...orders].sort((a, b) => {
    const c = a.camion.localeCompare(b.camion, "es")
    if (c !== 0) return c
    return a.stop_index - b.stop_index
  })

  for (const o of sorted) {
    const arr = byTruck.get(o.camion)
    if (arr) arr.push(o)
    else byTruck.set(o.camion, [o])
  }

  const rows: OrsVisitRow[] = []

  for (const [camion, stops] of byTruck) {
    const route = routeByCamion.get(camion)
    const durationMin = Number(route?.duration_min) || 0
    const legMin =
      stops.length > 1 ? durationMin / (stops.length - 1) : durationMin > 0 ? durationMin : 0
    let cumulative = 0

    for (const stop of stops) {
      rows.push({
        document_id: stop.document_id,
        stop_index: stop.stop_index,
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

  return rows
}
