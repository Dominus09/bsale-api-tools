import type { PlanificacionStoredOrder } from "@/lib/planificacion-despacho-storage"

export function orderHasGeo(o: PlanificacionStoredOrder): boolean {
  if (o.has_georef === false) return false
  const lat = o.lat
  const lng = o.lng
  return lat != null && lng != null && Number.isFinite(lat) && Number.isFinite(lng)
}

export function splitOrdersByGeo(orders: PlanificacionStoredOrder[]) {
  const routable: PlanificacionStoredOrder[] = []
  const pendingGeoref: PlanificacionStoredOrder[] = []
  for (const o of orders) {
    if (orderHasGeo(o)) routable.push(o)
    else pendingGeoref.push(o)
  }
  return { routable, pendingGeoref }
}
