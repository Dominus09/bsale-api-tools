import type { DistribuidoraPlanningLiveMetrics } from "@/lib/api"
import type { PlanificacionStoredOrder } from "@/lib/planificacion-despacho-storage"

/** Fusiona métricas live de Bsale/ERP sobre órdenes en sesión de planificación. */
export function mergeLiveMetricsIntoPlanOrders(
  orders: PlanificacionStoredOrder[],
  liveItems: DistribuidoraPlanningLiveMetrics[],
): PlanificacionStoredOrder[] {
  const byId = new Map(liveItems.map((r) => [r.document_id, r]))
  return orders.map((o) => {
    const live = byId.get(o.document_id)
    if (!live) return o
    return {
      ...o,
      nombre_fantasia: live.nombre_fantasia ?? o.nombre_fantasia,
      total_amount:
        live.total_amount != null ? Number(live.total_amount) : o.total_amount,
      weight_kg: live.weight_kg != null ? Number(live.weight_kg) : o.weight_kg,
      municipality: live.municipality ?? live.city ?? o.municipality,
      direccion: live.address ?? o.direccion,
      observaciones: live.observaciones ?? o.observaciones,
      dia_entrega_label: live.dia_entrega_label ?? o.dia_entrega_label,
      dia_entrega_detectado: live.dia_entrega_detectado ?? o.dia_entrega_detectado,
      lat: live.lat != null ? Number(live.lat) : o.lat,
      lng: live.lng != null ? Number(live.lng) : o.lng,
      has_georef:
        live.has_georef != null
          ? Boolean(live.has_georef)
          : o.has_georef,
    }
  })
}

export function countBsaleUpdatedPending(
  liveItems: DistribuidoraPlanningLiveMetrics[],
): number {
  return liveItems.filter((r) => r.bsale_updated_pending).length
}
