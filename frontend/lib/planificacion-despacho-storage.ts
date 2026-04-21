export const PLANIFICACION_STORAGE_KEY = "distribuidora_planificacion_payload_v1"

export type PlanificacionStoredOrder = {
  document_id: number
  client_id?: number | null
  lat: number
  lng: number
  /** Id de ``distribuidora.trucks`` (pre-despacho). */
  truck_id: number
  /** Etiqueta para agrupar rutas (p. ej. ``Hino 3 (5600 kg)``). */
  camion: string
  oc?: number | null
  nombre_fantasia?: string | null
  total_amount?: number | null
  stop_index: number
}

export type PlanificacionStoredPayload = {
  submittedAt: string
  orders: PlanificacionStoredOrder[]
}

export function readPlanificacionPayload(): PlanificacionStoredPayload | null {
  if (typeof window === "undefined") return null
  try {
    const raw = sessionStorage.getItem(PLANIFICACION_STORAGE_KEY)
    if (!raw?.trim()) return null
    const data = JSON.parse(raw) as PlanificacionStoredPayload
    if (!data || !Array.isArray(data.orders)) return null
    return data
  } catch {
    return null
  }
}

export function writePlanificacionPayload(payload: PlanificacionStoredPayload): void {
  if (typeof window === "undefined") return
  sessionStorage.setItem(PLANIFICACION_STORAGE_KEY, JSON.stringify(payload))
}

export function clearPlanificacionPayload(): void {
  if (typeof window === "undefined") return
  sessionStorage.removeItem(PLANIFICACION_STORAGE_KEY)
}
