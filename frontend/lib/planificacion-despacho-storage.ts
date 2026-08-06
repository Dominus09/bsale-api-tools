export const PLANIFICACION_STORAGE_KEY = "distribuidora_planificacion_payload_v1"

export type PlanificacionStoredOrder = {
  document_id: number
  client_id?: number | null
  lat?: number | null
  lng?: number | null
  /** false = pendiente de georreferenciar (no entra en ORS). */
  has_georef?: boolean
  /** Id de ``distribuidora.trucks`` (pre-despacho). */
  truck_id: number
  /** Etiqueta para agrupar rutas (p. ej. ``Hino 3 (5600 kg)``). */
  camion: string
  oc?: number | null
  nombre_fantasia?: string | null
  total_amount?: number | null
  weight_kg?: number | null
  cantidad_cajas?: number | null
  cantidad_unidades?: number | null
  porcentaje_cobertura_peso?: number | null
  productos_sin_peso?: number | null
  peso_total_kg?: number | null
  weight?: {
    value_kg?: number | null
    status?: string | null
    source?: string | null
    reason?: string | null
  } | null
  stop_index: number
  /** Comuna / municipio del cliente. */
  municipality?: string | null
  direccion?: string | null
  /** Observaciones de la OC (texto libre). */
  observaciones?: string | null
  /** Día de entrega detectado (ej. viernes). */
  dia_entrega_label?: string | null
  dia_entrega_detectado?: string | null
}

export type PlanificacionStoredPayload = {
  submittedAt: string
  /** Identificador de sesión para persistir dotación/costos en backend. */
  planSessionId: string
  orders: PlanificacionStoredOrder[]
}

export function createPlanSessionId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID()
  }
  return `plan-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`
}

export function ensurePlanSessionId(payload: PlanificacionStoredPayload): PlanificacionStoredPayload {
  if (payload.planSessionId?.trim().length >= 8) return payload
  return { ...payload, planSessionId: createPlanSessionId() }
}

export function readPlanificacionPayload(): PlanificacionStoredPayload | null {
  if (typeof window === "undefined") return null
  try {
    const raw = sessionStorage.getItem(PLANIFICACION_STORAGE_KEY)
    if (!raw?.trim()) return null
    const data = JSON.parse(raw) as PlanificacionStoredPayload
    if (!data || !Array.isArray(data.orders)) return null
    const normalized = ensurePlanSessionId(data)
    if (!data.planSessionId?.trim()) {
      sessionStorage.setItem(PLANIFICACION_STORAGE_KEY, JSON.stringify(normalized))
    }
    return normalized
  } catch {
    return null
  }
}

export function writePlanificacionPayload(payload: PlanificacionStoredPayload): void {
  if (typeof window === "undefined") return
  sessionStorage.setItem(
    PLANIFICACION_STORAGE_KEY,
    JSON.stringify(ensurePlanSessionId(payload)),
  )
}

export function clearPlanificacionPayload(): void {
  if (typeof window === "undefined") return
  sessionStorage.removeItem(PLANIFICACION_STORAGE_KEY)
}
