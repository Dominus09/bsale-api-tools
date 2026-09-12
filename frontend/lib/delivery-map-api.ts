import { getApiBaseUrl } from "@/lib/api-base"
import { getAuthHeaders } from "@/lib/api"

const API_URL = getApiBaseUrl()

export type DeliveryStopStatus = "pending" | "delivered"

export type DeliveryMapOrder = {
  oc_document_id?: number | null
  oc_number?: number | null
  document_type?: string | null
  payment_method?: string | null
  amount?: number | null
  units?: number | null
  route_order?: number | null
}

export type DeliveryMapStop = {
  customer_key: string
  customer_id: number | null
  customer_name: string
  fantasy_name?: string | null
  address?: string | null
  city?: string | null
  latitude: number | null
  longitude: number | null
  has_coordinates: boolean
  coordinates_source?: string | null
  status: DeliveryStopStatus
  status_updated_at?: string | null
  status_updated_by?: string | null
  orders: DeliveryMapOrder[]
  documents: string[]
  amount: number
  items_count: number
  units_count: number
  search_text?: string
}

export type DeliveryMapPayload = {
  load: {
    id: number
    plan_id: number
    picking_number: string
    status?: string | null
    planning_date?: string | null
    truck_name?: string | null
    route_name?: string | null
    driver_name?: string | null
  }
  summary: {
    orders: number
    customers: number
    with_coordinates: number
    without_coordinates: number
    delivered: number
    pending: number
    coordinates_primary_source?: string
  }
  stops: DeliveryMapStop[]
  source_load_id?: number
}

async function parseJson<T>(res: Response): Promise<T> {
  const data = await res.json().catch(() => ({}))
  if (!res.ok) {
    const detail =
      typeof data?.detail === "string"
        ? data.detail
        : Array.isArray(data?.detail)
          ? data.detail.map((d: { msg?: string }) => d.msg).filter(Boolean).join("; ")
          : `Error ${res.status}`
    throw new Error(detail || `Error ${res.status}`)
  }
  return data as T
}

export async function getDeliveryMapByPlan(
  planId: number,
): Promise<DeliveryMapPayload> {
  const res = await fetch(
    `${API_URL}/distribuidora/dispatch-plans/${planId}/delivery-map`,
    { headers: getAuthHeaders(), cache: "no-store" },
  )
  return parseJson(res)
}

export async function getDeliveryMapByCode(
  planningCode: string,
): Promise<DeliveryMapPayload> {
  const res = await fetch(
    `${API_URL}/distribuidora/dispatch-plans/by-code/${encodeURIComponent(planningCode)}/delivery-map`,
    { headers: getAuthHeaders(), cache: "no-store" },
  )
  return parseJson(res)
}

export async function getDeliveryMapByLoad(
  loadId: number,
): Promise<DeliveryMapPayload> {
  const res = await fetch(`${API_URL}/cargas/${loadId}/delivery-map`, {
    headers: getAuthHeaders(),
    cache: "no-store",
  })
  return parseJson(res)
}

export async function setDeliveryStopStatus(opts: {
  planId: number
  customerKey: string
  status: DeliveryStopStatus
  loadId?: number | null
}): Promise<DeliveryMapPayload> {
  const key = encodeURIComponent(opts.customerKey)
  const url = opts.loadId
    ? `${API_URL}/cargas/${opts.loadId}/delivery-map/stops/${key}/status`
    : `${API_URL}/distribuidora/dispatch-plans/${opts.planId}/delivery-map/stops/${key}/status`
  const res = await fetch(url, {
    method: "POST",
    headers: { ...getAuthHeaders(), "Content-Type": "application/json" },
    body: JSON.stringify({ status: opts.status }),
  })
  return parseJson(res)
}

/** Distancia Haversine en km. */
export function haversineKm(
  lat1: number,
  lon1: number,
  lat2: number,
  lon2: number,
): number {
  const R = 6371
  const toRad = (d: number) => (d * Math.PI) / 180
  const dLat = toRad(lat2 - lat1)
  const dLon = toRad(lon2 - lon1)
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
}

export function googleMapsNavUrl(lat: number, lng: number): string {
  return `https://www.google.com/maps/dir/?api=1&destination=${lat},${lng}&travelmode=driving`
}

export function googleMapsSearchUrl(query: string): string {
  return `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(query)}`
}

export function formatKm(km: number | null | undefined): string {
  if (km == null || !Number.isFinite(km)) return "—"
  if (km < 1) return `${Math.round(km * 1000)} m`
  return `${km.toFixed(1)} km`
}
