import { authHeaders, clearSession, isUnauthorized, type AuthMe } from "./auth"
import { getApiBase } from "./api-base"

export class ApiError extends Error {
  status: number
  constructor(status: number, message: string) {
    super(message)
    this.status = status
  }
}

async function parseError(res: Response): Promise<string> {
  const raw = await res.text().catch(() => "")
  try {
    const j = JSON.parse(raw) as { detail?: unknown }
    const d = j.detail
    if (typeof d === "string") return d
    if (d && typeof d === "object" && !Array.isArray(d) && "message" in d) {
      return String((d as { message?: string }).message || raw)
    }
  } catch {
    /* raw */
  }
  return raw.slice(0, 240) || `Error ${res.status}`
}

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${getApiBase()}${path}`, {
    ...init,
    headers: { ...authHeaders(), ...(init?.headers || {}) },
  })
  if (isUnauthorized(res.status)) {
    clearSession()
    if (typeof window !== "undefined") {
      window.location.href = "/login?expired=1"
    }
    throw new ApiError(401, "Sesión expirada")
  }
  if (!res.ok) {
    throw new ApiError(res.status, await parseError(res))
  }
  return res.json() as Promise<T>
}

export type LoginResponse = {
  token: string
  email: string
  role?: string
  id: number
}

export async function login(email: string, password: string): Promise<LoginResponse> {
  const res = await fetch(`${getApiBase()}/login`, {
    method: "POST",
    headers: { Accept: "application/json", "Content-Type": "application/json" },
    body: JSON.stringify({ email, password }),
  })
  if (!res.ok) throw new ApiError(res.status, await parseError(res))
  return res.json()
}

export async function fetchMe(): Promise<AuthMe> {
  return apiFetch<AuthMe>("/auth/me")
}

export type DriverDespacho = {
  id: number
  planning_code?: string | null
  planning_name?: string | null
  planning_date?: string | null
  truck_id?: number | null
  truck_name?: string | null
  truck_plate?: string | null
  route_name?: string | null
  status?: string | null
  driver_user_id?: number | null
  driver_name?: string | null
  confirmed_at?: string | null
  order_count?: number | null
  client_count?: number | null
}

export type LoadGroupRef = {
  load_batch_id?: number | null
  name?: string | null
  sort_order?: number | null
}

export type DriverClientCard = {
  customer_key: string
  client_id?: number | null
  client_name?: string | null
  fantasy_name?: string | null
  city?: string | null
  address?: string | null
  document_count: number
  units_total: number
  load_groups: LoadGroupRef[]
  has_coordinates: boolean
  route_order?: number | null
}

export type DriverProduct = {
  description: string
  name_short?: string
  producto?: string
  variante?: string
  product_id?: number | null
  variant_id?: number | null
  sku_code?: number | null
  codigo_barras?: string | null
  unidades?: number | null
  cajas_equivalentes?: number | null
  units_per_box?: number | null
  sin_unidad_caja?: boolean
  equivalence_status?: "verified" | "pending_validation" | string
  equivalence_label?: string | null
  load_group?: LoadGroupRef | null
}

export type DriverDocument = {
  related_document_id?: number | null
  document_number?: number | null
  document_type?: string | null
  document_total?: number | null
  oc_document_id?: number | null
  oc_number?: number | null
  payment_method?: string | null
  seller_name?: string | null
  units?: number | null
  load_group?: LoadGroupRef | null
  products: DriverProduct[]
  product_count: number
}

export type DriverMapMarker = {
  customer_key: string
  client_id?: number | null
  client_name?: string | null
  address?: string | null
  city?: string | null
  latitude: number
  longitude: number
  coordinates_source?: string | null
  document_count: number
  units_total: number
  load_groups: LoadGroupRef[]
  distance_m?: number | null
  navigation?: {
    google_maps_url?: string
    geo_uri?: string
  }
}

export async function listMyDespachos(): Promise<{
  items: DriverDespacho[]
  count: number
  driver_user_id: number
}> {
  return apiFetch("/distribuidora/pwa/driver/despachos")
}

export async function getMyDespacho(planId: number): Promise<{
  plan: DriverDespacho
  driver_user_id: number
  read_only: boolean
  nav?: { mapa?: boolean; clientes?: boolean; ficha?: boolean; note_es?: string }
  nav_placeholders?: { mapa?: boolean; clientes?: boolean; note_es?: string }
}> {
  return apiFetch(`/distribuidora/pwa/driver/despachos/${planId}`)
}

export async function listMyClientes(
  planId: number,
  q?: string,
): Promise<{
  items: DriverClientCard[]
  count: number
  summary?: {
    clients?: number
    documents?: number
    with_coordinates?: number
    without_coordinates?: number
  }
  picking_ready?: boolean
  picking_reason?: string | null
}> {
  const qs = q?.trim() ? `?q=${encodeURIComponent(q.trim())}` : ""
  return apiFetch(`/distribuidora/pwa/driver/despachos/${planId}/clientes${qs}`)
}

export async function getMyCliente(
  planId: number,
  customerKey: string,
): Promise<{
  customer_key: string
  client: {
    customer_key: string
    client_id?: number | null
    client_name?: string | null
    fantasy_name?: string | null
    rut?: string | null
    seller_name?: string | null
    sellers?: string[]
    address?: string | null
    city?: string | null
    phone?: string | null
    observations?: string | null
    latitude?: number | null
    longitude?: number | null
    has_coordinates?: boolean
    coordinates_source?: string | null
    route_order?: number | null
    load_groups?: LoadGroupRef[]
    facade_photo?: string | null
    facade_photo_placeholder?: boolean
  }
  navigation?: {
    available: boolean
    destination_latitude?: number
    destination_longitude?: number
    destination_label?: string | null
    google_maps_url?: string
    google_maps_place_url?: string
    google_maps_directions_url?: string
    likely_no_road_route?: boolean
    warning_es?: string | null
    note_es?: string
    geo_uri?: string
  }
  documents: DriverDocument[]
  document_count: number
  units_total: number
}> {
  return apiFetch(
    `/distribuidora/pwa/driver/despachos/${planId}/clientes/${encodeURIComponent(customerKey)}`,
  )
}

export async function getMyMapa(
  planId: number,
  origin?: { lat: number; lng: number },
): Promise<{
  markers: DriverMapMarker[]
  count: number
  summary: {
    clients: number
    with_coordinates: number
    without_coordinates: number
  }
  empty_state?: { code?: string; message_es?: string } | null
  device_location?: {
    requested?: boolean
    latitude?: number | null
    longitude?: number | null
    tracking_persistent?: boolean
    note_es?: string
  }
}> {
  const qs =
    origin != null
      ? `?origin_lat=${encodeURIComponent(String(origin.lat))}&origin_lng=${encodeURIComponent(String(origin.lng))}`
      : ""
  return apiFetch(`/distribuidora/pwa/driver/despachos/${planId}/mapa${qs}`)
}
