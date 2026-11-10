import {
  authHeaders,
  clearSession,
  isUnauthorized,
  jsonAuthHeaders,
  type AuthMe,
} from "./auth"
import { multipartAuthHeaders } from "./multipart-headers"
import { getApiBase } from "./api-base"
import { CONNECT_ERROR_ES } from "./api-proxy-target"

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
  if (res.status === 502 || res.status === 503 || res.status === 504) {
    return CONNECT_ERROR_ES
  }
  return raw.slice(0, 240) || `Error ${res.status}`
}

function networkErrorMessage(_err: unknown): string {
  return CONNECT_ERROR_ES
}

async function fetchOnce(url: string, init?: RequestInit): Promise<Response> {
  try {
    return await fetch(url, init)
  } catch (err) {
    throw new ApiError(503, networkErrorMessage(err))
  }
}

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetchOnce(`${getApiBase()}${path}`, {
    ...init,
    headers: { ...jsonAuthHeaders(), ...(init?.headers || {}) },
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
  const res = await fetchOnce(`${getApiBase()}/login`, {
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
  commercial_name?: string
  name_short?: string
  category_name?: string | null
  product_name?: string | null
  variant_name?: string | null
  producto?: string
  variante?: string
  product_id?: number | null
  variant_id?: number | null
  sku_code?: number | null
  codigo_barras?: string | null
  barcode?: string | null
  unidades?: number | null
  cajas_equivalentes?: number | null
  units_per_box?: number | null
  sin_unidad_caja?: boolean
  equivalence_status?: "verified" | "pending_validation" | string
  equivalence_label?: string | null
  load_group?: LoadGroupRef | null
  source_detail_id?: number | null
  line_number?: number | null
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
  line_identity?: "reliable" | "ambiguous_display_only" | "empty" | string
  quantities_registrable?: boolean
  group_assignment_conflict?: {
    code: string
    message_es: string
    oc_document_id?: number
    load_batch_ids?: number[]
  } | null
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
    location_registration?: {
      status?: string
      label_es?: string
      can_register?: boolean
      pending_proposal_id?: number | null
      verified?: boolean
      previous_lat?: number | null
      previous_lng?: number | null
      proposed_lat?: number | null
      proposed_lng?: number | null
    }
    facade?: {
      has_photo?: boolean
      photo_id?: number | null
      status?: string | null
      url?: string | null
      is_current_approved?: boolean
    }
  }
  p3?: {
    tables_ready?: boolean
    r2?: { configured?: boolean; note_es?: string | null }
    gps_warn_accuracy_m?: number
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
  p4?: {
    tables_ready?: boolean
    mode?: string
    note_es?: string
  }
  active_visit?: DeliveryVisit | null
  capabilities?: {
    start_delivery_visit?: boolean
    mode?: string
  }
}> {
  return apiFetch(
    `/distribuidora/pwa/driver/despachos/${planId}/clientes/${encodeURIComponent(customerKey)}`,
  )
}

export type DeliveryVisit = {
  id: number
  company_id: number
  dispatch_plan_id: number
  customer_key: string
  client_id?: number | null
  driver_user_id: number
  status: string
  result_reason?: string | null
  result_note?: string | null
  started_at?: string | null
  started_at_local?: string | null
  ended_at?: string | null
  ended_at_local?: string | null
  start_gps?: {
    lat?: number | null
    lng?: number | null
    accuracy_m?: number | null
    captured_at?: string | null
    source?: string | null
  }
  gps_exception_code?: string | null
  gps_exception_note?: string | null
  version: number
}

export async function startDeliveryVisit(
  planId: number,
  customerKey: string,
  body: {
    latitude?: number | null
    longitude?: number | null
    accuracy_m?: number | null
    gps_captured_at?: string
    gps_source?: string
    gps_exception_code?: string
    gps_exception_note?: string
    idempotency_key?: string
  },
): Promise<{
  visit: DeliveryVisit
  created: boolean
  resumed: boolean
  idempotent_replay?: boolean
  mode?: string
  ui_status_es?: string
  note_es?: string
}> {
  return apiFetch(
    `/distribuidora/pwa/driver/despachos/${planId}/clientes/${encodeURIComponent(customerKey)}/visitas/start`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    },
  )
}

export async function finalizeDeliveryVisit(
  planId: number,
  visitId: number,
  body: {
    expected_version: number
    status: string
    result_reason?: string | null
    result_note?: string | null
    latitude?: number | null
    longitude?: number | null
    accuracy_m?: number | null
    gps_captured_at?: string
    gps_source?: string
    gps_exception_code?: string
    gps_exception_note?: string
  },
): Promise<{
  visit: DeliveryVisit
  finalized: boolean
  idempotent_replay?: boolean
  persisted?: boolean
  closure_review_status?: string
  mode?: string
  note_es?: string
}> {
  return apiFetch(
    `/distribuidora/pwa/driver/despachos/${planId}/visitas/${visitId}/finalize`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    },
  )
}

export async function saveVisitDocumentLines(
  planId: number,
  visitId: number,
  body: {
    related_document_id: number
    oc_document_id?: number | null
    document_number?: number | null
    document_type?: string | null
    document_total?: number | null
    result_status: string
    notes?: string | null
    /** Control de concurrencia P4.1+; omitir solo en clientes legacy. */
    expected_version?: number | null
    lines: Array<{
      source_detail_id: number
      line_number?: number | null
      variant_id?: number | null
      sku_code?: string | null
      product_name?: string | null
      planned_units: number
      delivered_units: number
      undelivered_reason?: string | null
      undelivered_note?: string | null
    }>
  },
): Promise<Record<string, unknown>> {
  return apiFetch(
    `/distribuidora/pwa/driver/despachos/${planId}/visitas/${visitId}/documentos`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    },
  )
}

export async function declareVisitCollection(
  planId: number,
  visitId: number,
  body: {
    document_result_id?: number | null
    method: string
    document_amount?: number | null
    declared_amount?: number | null
    note?: string | null
  },
): Promise<Record<string, unknown>> {
  return apiFetch(
    `/distribuidora/pwa/driver/despachos/${planId}/visitas/${visitId}/cobranza`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    },
  )
}

export async function declareVisitCollectionMixed(
  planId: number,
  visitId: number,
  body: {
    document_result_id: number
    document_amount?: number | null
    payments: Array<{
      method: string
      declared_amount?: number | null
      note?: string | null
    }>
  },
): Promise<Record<string, unknown>> {
  return apiFetch(
    `/distribuidora/pwa/driver/despachos/${planId}/visitas/${visitId}/cobranza/mixta`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    },
  )
}

export type WhatsappReportResponse = {
  report_text: string
  snapshot_id?: number
  template_key?: string
  header?: string
  demo?: boolean
  share: {
    whatsapp_url: string
    delivery_claimed: boolean
    note_es?: string
    auto_send?: boolean
    states?: {
      report_generated?: boolean
      share_requested?: boolean
      delivery_verified?: boolean
    }
  }
  persisted?: boolean
  omitted?: string[]
}

export async function generateVisitWhatsappReport(
  planId: number,
  visitId: number,
  body?: { client_name?: string; planning_code?: string; driver_name?: string },
): Promise<WhatsappReportResponse> {
  return apiFetch(
    `/distribuidora/pwa/driver/despachos/${planId}/visitas/${visitId}/reporte-whatsapp`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {}),
    },
  )
}

export async function getVisitDetail(
  planId: number,
  visitId: number,
): Promise<{
  visit: DeliveryVisit
  documents: Array<Record<string, unknown>>
  lines: Array<Record<string, unknown>>
  collections: Array<Record<string, unknown>>
  evidence: Array<Record<string, unknown>>
  conformity: Record<string, unknown> | null
}> {
  return apiFetch(`/distribuidora/pwa/driver/despachos/${planId}/visitas/${visitId}`)
}

export async function uploadVisitEvidence(
  planId: number,
  visitId: number,
  file: Blob,
  opts: {
    evidence_kind: string
    latitude?: number | null
    longitude?: number | null
    accuracy_m?: number | null
    idempotency_key?: string
  },
): Promise<Record<string, unknown>> {
  const fd = new FormData()
  fd.append("file", file, "evidencia.jpg")
  fd.append("evidence_kind", opts.evidence_kind)
  if (opts.latitude != null) fd.append("latitude", String(opts.latitude))
  if (opts.longitude != null) fd.append("longitude", String(opts.longitude))
  if (opts.accuracy_m != null) fd.append("accuracy_m", String(opts.accuracy_m))
  if (opts.idempotency_key) fd.append("idempotency_key", opts.idempotency_key)
  const res = await fetchOnce(
    `${getApiBase()}/distribuidora/pwa/driver/despachos/${planId}/visitas/${visitId}/evidencia`,
    {
      method: "POST",
      headers: multipartAuthHeaders(authHeaders()),
      body: fd,
    },
  )
  if (isUnauthorized(res.status)) {
    clearSession()
    if (typeof window !== "undefined") window.location.href = "/login?expired=1"
    throw new ApiError(401, "Sesión expirada")
  }
  if (!res.ok) throw new ApiError(res.status, await parseError(res))
  return res.json()
}

export async function registerVisitEvidenceException(
  planId: number,
  visitId: number,
  body: {
    evidence_kind: string
    exception_note: string
    latitude?: number | null
    longitude?: number | null
  },
): Promise<Record<string, unknown>> {
  return apiFetch(
    `/distribuidora/pwa/driver/despachos/${planId}/visitas/${visitId}/evidencia/excepcion`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    },
  )
}

export async function submitVisitConformity(
  planId: number,
  visitId: number,
  opts: {
    receptor_name: string
    refused?: boolean
    refuse_reason?: string
    latitude?: number | null
    longitude?: number | null
    accuracy_m?: number | null
    signatureBlob?: Blob | null
  },
): Promise<Record<string, unknown>> {
  const name = String(opts.receptor_name || "").trim()
  const fd = new FormData()
  fd.append("receptor_name", name)
  fd.append("refused", opts.refused ? "true" : "false")
  if (opts.refuse_reason) fd.append("refuse_reason", opts.refuse_reason)
  if (opts.latitude != null) fd.append("latitude", String(opts.latitude))
  if (opts.longitude != null) fd.append("longitude", String(opts.longitude))
  if (opts.accuracy_m != null) fd.append("accuracy_m", String(opts.accuracy_m))
  if (opts.signatureBlob) fd.append("signature", opts.signatureBlob, "firma.jpg")
  const headers = multipartAuthHeaders(authHeaders())
  // Diagnóstico seguro: sin nombre real ni JWT.
  if (typeof console !== "undefined" && console.info) {
    console.info("[pwa:conformidad]", {
      endpoint: "conformidad",
      name_present: name.length > 0,
      name_len: name.length,
      refused: Boolean(opts.refused),
      has_signature: Boolean(opts.signatureBlob),
      content_type_forced: Boolean(
        headers["Content-Type"] || headers["content-type"],
      ),
    })
  }
  const res = await fetchOnce(
    `${getApiBase()}/distribuidora/pwa/driver/despachos/${planId}/visitas/${visitId}/conformidad`,
    { method: "POST", headers, body: fd },
  )
  if (isUnauthorized(res.status)) {
    clearSession()
    if (typeof window !== "undefined") window.location.href = "/login?expired=1"
    throw new ApiError(401, "Sesión expirada")
  }
  if (!res.ok) {
    if (typeof console !== "undefined" && console.info) {
      console.info("[pwa:conformidad]", {
        http_status: res.status,
        error_kind: "http",
        name_present: name.length > 0,
        name_len: name.length,
      })
    }
    throw new ApiError(res.status, await parseError(res))
  }
  return res.json()
}

export async function listDemoWhatsappReports(): Promise<{
  demo: boolean
  writes_production: boolean
  count: number
  note_es?: string
  scenarios: Array<{
    scenario_key: string
    title_es: string
    report_text: string
    header?: string
    template_key?: string
    demo?: boolean
    share: WhatsappReportResponse["share"]
    writes_production?: boolean
  }>
}> {
  return apiFetch(`/distribuidora/pwa/driver/reportes-whatsapp/demo`)
}

export async function proposeClientLocation(
  planId: number,
  customerKey: string,
  body: {
    latitude: number
    longitude: number
    accuracy_m?: number | null
    captured_at?: string
    confirm_low_accuracy?: boolean
    confirm_significant_move?: boolean
    facade_photo_id?: number | null
    idempotency_key?: string
  },
): Promise<Record<string, unknown>> {
  return apiFetch(
    `/distribuidora/pwa/driver/despachos/${planId}/clientes/${encodeURIComponent(customerKey)}/ubicacion`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    },
  )
}

export async function uploadFacadePhoto(
  planId: number,
  customerKey: string,
  file: Blob,
  opts?: { idempotency_key?: string; contentType?: string },
): Promise<Record<string, unknown>> {
  const fd = new FormData()
  const type = opts?.contentType || (file.type || "image/jpeg")
  fd.append("file", file, "fachada.jpg")
  if (opts?.idempotency_key) fd.append("idempotency_key", opts.idempotency_key)
  const res = await fetchOnce(
    `${getApiBase()}/distribuidora/pwa/driver/despachos/${planId}/clientes/${encodeURIComponent(customerKey)}/foto-fachada`,
    {
      method: "POST",
      headers: multipartAuthHeaders(authHeaders()),
      body: fd,
    },
  )
  if (isUnauthorized(res.status)) {
    clearSession()
    if (typeof window !== "undefined") window.location.href = "/login?expired=1"
    throw new ApiError(401, "Sesión expirada")
  }
  if (!res.ok) throw new ApiError(res.status, await parseError(res))
  return res.json()
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
