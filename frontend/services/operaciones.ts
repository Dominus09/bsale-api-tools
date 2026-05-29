import { getApiBaseUrl } from "@/lib/api-base"
import { getAuthHeaders } from "@/lib/api"

const API_URL = getApiBaseUrl()

export type EstadoConexion = "activo" | "atrasado" | "offline"

export interface GpsActual {
  lat: number | null
  lon: number | null
  updated_at: string | null
}

export interface VendedorOperacionesRow {
  codigo: string
  nombre: string
  activo: boolean
  ruta_id: number | null
  estado_ruta: string | null
  estado_conexion: EstadoConexion
  visitas_realizadas: number
  visitas_pendientes: number
  incidencias: number
  porcentaje_avance: number
  ultima_sync: string | null
  pending_sync_count: number
  bateria_pct: number | null
  gps: GpsActual | null
  kilometros_recorridos: number
  usa_heartbeat?: boolean
  conexion_red?: string | null
}

export interface OperacionesDashboardKpis {
  fecha: string
  total_clientes: number
  clientes_visitados: number
  clientes_pendientes: number
  incidencias: number
  vendedores_activos: number
  vendedores_total: number
  porcentaje_cumplimiento: number
  visitas_pending_sync: number
  ultima_sincronizacion: string | null
  kilometros_recorridos: number
}

export interface OperacionesDashboardResponse {
  kpis: OperacionesDashboardKpis
  vendedores_resumen: VendedorOperacionesRow[]
}

export interface VisitaTimelineItem {
  id: number
  cliente_id: string
  nombre_fantasia: string | null
  direccion: string | null
  comuna: string | null
  orden_ruta: number
  estado: string
  tipo_incidencia: string | null
  observacion: string | null
  foto_url: string | null
  fecha_hora_visita: string | null
  lat_visita: number | null
  lon_visita: number | null
  distancia_metros: number | null
  sync_status: string
}

export interface VendedorDetalleMetricas {
  clientes_asignados: number
  visitados: number
  incidencias: number
  km_recorridos: number
  km_gps: number
  km_ruta_planificada: number
  desviacion_km: number
  primera_visita: string | null
  ultima_visita: string | null
  tiempo_activo_minutos: number | null
  gps_puntos_recibidos: number
  promedio_minutos_entre_visitas: number | null
}

export interface VendedorDetalleResponse {
  codigo: string
  nombre: string
  fecha: string
  ruta_id: number | null
  estado_ruta: string | null
  hora_inicio: string | null
  hora_fin: string | null
  porcentaje_cumplimiento: number
  kilometros_recorridos: number
  estado_conexion: EstadoConexion
  ultima_sync: string | null
  timeline: VisitaTimelineItem[]
  incidencias: VisitaTimelineItem[]
  metricas?: VendedorDetalleMetricas
}

export interface RecorridoPunto {
  orden: number
  tipo: "visita" | "incidencia" | "inicio" | "gps"
  cliente: string | null
  cliente_id: string | null
  visita_id: number | null
  lat: number
  lon: number
  timestamp: string | null
  detalle: string | null
}

export interface IntervaloEntreVisitas {
  desde_cliente: string
  hasta_cliente: string
  desde_ts: string | null
  hasta_ts: string | null
  minutos: number
}

export interface VendedorRecorridoResponse {
  vendedor_id: string
  vendedor_nombre: string | null
  fecha: string
  ruta_id: number | null
  inicio: { lat: number | null; lon: number | null; timestamp: string | null; fuente: string | null } | null
  ultima_posicion: {
    lat: number | null
    lon: number | null
    timestamp: string | null
    fuente: string | null
  } | null
  puntos: RecorridoPunto[]
  linea_gps: { lat: number; lon: number; timestamp: string | null }[]
  linea_heartbeat: { lat: number; lon: number; timestamp: string | null }[]
  km_recorridos: number
  km_gps: number
  km_ruta_planificada: number
  desviacion_km: number
  intervalos_entre_visitas: IntervaloEntreVisitas[]
  promedio_minutos_entre_visitas: number | null
  metricas: VendedorDetalleMetricas
}

export interface MapaGlobalVendedor {
  codigo: string
  nombre: string
  lat: number
  lon: number
  color: string
  estado_conexion: EstadoConexion
  ultima_sync: string | null
  bateria_pct: number | null
  visitas_realizadas: number
  incidencias: number
  km_gps: number
}

export interface MapaGlobalResponse {
  fecha: string
  vendedores: MapaGlobalVendedor[]
}

export function getOperacionesMapaGlobal(fecha?: string) {
  return operacionesFetch<MapaGlobalResponse>("/mapa-global", { fecha })
}

export interface GeorefHistorialRow {
  id: number
  ruta_id: number
  estado_anterior: string | null
  estado_nuevo: string
  lat: number | null
  lon: number | null
  usuario: string | null
  fecha: string
  motivo: string | null
}

export function getOperacionesGeorefHistorial(rutaId: number) {
  return operacionesFetch<{ ruta_id: number; items: GeorefHistorialRow[] }>(
    `/georef-historial/${rutaId}`,
  )
}

export function getOperacionesVendedorRecorrido(codigo: string, fecha?: string) {
  return operacionesFetch<VendedorRecorridoResponse>(
    `/vendedor/${encodeURIComponent(codigo)}/recorrido`,
    { fecha },
  )
}

export interface MarcadorMapa {
  visita_id: number
  cliente_id: string
  nombre_fantasia: string | null
  lat: number
  lon: number
  estado: "visitado" | "pendiente" | "incidencia"
  vendedor: string
  tipo_incidencia: string | null
}

export interface RutaMapaResponse {
  fecha: string
  ruta_id: number
  vendedor: string
  vendedor_nombre: string | null
  marcadores: MarcadorMapa[]
  vendedor_ubicacion: {
    codigo: string
    nombre: string
    lat: number
    lon: number
    estado_conexion: EstadoConexion
    updated_at: string | null
  } | null
}

export interface IncidenciaRow {
  id: number
  ruta_id: number
  vendedor: string
  vendedor_nombre: string | null
  cliente_id: string
  nombre_fantasia: string | null
  comuna: string | null
  tipo_incidencia: string | null
  observacion: string | null
  foto_url: string | null
  tiene_foto?: boolean
  fecha_hora_visita: string | null
  sync_status: string
}

async function operacionesFetch<T>(path: string, params?: Record<string, string | number | undefined>): Promise<T> {
  const q = new URLSearchParams()
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      if (v !== undefined && v !== "") q.set(k, String(v))
    }
  }
  const qs = q.toString()
  const url = `${API_URL}/operaciones${path}${qs ? `?${qs}` : ""}`
  const res = await fetch(url, { headers: getAuthHeaders() })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(text || `HTTP ${res.status}`)
  }
  return res.json() as Promise<T>
}

export function localIsoDate(d = new Date()): string {
  const y = d.getFullYear()
  const m = String(d.getMonth() + 1).padStart(2, "0")
  const day = String(d.getDate()).padStart(2, "0")
  return `${y}-${m}-${day}`
}

export function getOperacionesDashboard(fecha?: string) {
  return operacionesFetch<OperacionesDashboardResponse>("/dashboard", { fecha })
}

export function getOperacionesVendedores(fecha?: string) {
  return operacionesFetch<{ fecha: string; items: VendedorOperacionesRow[] }>("/vendedores", { fecha })
}

export function getOperacionesVendedor(codigo: string, fecha?: string) {
  return operacionesFetch<VendedorDetalleResponse>(`/vendedor/${encodeURIComponent(codigo)}`, { fecha })
}

export function getOperacionesRuta(rutaId: number) {
  return operacionesFetch<RutaMapaResponse>(`/ruta/${rutaId}`)
}

export function getOperacionesIncidencias(opts?: { fecha?: string; vendedor?: string; limit?: number }) {
  return operacionesFetch<{ fecha: string; total: number; items: IncidenciaRow[] }>("/incidencias", {
    fecha: opts?.fecha,
    vendedor: opts?.vendedor,
    limit: opts?.limit,
  })
}

export function getOperacionesMetricas(fecha?: string) {
  return operacionesFetch<{ fecha: string; dashboard: OperacionesDashboardKpis; por_vendedor: VendedorOperacionesRow[] }>(
    "/metricas",
    { fecha },
  )
}

export const OPERACIONES_POLL_MS = Number(process.env.NEXT_PUBLIC_OPERACIONES_POLL_MS || "30000")

export type GeorefEstado = "pendiente" | "capturada" | "aplicada"

export type GeorefFiltroEstado = "todas" | GeorefEstado

export interface ClienteGeorefRow {
  cliente_codigo: string
  cliente_nombre: string
  vendedor_codigo: string
  ruta_id: number
  direccion: string | null
  comuna?: string | null
  lat: number | null
  lon: number | null
  georef_estado: GeorefEstado | string
  georef_actualizada_at?: string | null
  georef_actualizada_por?: string | null
}

export interface GeorefResumen {
  total: number
  pendientes: number
  capturados: number
  aplicados: number
}

export interface GeorefPendientesResponse {
  total: number
  items: ClienteGeorefRow[]
  resumen: GeorefResumen
}

export function getOperacionesGeorefPendientes(opts?: {
  vendedor?: string
  vista?: "erp"
  estado?: GeorefFiltroEstado
}) {
  const q: Record<string, string | undefined> = {
    vendedor: opts?.vendedor,
    vista: opts?.vista,
    estado: opts?.estado ?? "todas",
  }
  return operacionesFetch<GeorefPendientesResponse>("/georef-pendientes", q)
}

export async function downloadOperacionesGeorefExport(opts?: {
  vendedor?: string
  estado?: GeorefFiltroEstado
}) {
  const q = new URLSearchParams()
  if (opts?.vendedor) q.set("vendedor", opts.vendedor)
  q.set("estado", opts?.estado ?? "todas")
  const url = `${API_URL}/operaciones/georef-export?${q}`
  const res = await fetch(url, { headers: getAuthHeaders() })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(text || `HTTP ${res.status}`)
  }
  const blob = await res.blob()
  const href = URL.createObjectURL(blob)
  const a = document.createElement("a")
  a.href = href
  a.download = `georef_${opts?.estado ?? "todas"}.csv`
  a.click()
  URL.revokeObjectURL(href)
}

function parseApiErrorMessage(text: string, status: number): string {
  try {
    const j = JSON.parse(text) as { detail?: string | { msg?: string }[] }
    if (typeof j.detail === "string") return j.detail
    if (Array.isArray(j.detail) && j.detail[0]?.msg) return j.detail[0].msg
  } catch {
    /* texto plano */
  }
  return text.trim() || `HTTP ${status}`
}

export function tieneGeorefEfectiva(row: Pick<ClienteGeorefRow, "lat" | "lon">): boolean {
  if (row.lat == null || row.lon == null) return false
  if (row.lat === 0 && row.lon === 0) return false
  return Number.isFinite(row.lat) && Number.isFinite(row.lon)
}

export function coordsGeorefText(row: Pick<ClienteGeorefRow, "lat" | "lon">): string | null {
  if (!tieneGeorefEfectiva(row)) return null
  return `${row.lat},${row.lon}`
}

export function googleMapsUrl(lat: number, lon: number) {
  return `https://maps.google.com/?q=${lat},${lon}`
}

export async function patchOperacionesGeorefEstado(
  rutaId: number,
  georefEstado: "pendiente" | "aplicada",
) {
  const res = await fetch(`${API_URL}/operaciones/georef-estado`, {
    method: "PATCH",
    headers: {
      ...getAuthHeaders(),
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      ruta_id: rutaId,
      georef_estado: georefEstado,
    }),
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(parseApiErrorMessage(text, res.status))
  }
  return res.json()
}
