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

export interface ClienteGeorefRow {
  cliente_codigo: string
  cliente_nombre: string
  vendedor_codigo: string
  ruta_id: number
  direccion: string | null
  lat: number | null
  lon: number | null
  georef_estado: GeorefEstado | string
  georef_actualizada_at?: string | null
  georef_actualizada_por?: string | null
}

export function getOperacionesGeorefPendientes(opts?: { vendedor?: string; vista?: "erp" }) {
  return operacionesFetch<{ total: number; items: ClienteGeorefRow[] }>("/georef-pendientes", {
    vendedor: opts?.vendedor,
    vista: opts?.vista,
  })
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
    body: JSON.stringify({ ruta_id: rutaId, georef_estado: georefEstado }),
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(text || `HTTP ${res.status}`)
  }
  return res.json()
}
