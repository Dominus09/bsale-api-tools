"use client"

import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ChangeEvent,
  type CSSProperties,
} from "react"
import dynamic from "next/dynamic"
import L from "leaflet"
import { useMap } from "react-leaflet"
import {
  DndContext,
  closestCenter,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
  type DragEndEvent,
  type DragOverEvent,
} from "@dnd-kit/core"
import {
  SortableContext,
  arrayMove,
  sortableKeyboardCoordinates,
  useSortable,
  verticalListSortingStrategy,
} from "@dnd-kit/sortable"
import { CSS } from "@dnd-kit/utilities"
import { Car, GripVertical, Loader2, Lock, Pause, Play, RefreshCw, RotateCcw } from "lucide-react"

import polylineModule from "@mapbox/polyline"

import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  getDistribuidoraMapa,
  getDistribuidoraRutaDetalle,
  getDistribuidoraRutaSugerencias,
  isDistribuidoraRutaDetalleOk,
  postDistribuidoraOptimizarRuta,
  postDistribuidoraOptimizarRutaDesde,
  postDistribuidoraOrdenManualBulk,
  postDistribuidoraOrdenManualReset,
  type DistribuidoraMapaCliente,
  type DistribuidoraPuntoBase,
  type DistribuidoraRutaDetalleJson,
  type DistribuidoraRutaSugerenciaJson,
} from "@/lib/api"

import "leaflet/dist/leaflet.css"
import "react-leaflet-cluster/dist/assets/MarkerCluster.css"
import "react-leaflet-cluster/dist/assets/MarkerCluster.Default.css"

const MapContainer = dynamic(() => import("react-leaflet").then((m) => m.MapContainer), { ssr: false })
const TileLayer = dynamic(() => import("react-leaflet").then((m) => m.TileLayer), { ssr: false })
const Marker = dynamic(() => import("react-leaflet").then((m) => m.Marker), { ssr: false })
const Popup = dynamic(() => import("react-leaflet").then((m) => m.Popup), { ssr: false })
const Tooltip = dynamic(() => import("react-leaflet").then((m) => m.Tooltip), { ssr: false })
const MarkerClusterGroup = dynamic(() => import("react-leaflet-cluster").then((m) => m.default), {
  ssr: false,
})

const MAP_CENTER: [number, number] = [-42.6, -73.8]
const MAP_ZOOM = 10

/** `dia_atencion` desde Bsale como atención telefónica (no entra a rutas / mapa / ORS). */
function esDiaAtencionTelefonico(value: string | null | undefined): boolean {
  return String(value ?? "").trim().toLowerCase() === "telefonico"
}

function esTipoAtencionTelefonicoMapa(c: DistribuidoraMapaCliente): boolean {
  const t = String(c.tipo_atencion ?? "").trim().toLowerCase()
  return t.includes("telefon")
}

function diasCatalogoDesdeMapaResp(data: {
  clientes?: unknown
  dias_atencion?: unknown
}): string[] {
  const set = new Set<string>()
  if (Array.isArray(data.dias_atencion)) {
    for (const x of data.dias_atencion) {
      const d = String(x).trim()
      if (d) set.add(d)
    }
  }
  const arr = Array.isArray(data.clientes) ? (data.clientes as DistribuidoraMapaCliente[]) : []
  for (const c of arr) {
    const d = c.dia_atencion?.trim()
    if (d) set.add(d)
  }
  return Array.from(set).sort((a, b) => a.localeCompare(b, "es"))
}

function formatearMinutos(m: number): string {
  if (!Number.isFinite(m)) return "—"
  return m >= 120 ? `${(m / 60).toFixed(1)} h` : `${Math.round(m)} min`
}

/** Fila de cliente en respuesta ruta-detalle / optimizar-ruta. */
type RutaClienteFila = Record<string, unknown>

/** Voyager: más contraste y calles que light_all (sigue siendo CARTO / OSM). */
const CARTO_VOYAGER_TILES = "https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}.png"

/** Evita franjas grises: Leaflet debe medir el contenedor tras layout, sidebar y carga de tiles. */
function MapaRuteroInvalidateSize() {
  const map = useMap()
  useEffect(() => {
    const fix = () => {
      map.invalidateSize()
    }
    const container = map.getContainer()
    const outer = container.parentElement

    map.whenReady(() => {
      fix()
      window.setTimeout(fix, 0)
    })
    fix()
    const timeouts = [50, 150, 400, 800, 1500].map((ms) => window.setTimeout(fix, ms))
    window.addEventListener("resize", fix)

    const ro =
      typeof ResizeObserver !== "undefined" && outer
        ? new ResizeObserver(() => {
            window.requestAnimationFrame(fix)
          })
        : null
    if (outer && ro) ro.observe(outer)

    return () => {
      timeouts.forEach((id) => window.clearTimeout(id))
      window.removeEventListener("resize", fix)
      ro?.disconnect()
    }
  }, [map])
  return null
}

const FILTER_ALL = "__all__"

/** Clientes en mapa: estado normal / hover / seleccionado. */
const MAP_CLIENTE_COLOR = "#2563eb"
const MAP_CLIENTE_COLOR_HOVER = "#ea580c"
const MAP_CLIENTE_COLOR_HIGHLIGHT = "#16a34a"
/** Cliente esperado por la simulación (llegada en orden; verde). */
const MAP_CLIENTE_COLOR_SIM_VISITA = "#22c55e"

const SIM_DWELL_MS = 2000

type PolylineDecodeFn = (str: string, precision?: number) => [number, number][]

function getPolylineDecode(): PolylineDecodeFn | null {
  const m = polylineModule as unknown
  if (m && typeof m === "object" && "decode" in m && typeof (m as { decode: unknown }).decode === "function") {
    return (m as { decode: PolylineDecodeFn }).decode.bind(m) as PolylineDecodeFn
  }
  const d = (m as { default?: unknown })?.default
  if (d && typeof d === "object" && "decode" in d && typeof (d as { decode: unknown }).decode === "function") {
    return (d as { decode: PolylineDecodeFn }).decode.bind(d) as PolylineDecodeFn
  }
  return null
}

/** ORS suele mandar polyline codificada (precisión 5 o 6). @mapbox/polyline devuelve [lat, lon]. */
function decodeEncodedPolylineToLatLngs(encoded: string): L.LatLngTuple[] {
  const decode = getPolylineDecode()
  if (!decode || !encoded.trim()) return []
  const trimmed = encoded.trim()
  for (const precision of [5, 6] as const) {
    try {
      const decoded = decode(trimmed, precision)
      if (Array.isArray(decoded) && decoded.length >= 2) {
        return decoded.map(([lat, lon]) => [lat, lon] as L.LatLngTuple)
      }
    } catch {
      /* siguiente precisión */
    }
  }
  try {
    const decoded = decode(trimmed)
    return decoded.map(([lat, lon]) => [lat, lon] as L.LatLngTuple)
  } catch {
    return []
  }
}

/** Inserta puntos intermedios para que la simulación no “salte” visitas (umbral 50 m). */
function densificarRutaParaSimulacion(
  map: L.Map,
  pts: L.LatLngTuple[],
  maxSegmentM: number,
): L.LatLngTuple[] {
  if (pts.length < 2) return pts.slice()
  const out: L.LatLngTuple[] = [pts[0]]
  for (let j = 1; j < pts.length; j++) {
    const a = pts[j - 1]
    const b = pts[j]
    const d = map.distance(a, b)
    if (d <= maxSegmentM || d < 0.5) {
      out.push(b)
      continue
    }
    const steps = Math.max(2, Math.ceil(d / maxSegmentM))
    for (let s = 1; s < steps; s++) {
      const t = s / steps
      out.push([a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t] as L.LatLngTuple)
    }
    out.push(b)
  }
  return out
}

function escapeHtmlTexto(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;")
}

function geometryToLatLngs(geometry: unknown): L.LatLngTuple[] {
  if (geometry == null) return []

  if (typeof geometry === "string") {
    return decodeEncodedPolylineToLatLngs(geometry)
  }

  if (typeof geometry === "object" && geometry !== null) {
    const o = geometry as Record<string, unknown>
    if (typeof o.coordinates === "string" && o.coordinates.trim()) {
      return decodeEncodedPolylineToLatLngs(o.coordinates)
    }
    if (o.type === "LineString" && Array.isArray(o.coordinates)) {
      const coords = o.coordinates as [number, number][]
      return coords.map(([lon, lat]) => [lat, lon] as L.LatLngTuple)
    }
  }

  return []
}

declare global {
  interface Window {
    /** Capa de ruta ORS (compatibilidad / depuración; preferir limpiar vía ref en efecto). */
    currentRoute?: L.Polyline
  }
}

/** Comportamiento de cámara al redibujar la polyline ORS (ver ref en el padre). */
type OrsRouteMapBehavior =
  | { kind: "fitBounds" }
  | { kind: "setView"; lat: number; lng: number; zoom: number }

function MapaRuteroRegisterMap({ mapRef }: { mapRef: React.MutableRefObject<L.Map | null> }) {
  const map = useMap()
  useEffect(() => {
    mapRef.current = map
    return () => {
      mapRef.current = null
    }
  }, [map, mapRef])
  return null
}

/** Ruta ORS: polyline azul; fitBounds solo si el padre lo indica (carga / cambio vendedor+día). */
function MapaRuteroOrsRoute({
  detalle,
  viewBehaviorRef,
}: {
  detalle: DistribuidoraRutaDetalleJson | null
  viewBehaviorRef: React.MutableRefObject<OrsRouteMapBehavior>
}) {
  const map = useMap()
  const routeRef = useRef<L.Polyline | null>(null)

  useEffect(() => {
    const removeRoute = () => {
      const layer =
        routeRef.current ?? (typeof window !== "undefined" ? window.currentRoute : undefined)
      if (layer) {
        try {
          map.removeLayer(layer)
        } catch {
          /* capa ya retirada */
        }
      }
      routeRef.current = null
      if (typeof window !== "undefined") {
        window.currentRoute = undefined
      }
    }

    removeRoute()

    if (!detalle || typeof detalle !== "object") {
      return removeRoute
    }
    const hasClientesErr =
      "error" in detalle &&
      detalle.error &&
      Array.isArray(detalle.clientes) &&
      detalle.clientes.length > 0
    if ("error" in detalle && detalle.error && !hasClientesErr) {
      return removeRoute
    }

    const geom = detalle.geometry
    if (geom == null || (typeof geom === "string" && !geom.trim())) {
      return removeRoute
    }

    const latlngs = geometryToLatLngs(geom)
    if (latlngs.length < 2) {
      return removeRoute
    }

    const routeLine = L.polyline(latlngs, {
      color: "#2563eb",
      weight: 5,
      opacity: 0.9,
      lineJoin: "round",
      lineCap: "round",
    }).addTo(map)
    routeRef.current = routeLine
    if (typeof window !== "undefined") {
      window.currentRoute = routeLine
    }

    const behavior = viewBehaviorRef.current
    if (behavior.kind === "fitBounds") {
      map.fitBounds(routeLine.getBounds(), { padding: [48, 48], maxZoom: 14, animate: false })
    } else {
      map.setView([behavior.lat, behavior.lng], behavior.zoom, { animate: false })
    }
    window.setTimeout(() => map.invalidateSize(), 0)

    return removeRoute
  }, [map, detalle, viewBehaviorRef])

  return null
}

function MapaRuteroFlyTo({
  flyTo,
}: {
  flyTo: { lat: number; lon: number; zoom: number } | null
}) {
  const map = useMap()
  useEffect(() => {
    if (!flyTo) return
    map.flyTo([flyTo.lat, flyTo.lon], flyTo.zoom, { duration: 0.45, easeLinearity: 0.25 })
  }, [map, flyTo])
  return null
}

function nombreCliente(c: DistribuidoraMapaCliente): string {
  const fan = c.nombre_fantasia?.trim()
  if (fan) return fan
  const fn = c.first_name?.trim() ?? ""
  const ln = c.last_name?.trim() ?? ""
  const full = `${fn} ${ln}`.trim()
  return full || `Cliente #${c.bsale_id}`
}

const MARKER_PX = 14
const MARKER_BORDER = 2

const clienteIconCache = new Map<string, L.DivIcon>()

function getClienteDivIcon(fillColor: string, opts?: { simDestacado?: boolean }): L.DivIcon {
  const sim = Boolean(opts?.simDestacado)
  const key = `${fillColor}|${sim ? "s" : "n"}`
  let icon = clienteIconCache.get(key)
  if (!icon) {
    const size = sim ? 20 : MARKER_PX
    const b = MARKER_BORDER
    const shadow = sim
      ? "box-shadow:0 0 0 4px rgba(192,38,211,0.55),0 4px 16px rgba(15,23,42,0.4);transform:scale(1.12);"
      : "box-shadow:0 2px 8px rgba(15,23,42,0.28);"
    icon = L.divIcon({
      className: "mapa-rutero-cliente-icon",
      html: `<div style="width:${size}px;height:${size}px;border-radius:9999px;background:${fillColor};border:${b}px solid #ffffff;${shadow}box-sizing:content-box;"></div>`,
      iconSize: [size + b * 2, size + b * 2],
      iconAnchor: [(size + b * 2) / 2, (size + b * 2) / 2],
      popupAnchor: [0, -10],
    })
    clienteIconCache.set(key, icon)
  }
  return icon
}

let simVehiculoIconSingleton: L.DivIcon | null = null
function getSimulacionVehiculoIcon(): L.DivIcon {
  if (!simVehiculoIconSingleton) {
    simVehiculoIconSingleton = L.divIcon({
      className: "mapa-rutero-sim-vehiculo",
      html:
        '<div style="width:38px;height:38px;border-radius:50%;background:linear-gradient(160deg,#0f766e,#14b8a6);border:3px solid #fff;box-shadow:0 4px 14px rgba(0,0,0,.4);display:flex;align-items:center;justify-content:center;font-size:20px;line-height:1;" aria-hidden="true">🚐</div>',
      iconSize: [38, 38],
      iconAnchor: [19, 19],
    })
  }
  return simVehiculoIconSingleton
}

type SimulacionParada = {
  bsale_id: number
  lat: number
  lon: number
  nombre: string
  /** orden_manual (solo representación; no se reordena). */
  orden: number
  /** 1-based según orden de ruta (para “Cliente X de N”). */
  paso: number
  totalParadas: number
  direccionLinea: string
}

function latLngCasiIguales(a: L.LatLngTuple, b: L.LatLngTuple, eps = 1e-5): boolean {
  return Math.abs(a[0] - b[0]) < eps && Math.abs(a[1] - b[1]) < eps
}

/** Terreno operativo: no `dia_atencion = telefonico` (Bsale) ni `tipo_atencion` telefónico. */
function filaRutaEsTerreno(r: RutaClienteFila): boolean {
  const dia = (r as Record<string, unknown>).dia_atencion
  if (esDiaAtencionTelefonico(dia == null ? undefined : String(dia))) return false
  const raw = (r as Record<string, unknown>).tipo_atencion
  const t = String(raw ?? "TERRENO").trim().toUpperCase()
  if (!t) return true
  return t === "TERRENO"
}

function ordenManualSimKey(r: RutaClienteFila): number {
  const om = (r as Record<string, unknown>).orden_manual
  if (om == null || om === "") return Number.POSITIVE_INFINITY
  const n = typeof om === "number" ? om : Number(om)
  return Number.isFinite(n) && n > 0 ? n : Number.POSITIVE_INFINITY
}

/**
 * Recorre base → clientes → base en el orden EXACTO de `waypoints` (p. ej. orden_manual ASC).
 * Cada tramo se densifica en línea recta; al terminar un tramo que llega a un cliente → popup (sin nearest-neighbor).
 */
function MapaRuteroSimulacionVehiculo({
  runId,
  running,
  paused,
  speedMult,
  waypoints,
  stops,
  onVisitClient,
  onComplete,
}: {
  runId: number
  running: boolean
  paused: boolean
  speedMult: number
  waypoints: L.LatLngTuple[] | null
  stops: SimulacionParada[]
  onVisitClient: (bsaleId: number) => void
  onComplete: () => void
}) {
  const map = useMap()
  const pausedRef = useRef(paused)
  const speedRef = useRef(speedMult)
  useEffect(() => {
    pausedRef.current = paused
  }, [paused])
  useEffect(() => {
    speedRef.current = speedMult
  }, [speedMult])

  useEffect(() => {
    if (!running || !waypoints || waypoints.length < 2) {
      return
    }

    const segments: L.LatLngTuple[][] = []
    for (let s = 0; s < waypoints.length - 1; s++) {
      const piece = densificarRutaParaSimulacion(map, [waypoints[s], waypoints[s + 1]], 22)
      if (piece.length >= 2) segments.push(piece)
    }
    if (segments.length === 0) {
      return
    }

    let cancelled = false
    let segIdx = 0
    let subIdx = 0
    let dwellUntil = 0
    let timeoutId: ReturnType<typeof setTimeout> | undefined

    const start = segments[0][0]
    const marker = L.marker(start, {
      icon: getSimulacionVehiculoIcon(),
      zIndexOffset: 2500,
    }).addTo(map)

    const tick = () => {
      if (cancelled) return
      const now = Date.now()
      if (now < dwellUntil) {
        timeoutId = window.setTimeout(tick, 90)
        return
      }
      if (pausedRef.current) {
        timeoutId = window.setTimeout(tick, 120)
        return
      }
      if (segIdx >= segments.length) {
        try {
          if (map.hasLayer(marker)) map.removeLayer(marker)
        } catch {
          /* */
        }
        onComplete()
        return
      }

      const path = segments[segIdx]
      if (subIdx >= path.length) {
        const llegadaIdx = segIdx + 1
        if (llegadaIdx >= 1 && llegadaIdx <= stops.length) {
          const esperado = stops[llegadaIdx - 1]
          onVisitClient(esperado.bsale_id)
          const dir = escapeHtmlTexto(esperado.direccionLinea)
          const nom = escapeHtmlTexto(esperado.nombre)
          L.popup({ maxWidth: 320, className: "mapa-rutero-sim-popup", autoPan: true })
            .setLatLng([esperado.lat, esperado.lon])
            .setContent(
              `<div style="padding:6px;font-size:13px;line-height:1.35;color:#0f172a;">
                <div style="margin-bottom:4px;font-size:11px;font-weight:600;color:#475569">Cliente ${esperado.paso} de ${esperado.totalParadas}</div>
                <div style="font-weight:600">${nom}</div>
                <div style="margin-top:2px;font-size:11px;color:#64748b">Orden manual: ${Number.isFinite(esperado.orden) ? esperado.orden : "—"}</div>
                <div style="margin-top:6px;font-size:11px;color:#334155">${dir}</div>
              </div>`,
            )
            .openOn(map)
          dwellUntil = Date.now() + SIM_DWELL_MS
        }
        segIdx += 1
        if (segIdx >= segments.length) {
          timeoutId = window.setTimeout(tick, 10)
          return
        }
        subIdx = 0
        const prev = path[path.length - 1]
        const nextPath = segments[segIdx]
        if (nextPath.length && latLngCasiIguales(prev, nextPath[0])) {
          subIdx = 1
        }
        timeoutId = window.setTimeout(tick, 10)
        return
      }

      marker.setLatLng(path[subIdx])
      subIdx += 1
      const baseDelay = 50 / Math.max(1, speedRef.current)
      const delay = Math.max(10, Math.round(baseDelay))
      timeoutId = window.setTimeout(tick, delay)
    }

    timeoutId = window.setTimeout(tick, 40)

    return () => {
      cancelled = true
      if (timeoutId !== undefined) window.clearTimeout(timeoutId)
      try {
        if (map.hasLayer(marker)) map.removeLayer(marker)
      } catch {
        /* */
      }
    }
  }, [runId, running, map, waypoints, stops, onVisitClient, onComplete])

  return null
}

/** Ruta absoluta desde `public/` (Next.js); evita imágenes rotas por emoji o rutas relativas. */
const BASE_MARKER_ICON_URL = "/icons/base.png"

let baseIconSingleton: L.Icon | null = null
function getBaseMapIcon(): L.Icon {
  if (!baseIconSingleton) {
    baseIconSingleton = L.icon({
      iconUrl: BASE_MARKER_ICON_URL,
      iconSize: [32, 32],
      iconAnchor: [16, 32],
      popupAnchor: [0, -28],
      className: "mapa-rutero-base-leaflet-icon",
    })
  }
  return baseIconSingleton
}

/** Base en mapa de ruta: mismo asset, tamaño mayor para inicio/fin. */
let basePuntoRutaIcon: L.Icon | null = null
function getBasePuntoRutaMapIcon(): L.Icon {
  if (!basePuntoRutaIcon) {
    basePuntoRutaIcon = L.icon({
      iconUrl: BASE_MARKER_ICON_URL,
      iconSize: [40, 40],
      iconAnchor: [20, 40],
      popupAnchor: [0, -40],
      className: "mapa-rutero-base-leaflet-icon mapa-rutero-base-ruta-leaflet-icon",
    })
  }
  return basePuntoRutaIcon
}

const SELECT_CLASS =
  "h-9 min-w-[140px] rounded-md border border-input bg-background px-3 text-sm shadow-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"

function ordenManualDisplay(c: DistribuidoraMapaCliente): number | null {
  const v = c.orden_manual
  if (v == null) return null
  const n = typeof v === "number" ? v : Number(v)
  return Number.isFinite(n) ? n : null
}

function rutaClientesOrdenados(rutaDetalle: DistribuidoraRutaDetalleJson | null): RutaClienteFila[] {
  if (!rutaDetalle || typeof rutaDetalle !== "object") return []
  const arr = rutaDetalle.clientes
  if (!Array.isArray(arr)) return []
  const out = arr.filter((x): x is RutaClienteFila => x != null && typeof x === "object")
  if (out.length > 0) return out
  if ("error" in rutaDetalle && rutaDetalle.error) return []
  return out
}

function ordenMostradoEnMapa(c: DistribuidoraMapaCliente, rutaDetalle: DistribuidoraRutaDetalleJson | null): number | null {
  const m = ordenManualDisplay(c)
  if (m != null) return m
  for (const row of rutaClientesOrdenados(rutaDetalle)) {
    if (Number(row.bsale_id) !== c.bsale_id) continue
    const ov = row.orden_visita
    if (typeof ov === "number" && Number.isFinite(ov)) return ov
    if (typeof ov === "string" && ov.trim()) {
      const n = Number(ov)
      return Number.isFinite(n) ? n : null
    }
    return null
  }
  return null
}

function clienteEnRutaActual(c: DistribuidoraMapaCliente, rutaDetalle: DistribuidoraRutaDetalleJson | null): boolean {
  return rutaClientesOrdenados(rutaDetalle).some((row) => Number(row.bsale_id) === c.bsale_id)
}

function captureMapViewBeforeRutaUpdate(
  mapRef: React.MutableRefObject<L.Map | null>,
  viewBehaviorRef: React.MutableRefObject<OrsRouteMapBehavior>,
) {
  const m = mapRef.current
  if (!m) {
    viewBehaviorRef.current = { kind: "fitBounds" }
    return
  }
  const c = m.getCenter()
  viewBehaviorRef.current = {
    kind: "setView",
    lat: c.lat,
    lng: c.lng,
    zoom: m.getZoom(),
  }
}

function reordenarParaBulk(
  clientesRuta: RutaClienteFila[],
  bsaleId: number,
  nuevaPosicion1Based: number,
): { id: number; orden_manual: number }[] {
  const sorted = [...clientesRuta].sort(
    (a, b) => Number(a.orden_visita ?? 0) - Number(b.orden_visita ?? 0),
  )
  const idx = sorted.findIndex((row) => Number(row.bsale_id) === bsaleId)
  if (idx < 0) {
    throw new Error("Cliente no está en la ruta actual")
  }
  const max = sorted.length
  const p = Math.min(Math.max(1, Math.floor(nuevaPosicion1Based)), max)
  const [row] = sorted.splice(idx, 1)
  sorted.splice(p - 1, 0, row)
  return sorted.map((r, i) => ({
    id: Number(r.bsale_id),
    orden_manual: i + 1,
  }))
}

/** Intercambia dos visitas consecutivas y devuelve payload para orden-manual-bulk. */
function bulkPorSwapAdyacente(rows: RutaClienteFila[], indiceA: number): { id: number; orden_manual: number }[] {
  const sorted = [...rows].sort((a, b) => Number(a.orden_visita ?? 0) - Number(b.orden_visita ?? 0))
  if (indiceA < 0 || indiceA >= sorted.length - 1) {
    throw new Error("Intercambio no válido para la ruta actual")
  }
  const sw = [...sorted]
  const t = sw[indiceA]
  sw[indiceA] = sw[indiceA + 1]!
  sw[indiceA + 1] = t
  return sw.map((r, i) => ({
    id: Number(r.bsale_id),
    orden_manual: i + 1,
  }))
}

function nombreClienteDesdeFilaRuta(row: RutaClienteFila): string {
  const fan = String(row.nombre_fantasia ?? "").trim()
  if (fan) return fan
  const fn = String(row.first_name ?? "").trim()
  const ln = String(row.last_name ?? "").trim()
  const full = `${fn} ${ln}`.trim()
  const id = Number(row.bsale_id)
  return full || (Number.isFinite(id) ? `Cliente #${id}` : "Cliente")
}

function municipioDesdeFilaRuta(row: RutaClienteFila): string {
  return String(row.municipality ?? "").trim() || "—"
}

function baseCoordsParaMapa(
  rutaDetalle: DistribuidoraRutaDetalleJson | null,
  bases: DistribuidoraPuntoBase[],
  vendedorFilter: string,
): { nombre: string; lat: number; lon: number } | null {
  if (rutaDetalle && typeof rutaDetalle === "object" && !("error" in rutaDetalle && rutaDetalle.error)) {
    const b = rutaDetalle.base as Record<string, unknown> | undefined
    if (b) {
      const lat = Number(b.lat)
      const lon = Number(b.lon)
      if (Number.isFinite(lat) && Number.isFinite(lon)) {
        const nombre = String(b.nombre ?? "").trim() || "Base"
        return { nombre, lat, lon }
      }
    }
  }
  const f = bases.find((x) => x.vendedor?.trim() === vendedorFilter)
  if (f && Number.isFinite(f.lat) && Number.isFinite(f.lon)) {
    return { nombre: f.nombre?.trim() || "Base", lat: f.lat, lon: f.lon }
  }
  return null
}

/** Si no hay polyline ORS, centra el mapa en clientes del día + base para que nunca quede vacío. */
function MapaRuteroFitBoundsClientes({
  rutaDetalle,
  clientesVisibles,
  bases,
  vendedorFilter,
  diaFilter,
  activo,
}: {
  rutaDetalle: DistribuidoraRutaDetalleJson | null
  clientesVisibles: DistribuidoraMapaCliente[]
  bases: DistribuidoraPuntoBase[]
  vendedorFilter: string
  diaFilter: string
  activo: boolean
}) {
  const map = useMap()
  useEffect(() => {
    if (!activo || vendedorFilter === FILTER_ALL || diaFilter === FILTER_ALL) return
    if (!rutaDetalle || typeof rutaDetalle !== "object") return
    const errOnly = "error" in rutaDetalle && rutaDetalle.error && !rutaClientesOrdenados(rutaDetalle).length
    if (errOnly) return

    const g = rutaDetalle.geometry
    const latlngsFromGeom = geometryToLatLngs(g)
    if (latlngsFromGeom.length >= 2) return

    const pts: L.LatLngTuple[] = []
    for (const c of clientesVisibles) {
      if (Number.isFinite(c.lat) && Number.isFinite(c.lon)) pts.push([c.lat, c.lon])
    }
    const bc = baseCoordsParaMapa(rutaDetalle, bases, vendedorFilter)
    if (bc && Number.isFinite(bc.lat) && Number.isFinite(bc.lon)) pts.push([bc.lat, bc.lon])
    if (pts.length === 0) return

    const t = window.setTimeout(() => {
      try {
        map.fitBounds(L.latLngBounds(pts), { padding: [48, 48], maxZoom: 14, animate: false })
        map.invalidateSize()
      } catch {
        /* bounds inválidos */
      }
    }, 150)
    return () => window.clearTimeout(t)
  }, [map, activo, rutaDetalle, clientesVisibles, bases, vendedorFilter, diaFilter])
  return null
}

function RutaClienteSortableFila({
  row,
  filaIndex,
  highlightBsaleId,
  hoverBsaleId,
  dragActiveId,
  dragOverId,
  onHoverLista,
  onSelectCliente,
  onReoptimizarDesde,
  onBloquearHasta,
  totalFilas,
  setRowRef,
  disabled,
}: {
  row: RutaClienteFila
  filaIndex: number
  highlightBsaleId: number | null
  hoverBsaleId: number | null
  dragActiveId: string | null
  dragOverId: string | null
  onHoverLista: (bsaleId: number | null) => void
  onSelectCliente: (row: RutaClienteFila) => void
  onReoptimizarDesde?: (desdeIndice: number) => void
  /** Fija visitas 0..filaIndex inclusive; optimiza solo la cola (índice servidor = filaIndex + 1). */
  onBloquearHasta?: (filaIndex: number) => void
  totalFilas: number
  setRowRef: (bsaleId: number, el: HTMLElement | null) => void
  disabled: boolean
}) {
  const bid = Number(row.bsale_id)
  const id = String(bid)
  const ord = Number(row.orden_visita ?? 0)
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({
    id,
    disabled,
  })
  const style: CSSProperties = {
    transform: CSS.Transform.toString(transform),
    transition: transition ?? undefined,
  }
  const active = highlightBsaleId === bid
  const ho = hoverBsaleId === bid
  const isDropTarget = dragOverId === id && dragActiveId != null && dragActiveId !== id
  const puedeBloquearHasta =
    Boolean(onBloquearHasta) && totalFilas >= 2 && filaIndex < totalFilas - 1

  return (
    <li
      ref={(node) => {
        setNodeRef(node)
        setRowRef(bid, node)
      }}
      style={style}
      className={cn(
        "list-none rounded-md border border-transparent transition-[box-shadow,transform,opacity,background-color] duration-200 ease-out",
        (active || ho) && !isDragging && "border-primary/50 bg-primary/8 ring-1 ring-primary/35",
        isDragging && "z-[5] scale-[1.02] opacity-95 shadow-lg ring-2 ring-primary/50",
        isDropTarget && "border-primary/55 bg-primary/12 ring-2 ring-primary/40",
      )}
    >
      <div className="flex items-stretch gap-0.5">
        <button
          type="button"
          className={cn(
            "text-muted-foreground hover:text-foreground flex shrink-0 cursor-grab touch-none items-center rounded-l-md px-1.5 py-2 active:cursor-grabbing",
            disabled && "pointer-events-none opacity-40",
          )}
          aria-label="Arrastrar para reordenar visita"
          {...attributes}
          {...listeners}
        >
          <GripVertical className="size-4" aria-hidden />
        </button>
        <button
          type="button"
          className="min-w-0 flex-1 px-1 py-2 text-left transition-colors"
          onClick={() => onSelectCliente(row)}
          onMouseEnter={() => onHoverLista(bid)}
          onMouseLeave={() => onHoverLista(null)}
        >
          <span className="inline-flex min-w-[1.5rem] font-semibold tabular-nums text-primary">{ord}.</span>
          <span className="mt-0.5 block font-medium leading-snug text-foreground">
            {nombreClienteDesdeFilaRuta(row)}
          </span>
          <span className="text-xs text-muted-foreground">{municipioDesdeFilaRuta(row)}</span>
        </button>
        <div className="flex shrink-0 flex-col justify-stretch overflow-hidden rounded-r-md border-l border-border/60">
          {onBloquearHasta && puedeBloquearHasta ? (
            <button
              type="button"
              className={cn(
                "text-muted-foreground hover:text-primary hover:bg-primary/10 flex flex-1 items-center justify-center px-1.5 py-1.5 transition-colors",
                disabled && "pointer-events-none opacity-40",
              )}
              title="Bloquear hasta aquí: las visitas hasta esta fila no se mueven; solo se reordena el resto"
              aria-label="Bloquear hasta aquí"
              onClick={(e) => {
                e.stopPropagation()
                onBloquearHasta(filaIndex)
              }}
            >
              <Lock className="size-3.5" aria-hidden />
            </button>
          ) : null}
          {onReoptimizarDesde ? (
            <button
              type="button"
              className={cn(
                "text-muted-foreground hover:text-primary hover:bg-primary/10 flex flex-1 items-center justify-center px-1.5 py-1.5 transition-colors",
                disabled && "pointer-events-none opacity-40",
              )}
              title="Reoptimizar desde aquí: lo anterior queda igual; se recalcula el orden del tramo con el optimizador local y se traza con ORS"
              aria-label="Reoptimizar desde aquí"
              onClick={(e) => {
                e.stopPropagation()
                onReoptimizarDesde(filaIndex)
              }}
            >
              <RefreshCw className="size-3.5" aria-hidden />
            </button>
          ) : null}
        </div>
      </div>
    </li>
  )
}

function MapaRuteroPanelSugerencias({
  sugerencias,
  ignoradas,
  loading,
  error,
  ordenGuardando,
  onIgnorar,
  onAplicar,
}: {
  sugerencias: DistribuidoraRutaSugerenciaJson[]
  ignoradas: string[]
  loading: boolean
  error: string
  ordenGuardando: boolean
  onIgnorar: (id: string) => void
  onAplicar: (s: DistribuidoraRutaSugerenciaJson) => void
}) {
  const visibles = sugerencias.filter((s) => !ignoradas.includes(s.id))
  return (
    <div className="border-t border-border px-2 py-2">
      <h3 className="mb-1.5 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
        Sugerencias locales
      </h3>
      <p className="mb-2 text-[11px] leading-snug text-muted-foreground">
        Propuestas puntuales (swap de visitas vecinas). No se aplica nada sola: elige aplicar o ignorar.
      </p>
      {error ? <p className="text-xs text-destructive">{error}</p> : null}
      {loading ? (
        <p className="flex items-center gap-2 text-xs text-muted-foreground">
          <Loader2 className="h-3.5 w-3.5 shrink-0 animate-spin" />
          Analizando…
        </p>
      ) : null}
      {!loading && !error && visibles.length === 0 ? (
        <p className="text-xs text-muted-foreground">Sin sugerencias por encima del umbral (0,5 km Haversine local).</p>
      ) : null}
      <ul className="max-h-36 space-y-2 overflow-y-auto">
        {visibles.map((s) => (
          <li
            key={s.id}
            className="rounded-md border border-border/80 bg-muted/25 px-2 py-1.5 text-[11px] leading-snug text-foreground"
          >
            <p className="text-foreground/95">{s.mensaje}</p>
            <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
              <span className="rounded bg-primary/12 px-1.5 py-0.5 font-medium tabular-nums text-primary">
                Δ ≈ {s.delta_km} km
              </span>
              <Button
                type="button"
                variant="outline"
                size="sm"
                className="h-7 text-[11px]"
                disabled={ordenGuardando}
                onClick={() => onIgnorar(s.id)}
              >
                Ignorar
              </Button>
              <Button
                type="button"
                variant="default"
                size="sm"
                className="h-7 text-[11px]"
                disabled={ordenGuardando}
                onClick={() => void onAplicar(s)}
              >
                Aplicar
              </Button>
            </div>
          </li>
        ))}
      </ul>
    </div>
  )
}

function MapaRuteroPanelRuta({
  rutaDetalle,
  loading,
  highlightBsaleId,
  hoverBsaleId,
  onHoverLista,
  onSelectCliente,
  onReoptimizarDesde,
  onBloquearHasta,
  setListItemRef,
  onOrdenCambiado,
  ordenGuardando,
  sugerencias,
  sugerenciasIgnoradas,
  sugerenciasLoading,
  sugerenciasError,
  onSugerenciaIgnorar,
  onSugerenciaAplicar,
}: {
  rutaDetalle: DistribuidoraRutaDetalleJson | null
  loading: boolean
  highlightBsaleId: number | null
  hoverBsaleId: number | null
  onHoverLista: (bsaleId: number | null) => void
  onSelectCliente: (row: RutaClienteFila) => void
  onReoptimizarDesde?: (desdeIndice: number) => void
  onBloquearHasta?: (filaIndex: number) => void
  setListItemRef: (bsaleId: number, el: HTMLElement | null) => void
  onOrdenCambiado: (bulk: { id: number; orden_manual: number }[]) => Promise<void>
  ordenGuardando: boolean
  sugerencias: DistribuidoraRutaSugerenciaJson[]
  sugerenciasIgnoradas: string[]
  sugerenciasLoading: boolean
  sugerenciasError: string
  onSugerenciaIgnorar: (id: string) => void
  onSugerenciaAplicar: (s: DistribuidoraRutaSugerenciaJson) => void
}) {
  const [items, setItems] = useState<RutaClienteFila[]>([])
  const [dragActiveId, setDragActiveId] = useState<string | null>(null)
  const [dragOverId, setDragOverId] = useState<string | null>(null)

  const sensors = useSensors(
    useSensor(PointerSensor, {
      activationConstraint: { distance: 6 },
    }),
    useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates }),
  )

  const baseNombre = useMemo(() => {
    if (!rutaDetalle || typeof rutaDetalle !== "object") return "Base"
    if ("error" in rutaDetalle && rutaDetalle.error) return "Base"
    const b = rutaDetalle.base as Record<string, unknown> | undefined
    const n = b?.nombre
    return typeof n === "string" && n.trim() ? n.trim() : "Base"
  }, [rutaDetalle])

  useEffect(() => {
    const raw = rutaClientesOrdenados(rutaDetalle)
    const sorted = [...raw].sort((a, b) => Number(a.orden_visita ?? 0) - Number(b.orden_visita ?? 0))
    setItems(sorted)
  }, [rutaDetalle])

  const errMsg =
    rutaDetalle && typeof rutaDetalle === "object" && "error" in rutaDetalle && rutaDetalle.error
      ? String(rutaDetalle.error)
      : null

  const ok = rutaDetalle != null && isDistribuidoraRutaDetalleOk(rutaDetalle)

  const nClientes = ok && Array.isArray(rutaDetalle.clientes) ? rutaDetalle.clientes.length : 0
  const km = ok ? Number(rutaDetalle.km_totales) : 0
  const min = ok ? Number(rutaDetalle.minutos_totales) : 0
  const minCond =
    ok && typeof rutaDetalle.minutos_conduccion === "number"
      ? Number(rutaDetalle.minutos_conduccion)
      : null
  const minAt =
    ok && typeof rutaDetalle.minutos_atencion === "number" ? Number(rutaDetalle.minutos_atencion) : null
  const minTotalReal =
    ok && typeof rutaDetalle.minutos_total_real === "number"
      ? Number(rutaDetalle.minutos_total_real)
      : null
  const tCli =
    ok && typeof rutaDetalle.tiempo_por_cliente_min === "number"
      ? Number(rutaDetalle.tiempo_por_cliente_min)
      : null

  const sortableIds = useMemo(() => items.map((r) => String(r.bsale_id)), [items])

  const handleDragEnd = useCallback(
    async (event: DragEndEvent) => {
      const { active, over } = event
      setDragActiveId(null)
      setDragOverId(null)
      if (ordenGuardando || !over || active.id === over.id) return
      const oldIndex = items.findIndex((r) => String(r.bsale_id) === active.id)
      const newIndex = items.findIndex((r) => String(r.bsale_id) === over.id)
      if (oldIndex < 0 || newIndex < 0) return
      const snapshot = items
      const reordered: RutaClienteFila[] = arrayMove(items, oldIndex, newIndex).map((row, i) => ({
        ...row,
        orden_visita: i + 1,
      }))
      setItems(reordered)
      const bulk = reordered.map((row, i) => ({
        id: Number(row.bsale_id),
        orden_manual: i + 1,
      }))
      try {
        await onOrdenCambiado(bulk)
      } catch {
        setItems(snapshot)
      }
    },
    [items, onOrdenCambiado, ordenGuardando],
  )

  return (
    <aside
      className="flex w-full max-h-[75vh] shrink-0 flex-col overflow-hidden rounded-lg border border-border bg-card text-card-foreground shadow-sm lg:w-80"
      aria-label="Ruta del día: inicio, visitas y fin"
    >
      <div className="border-b border-border px-3 py-2">
        <h2 className="text-sm font-semibold tracking-tight">Ruta del día</h2>
        <p className="text-xs text-muted-foreground">
          Arrastra con ⋮⋮; clic en el nombre centra el mapa. Candado: fija el prefijo de la ruta y solo reordena
          lo que sigue. Flechas: reoptimiza la cola desde esa visita (orden local + trazado ORS).
        </p>
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto px-2 py-2">
        {loading ? (
          <p className="flex items-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 shrink-0 animate-spin" />
            Cargando ruta…
          </p>
        ) : errMsg && items.length === 0 ? (
          <p className="text-sm text-destructive">{errMsg}</p>
        ) : (
          <div className="space-y-2 text-sm">
            {errMsg && items.length > 0 ? (
              <p className="rounded-md border border-destructive/30 bg-destructive/10 px-2 py-1.5 text-xs text-destructive">
                {errMsg}
              </p>
            ) : null}
            <div className="rounded-md bg-emerald-600/12 px-2 py-1.5 text-emerald-900 dark:bg-emerald-500/15 dark:text-emerald-100">
              <span aria-hidden>🟢 </span>
              <span className="font-medium">Inicio:</span> {baseNombre}
            </div>
            {items.length === 0 ? (
              <p className="text-muted-foreground">No hay clientes en terreno para este día.</p>
            ) : (
              <DndContext
                sensors={sensors}
                collisionDetection={closestCenter}
                onDragStart={(e) => setDragActiveId(String(e.active.id))}
                onDragOver={(e: DragOverEvent) => setDragOverId(e.over ? String(e.over.id) : null)}
                onDragEnd={(e) => void handleDragEnd(e)}
                onDragCancel={() => {
                  setDragActiveId(null)
                  setDragOverId(null)
                }}
              >
                <SortableContext items={sortableIds} strategy={verticalListSortingStrategy}>
                  <ul className="space-y-1">
                    {items.map((row, filaIndex) => (
                      <RutaClienteSortableFila
                        key={String(row.bsale_id)}
                        row={row}
                        filaIndex={filaIndex}
                        highlightBsaleId={highlightBsaleId}
                        hoverBsaleId={hoverBsaleId}
                        dragActiveId={dragActiveId}
                        dragOverId={dragOverId}
                        onHoverLista={onHoverLista}
                        onSelectCliente={onSelectCliente}
                        onReoptimizarDesde={onReoptimizarDesde}
                        onBloquearHasta={onBloquearHasta}
                        totalFilas={items.length}
                        setRowRef={setListItemRef}
                        disabled={ordenGuardando}
                      />
                    ))}
                  </ul>
                </SortableContext>
              </DndContext>
            )}
            <div className="rounded-md bg-red-600/12 px-2 py-1.5 text-red-900 dark:bg-red-500/15 dark:text-red-100">
              <span aria-hidden>🔴 </span>
              <span className="font-medium">Fin:</span> {baseNombre}
            </div>
          </div>
        )}
      </div>
      {ok && items.length >= 2 ? (
        <MapaRuteroPanelSugerencias
          sugerencias={sugerencias}
          ignoradas={sugerenciasIgnoradas}
          loading={sugerenciasLoading}
          error={sugerenciasError}
          ordenGuardando={ordenGuardando}
          onIgnorar={onSugerenciaIgnorar}
          onAplicar={onSugerenciaAplicar}
        />
      ) : null}
      {ok ? (
        <div className="space-y-1 border-t border-border px-3 py-2 text-xs text-muted-foreground">
          <div className="flex justify-between gap-2">
            <span>Clientes en ruta</span>
            <span className="font-medium tabular-nums text-foreground">{nClientes}</span>
          </div>
          <div className="flex justify-between gap-2">
            <span>Km totales</span>
            <span className="font-medium tabular-nums text-foreground">{km.toFixed(1)} km</span>
          </div>
          {minCond != null && minAt != null && minTotalReal != null ? (
            <>
              <div className="flex justify-between gap-2">
                <span>Conducción (ORS)</span>
                <span className="font-medium tabular-nums text-foreground">{formatearMinutos(minCond)}</span>
              </div>
              <div className="flex justify-between gap-2">
                <span>
                  Atención
                  {tCli != null ? (
                    <span className="text-muted-foreground/80"> ({nClientes}×{tCli} min)</span>
                  ) : null}
                </span>
                <span className="font-medium tabular-nums text-foreground">{formatearMinutos(minAt)}</span>
              </div>
              <div className="flex justify-between gap-2 border-t border-border/60 pt-1 font-medium text-foreground">
                <span>Tiempo total real</span>
                <span className="tabular-nums">{formatearMinutos(minTotalReal)}</span>
              </div>
            </>
          ) : (
            <div className="flex justify-between gap-2">
              <span>Tiempo conducción</span>
              <span className="font-medium tabular-nums text-foreground">{formatearMinutos(min)}</span>
            </div>
          )}
        </div>
      ) : null}
    </aside>
  )
}

export default function MapaRuteroClient() {
  const [clientes, setClientes] = useState<DistribuidoraMapaCliente[]>([])
  const [diasAtencionOpciones, setDiasAtencionOpciones] = useState<string[]>([])
  const [bases, setBases] = useState<DistribuidoraPuntoBase[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState("")
  const [vendedorFilter, setVendedorFilter] = useState(FILTER_ALL)
  const [diaFilter, setDiaFilter] = useState(FILTER_ALL)
  const [rutaDetalle, setRutaDetalle] = useState<DistribuidoraRutaDetalleJson | null>(null)
  const [rutaDetalleLoading, setRutaDetalleLoading] = useState(false)
  const [ordenGuardando, setOrdenGuardando] = useState(false)
  const [ordenMensaje, setOrdenMensaje] = useState("")
  const [moverCliente, setMoverCliente] = useState<DistribuidoraMapaCliente | null>(null)
  const [nuevaPosicion, setNuevaPosicion] = useState("1")
  const mounted = useRef(true)
  const mapRef = useRef<L.Map | null>(null)
  const orsRouteViewRef = useRef<OrsRouteMapBehavior>({ kind: "fitBounds" })
  const listItemRefs = useRef<Map<number, HTMLElement>>(new Map())
  const [flyTo, setFlyTo] = useState<{ lat: number; lon: number; zoom: number } | null>(null)
  const [highlightBsaleId, setHighlightBsaleId] = useState<number | null>(null)
  const [hoverBsaleId, setHoverBsaleId] = useState<number | null>(null)
  const [rutaSugerencias, setRutaSugerencias] = useState<DistribuidoraRutaSugerenciaJson[]>([])
  const [rutaSugerenciasLoading, setRutaSugerenciasLoading] = useState(false)
  const [rutaSugerenciasError, setRutaSugerenciasError] = useState("")
  const [sugerenciasIgnoradas, setSugerenciasIgnoradas] = useState<string[]>([])
  const [simRunning, setSimRunning] = useState(false)
  const [simPaused, setSimPaused] = useState(false)
  const [simSpeedMult, setSimSpeedMult] = useState(1)
  const [simRunId, setSimRunId] = useState(0)
  const [simPath, setSimPath] = useState<L.LatLngTuple[] | null>(null)
  const [simStops, setSimStops] = useState<SimulacionParada[]>([])
  const [simAvisoSinOrden, setSimAvisoSinOrden] = useState(false)
  const [simHighlightBsaleId, setSimHighlightBsaleId] = useState<number | null>(null)

  const setListItemRef = useCallback((bid: number, el: HTMLElement | null) => {
    if (el) listItemRefs.current.set(bid, el)
    else listItemRefs.current.delete(bid)
  }, [])

  const onSelectClienteLista = useCallback((row: RutaClienteFila) => {
    const lat = Number(row.lat)
    const lon = Number(row.lon)
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return
    const bid = Number(row.bsale_id)
    setHighlightBsaleId(Number.isFinite(bid) ? bid : null)
    setFlyTo({ lat, lon, zoom: 16 })
  }, [])

  const focusClienteEnLista = useCallback((bsaleId: number) => {
    setHighlightBsaleId(bsaleId)
    window.requestAnimationFrame(() => {
      listItemRefs.current.get(bsaleId)?.scrollIntoView({ behavior: "smooth", block: "nearest" })
    })
  }, [])

  useEffect(() => {
    setOrdenMensaje("")
  }, [vendedorFilter, diaFilter])

  useEffect(() => {
    setHighlightBsaleId(null)
    setHoverBsaleId(null)
    setFlyTo(null)
    setSimRunning(false)
    setSimPaused(false)
    setSimPath(null)
    setSimStops([])
    setSimAvisoSinOrden(false)
    setSimHighlightBsaleId(null)
  }, [vendedorFilter, diaFilter])

  useEffect(() => {
    setSugerenciasIgnoradas([])
  }, [vendedorFilter, diaFilter])

  useEffect(() => {
    if (!moverCliente) return
    const actual = ordenMostradoEnMapa(moverCliente, rutaDetalle)
    setNuevaPosicion(actual != null ? String(actual) : "1")
  }, [moverCliente, rutaDetalle])

  useEffect(() => {
    if (vendedorFilter === FILTER_ALL || diaFilter === FILTER_ALL) {
      setRutaDetalle(null)
      setRutaDetalleLoading(false)
      return
    }
    orsRouteViewRef.current = { kind: "fitBounds" }
    const ac = new AbortController()
    setRutaDetalle(null)
    setRutaDetalleLoading(true)
    getDistribuidoraRutaDetalle(vendedorFilter, diaFilter, ac.signal)
      .then((json) => {
        if (!mounted.current || ac.signal.aborted) return
        setRutaDetalle(json)
      })
      .catch(() => {
        if (!mounted.current || ac.signal.aborted) return
        setRutaDetalle({ error: "No se pudo cargar la ruta" })
      })
      .finally(() => {
        if (mounted.current && !ac.signal.aborted) setRutaDetalleLoading(false)
      })
    return () => ac.abort()
  }, [vendedorFilter, diaFilter])

  useEffect(() => {
    mounted.current = true
    setLoading(true)
    setError("")
    getDistribuidoraMapa()
      .then((data) => {
        if (!mounted.current) return
        setClientes(Array.isArray(data.clientes) ? data.clientes : [])
        setDiasAtencionOpciones(diasCatalogoDesdeMapaResp(data))
        setBases(Array.isArray(data.bases) ? data.bases : [])
      })
      .catch((e: unknown) => {
        if (!mounted.current) return
        setError(e instanceof Error ? e.message : "Error al cargar el mapa")
      })
      .finally(() => {
        if (mounted.current) setLoading(false)
      })
    return () => {
      mounted.current = false
    }
  }, [])

  const vendedorOptions = useMemo(() => {
    const set = new Set<string>()
    for (const c of clientes) {
      const v = c.vendedor?.trim()
      if (v) set.add(v)
    }
    for (const b of bases) {
      const v = b.vendedor?.trim()
      if (v) set.add(v)
    }
    return Array.from(set).sort((a, b) => a.localeCompare(b, "es"))
  }, [clientes, bases])

  const clientesVisibles = useMemo(() => {
    return clientes.filter((c) => {
      if (vendedorFilter !== FILTER_ALL) {
        const v = c.vendedor?.trim() ?? ""
        if (v !== vendedorFilter) return false
      }
      if (diaFilter !== FILTER_ALL) {
        const d = c.dia_atencion?.trim() ?? ""
        if (d !== diaFilter) return false
      }
      return true
    })
  }, [clientes, vendedorFilter, diaFilter])

  /** Marcadores del mapa: excluye día y tipo telefónicos (coherente con API /distribuidora/mapa). */
  const clientesMarcadoresMapa = useMemo(() => {
    return clientesVisibles.filter((c) => !esDiaAtencionTelefonico(c.dia_atencion) && !esTipoAtencionTelefonicoMapa(c))
  }, [clientesVisibles])

  const sinClientesTerrenoEnRuta = useMemo(() => {
    if (vendedorFilter === FILTER_ALL || diaFilter === FILTER_ALL) return false
    if (rutaDetalleLoading) return false
    if (!rutaDetalle || typeof rutaDetalle !== "object") return false
    if ("error" in rutaDetalle && rutaDetalle.error) return false
    const arr = rutaDetalle.clientes
    return Array.isArray(arr) && arr.length === 0
  }, [vendedorFilter, diaFilter, rutaDetalle, rutaDetalleLoading])

  const onVendedorChange = useCallback((e: ChangeEvent<HTMLSelectElement>) => {
    setVendedorFilter(e.target.value)
  }, [])

  const onDiaChange = useCallback((e: ChangeEvent<HTMLSelectElement>) => {
    setDiaFilter(e.target.value)
  }, [])

  const mensajeRutaInformativa = useMemo(() => {
    if (!rutaDetalle || typeof rutaDetalle !== "object") return ""
    const adv = rutaDetalle.advertencia_ors
    if (typeof adv === "string" && adv.trim()) return adv.trim()
    if (rutaDetalle.sin_orden_manual === true) {
      return "Ruta sin optimizar - clientes sin orden asignado"
    }
    return ""
  }, [rutaDetalle])

  const puedeEditarOrden =
    vendedorFilter !== FILTER_ALL && diaFilter !== FILTER_ALL && !loading && !error

  const rutaOrdenFirma = useMemo(() => {
    if (!isDistribuidoraRutaDetalleOk(rutaDetalle)) return ""
    const arr = rutaDetalle.clientes as RutaClienteFila[]
    return [...arr]
      .sort((a, b) => Number(a.orden_visita ?? 0) - Number(b.orden_visita ?? 0))
      .map((r) => `${Number(r.bsale_id)}:${Number(r.orden_visita ?? 0)}`)
      .join("|")
  }, [rutaDetalle])

  useEffect(() => {
    if (!puedeEditarOrden || rutaDetalleLoading || !isDistribuidoraRutaDetalleOk(rutaDetalle)) {
      setRutaSugerencias([])
      setRutaSugerenciasError("")
      setRutaSugerenciasLoading(false)
      return
    }
    const ac = new AbortController()
    setRutaSugerenciasLoading(true)
    setRutaSugerenciasError("")
    getDistribuidoraRutaSugerencias(vendedorFilter, diaFilter, { signal: ac.signal })
      .then((data) => {
        if (ac.signal.aborted || !mounted.current) return
        if (data.error) {
          setRutaSugerencias([])
          setRutaSugerenciasError(String(data.error))
        } else {
          setRutaSugerencias(Array.isArray(data.sugerencias) ? data.sugerencias : [])
        }
      })
      .catch((e: unknown) => {
        if (ac.signal.aborted || !mounted.current) return
        setRutaSugerencias([])
        setRutaSugerenciasError(e instanceof Error ? e.message : "Error al cargar sugerencias")
      })
      .finally(() => {
        if (!ac.signal.aborted && mounted.current) setRutaSugerenciasLoading(false)
      })
    return () => ac.abort()
  }, [puedeEditarOrden, vendedorFilter, diaFilter, rutaOrdenFirma, rutaDetalleLoading, rutaDetalle])

  const rutaLista = useMemo(() => rutaClientesOrdenados(rutaDetalle), [rutaDetalle])

  const onSimVisit = useCallback((bsaleId: number) => {
    setSimHighlightBsaleId(bsaleId)
    focusClienteEnLista(bsaleId)
  }, [focusClienteEnLista])

  const onSimComplete = useCallback(() => {
    setSimRunning(false)
    setSimPaused(false)
    setSimAvisoSinOrden(false)
    setSimHighlightBsaleId(null)
    try {
      mapRef.current?.closePopup()
    } catch {
      /* */
    }
  }, [])

  const iniciarSimulacion = useCallback(() => {
    if (!puedeEditarOrden || !isDistribuidoraRutaDetalleOk(rutaDetalle)) return
    const bc = baseCoordsParaMapa(rutaDetalle, bases, vendedorFilter)
    if (!bc || !Number.isFinite(bc.lat) || !Number.isFinite(bc.lon)) {
      setOrdenMensaje("No hay coordenadas de base para simular.")
      return
    }
    const baseTuple: L.LatLngTuple = [bc.lat, bc.lon]
    const terrenoEnRuta = rutaLista.filter(filaRutaEsTerreno)
    const sinOrdenManual = terrenoEnRuta.filter((r) => {
      const om = Number((r as Record<string, unknown>).orden_manual)
      return !(Number.isFinite(om) && om > 0)
    })
    setSimAvisoSinOrden(sinOrdenManual.length > 0)

    const conCoords = terrenoEnRuta.filter((r) => {
      const lat = Number(r.lat)
      const lon = Number(r.lon)
      return Number.isFinite(lat) && Number.isFinite(lon)
    })
    const ordenados = [...conCoords].sort((a, b) => {
      const ka = ordenManualSimKey(a)
      const kb = ordenManualSimKey(b)
      if (ka !== kb) return ka - kb
      return Number(a.bsale_id) - Number(b.bsale_id)
    })
    console.log(
      "SIM ORDEN:",
      ordenados.map((r) => ({
        bsale_id: Number(r.bsale_id),
        orden_manual: (r as Record<string, unknown>).orden_manual,
      })),
    )
    const totalParadas = ordenados.length
    const stops: SimulacionParada[] = []
    for (let i = 0; i < ordenados.length; i++) {
      const r = ordenados[i]!
      const lat = Number(r.lat)
      const lon = Number(r.lon)
      const om = Number((r as Record<string, unknown>).orden_manual)
      const row = r as Record<string, unknown>
      const mun = String(row.municipality ?? "").trim()
      const calle = String(row.direccion ?? row.address ?? row.calle ?? "").trim()
      const direccionLinea = [calle, mun].filter(Boolean).join(" · ") || "—"
      stops.push({
        bsale_id: Number(r.bsale_id),
        lat,
        lon,
        nombre: nombreClienteDesdeFilaRuta(r),
        orden: Number.isFinite(om) && om > 0 ? om : Number.NaN,
        paso: i + 1,
        totalParadas,
        direccionLinea,
      })
    }
    if (stops.length === 0) {
      setOrdenMensaje("No hay clientes en terreno con coordenadas para simular.")
      setSimAvisoSinOrden(false)
      return
    }
    const waypoints: L.LatLngTuple[] = [baseTuple, ...stops.map((s) => [s.lat, s.lon] as L.LatLngTuple), baseTuple]
    setOrdenMensaje("")
    setSimPath(waypoints)
    setSimStops(stops)
    setSimPaused(false)
    setSimHighlightBsaleId(null)
    setSimRunId((n) => n + 1)
    setSimRunning(true)
  }, [puedeEditarOrden, rutaDetalle, rutaLista, bases, vendedorFilter])

  const detenerSimulacion = useCallback(() => {
    setSimRunning(false)
    setSimPaused(false)
    setSimPath(null)
    setSimStops([])
    setSimAvisoSinOrden(false)
    setSimHighlightBsaleId(null)
    try {
      mapRef.current?.closePopup()
    } catch {
      /* */
    }
  }, [])

  const reiniciarSimulacion = useCallback(() => {
    if (!simPath || simPath.length < 2) return
    setSimPaused(false)
    setSimHighlightBsaleId(null)
    try {
      mapRef.current?.closePopup()
    } catch {
      /* */
    }
    setSimRunId((n) => n + 1)
  }, [simPath])

  const puedeSimularRuta =
    puedeEditarOrden &&
    isDistribuidoraRutaDetalleOk(rutaDetalle) &&
    !rutaDetalleLoading &&
    baseCoordsParaMapa(rutaDetalle, bases, vendedorFilter) != null &&
    rutaLista.some((r) => {
      if (!filaRutaEsTerreno(r)) return false
      const lat = Number(r.lat)
      const lon = Number(r.lon)
      return Number.isFinite(lat) && Number.isFinite(lon)
    })

  const onOrdenPanelReorder = useCallback(
    async (bulk: { id: number; orden_manual: number }[]) => {
      if (!puedeEditarOrden || ordenGuardando) return
      setOrdenGuardando(true)
      setOrdenMensaje("")
      try {
        await postDistribuidoraOrdenManualBulk(bulk)
        const mapData = await getDistribuidoraMapa()
        if (!mounted.current) return
        setClientes(Array.isArray(mapData.clientes) ? mapData.clientes : [])
        setDiasAtencionOpciones(diasCatalogoDesdeMapaResp(mapData))
        const json = await getDistribuidoraRutaDetalle(vendedorFilter, diaFilter)
        if (!mounted.current) return
        captureMapViewBeforeRutaUpdate(mapRef, orsRouteViewRef)
        setRutaDetalle(json)
      } catch (e: unknown) {
        setOrdenMensaje(e instanceof Error ? e.message : "Error al guardar orden")
        throw e
      } finally {
        if (mounted.current) setOrdenGuardando(false)
      }
    },
    [puedeEditarOrden, ordenGuardando, vendedorFilter, diaFilter],
  )

  const onSugerenciaIgnorar = useCallback((id: string) => {
    setSugerenciasIgnoradas((prev) => (prev.includes(id) ? prev : [...prev, id]))
  }, [])

  const onSugerenciaAplicar = useCallback(
    async (s: DistribuidoraRutaSugerenciaJson) => {
      if (!puedeEditarOrden || ordenGuardando || !isDistribuidoraRutaDetalleOk(rutaDetalle)) return
      setOrdenMensaje("")
      try {
        const rows = rutaClientesOrdenados(rutaDetalle)
        const sorted = [...rows].sort((a, b) => Number(a.orden_visita ?? 0) - Number(b.orden_visita ?? 0))
        const bulk = bulkPorSwapAdyacente(sorted, s.indice_a)
        await onOrdenPanelReorder(bulk)
        setSugerenciasIgnoradas((prev) => prev.filter((id) => id !== s.id))
      } catch {
        /* mensaje vía onOrdenPanelReorder */
      }
    },
    [puedeEditarOrden, ordenGuardando, rutaDetalle, onOrdenPanelReorder],
  )

  const onBloquearHastaFila = useCallback(
    async (filaIndex: number) => {
      if (!puedeEditarOrden || ordenGuardando) return
      const k = filaIndex + 1
      setOrdenGuardando(true)
      setOrdenMensaje("")
      try {
        const json = await postDistribuidoraOptimizarRuta({
          vendedor: vendedorFilter,
          dia: diaFilter,
          bloque_hasta_indice: k,
        })
        if (!mounted.current) return
        if (json && typeof json === "object" && "error" in json && json.error) {
          setOrdenMensaje(String(json.error))
          return
        }
        const mapData = await getDistribuidoraMapa()
        if (!mounted.current) return
        setClientes(Array.isArray(mapData.clientes) ? mapData.clientes : [])
        setDiasAtencionOpciones(diasCatalogoDesdeMapaResp(mapData))
        captureMapViewBeforeRutaUpdate(mapRef, orsRouteViewRef)
        setRutaDetalle(json as DistribuidoraRutaDetalleJson)
      } catch (e: unknown) {
        setOrdenMensaje(e instanceof Error ? e.message : "Error al optimizar con tramo bloqueado")
      } finally {
        if (mounted.current) setOrdenGuardando(false)
      }
    },
    [puedeEditarOrden, ordenGuardando, vendedorFilter, diaFilter],
  )

  const onOptimizarRuta = useCallback(async () => {
    if (!puedeEditarOrden || ordenGuardando) return
    setOrdenGuardando(true)
    setOrdenMensaje("")
    try {
      const json = await postDistribuidoraOptimizarRuta({
        vendedor: vendedorFilter,
        dia: diaFilter,
      })
      if (!mounted.current) return
      if (json && typeof json === "object" && "error" in json && json.error) {
        setOrdenMensaje(String(json.error))
        return
      }
      const mapData = await getDistribuidoraMapa()
      if (!mounted.current) return
      setClientes(Array.isArray(mapData.clientes) ? mapData.clientes : [])
      setDiasAtencionOpciones(diasCatalogoDesdeMapaResp(mapData))
      captureMapViewBeforeRutaUpdate(mapRef, orsRouteViewRef)
      setRutaDetalle(json as DistribuidoraRutaDetalleJson)
    } catch (e: unknown) {
      setOrdenMensaje(e instanceof Error ? e.message : "Error al optimizar la ruta")
    } finally {
      if (mounted.current) setOrdenGuardando(false)
    }
  }, [puedeEditarOrden, ordenGuardando, vendedorFilter, diaFilter])

  const onReoptimizarDesdeIndice = useCallback(
    async (desdeIndice: number) => {
      if (!puedeEditarOrden || ordenGuardando) return
      setOrdenGuardando(true)
      setOrdenMensaje("")
      try {
        const json = await postDistribuidoraOptimizarRutaDesde({
          vendedor: vendedorFilter,
          dia: diaFilter,
          desde_indice: desdeIndice,
        })
        if (!mounted.current) return
        if (json && typeof json === "object" && "error" in json && json.error) {
          setOrdenMensaje(String(json.error))
          return
        }
        const mapData = await getDistribuidoraMapa()
        if (!mounted.current) return
        setClientes(Array.isArray(mapData.clientes) ? mapData.clientes : [])
        setDiasAtencionOpciones(diasCatalogoDesdeMapaResp(mapData))
        captureMapViewBeforeRutaUpdate(mapRef, orsRouteViewRef)
        setRutaDetalle(json as DistribuidoraRutaDetalleJson)
      } catch (e: unknown) {
        setOrdenMensaje(e instanceof Error ? e.message : "Error al reoptimizar desde aquí")
      } finally {
        if (mounted.current) setOrdenGuardando(false)
      }
    },
    [puedeEditarOrden, ordenGuardando, vendedorFilter, diaFilter],
  )

  const onConfirmarMoverOrden = useCallback(async () => {
    if (!puedeEditarOrden || !moverCliente || ordenGuardando) return
    const lista = rutaLista
    if (lista.length === 0) {
      setOrdenMensaje("No hay ruta cargada para reordenar.")
      return
    }
    const pos = Number.parseInt(nuevaPosicion, 10)
    if (!Number.isFinite(pos) || pos < 1) {
      setOrdenMensaje("Elige una posición válida.")
      return
    }
    setOrdenGuardando(true)
    setOrdenMensaje("")
    try {
      const bulk = reordenarParaBulk(lista, moverCliente.bsale_id, pos)
      await postDistribuidoraOrdenManualBulk(bulk)
      const mapData = await getDistribuidoraMapa()
      if (!mounted.current) return
      setClientes(Array.isArray(mapData.clientes) ? mapData.clientes : [])
      setDiasAtencionOpciones(diasCatalogoDesdeMapaResp(mapData))
      const json = await getDistribuidoraRutaDetalle(vendedorFilter, diaFilter)
      if (!mounted.current) return
      captureMapViewBeforeRutaUpdate(mapRef, orsRouteViewRef)
      setRutaDetalle(json)
      setMoverCliente(null)
    } catch (e: unknown) {
      setOrdenMensaje(e instanceof Error ? e.message : "Error al guardar el nuevo orden")
    } finally {
      if (mounted.current) setOrdenGuardando(false)
    }
  }, [
    puedeEditarOrden,
    moverCliente,
    ordenGuardando,
    nuevaPosicion,
    rutaLista,
    vendedorFilter,
    diaFilter,
  ])

  const onResetOrdenManual = useCallback(async () => {
    if (!puedeEditarOrden || ordenGuardando) return
    setOrdenGuardando(true)
    setOrdenMensaje("")
    try {
      await postDistribuidoraOrdenManualReset({ vendedor: vendedorFilter, dia: diaFilter })
      const mapData = await getDistribuidoraMapa()
      if (mounted.current) {
        setClientes(Array.isArray(mapData.clientes) ? mapData.clientes : [])
        setDiasAtencionOpciones(diasCatalogoDesdeMapaResp(mapData))
      }
      const json = await getDistribuidoraRutaDetalle(vendedorFilter, diaFilter)
      if (mounted.current) {
        captureMapViewBeforeRutaUpdate(mapRef, orsRouteViewRef)
        setRutaDetalle(json)
      }
    } catch (e: unknown) {
      setOrdenMensaje(e instanceof Error ? e.message : "Error al limpiar orden")
    } finally {
      if (mounted.current) setOrdenGuardando(false)
    }
  }, [puedeEditarOrden, ordenGuardando, vendedorFilter, diaFilter])

  return (
    <div className="p-4">
      <div className="rounded-xl bg-white p-4 shadow dark:bg-card">
        <div className="mb-3 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <h1 className="text-lg font-semibold tracking-tight text-foreground">Mapa Rutero</h1>
            <p className="text-sm text-muted-foreground">
              Clientes en mapa (terreno):{" "}
              <span className="font-medium tabular-nums text-foreground">{clientesMarcadoresMapa.length}</span>
              {vendedorFilter !== FILTER_ALL && diaFilter !== FILTER_ALL ? (
                <span className="mt-1 block text-xs text-muted-foreground/90">
                  &quot;Optimizar ruta&quot; recalcula el orden con el optimizador local (sectores + 2-opt) y
                  traza con OpenRouteService; lo guarda como orden de visita. Puedes candar un prefijo o
                  reoptimizar desde una fila sin perder lo anterior. Si ya hay orden en base, no se
                  sobrescribe hasta que optimices; &quot;Limpiar orden manual&quot; borra ese orden fijo.
                </span>
              ) : (
                <span className="mt-1 block text-xs text-muted-foreground/90">
                  Elige vendedor y día para trazar la ruta real en el mapa.
                </span>
              )}
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            <select
              className={SELECT_CLASS}
              value={vendedorFilter}
              onChange={onVendedorChange}
              disabled={simRunning}
              aria-label="Filtrar por vendedor"
            >
              <option value={FILTER_ALL}>Todos los vendedores</option>
              {vendedorOptions.map((v) => (
                <option key={v} value={v}>
                  {v}
                </option>
              ))}
            </select>
            <select
              className={SELECT_CLASS}
              value={diaFilter}
              onChange={onDiaChange}
              disabled={simRunning}
              aria-label="Filtrar por día"
            >
              <option value={FILTER_ALL}>Todos los días</option>
              {diasAtencionOpciones.map((d) => (
                <option key={d} value={d}>
                  {d}
                </option>
              ))}
            </select>
            {puedeEditarOrden ? (
              <>
                <Button
                  type="button"
                  variant="default"
                  size="sm"
                  disabled={ordenGuardando || rutaDetalleLoading || simRunning || sinClientesTerrenoEnRuta}
                  onClick={() => void onOptimizarRuta()}
                >
                  Optimizar ruta
                </Button>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  disabled={ordenGuardando || simRunning || sinClientesTerrenoEnRuta}
                  onClick={() => void onResetOrdenManual()}
                >
                  Limpiar orden manual
                </Button>
                {!simRunning ? (
                  <Button
                    type="button"
                    variant="secondary"
                    size="sm"
                    disabled={!puedeSimularRuta}
                    title={
                      !puedeSimularRuta
                        ? "Requiere vendedor y día, base con coordenadas y al menos un cliente TERRENO con ubicación."
                        : "Simulación en orden manual (base → clientes → base); no optimiza ni reordena."
                    }
                    onClick={() => void iniciarSimulacion()}
                  >
                    <Car className="mr-1.5 h-4 w-4" aria-hidden />
                    Simular ruta
                  </Button>
                ) : (
                  <>
                    <Button
                      type="button"
                      variant="secondary"
                      size="sm"
                      onClick={() => setSimPaused((p) => !p)}
                      aria-pressed={simPaused}
                    >
                      {simPaused ? (
                        <>
                          <Play className="mr-1.5 h-4 w-4" aria-hidden />
                          Reanudar
                        </>
                      ) : (
                        <>
                          <Pause className="mr-1.5 h-4 w-4" aria-hidden />
                          Pausar
                        </>
                      )}
                    </Button>
                    <Button type="button" variant="outline" size="sm" onClick={() => void reiniciarSimulacion()}>
                      <RotateCcw className="mr-1.5 h-4 w-4" aria-hidden />
                      Reiniciar
                    </Button>
                    <Button type="button" variant="outline" size="sm" onClick={() => void detenerSimulacion()}>
                      Detener
                    </Button>
                    <span className="text-xs font-medium text-muted-foreground sm:ml-1">Velocidad</span>
                    {([1, 2, 4] as const).map((m) => (
                      <Button
                        key={m}
                        type="button"
                        variant={simSpeedMult === m ? "default" : "outline"}
                        size="sm"
                        className="min-w-[2.75rem] px-2"
                        onClick={() => setSimSpeedMult(m)}
                      >
                        {m}x
                      </Button>
                    ))}
                  </>
                )}
              </>
            ) : null}
          </div>
        </div>
        {ordenMensaje ? (
          <p className="-mt-2 mb-3 text-sm text-destructive" role="alert">
            {ordenMensaje}
          </p>
        ) : null}
        {mensajeRutaInformativa ? (
          <p
            className="-mt-2 mb-3 rounded-md border border-amber-200/90 bg-amber-50 px-3 py-2 text-sm text-amber-950 dark:border-amber-800/60 dark:bg-amber-950/35 dark:text-amber-50"
            role="status"
          >
            {mensajeRutaInformativa}
          </p>
        ) : null}
        {sinClientesTerrenoEnRuta ? (
          <p
            className="-mt-2 mb-3 rounded-md border border-slate-200 bg-slate-100 px-3 py-2 text-sm text-slate-800 dark:border-slate-600 dark:bg-slate-800/60 dark:text-slate-100"
            role="status"
          >
            No hay clientes en terreno para este día
          </p>
        ) : null}
        {simRunning && simAvisoSinOrden ? (
          <p
            className="-mt-2 mb-3 rounded-md border border-amber-200/90 bg-amber-50 px-3 py-2 text-sm font-medium text-amber-950 dark:border-amber-800/60 dark:bg-amber-950/35 dark:text-amber-50"
            role="status"
          >
            Ruta sin orden definido
          </p>
        ) : null}

        <Dialog
          open={moverCliente != null}
          onOpenChange={(open) => {
            if (!open) setMoverCliente(null)
          }}
        >
          <DialogContent className="sm:max-w-md">
            <DialogHeader>
              <DialogTitle>Mover en orden de visita</DialogTitle>
              <DialogDescription>
                {moverCliente ? (
                  <>
                    Cliente: <span className="font-medium text-foreground">{nombreCliente(moverCliente)}</span>.
                    Elige la nueva posición en la ruta (1 = primero).
                  </>
                ) : null}
              </DialogDescription>
            </DialogHeader>
            {moverCliente && rutaLista.length > 0 ? (
              <div className="grid gap-2">
                <label htmlFor="nueva-posicion-ruta" className="text-sm font-medium text-foreground">
                  Nueva posición
                </label>
                <select
                  id="nueva-posicion-ruta"
                  className={SELECT_CLASS}
                  value={nuevaPosicion}
                  onChange={(e) => setNuevaPosicion(e.target.value)}
                  aria-label="Nueva posición en la ruta"
                >
                  {Array.from({ length: rutaLista.length }, (_, i) => i + 1).map((n) => (
                    <option key={n} value={String(n)}>
                      {n}
                    </option>
                  ))}
                </select>
              </div>
            ) : null}
            <DialogFooter>
              <Button type="button" variant="outline" onClick={() => setMoverCliente(null)}>
                Cancelar
              </Button>
              <Button
                type="button"
                disabled={ordenGuardando || rutaLista.length === 0}
                onClick={() => void onConfirmarMoverOrden()}
              >
                Guardar
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>

        <div className="flex flex-col gap-3 lg:flex-row lg:items-stretch">
          {puedeEditarOrden ? (
            <MapaRuteroPanelRuta
              rutaDetalle={rutaDetalle}
              loading={rutaDetalleLoading}
              highlightBsaleId={highlightBsaleId}
              hoverBsaleId={hoverBsaleId}
              onHoverLista={setHoverBsaleId}
              onSelectCliente={onSelectClienteLista}
              onReoptimizarDesde={(i) => void onReoptimizarDesdeIndice(i)}
              onBloquearHasta={(i) => void onBloquearHastaFila(i)}
              setListItemRef={setListItemRef}
              onOrdenCambiado={onOrdenPanelReorder}
              ordenGuardando={ordenGuardando}
              sugerencias={rutaSugerencias}
              sugerenciasIgnoradas={sugerenciasIgnoradas}
              sugerenciasLoading={rutaSugerenciasLoading}
              sugerenciasError={rutaSugerenciasError}
              onSugerenciaIgnorar={onSugerenciaIgnorar}
              onSugerenciaAplicar={onSugerenciaAplicar}
            />
          ) : null}
          <div className="mapa-rutero-wrapper relative h-[75vh] min-h-[320px] w-full min-w-0 flex-1 overflow-hidden rounded-lg bg-slate-200/80 shadow-inner ring-1 ring-black/5 dark:bg-slate-900/40 dark:ring-white/10">
            {loading ? (
              <div className="flex h-full items-center justify-center gap-2 text-muted-foreground">
                <Loader2 className="h-6 w-6 animate-spin" />
                Cargando mapa…
              </div>
            ) : error ? (
              <div className="flex h-full items-center justify-center p-4 text-center text-sm text-destructive">
                {error}
              </div>
            ) : (
              <>
                <div className="absolute inset-0 z-0 min-h-0 min-w-0">
                  <MapContainer
                    center={MAP_CENTER}
                    zoom={MAP_ZOOM}
                    className="mapa-rutero-leaflet z-0 h-full w-full"
                    style={{ height: "100%", width: "100%" }}
                    scrollWheelZoom
                    attributionControl
                  >
                    <TileLayer
                      url={CARTO_VOYAGER_TILES}
                      attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/attributions">CARTO</a>'
                      subdomains="abcd"
                    />
                    <MapaRuteroInvalidateSize />
                    <MapaRuteroRegisterMap mapRef={mapRef} />
                    <MapaRuteroFlyTo flyTo={flyTo} />
                    <MapaRuteroFitBoundsClientes
                      rutaDetalle={rutaDetalle}
                      clientesVisibles={clientesMarcadoresMapa}
                      bases={bases}
                      vendedorFilter={vendedorFilter}
                      diaFilter={diaFilter}
                      activo={puedeEditarOrden}
                    />
                    <MapaRuteroOrsRoute detalle={rutaDetalle} viewBehaviorRef={orsRouteViewRef} />
                    {simRunning && simPath && simPath.length >= 2 ? (
                      <MapaRuteroSimulacionVehiculo
                        runId={simRunId}
                        running={simRunning}
                        paused={simPaused}
                        speedMult={simSpeedMult}
                        waypoints={simPath}
                        stops={simStops}
                        onVisitClient={onSimVisit}
                        onComplete={onSimComplete}
                      />
                    ) : null}
                    {puedeEditarOrden ? (
                      <>
                        {clientesMarcadoresMapa.map((c) => {
                          const om = ordenMostradoEnMapa(c, rutaDetalle)
                          const enRuta = clienteEnRutaActual(c, rutaDetalle)
                          const puedeMover =
                            enRuta &&
                            c.vendedor?.trim() === vendedorFilter &&
                            c.dia_atencion?.trim() === diaFilter
                          const simHit = simHighlightBsaleId === c.bsale_id
                          const fill = simHit
                            ? MAP_CLIENTE_COLOR_SIM_VISITA
                            : highlightBsaleId === c.bsale_id
                              ? MAP_CLIENTE_COLOR_HIGHLIGHT
                              : hoverBsaleId === c.bsale_id
                                ? MAP_CLIENTE_COLOR_HOVER
                                : MAP_CLIENTE_COLOR
                          return (
                            <Marker
                              key={c.bsale_id}
                              position={[c.lat, c.lon]}
                              icon={getClienteDivIcon(fill, { simDestacado: simHit })}
                              eventHandlers={{
                                click: () => focusClienteEnLista(c.bsale_id),
                                mouseover: () => setHoverBsaleId(c.bsale_id),
                                mouseout: () => setHoverBsaleId(null),
                              }}
                            >
                              {om != null ? (
                                <Tooltip permanent direction="top" opacity={1} className="orden-tooltip">
                                  {String(om)}
                                </Tooltip>
                              ) : null}
                              <Popup>
                                <div className="mapa-rutero-popup-inner space-y-3 p-3 text-sm">
                                  <p className="font-semibold leading-snug text-foreground">{nombreCliente(c)}</p>
                                  <dl className="space-y-1.5 text-muted-foreground">
                                    <div className="grid grid-cols-[5.5rem_1fr] gap-x-2 gap-y-1">
                                      <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground/80">
                                        Vendedor
                                      </dt>
                                      <dd className="text-foreground">{c.vendedor?.trim() || "—"}</dd>
                                      <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground/80">
                                        Día
                                      </dt>
                                      <dd className="text-foreground">{c.dia_atencion?.trim() || "—"}</dd>
                                      <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground/80">
                                        Teléfono
                                      </dt>
                                      <dd className="text-foreground">{c.phone?.trim() || "—"}</dd>
                                      <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground/80">
                                        Municipio
                                      </dt>
                                      <dd className="text-foreground">{c.municipality?.trim() || "—"}</dd>
                                      {om != null ? (
                                        <>
                                          <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground/80">
                                            Orden visita
                                          </dt>
                                          <dd className="text-foreground">{om}</dd>
                                        </>
                                      ) : null}
                                    </div>
                                  </dl>
                                  {c.vendedor?.trim() === vendedorFilter &&
                                  c.dia_atencion?.trim() === diaFilter ? (
                                    <Button
                                      type="button"
                                      variant="secondary"
                                      size="sm"
                                      className="w-full"
                                      disabled={!puedeMover || ordenGuardando}
                                      title={
                                        !enRuta
                                          ? "Este cliente no está en la ruta del día (p. ej. solo teléfono)."
                                          : undefined
                                      }
                                      onClick={() => setMoverCliente(c)}
                                    >
                                      Mover en orden
                                    </Button>
                                  ) : null}
                                </div>
                              </Popup>
                            </Marker>
                          )
                        })}
                        {(() => {
                          const bc = baseCoordsParaMapa(rutaDetalle, bases, vendedorFilter)
                          if (!bc) return null
                          return (
                            <Marker
                              key="ruta-base-unica"
                              position={[bc.lat, bc.lon]}
                              icon={getBasePuntoRutaMapIcon()}
                              interactive={false}
                            />
                          )
                        })()}
                      </>
                    ) : (
                      <>
                        <MarkerClusterGroup chunkedLoading showCoverageOnHover={false}>
                          {clientesMarcadoresMapa.map((c) => (
                            <Marker
                              key={c.bsale_id}
                              position={[c.lat, c.lon]}
                              icon={getClienteDivIcon(MAP_CLIENTE_COLOR)}
                            >
                              <Popup>
                                <div className="mapa-rutero-popup-inner p-3 text-sm">
                                  <p className="font-semibold leading-snug text-foreground">{nombreCliente(c)}</p>
                                  <dl className="mt-2 space-y-1.5 text-muted-foreground">
                                    <div className="grid grid-cols-[5.5rem_1fr] gap-x-2 gap-y-1">
                                      <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground/80">
                                        Vendedor
                                      </dt>
                                      <dd className="text-foreground">{c.vendedor?.trim() || "—"}</dd>
                                      <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground/80">
                                        Día
                                      </dt>
                                      <dd className="text-foreground">{c.dia_atencion?.trim() || "—"}</dd>
                                      <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground/80">
                                        Teléfono
                                      </dt>
                                      <dd className="text-foreground">{c.phone?.trim() || "—"}</dd>
                                      <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground/80">
                                        Municipio
                                      </dt>
                                      <dd className="text-foreground">{c.municipality?.trim() || "—"}</dd>
                                    </div>
                                  </dl>
                                </div>
                              </Popup>
                            </Marker>
                          ))}
                        </MarkerClusterGroup>
                        {bases.map((b, i) => {
                          const key = `${b.vendedor ?? "b"}-${b.lat}-${b.lon}-${i}`
                          return (
                            <Marker key={key} position={[b.lat, b.lon]} icon={getBaseMapIcon()}>
                              <Popup>
                                <div className="mapa-rutero-popup-inner p-2 text-sm">
                                  <p className="font-semibold text-foreground">{b.nombre?.trim() || "Punto base"}</p>
                                  <p className="text-muted-foreground">{b.vendedor?.trim() || "—"}</p>
                                </div>
                              </Popup>
                            </Marker>
                          )
                        })}
                      </>
                    )}
                  </MapContainer>
                </div>
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
