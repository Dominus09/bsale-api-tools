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
import { GripVertical, Loader2, RefreshCw } from "lucide-react"

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
  isDistribuidoraRutaDetalleOk,
  postDistribuidoraOptimizarRuta,
  postDistribuidoraOptimizarRutaDesde,
  postDistribuidoraOrdenManualBulk,
  postDistribuidoraOrdenManualReset,
  type DistribuidoraMapaCliente,
  type DistribuidoraPuntoBase,
  type DistribuidoraRutaDetalleJson,
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
/** Punto base (mapa general). */
const MAP_BASE_COLOR = "#dc2626"

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

    if (!detalle || typeof detalle !== "object" || ("error" in detalle && detalle.error)) {
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

function getClienteDivIcon(fillColor: string): L.DivIcon {
  const key = fillColor
  let icon = clienteIconCache.get(key)
  if (!icon) {
    const size = MARKER_PX
    const b = MARKER_BORDER
    icon = L.divIcon({
      className: "mapa-rutero-cliente-icon",
      html: `<div style="width:${size}px;height:${size}px;border-radius:9999px;background:${fillColor};border:${b}px solid #ffffff;box-shadow:0 2px 8px rgba(15,23,42,0.28);box-sizing:content-box;"></div>`,
      iconSize: [size + b * 2, size + b * 2],
      iconAnchor: [(size + b * 2) / 2, (size + b * 2) / 2],
      popupAnchor: [0, -10],
    })
    clienteIconCache.set(key, icon)
  }
  return icon
}

let baseIconSingleton: L.DivIcon | null = null
function getBaseDivIcon(): L.DivIcon {
  if (!baseIconSingleton) {
    const s = 12
    const b = 2
    baseIconSingleton = L.divIcon({
      className: "mapa-rutero-base-icon",
      html: `<div style="width:${s}px;height:${s}px;border-radius:4px;background:${MAP_BASE_COLOR};border:${b}px solid #ffffff;box-shadow:0 2px 8px rgba(15,23,42,0.28);box-sizing:content-box;"></div>`,
      iconSize: [s + b * 2, s + b * 2],
      iconAnchor: [(s + b * 2) / 2, (s + b * 2) / 2],
      popupAnchor: [0, -8],
    })
  }
  return baseIconSingleton
}

/** Base de ruta: ícono grande (bandera), no numerar — solo informativo en mapa. */
let basePuntoRutaIcon: L.DivIcon | null = null
function getBasePuntoRutaIcon(): L.DivIcon {
  if (!basePuntoRutaIcon) {
    basePuntoRutaIcon = L.divIcon({
      className: "mapa-rutero-base-ruta-icon",
      html: `<div style="display:flex;flex-direction:column;align-items:center;gap:2px;pointer-events:none;"><span style="font-size:26px;line-height:1;text-shadow:0 1px 3px rgba(0,0,0,0.35);">&#128681;</span><span style="font-size:10px;font-weight:800;letter-spacing:0.04em;background:${MAP_BASE_COLOR};color:#fff;padding:2px 6px;border-radius:4px;border:2px solid #fff;box-shadow:0 2px 8px rgba(15,23,42,0.25);">BASE</span></div>`,
      iconSize: [48, 52],
      iconAnchor: [24, 52],
      popupAnchor: [0, -56],
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
  if ("error" in rutaDetalle && rutaDetalle.error) return []
  const arr = rutaDetalle.clientes
  if (!Array.isArray(arr)) return []
  return arr.filter((x): x is RutaClienteFila => x != null && typeof x === "object")
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
        {onReoptimizarDesde ? (
          <button
            type="button"
            className={cn(
              "text-muted-foreground hover:text-primary hover:bg-primary/10 flex shrink-0 items-center rounded-r-md px-1.5 py-2 transition-colors",
              disabled && "pointer-events-none opacity-40",
            )}
            title="Reoptimizar con ORS desde este cliente en adelante (lo anterior no cambia)"
            aria-label="Reoptimizar desde aquí"
            onClick={(e) => {
              e.stopPropagation()
              onReoptimizarDesde(filaIndex)
            }}
          >
            <RefreshCw className="size-4" aria-hidden />
          </button>
        ) : null}
      </div>
    </li>
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
  setListItemRef,
  onOrdenCambiado,
  ordenGuardando,
}: {
  rutaDetalle: DistribuidoraRutaDetalleJson | null
  loading: boolean
  highlightBsaleId: number | null
  hoverBsaleId: number | null
  onHoverLista: (bsaleId: number | null) => void
  onSelectCliente: (row: RutaClienteFila) => void
  onReoptimizarDesde?: (desdeIndice: number) => void
  setListItemRef: (bsaleId: number, el: HTMLElement | null) => void
  onOrdenCambiado: (bulk: { id: number; orden_manual: number }[]) => Promise<void>
  ordenGuardando: boolean
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
          Arrastra con ⋮⋮; clic en el nombre centra el mapa; el botón circular a la derecha reoptimiza con ORS
          solo desde esa visita en adelante.
        </p>
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto px-2 py-2">
        {loading ? (
          <p className="flex items-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 shrink-0 animate-spin" />
            Cargando ruta…
          </p>
        ) : errMsg ? (
          <p className="text-sm text-destructive">{errMsg}</p>
        ) : (
          <div className="space-y-2 text-sm">
            <div className="rounded-md bg-emerald-600/12 px-2 py-1.5 text-emerald-900 dark:bg-emerald-500/15 dark:text-emerald-100">
              <span aria-hidden>🟢 </span>
              <span className="font-medium">Inicio:</span> {baseNombre}
            </div>
            {items.length === 0 ? (
              <p className="text-muted-foreground">Sin clientes en la ruta.</p>
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
          <div className="flex justify-between gap-2">
            <span>Tiempo estimado</span>
            <span className="font-medium tabular-nums text-foreground">
              {min >= 120 ? `${(min / 60).toFixed(1)} h` : `${Math.round(min)} min`}
            </span>
          </div>
        </div>
      ) : null}
    </aside>
  )
}

export default function MapaRuteroClient() {
  const [clientes, setClientes] = useState<DistribuidoraMapaCliente[]>([])
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
    return Array.from(set).sort((a, b) => a.localeCompare(b, "es"))
  }, [clientes])

  const diaOptions = useMemo(() => {
    const set = new Set<string>()
    for (const c of clientes) {
      const d = c.dia_atencion?.trim()
      if (d) set.add(d)
    }
    return Array.from(set).sort((a, b) => a.localeCompare(b, "es"))
  }, [clientes])

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

  const onVendedorChange = useCallback((e: ChangeEvent<HTMLSelectElement>) => {
    setVendedorFilter(e.target.value)
  }, [])

  const onDiaChange = useCallback((e: ChangeEvent<HTMLSelectElement>) => {
    setDiaFilter(e.target.value)
  }, [])

  const puedeEditarOrden =
    vendedorFilter !== FILTER_ALL && diaFilter !== FILTER_ALL && !loading && !error

  const rutaLista = useMemo(() => rutaClientesOrdenados(rutaDetalle), [rutaDetalle])

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
              Clientes visibles:{" "}
              <span className="font-medium tabular-nums text-foreground">{clientesVisibles.length}</span>
              {vendedorFilter !== FILTER_ALL && diaFilter !== FILTER_ALL ? (
                <span className="mt-1 block text-xs text-muted-foreground/90">
                  &quot;Optimizar ruta&quot; calcula el orden con ORS y lo guarda. Luego puedes abrir un
                  cliente y usar &quot;Mover en orden&quot; para afinar. Si hay orden guardado en base,
                  la secuencia es fija (sin reoptimizar); &quot;Limpiar orden manual&quot; vuelve al
                  modo solo ORS.
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
              aria-label="Filtrar por día"
            >
              <option value={FILTER_ALL}>Todos los días</option>
              {diaOptions.map((d) => (
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
                  disabled={ordenGuardando || rutaDetalleLoading}
                  onClick={() => void onOptimizarRuta()}
                >
                  Optimizar ruta
                </Button>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  disabled={ordenGuardando}
                  onClick={() => void onResetOrdenManual()}
                >
                  Limpiar orden manual
                </Button>
              </>
            ) : null}
          </div>
        </div>
        {ordenMensaje ? (
          <p className="-mt-2 mb-3 text-sm text-destructive" role="alert">
            {ordenMensaje}
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
              setListItemRef={setListItemRef}
              onOrdenCambiado={onOrdenPanelReorder}
              ordenGuardando={ordenGuardando}
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
                    <MapaRuteroOrsRoute detalle={rutaDetalle} viewBehaviorRef={orsRouteViewRef} />
                    {puedeEditarOrden ? (
                      <>
                        {clientesVisibles.map((c) => {
                          const om = ordenMostradoEnMapa(c, rutaDetalle)
                          const enRuta = clienteEnRutaActual(c, rutaDetalle)
                          const puedeMover =
                            enRuta &&
                            c.vendedor?.trim() === vendedorFilter &&
                            c.dia_atencion?.trim() === diaFilter
                          const fill =
                            highlightBsaleId === c.bsale_id
                              ? MAP_CLIENTE_COLOR_HIGHLIGHT
                              : hoverBsaleId === c.bsale_id
                                ? MAP_CLIENTE_COLOR_HOVER
                                : MAP_CLIENTE_COLOR
                          return (
                            <Marker
                              key={c.bsale_id}
                              position={[c.lat, c.lon]}
                              icon={getClienteDivIcon(fill)}
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
                              icon={getBasePuntoRutaIcon()}
                              interactive={false}
                            />
                          )
                        })()}
                      </>
                    ) : (
                      <>
                        <MarkerClusterGroup chunkedLoading showCoverageOnHover={false}>
                          {clientesVisibles.map((c) => (
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
                            <Marker key={key} position={[b.lat, b.lon]} icon={getBaseDivIcon()}>
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
