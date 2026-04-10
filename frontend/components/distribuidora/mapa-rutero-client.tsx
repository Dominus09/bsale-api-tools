"use client"

import { useCallback, useEffect, useMemo, useRef, useState, type ChangeEvent } from "react"
import dynamic from "next/dynamic"
import L from "leaflet"
import { useMap } from "react-leaflet"
import { Loader2 } from "lucide-react"

import polylineModule from "@mapbox/polyline"

import {
  getDistribuidoraMapa,
  getDistribuidoraRutaDetalle,
  isDistribuidoraRutaDetalleOk,
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
const MarkerClusterGroup = dynamic(() => import("react-leaflet-cluster").then((m) => m.default), {
  ssr: false,
})

const MAP_CENTER: [number, number] = [-42.6, -73.8]
const MAP_ZOOM = 10

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

/** Clientes en mapa con ruta ORS (pedido: azul). */
const MAP_CLIENTE_COLOR = "#2563eb"
/** Punto base (pedido: rojo). */
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

/** Ruta ORS: polyline azul + fitBounds (datos ya cargados en el padre). */
function MapaRuteroOrsRoute({ detalle }: { detalle: DistribuidoraRutaDetalleJson | null }) {
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

    map.fitBounds(routeLine.getBounds(), { padding: [48, 48], maxZoom: 14, animate: false })
    window.setTimeout(() => map.invalidateSize(), 0)

    return removeRoute
  }, [map, detalle])

  return null
}

function MapaRuteroResumenRuta({
  loading,
  data,
}: {
  loading: boolean
  data: DistribuidoraRutaDetalleJson | null
}) {
  return (
    <div
      id="resumen-ruta"
      className="resumen-box pointer-events-auto text-foreground"
      role="region"
      aria-label="Resumen de ruta ORS"
    >
      <h4>Resumen Ruta</h4>
      {loading ? (
        <p className="mb-0 text-sm text-muted-foreground">Cargando métricas…</p>
      ) : !data ? (
        <p className="mb-0 text-sm text-muted-foreground">Sin datos.</p>
      ) : "error" in data && data.error ? (
        <p className="mb-0 text-sm text-destructive">
          {String(data.error)}
          {"detalle" in data && data.detalle != null ? ` — ${String(data.detalle)}` : ""}
        </p>
      ) : isDistribuidoraRutaDetalleOk(data) ? (
        <>
          <div className="resumen-item">
            <span>Vendedor</span>
            <b>{String(data.vendedor)}</b>
          </div>
          <div className="resumen-item">
            <span>Día</span>
            <b>{String(data.dia)}</b>
          </div>
          <div className="resumen-item">
            <span>Clientes</span>
            <b>{Array.isArray(data.clientes) ? data.clientes.length : 0}</b>
          </div>
          <div className="resumen-item">
            <span>KM</span>
            <b>{Number(data.km_totales).toFixed(1)} km</b>
          </div>
          <div className="resumen-item">
            <span>Tiempo</span>
            <b>
              {Number(data.minutos_totales) >= 120
                ? `${(Number(data.minutos_totales) / 60).toFixed(1)} h`
                : `${Math.round(Number(data.minutos_totales))} min`}
            </b>
          </div>
          {Array.isArray(data.clientes) && data.clientes.length > 0 ? (
            <div className="resumen-item">
              <span>Promedio</span>
              <b>{`${(Number(data.km_totales) / data.clientes.length).toFixed(1)} km/cliente`}</b>
            </div>
          ) : null}
        </>
      ) : (
        <p className="mb-0 text-sm text-muted-foreground">Respuesta sin métricas reconocibles.</p>
      )}
    </div>
  )
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

const SELECT_CLASS =
  "h-9 min-w-[140px] rounded-md border border-input bg-background px-3 text-sm shadow-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"

export default function MapaRuteroClient() {
  const [clientes, setClientes] = useState<DistribuidoraMapaCliente[]>([])
  const [bases, setBases] = useState<DistribuidoraPuntoBase[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState("")
  const [vendedorFilter, setVendedorFilter] = useState(FILTER_ALL)
  const [diaFilter, setDiaFilter] = useState(FILTER_ALL)
  const [rutaDetalle, setRutaDetalle] = useState<DistribuidoraRutaDetalleJson | null>(null)
  const [rutaDetalleLoading, setRutaDetalleLoading] = useState(false)
  const mounted = useRef(true)

  useEffect(() => {
    if (vendedorFilter === FILTER_ALL || diaFilter === FILTER_ALL) {
      setRutaDetalle(null)
      setRutaDetalleLoading(false)
      return
    }
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
                  Ruta ORS (línea azul) según vendedor y día seleccionados.
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
          </div>
        </div>

        <div className="mapa-rutero-wrapper relative h-[75vh] min-h-[320px] w-full min-w-0 overflow-hidden rounded-lg bg-slate-200/80 shadow-inner ring-1 ring-black/5 dark:bg-slate-900/40 dark:ring-white/10">
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
                  <MapaRuteroOrsRoute detalle={rutaDetalle} />
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
                </MapContainer>
              </div>
              {vendedorFilter !== FILTER_ALL && diaFilter !== FILTER_ALL ? (
                <MapaRuteroResumenRuta loading={rutaDetalleLoading} data={rutaDetalle} />
              ) : null}
            </>
          )}
        </div>
      </div>
    </div>
  )
}
