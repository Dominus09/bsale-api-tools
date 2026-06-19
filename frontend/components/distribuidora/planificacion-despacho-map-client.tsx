"use client"

import { useEffect, useMemo, useRef } from "react"
import dynamic from "next/dynamic"
import L from "leaflet"
import { Marker, Popup, Tooltip, useMap } from "react-leaflet"
import "leaflet/dist/leaflet.css"
import { OrsStopPopup } from "@/components/distribuidora/planificacion/OrsStopPopup"
import { SEMAPHORE_RING_COLOR, type CommercialSemaphore } from "@/lib/ors-commercial-semaphore"
import type { OrsStopPopupData } from "@/lib/ors-map-ui"

const MapContainer = dynamic(() => import("react-leaflet").then((m) => m.MapContainer), {
  ssr: false,
})
const TileLayer = dynamic(() => import("react-leaflet").then((m) => m.TileLayer), { ssr: false })
const Polyline = dynamic(() => import("react-leaflet").then((m) => m.Polyline), { ssr: false })

export type PlanificacionMapStop = {
  lat: number
  lng: number
  num: number
  stopKey: string
  label: string
  comuna?: string | null
  semaphore?: CommercialSemaphore
  documentId?: number
  clientId?: number | null
  popup?: OrsStopPopupData
}

export type PlanificacionMapRoute = {
  camion: string
  color: string
  positions: [number, number][]
  stops: PlanificacionMapStop[]
}

function FitBoundsInner({
  routes,
  depot,
}: {
  routes: PlanificacionMapRoute[]
  depot?: { lat: number; lng: number } | null
}) {
  const map = useMap()
  useEffect(() => {
    const pts: L.LatLngExpression[] = []
    if (depot) pts.push([depot.lat, depot.lng])
    for (const r of routes) {
      for (const [lat, lng] of r.positions) pts.push([lat, lng])
      for (const s of r.stops) pts.push([s.lat, s.lng])
    }
    if (pts.length === 0) return
    if (pts.length === 1) {
      map.setView(pts[0] as L.LatLngTuple, 14)
      return
    }
    map.fitBounds(L.latLngBounds(pts as L.LatLngTuple[]), { padding: [40, 40], maxZoom: 14 })
  }, [map, routes, depot])
  return null
}

function MapFlyToController({
  flyToTarget,
}: {
  flyToTarget?: { lat: number; lng: number; zoom?: number; seq?: number } | null
}) {
  const map = useMap()
  useEffect(() => {
    if (!flyToTarget) return
    map.flyTo(
      [flyToTarget.lat, flyToTarget.lng],
      flyToTarget.zoom ?? 15,
      { duration: 0.5, easeLinearity: 0.25 },
    )
  }, [map, flyToTarget?.lat, flyToTarget?.lng, flyToTarget?.zoom, flyToTarget?.seq])
  return null
}

function numberedIcon(
  n: number,
  color: string,
  highlighted?: boolean,
  comuna?: string | null,
  semaphore?: CommercialSemaphore,
) {
  const ringColor = semaphore ? SEMAPHORE_RING_COLOR[semaphore] : color
  const ring = highlighted
    ? `box-shadow:0 0 0 3px rgba(255,255,255,.95),0 0 0 5px ${ringColor}88,0 2px 8px rgba(0,0,0,.4)`
    : `box-shadow:0 0 0 2px ${ringColor}99,0 2px 6px rgba(0,0,0,.35)`
  const scale = highlighted ? "transform:scale(1.12)" : ""
  const comunaHtml = comuna?.trim()
    ? `<span style="display:block;margin-top:2px;font:600 8px/1.1 system-ui,sans-serif;color:#334155;max-width:72px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${comuna}</span>`
    : ""
  return L.divIcon({
    className: "planificacion-num-marker-wrap",
    html: `<div style="text-align:center"><div style="width:26px;height:26px;border-radius:9999px;background:${color};color:#fff;font:700 12px/26px system-ui,sans-serif;text-align:center;border:2.5px solid #fff;${ring};${scale}">${n}</div>${comunaHtml}</div>`,
    iconSize: comunaHtml ? [72, 38] : [26, 26],
    iconAnchor: comunaHtml ? [36, 13] : [13, 13],
    popupAnchor: [0, comunaHtml ? -16 : -12],
  })
}

function RouteStopMarker({
  stop,
  routeColor,
  camion,
  highlighted,
  openPopupStopKey,
  onStopClick,
  onPopupClose,
}: {
  stop: PlanificacionMapStop
  routeColor: string
  camion: string
  highlighted?: boolean
  openPopupStopKey?: string | null
  onStopClick?: (stop: PlanificacionMapStop) => void
  onPopupClose?: () => void
}) {
  const markerRef = useRef<L.Marker>(null)

  useEffect(() => {
    if (openPopupStopKey !== stop.stopKey) return
    const timer = window.setTimeout(() => markerRef.current?.openPopup(), 400)
    return () => window.clearTimeout(timer)
  }, [openPopupStopKey, stop.stopKey])

  const icon = numberedIcon(stop.num, routeColor, highlighted, stop.comuna, stop.semaphore)

  return (
    <Marker
      ref={markerRef}
      position={[stop.lat, stop.lng]}
      icon={icon}
      zIndexOffset={highlighted ? 1000 : stop.num}
      eventHandlers={{
        click: () => onStopClick?.(stop),
        popupclose: () => {
          if (openPopupStopKey === stop.stopKey) onPopupClose?.()
        },
      }}
    >
      {stop.popup ? (
        <Popup closeButton minWidth={180} maxWidth={280}>
          <OrsStopPopup {...stop.popup} />
        </Popup>
      ) : null}
      <Tooltip direction="top" offset={[0, -10]} opacity={0.95}>
        <span className="text-xs font-semibold">{stop.label}</span>
        <span className="block text-[10px] opacity-80">{camion}</span>
      </Tooltip>
    </Marker>
  )
}

const DEFAULT_CENTER: [number, number] = [-33.0, -71.5]

type PlanificacionDespachoMapClientProps = {
  routes: PlanificacionMapRoute[]
  depot?: { lat: number; lng: number } | null
  className?: string
  highlightedStopKey?: string | null
  flyToTarget?: { lat: number; lng: number; zoom?: number; seq?: number } | null
  openPopupStopKey?: string | null
  onPopupClose?: () => void
  onStopClick?: (stop: PlanificacionMapStop) => void
}

function depotIcon() {
  return L.divIcon({
    className: "planificacion-depot-marker-wrap",
    html: `<div style="width:28px;height:28px;border-radius:4px;background:#1e293b;color:#fff;font:700 9px/28px system-ui,sans-serif;text-align:center;border:2.5px solid #f8fafc;box-shadow:0 2px 8px rgba(0,0,0,.45)">BD</div>`,
    iconSize: [28, 28],
    iconAnchor: [14, 14],
  })
}

export function PlanificacionDespachoMapClient({
  routes,
  depot,
  className,
  highlightedStopKey,
  flyToTarget,
  openPopupStopKey,
  onPopupClose,
  onStopClick,
}: PlanificacionDespachoMapClientProps) {
  const center = useMemo((): [number, number] => {
    if (depot) return [depot.lat, depot.lng]
    for (const r of routes) {
      const s0 = r.stops[0]
      if (s0) return [s0.lat, s0.lng]
      const p0 = r.positions[0]
      if (p0) return p0
    }
    return DEFAULT_CENTER
  }, [routes, depot])

  return (
    <div
      className={
        className ??
        "h-full min-h-[420px] w-full overflow-hidden rounded-lg border border-border/80 bg-slate-950/5 shadow-inner dark:bg-slate-950/40"
      }
    >
      <MapContainer
        center={center}
        zoom={11}
        className="h-full w-full [&_.leaflet-control-zoom]:border-border/80 [&_.leaflet-control-zoom]:shadow-md"
        scrollWheelZoom
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a>'
          url="https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png"
        />
        <FitBoundsInner routes={routes} depot={depot} />
        <MapFlyToController flyToTarget={flyToTarget} />
        {depot ? (
          <Marker position={[depot.lat, depot.lng]} icon={depotIcon()} zIndexOffset={2000}>
            <Tooltip direction="top" opacity={1}>
              <span className="text-xs font-semibold">Bodega</span>
              <span className="block text-[10px] opacity-80">Inicio y fin de ruta</span>
            </Tooltip>
          </Marker>
        ) : null}
        {routes.map((r) => (
          <Polyline
            key={r.camion}
            positions={r.positions}
            pathOptions={{
              color: r.color,
              weight: 6,
              opacity: 0.92,
              lineCap: "round",
              lineJoin: "round",
            }}
          />
        ))}
        {routes.flatMap((r) =>
          r.stops.map((s) => {
            const hi = highlightedStopKey === s.stopKey
            return (
              <RouteStopMarker
                key={`${s.stopKey}-${s.lat}-${s.lng}`}
                stop={s}
                routeColor={r.color}
                camion={r.camion}
                highlighted={hi}
                openPopupStopKey={openPopupStopKey}
                onStopClick={onStopClick}
                onPopupClose={onPopupClose}
              />
            )
          }),
        )}
      </MapContainer>
    </div>
  )
}
