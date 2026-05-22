"use client"

import { useEffect, useMemo } from "react"
import dynamic from "next/dynamic"
import L from "leaflet"
import { useMap } from "react-leaflet"
import "leaflet/dist/leaflet.css"

const MapContainer = dynamic(() => import("react-leaflet").then((m) => m.MapContainer), {
  ssr: false,
})
const TileLayer = dynamic(() => import("react-leaflet").then((m) => m.TileLayer), { ssr: false })
const Polyline = dynamic(() => import("react-leaflet").then((m) => m.Polyline), { ssr: false })
const Marker = dynamic(() => import("react-leaflet").then((m) => m.Marker), { ssr: false })
const Tooltip = dynamic(() => import("react-leaflet").then((m) => m.Tooltip), { ssr: false })

export type PlanificacionMapStop = {
  lat: number
  lng: number
  num: number
  label: string
}

export type PlanificacionMapRoute = {
  camion: string
  color: string
  positions: [number, number][]
  stops: PlanificacionMapStop[]
}

function FitBoundsInner({ routes }: { routes: PlanificacionMapRoute[] }) {
  const map = useMap()
  useEffect(() => {
    const pts: L.LatLngExpression[] = []
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
  }, [map, routes])
  return null
}

function numberedIcon(n: number, color: string, highlighted?: boolean) {
  const ring = highlighted ? "box-shadow:0 0 0 3px rgba(255,255,255,.95),0 0 0 5px " + color + "55,0 2px 8px rgba(0,0,0,.4)" : "box-shadow:0 2px 6px rgba(0,0,0,.35)"
  const scale = highlighted ? "transform:scale(1.12)" : ""
  return L.divIcon({
    className: "planificacion-num-marker-wrap",
    html: `<div style="width:26px;height:26px;border-radius:9999px;background:${color};color:#fff;font:700 12px/26px system-ui,sans-serif;text-align:center;border:2.5px solid #fff;${ring};${scale}">${n}</div>`,
    iconSize: [26, 26],
    iconAnchor: [13, 13],
  })
}

const DEFAULT_CENTER: [number, number] = [-33.0, -71.5]

type PlanificacionDespachoMapClientProps = {
  routes: PlanificacionMapRoute[]
  className?: string
  highlightedStopKey?: string | null
}

export function PlanificacionDespachoMapClient({
  routes,
  className,
  highlightedStopKey,
}: PlanificacionDespachoMapClientProps) {
  const center = useMemo((): [number, number] => {
    for (const r of routes) {
      const s0 = r.stops[0]
      if (s0) return [s0.lat, s0.lng]
      const p0 = r.positions[0]
      if (p0) return p0
    }
    return DEFAULT_CENTER
  }, [routes])

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
        <FitBoundsInner routes={routes} />
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
            const key = `${r.camion}-${s.num}`
            const hi = highlightedStopKey === key
            return (
              <Marker
                key={`${r.camion}-${s.num}-${s.lat}-${s.lng}`}
                position={[s.lat, s.lng]}
                icon={numberedIcon(s.num, r.color, hi)}
                zIndexOffset={hi ? 1000 : s.num}
              >
                <Tooltip direction="top" offset={[0, -10]} opacity={1}>
                  <span className="text-xs font-semibold">{s.label}</span>
                  <span className="block text-[10px] opacity-80">{r.camion}</span>
                </Tooltip>
              </Marker>
            )
          }),
        )}
      </MapContainer>
    </div>
  )
}
