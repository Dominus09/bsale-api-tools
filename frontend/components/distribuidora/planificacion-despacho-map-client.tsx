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

function numberedIcon(n: number, color: string) {
  return L.divIcon({
    className: "planificacion-num-marker-wrap",
    html: `<div style="width:22px;height:22px;border-radius:9999px;background:${color};color:#fff;font:700 11px/22px system-ui,sans-serif;text-align:center;border:2px solid #fff;box-shadow:0 1px 4px rgba(0,0,0,.35)">${n}</div>`,
    iconSize: [22, 22],
    iconAnchor: [11, 11],
  })
}

const DEFAULT_CENTER: [number, number] = [-33.0, -71.5]

type PlanificacionDespachoMapClientProps = {
  routes: PlanificacionMapRoute[]
}

export function PlanificacionDespachoMapClient({ routes }: PlanificacionDespachoMapClientProps) {
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
    <div className="h-[min(520px,70vh)] w-full overflow-hidden rounded-xl border border-border/60">
      <MapContainer center={center} zoom={11} className="h-full w-full" scrollWheelZoom>
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a>'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        <FitBoundsInner routes={routes} />
        {routes.map((r) => (
          <Polyline
            key={r.camion}
            positions={r.positions}
            pathOptions={{ color: r.color, weight: 5, opacity: 0.85 }}
          />
        ))}
        {routes.flatMap((r) =>
          r.stops.map((s) => (
            <Marker
              key={`${r.camion}-${s.num}-${s.lat}-${s.lng}`}
              position={[s.lat, s.lng]}
              icon={numberedIcon(s.num, r.color)}
            >
              <Tooltip direction="top" offset={[0, -8]} opacity={0.95}>
                <span className="text-xs font-medium">{s.label}</span>
                <span className="block text-[10px] text-muted-foreground">{r.camion}</span>
              </Tooltip>
            </Marker>
          )),
        )}
      </MapContainer>
    </div>
  )
}
