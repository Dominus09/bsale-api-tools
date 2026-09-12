"use client"

import { useEffect, useMemo, useRef } from "react"
import {
  MapContainer,
  Marker,
  Popup,
  TileLayer,
  useMap,
} from "react-leaflet"
import L from "leaflet"
import type { Map as LeafletMap } from "leaflet"
import "leaflet/dist/leaflet.css"

import type { DeliveryMapStop } from "@/lib/delivery-map-api"

type StopWithDist = DeliveryMapStop & { distance_km?: number | null }

type Props = {
  stops: StopWithDist[]
  myPos: { lat: number; lng: number } | null
  selectedKey: string | null
  focusToken: number
  onSelect: (key: string) => void
  mapRef: React.MutableRefObject<LeafletMap | null>
}

function pinIcon(color: string, selected: boolean) {
  const size = selected ? 28 : 22
  return L.divIcon({
    className: "",
    html: `<div style="width:${size}px;height:${size}px;border-radius:50%;background:${color};border:3px solid #fff;box-shadow:0 1px 4px rgba(0,0,0,.35)"></div>`,
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
  })
}

const meIcon = L.divIcon({
  className: "",
  html: `<div style="width:18px;height:18px;border-radius:50%;background:#2563eb;border:3px solid #fff;box-shadow:0 0 0 6px rgba(37,99,235,.25)"></div>`,
  iconSize: [18, 18],
  iconAnchor: [9, 9],
})

function FitBounds({
  stops,
  myPos,
}: {
  stops: StopWithDist[]
  myPos: { lat: number; lng: number } | null
}) {
  const map = useMap()
  const done = useRef(false)
  useEffect(() => {
    if (done.current && !myPos) return
    const pts: [number, number][] = []
    for (const s of stops) {
      if (s.has_coordinates && s.latitude != null && s.longitude != null) {
        pts.push([s.latitude, s.longitude])
      }
    }
    if (myPos) pts.push([myPos.lat, myPos.lng])
    if (!pts.length) {
      map.setView([-42.48, -73.76], 10)
      return
    }
    if (pts.length === 1) {
      map.setView(pts[0], 14)
      done.current = true
      return
    }
    map.fitBounds(L.latLngBounds(pts), { padding: [36, 36], maxZoom: 14 })
    done.current = true
  }, [map, stops, myPos])
  return null
}

function FocusSelected({
  stop,
  token,
}: {
  stop: StopWithDist | null
  token: number
}) {
  const map = useMap()
  useEffect(() => {
    if (!stop?.has_coordinates || stop.latitude == null || stop.longitude == null) {
      return
    }
    map.flyTo([stop.latitude, stop.longitude], 15, { duration: 0.55 })
  }, [map, stop, token])
  return null
}

function MapRefCapture({
  mapRef,
}: {
  mapRef: React.MutableRefObject<LeafletMap | null>
}) {
  const map = useMap()
  useEffect(() => {
    mapRef.current = map
  }, [map, mapRef])
  return null
}

export default function DeliveryLeafletMap({
  stops,
  myPos,
  selectedKey,
  focusToken,
  onSelect,
  mapRef,
}: Props) {
  const selected = useMemo(
    () => stops.find((s) => s.customer_key === selectedKey) || null,
    [stops, selectedKey],
  )

  const mapped = stops.filter(
    (s) => s.has_coordinates && s.latitude != null && s.longitude != null,
  )

  return (
    <MapContainer
      center={[-42.48, -73.76]}
      zoom={10}
      className="h-full w-full"
      scrollWheelZoom
      zoomControl
    >
      <TileLayer
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a>'
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />
      <MapRefCapture mapRef={mapRef} />
      <FitBounds stops={mapped} myPos={myPos} />
      <FocusSelected stop={selected} token={focusToken} />
      {myPos ? (
        <Marker position={[myPos.lat, myPos.lng]} icon={meIcon}>
          <Popup>Mi ubicación</Popup>
        </Marker>
      ) : null}
      {mapped.map((s) => {
        const color =
          s.status === "delivered"
            ? "#059669"
            : selectedKey === s.customer_key
              ? "#1d4ed8"
              : "#ea580c"
        return (
          <Marker
            key={s.customer_key}
            position={[s.latitude!, s.longitude!]}
            icon={pinIcon(color, selectedKey === s.customer_key)}
            eventHandlers={{
              click: () => onSelect(s.customer_key),
            }}
          >
            <Popup>
              <strong>{s.customer_name}</strong>
              <br />
              {s.address || ""}
            </Popup>
          </Marker>
        )
      })}
    </MapContainer>
  )
}
