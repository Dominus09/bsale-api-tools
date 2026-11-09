"use client"

import { useEffect, useRef } from "react"
import type { DriverMapMarker } from "@/lib/api"

type Props = {
  markers: DriverMapMarker[]
  userLocation?: { lat: number; lng: number; accuracy?: number | null } | null
  onSelect?: (customerKey: string) => void
}

const STYLE_URL = "https://tiles.openfreemap.org/styles/liberty"

export function DeliveryMap({ markers, userLocation, onSelect }: Props) {
  const containerRef = useRef<HTMLDivElement | null>(null)
  const mapRef = useRef<import("maplibre-gl").Map | null>(null)

  useEffect(() => {
    let cancelled = false
    let map: import("maplibre-gl").Map | null = null

    async function boot() {
      if (!containerRef.current) return
      const maplibregl = (await import("maplibre-gl")).default
      await import("maplibre-gl/dist/maplibre-gl.css")
      if (cancelled || !containerRef.current) return

      const first = markers[0]
      const center: [number, number] = first
        ? [first.longitude, first.latitude]
        : userLocation
          ? [userLocation.lng, userLocation.lat]
          : [-73.8, -42.5]

      map = new maplibregl.Map({
        container: containerRef.current,
        style: STYLE_URL,
        center,
        zoom: markers.length ? 11 : 8,
        attributionControl: { compact: true },
      })
      map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right")
      mapRef.current = map

      map.on("load", () => {
        if (!map) return
        const bounds = new maplibregl.LngLatBounds()
        let hasBound = false

        for (const m of markers) {
          const el = document.createElement("button")
          el.type = "button"
          el.className = "map-marker"
          el.setAttribute("aria-label", m.client_name || "Cliente")
          el.addEventListener("click", () => onSelect?.(m.customer_key))
          new maplibregl.Marker({ element: el })
            .setLngLat([m.longitude, m.latitude])
            .addTo(map)
          bounds.extend([m.longitude, m.latitude])
          hasBound = true
        }

        if (userLocation) {
          const el = document.createElement("div")
          el.className = "map-user"
          new maplibregl.Marker({ element: el })
            .setLngLat([userLocation.lng, userLocation.lat])
            .addTo(map)
          bounds.extend([userLocation.lng, userLocation.lat])
          hasBound = true
        }

        if (hasBound && markers.length > 1) {
          map.fitBounds(bounds, { padding: 48, maxZoom: 14 })
        } else if (markers.length === 1) {
          map.setCenter([markers[0].longitude, markers[0].latitude])
          map.setZoom(13)
        }
      })
    }

    void boot()
    return () => {
      cancelled = true
      map?.remove()
      mapRef.current = null
    }
  }, [markers, userLocation, onSelect])

  return <div ref={containerRef} className="map-canvas" role="application" aria-label="Mapa de entregas" />
}
