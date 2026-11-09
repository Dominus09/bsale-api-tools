"use client"

import Link from "next/link"
import { useCallback, useEffect, useRef, useState } from "react"
import type { DriverMapMarker } from "@/lib/api"
import {
  classifyMapFailure,
  classifyMapInitError,
  type ClassifiedMapFailure,
} from "@/lib/map-errors"

type Props = {
  markers: DriverMapMarker[]
  userLocation?: { lat: number; lng: number; accuracy?: number | null } | null
  onSelect?: (customerKey: string) => void
  clientesHref: string
}

const STYLE_URL = "https://tiles.openfreemap.org/styles/liberty"

export function DeliveryMap({ markers, userLocation, onSelect, clientesHref }: Props) {
  const containerRef = useRef<HTMLDivElement | null>(null)
  const mapRef = useRef<import("maplibre-gl").Map | null>(null)
  const styleLoadedRef = useRef(false)
  const [retryToken, setRetryToken] = useState(0)
  const [fatal, setFatal] = useState<ClassifiedMapFailure | null>(null)
  const [degraded, setDegraded] = useState<ClassifiedMapFailure | null>(null)
  const [booting, setBooting] = useState(true)

  const retry = useCallback(() => {
    setFatal(null)
    setDegraded(null)
    setBooting(true)
    setRetryToken((n) => n + 1)
  }, [])

  useEffect(() => {
    let cancelled = false
    let map: import("maplibre-gl").Map | null = null
    styleLoadedRef.current = false

    const onError = (e: { error?: Error; sourceId?: string }) => {
      if (cancelled) return
      const classified = classifyMapFailure(e?.error || e, {
        styleLoaded: styleLoadedRef.current,
        sourceId: e?.sourceId,
      })
      if (classified.kind === "fatal") {
        setFatal(classified)
        setBooting(false)
        try {
          map?.remove()
        } catch {
          /* ignore */
        }
        map = null
        mapRef.current = null
      } else {
        setDegraded(classified)
      }
    }

    async function boot() {
      if (!containerRef.current) return
      try {
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
          attributionControl: {
            compact: true,
            customAttribution:
              '© <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener noreferrer">OpenStreetMap</a> · <a href="https://openfreemap.org/" target="_blank" rel="noopener noreferrer">OpenFreeMap</a>',
          },
        })
        map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right")
        mapRef.current = map
        map.on("error", onError)

        map.on("load", () => {
          if (cancelled || !map) return
          styleLoadedRef.current = true
          setBooting(false)
          setFatal(null)

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
      } catch (err) {
        if (cancelled) return
        setFatal(classifyMapInitError(err))
        setBooting(false)
        try {
          map?.remove()
        } catch {
          /* ignore */
        }
        map = null
        mapRef.current = null
      }
    }

    void boot()
    return () => {
      cancelled = true
      if (map) {
        try {
          map.off("error", onError)
        } catch {
          /* ignore */
        }
        try {
          map.remove()
        } catch {
          /* ignore */
        }
      }
      mapRef.current = null
    }
  }, [markers, userLocation, onSelect, retryToken])

  if (fatal) {
    return (
      <div className="card stack-gap" role="alert">
        <p style={{ margin: 0 }}>{fatal.messageEs}</p>
        <p className="muted" style={{ margin: 0, fontSize: "0.8rem" }}>
          Atribución: © OpenStreetMap · OpenFreeMap
        </p>
        <button type="button" className="btn btn-ghost" onClick={retry}>
          Reintentar mapa
        </button>
        <Link href={clientesHref} className="btn btn-primary" style={{ textAlign: "center" }}>
          Ir a lista de clientes
        </Link>
      </div>
    )
  }

  return (
    <div className="stack-gap">
      {booting ? <p className="muted" style={{ margin: 0 }}>Preparando mapa…</p> : null}
      {degraded ? (
        <div className="banner banner-info" role="status">
          {degraded.messageEs}
          <div style={{ marginTop: 8 }}>
            <Link href={clientesHref} className="muted" style={{ color: "var(--accent)" }}>
              Ir a lista de clientes
            </Link>
          </div>
        </div>
      ) : null}
      <div ref={containerRef} className="map-canvas" role="application" aria-label="Mapa de entregas" />
    </div>
  )
}
