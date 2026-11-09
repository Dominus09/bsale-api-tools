"use client"

import Link from "next/link"
import { useCallback, useEffect, useRef, useState } from "react"
import type { DriverMapMarker } from "@/lib/api"
import {
  classifyMapFailure,
  classifyMapInitError,
  classifyMapTimeout,
  type ClassifiedMapFailure,
} from "@/lib/map-errors"

type Props = {
  markers: DriverMapMarker[]
  userLocation?: { lat: number; lng: number; accuracy?: number | null } | null
  onSelect?: (customerKey: string) => void
  clientesHref: string
}

const STYLE_URL = "https://tiles.openfreemap.org/styles/liberty"
const WORKER_URL = "/maplibre-gl-worker.mjs"
const LOAD_TIMEOUT_MS = 20000

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
    let timeoutId: ReturnType<typeof setTimeout> | null = null
    styleLoadedRef.current = false

    const onError = (e: { error?: Error; sourceId?: string }) => {
      if (cancelled) return
      const classified = classifyMapFailure(e?.error || e, {
        styleLoaded: styleLoadedRef.current,
        sourceId: e?.sourceId,
        phase: e?.sourceId ? "source" : "style",
      })
      if (classified.kind === "fatal") {
        setFatal(classified)
        setBooting(false)
      } else {
        setDegraded(classified)
      }
    }

    async function boot() {
      if (!containerRef.current) return
      try {
        const ml = await import("maplibre-gl")
        await import("maplibre-gl/dist/maplibre-gl.css")
        if (cancelled || !containerRef.current) return

        // v6 + webpack/Next: import.meta.url del worker no resuelve; servir desde /public.
        if (typeof ml.setWorkerUrl === "function") {
          ml.setWorkerUrl(`${window.location.origin}${WORKER_URL}`)
        }

        const MapCtor = ml.Map
        const Marker = ml.Marker
        const NavigationControl = ml.NavigationControl
        const LngLatBounds = ml.LngLatBounds

        const first = markers[0]
        const center: [number, number] = first
          ? [first.longitude, first.latitude]
          : userLocation
            ? [userLocation.lng, userLocation.lat]
            : [-73.8, -42.5]

        map = new MapCtor({
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
        map.addControl(new NavigationControl({ showCompass: false }), "top-right")
        mapRef.current = map
        map.on("error", onError)

        timeoutId = setTimeout(() => {
          if (cancelled || styleLoadedRef.current) return
          setFatal(classifyMapTimeout())
          setBooting(false)
        }, LOAD_TIMEOUT_MS)

        map.on("load", () => {
          if (cancelled || !map) return
          styleLoadedRef.current = true
          if (timeoutId) {
            clearTimeout(timeoutId)
            timeoutId = null
          }
          setBooting(false)
          setFatal(null)

          const bounds = new LngLatBounds()
          let hasBound = false

          for (const m of markers) {
            const el = document.createElement("button")
            el.type = "button"
            el.className = "map-marker"
            el.setAttribute("aria-label", m.client_name || "Cliente")
            el.addEventListener("click", () => onSelect?.(m.customer_key))
            new Marker({ element: el }).setLngLat([m.longitude, m.latitude]).addTo(map)
            bounds.extend([m.longitude, m.latitude])
            hasBound = true
          }

          if (userLocation) {
            const el = document.createElement("div")
            el.className = "map-user"
            new Marker({ element: el })
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
      if (timeoutId) clearTimeout(timeoutId)
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

  return (
    <div className="stack-gap">
      {fatal ? (
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
      ) : null}

      {!fatal && booting ? (
        <p className="muted" style={{ margin: 0 }}>
          Preparando mapa…
        </p>
      ) : null}

      {!fatal && degraded ? (
        <div className="banner banner-info" role="status">
          {degraded.messageEs}
          <div style={{ marginTop: 8 }}>
            <Link href={clientesHref} className="muted" style={{ color: "var(--accent)" }}>
              Ir a lista de clientes
            </Link>
          </div>
        </div>
      ) : null}

      {/* Contenedor siempre montado (salvo fatal de init) para no perder el ref mid-boot. */}
      <div
        ref={containerRef}
        className="map-canvas"
        role="application"
        aria-label="Mapa de entregas"
        style={fatal ? { display: "none" } : undefined}
      />
    </div>
  )
}
