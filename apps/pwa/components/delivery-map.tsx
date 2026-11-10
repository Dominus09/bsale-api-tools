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
  selectedKey?: string | null
  clientesHref: string
}

const STYLE_URL = "https://tiles.openfreemap.org/styles/dark"
const WORKER_URL = "/maplibre-gl-worker.mjs"
const LOAD_TIMEOUT_MS = 20000

function stopNumber(m: DriverMapMarker, index: number): number {
  if (typeof m.route_order === "number" && Number.isFinite(m.route_order) && m.route_order > 0) {
    return Math.trunc(m.route_order)
  }
  return index + 1
}

export function DeliveryMap({
  markers,
  userLocation,
  onSelect,
  selectedKey,
  clientesHref,
}: Props) {
  const containerRef = useRef<HTMLDivElement | null>(null)
  const mapRef = useRef<import("maplibre-gl").Map | null>(null)
  const markerRefs = useRef<import("maplibre-gl").Marker[]>([])
  const userMarkerRef = useRef<import("maplibre-gl").Marker | null>(null)
  const MarkerCtorRef = useRef<typeof import("maplibre-gl").Marker | null>(null)
  const styleLoadedRef = useRef(false)
  const onSelectRef = useRef(onSelect)
  const [retryToken, setRetryToken] = useState(0)
  const [fatal, setFatal] = useState<ClassifiedMapFailure | null>(null)
  const [degraded, setDegraded] = useState<ClassifiedMapFailure | null>(null)
  const [booting, setBooting] = useState(true)

  useEffect(() => {
    onSelectRef.current = onSelect
  }, [onSelect])

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
    markerRefs.current = []
    userMarkerRef.current = null

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

        if (typeof ml.setWorkerUrl === "function") {
          ml.setWorkerUrl(`${window.location.origin}${WORKER_URL}`)
        }

        const MapCtor = ml.Map
        const Marker = ml.Marker
        const NavigationControl = ml.NavigationControl
        const LngLatBounds = ml.LngLatBounds
        MarkerCtorRef.current = Marker

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

          markers.forEach((m, index) => {
            const el = document.createElement("button")
            el.type = "button"
            el.className = "map-marker map-marker-pending"
            el.textContent = String(stopNumber(m, index))
            el.setAttribute(
              "aria-label",
              `${m.client_name || "Cliente"} · ${m.attention_label_es || "Pendiente"}`,
            )
            el.addEventListener("click", (ev) => {
              ev.stopPropagation()
              onSelectRef.current?.(m.customer_key)
            })
            const marker = new Marker({ element: el }).setLngLat([m.longitude, m.latitude]).addTo(map!)
            markerRefs.current.push(marker)
            bounds.extend([m.longitude, m.latitude])
            hasBound = true
          })

          if (userLocation) {
            const el = document.createElement("div")
            el.className = "map-user"
            el.setAttribute("aria-label", "Mi ubicación")
            userMarkerRef.current = new Marker({ element: el })
              .setLngLat([userLocation.lng, userLocation.lat])
              .addTo(map!)
            bounds.extend([userLocation.lng, userLocation.lat])
            hasBound = true
          }

          if (hasBound && markers.length > 1) {
            map.fitBounds(bounds, { padding: 56, maxZoom: 14 })
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
      for (const mk of markerRefs.current) {
        try {
          mk.remove()
        } catch {
          /* ignore */
        }
      }
      markerRefs.current = []
      if (userMarkerRef.current) {
        try {
          userMarkerRef.current.remove()
        } catch {
          /* ignore */
        }
        userMarkerRef.current = null
      }
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
    // Solo remount por markers / retry; GPS se actualiza aparte.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [markers, retryToken])

  useEffect(() => {
    const map = mapRef.current
    const Marker = MarkerCtorRef.current
    if (!map || !styleLoadedRef.current || !Marker) return
    if (!userLocation) {
      if (userMarkerRef.current) {
        try {
          userMarkerRef.current.remove()
        } catch {
          /* ignore */
        }
        userMarkerRef.current = null
      }
      return
    }
    if (userMarkerRef.current) {
      userMarkerRef.current.setLngLat([userLocation.lng, userLocation.lat])
      return
    }
    const el = document.createElement("div")
    el.className = "map-user"
    el.setAttribute("aria-label", "Mi ubicación")
    userMarkerRef.current = new Marker({ element: el })
      .setLngLat([userLocation.lng, userLocation.lat])
      .addTo(map)
  }, [userLocation])

  useEffect(() => {
    // Resaltar pin seleccionado sin recrear el mapa.
    markerRefs.current.forEach((mk, i) => {
      const el = mk.getElement()
      if (!el) return
      const key = markers[i]?.customer_key
      if (key && key === selectedKey) {
        el.style.transform = "scale(1.15)"
        el.style.boxShadow = "0 0 0 3px rgba(196,41,60,0.45), 0 2px 8px rgba(0,0,0,0.45)"
      } else {
        el.style.transform = ""
        el.style.boxShadow = ""
      }
    })
  }, [selectedKey, markers])

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
