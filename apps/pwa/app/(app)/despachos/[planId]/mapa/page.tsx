"use client"

import Link from "next/link"
import { useParams } from "next/navigation"
import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { DeliveryMap } from "@/components/delivery-map"
import { ApiError, getMyMapa, type DriverMapMarker } from "@/lib/api"
import { moneyCLP } from "@/lib/delivery-flow"
import { formatDistanceM } from "@/lib/geo"
import { asAmount } from "@/lib/money-display"
import { resolveSemaphore } from "@/lib/semaphore"

type UserLoc = { lat: number; lng: number; accuracy: number | null }

const GEO_PREF_KEY = "pwa.mapa.liveGps"

type SortMode = "near" | "route"

export default function MapaPage() {
  const params = useParams()
  const planId = Number(params.planId)
  const [markers, setMarkers] = useState<DriverMapMarker[]>([])
  const [emptyMsg, setEmptyMsg] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [userLoc, setUserLoc] = useState<UserLoc | null>(null)
  const [geoNote, setGeoNote] = useState<string | null>(null)
  const [liveGps, setLiveGps] = useState(false)
  const [selectedKey, setSelectedKey] = useState<string | null>(null)
  const [sortMode, setSortMode] = useState<SortMode>("route")
  const watchRef = useRef<number | null>(null)
  const originalOrderRef = useRef<string[]>([])

  const loadMap = useCallback(
    async (origin?: UserLoc | null) => {
      if (!Number.isFinite(planId) || planId < 1) {
        setError("Despacho inválido")
        setLoading(false)
        return
      }
      setLoading(true)
      try {
        const data = await getMyMapa(
          planId,
          origin ? { lat: origin.lat, lng: origin.lng } : undefined,
        )
        const items = data.markers || []
        setMarkers(items)
        if (originalOrderRef.current.length === 0) {
          originalOrderRef.current = items.map((m) => m.customer_key)
        }
        setEmptyMsg(data.empty_state?.message_es || null)
        setError(null)
      } catch (err) {
        if (!navigator.onLine) {
          setError("Sin conexión. Reintenta cuando vuelva la red.")
        } else if (err instanceof ApiError && (err.status === 404 || err.status === 403)) {
          setError("Despacho no autorizado o no encontrado.")
        } else {
          setError(err instanceof ApiError ? err.message : "Error al cargar mapa")
        }
      } finally {
        setLoading(false)
      }
    },
    [planId],
  )

  const stopWatch = useCallback(() => {
    if (watchRef.current != null && navigator.geolocation) {
      navigator.geolocation.clearWatch(watchRef.current)
    }
    watchRef.current = null
  }, [])

  const startWatch = useCallback(() => {
    if (!navigator.geolocation) {
      setGeoNote("Este dispositivo no soporta geolocalización.")
      return
    }
    stopWatch()
    setGeoNote(null)
    watchRef.current = navigator.geolocation.watchPosition(
      (pos) => {
        const loc: UserLoc = {
          lat: pos.coords.latitude,
          lng: pos.coords.longitude,
          accuracy: pos.coords.accuracy ?? null,
        }
        setUserLoc(loc)
        setLiveGps(true)
        try {
          sessionStorage.setItem(GEO_PREF_KEY, "1")
        } catch {
          /* ignore */
        }
        if (pos.coords.accuracy != null && pos.coords.accuracy > 80) {
          setGeoNote(`Señal imprecisa (~${Math.round(pos.coords.accuracy)} m).`)
        } else {
          setGeoNote(null)
        }
      },
      (err) => {
        setLiveGps(false)
        setGeoNote(
          err.code === err.PERMISSION_DENIED
            ? "Permiso de ubicación denegado. Actívelo para ver su posición."
            : "No se pudo obtener la ubicación (sin señal o timeout).",
        )
      },
      { enableHighAccuracy: true, timeout: 15000, maximumAge: 10000 },
    )
  }, [stopWatch])

  useEffect(() => {
    void loadMap(null)
    let pref = false
    try {
      pref = sessionStorage.getItem(GEO_PREF_KEY) === "1"
    } catch {
      pref = false
    }
    if (pref) startWatch()
    return () => stopWatch()
  }, [loadMap, startWatch, stopWatch])

  const orderedOnce = useRef(false)
  useEffect(() => {
    if (!userLoc || orderedOnce.current) return
    orderedOnce.current = true
    void loadMap(userLoc)
  }, [userLoc, loadMap])

  const displayMarkers = useMemo(() => {
    const list = [...markers]
    if (sortMode === "near") {
      list.sort((a, b) => {
        const da = a.distance_m
        const db = b.distance_m
        if (da == null && db == null) return 0
        if (da == null) return 1
        if (db == null) return -1
        return da - db
      })
      return list
    }
    const order = originalOrderRef.current
    if (order.length) {
      list.sort((a, b) => {
        const ia = order.indexOf(a.customer_key)
        const ib = order.indexOf(b.customer_key)
        return (ia < 0 ? 9999 : ia) - (ib < 0 ? 9999 : ib)
      })
    }
    return list
  }, [markers, sortMode])

  const selected = displayMarkers.find((m) => m.customer_key === selectedKey) || null
  const fichaHref = selected
    ? `/despachos/${planId}/clientes/${encodeURIComponent(selected.customer_key)}`
    : null
  const amount = asAmount(selected?.amount_total)
  const distLabel = formatDistanceM(selected?.distance_m)

  return (
    <div className="stack-gap">
      <div className="despacho-hero-head">
        <div>
          <h2 className="section-title" style={{ marginBottom: 2 }}>
            Mapa del despacho
          </h2>
          <p className="muted" style={{ margin: 0, fontSize: "0.8rem" }}>
            {displayMarkers.length} cliente{displayMarkers.length === 1 ? "" : "s"} con GPS
          </p>
        </div>
        <button
          type="button"
          className={`btn btn-compact ${liveGps ? "btn-secondary" : "btn-primary"}`}
          onClick={() => {
            if (liveGps) {
              stopWatch()
              setLiveGps(false)
              try {
                sessionStorage.removeItem(GEO_PREF_KEY)
              } catch {
                /* ignore */
              }
              setGeoNote("Ubicación en vivo desactivada.")
            } else {
              startWatch()
            }
          }}
        >
          {liveGps ? "GPS on" : "Mi GPS"}
        </button>
      </div>

      <div className="filter-chips" role="group" aria-label="Orden del mapa">
        <button
          type="button"
          className={`filter-chip${sortMode === "near" ? " active" : ""}`}
          onClick={() => {
            setSortMode("near")
            if (userLoc) void loadMap(userLoc)
            else startWatch()
          }}
        >
          Ordenar: Más cercano
        </button>
        <button
          type="button"
          className={`filter-chip${sortMode === "route" ? " active" : ""}`}
          onClick={() => setSortMode("route")}
        >
          Orden de ruta
        </button>
      </div>

      {geoNote ? <div className="banner banner-info">{geoNote}</div> : null}
      {error ? <div className="banner banner-error">{error}</div> : null}

      {loading && markers.length === 0 ? <div className="empty">Cargando mapa…</div> : null}

      {!loading && !error && markers.length === 0 ? (
        <div className="card">
          <p style={{ marginTop: 0 }}>{emptyMsg || "Sin clientes georreferenciados."}</p>
          <Link
            href={`/despachos/${planId}/clientes`}
            className="btn btn-primary"
            style={{ textAlign: "center" }}
          >
            Ir a lista de clientes
          </Link>
        </div>
      ) : null}

      {markers.length > 0 ? (
        <div className="map-stage">
          <DeliveryMap
            markers={displayMarkers}
            userLocation={userLoc}
            selectedKey={selectedKey}
            onSelect={(key) => setSelectedKey(key)}
            clientesHref={`/despachos/${planId}/clientes`}
          />

          {selected ? (
            <div className="map-sheet" role="dialog" aria-label="Cliente seleccionado">
              <div className="map-sheet-handle" aria-hidden />
              <div className="despacho-hero-head">
                <h3 className="map-sheet-title">{selected.client_name || "Cliente"}</h3>
                <button
                  type="button"
                  className="btn btn-ghost btn-compact"
                  onClick={() => setSelectedKey(null)}
                  aria-label="Cerrar"
                >
                  ✕
                </button>
              </div>
              <p className="muted" style={{ margin: "4px 0 0", fontSize: "0.85rem" }}>
                {[selected.address, selected.city].filter(Boolean).join(", ") || "Sin dirección"}
              </p>
              <div className="map-sheet-meta">
                {(() => {
                  const sem = resolveSemaphore({
                    status: selected.attention_status,
                    resultReason: selected.result_reason,
                    semaphoreColor: selected.semaphore_color,
                    attentionLabel: selected.attention_label_es,
                  })
                  return <span className={`sem-pill ${sem.cssClass}`}>{sem.labelEs}</span>
                })()}
                {distLabel ? <span className="pill">{distLabel}</span> : null}
                <span className="pill">
                  {selected.document_count} doc{selected.document_count === 1 ? "" : "s"}
                </span>
                <span className="pill">
                  {amount != null ? moneyCLP(amount) : "Monto —"}
                </span>
                <span className="pill pill-ok">GPS disponible</span>
              </div>
              {(selected.observations || "").trim() ? (
                <div className="obs-block">Obs: {(selected.observations || "").trim()}</div>
              ) : null}
              <div className="btn-row" style={{ marginTop: 12 }}>
                {fichaHref ? (
                  <Link href={fichaHref} className="btn btn-secondary" style={{ textAlign: "center" }}>
                    Ver ficha
                  </Link>
                ) : null}
                {selected.navigation?.google_maps_url ? (
                  <a
                    href={selected.navigation.google_maps_url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="btn btn-primary"
                    style={{ textAlign: "center" }}
                  >
                    Navegar
                  </a>
                ) : null}
              </div>
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  )
}
