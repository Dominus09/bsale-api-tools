"use client"

import Link from "next/link"
import { useParams } from "next/navigation"
import { useCallback, useEffect, useRef, useState } from "react"
import { DeliveryMap } from "@/components/delivery-map"
import { ApiError, getMyMapa, type DriverMapMarker } from "@/lib/api"
import { moneyCLP } from "@/lib/delivery-flow"
type UserLoc = { lat: number; lng: number; accuracy: number | null }

const GEO_PREF_KEY = "pwa.mapa.liveGps"

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
  const watchRef = useRef<number | null>(null)

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
        setMarkers(data.markers || [])
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
      watchRef.current = null
    }
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

  // Primera fijación GPS: una sola recarga para ordenar por proximidad (sin tracking al servidor).
  const orderedOnce = useRef(false)
  useEffect(() => {
    if (!userLoc || orderedOnce.current) return
    orderedOnce.current = true
    void loadMap(userLoc)
  }, [userLoc, loadMap])

  const selected = markers.find((m) => m.customer_key === selectedKey) || null
  const fichaHref = selected
    ? `/despachos/${planId}/clientes/${encodeURIComponent(selected.customer_key)}`
    : null

  return (
    <div className="stack-gap">
      <div>
        <h2 style={{ margin: "0 0 4px", fontSize: "1.15rem" }}>Mapa</h2>
        <p className="muted" style={{ margin: 0 }}>
          Coordenadas de clientes · ubicación propia solo en este dispositivo
        </p>
      </div>

      <div className="btn-row">
        <button
          type="button"
          className={`btn ${liveGps ? "btn-secondary" : "btn-primary"}`}
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
          {liveGps ? "Desactivar mi ubicación" : "Activar mi ubicación"}
        </button>
      </div>
      {userLoc ? (
        <p className="muted" style={{ margin: 0 }}>
          Precisión aprox.{" "}
          {userLoc.accuracy != null ? `${Math.round(userLoc.accuracy)} m` : "—"}
          {liveGps ? " · en vivo" : ""}
        </p>
      ) : null}
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
        <>
          <DeliveryMap
            markers={markers}
            userLocation={userLoc}
            onSelect={(key) => setSelectedKey(key)}
            clientesHref={`/despachos/${planId}/clientes`}
          />
          <p className="muted" style={{ margin: 0 }}>
            {markers.length} marcador{markers.length === 1 ? "" : "es"}
            {userLoc ? " · cercanos primero" : ""}
            . Pulse un pin para ver la ficha.
          </p>

          {selected ? (
            <div className="map-sheet" role="dialog" aria-label="Cliente seleccionado">
              <strong style={{ fontSize: "1.05rem" }}>
                {selected.client_name || "Cliente"}
              </strong>
              <p className="muted" style={{ margin: "4px 0 8px" }}>
                {[selected.address, selected.city].filter(Boolean).join(", ") || "Sin dirección"}
              </p>
              <div className="meta-row">
                <span className="muted">Documentos</span>
                <span>{selected.document_count}</span>
              </div>
              <div className="meta-row">
                <span className="muted">Monto</span>
                <span>
                  {typeof selected.amount_total === "number"
                    ? moneyCLP(selected.amount_total)
                    : "Monto no disponible"}
                </span>
              </div>
              <div className="meta-row">
                <span className="muted">Estado</span>
                <span>{selected.attention_label_es || "Pendiente"}</span>
              </div>
              <p className="muted" style={{ fontSize: "0.75rem", margin: "6px 0 10px" }}>
                Coordenadas precargadas · no implica GPS verificado en terreno
              </p>
              <div className="btn-row">
                {fichaHref ? (
                  <Link href={fichaHref} className="btn btn-primary" style={{ textAlign: "center" }}>
                    Ver ficha
                  </Link>
                ) : null}
                {selected.navigation?.google_maps_url ? (
                  <a
                    href={selected.navigation.google_maps_url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="btn btn-secondary"
                    style={{ textAlign: "center" }}
                  >
                    Navegar
                  </a>
                ) : null}
              </div>
              <button
                type="button"
                className="btn btn-ghost"
                style={{ marginTop: 8 }}
                onClick={() => setSelectedKey(null)}
              >
                Cerrar
              </button>
            </div>
          ) : null}
        </>
      ) : null}

    </div>
  )
}
