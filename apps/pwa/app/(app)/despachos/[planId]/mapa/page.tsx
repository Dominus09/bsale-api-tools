"use client"

import Link from "next/link"
import { useParams, useRouter } from "next/navigation"
import { useCallback, useEffect, useState } from "react"
import { DeliveryMap } from "@/components/delivery-map"
import { ApiError, getMyMapa, type DriverMapMarker } from "@/lib/api"

type UserLoc = { lat: number; lng: number; accuracy: number | null }

export default function MapaPage() {
  const params = useParams()
  const router = useRouter()
  const planId = Number(params.planId)
  const [markers, setMarkers] = useState<DriverMapMarker[]>([])
  const [emptyMsg, setEmptyMsg] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [userLoc, setUserLoc] = useState<UserLoc | null>(null)
  const [geoNote, setGeoNote] = useState<string | null>(null)
  const [permPending, setPermPending] = useState(false)

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

  useEffect(() => {
    void loadMap(null)
  }, [loadMap])

  async function requestLocation() {
    if (!navigator.geolocation) {
      setGeoNote("Este dispositivo no soporta geolocalización.")
      return
    }
    setPermPending(true)
    setGeoNote(null)
    navigator.geolocation.getCurrentPosition(
      (pos) => {
        const loc: UserLoc = {
          lat: pos.coords.latitude,
          lng: pos.coords.longitude,
          accuracy: pos.coords.accuracy ?? null,
        }
        setUserLoc(loc)
        setPermPending(false)
        void loadMap(loc)
      },
      (err) => {
        setPermPending(false)
        setGeoNote(
          err.code === err.PERMISSION_DENIED
            ? "Permiso de ubicación denegado."
            : "No se pudo obtener la ubicación.",
        )
      },
      { enableHighAccuracy: true, timeout: 12000, maximumAge: 15000 },
    )
  }

  const onSelect = useCallback(
    (customerKey: string) => {
      router.push(`/despachos/${planId}/clientes/${encodeURIComponent(customerKey)}`)
    },
    [planId, router],
  )

  return (
    <div className="stack-gap">
      <div>
        <h2 style={{ margin: "0 0 4px", fontSize: "1.15rem" }}>Mapa</h2>
        <p className="muted" style={{ margin: 0 }}>
          Solo coordenadas verificables · sin tracking permanente
        </p>
      </div>

      <button
        type="button"
        className="btn btn-ghost"
        onClick={() => void requestLocation()}
        disabled={permPending}
      >
        {permPending ? "Obteniendo ubicación…" : "Usar mi ubicación"}
      </button>
      {userLoc ? (
        <p className="muted" style={{ margin: 0 }}>
          Precisión aprox. {userLoc.accuracy != null ? `${Math.round(userLoc.accuracy)} m` : "—"}
        </p>
      ) : null}
      {geoNote ? <div className="banner banner-info">{geoNote}</div> : null}
      {error ? <div className="banner banner-error">{error}</div> : null}

      {loading ? <div className="empty">Cargando mapa…</div> : null}

      {!loading && !error && markers.length === 0 ? (
        <div className="card">
          <p style={{ marginTop: 0 }}>{emptyMsg || "Sin clientes georreferenciados."}</p>
          <Link href={`/despachos/${planId}/clientes`} className="btn btn-primary" style={{ textAlign: "center" }}>
            Ir a lista de clientes
          </Link>
        </div>
      ) : null}

      {!loading && markers.length > 0 ? (
        <>
          <DeliveryMap markers={markers} userLocation={userLoc} onSelect={onSelect} />
          <div className="stack-gap">
            <p className="muted" style={{ margin: 0 }}>
              {markers.length} marcador{markers.length === 1 ? "" : "es"}
              {userLoc ? " · ordenados por proximidad" : ""}
            </p>
            {markers.slice(0, 8).map((m) => (
              <div key={m.customer_key} className="card tap-card" style={{ padding: 12 }}>
                <div className="client-card-head">
                  <button
                    type="button"
                    className="linkish"
                    onClick={() => onSelect(m.customer_key)}
                  >
                    {m.client_name}
                  </button>
                  {m.distance_m != null ? (
                    <span className="pill">
                      {m.distance_m >= 1000
                        ? `${(m.distance_m / 1000).toFixed(1)} km`
                        : `${Math.round(m.distance_m)} m`}
                    </span>
                  ) : null}
                </div>
                <div className="muted">{m.address || m.city || "—"}</div>
                {m.navigation?.google_maps_url ? (
                  <a
                    href={m.navigation.google_maps_url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="muted"
                    style={{ color: "var(--accent)", display: "inline-block", marginTop: 6 }}
                  >
                    Navegar
                  </a>
                ) : null}
              </div>
            ))}
          </div>
        </>
      ) : null}
    </div>
  )
}
