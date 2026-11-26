"use client"

import Link from "next/link"
import { useParams } from "next/navigation"
import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { DeliveryMap } from "@/components/delivery-map"
import { RouteProgressCard } from "@/components/route-progress-card"
import {
  ApiError,
  getMyDespachoProgress,
  getMyMapa,
  type DriverMapClientNoGps,
  type DriverMapMarker,
  type RouteProgress,
} from "@/lib/api"
import { moneyCLP } from "@/lib/delivery-flow"
import { formatDistanceM } from "@/lib/geo"
import { asAmount } from "@/lib/money-display"
import { buildAddressSearchUrl } from "@/lib/navigation"
import { resolveSemaphore, validationLabelEs } from "@/lib/semaphore"

type UserLoc = { lat: number; lng: number; accuracy: number | null }

const GEO_PREF_KEY = "pwa.mapa.liveGps"

type SortMode = "near" | "route"

function isManagedStatus(status: string | null | undefined) {
  const st = status || "pending"
  return st === "completed" || st === "partial" || st === "not_delivered"
}

export default function MapaPage() {
  const params = useParams()
  const planId = Number(params.planId)
  const [markers, setMarkers] = useState<DriverMapMarker[]>([])
  const [withoutGps, setWithoutGps] = useState<DriverMapClientNoGps[]>([])
  const [summary, setSummary] = useState<{
    clients: number
    with_coordinates: number
    without_coordinates: number
  } | null>(null)
  const [progress, setProgress] = useState<RouteProgress | null>(null)
  const [mapEmptyMsg, setMapEmptyMsg] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [userLoc, setUserLoc] = useState<UserLoc | null>(null)
  const [geoNote, setGeoNote] = useState<string | null>(null)
  const [liveGps, setLiveGps] = useState(false)
  const [selectedKey, setSelectedKey] = useState<string | null>(null)
  const [sortMode, setSortMode] = useState<SortMode>("route")
  const watchRef = useRef<number | null>(null)
  const originalOrderRef = useRef<string[]>([])

  const loadProgress = useCallback(
    async (origin?: UserLoc | null, mode: SortMode = "route") => {
      if (!Number.isFinite(planId) || planId < 1) return
      try {
        // Solo pasar origen en modo cercanía; en orden de ruta incluir sin GPS.
        const useOrigin = mode === "near" && origin
        const data = await getMyDespachoProgress(
          planId,
          useOrigin ? { lat: origin.lat, lng: origin.lng } : null,
        )
        setProgress(data.progress || null)
      } catch {
        /* progreso opcional en mapa */
      }
    },
    [planId],
  )

  const loadMap = useCallback(
    async (origin?: UserLoc | null, mode: SortMode = sortMode) => {
      if (!Number.isFinite(planId) || planId < 1) {
        setError("Despacho inválido")
        setLoading(false)
        return
      }
      setLoading(true)
      try {
        const data = await getMyMapa(
          planId,
          mode === "near" && origin
            ? { lat: origin.lat, lng: origin.lng }
            : undefined,
        )
        const items = data.markers || []
        setMarkers(items)
        setWithoutGps(data.without_gps || [])
        setSummary(data.summary || null)
        if (originalOrderRef.current.length === 0) {
          originalOrderRef.current = items.map((m) => m.customer_key)
        }
        setMapEmptyMsg(
          data.map_empty_state?.message_es ||
            data.empty_state?.message_es ||
            null,
        )
        setError(null)
        void loadProgress(origin, mode)
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
    [planId, sortMode, loadProgress],
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
    void loadMap(null, "route")
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
    if (sortMode === "near") void loadMap(userLoc, "near")
    else void loadProgress(userLoc, "route")
  }, [userLoc, loadMap, loadProgress, sortMode])

  useEffect(() => {
    if (!userLoc) return
    void loadProgress(userLoc, sortMode)
  }, [userLoc, loadProgress, sortMode])

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

  const next = progress?.next_customer
  const nextHref = next?.customer_key
    ? `/despachos/${planId}/clientes/${encodeURIComponent(next.customer_key)}`
    : null
  const nextOnMap = Boolean(
    next?.customer_key &&
      displayMarkers.some((m) => m.customer_key === next.customer_key),
  )

  const withGpsN = summary?.with_coordinates ?? markers.length
  const withoutGpsN = summary?.without_coordinates ?? withoutGps.length
  const totalN = summary?.clients ?? withGpsN + withoutGpsN

  function selectNextPending() {
    if (next?.customer_key) {
      if (nextOnMap) {
        setSelectedKey(next.customer_key)
        return
      }
      const el = document.getElementById("clientes-sin-gps")
      el?.scrollIntoView({ behavior: "smooth", block: "start" })
      return
    }
    const pending = displayMarkers.find(
      (m) => !isManagedStatus(m.attention_status || m.visit_status),
    )
    if (pending) {
      setSelectedKey(pending.customer_key)
      return
    }
    if (withoutGps.some((c) => !isManagedStatus(c.attention_status || c.visit_status))) {
      document
        .getElementById("clientes-sin-gps")
        ?.scrollIntoView({ behavior: "smooth", block: "start" })
    }
  }

  return (
    <div className="stack-gap driver-map-page">
      <div className="despacho-hero-head driver-screen-heading">
        <div>
          <h2 className="section-title" style={{ marginBottom: 2 }}>
            Mapa del despacho
          </h2>
          <p className="muted" style={{ margin: 0, fontSize: "0.8rem" }}>
            {withGpsN} con GPS · {withoutGpsN} sin ubicación
            {totalN ? ` · ${totalN} en recorrido` : ""}
            {sortMode === "near" ? " · cercanía geográfica" : " · orden de ruta"}
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
          className={`filter-chip${sortMode === "route" ? " active" : ""}`}
          aria-pressed={sortMode === "route"}
          onClick={() => {
            setSortMode("route")
            void loadMap(userLoc, "route")
          }}
        >
          Orden de ruta
        </button>
        <button
          type="button"
          className={`filter-chip${sortMode === "near" ? " active" : ""}`}
          aria-pressed={sortMode === "near"}
          onClick={() => {
            setSortMode("near")
            if (userLoc) void loadMap(userLoc, "near")
            else startWatch()
          }}
        >
          Más cercanos
        </button>
        <button
          type="button"
          className="filter-chip"
          onClick={() => selectNextPending()}
          disabled={!next && displayMarkers.length === 0 && withoutGps.length === 0}
        >
          Ver siguiente pendiente
        </button>
      </div>

      {sortMode === "near" && withoutGpsN > 0 ? (
        <div className="banner banner-info" style={{ fontSize: "0.82rem" }}>
          Cercanía solo ordena clientes con GPS. {withoutGpsN} sin ubicación
          permanecen en la sección inferior (orden de ruta).
        </div>
      ) : null}

      {geoNote ? <div className="banner banner-info">{geoNote}</div> : null}
      {error ? <div className="banner banner-error">{error}</div> : null}

      {loading && markers.length === 0 && withoutGps.length === 0 ? (
        <div className="empty">Cargando mapa…</div>
      ) : null}

      {markers.length > 0 ? (
        <div className="map-stage">
          {progress ? (
            <RouteProgressCard progress={progress} variant="overlay" />
          ) : null}
          <DeliveryMap
            markers={displayMarkers}
            userLocation={userLoc}
            selectedKey={selectedKey}
            onSelect={(key) => setSelectedKey(key)}
            clientesHref={`/despachos/${planId}/clientes`}
          />

          {selected ? (
            <div className="map-sheet driver-map-selected" role="region" aria-label="Cliente seleccionado">
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
                  const val = validationLabelEs(selected.closure_review_status)
                  return (
                    <>
                      <span className={`sem-pill ${sem.cssClass}`}>{sem.labelEs}</span>
                      {val ? (
                        <span
                          className={`pill${
                            selected.closure_review_status === "pending_review"
                              ? " pill-warn"
                              : ""
                          }`}
                        >
                          {val}
                        </span>
                      ) : null}
                    </>
                  )
                })()}
                {distLabel ? (
                  <span className="pill">
                    {distLabel}
                    <span className="muted"> · en línea recta</span>
                  </span>
                ) : null}
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
          ) : next && nextHref ? (
            <div className="map-sheet driver-map-next" role="region" aria-label="Siguiente cliente pendiente">
              <div className="map-sheet-handle" aria-hidden />
              <p className="muted" style={{ margin: 0, fontSize: "0.75rem" }}>
                Siguiente pendiente · {next.suggestion_reason_es || "Siguiente en ruta"}
                {next.has_coordinates === false ? " · sin GPS" : ""}
              </p>
              <h3 className="map-sheet-title" style={{ marginTop: 4 }}>
                {next.client_name || "Cliente"}
              </h3>
              <div className="btn-row" style={{ marginTop: 10 }}>
                {nextOnMap ? (
                  <button
                    type="button"
                    className="btn btn-secondary"
                    onClick={() => setSelectedKey(next.customer_key)}
                  >
                    Ver en mapa
                  </button>
                ) : null}
                <Link href={nextHref} className="btn btn-primary" style={{ textAlign: "center" }}>
                  Ir a ficha
                </Link>
              </div>
            </div>
          ) : null}
        </div>
      ) : !loading && !error ? (
        <div className="card">
          <p style={{ marginTop: 0 }}>
            {mapEmptyMsg || "Sin clientes georreferenciados en el mapa."}
          </p>
          {withoutGps.length === 0 ? (
            <Link
              href={`/despachos/${planId}/clientes`}
              className="btn btn-primary"
              style={{ textAlign: "center" }}
            >
              Ir a lista de clientes
            </Link>
          ) : null}
        </div>
      ) : null}

      {withoutGps.length > 0 ? (
        <section
          id="clientes-sin-gps"
          className="no-gps-section"
          aria-label="Clientes sin georreferencia"
        >
          <div className="despacho-hero-head">
            <div>
              <h3 className="section-title" style={{ marginBottom: 2 }}>
                Clientes sin georreferencia
              </h3>
              <p className="muted" style={{ margin: 0, fontSize: "0.78rem" }}>
                Siguen en el recorrido. No se inventan coordenadas ni distancias.
              </p>
            </div>
            <span className="pill">{withoutGps.length}</span>
          </div>
          <div className="stack-gap">
            {withoutGps.map((c) => {
              const sem = resolveSemaphore({
                status: c.attention_status,
                resultReason: c.result_reason,
                semaphoreColor: c.semaphore_color,
                attentionLabel: c.attention_label_es,
              })
              const val = validationLabelEs(c.closure_review_status)
              const ficha = `/despachos/${planId}/clientes/${encodeURIComponent(c.customer_key)}`
              const addrUrl = buildAddressSearchUrl({
                address: c.address,
                city: c.city,
                clientName: c.client_name,
              })
              const amt = asAmount(c.amount_total)
              const obs = (c.observations || "").trim()
              return (
                <div key={c.customer_key} className="card no-gps-card">
                  <div className="despacho-hero-head">
                    <h3 className="map-sheet-title" style={{ fontSize: "1rem" }}>
                      {typeof c.route_order === "number" ? (
                        <span className="client-stop">{Math.trunc(c.route_order)}</span>
                      ) : null}
                      {c.client_name || "Cliente"}
                    </h3>
                    <span className="pill">Sin GPS</span>
                  </div>
                  <p className="muted" style={{ margin: "4px 0 0", fontSize: "0.85rem" }}>
                    {[c.address, c.city].filter(Boolean).join(", ") || "Sin dirección"}
                  </p>
                  <div className="map-sheet-meta">
                    <span className={`sem-pill ${sem.cssClass}`}>{sem.labelEs}</span>
                    {val ? (
                      <span
                        className={`pill${
                          c.closure_review_status === "pending_review" ? " pill-warn" : ""
                        }`}
                      >
                        {val}
                      </span>
                    ) : null}
                    <span className="pill">
                      {c.document_count} doc{c.document_count === 1 ? "" : "s"}
                    </span>
                    <span className="pill">{amt != null ? moneyCLP(amt) : "Monto —"}</span>
                  </div>
                  {obs ? <div className="obs-block">Obs: {obs}</div> : null}
                  <p className="muted" style={{ margin: "8px 0 0", fontSize: "0.72rem" }}>
                    {c.location_note_es ||
                      "Ubicación no verificada. Buscar dirección es estimado."}
                  </p>
                  <div className="btn-row" style={{ marginTop: 10 }}>
                    <Link href={ficha} className="btn btn-secondary" style={{ textAlign: "center" }}>
                      Ver ficha
                    </Link>
                    {addrUrl ? (
                      <a
                        href={addrUrl}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="btn btn-primary"
                        style={{ textAlign: "center" }}
                      >
                        Ver dirección
                      </a>
                    ) : null}
                  </div>
                </div>
              )
            })}
          </div>
        </section>
      ) : null}
    </div>
  )
}
