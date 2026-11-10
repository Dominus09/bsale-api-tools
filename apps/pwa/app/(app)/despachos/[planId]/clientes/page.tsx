"use client"

import Link from "next/link"
import { useParams } from "next/navigation"
import { useEffect, useMemo, useState } from "react"
import { ApiError, listMyClientes, type DriverClientCard } from "@/lib/api"
import { moneyCLP } from "@/lib/delivery-flow"
import { formatDistanceM, haversineMeters } from "@/lib/geo"
import { asAmount } from "@/lib/money-display"
import { resolveSemaphore, validationLabelEs } from "@/lib/semaphore"

type Filter = "all" | "pending" | "delivered"
type SortMode = "route" | "near" | "name"

function isDelivered(c: DriverClientCard) {
  const st = c.attention_status || c.visit_status || "pending"
  return st === "completed" || st === "partial" || st === "not_delivered"
}

function stopLabel(c: DriverClientCard, index: number) {
  if (typeof c.route_order === "number" && c.route_order > 0) return Math.trunc(c.route_order)
  return index + 1
}

export default function ClientesPage() {
  const params = useParams()
  const planId = Number(params.planId)
  const [items, setItems] = useState<DriverClientCard[]>([])
  const [q, setQ] = useState("")
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [offline, setOffline] = useState(false)
  const [filter, setFilter] = useState<Filter>("all")
  const [sortMode, setSortMode] = useState<SortMode>("route")
  const [origin, setOrigin] = useState<{ lat: number; lng: number } | null>(null)

  useEffect(() => {
    function syncOnline() {
      setOffline(typeof navigator !== "undefined" && !navigator.onLine)
    }
    syncOnline()
    window.addEventListener("online", syncOnline)
    window.addEventListener("offline", syncOnline)
    return () => {
      window.removeEventListener("online", syncOnline)
      window.removeEventListener("offline", syncOnline)
    }
  }, [])

  useEffect(() => {
    if (!Number.isFinite(planId) || planId < 1) {
      setError("Despacho inválido")
      setLoading(false)
      return
    }
    let cancelled = false
    const handle = window.setTimeout(async () => {
      setLoading(true)
      try {
        const data = await listMyClientes(planId, q)
        if (cancelled) return
        setItems(data.items || [])
        setError(null)
      } catch (err) {
        if (cancelled) return
        if (!navigator.onLine) {
          setError("Sin conexión. Reintenta cuando vuelva la red.")
        } else if (err instanceof ApiError && (err.status === 404 || err.status === 403)) {
          setError("Despacho no autorizado o no encontrado.")
        } else {
          setError(err instanceof ApiError ? err.message : "Error al cargar clientes")
        }
      } finally {
        if (!cancelled) setLoading(false)
      }
    }, q ? 220 : 0)
    return () => {
      cancelled = true
      window.clearTimeout(handle)
    }
  }, [planId, q])

  useEffect(() => {
    if (sortMode !== "near") return
    if (!navigator.geolocation) return
    const id = navigator.geolocation.watchPosition(
      (pos) => {
        setOrigin({ lat: pos.coords.latitude, lng: pos.coords.longitude })
      },
      () => {
        setOrigin(null)
      },
      { enableHighAccuracy: true, maximumAge: 10000, timeout: 15000 },
    )
    return () => navigator.geolocation.clearWatch(id)
  }, [sortMode])

  const counts = useMemo(() => {
    const pending = items.filter((c) => !isDelivered(c)).length
    const delivered = items.filter((c) => isDelivered(c)).length
    return { all: items.length, pending, delivered }
  }, [items])

  const visible = useMemo(() => {
    let list = items.filter((c) => {
      if (filter === "pending") return !isDelivered(c)
      if (filter === "delivered") return isDelivered(c)
      return true
    })

    const withDist = list.map((c, index) => {
      let distance_m: number | null = null
      if (origin && c.has_coordinates) {
        // Lat/lng no vienen en list cards; distancia solo si se amplía API.
        // Placeholder: sin coords en list, distance queda null salvo que ampliemos.
        distance_m = null
      }
      return { c, index, distance_m }
    })

    if (sortMode === "name") {
      withDist.sort((a, b) =>
        String(a.c.client_name || "").localeCompare(String(b.c.client_name || ""), "es"),
      )
    } else if (sortMode === "near") {
      withDist.sort((a, b) => {
        const aHas = a.c.has_coordinates
        const bHas = b.c.has_coordinates
        if (aHas !== bHas) return aHas ? -1 : 1
        const da = a.distance_m
        const db = b.distance_m
        if (da == null && db == null) return 0
        if (da == null) return 1
        if (db == null) return -1
        return da - db
      })
    } else {
      withDist.sort((a, b) => {
        const ra = a.c.route_order
        const rb = b.c.route_order
        if (ra == null && rb == null) return 0
        if (ra == null) return 1
        if (rb == null) return -1
        return ra - rb
      })
    }

    return withDist
  }, [items, filter, sortMode, origin])

  // Ampliar distancia: pedir mapa liviano solo cuando ordenamos por cercanía.
  const [nearDist, setNearDist] = useState<Record<string, number | null>>({})
  useEffect(() => {
    if (sortMode !== "near" || !origin || !Number.isFinite(planId)) {
      setNearDist({})
      return
    }
    let cancelled = false
    async function loadDistances() {
      try {
        const { getMyMapa } = await import("@/lib/api")
        const data = await getMyMapa(planId, origin)
        if (cancelled) return
        const map: Record<string, number | null> = {}
        for (const m of data.markers || []) {
          map[m.customer_key] =
            typeof m.distance_m === "number"
              ? m.distance_m
              : m.latitude != null && m.longitude != null
                ? haversineMeters(origin!.lat, origin!.lng, m.latitude, m.longitude)
                : null
        }
        setNearDist(map)
      } catch {
        if (!cancelled) setNearDist({})
      }
    }
    void loadDistances()
    return () => {
      cancelled = true
    }
  }, [sortMode, origin, planId])

  const sortedVisible = useMemo(() => {
    if (sortMode !== "near") return visible
    const copy = [...visible]
    copy.sort((a, b) => {
      const da = nearDist[a.c.customer_key]
      const db = nearDist[b.c.customer_key]
      const aHas = a.c.has_coordinates
      const bHas = b.c.has_coordinates
      if (aHas !== bHas) return aHas ? -1 : 1
      if (da == null && db == null) return 0
      if (da == null) return 1
      if (db == null) return -1
      return da - db
    })
    return copy
  }, [visible, sortMode, nearDist])

  return (
    <div className="stack-gap">
      <div>
        <h2 className="section-title">Clientes</h2>
        <p className="muted" style={{ margin: 0, fontSize: "0.82rem" }}>
          {counts.all} en despacho
          {sortMode === "near" ? " · orden por cercanía" : sortMode === "name" ? " · A–Z" : " · orden de ruta"}
        </p>
      </div>

      {offline ? <div className="banner banner-error">Sin conexión a internet.</div> : null}

      <div className="field" style={{ marginBottom: 0 }}>
        <label htmlFor="client-search">Buscar</label>
        <input
          id="client-search"
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="Local, dirección o folio"
          autoComplete="off"
          enterKeyHint="search"
        />
      </div>

      <div className="filter-chips" role="group" aria-label="Filtro de clientes">
        <button
          type="button"
          className={`filter-chip${filter === "all" ? " active" : ""}`}
          onClick={() => setFilter("all")}
        >
          Todos ({counts.all})
        </button>
        <button
          type="button"
          className={`filter-chip${filter === "pending" ? " active" : ""}`}
          onClick={() => setFilter("pending")}
        >
          Pendientes ({counts.pending})
        </button>
        <button
          type="button"
          className={`filter-chip${filter === "delivered" ? " active" : ""}`}
          onClick={() => setFilter("delivered")}
        >
          Entregados ({counts.delivered})
        </button>
      </div>

      <div className="filter-chips" role="group" aria-label="Orden de clientes">
        <button
          type="button"
          className={`filter-chip${sortMode === "route" ? " active" : ""}`}
          onClick={() => setSortMode("route")}
        >
          Orden ruta
        </button>
        <button
          type="button"
          className={`filter-chip${sortMode === "near" ? " active" : ""}`}
          onClick={() => setSortMode("near")}
        >
          Más cercano
        </button>
        <button
          type="button"
          className={`filter-chip${sortMode === "name" ? " active" : ""}`}
          onClick={() => setSortMode("name")}
        >
          Nombre
        </button>
      </div>

      {error ? <div className="banner banner-error">{error}</div> : null}
      {loading ? <div className="empty">Cargando clientes…</div> : null}

      {!loading && !error && sortedVisible.length === 0 ? (
        <div className="empty">No hay clientes para mostrar.</div>
      ) : null}

      <div className="stack-gap">
        {sortedVisible.map(({ c, index }) => {
          const obs = (c.observations || "").trim()
          const amount = asAmount(c.amount_total)
          const dist = formatDistanceM(nearDist[c.customer_key])
          return (
            <Link
              key={c.customer_key}
              href={`/despachos/${planId}/clientes/${encodeURIComponent(c.customer_key)}`}
              className="card tap-card client-card"
            >
              <div className="client-card-head">
                <h3 className="client-card-name">
                  <span className="client-stop">{stopLabel(c, index)}</span>
                  {c.client_name || "Sin nombre"}
                </h3>
                <span className={`pill${c.has_coordinates ? " pill-ok" : ""}`}>
                  {c.has_coordinates ? "GPS" : "Sin GPS"}
                </span>
              </div>

              <p className="muted" style={{ margin: "6px 0 0", fontSize: "0.86rem" }}>
                {[c.city, c.address].filter(Boolean).join(" · ") || "Sin dirección"}
                {dist ? ` · ${dist}` : !c.has_coordinates && sortMode === "near" ? " · sin coordenadas" : ""}
              </p>

              <div className="client-card-meta">
                <span>
                  {c.document_count} doc{c.document_count === 1 ? "" : "s"}
                </span>
                <span>{amount != null ? moneyCLP(amount) : "Monto —"}</span>
              </div>

              {(() => {
                const sem = resolveSemaphore({
                  status: c.attention_status,
                  resultReason: c.result_reason,
                  semaphoreColor: c.semaphore_color,
                  attentionLabel: c.attention_label_es,
                })
                const val = validationLabelEs(c.closure_review_status)
                return (
                  <div className="group-row" style={{ marginTop: 6 }}>
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
                  </div>
                )
              })()}

              <div className={obs ? "obs-block" : "obs-block obs-empty"}>
                {obs ? `Obs: ${obs}` : "Sin observaciones"}
              </div>
            </Link>
          )
        })}
      </div>
    </div>
  )
}
