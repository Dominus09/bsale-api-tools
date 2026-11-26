"use client"

import { useParams } from "next/navigation"
import { useEffect, useMemo, useState } from "react"
import { AppLink } from "@/components/app-link"
import { RouteProgressCard } from "@/components/route-progress-card"
import {
  ApiError,
  getMyDespachoProgress,
  listMyClientes,
  type RouteProgress,
} from "@/lib/api"
import { getSessionUserId } from "@/lib/auth"
import { moneyCLP } from "@/lib/delivery-flow"
import { formatDistanceM, haversineMeters } from "@/lib/geo"
import { asAmount } from "@/lib/money-display"
import {
  normalizeClientCards,
  type SafeClientCard,
} from "@/lib/clients-list-safe"
import { isDeviceOffline, readOfflineClients } from "@/lib/offline-load"
import { isLikelyOfflineError } from "@/lib/offline-store"
import { setActivePlanId } from "@/lib/active-plan"
import { offlineSyncRuntime } from "@/lib/offline-sync-runtime"
import { refreshOfflineV2PilotAuthorization } from "@/lib/offline-v2-pilot-auth"
import { resolveSemaphore, validationLabelEs } from "@/lib/semaphore"

type Filter = "all" | "pending" | "in_progress" | "managed" | "incidents"
type SortMode = "route" | "near" | "name"

function attentionOf(c: SafeClientCard) {
  return c.attention_status || c.visit_status || "pending"
}

function isManaged(c: SafeClientCard) {
  const st = attentionOf(c)
  return st === "completed" || st === "partial" || st === "not_delivered"
}

function isIncident(c: SafeClientCard) {
  const st = attentionOf(c)
  return (
    st === "not_delivered" ||
    st === "partial" ||
    c.closure_review_status === "pending_review" ||
    Boolean(c.admin_review_pending)
  )
}

function stopLabel(c: SafeClientCard, index: number) {
  if (typeof c.route_order === "number" && c.route_order > 0) return Math.trunc(c.route_order)
  return index + 1
}

function applyClientItems(
  raw: unknown,
  setItems: (items: SafeClientCard[]) => void,
  setDataWarning: (msg: string | null) => void,
) {
  const { items, skipped } = normalizeClientCards(raw)
  setItems(items)
  setDataWarning(
    skipped > 0
      ? `${skipped} ficha(s) local(es) inválida(s) se omitieron.`
      : null,
  )
  return items
}

export default function ClientesPage() {
  const params = useParams()
  const planId = Number(params.planId)
  const [items, setItems] = useState<SafeClientCard[]>([])
  const [progress, setProgress] = useState<RouteProgress | null>(null)
  const [q, setQ] = useState("")
  const [error, setError] = useState<string | null>(null)
  const [dataWarning, setDataWarning] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [filter, setFilter] = useState<Filter>("all")
  const [sortMode, setSortMode] = useState<SortMode>("route")
  const [origin, setOrigin] = useState<{ lat: number; lng: number } | null>(null)

  useEffect(() => {
    if (!Number.isFinite(planId) || planId < 1) {
      setError("Despacho inválido")
      setLoading(false)
      return
    }
    let cancelled = false
    const handle = window.setTimeout(async () => {
      setLoading(true)

      async function loadFromOffline() {
        if (q.trim()) {
          setError("La búsqueda requiere conexión. Limpia el filtro para ver datos sincronizados.")
          setItems([])
          return
        }
        const outcome = await readOfflineClients(planId)
        if (cancelled) return
        if (outcome.status === "success") {
          applyClientItems(outcome.items, setItems, setDataWarning)
          setError(
            outcome.incomplete
              ? "Descarga incompleta: algunas fichas/productos pueden faltar. Reabre Clientes con internet."
              : null,
          )
          return
        }
        setItems([])
        setError(outcome.message)
      }

      let hasLocal = false
      try {
        // IDB-FIRST: pintar ya; no esperar API (avión / onLine falso).
        if (!q.trim()) {
          const outcome = await readOfflineClients(planId)
          if (!cancelled && outcome.status === "success") {
            applyClientItems(outcome.items, setItems, setDataWarning)
            setError(
              outcome.incomplete
                ? "Descarga incompleta: algunas fichas/productos pueden faltar. Reabre Clientes con internet."
                : null,
            )
            setLoading(false)
            hasLocal = true
          }
        }
        if (isDeviceOffline()) {
          if (!hasLocal) await loadFromOffline()
          return
        }
        // Con snapshot local: revalidar en background (sin bloquear navegación).
        if (hasLocal && !q.trim()) {
          const uid = getSessionUserId()
          if (uid) setActivePlanId(uid, planId)
          // Confirmar/persistir grant V2 mientras hay red (para modo avión después).
          void refreshOfflineV2PilotAuthorization(planId)
          void Promise.all([
            listMyClientes(planId, q),
            getMyDespachoProgress(planId).catch(() => null),
          ])
            .then(([data, prog]) => {
              if (cancelled) return
              applyClientItems(data.items || [], setItems, setDataWarning)
              if (prog?.progress) setProgress(prog.progress)
              setError(null)
            })
            .catch(() => undefined)
          return
        }
        const [data, prog] = await Promise.all([
          listMyClientes(planId, q),
          getMyDespachoProgress(planId).catch(() => null),
        ])
        if (cancelled) return
        applyClientItems(data.items || [], setItems, setDataWarning)
        if (prog?.progress) setProgress(prog.progress)
        setError(null)
        if (!cancelled) setLoading(false)
        const uid = getSessionUserId()
        if (uid && !q.trim() && !cancelled) {
          setActivePlanId(uid, planId)
          void refreshOfflineV2PilotAuthorization(planId)
          const st = offlineSyncRuntime.getState()
          if (st.plan?.id === planId && st.phase === "syncing") {
            /* join in-flight */
          } else if (st.priority?.planId === planId || st.plan?.id === planId) {
            void offlineSyncRuntime
              .syncPlan(planId, { force: false, plan: st.plan || undefined })
              .catch(() => undefined)
          }
        }
        return
      } catch (err) {
        if (cancelled || hasLocal) return
        if (isLikelyOfflineError(err) || isDeviceOffline()) {
          await loadFromOffline()
          return
        }
        if (err instanceof ApiError && (err.status === 404 || err.status === 403)) {
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

  useEffect(() => {
    if (!origin || !Number.isFinite(planId)) return
    if (isDeviceOffline()) return
    let cancelled = false
    const timer = window.setTimeout(() => {
      void getMyDespachoProgress(planId, origin)
        .then((res) => {
          if (!cancelled && res.progress) setProgress(res.progress)
        })
        .catch(() => undefined)
    }, 1500)
    return () => {
      cancelled = true
      window.clearTimeout(timer)
    }
  }, [origin, planId])

  const counts = useMemo(() => {
    // Contadores globales del listado (no del filtro activo).
    const pending = items.filter((c) => !isManaged(c) && attentionOf(c) !== "in_progress").length
    const inProgress = items.filter((c) => attentionOf(c) === "in_progress").length
    const managed = items.filter((c) => isManaged(c)).length
    const incidents = items.filter((c) => isIncident(c)).length
    return { all: items.length, pending, in_progress: inProgress, managed, incidents }
  }, [items])

  const visible = useMemo(() => {
    let list = items.filter((c) => {
      if (filter === "pending") return !isManaged(c) && attentionOf(c) !== "in_progress"
      if (filter === "in_progress") return attentionOf(c) === "in_progress"
      if (filter === "managed") return isManaged(c)
      if (filter === "incidents") return isIncident(c)
      return true
    })

    const withDist = list.map((c, index) => ({ c, index, distance_m: null as number | null }))

    if (sortMode === "name") {
      withDist.sort((a, b) =>
        String(a.c.client_name || "").localeCompare(String(b.c.client_name || ""), "es"),
      )
    } else if (sortMode === "near") {
      withDist.sort((a, b) => {
        const aHas = a.c.has_coordinates
        const bHas = b.c.has_coordinates
        if (aHas !== bHas) return aHas ? -1 : 1
        return 0
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
  }, [items, filter, sortMode])

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

  const remainingGlobal =
    progress?.remaining_clients ??
    items.filter((c) => !isManaged(c)).length

  const nextHref = progress?.next_customer?.customer_key
    ? `/despachos/${planId}/clientes/${encodeURIComponent(progress.next_customer.customer_key)}`
    : null

  return (
    <div className="stack-gap">
      <div>
        <h2 className="section-title">Clientes</h2>
        <p className="muted" style={{ margin: 0, fontSize: "0.82rem" }}>
          Faltan {remainingGlobal} cliente{remainingGlobal === 1 ? "" : "s"}
          {sortMode === "near"
            ? " · orden por cercanía geográfica"
            : sortMode === "name"
              ? " · A–Z"
              : " · orden de ruta"}
        </p>
      </div>

      {progress ? (
        <RouteProgressCard progress={progress} variant="compact" nextHref={nextHref} />
      ) : null}

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
          className={`filter-chip${filter === "in_progress" ? " active" : ""}`}
          onClick={() => setFilter("in_progress")}
        >
          En atención ({counts.in_progress})
        </button>
        <button
          type="button"
          className={`filter-chip${filter === "managed" ? " active" : ""}`}
          onClick={() => setFilter("managed")}
        >
          Gestionados ({counts.managed})
        </button>
        <button
          type="button"
          className={`filter-chip${filter === "incidents" ? " active" : ""}`}
          onClick={() => setFilter("incidents")}
        >
          Incidencias ({counts.incidents})
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
      {dataWarning ? (
        <p className="muted" style={{ margin: 0, fontSize: "0.78rem" }} role="status">
          {dataWarning}
        </p>
      ) : null}
      {loading && items.length === 0 ? (
        <div className="empty">Cargando clientes…</div>
      ) : null}

      {!loading && !error && sortedVisible.length === 0 ? (
        <div className="empty">No hay clientes para mostrar.</div>
      ) : null}

      <div className="stack-gap">
        {sortedVisible.map(({ c, index }) => {
          const obs = (c.observations || "").trim()
          const amount = asAmount(c.amount_total)
          const dist = formatDistanceM(nearDist[c.customer_key])
          const st = attentionOf(c)
          const sem = resolveSemaphore({
            status: st,
            resultReason: c.result_reason,
            semaphoreColor: c.semaphore_color,
            attentionLabel: c.attention_label_es,
          })
          const label =
            st === "pending" || (!c.visit_status && !c.attention_status)
              ? "Pendiente"
              : st === "in_progress"
                ? "En atención"
                : sem.labelEs
          const val = validationLabelEs(c.closure_review_status)
          return (
            <AppLink
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
                {dist
                  ? ` · ${dist}`
                  : !c.has_coordinates && sortMode === "near"
                    ? " · sin coordenadas"
                    : ""}
              </p>

              <div className="client-card-meta">
                <span>
                  {c.document_count} doc{c.document_count === 1 ? "" : "s"}
                </span>
                <span>{amount != null ? moneyCLP(amount) : "Monto —"}</span>
              </div>

              <div className="group-row" style={{ marginTop: 6 }}>
                <span className={`sem-pill ${sem.cssClass}`}>{label}</span>
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

              <div className={obs ? "obs-block" : "obs-block obs-empty"}>
                {obs ? `Obs: ${obs}` : "Sin observaciones"}
              </div>
            </AppLink>
          )
        })}
      </div>
    </div>
  )
}
