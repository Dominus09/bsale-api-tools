"use client"

import Link from "next/link"
import { useEffect, useMemo, useState } from "react"
import { RouteProgressCard } from "@/components/route-progress-card"
import {
  ApiError,
  getMyDespachoProgress,
  listMyDespachos,
  type DriverDespacho,
  type RouteProgress,
} from "@/lib/api"
import { getSessionUserId } from "@/lib/auth"
import { moneyCLP } from "@/lib/delivery-flow"
import { formatDate, statusLabel } from "@/lib/format"
import { asAmount } from "@/lib/money-display"
import {
  formatSyncedAt,
  getOfflinePlans,
  isLikelyOfflineError,
  listOfflineReadyPlans,
  saveOfflineMeta,
  saveOfflinePlans,
} from "@/lib/offline-store"
import { partitionPlansByDate, santiagoTodayYmd } from "@/lib/plan-buckets"

function PlanCard(props: {
  plan: DriverDespacho
  hero?: boolean
  compact?: boolean
  cta?: string
  progress?: RouteProgress | null
}) {
  const p = props.plan
  const amount = asAmount(p.amount_total)
  const docs =
    typeof p.document_count === "number" ? p.document_count : null
  const clients =
    typeof p.client_count === "number" ? p.client_count : null
  const remaining = props.progress?.remaining_clients
  const hasPendingOps =
    props.progress != null &&
    (props.progress.remaining_clients > 0 ||
      (props.progress.by_result?.in_progress || 0) > 0)
  const cta =
    props.cta ||
    (props.compact
      ? hasPendingOps
        ? "Continuar recorrido"
        : "Ver detalle"
      : "Abrir despacho")

  if (props.compact) {
    return (
      <article className="card plan-card-flow history-card-compact">
        <div className="plan-card-body">
          <div className="despacho-hero-head">
            <div>
              <h2 style={{ margin: 0, fontSize: "1rem" }}>
                {p.planning_code || `Plan #${p.id}`}
              </h2>
              <p className="muted" style={{ margin: "2px 0 0", fontSize: "0.78rem" }}>
                {formatDate(p.planning_date)} · {p.route_name || "Sin ruta"}
              </p>
            </div>
            <span className={`pill${p.status === "picking_generated" ? " pill-ok" : ""}`}>
              {statusLabel(p.status)}
            </span>
          </div>
          <div className="client-card-meta plan-card-meta">
            <span>
              {clients ?? "—"} cliente{(clients || 0) === 1 ? "" : "s"}
            </span>
            <span>{docs ?? "—"} doc{(docs || 0) === 1 ? "" : "s"}</span>
            <span className="plan-card-amount">
              {amount != null ? moneyCLP(amount) : "Monto —"}
            </span>
          </div>
          {props.progress ? (
            <RouteProgressCard progress={props.progress} variant="compact" />
          ) : null}
          {remaining != null && remaining > 0 && !props.progress ? (
            <p className="muted plan-card-remaining">
              Faltan {remaining} cliente{remaining === 1 ? "" : "s"} por gestionar
            </p>
          ) : null}
        </div>
        <div className="plan-card-footer">
          <Link
            href={`/despachos/${p.id}`}
            className={`btn ${hasPendingOps ? "btn-primary" : "btn-secondary"}`}
          >
            {cta}
          </Link>
        </div>
      </article>
    )
  }

  return (
    <article className={`card plan-card-flow${props.hero ? " despacho-hero" : ""}`}>
      <div className="plan-card-body">
        <div className="despacho-hero-head">
          <h2 style={{ margin: 0, fontSize: props.hero ? "1.15rem" : "1.02rem" }}>
            {p.planning_code || `Plan #${p.id}`}
          </h2>
          <span className={`pill${p.status === "picking_generated" ? " pill-ok" : ""}`}>
            {statusLabel(p.status)}
          </span>
        </div>
        {!props.hero ? (
          <div className="muted" style={{ marginTop: 4, fontSize: "0.8rem" }}>
            {formatDate(p.planning_date)}
          </div>
        ) : null}

        <div className="despacho-hero-grid">
          <div>
            <span className="meta-label">Ruta</span>
            <span className="meta-value">{p.route_name || "—"}</span>
          </div>
          <div>
            <span className="meta-label">Vehículo</span>
            <span className="meta-value">{p.truck_name || "—"}</span>
          </div>
          <div>
            <span className="meta-label">Patente</span>
            <span className="meta-value">{p.truck_plate || "—"}</span>
          </div>
          <div>
            <span className="meta-label">Clientes</span>
            <span className="meta-value">{clients ?? "—"}</span>
          </div>
          <div>
            <span className="meta-label">Documentos</span>
            <span className="meta-value">{docs ?? "—"}</span>
          </div>
          <div>
            <span className="meta-label">Fecha</span>
            <span className="meta-value">{formatDate(p.planning_date)}</span>
          </div>
        </div>

        {props.progress ? (
          <RouteProgressCard progress={props.progress} variant="compact" />
        ) : null}

        <div className="despacho-amount">
          <span className="despacho-amount-label">Monto picking</span>
          <div className="despacho-amount-value">
            {amount != null ? moneyCLP(amount) : "Monto no disponible"}
          </div>
        </div>
      </div>

      <div className="plan-card-footer">
        <Link
          href={`/despachos/${p.id}`}
          className={`btn ${props.hero ? "btn-primary btn-with-arrow" : "btn-secondary"}`}
        >
          {cta}
          {props.hero ? (
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden>
              <path d="M5 12h14" />
              <path d="m13 6 6 6-6 6" />
            </svg>
          ) : null}
        </Link>
      </div>
    </article>
  )
}

export default function DespachosPage() {
  const [items, setItems] = useState<DriverDespacho[] | null>(null)
  const [progressById, setProgressById] = useState<Record<number, RouteProgress>>({})
  const [error, setError] = useState<string | null>(null)
  const [offlineSource, setOfflineSource] = useState<string | null>(null)
  const [offlineReadyCount, setOfflineReadyCount] = useState(0)
  const todayLabel = useMemo(() => {
    const ymd = santiagoTodayYmd()
    const [y, m, d] = ymd.split("-")
    return `${d}/${m}/${y}`
  }, [])

  useEffect(() => {
    let cancelled = false
    async function load() {
      try {
        const data = await listMyDespachos()
        if (cancelled) return
        const list = data.items || []
        setItems(list)
        setOfflineSource(null)
        setOfflineReadyCount(0)
        setError(null)
        const uid = getSessionUserId() || String(data.driver_user_id || "")
        if (uid) {
          const syncedAt = new Date().toISOString()
          await saveOfflinePlans(uid, {
            items: list,
            count: list.length,
            syncedAt,
          })
          await saveOfflineMeta({
            userId: uid,
            email: "",
            profiles: ["chofer"],
            syncedAt,
          })
        }
        const buckets = partitionPlansByDate(list)
        const map: Record<number, RouteProgress> = {}
        await Promise.all(
          buckets.today.map(async (p) => {
            try {
              const res = await getMyDespachoProgress(p.id)
              if (res.progress) map[p.id] = res.progress
            } catch {
              /* ignore */
            }
          }),
        )
        if (!cancelled) setProgressById(map)

        const past = [...buckets.history, ...buckets.unknown].slice(0, 6)
        for (const p of past) {
          if (cancelled) break
          try {
            const res = await getMyDespachoProgress(p.id)
            if (cancelled || !res.progress) continue
            setProgressById((prev) =>
              prev[p.id] ? prev : { ...prev, [p.id]: res.progress },
            )
          } catch {
            /* ignore */
          }
        }
      } catch (err) {
        if (cancelled) return
        if (isLikelyOfflineError(err)) {
          const uid = getSessionUserId()
          const cached = uid ? await getOfflinePlans(uid) : null
          const ready = uid ? await listOfflineReadyPlans(uid) : []
          if (cached?.items?.length) {
            setItems(cached.items as DriverDespacho[])
            setOfflineSource(cached.syncedAt)
            setOfflineReadyCount(ready.length)
            setError(null)
            return
          }
          setItems([])
          setOfflineSource(null)
          setError(
            "Sin conexión y no hay despacho descargado en este dispositivo. Conéctate, abre Mis despachos y vuelve a intentar.",
          )
          return
        }
        setError(err instanceof ApiError ? err.message : "No se pudieron cargar los despachos")
        setItems([])
      }
    }
    void load()
    return () => {
      cancelled = true
    }
  }, [])

  const buckets = useMemo(
    () => partitionPlansByDate(items || []),
    [items],
  )

  const pendingPast = useMemo(() => {
    return [...buckets.history, ...buckets.unknown].filter((p) => {
      const pr = progressById[p.id]
      if (!pr) return true
      return pr.remaining_clients > 0 || (pr.by_result?.in_progress || 0) > 0
    })
  }, [buckets.history, buckets.unknown, progressById])

  const closedHistory = useMemo(() => {
    return [...buckets.history, ...buckets.unknown].filter((p) => {
      const pr = progressById[p.id]
      if (!pr) return false
      return pr.remaining_clients <= 0 && (pr.by_result?.in_progress || 0) <= 0
    })
  }, [buckets.history, buckets.unknown, progressById])

  if (items === null && !error) {
    return <div className="empty">Cargando despachos…</div>
  }

  if (error) {
    return <div className="banner banner-error">{error}</div>
  }

  if (!items || items.length === 0) {
    return (
      <div className="card empty">
        <h2 style={{ marginBottom: 8 }}>Sin despachos asignados</h2>
        <p className="muted">
          No hay planificaciones con tu usuario como chofer. Si crees que falta
          uno, pide a operaciones que te asigne con el selector de chofer.
        </p>
      </div>
    )
  }

  return (
    <div className="stack-gap">
      {offlineSource ? (
        <div className="banner banner-warn" role="status">
          {offlineReadyCount > 0
            ? `Datos disponibles sin conexión (${offlineReadyCount} completo${offlineReadyCount === 1 ? "" : "s"}) · Última actualización: ${formatSyncedAt(offlineSource)} · solo lectura`
            : `Lista de despachos en caché · ${formatSyncedAt(offlineSource)}. Para clientes/productos abre cada despacho → Clientes con internet.`}
        </div>
      ) : null}
      <section>
        <h2 className="section-title">Despacho de hoy</h2>
        <p className="muted" style={{ margin: "0 0 10px", fontSize: "0.78rem" }}>
          Fecha operativa Chile · {todayLabel}
        </p>
        {buckets.today.length === 0 ? (
          <div className="card">
            <p style={{ margin: 0 }}>No tienes un despacho asignado para hoy.</p>
            <p className="muted" style={{ margin: "8px 0 0", fontSize: "0.85rem" }}>
              {pendingPast.length > 0
                ? `Hay ${pendingPast.length} despacho(s) de días anteriores con gestiones pendientes.`
                : "Consulta próximos o historial más abajo."}
            </p>
          </div>
        ) : (
          <div className="stack-gap">
            {buckets.today.map((p) => (
              <PlanCard
                key={p.id}
                plan={p}
                hero
                cta="Abrir despacho"
                progress={progressById[p.id] || null}
              />
            ))}
          </div>
        )}
      </section>

      {pendingPast.length > 0 ? (
        <section>
          <h2 className="section-title">Despachos pendientes</h2>
          <p className="muted" style={{ margin: "0 0 10px", fontSize: "0.78rem" }}>
            Planes de días anteriores con clientes aún por gestionar. La fecha del
            picking no se altera.
          </p>
          <div className="stack-gap">
            {pendingPast.map((p) => (
              <PlanCard
                key={p.id}
                plan={p}
                compact
                progress={progressById[p.id] || null}
              />
            ))}
          </div>
        </section>
      ) : null}

      <details className="details-block">
        <summary>
          Próximos despachos
          <span className="muted">{buckets.upcoming.length}</span>
        </summary>
        <div className="details-body">
          {buckets.upcoming.length === 0 ? (
            <p className="muted" style={{ margin: 0 }}>
              Sin despachos futuros asignados.
            </p>
          ) : (
            buckets.upcoming.map((p) => (
              <PlanCard key={p.id} plan={p} compact cta="Ver detalle" />
            ))
          )}
        </div>
      </details>

      <details className="details-block">
        <summary>
          Historial
          <span className="muted">{closedHistory.length}</span>
        </summary>
        <div className="details-body">
          {closedHistory.length === 0 ? (
            <p className="muted" style={{ margin: 0 }}>
              Sin historial gestionado aún.
            </p>
          ) : (
            closedHistory.map((p) => (
              <PlanCard
                key={p.id}
                plan={p}
                compact
                cta="Ver detalle"
                progress={progressById[p.id] || null}
              />
            ))
          )}
        </div>
      </details>
    </div>
  )
}
