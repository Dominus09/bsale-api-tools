"use client"

import { useEffect, useMemo, useState } from "react"
import { AppLink } from "@/components/app-link"
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
import { isDeviceOffline, readOfflineDespachos } from "@/lib/offline-load"
import {
  formatSyncedAt,
  isLikelyOfflineError,
  listOfflineReadyPlans,
  saveOfflineMeta,
  saveOfflinePlans,
  type OfflinePlanReady,
} from "@/lib/offline-store"
import { syncPriorityPlansForOffline } from "@/lib/offline-sync"
import { partitionPlansByDate, santiagoTodayYmd } from "@/lib/plan-buckets"

function PlanCard(props: {
  plan: DriverDespacho
  hero?: boolean
  compact?: boolean
  cta?: string
  progress?: RouteProgress | null
  offlineReady?: OfflinePlanReady | null
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
          {props.offlineReady?.complete ? (
            <p className="muted" style={{ margin: "8px 0 0", fontSize: "0.78rem" }}>
              {p.planning_code || `Plan #${p.id}`} · Listo para usar sin conexión ·{" "}
              {formatSyncedAt(props.offlineReady.syncedAt)}
            </p>
          ) : null}
        </div>
        <div className="plan-card-footer">
          <AppLink
            href={`/despachos/${p.id}`}
            className={`btn ${hasPendingOps ? "btn-primary" : "btn-secondary"}`}
          >
            {cta}
          </AppLink>
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

        {props.offlineReady?.complete ? (
          <p className="muted" style={{ margin: "10px 0 0", fontSize: "0.78rem" }}>
            {p.planning_code || `Plan #${p.id}`} · Listo sin conexión ·{" "}
            {formatSyncedAt(props.offlineReady.syncedAt)}
          </p>
        ) : null}
      </div>

      <div className="plan-card-footer">
        <AppLink
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
        </AppLink>
      </div>
    </article>
  )
}

export default function DespachosPage() {
  const [items, setItems] = useState<DriverDespacho[] | null>(null)
  const [progressById, setProgressById] = useState<Record<number, RouteProgress>>({})
  const [error, setError] = useState<string | null>(null)
  const [syncNote, setSyncNote] = useState<string | null>(null)
  const [readyById, setReadyById] = useState<Record<number, OfflinePlanReady>>({})
  const todayLabel = useMemo(() => {
    const ymd = santiagoTodayYmd()
    const [y, m, d] = ymd.split("-")
    return `${d}/${m}/${y}`
  }, [])

  useEffect(() => {
    let cancelled = false

    async function applyOfflineSnapshot() {
      const outcome = await readOfflineDespachos()
      if (cancelled) return
      if (outcome.status === "success") {
        setItems(outcome.items as DriverDespacho[])
        setError(null)
        const ready = await listOfflineReadyPlans(getSessionUserId() || "")
        if (!cancelled) {
          const map: Record<number, OfflinePlanReady> = {}
          for (const r of ready) map[r.planId] = r
          setReadyById(map)
        }
        return
      }
      setItems([])
      setError(
        outcome.status === "empty"
          ? outcome.message
          : outcome.message ||
              "No tienes despachos descargados. Conéctate a internet para sincronizarlos.",
      )
    }

    async function load() {
      // Causa raíz iPhone: fetch API cuelga offline → nunca caía al catch de IDB.
      if (isDeviceOffline()) {
        await applyOfflineSnapshot()
        return
      }

      try {
        const data = await listMyDespachos()
        if (cancelled) return
        const list = data.items || []
        setItems(list)
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

        // Sync offline en background: no bloquea la lista ni añade banners duplicados.
        if (uid && !cancelled) {
          const existing = await listOfflineReadyPlans(uid)
          if (!cancelled) {
            const map: Record<number, OfflinePlanReady> = {}
            for (const r of existing) map[r.planId] = r
            setReadyById(map)
          }
          const priority = [
            ...buckets.today,
            ...buckets.history.slice(0, 2),
          ].slice(0, 3)
          if (priority.length) {
            setSyncNote("Sincronizando para uso sin conexión…")
            void syncPriorityPlansForOffline(uid, priority, (p) => {
              if (!cancelled) setSyncNote(p.message)
            })
              .then((readyList) => {
                if (cancelled) return
                const readyMap: Record<number, OfflinePlanReady> = {}
                for (const r of readyList) readyMap[r.planId] = r
                setReadyById((prev) => ({ ...prev, ...readyMap }))
                const ok = readyList.filter((r) => r.complete).length
                setSyncNote(
                  ok > 0
                    ? `${ok} despacho(s) listo(s) sin conexión`
                    : "Descarga offline incompleta · reintenta con señal",
                )
              })
              .catch(() => {
                if (!cancelled) setSyncNote("No se pudo completar sync offline")
              })
          }
        }
      } catch (err) {
        if (cancelled) return
        if (isLikelyOfflineError(err) || isDeviceOffline()) {
          await applyOfflineSnapshot()
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
    return (
      <div className="stack-gap">
        <div className="banner banner-error" role="alert">
          {error}
        </div>
        <button
          type="button"
          className="btn btn-secondary"
          onClick={() => window.location.reload()}
        >
          Reintentar
        </button>
      </div>
    )
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
      {syncNote ? (
        <p className="muted" role="status" style={{ margin: 0, fontSize: "0.78rem" }}>
          {syncNote}
        </p>
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
                offlineReady={readyById[p.id] || null}
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
                offlineReady={readyById[p.id] || null}
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
