"use client"

import { useOfflineSyncOptional } from "@/components/offline-sync-provider"
import { formatSyncedAt } from "@/lib/offline-store"

export function OfflinePrepModal() {
  const ctx = useOfflineSyncOptional()
  if (!ctx) return null
  const { state, dismissPrep, syncNow, choosePlan } = ctx
  if (!state.showPrepModal && state.phase !== "needs_selection") return null

  if (state.phase === "needs_selection" && state.selectionPlans.length > 1) {
    return (
      <div className="offline-modal-root" role="dialog" aria-modal="true" aria-label="Elegir despacho">
        <div className="offline-modal-card">
          <h2>Varios recorridos activos</h2>
          <p className="muted">
            Elige qué despacho preparar para consulta sin internet. No se
            descargarán todos automáticamente.
          </p>
          <ul className="offline-modal-list">
            {state.selectionPlans.map((p) => (
              <li key={p.id}>
                <button
                  type="button"
                  className="btn btn-secondary"
                  style={{ width: "100%" }}
                  onClick={() => choosePlan(p.id)}
                >
                  {p.planning_code || `Plan #${p.id}`}
                  {p.planning_name ? ` · ${p.planning_name}` : ""}
                </button>
              </li>
            ))}
          </ul>
        </div>
      </div>
    )
  }

  if (!state.showPrepModal) return null

  const plan = state.plan
  const progress = state.progress
  const ready = state.ready
  const done = state.phase === "done" && Boolean(ready?.complete)
  const errored = state.phase === "error"
  const clientsDone = progress?.detailsDone ?? ready?.detailsCount ?? 0
  const clientsTotal = progress?.clientsTotal ?? ready?.clientsCount ?? 0
  const docsDone = progress?.documentsDone ?? ready?.documentsCount ?? 0
  const docsTotal =
    progress?.documentsTotal ?? ready?.fingerprint?.documents ?? docsDone
  const pct =
    clientsTotal > 0
      ? Math.min(99, Math.round((clientsDone / clientsTotal) * 100))
      : done
        ? 100
        : 0
  const barPct = done && ready?.complete ? 100 : pct

  return (
    <div
      className="offline-modal-root"
      role="dialog"
      aria-modal="true"
      aria-label="Preparando tu recorrido"
    >
      <div className="offline-modal-card">
        {done ? (
          <>
            <h2>¡Tu recorrido está listo para usar sin internet!</h2>
            <p className="muted">
              Puedes consultar clientes, pedidos y productos sin conexión. Las
              entregas siguen requiriendo internet.
            </p>
          </>
        ) : (
          <>
            <h2>Preparando tu recorrido</h2>
            <p className="muted">
              Estamos descargando tu despacho para que puedas consultar clientes y
              productos sin internet.
            </p>
          </>
        )}

        <div className="offline-modal-meta">
          <strong>
            {plan?.planning_code || (plan ? `Plan #${plan.id}` : "Despacho")}
          </strong>
          {plan?.planning_name ? (
            <span className="muted">{plan.planning_name}</span>
          ) : null}
        </div>

        <div className="offline-progress-track" aria-hidden>
          <div
            className="offline-progress-fill"
            style={{ width: `${barPct}%` }}
          />
        </div>
        <p className="muted" style={{ margin: "8px 0 0", fontSize: "0.8rem" }}>
          Clientes {clientsDone}/{clientsTotal || "—"} · Documentos {docsDone}/
          {docsTotal || "—"}
        </p>
        <p className="muted" style={{ margin: "4px 0 0", fontSize: "0.8rem" }} role="status">
          {state.message || progress?.message || "…"}
        </p>
        {ready?.syncedAt ? (
          <p className="muted" style={{ margin: "4px 0 0", fontSize: "0.75rem" }}>
            Última sync · {formatSyncedAt(ready.syncedAt)}
          </p>
        ) : null}

        {errored ? (
          <div className="offline-modal-actions">
            <button type="button" className="btn btn-primary" onClick={() => syncNow()}>
              Reintentar
            </button>
            <button type="button" className="btn btn-ghost" onClick={dismissPrep}>
              Seguir online
            </button>
          </div>
        ) : done ? (
          <div className="offline-modal-actions">
            <button type="button" className="btn btn-primary" onClick={dismissPrep}>
              Entendido
            </button>
          </div>
        ) : (
          <p className="muted" style={{ margin: "12px 0 0", fontSize: "0.75rem" }}>
            Puedes seguir usando la app con internet mientras termina la descarga.
          </p>
        )}
      </div>
    </div>
  )
}
