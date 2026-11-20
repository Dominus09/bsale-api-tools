"use client"

import { createPortal } from "react-dom"
import { useOfflineSync } from "@/components/offline-sync-provider"
import { formatSyncedAt } from "@/lib/offline-store"

export function OfflineStatusPanel({ onClose }: { onClose: () => void }) {
  const { state, syncNow } = useOfflineSync()
  const plan = state.plan
  const ready = state.ready
  const canSync =
    state.apiReachable !== false &&
    typeof navigator !== "undefined" &&
    navigator.onLine !== false

  const connLabel =
    state.conn === "api"
      ? "API accesible"
      : state.conn === "declared_offline"
        ? "Sin conexión (declarada)"
        : state.conn === "declared_online_unreachable"
          ? "Sin alcance a la API"
          : "Estado de red desconocido"

  const integrity =
    ready?.complete
      ? "Completo (datos + navegación + build)"
      : ready?.dataComplete
        ? "Datos OK · falta navegación/build"
        : ready
          ? "Incompleto"
          : "Sin snapshot"

  const body = (
    <div className="offline-panel-root" role="dialog" aria-modal="true" aria-label="Estado offline">
      <button type="button" className="offline-panel-backdrop" aria-label="Cerrar" onClick={onClose} />
      <aside className="offline-panel-card">
        <div className="offline-panel-head">
          <strong>Estado sin conexión</strong>
          <button type="button" className="btn btn-ghost btn-compact" onClick={onClose}>
            Cerrar
          </button>
        </div>
        <dl className="offline-panel-dl">
          <div>
            <dt>Conexión</dt>
            <dd>{connLabel}</dd>
          </div>
          <div>
            <dt>Despacho preparado</dt>
            <dd>
              {plan
                ? `${plan.planning_code || `Plan #${plan.id}`}${
                    plan.planning_name ? ` · ${plan.planning_name}` : ""
                  }`
                : ready
                  ? `Plan #${ready.planId}`
                  : "Ninguno"}
            </dd>
          </div>
          <div>
            <dt>Última sincronización</dt>
            <dd>{ready?.syncedAt ? formatSyncedAt(ready.syncedAt) : "—"}</dd>
          </div>
          <div>
            <dt>Clientes</dt>
            <dd>
              {ready
                ? `${ready.detailsCount}/${ready.clientsCount}`
                : "—"}
            </dd>
          </div>
          <div>
            <dt>Documentos</dt>
            <dd>
              {ready?.documentsCount != null
                ? String(ready.documentsCount)
                : ready?.fingerprint?.documents != null
                  ? String(ready.fingerprint.documents)
                  : "—"}
            </dd>
          </div>
          <div>
            <dt>Integridad</dt>
            <dd>{integrity}</dd>
          </div>
          {state.message ? (
            <div>
              <dt>Detalle</dt>
              <dd>{state.message}</dd>
            </div>
          ) : null}
        </dl>
        {canSync ? (
          <button
            type="button"
            className="btn btn-primary"
            style={{ width: "100%", marginTop: 12 }}
            onClick={() => {
              syncNow()
              onClose()
            }}
            disabled={state.phase === "syncing"}
          >
            Sincronizar ahora
          </button>
        ) : (
          <p className="muted" style={{ marginTop: 12, fontSize: "0.8rem" }}>
            Sin conectividad API: no se puede sincronizar ahora.
          </p>
        )}
        <p className="muted" style={{ marginTop: 10, fontSize: "0.72rem" }}>
          Offline V1: consulta de fichas. Offline V2: puede guardar entrega
          completa o local cerrado en este dispositivo (pendiente de sync; sin
          envío al servidor en esta fase).
        </p>
      </aside>
    </div>
  )

  if (typeof document === "undefined") return null
  return createPortal(body, document.body)
}
