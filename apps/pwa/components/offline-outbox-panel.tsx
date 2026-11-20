"use client"

import { createPortal } from "react-dom"
import { useEffect, useState } from "react"
import { AppLink } from "@/components/app-link"
import { getSessionUserId } from "@/lib/auth"
import {
  listPendingOutboxOps,
  outboxResultLabelEs,
  outboxStatusLabelEs,
  type OutboxOperation,
} from "@/lib/offline-outbox"
import { isOutboxSyncWritesEnabled } from "@/lib/offline-outbox-policy"
import { outboxSyncRuntime } from "@/lib/offline-outbox-sync"
import { formatSyncedAt } from "@/lib/offline-store"

type Props = {
  open: boolean
  onClose: () => void
}

export function OfflineOutboxPanel({ open, onClose }: Props) {
  const [ops, setOps] = useState<OutboxOperation[]>([])
  const [mounted, setMounted] = useState(false)
  const [syncMsg, setSyncMsg] = useState<string | null>(null)
  const writesOn = isOutboxSyncWritesEnabled()

  useEffect(() => {
    setMounted(true)
  }, [])

  useEffect(() => {
    if (!open) return
    const uid = getSessionUserId()
    if (!uid) {
      setOps([])
      return
    }
    void listPendingOutboxOps(uid).then(setOps).catch(() => setOps([]))
    return outboxSyncRuntime.subscribe((s) => {
      setSyncMsg(s.message || s.lastResult)
    })
  }, [open])

  if (!open || !mounted || typeof document === "undefined") return null

  const body = (
    <div
      className="offline-panel-root"
      role="dialog"
      aria-modal="true"
      aria-label="Operaciones pendientes de sincronización"
    >
      <button
        type="button"
        className="offline-panel-backdrop"
        aria-label="Cerrar"
        onClick={onClose}
      />
      <aside className="offline-panel-card outbox-panel-card">
        <div className="offline-panel-head">
          <strong>Operaciones pendientes de sincronización</strong>
          <button type="button" className="btn btn-ghost btn-compact" onClick={onClose}>
            Cerrar
          </button>
        </div>
        <p className="muted" style={{ margin: "0 0 12px", fontSize: "0.78rem" }}>
          {writesOn
            ? "Motor de sync de laboratorio activo. No usar contra API productiva."
            : "Guardadas en este dispositivo. Sync de escrituras deshabilitado por defecto."}
        </p>
        {syncMsg ? (
          <div className="banner banner-info" style={{ fontSize: "0.78rem" }}>
            {syncMsg}
          </div>
        ) : null}
        {writesOn ? (
          <button
            type="button"
            className="btn btn-primary"
            style={{ width: "100%", marginBottom: 12 }}
            onClick={() => {
              void outboxSyncRuntime.syncNow().then(async () => {
                const uid = getSessionUserId()
                if (uid) setOps(await listPendingOutboxOps(uid))
              })
            }}
          >
            Sincronizar ahora
          </button>
        ) : null}
        {ops.length === 0 ? (
          <p className="empty">No hay operaciones pendientes.</p>
        ) : (
          <ul className="outbox-op-list">
            {ops.map((op) => (
              <li key={op.localUuid} className="outbox-op-item">
                <div className="outbox-op-title">
                  {op.clientName || op.customerKey}
                </div>
                <div className="muted" style={{ fontSize: "0.75rem" }}>
                  Plan #{op.planId} · {outboxResultLabelEs(op)}
                </div>
                <div className="muted" style={{ fontSize: "0.75rem" }}>
                  {formatSyncedAt(op.captured_at)} · {outboxStatusLabelEs(op.status)}
                  {op.attempt_count > 0 ? ` · intentos ${op.attempt_count}` : ""}
                </div>
                {op.last_error ? (
                  <div className="banner banner-error" style={{ fontSize: "0.72rem", marginTop: 6 }}>
                    {op.last_error}
                  </div>
                ) : null}
                {op.evidence ? (
                  <div className="pill" style={{ marginTop: 6 }}>
                    Foto guardada en dispositivo
                  </div>
                ) : null}
                <div className="btn-row" style={{ marginTop: 8 }}>
                  <AppLink
                    href={`/despachos/${op.planId}/clientes/${encodeURIComponent(op.customerKey)}`}
                    className="btn btn-secondary btn-compact"
                    onClick={onClose}
                  >
                    Ver cliente
                  </AppLink>
                  {writesOn &&
                  (op.status === "pending" || op.status === "failed_needs_action") ? (
                    <button
                      type="button"
                      className="btn btn-ghost btn-compact"
                      onClick={() => void outboxSyncRuntime.syncNow()}
                    >
                      Reintentar
                    </button>
                  ) : null}
                </div>
              </li>
            ))}
          </ul>
        )}
      </aside>
    </div>
  )

  return createPortal(body, document.body)
}
