"use client"

import { createPortal } from "react-dom"
import { useEffect, useState } from "react"
import { AppLink } from "@/components/app-link"
import { getSessionUserId } from "@/lib/auth"
import { listOutboxOpsForUser } from "@/lib/offline-outbox-store"
import { OUTBOX_UI_LABELS, outboxUiGroup, type OutboxOperation, type OutboxUiGroup } from "@/lib/offline-outbox-types"
import { isOutboxSyncWritesEnabled } from "@/lib/offline-outbox-policy"
import { outboxSyncRuntime } from "@/lib/offline-outbox-sync"
import { formatSyncedAt } from "@/lib/offline-store"

type Props = { open: boolean; onClose: () => void }
type Filter = "all" | OutboxUiGroup
const groups: OutboxUiGroup[] = ["local", "sending", "confirmed", "review"]
const explanations: Record<OutboxUiGroup, string> = {
  local: "Guardada en tu teléfono. Aún no la recibe La Quillotana.",
  sending: "Estamos enviando esta entrega. No vuelvas a registrarla.",
  confirmed: "La Quillotana ya recibió esta entrega.",
  review: "Conservamos el registro. Solicita revisión al encargado.",
}

export function OfflineOutboxPanel({ open, onClose }: Props) {
  const [ops, setOps] = useState<OutboxOperation[]>([])
  const [mounted, setMounted] = useState(false)
  const [filter, setFilter] = useState<Filter>("all")
  const [online, setOnline] = useState(true)
  const [loaded, setLoaded] = useState(false)
  const [state, setState] = useState(() => outboxSyncRuntime.getState())
  const [clock, setClock] = useState(() => Date.now())
  const writesOn = isOutboxSyncWritesEnabled()

  useEffect(() => { setMounted(true) }, [])
  useEffect(() => {
    if (!open) return
    let cancelled = false
    let request = 0
    setLoaded(false)
    setFilter("all")
    setOps([])
    const refresh = async () => {
      const id = ++request
      const uid = getSessionUserId()
      const rows = uid ? await listOutboxOpsForUser(uid) : []
      if (!cancelled && id === request && uid === getSessionUserId()) {
        setOps(rows)
        setClock(Date.now())
        setLoaded(true)
      }
    }
    const onNetwork = () => { setOnline(navigator.onLine); void refresh() }
    onNetwork()
    const unsubscribe = outboxSyncRuntime.subscribe((s) => { setState(s); void refresh() })
    window.addEventListener("lq-outbox-changed", onNetwork)
    window.addEventListener("online", onNetwork)
    window.addEventListener("offline", onNetwork)
    document.addEventListener("visibilitychange", onNetwork)
    const timer = window.setInterval(() => setClock(Date.now()), 15_000)
    return () => {
      cancelled = true
      unsubscribe()
      window.removeEventListener("lq-outbox-changed", onNetwork)
      window.removeEventListener("online", onNetwork)
      window.removeEventListener("offline", onNetwork)
      document.removeEventListener("visibilitychange", onNetwork)
      window.clearInterval(timer)
    }
  }, [open])
  useEffect(() => {
    if (!open) return
    const onKey = (event: KeyboardEvent) => { if (event.key === "Escape") onClose() }
    document.addEventListener("keydown", onKey)
    return () => document.removeEventListener("keydown", onKey)
  }, [open, onClose])

  if (!open || !mounted || typeof document === "undefined") return null
  const count = (group: OutboxUiGroup) => ops.filter(op => outboxUiGroup(op, clock) === group).length
  const waiting = ops.filter(op => outboxUiGroup(op, clock) !== "confirmed")
  const confirmed = ops.filter(op => outboxUiGroup(op, clock) === "confirmed")
  const visible = filter === "all" ? [...waiting, ...confirmed.slice(0, 10)] :
    ops.filter(op => outboxUiGroup(op, clock) === filter)
  const busy = state.phase === "syncing"
  const sendable = ops.some(op => outboxUiGroup(op, clock) === "local" && op.status !== "draft")
  const connection = !online ? "Sin conexión" : state.phase === "paused_auth" ? "Sesión requerida" :
    !writesOn ? "Envío pausado" : busy ? "Enviando" : "Conexión disponible"

  return createPortal(
    <div className="offline-panel-root" role="dialog" aria-modal="true" aria-labelledby="driver-sync-title">
      <button type="button" className="offline-panel-backdrop" aria-label="Cerrar" onClick={onClose} />
      <aside className="offline-panel-card driver-sync-sheet">
        <div className="driver-sync-handle" aria-hidden="true" />
        <div className="driver-sync-heading">
          <div>
            <div className="driver-sync-eyebrow">OPERATIVA · CHOFERES</div>
            <h2 id="driver-sync-title">Mis entregas</h2>
            <p className="driver-sync-connection"><span className={"driver-sync-dot" + (online && writesOn ? " is-online" : "")} />{connection}</p>
          </div>
          <button type="button" className="btn btn-ghost btn-compact driver-sync-close" onClick={onClose} aria-label="Cerrar panel">×</button>
        </div>
        <div className="driver-sync-summary" aria-label="Resumen de entregas">
          {groups.map(group => (
            <button key={group} type="button" className="driver-sync-metric" data-tone={group}
              aria-pressed={filter === group} onClick={() => setFilter(filter === group ? "all" : group)}>
              <strong>{loaded ? count(group) : "—"}</strong><span>{OUTBOX_UI_LABELS[group]}</span>
            </button>
          ))}
        </div>
        <div className="driver-sync-callout" role="status">
          {!online ? "Puedes seguir trabajando con los despachos descargados. Tus registros no se pierden." :
           state.phase === "paused_auth" ? "Vuelve a iniciar sesión para enviar tus registros." :
           !writesOn ? "El envío está pausado. Tus registros siguen guardados en el dispositivo." :
           busy ? "Enviando entregas guardadas. No cierres el registro ni lo repitas." :
           waiting.length ? "Guardar una entrega en tu teléfono no significa que el servidor la haya recibido." :
           "Las entregas sin confirmar aparecerán aquí."}
        </div>
        {writesOn && sendable ? (
          <button type="button" className="btn btn-primary driver-sync-send" disabled={busy || !online || state.phase === "paused_auth"}
            onClick={() => void outboxSyncRuntime.syncNow().catch(() => undefined)}>
            {busy ? "Enviando…" : "Enviar pendientes"}
          </button>
        ) : null}
        <div className="driver-sync-list-head">
          <strong>{filter === "all" ? "Actividad reciente" : OUTBOX_UI_LABELS[filter]}</strong>
          {filter !== "all" ? <button className="btn btn-ghost btn-compact" onClick={() => setFilter("all")}>Ver todo</button> : null}
        </div>
        {!loaded ? <p className="empty">Cargando registros…</p> : visible.length === 0 ? (
          <p className="empty">No hay entregas en este estado.</p>
        ) : (
          <ul className="outbox-op-list driver-sync-list">
            {visible.map(op => {
              const group = outboxUiGroup(op, clock)
              return (
                <li key={op.localUuid} className="outbox-op-item driver-sync-item" data-tone={group}>
                  <div className="driver-sync-item-top"><span className="driver-sync-state" data-tone={group}>{op.status === "draft" ? "Borrador" : OUTBOX_UI_LABELS[group]}</span><span className="muted">{formatSyncedAt(op.captured_at)}</span></div>
                  <div className="outbox-op-title">{op.clientName || op.customerKey}</div>
                  <div className="driver-sync-item-meta">Despacho #{op.planId} · {op.op_type === "complete_delivery" ? "Entrega completa" : "Local cerrado"}</div>
                  <p className="driver-sync-description">{op.status === "draft" ? "Registro aún sin finalizar." : explanations[group]}</p>
                  {op.evidence ? <div className="driver-sync-item-meta">Evidencia fotográfica registrada</div> : null}
                  {group === "review" && op.last_error ? <details className="driver-sync-detail"><summary>Motivo de revisión</summary><p>{op.last_error}</p></details> : null}
                  <AppLink href={`/despachos/${op.planId}/clientes/${encodeURIComponent(op.customerKey)}`} className="btn btn-secondary driver-sync-client" onClick={onClose}>Ver cliente <span aria-hidden="true">→</span></AppLink>
                </li>
              )
            })}
          </ul>
        )}
      </aside>
    </div>, document.body)
}
