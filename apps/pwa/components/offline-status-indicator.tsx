"use client"

import { useState } from "react"
import { OfflineOutboxPanel } from "@/components/offline-outbox-panel"
import { OfflineOutboxPendingBadge } from "@/components/offline-outbox-pending-badge"
import { useOfflineSyncOptional } from "@/components/offline-sync-provider"
import type { IndicatorTone } from "@/lib/offline-sync-runtime"

function toneLabel(tone: IndicatorTone): string {
  switch (tone) {
    case "green":
      return "Conexión OK · recorrido preparado"
    case "orange":
      return "Sincronizando despacho"
    case "yellow":
      return "Sin conexión · consulta offline disponible"
    case "red":
      return "Sin conexión · sin datos preparados"
    case "gray":
    default:
      return "Pendiente de preparación offline"
  }
}

export function OfflineStatusIndicator() {
  const ctx = useOfflineSyncOptional()
  const [outboxOpen, setOutboxOpen] = useState(false)
  if (!ctx) return null
  const { state, setPanelOpen } = ctx
  const tone = state.tone

  return (
    <span className="offline-status-cluster">
      <OfflineOutboxPendingBadge onOpen={() => setOutboxOpen(true)} />
      <button
        type="button"
        className={`offline-dot offline-dot-${tone}`}
        aria-label={toneLabel(tone)}
        title={toneLabel(tone)}
        onClick={() => setPanelOpen(true)}
      >
        <span className="offline-dot-core" aria-hidden />
      </button>
      <OfflineOutboxPanel open={outboxOpen} onClose={() => setOutboxOpen(false)} />
    </span>
  )
}
