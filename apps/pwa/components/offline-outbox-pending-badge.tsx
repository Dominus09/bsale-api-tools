"use client"

import { useCallback, useEffect, useState } from "react"
import { getSessionUserId } from "@/lib/auth"
import { countPendingOutboxOps } from "@/lib/offline-outbox"

type Props = {
  onOpen: () => void
}

/** Contador junto al semáforo de conexión — no usa colores del semáforo comercial. */
export function OfflineOutboxPendingBadge({ onOpen }: Props) {
  const [count, setCount] = useState(0)

  const refresh = useCallback(() => {
    const uid = getSessionUserId()
    if (!uid) {
      setCount(0)
      return
    }
    void countPendingOutboxOps(uid).then(setCount).catch(() => setCount(0))
  }, [])

  useEffect(() => {
    refresh()
    const onVis = () => {
      if (document.visibilityState === "visible") refresh()
    }
    const onStorage = () => refresh()
    window.addEventListener("visibilitychange", onVis)
    window.addEventListener("lq-outbox-changed", onStorage)
    return () => {
      window.removeEventListener("visibilitychange", onVis)
      window.removeEventListener("lq-outbox-changed", onStorage)
    }
  }, [refresh])

  if (count < 1) return null

  return (
    <button
      type="button"
      className="outbox-pending-badge"
      aria-label={`${count} operaciones pendientes de sincronización`}
      title={`${count} pendientes de sincronización`}
      onClick={onOpen}
    >
      {count > 99 ? "99+" : count}
    </button>
  )
}

export function notifyOutboxChanged(): void {
  if (typeof window === "undefined") return
  window.dispatchEvent(new Event("lq-outbox-changed"))
}
