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
    document.addEventListener("visibilitychange", onVis)
    window.addEventListener("lq-outbox-changed", onStorage)
    return () => {
      document.removeEventListener("visibilitychange", onVis)
      window.removeEventListener("lq-outbox-changed", onStorage)
    }
  }, [refresh])

  return (
    <button
      type="button"
      className="outbox-pending-badge"
      aria-label={`Mis entregas, ${count} por resolver`}
      title="Mis entregas"
      onClick={onOpen}
    >
      <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="M4 7h16v13H4zM8 3h8l4 4M8 3 4 7m5 7 2 2 4-4" /></svg>
      {count > 0 ? <span>{count > 99 ? "99+" : count}</span> : null}
    </button>
  )
}

export function notifyOutboxChanged(): void {
  if (typeof window === "undefined") return
  window.dispatchEvent(new Event("lq-outbox-changed"))
}
