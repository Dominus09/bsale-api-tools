"use client"

import { useEffect, useState } from "react"
import { getSessionUserId } from "@/lib/auth"
import {
  formatSyncedAt,
  listOfflineReadyPlans,
} from "@/lib/offline-store"

export function OfflineSyncBanner() {
  const [online, setOnline] = useState(true)
  const [syncedAt, setSyncedAt] = useState<string | null>(null)
  const [readyCount, setReadyCount] = useState(0)

  useEffect(() => {
    function sync() {
      setOnline(typeof navigator === "undefined" ? true : navigator.onLine)
    }
    sync()
    window.addEventListener("online", sync)
    window.addEventListener("offline", sync)
    return () => {
      window.removeEventListener("online", sync)
      window.removeEventListener("offline", sync)
    }
  }, [])

  useEffect(() => {
    let cancelled = false
    async function load() {
      const uid = getSessionUserId()
      if (!uid) return
      const ready = await listOfflineReadyPlans(uid)
      if (cancelled) return
      setReadyCount(ready.length)
      const latest = ready
        .map((r) => r.syncedAt)
        .filter(Boolean)
        .sort()
        .at(-1)
      setSyncedAt(latest || null)
    }
    void load()
    return () => {
      cancelled = true
    }
  }, [online])

  if (online && readyCount > 0 && syncedAt) {
    return (
      <div className="banner banner-info" role="status" style={{ fontSize: "0.82rem" }}>
        Datos disponibles sin conexión ({readyCount} despacho
        {readyCount === 1 ? "" : "s"}) · Última actualización:{" "}
        {formatSyncedAt(syncedAt)}
      </div>
    )
  }

  if (!online) {
    return (
      <div className="banner banner-warn" role="status">
        {readyCount > 0
          ? `Sin conexión · consulta solo lectura · ${formatSyncedAt(syncedAt)}`
          : "Sin conexión · no hay despacho completo descargado (abre Clientes con internet)"}
      </div>
    )
  }

  return null
}
