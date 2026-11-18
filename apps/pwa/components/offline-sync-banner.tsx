"use client"

import { useEffect, useState } from "react"
import { getSessionUserId } from "@/lib/auth"
import { formatSyncedAt, getOfflineMeta, getOfflinePlans } from "@/lib/offline-store"

export function OfflineSyncBanner() {
  const [online, setOnline] = useState(true)
  const [syncedAt, setSyncedAt] = useState<string | null>(null)
  const [hasPlans, setHasPlans] = useState(false)

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
      const [meta, plans] = await Promise.all([
        getOfflineMeta(uid),
        getOfflinePlans(uid),
      ])
      if (cancelled) return
      setSyncedAt(meta?.syncedAt || plans?.syncedAt || null)
      setHasPlans(Boolean(plans?.items?.length))
    }
    void load()
    return () => {
      cancelled = true
    }
  }, [online])

  if (online && hasPlans && syncedAt) {
    return (
      <div className="banner banner-info" role="status" style={{ fontSize: "0.82rem" }}>
        Datos disponibles sin conexión · Última actualización: {formatSyncedAt(syncedAt)}
      </div>
    )
  }

  if (!online) {
    return (
      <div className="banner banner-warn" role="status">
        {hasPlans
          ? `Sin conexión · mostrando datos sincronizados · ${formatSyncedAt(syncedAt)}`
          : "Sin conexión · no hay despacho descargado en este dispositivo"}
      </div>
    )
  }

  return null
}
