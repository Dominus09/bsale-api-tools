"use client"

import { useEffect, useState } from "react"
import { getSessionUserId } from "@/lib/auth"
import { offlineBannerLabel } from "@/lib/offline-load"
import {
  formatSyncedAt,
  getOfflinePlans,
  listOfflineReadyPlans,
} from "@/lib/offline-store"

/** Único banner de estado de conexión / sync (evita duplicados AuthGate + página). */
export function OfflineSyncBanner() {
  const [online, setOnline] = useState(true)
  const [syncedAt, setSyncedAt] = useState<string | null>(null)
  const [readyCount, setReadyCount] = useState(0)
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
      const [ready, plans] = await Promise.all([
        listOfflineReadyPlans(uid),
        getOfflinePlans(uid),
      ])
      if (cancelled) return
      setReadyCount(ready.length)
      setHasPlans(Boolean(plans?.items?.length))
      const latest =
        ready
          .map((r) => r.syncedAt)
          .filter(Boolean)
          .sort()
          .at(-1) ||
        plans?.syncedAt ||
        null
      setSyncedAt(latest)
    }
    void load()
    return () => {
      cancelled = true
    }
  }, [online])

  if (!online) {
    return (
      <div className="banner banner-warn" role="status" style={{ fontSize: "0.82rem" }}>
        {readyCount > 0 || hasPlans
          ? offlineBannerLabel(syncedAt)
          : "Sin conexión · No tienes despachos descargados. Conéctate a internet para sincronizarlos."}
      </div>
    )
  }

  if (readyCount > 0 && syncedAt) {
    return (
      <div className="banner banner-info" role="status" style={{ fontSize: "0.82rem" }}>
        {readyCount} despacho{readyCount === 1 ? "" : "s"} listo
        {readyCount === 1 ? "" : "s"} para usar sin conexión · Última sync:{" "}
        {formatSyncedAt(syncedAt)}
      </div>
    )
  }

  return null
}
