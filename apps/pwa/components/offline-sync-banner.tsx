"use client"

import { useEffect, useState } from "react"
import { getSessionUserId } from "@/lib/auth"
import { isOfflineUiReady, loadOfflineBuildInfo } from "@/lib/offline-build"
import {
  formatSyncedAt,
  getOfflinePlans,
  listOfflineReadyPlans,
} from "@/lib/offline-store"

/** Único aviso compacto de conexión / sync. */
export function OfflineSyncBanner() {
  const [online, setOnline] = useState(true)
  const [syncedAt, setSyncedAt] = useState<string | null>(null)
  const [readyCount, setReadyCount] = useState(0)
  const [hasPlans, setHasPlans] = useState(false)
  const [hasDataOnly, setHasDataOnly] = useState(false)

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
      const build = await loadOfflineBuildInfo()
      const [readyRows, plans] = await Promise.all([
        listOfflineReadyPlans(uid, { includeDataOnly: true }),
        getOfflinePlans(uid),
      ])
      if (cancelled) return
      const uiReady = readyRows.filter((r) => isOfflineUiReady(r, build.navBuildId))
      const dataOnly = readyRows.filter(
        (r) => Boolean(r.dataComplete ?? r.complete) && !isOfflineUiReady(r, build.navBuildId),
      )
      setReadyCount(uiReady.length)
      setHasDataOnly(dataOnly.length > 0)
      setHasPlans(Boolean(plans?.items?.length))
      const latest =
        readyRows
          .map((r) => r.syncedAt)
          .filter(Boolean)
          .sort()
          .at(-1) ||
        plans?.syncedAt ||
        null
      setSyncedAt(latest)
    }
    void load()
    const t = window.setInterval(() => {
      void load()
    }, 8000)
    return () => {
      cancelled = true
      window.clearInterval(t)
    }
  }, [online])

  if (!online) {
    const label =
      readyCount > 0
        ? `Sin conexión · Datos guardados: ${formatSyncedAt(syncedAt)}`
        : hasDataOnly || hasPlans
          ? `Sin conexión · Datos locales · falta cache de esta versión`
          : "Sin conexión · sin despachos descargados"
    return (
      <p
        className="muted"
        role="status"
        style={{ margin: "0 0 10px", fontSize: "0.78rem", lineHeight: 1.35 }}
      >
        {label}
      </p>
    )
  }

  if (readyCount > 0 && syncedAt) {
    return (
      <p
        className="muted"
        role="status"
        style={{ margin: "0 0 10px", fontSize: "0.78rem", lineHeight: 1.35 }}
      >
        {readyCount} listo{readyCount === 1 ? "" : "s"} sin conexión ·{" "}
        {formatSyncedAt(syncedAt)}
      </p>
    )
  }

  if (hasDataOnly) {
    return (
      <p
        className="muted"
        role="status"
        style={{ margin: "0 0 10px", fontSize: "0.78rem", lineHeight: 1.35 }}
      >
        Datos guardados · preparando navegación de esta versión…
      </p>
    )
  }

  return null
}
