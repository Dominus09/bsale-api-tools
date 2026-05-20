"use client"

import { useCallback, useEffect, useState } from "react"
import { Loader2, RefreshCw } from "lucide-react"

import {
  getDistribuidoraSyncStatus,
  postDistribuidoraSyncLiveNow,
  type DistribuidoraLiveSyncLayerStatus,
  type DistribuidoraSyncStatusResponse,
} from "@/lib/api"
import { Button } from "@/components/ui/button"

function formatRelativeTime(iso: string | null | undefined): string {
  if (!iso) return "Sin datos"
  const t = Date.parse(iso)
  if (!Number.isFinite(t)) return iso
  const diffSec = Math.round((Date.now() - t) / 1000)
  if (diffSec < 60) return "hace unos segundos"
  if (diffSec < 3600) return `hace ${Math.floor(diffSec / 60)} min`
  if (diffSec < 86400) return `hace ${Math.floor(diffSec / 3600)} h`
  return new Date(t).toLocaleString("es-CL")
}

type LiveBsaleSyncPanelProps = {
  onSyncComplete?: () => void
  className?: string
}

export function LiveBsaleSyncPanel({ onSyncComplete, className }: LiveBsaleSyncPanelProps) {
  const [status, setStatus] = useState<DistribuidoraSyncStatusResponse | null>(null)
  const [syncing, setSyncing] = useState(false)
  const [message, setMessage] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  const loadStatus = useCallback(async () => {
    try {
      const s = await getDistribuidoraSyncStatus()
      setStatus(s)
    } catch {
      /* panel opcional */
    }
  }, [])

  useEffect(() => {
    void loadStatus()
    const id = window.setInterval(() => void loadStatus(), 60_000)
    return () => window.clearInterval(id)
  }, [loadStatus])

  const handleSync = async () => {
    if (syncing) return
    setSyncing(true)
    setError(null)
    setMessage("Sincronizando…")
    try {
      const res = await postDistribuidoraSyncLiveNow()
      if (!res.ok) {
        const msg =
          res.status === "already_running"
            ? "Ya hay una sincronización en ejecución"
            : res.message || res.error || "Error al sincronizar"
        setError(msg)
        setMessage(null)
        return
      }
      setMessage("Datos actualizados hace unos segundos")
      await loadStatus()
      onSyncComplete?.()
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error al sincronizar")
      setMessage(null)
    } finally {
      setSyncing(false)
    }
  }

  const live = status?.live_sync
  const layers: DistribuidoraLiveSyncLayerStatus[] = live
    ? [
        live.documents_live,
        live.details_live,
        live.related_live,
        live.probable_live,
      ].filter(Boolean) as DistribuidoraLiveSyncLayerStatus[]
    : []

  const globalBusy = status?.live_sync_global_busy === true

  return (
    <div
      className={
        className ??
        "flex flex-col gap-3 rounded-lg border bg-muted/30 p-4 text-sm"
      }
    >
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <p className="font-medium text-foreground">Sincronización Bsale (live)</p>
          <p className="text-xs text-muted-foreground">
            Ventanas cortas: documentos, detalles, relaciones y probables
          </p>
        </div>
        <Button
          type="button"
          size="sm"
          className="gap-2"
          disabled={syncing || globalBusy}
          onClick={() => void handleSync()}
        >
          {syncing ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <RefreshCw className="h-4 w-4" />
          )}
          {syncing ? "Sincronizando…" : "Actualizar desde Bsale"}
        </Button>
      </div>

      {message ? (
        <p className="text-xs text-emerald-700 dark:text-emerald-400">{message}</p>
      ) : null}
      {error ? <p className="text-xs text-destructive">{error}</p> : null}
      {globalBusy && !syncing ? (
        <p className="text-xs text-amber-700 dark:text-amber-400">
          Ya hay una sincronización en ejecución
        </p>
      ) : null}

      <div className="grid gap-1.5 sm:grid-cols-2 lg:grid-cols-4">
        {layers.length === 0 ? (
          <p className="text-xs text-muted-foreground col-span-full">
            Última actualización: sin corridas live registradas aún
          </p>
        ) : (
          layers.map((layer) => (
            <div
              key={layer.label}
              className="rounded-md border bg-background/80 px-2.5 py-2"
            >
              <p className="text-xs font-medium">{layer.label}</p>
              <p className="text-xs text-muted-foreground">
                {formatRelativeTime(layer.last_success_at)}
              </p>
              {layer.status === "error" && layer.error_summary ? (
                <p className="mt-0.5 truncate text-[10px] text-destructive" title={layer.error_summary}>
                  Error
                </p>
              ) : null}
            </div>
          ))
        )}
      </div>
    </div>
  )
}
