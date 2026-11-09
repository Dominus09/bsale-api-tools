"use client"

/**
 * P3 — Ubicaciones de clientes pendientes (propuestas de chofer).
 */

import { useCallback, useEffect, useState } from "react"
import { Loader2, RefreshCw } from "lucide-react"

import {
  getClientLocationHistory,
  getClientLocationsPending,
  reviewClientLocationProposal,
  type ClientLocationPendingItem,
} from "@/lib/api"
import { Button } from "@/components/ui/button"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"

export default function UbicacionesChoferPage() {
  const [items, setItems] = useState<ClientLocationPendingItem[]>([])
  const [loading, setLoading] = useState(true)
  const [busyId, setBusyId] = useState<number | null>(null)
  const [message, setMessage] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [tablesReady, setTablesReady] = useState(true)
  const [history, setHistory] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await getClientLocationsPending(100)
      setItems(res.items || [])
      setTablesReady(res.tables_ready !== false)
      if (res.message_es) setMessage(res.message_es)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al cargar")
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void load()
  }, [load])

  const review = async (id: number, approve: boolean) => {
    setBusyId(id)
    setError(null)
    try {
      const note = approve
        ? undefined
        : window.prompt("Motivo del rechazo (opcional)") || undefined
      const res = await reviewClientLocationProposal(id, { approve, note })
      setMessage(res.message_es || (approve ? "Aprobada" : "Rechazada"))
      await load()
    } catch (e) {
      setError(e instanceof Error ? e.message : "No se pudo revisar")
    } finally {
      setBusyId(null)
    }
  }

  const showHistory = async (clientId: number) => {
    try {
      const res = await getClientLocationHistory(clientId)
      setHistory(JSON.stringify(res, null, 2))
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error historial")
    }
  }

  return (
    <div className="space-y-4 p-4 sm:p-6">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <h1 className="text-xl font-semibold">Ubicaciones de clientes pendientes</h1>
          <p className="text-sm text-muted-foreground">
            Propuestas GPS y fachadas capturadas por choferes. Solo personal ERP puede
            aprobar o rechazar.
          </p>
        </div>
        <Button type="button" variant="outline" size="sm" onClick={() => void load()}>
          {loading ? (
            <Loader2 className="mr-1 size-3.5 animate-spin" />
          ) : (
            <RefreshCw className="mr-1 size-3.5" />
          )}
          Actualizar
        </Button>
      </div>

      {!tablesReady ? (
        <Alert>
          <AlertTitle>Migración 055 pendiente</AlertTitle>
          <AlertDescription>
            Aplique <code>055_client_location_photos.sql</code> en analytics de forma
            controlada antes de usar esta cola.
          </AlertDescription>
        </Alert>
      ) : null}

      {message ? <p className="text-sm text-muted-foreground">{message}</p> : null}
      {error ? (
        <Alert className="border-red-500/40">
          <AlertTitle>Error</AlertTitle>
          <AlertDescription className="text-xs">{error}</AlertDescription>
        </Alert>
      ) : null}

      <div className="space-y-3">
        {items.map((it) => (
          <div
            key={it.id}
            className="space-y-2 rounded-lg border border-border/70 bg-card/40 p-3 text-sm"
          >
            <div className="flex flex-wrap justify-between gap-2">
              <div>
                <div className="font-medium">
                  Cliente #{it.client_id} · {it.customer_key || "—"}
                </div>
                <div className="text-xs text-muted-foreground">
                  Plan {it.dispatch_plan_id} · chofer user #{it.proposed_by_user_id} ·{" "}
                  {it.captured_at || it.created_at || "—"}
                </div>
              </div>
              <div className="flex flex-wrap gap-1">
                <Button
                  size="sm"
                  disabled={busyId === it.id}
                  onClick={() => void review(it.id, true)}
                >
                  Aprobar
                </Button>
                <Button
                  size="sm"
                  variant="outline"
                  disabled={busyId === it.id}
                  onClick={() => void review(it.id, false)}
                >
                  Rechazar
                </Button>
                <Button
                  size="sm"
                  variant="ghost"
                  onClick={() => void showHistory(it.client_id)}
                >
                  Historial
                </Button>
              </div>
            </div>
            <div className="grid gap-2 sm:grid-cols-2 text-xs">
              <div>
                <div className="text-muted-foreground">Anterior</div>
                <div>
                  {it.previous_lat != null && it.previous_lng != null
                    ? `${it.previous_lat.toFixed(5)}, ${it.previous_lng.toFixed(5)}`
                    : "—"}
                </div>
                <div className="text-muted-foreground">{it.previous_source || ""}</div>
              </div>
              <div>
                <div className="text-muted-foreground">Propuesta</div>
                <div>
                  {it.proposed_lat.toFixed(5)}, {it.proposed_lng.toFixed(5)}
                  {it.accuracy_m != null ? ` · ±${Math.round(it.accuracy_m)} m` : ""}
                </div>
                {it.distance_from_previous_m != null ? (
                  <div className="text-muted-foreground">
                    Distancia: {Math.round(it.distance_from_previous_m)} m
                  </div>
                ) : null}
              </div>
            </div>
            {it.facade_url ? (
              // eslint-disable-next-line @next/next/no-img-element
              <img
                src={it.facade_url}
                alt="Fachada"
                className="mt-1 max-h-48 rounded border object-cover"
              />
            ) : (
              <p className="text-xs text-muted-foreground">Sin fotografía asociada</p>
            )}
          </div>
        ))}
        {!loading && !items.length ? (
          <p className="text-sm text-muted-foreground">No hay propuestas pendientes.</p>
        ) : null}
      </div>

      {history ? (
        <pre className="max-h-64 overflow-auto rounded border bg-muted/20 p-2 text-[11px]">
          {history}
        </pre>
      ) : null}
    </div>
  )
}
