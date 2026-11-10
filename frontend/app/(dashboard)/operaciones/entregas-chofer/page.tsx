"use client"

/**
 * P4 — Visitas de entrega (solo visibilidad operativa).
 * Sin descuento de stock ni cierre financiero.
 */

import { useCallback, useEffect, useState } from "react"
import { Loader2, RefreshCw } from "lucide-react"

import { getDeliveryVisitsErp, type DeliveryVisitErpItem } from "@/lib/api"
import { Button } from "@/components/ui/button"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"

const STATUS_ES: Record<string, string> = {
  in_progress: "En atención",
  completed: "Entrega completa",
  partial: "Entrega parcial",
  not_delivered: "No entregada",
  cancelled: "Cancelada",
}

const CONFORMITY_ES: Record<string, string> = {
  signed: "Firma registrada",
  refused: "Negativa a firmar",
  pending: "Conformidad pendiente",
  invalidated: "Firma invalidada",
}

export default function EntregasChoferPage() {
  const [items, setItems] = useState<DeliveryVisitErpItem[]>([])
  const [summary, setSummary] = useState<Record<string, number>>({})
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [tablesReady, setTablesReady] = useState(true)
  const [note, setNote] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await getDeliveryVisitsErp(100)
      setItems(res.items || [])
      setSummary(res.summary || {})
      setTablesReady(res.tables_ready !== false)
      setNote(res.note_es || res.message_es || null)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al cargar")
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void load()
  }, [load])

  return (
    <div className="space-y-4 p-4 sm:p-6">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <h1 className="text-xl font-semibold">Entregas en ruta</h1>
          <p className="text-sm text-muted-foreground">
            Visitas PWA del chofer. Solo lectura operativa — sin stock ni cuadratura.
          </p>
        </div>
        <Button variant="outline" size="sm" onClick={() => void load()} disabled={loading}>
          {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
          <span className="ml-2">Actualizar</span>
        </Button>
      </div>

      {note ? (
        <Alert>
          <AlertTitle>Modo online / visibilidad</AlertTitle>
          <AlertDescription>{note}</AlertDescription>
        </Alert>
      ) : null}

      {error ? (
        <Alert variant="destructive">
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      {!tablesReady ? (
        <Alert>
          <AlertTitle>Migración 056 pendiente</AlertTitle>
          <AlertDescription>
            Las tablas de visitas aún no están aplicadas en este entorno.
          </AlertDescription>
        </Alert>
      ) : null}

      <div className="flex flex-wrap gap-2 text-sm">
        {Object.entries(summary).map(([k, v]) => (
          <span
            key={k}
            className="rounded-full border px-3 py-1 text-muted-foreground"
          >
            {STATUS_ES[k] || k}: {v}
          </span>
        ))}
      </div>

      {loading && items.length === 0 ? (
        <div className="flex items-center gap-2 text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin" /> Cargando…
        </div>
      ) : items.length === 0 ? (
        <p className="text-sm text-muted-foreground">Sin visitas registradas.</p>
      ) : (
        <div className="overflow-x-auto rounded-lg border">
          <table className="w-full text-sm">
            <thead className="bg-muted/40 text-left">
              <tr>
                <th className="p-2">Cliente</th>
                <th className="p-2">Chofer</th>
                <th className="p-2">Estado</th>
                <th className="p-2">Revisión</th>
                <th className="p-2">Inicio / Término</th>
                <th className="p-2">Docs</th>
                <th className="p-2">Unid. ent/pend</th>
                <th className="p-2">Cobranza decl.</th>
                <th className="p-2">Firma</th>
                <th className="p-2">Evidencias</th>
              </tr>
            </thead>
            <tbody>
              {items.map((it) => (
                <tr key={it.id} className="border-t">
                  <td className="p-2">
                    <div>{it.customer_key}</div>
                    <div className="text-xs text-muted-foreground">
                      Plan {it.dispatch_plan_id} · #{it.id}
                    </div>
                  </td>
                  <td className="p-2">{it.driver_user_id}</td>
                  <td className="p-2">
                    {STATUS_ES[it.status] || it.status}
                    {it.result_reason ? (
                      <div className="text-xs text-muted-foreground">{it.result_reason}</div>
                    ) : null}
                  </td>
                  <td className="p-2">
                    {it.closure_review_status === "pending_review"
                      ? "Pendiente revisión"
                      : it.closure_review_status === "verified"
                        ? "Verificada"
                        : "—"}
                  </td>
                  <td className="p-2 whitespace-nowrap text-xs">
                    {it.started_at_local?.slice(0, 16) || "—"}
                    <br />
                    {it.ended_at_local?.slice(0, 16) || "—"}
                  </td>
                  <td className="p-2">{it.document_count ?? "—"}</td>
                  <td className="p-2">
                    {it.delivered_units ?? 0}/{it.pending_units ?? 0}
                  </td>
                  <td className="p-2">
                    {new Intl.NumberFormat("es-CL").format(it.declared_collected || 0)}
                    <div className="text-xs text-muted-foreground">
                      pend.{" "}
                      {new Intl.NumberFormat("es-CL").format(it.pending_balance_sum || 0)}
                    </div>
                  </td>
                  <td className="p-2">
                    {CONFORMITY_ES[it.conformity_status || ""] ||
                      it.conformity_status ||
                      "—"}
                  </td>
                  <td className="p-2">{it.evidence_count ?? 0}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
