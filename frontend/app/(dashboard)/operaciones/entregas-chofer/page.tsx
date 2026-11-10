"use client"

/**
 * P4 — Visitas de entrega (solo visibilidad operativa).
 * Sin descuento de stock ni cierre financiero.
 */

import { useCallback, useEffect, useState } from "react"
import { Eye, Loader2, RefreshCw } from "lucide-react"

import {
  getDeliveryVisitErpDetail,
  getDeliveryVisitsErp,
  type DeliveryVisitErpDetail,
  type DeliveryVisitErpItem,
  type DeliveryVisitErpMediaItem,
} from "@/lib/api"
import { Button } from "@/components/ui/button"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"

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

function MediaBlock({
  title,
  item,
}: {
  title: string
  item: DeliveryVisitErpMediaItem | null | undefined
}) {
  if (!item) {
    return (
      <div className="rounded border p-3 text-sm text-muted-foreground">
        {title}: sin foto
      </div>
    )
  }
  return (
    <div className="space-y-2 rounded border p-3">
      <div className="text-sm font-medium">{item.label_es || title}</div>
      <div className="text-xs text-muted-foreground">
        Estado: {item.validation_status || "—"}
        {item.has_gps != null ? ` · GPS: ${item.has_gps ? "sí" : "no"}` : ""}
        {item.expires_in_sec ? ` · URL ~${item.expires_in_sec}s` : ""}
      </div>
      {item.note_es ? (
        <p className="text-xs text-amber-700 dark:text-amber-400">{item.note_es}</p>
      ) : null}
      {item.exception_note ? (
        <p className="text-xs text-muted-foreground">Excepción: {item.exception_note}</p>
      ) : null}
      {item.url ? (
        // eslint-disable-next-line @next/next/no-img-element
        <img
          src={item.url}
          alt={item.label_es || title}
          className="max-h-[70vh] w-full rounded object-contain bg-muted/30"
        />
      ) : (
        <p className="text-sm text-muted-foreground">
          {item.upload_confirmed === false
            ? "Carga no confirmada / excepción sin imagen."
            : "Sin URL firmada disponible."}
        </p>
      )}
    </div>
  )
}

export default function EntregasChoferPage() {
  const [items, setItems] = useState<DeliveryVisitErpItem[]>([])
  const [summary, setSummary] = useState<Record<string, number>>({})
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [tablesReady, setTablesReady] = useState(true)
  const [note, setNote] = useState<string | null>(null)

  const [detailOpen, setDetailOpen] = useState(false)
  const [detailLoading, setDetailLoading] = useState(false)
  const [detailError, setDetailError] = useState<string | null>(null)
  const [detail, setDetail] = useState<DeliveryVisitErpDetail | null>(null)
  const [selected, setSelected] = useState<DeliveryVisitErpItem | null>(null)

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

  async function openDetail(it: DeliveryVisitErpItem) {
    setSelected(it)
    setDetailOpen(true)
    setDetail(null)
    setDetailError(null)
    setDetailLoading(true)
    try {
      const res = await getDeliveryVisitErpDetail(it.id)
      setDetail(res)
    } catch (e) {
      setDetailError(e instanceof Error ? e.message : "Error al cargar detalle")
    } finally {
      setDetailLoading(false)
    }
  }

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
                <th className="p-2">Detalle</th>
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
                  <td className="p-2">
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      onClick={() => void openDetail(it)}
                    >
                      <Eye className="mr-1 h-3.5 w-3.5" />
                      Ver
                    </Button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <Dialog open={detailOpen} onOpenChange={setDetailOpen}>
        <DialogContent className="max-h-[90vh] w-[min(96vw,720px)] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>
              Entrega #{selected?.id} · {selected?.customer_key}
            </DialogTitle>
            <DialogDescription>
              Fotos privadas con URL firmada. Fachada ≠ evidencia de visita.
            </DialogDescription>
          </DialogHeader>

          {detailLoading ? (
            <div className="flex items-center gap-2 text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" /> Cargando detalle…
            </div>
          ) : null}
          {detailError ? (
            <Alert variant="destructive">
              <AlertTitle>Error</AlertTitle>
              <AlertDescription>{detailError}</AlertDescription>
            </Alert>
          ) : null}

          {detail ? (
            <div className="space-y-3">
              {detail.note_es ? (
                <p className="text-xs text-muted-foreground">{detail.note_es}</p>
              ) : null}

              <MediaBlock title="Fachada del cliente" item={detail.facade_photo} />

              <div className="space-y-2">
                <h3 className="text-sm font-medium">Evidencias de incidencia</h3>
                {(detail.incident_evidence || []).length === 0 ? (
                  <p className="text-sm text-muted-foreground">Sin evidencias de visita.</p>
                ) : (
                  (detail.incident_evidence || []).map((ev) => (
                    <MediaBlock
                      key={ev.id || `${ev.evidence_kind}-${ev.captured_at}`}
                      title={ev.label_es || "Evidencia"}
                      item={ev}
                    />
                  ))
                )}
              </div>

              <div className="space-y-2 rounded border p-3">
                <div className="text-sm font-medium">Conformidad / firma</div>
                {detail.conformity?.refused ? (
                  <p className="text-sm">
                    Negativa a firmar
                    {detail.conformity.refuse_reason
                      ? `: ${detail.conformity.refuse_reason}`
                      : ""}
                  </p>
                ) : detail.conformity?.signature_url ? (
                  // eslint-disable-next-line @next/next/no-img-element
                  <img
                    src={detail.conformity.signature_url}
                    alt="Firma del receptor"
                    className="max-h-48 w-full rounded object-contain bg-muted/30"
                  />
                ) : (
                  <p className="text-sm text-muted-foreground">Sin firma visualizable.</p>
                )}
                {detail.conformity?.receptor_name ? (
                  <p className="text-xs text-muted-foreground">
                    Receptor: {detail.conformity.receptor_name}
                  </p>
                ) : null}
              </div>
            </div>
          ) : null}
        </DialogContent>
      </Dialog>
    </div>
  )
}
