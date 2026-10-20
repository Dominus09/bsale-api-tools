"use client"

import { useEffect, useState } from "react"
import { Loader2 } from "lucide-react"

import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { getReception } from "@/lib/analytics-v2/api"
import { formatDate, formatDateTime, formatMoney, formatQty } from "@/lib/analytics-v2/labels"
import type { ReceptionDetail } from "@/lib/analytics-v2/types"

import { MovementBadge } from "./badges"

export function CostV2ReceptionDialog({
  companyId,
  receptionId,
  highlightDetailId,
  onOpenChange,
}: {
  companyId: number
  receptionId: number | null
  highlightDetailId?: number | null
  onOpenChange: (open: boolean) => void
}) {
  const [data, setData] = useState<ReceptionDetail | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (receptionId == null) return
    const ac = new AbortController()
    setLoading(true)
    setError(null)
    setData(null)
    getReception(companyId, receptionId, ac.signal)
      .then(setData)
      .catch((e: unknown) => {
        if (!ac.signal.aborted) setError(e instanceof Error ? e.message : "Error al cargar la recepción")
      })
      .finally(() => {
        if (!ac.signal.aborted) setLoading(false)
      })
    return () => ac.abort()
  }, [companyId, receptionId])

  return (
    <Dialog open={receptionId != null} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[85vh] overflow-y-auto sm:max-w-3xl">
        <DialogHeader>
          <DialogTitle>Recepción Bsale #{receptionId}</DialogTitle>
          <DialogDescription>
            Datos originales de BSALE RAW. Solo lectura: los costos no se modifican desde el ERP.
          </DialogDescription>
        </DialogHeader>
        {loading ? (
          <div className="flex justify-center py-8">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          </div>
        ) : error ? (
          <p className="text-sm text-destructive">{error}</p>
        ) : data ? (
          <div className="space-y-4 text-sm">
            <dl className="grid grid-cols-2 gap-x-4 gap-y-2 sm:grid-cols-3">
              <div>
                <dt className="text-[11px] uppercase text-muted-foreground">Fecha ingreso</dt>
                <dd>{formatDate(data.admission_date)}</dd>
              </div>
              <div>
                <dt className="text-[11px] uppercase text-muted-foreground">Sucursal</dt>
                <dd>{data.office_name ?? data.office_id ?? "—"}</dd>
              </div>
              <div>
                <dt className="text-[11px] uppercase text-muted-foreground">Tipo</dt>
                <dd>
                  <MovementBadge movement={data.movement_class} />
                </dd>
              </div>
              <div className="col-span-2">
                <dt className="text-[11px] uppercase text-muted-foreground">Documento</dt>
                <dd>
                  {data.document ?? "Sin etiqueta"} {data.document_number ? `N° ${data.document_number}` : ""}
                </dd>
              </div>
              <div>
                <dt className="text-[11px] uppercase text-muted-foreground">Despacho interno</dt>
                <dd>{data.internal_dispatch_id ? `#${data.internal_dispatch_id}` : "No"}</dd>
              </div>
              <div>
                <dt className="text-[11px] uppercase text-muted-foreground">Detalle</dt>
                <dd>
                  {data.details_complete ? "Completo" : "Incompleto"} ({data.details_count ?? "—"} líneas)
                </dd>
              </div>
              <div>
                <dt className="text-[11px] uppercase text-muted-foreground">Último cambio en Bsale</dt>
                <dd>{formatDateTime(data.last_changed_at)}</dd>
              </div>
              <div>
                <dt className="text-[11px] uppercase text-muted-foreground">Ausente desde</dt>
                <dd>{data.missing_since ? formatDateTime(data.missing_since) : "Vigente"}</dd>
              </div>
            </dl>
            <div className="overflow-x-auto rounded-md border">
              <table className="w-full text-sm">
                <thead className="bg-muted/40 text-left text-[11px] uppercase text-muted-foreground">
                  <tr>
                    <th className="px-2 py-1.5 font-medium">Línea</th>
                    <th className="px-2 py-1.5 font-medium">Producto</th>
                    <th className="px-2 py-1.5 text-right font-medium">Cantidad</th>
                    <th className="px-2 py-1.5 text-right font-medium">Costo unitario</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border/60">
                  {data.lines.map((l) => (
                    <tr
                      key={l.detail_id}
                      className={l.detail_id === highlightDetailId ? "bg-amber-50 dark:bg-amber-400/15" : l.missing_since ? "opacity-50" : ""}
                    >
                      <td className="px-2 py-1.5 tabular-nums">{l.detail_id}</td>
                      <td className="px-2 py-1.5">
                        <p className="font-medium">{l.product_name ?? "—"}</p>
                        <p className="text-[11px] text-muted-foreground">
                          {l.code ?? "—"}
                          {l.missing_since ? " · línea ausente en Bsale" : ""}
                        </p>
                      </td>
                      <td className="px-2 py-1.5 text-right tabular-nums">{formatQty(l.quantity)}</td>
                      <td className="px-2 py-1.5 text-right tabular-nums">{formatMoney(l.cost)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {data.events.length > 0 ? (
              <div>
                <h3 className="text-sm font-semibold">Bitácora de cambios RAW</h3>
                <ul className="mt-1 space-y-1 text-xs text-muted-foreground">
                  {data.events.map((e, i) => (
                    <li key={i}>
                      {formatDateTime(e.detected_at)} · {e.event_kind}
                      {e.cost_changed_detail_ids.length ? ` · costo cambió en ${e.cost_changed_detail_ids.length} línea(s)` : ""}
                      {e.quantity_changed_detail_ids.length
                        ? ` · cantidad cambió en ${e.quantity_changed_detail_ids.length} línea(s)`
                        : ""}
                      {e.variant_changed_detail_ids.length
                        ? ` · variante cambió en ${e.variant_changed_detail_ids.length} línea(s)`
                        : ""}
                      {e.added_lines ? ` · ${e.added_lines} línea(s) agregada(s)` : ""}
                      {e.removed_lines ? ` · ${e.removed_lines} línea(s) ausente(s)` : ""}
                    </li>
                  ))}
                </ul>
              </div>
            ) : null}
          </div>
        ) : null}
      </DialogContent>
    </Dialog>
  )
}
